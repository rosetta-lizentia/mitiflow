//! E2E chaos test for `EventReplayer`.
//!
//! Phase 1: Run the emulator binary with a producer + storage_agent topology
//!          and a scheduled chaos event (kill + restart store-1 at t=4s).
//!          The producer writes a JSONL published-events manifest.
//!
//! Phase 2: Spawn a fresh `mitiflow-emulator-storage-agent` process pointing
//!          at the same on-disk data directory so `EventReplayer` has a live
//!          store to query against.
//!
//! Phase 3: Open an in-process Zenoh peer session and run `EventReplayer`
//!          with `ReplayScope::All` + `ReplayEnd::Bounded` to drain every
//!          stored event.
//!
//! Phase 4: Assert ≥ 70% of published events were replayed and that no
//!          duplicate event IDs appear in the replay output.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use mitiflow::codec::CodecFormat;
use mitiflow::config::EventBusConfig;
use mitiflow::{Error, EventReplayer, ReplayEnd};
use mitiflow_emulator::config::{CodecConfig, RecoveryModeConfig};
use mitiflow_emulator::metrics::{EventManifestEntry, ManifestRole};
use mitiflow_emulator::role_config::{StorageAgentRoleConfig, ZenohRoleConfig, encode_config};

const KEY_PREFIX: &str = "rce2e/events";
const NUM_PARTITIONS: u32 = 4;

/// Topology template — `{DATA_DIR}` is replaced at runtime with an absolute path.
const TOPOLOGY_TEMPLATE: &str = r#"
zenoh:
  timestamping_enabled: true

manifest:
  enabled: true
  directory: "./manifest"

defaults:
  codec: json
  cache_size: 512
  heartbeat_ms: 500
  recovery_mode: both

topics:
  - name: events
    key_prefix: rce2e/events
    num_partitions: 4

components:
  - name: store-1
    kind: storage_agent
    topic: events
    capacity: 50
    data_dir: "{DATA_DIR}"

  - name: prod-1
    kind: producer
    topic: events
    rate: 15.0
    payload:
      generator: counter
      prefix: rce2e

chaos:
  enabled: true
  schedule:
    - at: 4s
      action: kill
      target: store-1
      restart_after: 2s
"#;

fn emulator_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mitiflow-emulator"))
}

fn storage_agent_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mitiflow-emulator-storage-agent"))
}

fn read_published_manifest(manifest_dir: &Path) -> Vec<EventManifestEntry> {
    let path = manifest_dir.join("prod-1-0-published.jsonl");
    let content = fs::read_to_string(&path).unwrap_or_default();
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<EventManifestEntry>(l).ok())
        .filter(|e| e.role == ManifestRole::Published)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayer_survives_storage_agent_chaos() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let work_dir = tmp.path().to_path_buf();
    let manifest_dir = work_dir.join("manifest");
    let data_dir = work_dir.join("store");
    fs::create_dir_all(&manifest_dir).expect("create manifest dir");
    fs::create_dir_all(&data_dir).expect("create data dir");

    // --- Phase 1: emulator run with producer + storage_agent + chaos ---
    let topology = TOPOLOGY_TEMPLATE.replace("{DATA_DIR}", data_dir.to_str().expect("utf8 path"));
    let topo_path = work_dir.join("topology.yaml");
    fs::write(&topo_path, &topology).expect("write topology");

    let output = std::process::Command::new(emulator_bin())
        .args([
            "run",
            topo_path.to_str().expect("utf8 path"),
            "--seed",
            "42",
            "--duration",
            "14",
        ])
        .current_dir(&work_dir)
        .output()
        .expect("run emulator binary");

    assert!(
        output.status.success(),
        "emulator exited non-zero\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let published = read_published_manifest(&manifest_dir);
    assert!(
        !published.is_empty(),
        "producer published no events — check producer binary and topology"
    );

    // --- Phase 2: spawn a fresh storage agent against the persisted data dir ---
    // Instance data dir follows supervisor convention: {data_dir}/{name}-{instance_index}
    let instance_data_dir = data_dir.join("store-1-0");
    assert!(
        instance_data_dir.exists(),
        "instance data dir not found at {instance_data_dir:?} — storage agent may not have written data"
    );

    let zenoh_b64 = encode_config(&ZenohRoleConfig {
        mode: "peer".to_string(),
        listen: vec![],
        connect: vec![],
        timestamping_enabled: true,
    })
    .expect("encode ZenohRoleConfig");

    let store_b64 = encode_config(&StorageAgentRoleConfig {
        key_prefix: KEY_PREFIX.to_string(),
        data_dir: instance_data_dir,
        num_partitions: NUM_PARTITIONS,
        replication_factor: 1,
        capacity: 50,
        node_id: Some("store-1-0".to_string()),
        codec: CodecConfig::Json,
        cache_size: 512,
        heartbeat_ms: 500,
        recovery_mode: RecoveryModeConfig::Both,
        log_level: None,
    })
    .expect("encode StorageAgentRoleConfig");

    let mut storage_agent = std::process::Command::new(storage_agent_bin())
        .env("MITIFLOW_ZENOH_CONFIG", &zenoh_b64)
        .env("MITIFLOW_EMU_CONFIG", &store_b64)
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn fresh storage agent");

    // Give the storage agent time to open its Zenoh session and register queryables.
    tokio::time::sleep(Duration::from_millis(900)).await;

    // --- Phase 3: in-process EventReplayer ---
    let mut zc = zenoh::Config::default();
    let me = |e: Box<dyn std::error::Error + Send + Sync>| e.to_string();
    zc.insert_json5("mode", r#""peer""#)
        .map_err(me)
        .expect("zenoh mode");
    zc.insert_json5("timestamping/enabled", "true")
        .map_err(|e: Box<dyn std::error::Error + Send + Sync>| e.to_string())
        .expect("timestamping");

    let session = zenoh::open(zc).await.expect("open zenoh session");

    let bus_config = EventBusConfig::builder(KEY_PREFIX)
        .codec(CodecFormat::Json)
        .cache_size(512)
        .num_partitions(NUM_PARTITIONS)
        .build()
        .expect("build EventBusConfig");

    let mut replayer = EventReplayer::builder(&session, bus_config)
        .all()
        .end(ReplayEnd::Bounded { limit: usize::MAX })
        .query_timeout(Duration::from_secs(10))
        .build()
        .await
        .expect("build EventReplayer");

    let mut replayed_ids: Vec<String> = Vec::new();
    loop {
        match replayer.recv_raw().await {
            Ok(ev) => {
                replayed_ids.push(ev.id.to_string());
            }
            Err(Error::EndOfReplay) => break,
            Err(e) => panic!("unexpected replay error: {e}"),
        }
    }

    // --- Phase 4: cleanup and assertions ---
    storage_agent.kill().ok();
    storage_agent.wait().ok();
    session.close().await.expect("close zenoh session");

    let published_count = published.len();
    let replayed_count = replayed_ids.len();
    let min_expected = published_count * 7 / 10;

    assert!(
        replayed_count >= min_expected,
        "replayed too few events: got {replayed_count} of {published_count} published (need ≥ {min_expected} = 70%)"
    );

    let unique: HashSet<&String> = replayed_ids.iter().collect();
    assert_eq!(
        unique.len(),
        replayed_count,
        "replay contained duplicates: {replayed_count} total, {} unique",
        unique.len()
    );
}
