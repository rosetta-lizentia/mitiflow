//! Cross-process determinism regression test for seeded random chaos.
//!
//! Runs the emulator binary twice with the same seed and once with a different
//! seed, then compares the chaos manifests. This test would have caught the
//! HashMap ordering bug (FIX 6) because the action sequence differs across
//! processes when keys are unsorted.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

const TEST_TOPOLOGY: &str = r#"
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
    key_prefix: det-test/events
    num_partitions: 2

components:
  - name: store-1
    kind: storage_agent
    topic: events
    capacity: 50

  - name: prod-1
    kind: producer
    topic: events
    rate: 50.0
    payload:
      generator: counter
      prefix: det

  - name: cons-1
    kind: consumer
    topic: events
    output:
      mode: count
      report_interval_sec: 5

chaos:
  enabled: true
  random:
    fault_rate: 1.0
    duration: 60s
    max_concurrent_faults: 3
    min_interval: 500ms
    actions:
      kill:
        probability: 0.3
        restart_after: [1s, 3s]
        pool: [store-1, cons-1]
      kill_random:
        probability: 0.2
        restart_after: [1s, 2s]
        pool: [store-1, cons-1, prod-1]
      slow:
        probability: 0.3
        delay_ms: [50, 200]
        pool: [prod-1]
      network_partition:
        probability: 0.2
        heal_after: [2s, 5s]
        pool: [store-1, cons-1]
"#;

fn emulator_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mitiflow-emulator"))
}

fn run_emulator(seed: u64, work_dir: &PathBuf) -> String {
    let topo_path = work_dir.join("topology.yaml");
    fs::write(&topo_path, TEST_TOPOLOGY).expect("write topology");

    let manifest_dir = work_dir.join("manifest");
    fs::create_dir_all(&manifest_dir).expect("create manifest dir");

    let output = Command::new(emulator_bin())
        .args([
            "run",
            topo_path.to_str().unwrap(),
            "--seed",
            &seed.to_string(),
            "--duration",
            "8",
        ])
        .current_dir(work_dir)
        .output()
        .expect("failed to run emulator");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "emulator exited with {:?}\nstderr:\n{}",
        output.status.code(),
        stderr
    );

    let manifest_path = manifest_dir.join("chaos-manifest.jsonl");
    fs::read_to_string(&manifest_path).unwrap_or_default()
}

/// Strip the wall-clock `timestamp` field from manifest lines for comparison.
fn normalize_manifest(raw: &str) -> Vec<serde_json::Value> {
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let mut entry: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
                panic!("invalid JSONL line: {line}\nerror: {e}");
            });
            if let Some(obj) = entry.as_object_mut() {
                obj.remove("timestamp");
            }
            entry
        })
        .collect()
}

#[test]
fn cross_process_same_seed_identical_manifest() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let base = tmp.path().to_path_buf();

    let run1_dir = base.join("run1");
    let run2_dir = base.join("run2");
    fs::create_dir_all(&run1_dir).expect("mkdir run1");
    fs::create_dir_all(&run2_dir).expect("mkdir run2");

    let manifest1 = run_emulator(42, &run1_dir);
    let manifest2 = run_emulator(42, &run2_dir);

    let all_entries1 = normalize_manifest(&manifest1);
    let all_entries2 = normalize_manifest(&manifest2);

    let cutoff_ms = 5000_u64;
    let entries1: Vec<_> = all_entries1
        .iter()
        .filter(|e| e.get("elapsed_ms").and_then(|v| v.as_u64()).unwrap_or(0) < cutoff_ms)
        .cloned()
        .collect();
    let entries2: Vec<_> = all_entries2
        .iter()
        .filter(|e| e.get("elapsed_ms").and_then(|v| v.as_u64()).unwrap_or(0) < cutoff_ms)
        .cloned()
        .collect();

    assert!(
        !entries1.is_empty(),
        "run1 produced no manifest entries within {cutoff_ms}ms — chaos scheduler may not be running"
    );

    assert_eq!(
        entries1.len(),
        entries2.len(),
        "same seed produced different entry counts (within {cutoff_ms}ms window): {} vs {}",
        entries1.len(),
        entries2.len()
    );

    for (i, (a, b)) in entries1.iter().zip(entries2.iter()).enumerate() {
        assert_eq!(
            a, b,
            "entry {i} differs between runs with same seed 42\nrun1: {a}\nrun2: {b}"
        );
    }
}

#[test]
fn cross_process_different_seed_different_manifest() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let base = tmp.path().to_path_buf();

    let run_a_dir = base.join("run_a");
    let run_b_dir = base.join("run_b");
    fs::create_dir_all(&run_a_dir).expect("mkdir run_a");
    fs::create_dir_all(&run_b_dir).expect("mkdir run_b");

    let manifest_a = run_emulator(42, &run_a_dir);
    let manifest_b = run_emulator(99, &run_b_dir);

    let entries_a = normalize_manifest(&manifest_a);
    let entries_b = normalize_manifest(&manifest_b);

    assert!(!entries_a.is_empty(), "run_a produced no manifest entries");

    // At least one entry must differ (action, target, timing, or params).
    let any_differs = entries_a.iter().zip(entries_b.iter()).any(|(a, b)| a != b);

    assert!(
        any_differs,
        "different seeds (42 vs 99) produced identical manifests — PRNG may not be seeded correctly"
    );
}
