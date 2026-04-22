//! Producer role binary — spawned by the supervisor.
//!
//! Reads configuration from MITIFLOW_EMU_CONFIG / MITIFLOW_ZENOH_CONFIG
//! environment variables, creates a Zenoh session and EventPublisher,
//! generates payloads at the configured rate using lightbench's rate controller.
//!

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use lightbench::{Benchmark, BenchmarkWork, WorkResult};
use mitiflow::codec::CodecFormat;
use mitiflow::config::{EventBusConfig, HeartbeatMode, RecoveryMode};
use mitiflow::publisher::EventPublisher;
use mitiflow_emulator::config::{PayloadConfig, RecoveryModeConfig};
use mitiflow_emulator::generator::PayloadGenerator;
use mitiflow_emulator::metrics::{
    EventManifestEntry, ManifestRole, ManifestWriter, payload_checksum,
};
use mitiflow_emulator::role_config::{ProducerRoleConfig, ZenohRoleConfig, decode_config};

// ---------------------------------------------------------------------------
// BenchmarkWork implementation for lightbench rate control
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct EmulatorPublishWork {
    session: zenoh::Session,
    bus_config: EventBusConfig,
    payload_config: PayloadConfig,
    durable: bool,
    /// Counts total successful publishes; shared with the rate-reporter task.
    published: Arc<AtomicU64>,
    manifest_writer: Option<Arc<std::sync::Mutex<ManifestWriter>>>,
}

struct EmulatorPublishState {
    publisher: EventPublisher,
    generator: PayloadGenerator,
}

impl BenchmarkWork for EmulatorPublishWork {
    type State = EmulatorPublishState;

    async fn init(&self) -> Self::State {
        let publisher = EventPublisher::new(&self.session, self.bus_config.clone())
            .await
            .expect("failed to create publisher");
        let generator = PayloadGenerator::from_config(&self.payload_config);
        EmulatorPublishState {
            publisher,
            generator,
        }
    }

    async fn work(&self, state: &mut Self::State) -> WorkResult {
        let payload = state.generator.generate();
        let checksum = payload_checksum(&payload);
        let result = if self.durable {
            state.publisher.publish_bytes_durable(payload).await
        } else {
            state.publisher.publish_bytes(payload).await
        };
        match result {
            Ok(receipt) => {
                self.published.fetch_add(1, Ordering::Relaxed);
                write_manifest_entry(&self.manifest_writer, &receipt, checksum);
                WorkResult::success(0)
            }
            Err(e) => WorkResult::error(e.to_string()),
        }
    }

    async fn cleanup(&self, state: Self::State) {
        state.publisher.shutdown().await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let zenoh_b64 = std::env::var("MITIFLOW_ZENOH_CONFIG")
        .map_err(|_| anyhow::anyhow!("MITIFLOW_ZENOH_CONFIG not set"))?;
    let config_b64 = std::env::var("MITIFLOW_EMU_CONFIG")
        .map_err(|_| anyhow::anyhow!("MITIFLOW_EMU_CONFIG not set"))?;

    let zenoh_cfg: ZenohRoleConfig = decode_config(&zenoh_b64)?;
    let config: ProducerRoleConfig = decode_config(&config_b64)?;

    let mut zc = zenoh::Config::default();
    let me = |e: Box<dyn std::error::Error + Send + Sync>| anyhow::anyhow!("{e}");
    match zenoh_cfg.mode.as_str() {
        "client" => {
            zc.insert_json5("mode", r#""client""#).map_err(&me)?;
        }
        "router" => {
            zc.insert_json5("mode", r#""router""#).map_err(&me)?;
        }
        _ => {
            zc.insert_json5("mode", r#""peer""#).map_err(&me)?;
        }
    }
    if !zenoh_cfg.listen.is_empty() {
        let json = serde_json::to_string(&zenoh_cfg.listen)?;
        zc.insert_json5("listen/endpoints", &json).map_err(&me)?;
    }
    if !zenoh_cfg.connect.is_empty() {
        let json = serde_json::to_string(&zenoh_cfg.connect)?;
        zc.insert_json5("connect/endpoints", &json).map_err(&me)?;
    }
    if zenoh_cfg.timestamping_enabled {
        zc.insert_json5("timestamping/enabled", "true")
            .map_err(&me)?;
    }

    let session = zenoh::open(zc).await.map_err(&me)?;

    let codec = match config.codec {
        mitiflow_emulator::config::CodecConfig::Json => CodecFormat::Json,
        mitiflow_emulator::config::CodecConfig::Msgpack => CodecFormat::MsgPack,
        mitiflow_emulator::config::CodecConfig::Postcard => CodecFormat::Postcard,
    };

    let recovery_mode = match config.recovery_mode {
        RecoveryModeConfig::Heartbeat => RecoveryMode::Heartbeat,
        RecoveryModeConfig::PeriodicQuery => RecoveryMode::PeriodicQuery(Duration::from_secs(1)),
        RecoveryModeConfig::Both => RecoveryMode::Both,
    };

    let mut bus_builder = EventBusConfig::builder(&config.key_prefix)
        .codec(codec)
        .cache_size(config.cache_size)
        .heartbeat(HeartbeatMode::Sporadic(Duration::from_millis(
            config.heartbeat_ms,
        )))
        .recovery_mode(recovery_mode)
        .num_partitions(config.num_partitions);

    if let Some(urgency) = config.urgency_ms {
        bus_builder = bus_builder.durable_urgency(Duration::from_millis(urgency));
    }

    let bus_config = bus_builder.build()?;
    let payload_config = PayloadConfig::from(&config.payload);

    let manifest_writer = init_manifest_writer();

    tracing::info!(
        "Producer started: prefix={}, rate={:?}, ramp_up={:?}, burst={:?}",
        config.key_prefix,
        config.rate_per_instance.or(config.rate),
        config.ramp_up_sec,
        config.burst_factor,
    );

    let published = Arc::new(AtomicU64::new(0));

    let bench_config = lightbench::BenchmarkConfig {
        rate: if config.rate_per_instance.is_some() {
            None
        } else {
            config.rate
        },
        rate_per_worker: config.rate_per_instance,
        workers: 1,
        duration: 86400, // Effectively infinite; supervisor SIGTERM handles shutdown.
        ramp_up: config.ramp_up_sec.map(|s| s as u64),
        ramp_start_rate: config.ramp_start_rate.unwrap_or(0.0),
        burst_factor: config.burst_factor.unwrap_or(1.0),
        csv: None,
        no_progress: true,
        hide_ramp_progress: true,
        drain_timeout: 0,
    };

    let work = EmulatorPublishWork {
        session: session.clone(),
        bus_config,
        payload_config,
        durable: config.durable,
        published: Arc::clone(&published),
        manifest_writer: manifest_writer.clone(),
    };

    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        cancel_clone.cancel();
    });
    #[cfg(unix)]
    {
        let cancel_sig = cancel.clone();
        tokio::spawn(async move {
            let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to register SIGTERM handler");
            sig.recv().await;
            cancel_sig.cancel();
        });
    }

    // Rate reporter: logs publish rate (eps) every 2 seconds so we can observe
    // whether a slow consumer causes the publisher to stall.
    {
        let published_ref = Arc::clone(&published);
        let cancel_report = cancel.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(2));
            let mut last = 0u64;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let total = published_ref.load(Ordering::Relaxed);
                        let delta = total.saturating_sub(last);
                        let eps = delta / 2;
                        tracing::info!("Producer rate: {} eps (total: {})", eps, total);
                        last = total;
                    }
                    _ = cancel_report.cancelled() => break,
                }
            }
        });
    }

    tokio::select! {
        _ = Benchmark::from_config(bench_config).work(work).run() => {}
        _ = cancel.cancelled() => {}
    }

    if let Some(writer) = &manifest_writer {
        let mut w = writer.lock().unwrap();
        if let Err(e) = w.flush() {
            tracing::warn!("Failed to flush manifest: {}", e);
        }
    }

    tracing::info!("Producer shutting down");
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

fn init_manifest_writer() -> Option<Arc<std::sync::Mutex<ManifestWriter>>> {
    let dir = match std::env::var("MITIFLOW_MANIFEST_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => return None,
    };
    let component = std::env::var("MITIFLOW_COMPONENT_NAME").unwrap_or_else(|_| "producer".into());
    let instance: usize = std::env::var("MITIFLOW_INSTANCE_INDEX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    match ManifestWriter::new(&dir, &component, instance, ManifestRole::Published) {
        Ok(writer) => {
            tracing::info!("Manifest writing enabled: {}", writer.path().display());
            Some(Arc::new(std::sync::Mutex::new(writer)))
        }
        Err(e) => {
            tracing::warn!("Failed to create manifest writer: {}", e);
            None
        }
    }
}

fn write_manifest_entry(
    manifest_writer: &Option<Arc<std::sync::Mutex<ManifestWriter>>>,
    receipt: &mitiflow::publisher::PublishReceipt,
    checksum: u32,
) {
    let Some(writer) = manifest_writer else {
        return;
    };
    let entry = EventManifestEntry {
        event_id: receipt.event_id.to_string(),
        seq: receipt.seq,
        publisher_id: receipt.publisher_id.to_string(),
        partition: receipt.partition,
        timestamp: receipt.timestamp.to_rfc3339(),
        payload_checksum: checksum,
        role: ManifestRole::Published,
    };
    let mut w = writer.lock().unwrap();
    if let Err(e) = w.write_entry(&entry) {
        tracing::warn!("Manifest write failed: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manifest_writer_none_when_env_unset() {
        unsafe { std::env::remove_var("MITIFLOW_MANIFEST_DIR") };
        let result = init_manifest_writer();
        assert!(result.is_none());
    }

    #[test]
    fn manifest_writer_created_when_env_set() {
        let tempdir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("MITIFLOW_MANIFEST_DIR", tempdir.path());
            std::env::set_var("MITIFLOW_COMPONENT_NAME", "test-producer");
            std::env::set_var("MITIFLOW_INSTANCE_INDEX", "1");
        }

        let result = init_manifest_writer();
        assert!(result.is_some());

        let writer_arc = result.unwrap();
        let w = writer_arc.lock().unwrap();
        assert!(w.path().exists());
        assert!(
            w.path()
                .to_string_lossy()
                .contains("test-producer-1-published")
        );

        unsafe {
            std::env::remove_var("MITIFLOW_MANIFEST_DIR");
            std::env::remove_var("MITIFLOW_COMPONENT_NAME");
            std::env::remove_var("MITIFLOW_INSTANCE_INDEX");
        }
    }

    #[test]
    fn write_manifest_entry_noop_when_none() {
        let receipt = mitiflow::publisher::PublishReceipt {
            event_id: mitiflow::types::EventId::new(),
            seq: 1,
            publisher_id: mitiflow::types::PublisherId::new(),
            partition: 0,
            timestamp: chrono::Utc::now(),
        };
        write_manifest_entry(&None, &receipt, payload_checksum(b"hello"));
    }

    #[test]
    fn write_manifest_entry_writes_valid_jsonl() {
        let tempdir = tempfile::tempdir().unwrap();
        let writer =
            ManifestWriter::new(tempdir.path(), "test-producer", 0, ManifestRole::Published)
                .unwrap();
        let path = writer.path().to_path_buf();
        let manifest = Some(Arc::new(std::sync::Mutex::new(writer)));

        let receipt = mitiflow::publisher::PublishReceipt {
            event_id: mitiflow::types::EventId::new(),
            seq: 7,
            publisher_id: mitiflow::types::PublisherId::new(),
            partition: 2,
            timestamp: chrono::Utc::now(),
        };
        let payload = b"test-payload";

        write_manifest_entry(&manifest, &receipt, payload_checksum(payload));

        {
            let mut w = manifest.as_ref().unwrap().lock().unwrap();
            w.flush().unwrap();
        }

        let content = std::fs::read_to_string(&path).unwrap();
        let entry: EventManifestEntry = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(entry.seq, 7);
        assert_eq!(entry.partition, 2);
        assert_eq!(entry.role, ManifestRole::Published);
        assert_eq!(entry.payload_checksum, crc32fast::hash(b"test-payload"));
    }
}
