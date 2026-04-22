//! Consumer role binary — spawned by the supervisor.
//!
//! Subscribes to a topic and counts/logs/writes received events.
//! When `MITIFLOW_MANIFEST_DIR` is set, writes JSONL manifest entries for
//! each received event.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mitiflow::codec::CodecFormat;
use mitiflow::config::{
    CommitMode, ConsumerGroupConfig, EventBusConfig, HeartbeatMode, OffloadConfig, OffsetReset,
    RecoveryMode,
};
use mitiflow::subscriber::EventSubscriber;
use mitiflow::subscriber::consumer_group::ConsumerGroupSubscriber;
use mitiflow_emulator::config::{OutputMode, RecoveryModeConfig};
use mitiflow_emulator::metrics::{
    EventManifestEntry, ManifestRole, ManifestWriter, payload_checksum,
};
use mitiflow_emulator::role_config::{ConsumerRoleConfig, ZenohRoleConfig, decode_config};

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
    let config: ConsumerRoleConfig = decode_config(&config_b64)?;

    // Build zenoh config.
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
        zc.insert_json5("timestamping/enabled", "true").map_err(&me)?;
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

    if config.num_processing_shards > 1 {
        bus_builder = bus_builder.num_processing_shards(config.num_processing_shards);
    }

    if config.offload_enabled {
        bus_builder = bus_builder.offload(OffloadConfig {
            enabled: true,
            ..OffloadConfig::default()
        });
    }

    let bus_config = bus_builder.build()?;

    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();

    // Handle SIGTERM / Ctrl-C.
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

    let count = Arc::new(AtomicU64::new(0));

    let manifest_writer = init_manifest_writer();

    let interval_sec = config.output.report_interval_sec;
    let output_mode = config.output.mode;
    if output_mode == OutputMode::Count {
        let count_ref = Arc::clone(&count);
        let cancel_report = cancel.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_sec));
            let mut last = 0u64;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let total = count_ref.load(Ordering::Relaxed);
                        let delta = total.saturating_sub(last);
                        let eps = delta / interval_sec.max(1);
                        tracing::info!("Consumer rate: {} eps (total: {})", eps, total);
                        last = total;
                    }
                    _ = cancel_report.cancelled() => break,
                }
            }
        });
    }

    if let Some(cg) = &config.consumer_group {
        let group_config = ConsumerGroupConfig {
            group_id: cg.group_id.clone(),
            member_id: uuid::Uuid::now_v7().to_string(),
            commit_mode: match cg.commit_mode {
                mitiflow_emulator::config::CommitModeDef::Auto => CommitMode::Auto {
                    interval: Duration::from_millis(cg.auto_commit_interval_ms),
                },
                mitiflow_emulator::config::CommitModeDef::Manual => CommitMode::Manual,
            },
            offset_reset: OffsetReset::Latest,
        };

        let subscriber = ConsumerGroupSubscriber::new(&session, bus_config, group_config).await?;

        tracing::info!(
            "Consumer (group={}) started: prefix={}",
            cg.group_id,
            config.key_prefix
        );

        loop {
            tokio::select! {
                result = subscriber.recv_raw() => {
                    match result {
                        Ok(event) => {
                            count.fetch_add(1, Ordering::Relaxed);
                            write_manifest_entry(&manifest_writer, &event);
                            handle_event(&event, output_mode);
                            if let Some(delay_ms) = config.processing_delay_ms {
                                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Consumer recv error: {}", e);
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => break,
            }
        }
    } else {
        let subscriber = EventSubscriber::new(&session, bus_config).await?;

        tracing::info!("Consumer started: prefix={}", config.key_prefix);

        loop {
            tokio::select! {
                result = subscriber.recv_raw() => {
                    match result {
                        Ok(event) => {
                            count.fetch_add(1, Ordering::Relaxed);
                            write_manifest_entry(&manifest_writer, &event);
                            handle_event(&event, output_mode);
                            if let Some(delay_ms) = config.processing_delay_ms {
                                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Consumer recv error: {}", e);
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => break,
            }
        }

        subscriber.shutdown().await;
    }

    if let Some(writer) = &manifest_writer {
        let mut w = writer.lock().unwrap();
        if let Err(e) = w.flush() {
            tracing::warn!("Failed to flush manifest: {}", e);
        }
    }

    let final_count = count.load(Ordering::Relaxed);
    tracing::info!("Consumer shutting down. Total events: {}", final_count);
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

fn init_manifest_writer() -> Option<std::sync::Mutex<ManifestWriter>> {
    let dir = match std::env::var("MITIFLOW_MANIFEST_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => return None,
    };
    let component = std::env::var("MITIFLOW_COMPONENT_NAME").unwrap_or_else(|_| "consumer".into());
    let instance: usize = std::env::var("MITIFLOW_INSTANCE_INDEX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    match ManifestWriter::new(&dir, &component, instance, ManifestRole::Received) {
        Ok(writer) => {
            tracing::info!("Manifest writing enabled: {}", writer.path().display());
            Some(std::sync::Mutex::new(writer))
        }
        Err(e) => {
            tracing::warn!("Failed to create manifest writer: {}", e);
            None
        }
    }
}

fn extract_partition(key_expr: &str) -> u32 {
    let mut segments = key_expr.split('/');
    while let Some(seg) = segments.next() {
        if seg == "p"
            && let Some(next) = segments.next()
            && let Ok(p) = next.parse::<u32>()
        {
            return p;
        }
    }
    0
}

fn write_manifest_entry(
    manifest_writer: &Option<std::sync::Mutex<ManifestWriter>>,
    event: &mitiflow::event::RawEvent,
) {
    let Some(writer) = manifest_writer else {
        return;
    };
    let entry = EventManifestEntry {
        event_id: event.id.to_string(),
        seq: event.seq,
        publisher_id: event.publisher_id.to_string(),
        partition: extract_partition(&event.key_expr),
        timestamp: event.timestamp.to_rfc3339(),
        payload_checksum: payload_checksum(&event.payload),
        role: ManifestRole::Received,
    };
    let mut w = writer.lock().unwrap();
    if let Err(e) = w.write_entry(&entry) {
        tracing::warn!("Manifest write failed: {}", e);
    }
}

fn handle_event(event: &mitiflow::event::RawEvent, mode: OutputMode) {
    match mode {
        OutputMode::Count => {}
        OutputMode::Log => {
            tracing::info!("Event seq={} id={}", event.seq, event.id,);
        }
        OutputMode::Discard => {}
        OutputMode::File => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_partition_from_keyed_path() {
        assert_eq!(extract_partition("app/p/3/k/order-123/5"), 3);
        assert_eq!(extract_partition("my/p/0/42"), 0);
        assert_eq!(extract_partition("prefix/p/15/k/user/1/data/99"), 15);
    }

    #[test]
    fn extract_partition_no_p_segment() {
        assert_eq!(extract_partition("app/events/42"), 0);
        assert_eq!(extract_partition("simple/key"), 0);
    }

    #[test]
    fn extract_partition_p_not_followed_by_number() {
        assert_eq!(extract_partition("app/p/abc/42"), 0);
    }

    #[test]
    fn manifest_writer_none_when_env_unset() {
        unsafe { std::env::remove_var("MITIFLOW_MANIFEST_DIR") };
        let result = init_manifest_writer();
        assert!(result.is_none());
    }

    #[test]
    fn manifest_writer_created_when_env_set() {
        let tempdir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("MITIFLOW_MANIFEST_DIR", tempdir.path());
            std::env::set_var("MITIFLOW_COMPONENT_NAME", "test-consumer");
            std::env::set_var("MITIFLOW_INSTANCE_INDEX", "3");
        }

        let result = init_manifest_writer();
        assert!(result.is_some());

        let writer = result.unwrap();
        let w = writer.lock().unwrap();
        assert!(w.path().exists());
        assert!(
            w.path()
                .to_string_lossy()
                .contains("test-consumer-3-received")
        );

        unsafe {
            std::env::remove_var("MITIFLOW_MANIFEST_DIR");
            std::env::remove_var("MITIFLOW_COMPONENT_NAME");
            std::env::remove_var("MITIFLOW_INSTANCE_INDEX");
        }
    }

    #[test]
    fn write_manifest_entry_noop_when_none() {
        let event = mitiflow::event::RawEvent {
            id: mitiflow::types::EventId::new(),
            seq: 1,
            publisher_id: mitiflow::types::PublisherId::new(),
            key_expr: "test/p/0/1".into(),
            payload: b"hello".to_vec(),
            timestamp: chrono::Utc::now(),
        };
        write_manifest_entry(&None, &event);
    }

    #[test]
    fn write_manifest_entry_writes_valid_jsonl() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer =
            ManifestWriter::new(tempdir.path(), "test", 0, ManifestRole::Received).unwrap();
        let path = writer.path().to_path_buf();
        writer.flush().unwrap();
        drop(writer);

        let writer =
            ManifestWriter::new(tempdir.path(), "test", 0, ManifestRole::Received).unwrap();
        let manifest = Some(std::sync::Mutex::new(writer));

        let event = mitiflow::event::RawEvent {
            id: mitiflow::types::EventId::new(),
            seq: 42,
            publisher_id: mitiflow::types::PublisherId::new(),
            key_expr: "app/p/3/k/order-123/5".into(),
            payload: b"test-payload".to_vec(),
            timestamp: chrono::Utc::now(),
        };

        write_manifest_entry(&manifest, &event);

        {
            let mut w = manifest.as_ref().unwrap().lock().unwrap();
            w.flush().unwrap();
        }

        let content = std::fs::read_to_string(&path).unwrap();
        let entry: EventManifestEntry = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(entry.seq, 42);
        assert_eq!(entry.partition, 3);
        assert_eq!(entry.role, ManifestRole::Received);
        assert_eq!(entry.payload_checksum, crc32fast::hash(b"test-payload"));
    }
}
