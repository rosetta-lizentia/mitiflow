//! Consumer role binary — spawned by the supervisor.
//!
//! Subscribes to a topic and counts/logs/writes received events.

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

    // Periodic reporter for count mode: shows per-window rate (eps) and running total.
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

    // Choose subscriber type based on consumer group config.
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

    let final_count = count.load(Ordering::Relaxed);
    tracing::info!("Consumer shutting down. Total events: {}", final_count);
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

fn handle_event(event: &mitiflow::event::RawEvent, mode: OutputMode) {
    match mode {
        OutputMode::Count => {} // Handled by periodic reporter.
        OutputMode::Log => {
            tracing::info!("Event seq={} id={}", event.seq, event.id,);
        }
        OutputMode::Discard => {}
        OutputMode::File => {
            // File writing handled separately if needed.
        }
    }
}
