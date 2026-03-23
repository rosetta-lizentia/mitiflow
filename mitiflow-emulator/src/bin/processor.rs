//! Processor role binary — subscribes to input, transforms, publishes to output.

use std::time::Duration;

use mitiflow::config::{
    EventBusConfig, HeartbeatMode, RecoveryMode,
};
use mitiflow::codec::CodecFormat;
use mitiflow::publisher::EventPublisher;
use mitiflow::subscriber::EventSubscriber;
use mitiflow_emulator::config::{ProcessingMode, RecoveryModeConfig};
use mitiflow_emulator::role_config::{decode_config, ProcessorRoleConfig, ZenohRoleConfig};

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
    let config: ProcessorRoleConfig = decode_config(&config_b64)?;

    // Build zenoh config.
    let mut zc = zenoh::Config::default();
    let me = |e: Box<dyn std::error::Error + Send + Sync>| anyhow::anyhow!("{e}");
    match zenoh_cfg.mode.as_str() {
        "client" => { zc.insert_json5("mode", r#""client""#).map_err(&me)?; }
        "router" => { zc.insert_json5("mode", r#""router""#).map_err(&me)?; }
        _ => { zc.insert_json5("mode", r#""peer""#).map_err(&me)?; }
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
        RecoveryModeConfig::PeriodicQuery => {
            RecoveryMode::PeriodicQuery(Duration::from_secs(1))
        }
        RecoveryModeConfig::Both => RecoveryMode::Both,
    };

    // Input subscriber config.
    let input_config = EventBusConfig::builder(&config.input_key_prefix)
        .codec(codec)
        .cache_size(config.cache_size)
        .heartbeat(HeartbeatMode::Sporadic(Duration::from_millis(
            config.heartbeat_ms,
        )))
        .recovery_mode(recovery_mode.clone())
        .num_partitions(config.num_partitions)
        .build()?;

    // Output publisher config.
    let output_config = EventBusConfig::builder(&config.output_key_prefix)
        .codec(codec)
        .cache_size(config.cache_size)
        .heartbeat(HeartbeatMode::Sporadic(Duration::from_millis(
            config.heartbeat_ms,
        )))
        .recovery_mode(recovery_mode)
        .num_partitions(config.num_partitions)
        .build()?;

    let publisher = EventPublisher::new(&session, output_config).await?;

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
            let mut sig =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            sig.recv().await;
            cancel_sig.cancel();
        });
    }

    tracing::info!(
        "Processor started: {} -> {} (mode={:?})",
        config.input_key_prefix,
        config.output_key_prefix,
        config.processing_mode
    );

    let subscriber = EventSubscriber::new(&session, input_config).await?;

    let mut processed = 0u64;
    let mut rng = rand::thread_rng();

    loop {
        tokio::select! {
            result = subscriber.recv_raw() => {
                match result {
                    Ok(event) => {
                        match config.processing_mode {
                            ProcessingMode::Passthrough => {
                                let _ = publisher.publish_bytes(event.payload.clone()).await;
                            }
                            ProcessingMode::Delay => {
                                if let Some(ms) = config.delay_ms {
                                    tokio::time::sleep(Duration::from_millis(ms)).await;
                                }
                                let _ = publisher.publish_bytes(event.payload.clone()).await;
                            }
                            ProcessingMode::Filter => {
                                let drop_prob = config.drop_probability.unwrap_or(0.0);
                                use rand::Rng;
                                if !rng.gen_bool(drop_prob.min(1.0)) {
                                    let _ = publisher.publish_bytes(event.payload.clone()).await;
                                }
                            }
                            ProcessingMode::Map => {
                                // Map mode: wrap in {"processed": true, "original": ...}
                                if let Ok(original) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
                                    let mapped = serde_json::json!({
                                        "processed": true,
                                        "original": original,
                                    });
                                    let bytes = serde_json::to_vec(&mapped).unwrap_or_default();
                                    let _ = publisher.publish_bytes(bytes).await;
                                } else {
                                    let _ = publisher.publish_bytes(event.payload.clone()).await;
                                }
                            }
                        }
                        processed += 1;
                        if processed % 1000 == 0 {
                            tracing::info!("Processor: {} events processed", processed);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Processor recv error: {}", e);
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }

    tracing::info!("Processor shutting down. {} events processed.", processed);
    drop(publisher);
    subscriber.shutdown().await;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
