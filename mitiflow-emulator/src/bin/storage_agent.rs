//! Storage agent role binary — spawned by the supervisor.
//!
//! Starts a mitiflow-storage StorageAgent that handles event persistence.

use std::time::Duration;

use mitiflow::codec::CodecFormat;
use mitiflow::config::{EventBusConfig, HeartbeatMode, RecoveryMode};
use mitiflow_emulator::config::RecoveryModeConfig;
use mitiflow_emulator::role_config::{StorageAgentRoleConfig, ZenohRoleConfig, decode_config};
use mitiflow_storage::StorageAgent;
use mitiflow_storage::StorageAgentConfig;

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
    let config: StorageAgentRoleConfig = decode_config(&config_b64)?;

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
        zc.insert_json5("timestamping/enabled", "true")
            .map_err(&me)?;
    }

    let session = zenoh::open(zc).await.map_err(&me)?;

    // Build bus config for the storage agent.
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

    let bus_config = EventBusConfig::builder(&config.key_prefix)
        .codec(codec)
        .cache_size(config.cache_size)
        .heartbeat(HeartbeatMode::Sporadic(Duration::from_millis(
            config.heartbeat_ms,
        )))
        .recovery_mode(recovery_mode)
        .num_partitions(config.num_partitions)
        .build()?;

    // Ensure data directory exists.
    tokio::fs::create_dir_all(&config.data_dir).await?;

    // Build storage agent config.
    let mut agent_config_builder = StorageAgentConfig::builder(config.data_dir.clone(), bus_config);

    if let Some(node_id) = &config.node_id {
        agent_config_builder = agent_config_builder.node_id(node_id);
    }

    let agent_config = agent_config_builder
        .capacity(config.capacity)
        .num_partitions(config.num_partitions)
        .replication_factor(config.replication_factor)
        .build()?;

    tracing::info!(
        "Storage agent starting: prefix={}, data_dir={:?}",
        config.key_prefix,
        config.data_dir
    );

    let mut agent = StorageAgent::start(&session, agent_config).await?;

    // Wait for shutdown signal.
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

    cancel.cancelled().await;

    tracing::info!("Storage agent shutting down");
    let _ = agent.shutdown().await;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
