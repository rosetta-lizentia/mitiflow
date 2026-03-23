//! Orchestrator role binary — spawned by the supervisor.
//!
//! Starts a mitiflow-orchestrator that manages topic lifecycle, lag monitoring,
//! and cluster view.

use std::time::Duration;

use mitiflow_orchestrator::orchestrator::{Orchestrator, OrchestratorConfig};
use mitiflow_orchestrator::config::{RetentionPolicy, CompactionPolicy};
use mitiflow_emulator::role_config::{decode_config, OrchestratorRoleConfig, ZenohRoleConfig};

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
    let config: OrchestratorRoleConfig = decode_config(&config_b64)?;

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

    // Ensure data directory exists.
    tokio::fs::create_dir_all(&config.data_dir).await?;

    let orch_config = OrchestratorConfig {
        key_prefix: config.key_prefix.clone(),
        data_dir: config.data_dir.clone(),
        lag_interval: Duration::from_millis(config.lag_interval_ms),
        admin_prefix: None, // Use default.
        http_bind: None,
    };

    tracing::info!(
        "Orchestrator starting: prefix={}, data_dir={:?}",
        config.key_prefix,
        config.data_dir
    );

    let mut orchestrator = Orchestrator::new(&session, orch_config).map_err(&me)?;

    // Register topics.
    for topic in &config.topics {
        use mitiflow_orchestrator::TopicConfig;
        let topic_config = TopicConfig {
            name: topic.name.clone(),
            key_prefix: topic.key_prefix.clone(),
            num_partitions: topic.num_partitions,
            replication_factor: topic.replication_factor,
            retention: RetentionPolicy::default(),
            compaction: CompactionPolicy::default(),
            required_labels: std::collections::HashMap::new(),
            excluded_labels: std::collections::HashMap::new(),
        };
        if let Err(e) = orchestrator.create_topic(topic_config).await {
            tracing::warn!("Failed to create topic {}: {}", topic.name, e);
        }
    }

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
            let mut sig =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            sig.recv().await;
            cancel_sig.cancel();
        });
    }

    // Run orchestrator until shutdown.
    tokio::select! {
        result = orchestrator.run() => {
            if let Err(e) = result {
                tracing::error!("Orchestrator error: {}", e);
            }
        }
        _ = cancel.cancelled() => {}
    }

    tracing::info!("Orchestrator shutting down");
    orchestrator.shutdown().await;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
