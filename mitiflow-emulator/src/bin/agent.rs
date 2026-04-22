//! Multi-topic storage agent role binary — spawned by the supervisor.
//!
//! Starts a mitiflow-storage `StorageAgent` in multi-topic mode using `AgentConfig`.

use mitiflow_emulator::role_config::{AgentRoleConfig, ZenohRoleConfig, decode_config};
use mitiflow_storage::{AgentConfig, StorageAgent, TopicEntry};

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
    let config: AgentRoleConfig = decode_config(&config_b64)?;

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

    // Ensure data directory exists.
    tokio::fs::create_dir_all(&config.data_dir).await?;

    // Build AgentConfig from the role config.
    let topics: Vec<TopicEntry> = config
        .topics
        .iter()
        .map(|t| TopicEntry {
            name: t.name.clone(),
            key_prefix: t.key_prefix.clone(),
            num_partitions: t.num_partitions,
            replication_factor: t.replication_factor,
        })
        .collect();

    let mut builder = AgentConfig::builder(config.data_dir.clone())
        .capacity(config.capacity)
        .labels(config.labels.clone())
        .global_prefix(&config.global_prefix)
        .auto_discover_topics(config.auto_discover_topics)
        .topics(topics);

    if let Some(node_id) = &config.node_id {
        builder = builder.node_id(node_id);
    }

    let agent_config = builder.build()?;

    let topic_names: Vec<&str> = config.topics.iter().map(|t| t.name.as_str()).collect();
    tracing::info!(
        "Agent starting: global_prefix={}, data_dir={:?}, topics=[{}], auto_discover={}",
        config.global_prefix,
        config.data_dir,
        topic_names.join(", "),
        config.auto_discover_topics,
    );

    let mut agent = StorageAgent::start_multi(&session, agent_config).await?;

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

    tracing::info!("Agent shutting down");
    let _ = agent.shutdown().await;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
