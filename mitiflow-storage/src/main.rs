use std::path::PathBuf;

use clap::Parser;
use mitiflow::EventBusConfig;
use mitiflow_storage::{AgentYamlConfig, StorageAgent, StorageAgentConfig};
use tracing_subscriber::EnvFilter;

#[cfg(unix)]
async fn wait_for_sigterm() {
    use tokio::signal::unix::{SignalKind, signal};
    signal(SignalKind::terminate())
        .expect("failed to install SIGTERM handler")
        .recv()
        .await;
}

#[cfg(not(unix))]
async fn wait_for_sigterm() {
    std::future::pending::<()>().await;
}

/// Mitiflow storage agent — manages EventStore partitions via distributed assignment.
#[derive(Parser)]
#[command(name = "mitiflow-storage")]
struct Cli {
    /// Path to YAML configuration file.
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let session = zenoh::open(zenoh::Config::default())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut agent = if let Some(config_path) = cli.config {
        // YAML config mode
        let yaml_config = AgentYamlConfig::from_file(&config_path)?;
        let agent_config = yaml_config.into_agent_config()?;
        StorageAgent::start_multi(&session, agent_config).await?
    } else {
        // Legacy env-var mode
        let key_prefix = std::env::var("MITIFLOW_KEY_PREFIX").unwrap_or_else(|_| "mitiflow".into());
        let data_dir =
            std::env::var("MITIFLOW_DATA_DIR").unwrap_or_else(|_| "/tmp/mitiflow-storage".into());
        let node_id = std::env::var("MITIFLOW_NODE_ID").ok();
        let num_partitions: u32 = std::env::var("MITIFLOW_NUM_PARTITIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);
        let replication_factor: u32 = std::env::var("MITIFLOW_REPLICATION_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let capacity: u32 = std::env::var("MITIFLOW_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let bus_config = EventBusConfig::builder(&key_prefix).build()?;

        let mut builder = StorageAgentConfig::builder(PathBuf::from(&data_dir), bus_config)
            .capacity(capacity)
            .num_partitions(num_partitions)
            .replication_factor(replication_factor);

        if let Some(id) = node_id {
            builder = builder.node_id(id);
        }

        let config = builder.build()?;
        StorageAgent::start(&session, config).await?
    };

    // Wait for SIGINT (Ctrl+C) or SIGTERM (container stop)
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = wait_for_sigterm() => {},
    }

    tracing::info!("shutting down agent...");
    agent.shutdown().await?;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}
