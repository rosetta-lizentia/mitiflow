use std::path::PathBuf;

use mitiflow::EventBusConfig;
use mitiflow_agent::{StorageAgent, StorageAgentConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let key_prefix = std::env::var("MITIFLOW_KEY_PREFIX").unwrap_or_else(|_| "mitiflow".into());
    let data_dir = std::env::var("MITIFLOW_DATA_DIR").unwrap_or_else(|_| "/tmp/mitiflow-agent".into());
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

    let session = zenoh::open(zenoh::Config::default()).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let agent = StorageAgent::start(&session, config).await?;

    // Wait for Ctrl+C.
    tokio::signal::ctrl_c().await?;

    agent.shutdown().await?;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}
