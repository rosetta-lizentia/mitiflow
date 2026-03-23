//! mitiflow-orchestrator binary entry point.

use std::path::PathBuf;
use std::time::Duration;

use tracing_subscriber::EnvFilter;

use mitiflow_orchestrator::{Orchestrator, orchestrator::OrchestratorConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let key_prefix =
        std::env::var("MITIFLOW_KEY_PREFIX").unwrap_or_else(|_| "mitiflow".to_string());
    let data_dir = std::env::var("MITIFLOW_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./orchestrator_data"));
    let lag_interval_ms: u64 = std::env::var("MITIFLOW_LAG_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let session = zenoh::open(zenoh::Config::default()).await?;

    let config = OrchestratorConfig {
        key_prefix,
        data_dir,
        lag_interval: Duration::from_millis(lag_interval_ms),
        admin_prefix: None,
        http_bind: None,
    };

    let mut orchestrator = Orchestrator::new(&session, config)?;
    orchestrator.run().await?;

    tracing::info!("mitiflow-orchestrator running, press Ctrl+C to stop");

    // Wait forever (or until process is killed)
    tokio::select! {
        _ = std::future::pending::<()>() => {},
    }
    orchestrator.shutdown().await;

    Ok(())
}
