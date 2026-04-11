use std::path::Path;
use std::time::Duration;

use mitiflow_emulator::config::ChaosAction;
use mitiflow_emulator::{TopologyConfig, validate};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_topology_11() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("topologies")
        .join("11_chaos_storage_kill.yaml");

    let yaml = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let config = TopologyConfig::from_yaml(&yaml)
        .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()));

    validate(&config).unwrap_or_else(|e| panic!("validation failed for {}: {e}", path.display()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_storage_kill_config_valid() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("topologies")
        .join("11_chaos_storage_kill.yaml");
    let config = TopologyConfig::from_file(&path)
        .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()));

    let validation = validate(&config)
        .unwrap_or_else(|e| panic!("validation failed for {}: {e}", path.display()));
    assert!(
        validation.warnings.is_empty(),
        "unexpected warnings: {:?}",
        validation.warnings
    );

    assert!(config.chaos.enabled, "chaos must be enabled");
    assert_eq!(config.chaos.schedule.len(), 2, "expected 2 chaos events");

    for event in &config.chaos.schedule {
        assert_eq!(event.target.as_deref(), Some("storage-1"));
        assert_eq!(event.action, ChaosAction::Kill);
        assert_eq!(event.restart_after, Some(Duration::from_secs(3)));
    }
}
