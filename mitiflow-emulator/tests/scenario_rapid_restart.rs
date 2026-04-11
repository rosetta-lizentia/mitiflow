use std::path::Path;
use std::time::Duration;

use mitiflow_emulator::config::{ChaosAction, TopologyConfig};
use mitiflow_emulator::validation::validate;

fn load_topology_13() -> TopologyConfig {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("topologies")
        .join("13_chaos_rapid_restart.yaml");
    TopologyConfig::from_file(&path)
        .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_topology_13() {
    let config = load_topology_13();
    let result = validate(&config)
        .unwrap_or_else(|e| panic!("validation failed for 13_chaos_rapid_restart.yaml: {e}"));

    assert!(
        result.warnings.is_empty(),
        "unexpected warnings: {:?}",
        result.warnings
    );
    assert!(config.chaos.enabled);
    assert_eq!(config.chaos.schedule.len(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_rapid_restart_config_valid() {
    let config = load_topology_13();

    assert!(config.chaos.enabled);
    assert_eq!(config.chaos.schedule.len(), 5);

    for (idx, event) in config.chaos.schedule.iter().enumerate() {
        let expected_at = Duration::from_secs(((idx + 1) * 3) as u64);
        assert_eq!(event.action, ChaosAction::Kill, "event {idx} must be kill");
        assert_eq!(
            event.target.as_deref(),
            Some("cons-1"),
            "event {idx} must target cons-1"
        );
        assert_eq!(
            event.restart_after,
            Some(Duration::from_secs(1)),
            "event {idx} must restart after 1s"
        );
        assert_eq!(
            event.at,
            Some(expected_at),
            "event {idx} at offset mismatch"
        );
    }
}
