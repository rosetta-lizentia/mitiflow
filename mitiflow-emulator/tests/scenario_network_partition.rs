use std::path::Path;
use std::time::Duration;

use mitiflow_emulator::config::{ChaosAction, TopologyConfig};
use mitiflow_emulator::validation::validate;

fn topology_12_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("topologies")
        .join("12_chaos_network_partition.yaml")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_topology_12() {
    let path = topology_12_path();
    let config = TopologyConfig::from_file(&path)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path.display(), e));

    validate(&config).unwrap_or_else(|e| panic!("validation failed for {}: {}", path.display(), e));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_network_partition_config_valid() {
    let path = topology_12_path();
    let config = TopologyConfig::from_file(&path)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path.display(), e));

    assert!(config.chaos.enabled, "chaos must be enabled");
    assert_eq!(
        config.chaos.schedule.len(),
        1,
        "expected exactly one chaos event"
    );

    let event = &config.chaos.schedule[0];
    assert_eq!(event.action, ChaosAction::NetworkPartition);
    assert_eq!(
        event.partition_from,
        vec!["orchestrator".to_string()],
        "partition_from should isolate from orchestrator"
    );
    assert_eq!(event.heal_after, Some(Duration::from_secs(10)));

    let validation = validate(&config);
    assert!(
        validation.is_ok(),
        "expected validation to pass, got: {:?}",
        validation
    );
}
