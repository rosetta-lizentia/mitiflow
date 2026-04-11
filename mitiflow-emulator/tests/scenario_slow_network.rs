use std::path::Path;
use std::time::Duration;

use mitiflow_emulator::config::{ChaosAction, TopologyConfig};
use mitiflow_emulator::validation::validate;

fn topology_14_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("topologies")
        .join("14_chaos_slow_network.yaml")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_topology_14() {
    let path = topology_14_path();
    let config = TopologyConfig::from_file(&path)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path.display(), e));

    validate(&config).unwrap_or_else(|e| panic!("validation failed for {}: {}", path.display(), e));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scenario_slow_network_config_valid() {
    let path = topology_14_path();
    let config = TopologyConfig::from_file(&path)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path.display(), e));

    assert!(config.chaos.enabled, "chaos must be enabled");
    assert_eq!(
        config.chaos.schedule.len(),
        2,
        "expected exactly two chaos events"
    );

    let first = &config.chaos.schedule[0];
    assert_eq!(first.action, ChaosAction::Slow, "first event must be slow");
    assert_eq!(first.target.as_deref(), Some("cons-1"));
    assert_eq!(first.delay_ms, Some(200));
    assert_eq!(first.loss_percent, Some(5.0));
    assert_eq!(first.duration, Some(Duration::from_secs(10)));

    let second = &config.chaos.schedule[1];
    assert_eq!(
        second.action,
        ChaosAction::Slow,
        "second event must be slow"
    );
    assert_eq!(second.target.as_deref(), Some("cons-1"));
    assert_eq!(second.delay_ms, Some(500));
    assert_eq!(second.loss_percent, Some(10.0));
    assert_eq!(second.duration, Some(Duration::from_secs(5)));

    let validation = validate(&config);
    assert!(
        validation.is_ok(),
        "expected validation to pass, got: {:?}",
        validation
    );
}
