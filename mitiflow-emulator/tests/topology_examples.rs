//! Tests that validate example topology YAML files.

use mitiflow_emulator::config::TopologyConfig;
use mitiflow_emulator::validation::validate;
use std::path::Path;

fn load_and_validate(name: &str) -> mitiflow_emulator::ValidationResult {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("topologies")
        .join(name);
    let config = TopologyConfig::from_file(&path)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", name, e));
    validate(&config).unwrap_or_else(|e| panic!("validation failed for {}: {}", name, e))
}

#[test]
fn validate_01_minimal() {
    let result = load_and_validate("01_minimal.yaml");
    // No storage agent → expect a warning.
    assert!(
        result.warnings.iter().any(|w| w.message.contains("no storage_agent")),
        "expected storage warning"
    );
}

#[test]
fn validate_02_durable() {
    let result = load_and_validate("02_durable.yaml");
    assert!(result.warnings.is_empty(), "unexpected warnings: {:?}", result.warnings);
}

#[test]
fn validate_03_fanout() {
    let result = load_and_validate("03_fanout.yaml");
    assert!(result.warnings.is_empty(), "unexpected warnings: {:?}", result.warnings);
}

#[test]
fn validate_04_multi_stage() {
    let result = load_and_validate("04_multi_stage.yaml");
    // filtered and enriched have no storage agents.
    let storage_warnings: Vec<_> = result
        .warnings
        .iter()
        .filter(|w| w.message.contains("no storage_agent"))
        .collect();
    assert_eq!(storage_warnings.len(), 2, "expect warnings for filtered and enriched topics");
}

#[test]
fn validate_05_stress() {
    let result = load_and_validate("05_stress.yaml");
    assert!(result.warnings.is_empty(), "unexpected warnings: {:?}", result.warnings);
}

#[test]
fn validate_06_chaos() {
    let result = load_and_validate("06_chaos.yaml");
    assert!(result.warnings.is_empty(), "unexpected warnings: {:?}", result.warnings);
}
