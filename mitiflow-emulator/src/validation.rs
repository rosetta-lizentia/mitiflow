//! Topology validation — checks for structural correctness and consistency.

use std::collections::{HashMap, HashSet};

use crate::config::{
    ChaosAction, CodecConfig, ComponentDef, ComponentKind, DefaultsConfig, TopicDef, TopologyConfig,
};
use crate::error::{EmulatorError, ValidationWarning};

/// Result of topology validation.
#[derive(Debug)]
pub struct ValidationResult {
    pub warnings: Vec<ValidationWarning>,
}

/// Resolved configuration for a component after topic/defaults inheritance.
#[derive(Debug, Clone)]
pub struct ResolvedComponentConfig {
    pub key_prefix: String,
    pub codec: CodecConfig,
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub cache_size: usize,
    pub heartbeat_ms: u64,
}

/// Validate a topology configuration.
///
/// Returns warnings on success. Returns an error for structural problems
/// that would prevent the topology from running.
pub fn validate(config: &TopologyConfig) -> crate::error::Result<ValidationResult> {
    let mut warnings = Vec::new();

    if config.components.is_empty() {
        return Err(EmulatorError::Validation("components list is empty".into()));
    }

    // Build topic registry.
    let topic_registry = build_topic_registry(&config.topics)?;

    // Validate component names are unique.
    validate_unique_names(&config.components)?;

    // Validate topic references.
    validate_topic_references(&config.components, &topic_registry)?;

    // Validate pipeline DAG (no cycles).
    validate_no_cycles(&config.components, &topic_registry)?;

    // Check source coverage — every consumed topic must have a producer/processor.
    validate_source_coverage(&config.components, &topic_registry)?;

    // Check storage coverage — warn if a topic has no storage_agent.
    check_storage_coverage(&config.components, &config.topics, &mut warnings);

    // Validate chaos targets exist.
    if config.chaos.enabled {
        validate_chaos_targets(&config.chaos.schedule, &config.components)?;
    }

    Ok(ValidationResult { warnings })
}

/// Build a topic registry from the topic definitions.
fn build_topic_registry(topics: &[TopicDef]) -> crate::error::Result<HashMap<&str, &TopicDef>> {
    let mut registry = HashMap::new();
    for topic in topics {
        if registry.insert(topic.name.as_str(), topic).is_some() {
            return Err(EmulatorError::Validation(format!(
                "duplicate topic name: \"{}\"",
                topic.name
            )));
        }
    }
    Ok(registry)
}

/// Validate that component names are unique.
fn validate_unique_names(components: &[ComponentDef]) -> crate::error::Result<()> {
    let mut seen = HashSet::new();
    for comp in components {
        if !seen.insert(comp.name.as_str()) {
            return Err(EmulatorError::Validation(format!(
                "duplicate component name: \"{}\"",
                comp.name
            )));
        }
    }
    Ok(())
}

/// Validate that all topic references in components point to existing topics.
fn validate_topic_references(
    components: &[ComponentDef],
    registry: &HashMap<&str, &TopicDef>,
) -> crate::error::Result<()> {
    for comp in components {
        // Orchestrator doesn't reference a specific topic.
        if comp.kind == ComponentKind::Orchestrator {
            continue;
        }

        match comp.kind {
            ComponentKind::Processor => {
                let input = comp.input_topic.as_deref().ok_or_else(|| {
                    EmulatorError::Validation(format!(
                        "processor \"{}\" missing required field \"input_topic\"",
                        comp.name
                    ))
                })?;
                if !registry.contains_key(input) {
                    let available: Vec<&str> = registry.keys().copied().collect();
                    return Err(EmulatorError::Validation(format!(
                        "component \"{}\" references unknown topic \"{}\"\nAvailable topics: {}",
                        comp.name,
                        input,
                        available.join(", ")
                    )));
                }
                let output = comp.output_topic.as_deref().ok_or_else(|| {
                    EmulatorError::Validation(format!(
                        "processor \"{}\" missing required field \"output_topic\"",
                        comp.name
                    ))
                })?;
                if !registry.contains_key(output) {
                    let available: Vec<&str> = registry.keys().copied().collect();
                    return Err(EmulatorError::Validation(format!(
                        "component \"{}\" references unknown topic \"{}\"\nAvailable topics: {}",
                        comp.name,
                        output,
                        available.join(", ")
                    )));
                }
            }
            ComponentKind::Producer | ComponentKind::Consumer | ComponentKind::StorageAgent => {
                let topic = comp.topic.as_deref().ok_or_else(|| {
                    EmulatorError::Validation(format!(
                        "{:?} \"{}\" missing required field \"topic\"",
                        comp.kind, comp.name
                    ))
                })?;
                if !registry.contains_key(topic) {
                    let available: Vec<&str> = registry.keys().copied().collect();
                    return Err(EmulatorError::Validation(format!(
                        "component \"{}\" references unknown topic \"{}\"\nAvailable topics: {}",
                        comp.name,
                        topic,
                        available.join(", ")
                    )));
                }
            }
            ComponentKind::Agent => {
                if comp.managed_topics.is_empty() && !comp.auto_discover_topics.unwrap_or(false) {
                    return Err(EmulatorError::Validation(format!(
                        "agent \"{}\" must have at least one topic in \"topics\" or enable \"auto_discover_topics\"",
                        comp.name
                    )));
                }
                for topic_name in &comp.managed_topics {
                    if !registry.contains_key(topic_name.as_str()) {
                        let available: Vec<&str> = registry.keys().copied().collect();
                        return Err(EmulatorError::Validation(format!(
                            "agent \"{}\" references unknown topic \"{}\"\nAvailable topics: {}",
                            comp.name,
                            topic_name,
                            available.join(", ")
                        )));
                    }
                }
            }
            ComponentKind::Orchestrator => {}
        }
    }
    Ok(())
}

/// Validate the processor pipeline has no cycles.
///
/// Builds a DAG: topic → processor → output_topic and checks for cycles
/// via topological sort (Kahn's algorithm).
fn validate_no_cycles(
    components: &[ComponentDef],
    _registry: &HashMap<&str, &TopicDef>,
) -> crate::error::Result<()> {
    // Build adjacency: topic_name -> [topic_names] (through processors).
    let mut edges: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut all_nodes: HashSet<&str> = HashSet::new();

    for comp in components {
        if comp.kind == ComponentKind::Processor
            && let (Some(input), Some(output)) =
                (comp.input_topic.as_deref(), comp.output_topic.as_deref())
        {
            edges.entry(input).or_default().push(output);
            all_nodes.insert(input);
            all_nodes.insert(output);
        }
    }

    if all_nodes.is_empty() {
        return Ok(());
    }

    // Kahn's algorithm for cycle detection.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    for node in &all_nodes {
        in_degree.entry(node).or_insert(0);
    }
    for targets in edges.values() {
        for target in targets {
            *in_degree.entry(target).or_insert(0) += 1;
        }
    }

    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(node, _)| *node)
        .collect();

    let mut visited = 0;
    while let Some(node) = queue.pop() {
        visited += 1;
        if let Some(targets) = edges.get(node) {
            for &target in targets {
                let deg = in_degree.get_mut(target).expect(
                    "target node must exist in in_degree map; all nodes are inserted above",
                );
                *deg -= 1;
                if *deg == 0 {
                    queue.push(target);
                }
            }
        }
    }

    if visited != all_nodes.len() {
        // Find a cycle for error reporting.
        let cycle_nodes: Vec<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg > 0)
            .map(|(node, _)| *node)
            .collect();
        return Err(EmulatorError::Validation(format!(
            "cycle detected in processor pipeline involving topics: {}",
            cycle_nodes.join(" → ")
        )));
    }

    Ok(())
}

/// Validate that every consumed topic has at least one upstream source.
fn validate_source_coverage(
    components: &[ComponentDef],
    _registry: &HashMap<&str, &TopicDef>,
) -> crate::error::Result<()> {
    // Topics that have a producer or processor output.
    let mut produced: HashSet<&str> = HashSet::new();
    for comp in components {
        match comp.kind {
            ComponentKind::Producer => {
                if let Some(t) = comp.topic.as_deref() {
                    produced.insert(t);
                }
            }
            ComponentKind::Processor => {
                if let Some(t) = comp.output_topic.as_deref() {
                    produced.insert(t);
                }
            }
            _ => {}
        }
    }

    // Topics that are consumed (processor input, consumer topic).
    for comp in components {
        match comp.kind {
            ComponentKind::Consumer => {
                if let Some(t) = comp.topic.as_deref()
                    && !produced.contains(t)
                {
                    return Err(EmulatorError::Validation(format!(
                        "consumer \"{}\" subscribes to topic \"{}\" which has no producer or processor writing to it",
                        comp.name, t
                    )));
                }
            }
            ComponentKind::Processor => {
                if let Some(t) = comp.input_topic.as_deref()
                    && !produced.contains(t)
                {
                    return Err(EmulatorError::Validation(format!(
                        "processor \"{}\" subscribes to topic \"{}\" which has no producer or processor writing to it",
                        comp.name, t
                    )));
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Warn about topics with no storage_agent or agent component.
fn check_storage_coverage(
    components: &[ComponentDef],
    topics: &[TopicDef],
    warnings: &mut Vec<ValidationWarning>,
) {
    let mut stored: HashSet<&str> = components
        .iter()
        .filter(|c| c.kind == ComponentKind::StorageAgent)
        .filter_map(|c| c.topic.as_deref())
        .collect();

    // Include topics served by multi-topic agent components.
    for comp in components.iter().filter(|c| c.kind == ComponentKind::Agent) {
        for t in &comp.managed_topics {
            stored.insert(t.as_str());
        }
    }

    for topic in topics {
        if !stored.contains(topic.name.as_str()) {
            warnings.push(ValidationWarning {
                message: format!(
                    "topic \"{}\" has no storage_agent — events will not be persisted",
                    topic.name
                ),
            });
        }
    }
}

/// Validate chaos event targets reference existing components.
fn validate_chaos_targets(
    schedule: &[crate::config::ChaosEventDef],
    components: &[ComponentDef],
) -> crate::error::Result<()> {
    let names: HashSet<&str> = components.iter().map(|c| c.name.as_str()).collect();

    for event in schedule {
        if event.action == ChaosAction::NetworkPartition && event.partition_from.is_empty() {
            return Err(EmulatorError::Validation(
                "chaos event action network_partition requires non-empty partition_from".into(),
            ));
        }
        if let Some(target) = &event.target
            && !names.contains(target.as_str())
        {
            return Err(EmulatorError::Validation(format!(
                "chaos event targets unknown component: \"{}\"",
                target
            )));
        }
        for pool_target in &event.pool {
            if !names.contains(pool_target.as_str()) {
                return Err(EmulatorError::Validation(format!(
                    "chaos event pool contains unknown component: \"{}\"",
                    pool_target
                )));
            }
        }
    }
    Ok(())
}

/// Resolve a component's effective configuration by inheriting from topic and defaults.
///
/// Priority: component field → topic definition → defaults section → hardcoded default.
pub fn resolve_component_config(
    component: &ComponentDef,
    topic: Option<&TopicDef>,
    defaults: &DefaultsConfig,
) -> ResolvedComponentConfig {
    let key_prefix = topic.map(|t| t.key_prefix.clone()).unwrap_or_default();

    let codec = component
        .codec
        .or_else(|| topic.and_then(|t| t.codec))
        .unwrap_or(defaults.codec);

    let num_partitions = topic.map(|t| t.num_partitions).unwrap_or(16);

    let replication_factor = topic.map(|t| t.replication_factor).unwrap_or(1);

    let cache_size = component.cache_size.unwrap_or(defaults.cache_size);

    let heartbeat_ms = component.heartbeat_ms.unwrap_or(defaults.heartbeat_ms);

    ResolvedComponentConfig {
        key_prefix,
        codec,
        num_partitions,
        replication_factor,
        cache_size,
        heartbeat_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TopologyConfig;

    #[test]
    fn network_partition_empty_partition_from_is_rejected() {
        let yaml = r#"
components:
  - name: prod-1
    kind: producer
    topic: events
  - name: cons-1
    kind: consumer
    topic: events
topics:
  - name: events
    key_prefix: demo/events
chaos:
  enabled: true
  schedule:
    - at: 5s
      action: network_partition
      target: prod-1
      partition_from: []
"#;
        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        let result = validate(&config);
        assert!(
            result.is_err(),
            "expected validation error for network_partition with empty partition_from"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("partition_from"),
            "error message should mention partition_from, got: {err}"
        );
    }

    #[test]
    fn network_partition_with_partition_from_passes() {
        let yaml = r#"
components:
  - name: prod-1
    kind: producer
    topic: events
  - name: cons-1
    kind: consumer
    topic: events
topics:
  - name: events
    key_prefix: demo/events
chaos:
  enabled: true
  schedule:
    - at: 5s
      action: network_partition
      target: prod-1
      partition_from: [cons-1]
      heal_after: 3s
"#;
        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        let result = validate(&config);
        assert!(
            result.is_ok(),
            "expected validation to pass with non-empty partition_from, got: {:?}",
            result
        );
    }
}
