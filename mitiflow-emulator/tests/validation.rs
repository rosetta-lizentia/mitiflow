//! Tests for topology validation logic.

use mitiflow_emulator::config::TopologyConfig;
use mitiflow_emulator::validation::{resolve_component_config, validate};

fn parse(yaml: &str) -> TopologyConfig {
    TopologyConfig::from_yaml(yaml).expect("yaml parse")
}

// ---------------------------------------------------------------------------
// Valid topologies
// ---------------------------------------------------------------------------

#[test]
fn valid_minimal_pubsub() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: demo/events

components:
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events
"#,
    );
    let result = validate(&config).unwrap();
    // Should warn about no storage agent.
    assert!(
        result
            .warnings
            .iter()
            .any(|w| w.message.contains("no storage_agent"))
    );
}

#[test]
fn valid_with_storage() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: demo/events

components:
  - name: store
    kind: storage_agent
    topic: events
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events
"#,
    );
    let result = validate(&config).unwrap();
    assert!(result.warnings.is_empty());
}

#[test]
fn valid_pipeline_with_processor() {
    let config = parse(
        r#"
topics:
  - name: raw
    key_prefix: test/raw
  - name: enriched
    key_prefix: test/enriched

components:
  - name: prod
    kind: producer
    topic: raw
  - name: proc
    kind: processor
    input_topic: raw
    output_topic: enriched
  - name: cons
    kind: consumer
    topic: enriched
"#,
    );
    validate(&config).unwrap();
}

#[test]
fn valid_multi_stage_pipeline() {
    let config = parse(
        r#"
topics:
  - name: raw
    key_prefix: test/raw
  - name: stage1
    key_prefix: test/s1
  - name: stage2
    key_prefix: test/s2

components:
  - name: prod
    kind: producer
    topic: raw
  - name: proc1
    kind: processor
    input_topic: raw
    output_topic: stage1
  - name: proc2
    kind: processor
    input_topic: stage1
    output_topic: stage2
  - name: cons
    kind: consumer
    topic: stage2
"#,
    );
    validate(&config).unwrap();
}

#[test]
fn valid_with_orchestrator() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: demo/events

components:
  - name: orch
    kind: orchestrator
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events
"#,
    );
    validate(&config).unwrap();
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[test]
fn error_empty_components() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components: []
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("empty"), "got: {msg}");
}

#[test]
fn error_duplicate_topic_names() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events1
  - name: events
    key_prefix: test/events2

components:
  - name: prod
    kind: producer
    topic: events
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("duplicate topic"), "got: {msg}");
}

#[test]
fn error_duplicate_component_names() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
  - name: prod
    kind: consumer
    topic: events
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("duplicate component"), "got: {msg}");
}

#[test]
fn error_unknown_topic_reference() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: nonexistent
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown topic"), "got: {msg}");
    assert!(msg.contains("nonexistent"), "got: {msg}");
}

#[test]
fn error_processor_missing_input_topic() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
  - name: proc
    kind: processor
    output_topic: events
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("input_topic"), "got: {msg}");
}

#[test]
fn error_processor_missing_output_topic() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
  - name: proc
    kind: processor
    input_topic: events
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("output_topic"), "got: {msg}");
}

#[test]
fn error_consumer_no_upstream() {
    let config = parse(
        r#"
topics:
  - name: orphan
    key_prefix: test/orphan

components:
  - name: cons
    kind: consumer
    topic: orphan
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("no producer"), "got: {msg}");
}

#[test]
fn error_processor_cycle() {
    let config = parse(
        r#"
topics:
  - name: a
    key_prefix: test/a
  - name: b
    key_prefix: test/b

components:
  - name: prod
    kind: producer
    topic: a
  - name: proc1
    kind: processor
    input_topic: a
    output_topic: b
  - name: proc2
    kind: processor
    input_topic: b
    output_topic: a
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("cycle"), "got: {msg}");
}

#[test]
fn error_chaos_unknown_target() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events

chaos:
  enabled: true
  schedule:
    - at: 10s
      action: kill
      target: nonexistent
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown component"), "got: {msg}");
}

#[test]
fn error_chaos_unknown_pool_target() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events

chaos:
  enabled: true
  schedule:
    - every: 10s
      action: kill_random
      pool: [prod, ghost]
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("ghost"), "got: {msg}");
}

#[test]
fn error_random_chaos_unknown_action_name() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events

chaos:
  enabled: true
  random:
    fault_rate: 1.0
    duration: 5s
    actions:
      typo_action:
        probability: 1.0
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown action type"), "got: {msg}");
    assert!(msg.contains("typo_action"), "got: {msg}");
}

#[test]
fn error_random_chaos_mixed_valid_and_invalid_action_names() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events

chaos:
  enabled: true
  random:
    fault_rate: 1.0
    duration: 5s
    actions:
      kill:
        probability: 0.8
        pool: [prod]
      typo_action:
        probability: 0.2
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown action type"), "got: {msg}");
    assert!(msg.contains("typo_action"), "got: {msg}");
}

#[test]
fn valid_random_chaos_known_action_names() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events

chaos:
  enabled: true
  random:
    fault_rate: 1.0
    duration: 5s
    actions:
      kill:
        probability: 0.2
        restart_after: [1s, 2s]
        pool: [prod, cons]
      pause:
        probability: 0.2
        pause_duration: [100ms, 500ms]
        pool: [prod]
      restart:
        probability: 0.2
        restart_after: [100ms, 1s]
        pool: [cons]
      slow:
        probability: 0.1
        delay_ms: [10, 50]
        pool: [prod]
      network_partition:
        probability: 0.2
        heal_after: [1s, 2s]
        pool: [prod, cons]
      kill_random:
        probability: 0.1
        restart_after: [1s, 2s]
        pool: [prod, cons]
"#,
    );
    validate(&config).unwrap();
}

// ---------------------------------------------------------------------------
// Warnings
// ---------------------------------------------------------------------------

#[test]
fn warning_no_storage_agent() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: test/events
  - name: logs
    key_prefix: test/logs

components:
  - name: prod1
    kind: producer
    topic: events
  - name: prod2
    kind: producer
    topic: logs
  - name: cons1
    kind: consumer
    topic: events
  - name: cons2
    kind: consumer
    topic: logs
"#,
    );
    let result = validate(&config).unwrap();
    // Both topics should generate warnings.
    assert_eq!(result.warnings.len(), 2);
}

// ---------------------------------------------------------------------------
// resolve_component_config
// ---------------------------------------------------------------------------

#[test]
fn resolve_inherits_from_defaults() {
    let config = parse(
        r#"
defaults:
  codec: msgpack
  cache_size: 512
  heartbeat_ms: 2000

topics:
  - name: events
    key_prefix: demo/events

components:
  - name: prod
    kind: producer
    topic: events
"#,
    );
    let topic = &config.topics[0];
    let comp = &config.components[0];
    let resolved = resolve_component_config(comp, Some(topic), &config.defaults);
    assert_eq!(
        resolved.codec,
        mitiflow_emulator::config::CodecConfig::Msgpack
    );
    assert_eq!(resolved.cache_size, 512);
    assert_eq!(resolved.heartbeat_ms, 2000);
    assert_eq!(resolved.key_prefix, "demo/events");
}

#[test]
fn resolve_component_overrides_defaults() {
    let config = parse(
        r#"
defaults:
  codec: json
  cache_size: 256
  heartbeat_ms: 1000

topics:
  - name: events
    key_prefix: demo/events

components:
  - name: prod
    kind: producer
    topic: events
    codec: postcard
    cache_size: 1024
    heartbeat_ms: 500
"#,
    );
    let topic = &config.topics[0];
    let comp = &config.components[0];
    let resolved = resolve_component_config(comp, Some(topic), &config.defaults);
    assert_eq!(
        resolved.codec,
        mitiflow_emulator::config::CodecConfig::Postcard
    );
    assert_eq!(resolved.cache_size, 1024);
    assert_eq!(resolved.heartbeat_ms, 500);
}

#[test]
fn resolve_topic_codec_overrides_defaults() {
    let config = parse(
        r#"
defaults:
  codec: json

topics:
  - name: events
    key_prefix: demo/events
    codec: msgpack

components:
  - name: prod
    kind: producer
    topic: events
"#,
    );
    let topic = &config.topics[0];
    let comp = &config.components[0];
    let resolved = resolve_component_config(comp, Some(topic), &config.defaults);
    assert_eq!(
        resolved.codec,
        mitiflow_emulator::config::CodecConfig::Msgpack
    );
}

#[test]
fn resolve_inherits_partitions_from_topic() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: demo/events
    num_partitions: 32
    replication_factor: 3

components:
  - name: prod
    kind: producer
    topic: events
"#,
    );
    let topic = &config.topics[0];
    let comp = &config.components[0];
    let resolved = resolve_component_config(comp, Some(topic), &config.defaults);
    assert_eq!(resolved.num_partitions, 32);
    assert_eq!(resolved.replication_factor, 3);
}

// ---------------------------------------------------------------------------
// Agent component validation
// ---------------------------------------------------------------------------

#[test]
fn valid_agent_multi_topic() {
    let config = parse(
        r#"
topics:
  - name: orders
    key_prefix: app/orders
  - name: payments
    key_prefix: app/payments

components:
  - name: store
    kind: agent
    topics:
      - orders
      - payments
  - name: prod1
    kind: producer
    topic: orders
  - name: prod2
    kind: producer
    topic: payments
  - name: cons
    kind: consumer
    topic: orders
"#,
    );
    let result = validate(&config).unwrap();
    // No storage coverage warnings — agent covers both topics.
    assert!(result.warnings.is_empty(), "got: {:?}", result.warnings);
}

#[test]
fn valid_agent_auto_discover_no_topics() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: app/events

components:
  - name: store
    kind: agent
    auto_discover_topics: true
    global_prefix: app
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events
"#,
    );
    // Agent with auto_discover_topics but no managed_topics is valid.
    let result = validate(&config).unwrap();
    // However, topics will show a storage warning since the agent doesn't
    // statically declare them in managed_topics.
    assert!(!result.warnings.is_empty());
}

#[test]
fn error_agent_no_topics_no_auto_discover() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: app/events

components:
  - name: store
    kind: agent
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("at least one topic"), "got: {msg}");
}

#[test]
fn error_agent_unknown_topic_ref() {
    let config = parse(
        r#"
topics:
  - name: events
    key_prefix: app/events

components:
  - name: store
    kind: agent
    topics:
      - events
      - nonexistent
  - name: prod
    kind: producer
    topic: events
  - name: cons
    kind: consumer
    topic: events
"#,
    );
    let err = validate(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown topic"), "got: {msg}");
    assert!(msg.contains("nonexistent"), "got: {msg}");
}

#[test]
fn agent_storage_coverage_partial() {
    // Agent covers "orders" but not "payments" — should warn about "payments".
    let config = parse(
        r#"
topics:
  - name: orders
    key_prefix: app/orders
  - name: payments
    key_prefix: app/payments

components:
  - name: store
    kind: agent
    topics:
      - orders
  - name: prod1
    kind: producer
    topic: orders
  - name: prod2
    kind: producer
    topic: payments
  - name: cons
    kind: consumer
    topic: orders
"#,
    );
    let result = validate(&config).unwrap();
    assert_eq!(result.warnings.len(), 1);
    assert!(result.warnings[0].message.contains("payments"));
}
