//! Tests for YAML configuration parsing.

use mitiflow_emulator::config::*;

#[test]
fn parse_minimal_topology() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: demo/events

components:
  - name: prod-1
    kind: producer
    topic: events
    rate: 10.0

  - name: cons-1
    kind: consumer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.topics.len(), 1);
    assert_eq!(config.topics[0].name, "events");
    assert_eq!(config.topics[0].key_prefix, "demo/events");
    assert_eq!(config.components.len(), 2);
    assert_eq!(config.components[0].kind, ComponentKind::Producer);
    assert_eq!(config.components[1].kind, ComponentKind::Consumer);
    assert_eq!(config.components[0].rate, Some(10.0));
}

#[test]
fn parse_defaults_section() {
    let yaml = r#"
defaults:
  codec: msgpack
  isolation: container
  cache_size: 512
  heartbeat_ms: 2000
  recovery_mode: heartbeat

topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.defaults.codec, CodecConfig::Msgpack);
    assert_eq!(config.defaults.isolation, IsolationMode::Container);
    assert_eq!(config.defaults.cache_size, 512);
    assert_eq!(config.defaults.heartbeat_ms, 2000);
    assert_eq!(config.defaults.recovery_mode, RecoveryModeConfig::Heartbeat);
}

#[test]
fn parse_zenoh_config() {
    let yaml = r#"
zenoh:
  mode: client
  connect:
    - tcp/localhost:7447
  auto_router: true

topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.zenoh.mode, ZenohMode::Client);
    assert_eq!(config.zenoh.connect, vec!["tcp/localhost:7447".to_string()]);
    assert!(config.zenoh.auto_router);
}

#[test]
fn parse_logging_config() {
    let yaml = r#"
logging:
  mode: both
  directory: /tmp/logs
  format: json
  level: debug
  per_component: true

topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.logging.mode, LogMode::Both);
    assert_eq!(config.logging.format, LogFormat::Json);
    assert_eq!(config.logging.level, LogLevel::Debug);
    assert!(config.logging.per_component);
}

#[test]
fn parse_topic_with_retention_and_compaction() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events
    codec: postcard
    num_partitions: 8
    replication_factor: 3
    retention:
      max_age: 24h
      max_bytes: 1073741824
    compaction:
      enabled: true

components:
  - name: prod
    kind: producer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let topic = &config.topics[0];
    assert_eq!(topic.codec, Some(CodecConfig::Postcard));
    assert_eq!(topic.num_partitions, 8);
    assert_eq!(topic.replication_factor, 3);
    let retention = topic.retention.as_ref().unwrap();
    assert_eq!(
        retention.max_age,
        Some(std::time::Duration::from_secs(24 * 3600))
    );
    assert_eq!(retention.max_bytes, Some(1073741824));
    assert!(topic.compaction.as_ref().unwrap().enabled);
}

#[test]
fn parse_payload_generator_random_json() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
    payload:
      generator: random_json
      size_bytes: 512
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let payload = config.components[0].payload.as_ref().unwrap();
    assert_eq!(payload.generator, GeneratorType::RandomJson);
    assert_eq!(payload.size_bytes, 512);
}

#[test]
fn parse_payload_generator_counter() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
    payload:
      generator: counter
      prefix: test-event
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let payload = config.components[0].payload.as_ref().unwrap();
    assert_eq!(payload.generator, GeneratorType::Counter);
    assert_eq!(payload.prefix, Some("test-event".into()));
}

#[test]
fn parse_payload_generator_schema() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
    payload:
      generator: schema
      schema:
        temperature:
          type: float
          min: -40.0
          max: 60.0
        sensor_id:
          type: uuid
        active:
          type: bool
          probability: 0.8
        region:
          type: enum
          values: [us-east, us-west, eu-west]
        rack:
          type: string
          pattern: "rack-{0-99}"
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let payload = config.components[0].payload.as_ref().unwrap();
    assert_eq!(payload.generator, GeneratorType::Schema);
    assert_eq!(payload.schema.len(), 5);
    assert_eq!(
        payload.schema["temperature"].field_type,
        SchemaFieldType::Float
    );
    assert_eq!(payload.schema["temperature"].min, Some(-40.0));
    assert_eq!(payload.schema["region"].field_type, SchemaFieldType::Enum);
    assert_eq!(payload.schema["region"].values.as_ref().unwrap().len(), 3);
}

#[test]
fn parse_processing_modes() {
    let yaml = r#"
topics:
  - name: input
    key_prefix: test/input
  - name: output
    key_prefix: test/output

components:
  - name: prod
    kind: producer
    topic: input

  - name: proc
    kind: processor
    input_topic: input
    output_topic: output
    processing:
      mode: delay
      delay_ms: 100
      drop_probability: 0.1
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let proc = &config.components[1];
    let processing = proc.processing.as_ref().unwrap();
    assert_eq!(processing.mode, ProcessingMode::Delay);
    assert_eq!(processing.delay_ms, Some(100));
    assert_eq!(processing.drop_probability, Some(0.1));
}

#[test]
fn parse_consumer_group() {
    let yaml = r#"
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
    consumer_group:
      group_id: my-group
      commit_mode: manual
      auto_commit_interval_ms: 3000
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let cg = config.components[1].consumer_group.as_ref().unwrap();
    assert_eq!(cg.group_id, "my-group");
    assert_eq!(cg.commit_mode, CommitModeDef::Manual);
    assert_eq!(cg.auto_commit_interval_ms, 3000);
}

#[test]
fn parse_chaos_schedule() {
    let yaml = r#"
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
      target: prod
      restart_after: 2s
    - every: 30s
      action: pause
      target: cons
      duration: 5s
    - every: 60s
      action: kill_random
      pool: [prod, cons]
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert!(config.chaos.enabled);
    assert_eq!(config.chaos.schedule.len(), 3);

    let kill = &config.chaos.schedule[0];
    assert_eq!(kill.at, Some(std::time::Duration::from_secs(10)));
    assert_eq!(kill.action, ChaosAction::Kill);
    assert_eq!(kill.target, Some("prod".into()));
    assert_eq!(kill.restart_after, Some(std::time::Duration::from_secs(2)));

    let pause = &config.chaos.schedule[1];
    assert_eq!(pause.every, Some(std::time::Duration::from_secs(30)));
    assert_eq!(pause.action, ChaosAction::Pause);
    assert_eq!(pause.duration, Some(std::time::Duration::from_secs(5)));

    let kill_random = &config.chaos.schedule[2];
    assert_eq!(kill_random.action, ChaosAction::KillRandom);
    assert_eq!(kill_random.pool, vec!["prod", "cons"]);
}

#[test]
fn parse_depends_on() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: store
    kind: storage_agent
    topic: events

  - name: prod
    kind: producer
    topic: events
    depends_on:
      - store
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.components[1].depends_on, vec!["store"]);
}

#[test]
fn parse_isolation_override() {
    let yaml = r#"
defaults:
  isolation: process

topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
    isolation: container
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.defaults.isolation, IsolationMode::Process);
    assert_eq!(
        config.components[0].isolation,
        Some(IsolationMode::Container)
    );
}

#[test]
fn parse_output_config() {
    let yaml = r#"
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
    output:
      mode: log
      report_interval_sec: 10
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let output = config.components[1].output.as_ref().unwrap();
    assert_eq!(output.mode, OutputMode::Log);
    assert_eq!(output.report_interval_sec, 10);
}

#[test]
fn parse_orchestrator_component() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: orch
    kind: orchestrator
    lag_interval_ms: 2000

  - name: prod
    kind: producer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let orch = &config.components[0];
    assert_eq!(orch.kind, ComponentKind::Orchestrator);
    assert_eq!(orch.lag_interval_ms, Some(2000));
}

#[test]
fn parse_storage_agent_component() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: store-1
    kind: storage_agent
    topic: events
    capacity: 200
    instances: 2

  - name: prod
    kind: producer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let store = &config.components[0];
    assert_eq!(store.kind, ComponentKind::StorageAgent);
    assert_eq!(store.capacity, Some(200));
    assert_eq!(store.instances, 2);
}

#[test]
fn parse_durable_producer() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
    durable: true
    cache_size: 512
    heartbeat_ms: 500
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let prod = &config.components[0];
    assert!(prod.durable);
    assert_eq!(prod.cache_size, Some(512));
    assert_eq!(prod.heartbeat_ms, Some(500));
}

#[test]
fn defaults_applied_when_missing() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    // Check that defaults are populated.
    assert_eq!(config.defaults.codec, CodecConfig::Json);
    assert_eq!(config.defaults.cache_size, 256);
    assert_eq!(config.defaults.heartbeat_ms, 1000);
    assert_eq!(config.defaults.isolation, IsolationMode::Process);
    assert_eq!(config.zenoh.mode, ZenohMode::Peer);
    assert_eq!(config.logging.mode, LogMode::Stdout);
    assert_eq!(config.logging.level, LogLevel::Info);
    assert!(!config.chaos.enabled);
}

#[test]
fn parse_full_topology() {
    let yaml = r#"
zenoh:
  mode: peer

defaults:
  codec: json
  cache_size: 256
  heartbeat_ms: 1000
  recovery_mode: both

logging:
  mode: stdout
  level: info

topics:
  - name: raw-events
    key_prefix: app/raw
    num_partitions: 16
    replication_factor: 2
  - name: enriched-events
    key_prefix: app/enriched
    num_partitions: 8

components:
  - name: orch
    kind: orchestrator
    lag_interval_ms: 1000

  - name: store-raw
    kind: storage_agent
    topic: raw-events
    instances: 2
    capacity: 100

  - name: store-enriched
    kind: storage_agent
    topic: enriched-events

  - name: fast-producer
    kind: producer
    topic: raw-events
    rate: 1000.0
    payload:
      generator: schema
      schema:
        temp:
          type: float
          min: 20.0
          max: 40.0

  - name: enricher
    kind: processor
    input_topic: raw-events
    output_topic: enriched-events
    processing:
      mode: passthrough

  - name: analytics
    kind: consumer
    topic: enriched-events
    consumer_group:
      group_id: analytics-group
    output:
      mode: count
      report_interval_sec: 5

chaos:
  enabled: false
  schedule:
    - at: 30s
      action: kill
      target: store-raw
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.topics.len(), 2);
    assert_eq!(config.components.len(), 6);
}

#[test]
fn error_on_missing_components() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events
"#;
    let err = TopologyConfig::from_yaml(yaml);
    assert!(err.is_err());
}

#[test]
fn parse_multiple_instances() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
    instances: 3
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.components[0].instances, 3);
}

#[test]
fn parse_codec_override_at_component() {
    let yaml = r#"
defaults:
  codec: json

topics:
  - name: events
    key_prefix: test/events

components:
  - name: prod
    kind: producer
    topic: events
    codec: postcard
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    assert_eq!(config.components[0].codec, Some(CodecConfig::Postcard));
}

#[test]
fn parse_agent_component() {
    let yaml = r#"
topics:
  - name: orders
    key_prefix: app/orders
    num_partitions: 4
  - name: payments
    key_prefix: app/payments
    num_partitions: 8

components:
  - name: store
    kind: agent
    topics:
      - orders
      - payments
    capacity: 200
    global_prefix: app

  - name: prod
    kind: producer
    topic: orders
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let agent = &config.components[0];
    assert_eq!(agent.kind, ComponentKind::Agent);
    assert_eq!(agent.managed_topics, vec!["orders", "payments"]);
    assert_eq!(agent.capacity, Some(200));
    assert_eq!(agent.global_prefix, Some("app".into()));
}

#[test]
fn parse_agent_with_auto_discover() {
    let yaml = r#"
topics:
  - name: events
    key_prefix: app/events

components:
  - name: store
    kind: agent
    topics:
      - events
    auto_discover_topics: true
    global_prefix: app
    labels:
      rack: us-east-1a
      tier: ssd

  - name: prod
    kind: producer
    topic: events
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let agent = &config.components[0];
    assert_eq!(agent.kind, ComponentKind::Agent);
    assert_eq!(agent.auto_discover_topics, Some(true));
    let labels = agent.labels.as_ref().unwrap();
    assert_eq!(labels["rack"], "us-east-1a");
    assert_eq!(labels["tier"], "ssd");
}

#[test]
fn parse_agent_empty_topics_with_auto_discover() {
    let yaml = r#"
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
"#;
    let config = TopologyConfig::from_yaml(yaml).unwrap();
    let agent = &config.components[0];
    assert_eq!(agent.kind, ComponentKind::Agent);
    assert!(agent.managed_topics.is_empty());
    assert_eq!(agent.auto_discover_topics, Some(true));
}
