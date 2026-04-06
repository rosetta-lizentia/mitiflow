//! Tests for YAML configuration parsing.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow_storage::config::{AgentYamlConfig, ClusterYamlConfig, NodeYamlConfig};

#[test]
fn parse_agent_yaml_full() {
    let yaml = r#"
node:
  id: node-42
  data_dir: /var/lib/mitiflow
  capacity: 200
  health_interval: 15s
  drain_grace_period: 1m
  labels:
    rack: us-east-1a
    tier: ssd

cluster:
  global_prefix: myapp
  auto_discover_topics: true

topics:
  - name: events
    key_prefix: myapp/events
    num_partitions: 16
    replication_factor: 2
  - name: logs
    key_prefix: myapp/logs
    num_partitions: 8
    replication_factor: 1
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    assert_eq!(cfg.node.id, "node-42");
    assert_eq!(cfg.node.data_dir.to_str().unwrap(), "/var/lib/mitiflow");
    assert_eq!(cfg.node.capacity, 200);
    assert_eq!(cfg.node.health_interval, Duration::from_secs(15));
    assert_eq!(cfg.node.drain_grace_period, Duration::from_secs(60));
    assert_eq!(cfg.node.labels.get("rack").unwrap(), "us-east-1a");
    assert_eq!(cfg.node.labels.get("tier").unwrap(), "ssd");

    assert_eq!(cfg.cluster.global_prefix, "myapp");
    assert!(cfg.cluster.auto_discover_topics);

    assert_eq!(cfg.topics.len(), 2);
    assert_eq!(cfg.topics[0].name, "events");
    assert_eq!(cfg.topics[0].key_prefix, "myapp/events");
    assert_eq!(cfg.topics[0].num_partitions, 16);
    assert_eq!(cfg.topics[0].replication_factor, 2);
    assert_eq!(cfg.topics[1].name, "logs");
    assert_eq!(cfg.topics[1].num_partitions, 8);
}

#[test]
fn parse_agent_yaml_minimal() {
    let yaml = r#"
cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    // Default values
    assert_eq!(cfg.node.id, "auto");
    assert_eq!(cfg.node.data_dir.to_str().unwrap(), "/tmp/mitiflow-storage");
    assert_eq!(cfg.node.capacity, 100);
    assert_eq!(cfg.node.health_interval, Duration::from_secs(10));
    assert_eq!(cfg.node.drain_grace_period, Duration::from_secs(30));
    assert!(cfg.node.labels.is_empty());

    assert_eq!(cfg.cluster.global_prefix, "mitiflow");
    assert!(cfg.cluster.auto_discover_topics);
    assert!(cfg.topics.is_empty());
}

#[test]
fn parse_agent_yaml_static_topics() {
    let yaml = r#"
topics:
  - name: orders
    key_prefix: shop/orders
    num_partitions: 32
    replication_factor: 3
  - name: inventory
    key_prefix: shop/inventory
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    assert_eq!(cfg.topics.len(), 2);
    assert_eq!(cfg.topics[0].name, "orders");
    assert_eq!(cfg.topics[0].num_partitions, 32);
    assert_eq!(cfg.topics[0].replication_factor, 3);
    assert_eq!(cfg.topics[1].name, "inventory");
    // Defaults
    assert_eq!(cfg.topics[1].num_partitions, 16);
    assert_eq!(cfg.topics[1].replication_factor, 1);
}

#[test]
fn parse_agent_yaml_auto_discover_only() {
    let yaml = r#"
cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let agent_config = cfg.into_agent_config().unwrap();
    assert!(agent_config.auto_discover_topics);
    assert!(agent_config.topics.is_empty());
}

#[test]
fn yaml_to_agent_config_converts_correctly() {
    let yaml = r#"
node:
  id: test-node
  data_dir: /data
  capacity: 50
  labels:
    zone: a

cluster:
  global_prefix: acme

topics:
  - name: events
    key_prefix: acme/events
    num_partitions: 4
    replication_factor: 2
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let agent_config = cfg.into_agent_config().unwrap();

    assert_eq!(agent_config.node_id, "test-node");
    assert_eq!(agent_config.data_dir.to_str().unwrap(), "/data");
    assert_eq!(agent_config.capacity, 50);
    assert_eq!(agent_config.global_prefix, "acme");
    assert_eq!(agent_config.labels.get("zone").unwrap(), "a");
    assert_eq!(agent_config.topics.len(), 1);
    assert_eq!(agent_config.topics[0].name, "events");
    assert_eq!(agent_config.topics[0].num_partitions, 4);
}

#[test]
fn yaml_auto_node_id_generates_uuid() {
    let yaml = r#"
cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let agent_config = cfg.into_agent_config().unwrap();
    // When id == "auto", it should generate a UUID
    assert!(!agent_config.node_id.is_empty());
    assert_ne!(agent_config.node_id, "auto");
}

#[test]
fn yaml_rejects_no_topics_and_no_discover() {
    let yaml = r#"
node:
  id: lonely-node
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let err = cfg.into_agent_config().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("at least one topic"), "error: {msg}");
}

#[test]
fn yaml_roundtrip_serialization() {
    let cfg = AgentYamlConfig {
        node: NodeYamlConfig {
            id: "my-node".into(),
            data_dir: "/data".into(),
            capacity: 100,
            health_interval: Duration::from_secs(10),
            drain_grace_period: Duration::from_secs(30),
            labels: {
                let mut m = HashMap::new();
                m.insert("tier".into(), "ssd".into());
                m
            },
        },
        cluster: ClusterYamlConfig {
            global_prefix: "mitiflow".into(),
            auto_discover_topics: true,
        },
        topics: vec![],
    };

    let yaml_str = serde_yaml::to_string(&cfg).unwrap();
    let parsed: AgentYamlConfig = serde_yaml::from_str(&yaml_str).unwrap();
    assert_eq!(parsed.node.id, "my-node");
    assert_eq!(parsed.node.labels.get("tier").unwrap(), "ssd");
    assert!(parsed.cluster.auto_discover_topics);
}

#[test]
fn parse_agent_yaml_humantime_durations() {
    let yaml = r#"
node:
  health_interval: 500ms
  drain_grace_period: 2m 30s

cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    assert_eq!(cfg.node.health_interval, Duration::from_millis(500));
    assert_eq!(cfg.node.drain_grace_period, Duration::from_secs(150));
}
