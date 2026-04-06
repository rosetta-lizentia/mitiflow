//! Tests for TopicWatcher — dynamic topic discovery and placement labels.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow_storage::TopicEntry;
use mitiflow_storage::config::AgentConfig;
use mitiflow_storage::topic_watcher::{RemoteTopicConfig, should_serve_topic};

// =========================================================================
// should_serve_topic — label matching unit tests
// =========================================================================

fn remote_config(
    name: &str,
    required: HashMap<String, String>,
    excluded: HashMap<String, String>,
) -> RemoteTopicConfig {
    RemoteTopicConfig {
        name: name.to_string(),
        key_prefix: format!("test/{name}"),
        num_partitions: 4,
        replication_factor: 1,
        required_labels: required,
        excluded_labels: excluded,
    }
}

#[test]
fn should_serve_topic_no_constraints() {
    let cfg = remote_config("events", HashMap::new(), HashMap::new());
    let labels = HashMap::new();
    assert!(should_serve_topic(&cfg, &labels));
}

#[test]
fn should_serve_topic_required_all_match() {
    let mut required = HashMap::new();
    required.insert("tier".into(), "ssd".into());
    required.insert("rack".into(), "us-east".into());
    let cfg = remote_config("events", required, HashMap::new());

    let mut agent = HashMap::new();
    agent.insert("tier".into(), "ssd".into());
    agent.insert("rack".into(), "us-east".into());
    agent.insert("extra".into(), "ignored".into());
    assert!(should_serve_topic(&cfg, &agent));
}

#[test]
fn should_serve_topic_missing_required() {
    let mut required = HashMap::new();
    required.insert("tier".into(), "ssd".into());
    let cfg = remote_config("events", required, HashMap::new());

    let agent = HashMap::new(); // no labels
    assert!(!should_serve_topic(&cfg, &agent));
}

#[test]
fn should_serve_topic_required_wrong_value() {
    let mut required = HashMap::new();
    required.insert("tier".into(), "ssd".into());
    let cfg = remote_config("events", required, HashMap::new());

    let mut agent = HashMap::new();
    agent.insert("tier".into(), "hdd".into());
    assert!(!should_serve_topic(&cfg, &agent));
}

#[test]
fn should_serve_topic_excluded_matches() {
    let mut excluded = HashMap::new();
    excluded.insert("env".into(), "staging".into());
    let cfg = remote_config("events", HashMap::new(), excluded);

    let mut agent = HashMap::new();
    agent.insert("env".into(), "staging".into());
    assert!(!should_serve_topic(&cfg, &agent));
}

#[test]
fn should_serve_topic_excluded_no_match() {
    let mut excluded = HashMap::new();
    excluded.insert("env".into(), "staging".into());
    let cfg = remote_config("events", HashMap::new(), excluded);

    let mut agent = HashMap::new();
    agent.insert("env".into(), "prod".into());
    assert!(should_serve_topic(&cfg, &agent));
}

#[test]
fn should_serve_topic_excluded_agent_lacks_key() {
    let mut excluded = HashMap::new();
    excluded.insert("env".into(), "staging".into());
    let cfg = remote_config("events", HashMap::new(), excluded);

    let agent = HashMap::new(); // no labels at all
    assert!(should_serve_topic(&cfg, &agent));
}

#[test]
fn should_serve_topic_both_required_and_excluded() {
    let mut required = HashMap::new();
    required.insert("rack".into(), "us-east".into());
    let mut excluded = HashMap::new();
    excluded.insert("role".into(), "readonly".into());
    let cfg = remote_config("events", required, excluded);

    // Has required, no excluded → serve
    let mut agent1 = HashMap::new();
    agent1.insert("rack".into(), "us-east".into());
    assert!(should_serve_topic(&cfg, &agent1));

    // Has required but also excluded → skip
    let mut agent2 = HashMap::new();
    agent2.insert("rack".into(), "us-east".into());
    agent2.insert("role".into(), "readonly".into());
    assert!(!should_serve_topic(&cfg, &agent2));

    // Missing required → skip (regardless of excluded)
    let mut agent3 = HashMap::new();
    agent3.insert("role".into(), "readwrite".into());
    assert!(!should_serve_topic(&cfg, &agent3));
}

// =========================================================================
// TopicWatcher integration tests (with live Zenoh)
// =========================================================================

fn unique_prefix(test: &str) -> String {
    format!("test/tw_{test}_{}", uuid::Uuid::now_v7())
}

/// Helper to build an AgentConfig for a single-topic agent.
fn agent_config(
    prefix: &str,
    node_id: &str,
    auto_discover: bool,
    labels: HashMap<String, String>,
) -> (tempfile::TempDir, AgentConfig) {
    let tmp = tempfile::tempdir().unwrap();
    let config = AgentConfig {
        node_id: node_id.to_string(),
        data_dir: tmp.path().to_path_buf(),
        capacity: 100,
        labels,
        health_interval: Duration::from_secs(5),
        drain_grace_period: Duration::from_millis(200),
        topics: vec![],
        global_prefix: prefix.to_string(),
        auto_discover_topics: auto_discover,
    };
    (tmp, config)
}

/// Simulate an orchestrator config queryable by declaring the queryable +
/// publishing topic configs.
#[allow(dead_code)]
struct MockOrchestrator {
    session: zenoh::Session,
    prefix: String,
    configs: Vec<RemoteTopicConfig>,
    _queryable:
        Option<zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>>,
}

impl MockOrchestrator {
    async fn new(session: &zenoh::Session, prefix: &str) -> Self {
        Self {
            session: session.clone(),
            prefix: prefix.to_string(),
            configs: Vec::new(),
            _queryable: None,
        }
    }

    async fn start(&mut self) {
        let config_key = format!("{}/_config/**", self.prefix);
        let queryable = self.session.declare_queryable(&config_key).await.unwrap();

        let configs = self.configs.clone();
        let prefix = self.prefix.clone();
        let _session = self.session.clone();

        let qbl = self.session.declare_queryable(&config_key).await.unwrap();
        // We need to reply to queries with stored configs
        let configs_for_task = configs.clone();
        let prefix_for_task = prefix.clone();
        tokio::spawn(async move {
            while let Ok(query) = qbl.recv_async().await {
                for cfg in &configs_for_task {
                    let reply_key = format!("{}/_config/{}", prefix_for_task, cfg.name);
                    if let Ok(bytes) = serde_json::to_vec(cfg) {
                        let _ = query.reply(&reply_key, bytes).await;
                    }
                }
            }
        });

        self._queryable = Some(queryable);
    }

    #[allow(dead_code)]
    async fn publish_topic(&self, cfg: &RemoteTopicConfig) {
        let key = format!("{}/_config/{}", self.prefix, cfg.name);
        let bytes = serde_json::to_vec(cfg).unwrap();
        self.session.put(&key, bytes).await.unwrap();
    }

    #[allow(dead_code)]
    async fn delete_topic(&self, name: &str) {
        let key = format!("{}/_config/{}", self.prefix, name);
        self.session.delete(&key).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_discovers_existing_topics_on_startup() {
    let prefix = unique_prefix("discover");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Setup mock orchestrator with 2 topics.
    let mut mock = MockOrchestrator::new(&session, &prefix).await;
    mock.configs
        .push(remote_config_with_prefix("orders", &prefix));
    mock.configs
        .push(remote_config_with_prefix("logs", &prefix));
    mock.start().await;

    // Give queryable time to register.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start agent with auto_discover=true, no static topics.
    let (_tmp, config) = agent_config(&prefix, "node-1", true, HashMap::new());
    let mut agent = mitiflow_storage::StorageAgent::start_multi(&session, config)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let topics = agent.topics().await;
    assert!(
        topics.contains(&"orders".to_string()),
        "should discover 'orders', got {:?}",
        topics
    );
    assert!(
        topics.contains(&"logs".to_string()),
        "should discover 'logs', got {:?}",
        topics
    );

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_reacts_to_new_topic() {
    let prefix = unique_prefix("react_new");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Start agent with auto_discover, no existing topics in orchestrator.
    let (_tmp, config) = agent_config(&prefix, "node-1", true, HashMap::new());
    let mut agent = mitiflow_storage::StorageAgent::start_multi(&session, config)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(agent.topics().await.is_empty());

    // Now publish a topic config.
    let cfg = remote_config_with_prefix("events", &prefix);
    let key = format!("{}/_config/{}", prefix, cfg.name);
    let bytes = serde_json::to_vec(&cfg).unwrap();
    session.put(&key, bytes).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let topics = agent.topics().await;
    assert!(
        topics.contains(&"events".to_string()),
        "should pick up new topic, got {:?}",
        topics
    );

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_reacts_to_deleted_topic() {
    let prefix = unique_prefix("react_del");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Start agent and add a topic manually.
    let (_tmp, config) = agent_config(&prefix, "node-1", true, HashMap::new());
    let mut agent = mitiflow_storage::StorageAgent::start_multi(&session, config)
        .await
        .unwrap();

    let entry = TopicEntry {
        name: "ephemeral".into(),
        key_prefix: format!("{prefix}/ephemeral"),
        num_partitions: 2,
        replication_factor: 1,
    };
    agent.add_topic(entry).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(agent.has_topic("ephemeral").await);

    // Publish delete via Zenoh.
    let key = format!("{}/_config/ephemeral", prefix);
    session.delete(&key).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !agent.has_topic("ephemeral").await,
        "topic should be removed after delete"
    );

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_ignores_topic_missing_labels() {
    let prefix = unique_prefix("ignore_labels");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Agent has {tier: hdd}, topic requires {tier: ssd}.
    let mut agent_labels = HashMap::new();
    agent_labels.insert("tier".into(), "hdd".into());
    let (_tmp, config) = agent_config(&prefix, "node-1", true, agent_labels);
    let mut agent = mitiflow_storage::StorageAgent::start_multi(&session, config)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut required = HashMap::new();
    required.insert("tier".into(), "ssd".into());
    let cfg = RemoteTopicConfig {
        name: "hot-data".to_string(),
        key_prefix: format!("{prefix}/hot-data"),
        num_partitions: 4,
        replication_factor: 1,
        required_labels: required,
        excluded_labels: HashMap::new(),
    };
    let bytes = serde_json::to_vec(&cfg).unwrap();
    session
        .put(format!("{}/_config/hot-data", prefix), bytes)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !agent.has_topic("hot-data").await,
        "agent with hdd should not serve ssd topic"
    );

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_serves_topic_matching_labels() {
    let prefix = unique_prefix("match_labels");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let mut agent_labels = HashMap::new();
    agent_labels.insert("rack".into(), "us-east".into());
    let (_tmp, config) = agent_config(&prefix, "node-1", true, agent_labels);
    let mut agent = mitiflow_storage::StorageAgent::start_multi(&session, config)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut required = HashMap::new();
    required.insert("rack".into(), "us-east".into());
    let cfg = RemoteTopicConfig {
        name: "regional".to_string(),
        key_prefix: format!("{prefix}/regional"),
        num_partitions: 2,
        replication_factor: 1,
        required_labels: required,
        excluded_labels: HashMap::new(),
    };
    let bytes = serde_json::to_vec(&cfg).unwrap();
    session
        .put(format!("{}/_config/regional", prefix), bytes)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        agent.has_topic("regional").await,
        "agent matching labels should serve topic"
    );

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_excludes_by_label() {
    let prefix = unique_prefix("exclude_labels");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let mut agent_labels = HashMap::new();
    agent_labels.insert("env".into(), "staging".into());
    let (_tmp, config) = agent_config(&prefix, "node-1", true, agent_labels);
    let mut agent = mitiflow_storage::StorageAgent::start_multi(&session, config)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut excluded = HashMap::new();
    excluded.insert("env".into(), "staging".into());
    let cfg = RemoteTopicConfig {
        name: "prod-only".to_string(),
        key_prefix: format!("{prefix}/prod-only"),
        num_partitions: 2,
        replication_factor: 1,
        required_labels: HashMap::new(),
        excluded_labels: excluded,
    };
    let bytes = serde_json::to_vec(&cfg).unwrap();
    session
        .put(format!("{}/_config/prod-only", prefix), bytes)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !agent.has_topic("prod-only").await,
        "staging agent should not serve prod-only topic"
    );

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_idempotent_on_duplicate_config() {
    let prefix = unique_prefix("idempotent");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_tmp, config) = agent_config(&prefix, "node-1", true, HashMap::new());
    let mut agent = mitiflow_storage::StorageAgent::start_multi(&session, config)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let cfg = remote_config_with_prefix("events", &prefix);
    let key = format!("{}/_config/events", prefix);
    let bytes = serde_json::to_vec(&cfg).unwrap();

    // Publish same topic twice.
    session.put(&key, bytes.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    session.put(&key, bytes).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let topics = agent.topics().await;
    assert_eq!(
        topics.iter().filter(|t| *t == "events").count(),
        1,
        "should have exactly one 'events' topic, got {:?}",
        topics,
    );

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

// =========================================================================
// Helper
// =========================================================================

fn remote_config_with_prefix(name: &str, prefix: &str) -> RemoteTopicConfig {
    RemoteTopicConfig {
        name: name.to_string(),
        key_prefix: format!("{prefix}/{name}"),
        num_partitions: 4,
        replication_factor: 1,
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
    }
}
