//! TDD tests for schema registry race-condition fix.
//!
//! These tests exercise the acceptance criteria from doc 18 (revised design):
//!
//! Phase 1 — Orchestrator per-topic exact-key schema serving:
//!   - Exact per-topic schema query succeeds with only orchestrator running
//!   - Repeated queries return stable metadata
//!   - Bootstrapped topics never serve schema_version=0
//!
//! Phase 2 — Storage agent self-warming bootstrap:
//!   - Late-starting storage agent fetches schema from orchestrator
//!   - Storage agent serves schema after bootstrap (orchestrator down)
//!   - Idempotent same-version republish accepted silently
//!
//! All tests use Zenoh peer-to-peer mode — no external services required.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow::schema::fetch_schema_with_timeout;
use mitiflow::{CodecFormat, KeyFormat, TopicSchema};
use mitiflow_orchestrator::config::{CompactionPolicy, RetentionPolicy, TopicConfig};
use mitiflow_orchestrator::orchestrator::{Orchestrator, OrchestratorConfig};
use mitiflow_storage::{AgentConfig, SchemaStore, StorageAgent, TopicEntry};

/// Unique key prefix per test to avoid cross-talk.
fn unique_prefix(test: &str) -> String {
    format!("test/schema_fix_{test}_{}", uuid::Uuid::now_v7())
}

fn make_topic_config(name: &str, key_prefix: &str) -> TopicConfig {
    TopicConfig {
        name: name.into(),
        key_prefix: key_prefix.into(),
        num_partitions: 16,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: CodecFormat::Postcard,
        key_format: KeyFormat::Keyed,
        schema_version: 1,
    }
}

// ==========================================================================
// Phase 1: Orchestrator per-topic exact-key schema serving
// ==========================================================================

/// The orchestrator with a bootstrapped topic must answer exact per-topic
/// schema queries. This is the core fix: previously the orchestrator declared
/// a single root `_schema` queryable that never matched per-topic keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_serves_exact_per_topic_schema_query() {
    let prefix = unique_prefix("orch_exact_query");
    let topic_prefix = format!("{prefix}/workflows");

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();

    let mut orch = Orchestrator::new(
        &session,
        OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: None,
        },
    )
    .unwrap();

    // Create topic BEFORE run — so it's in the config store at startup
    orch.create_topic(make_topic_config("workflows", &topic_prefix))
        .await
        .unwrap();

    orch.run().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Query the exact per-topic schema key — this must succeed
    let schema = fetch_schema_with_timeout(&session, &topic_prefix, Duration::from_secs(2))
        .await
        .expect("orchestrator should answer exact per-topic schema query");

    assert_eq!(schema.name, "workflows");
    assert_eq!(schema.key_prefix, topic_prefix);
    assert_eq!(schema.codec, CodecFormat::Postcard);
    assert_eq!(schema.num_partitions, 16);
    assert_eq!(schema.key_format, KeyFormat::Keyed);

    orch.shutdown().await;
    session.close().await.unwrap();
}

/// When the orchestrator manages multiple topics with different key prefixes,
/// querying each topic's exact `_schema` key must return the correct schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_serves_multiple_topic_schemas() {
    let prefix = unique_prefix("orch_multi");
    let wf_prefix = format!("{prefix}/workflows");
    let steps_prefix = format!("{prefix}/steps");
    let events_prefix = format!("{prefix}/events");

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();

    let mut orch = Orchestrator::new(
        &session,
        OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: None,
        },
    )
    .unwrap();

    orch.create_topic(make_topic_config("workflows", &wf_prefix))
        .await
        .unwrap();
    orch.create_topic(make_topic_config("steps", &steps_prefix))
        .await
        .unwrap();
    orch.create_topic(make_topic_config("events", &events_prefix))
        .await
        .unwrap();

    orch.run().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Each topic must be independently queryable
    let wf_schema = fetch_schema_with_timeout(&session, &wf_prefix, Duration::from_secs(2))
        .await
        .expect("workflows schema query failed");
    assert_eq!(wf_schema.name, "workflows");

    let steps_schema = fetch_schema_with_timeout(&session, &steps_prefix, Duration::from_secs(2))
        .await
        .expect("steps schema query failed");
    assert_eq!(steps_schema.name, "steps");

    let events_schema = fetch_schema_with_timeout(&session, &events_prefix, Duration::from_secs(2))
        .await
        .expect("events schema query failed");
    assert_eq!(events_schema.name, "events");

    orch.shutdown().await;
    session.close().await.unwrap();
}

/// Repeated queries for the same schema must return consistent wire-level
/// metadata (name, codec, partitions, version).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_schema_replies_are_consistent() {
    let prefix = unique_prefix("orch_stable");
    let topic_prefix = format!("{prefix}/workflows");

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();

    let mut orch = Orchestrator::new(
        &session,
        OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: None,
        },
    )
    .unwrap();

    orch.create_topic(make_topic_config("workflows", &topic_prefix))
        .await
        .unwrap();
    orch.run().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let schema1 = fetch_schema_with_timeout(&session, &topic_prefix, Duration::from_secs(2))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let schema2 = fetch_schema_with_timeout(&session, &topic_prefix, Duration::from_secs(2))
        .await
        .unwrap();

    // Wire-level contract must be stable
    assert_eq!(schema1.name, schema2.name);
    assert_eq!(schema1.codec, schema2.codec);
    assert_eq!(schema1.num_partitions, schema2.num_partitions);
    assert_eq!(schema1.key_format, schema2.key_format);
    assert_eq!(schema1.schema_version, schema2.schema_version);

    orch.shutdown().await;
    session.close().await.unwrap();
}

/// Bootstrapped topics with default schema_version must be normalized to
/// version >= 1. Version 0 is reserved for "unset" and must never be served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_never_serves_schema_version_zero() {
    let prefix = unique_prefix("orch_no_v0");
    let topic_prefix = format!("{prefix}/workflows");

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();

    // Create a topic with schema_version=0 (simulates YAML bootstrap default)
    let mut topic = make_topic_config("workflows", &topic_prefix);
    topic.schema_version = 0;

    let mut orch = Orchestrator::new(
        &session,
        OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: None,
        },
    )
    .unwrap();

    orch.create_topic(topic).await.unwrap();
    orch.run().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let schema = fetch_schema_with_timeout(&session, &topic_prefix, Duration::from_secs(2))
        .await
        .unwrap();

    assert!(
        schema.schema_version >= 1,
        "schema_version must be >= 1, got {}",
        schema.schema_version
    );

    orch.shutdown().await;
    session.close().await.unwrap();
}

/// A topic created AFTER `run()` must also be queryable immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_schema_queryable_for_dynamically_created_topic() {
    let prefix = unique_prefix("orch_dynamic");
    let topic_prefix = format!("{prefix}/new_topic");

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();

    let mut orch = Orchestrator::new(
        &session,
        OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: None,
        },
    )
    .unwrap();

    orch.run().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create topic AFTER orchestrator is already running
    orch.create_topic(make_topic_config("new_topic", &topic_prefix))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let schema = fetch_schema_with_timeout(&session, &topic_prefix, Duration::from_secs(2))
        .await
        .expect("dynamically created topic should be queryable");

    assert_eq!(schema.name, "new_topic");

    orch.shutdown().await;
    session.close().await.unwrap();
}

// ==========================================================================
// Phase 2: Storage agent self-warming bootstrap
// ==========================================================================

/// A storage agent that starts AFTER the orchestrator has already published
/// schemas must still converge — by querying the orchestrator, not by relying
/// on pub/sub timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_agent_bootstraps_schema_from_orchestrator() {
    let prefix = unique_prefix("storage_bootstrap");
    let topic_prefix = format!("{prefix}/workflows");

    // Start orchestrator with a topic
    let orch_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let orch_dir = tempfile::tempdir().unwrap();

    let mut orch = Orchestrator::new(
        &orch_session,
        OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: orch_dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: None,
        },
    )
    .unwrap();

    orch.create_topic(make_topic_config("workflows", &topic_prefix))
        .await
        .unwrap();
    orch.run().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Start storage agent AFTER orchestrator — simulates the race scenario.
    // The storage agent missed publish_all_schemas() but should bootstrap by query.
    let agent_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let agent_dir = tempfile::tempdir().unwrap();

    let agent_config = AgentConfig::builder(agent_dir.path().to_path_buf())
        .node_id("test-storage-node")
        .global_prefix(prefix.clone())
        .topic(TopicEntry {
            name: "workflows".into(),
            key_prefix: topic_prefix.clone(),
            num_partitions: 16,
            replication_factor: 1,
        })
        .build()
        .unwrap();

    let mut agent = StorageAgent::start_multi(&agent_session, agent_config)
        .await
        .unwrap();

    // Give storage agent time to bootstrap
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the storage agent persisted the schema
    let schema_store_path = agent_dir.path().join("_schemas");
    let schema_store = SchemaStore::open(&schema_store_path).unwrap();
    let stored_schema = schema_store.get(&topic_prefix).unwrap();

    assert!(
        stored_schema.is_some(),
        "storage agent should have bootstrapped schema from orchestrator"
    );
    let stored = stored_schema.unwrap();
    assert_eq!(stored.name, "workflows");
    assert_eq!(stored.codec, CodecFormat::Postcard);

    agent.shutdown().await.unwrap();
    orch.shutdown().await;
    agent_session.close().await.unwrap();
    orch_session.close().await.unwrap();
}

/// After a storage agent has warmed up from the orchestrator, it must serve
/// schema queries itself — even if the orchestrator goes down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_agent_serves_schema_after_orchestrator_shutdown() {
    let prefix = unique_prefix("storage_serve");
    let topic_prefix = format!("{prefix}/workflows");

    // Start orchestrator, create topic
    let orch_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let orch_dir = tempfile::tempdir().unwrap();

    let mut orch = Orchestrator::new(
        &orch_session,
        OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: orch_dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: None,
        },
    )
    .unwrap();

    orch.create_topic(make_topic_config("workflows", &topic_prefix))
        .await
        .unwrap();
    orch.run().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Start storage agent, let it bootstrap
    let agent_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let agent_dir = tempfile::tempdir().unwrap();

    let agent_config = AgentConfig::builder(agent_dir.path().to_path_buf())
        .node_id("test-storage-node")
        .global_prefix(prefix.clone())
        .topic(TopicEntry {
            name: "workflows".into(),
            key_prefix: topic_prefix.clone(),
            num_partitions: 16,
            replication_factor: 1,
        })
        .build()
        .unwrap();

    let mut agent = StorageAgent::start_multi(&agent_session, agent_config)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shut down orchestrator
    orch.shutdown().await;
    orch_session.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Query from a fresh session — only storage agent can respond now
    let client_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let schema = fetch_schema_with_timeout(&client_session, &topic_prefix, Duration::from_secs(2))
        .await
        .expect("storage agent should serve schema after orchestrator shutdown");

    assert_eq!(schema.name, "workflows");
    assert_eq!(schema.codec, CodecFormat::Postcard);

    agent.shutdown().await.unwrap();
    client_session.close().await.unwrap();
    agent_session.close().await.unwrap();
}

/// Idempotent same-version republish: restarting a storage agent with an
/// existing volume should NOT produce stale-version rejections or warnings.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_schema_idempotent_same_version_republish() {
    let prefix = unique_prefix("storage_idempotent");
    let topic_prefix = format!("{prefix}/workflows");

    // Pre-populate the schema store to simulate a restart with existing volume
    let agent_dir = tempfile::tempdir().unwrap();
    let schema_dir = agent_dir.path().join("_schemas");
    {
        let store = SchemaStore::open(&schema_dir).unwrap();
        let schema = TopicSchema::new(
            &topic_prefix,
            "workflows",
            16,
            CodecFormat::Postcard,
            KeyFormat::Keyed,
            1,
        );
        store.put(&schema).unwrap();
    }

    // The same schema (version=1) should be accepted as a no-op
    let store = SchemaStore::open(&schema_dir).unwrap();
    let same_schema = TopicSchema::new(
        &topic_prefix,
        "workflows",
        16,
        CodecFormat::Postcard,
        KeyFormat::Keyed,
        1,
    );
    let accepted = store.put_if_newer(&same_schema).unwrap();

    // With the fix, same-version same-content should be accepted (idempotent)
    assert!(
        accepted,
        "same-version same-content republish should be accepted as idempotent"
    );
}

/// Same version but different wire contract must be rejected as a conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_schema_rejects_same_version_different_content() {
    let prefix = unique_prefix("storage_conflict");
    let topic_prefix = format!("{prefix}/workflows");

    let agent_dir = tempfile::tempdir().unwrap();
    let schema_dir = agent_dir.path().join("_schemas");
    let store = SchemaStore::open(&schema_dir).unwrap();

    // Store version 1 with Postcard
    let original = TopicSchema::new(
        &topic_prefix,
        "workflows",
        16,
        CodecFormat::Postcard,
        KeyFormat::Keyed,
        1,
    );
    store.put(&original).unwrap();

    // Try to republish version 1 with different codec (Json) — must be rejected
    let conflicting = TopicSchema::new(
        &topic_prefix,
        "workflows",
        16,
        CodecFormat::Json,
        KeyFormat::Keyed,
        1,
    );
    let accepted = store.put_if_newer(&conflicting).unwrap();

    assert!(
        !accepted,
        "same-version different-content should be rejected"
    );

    // Original must still be intact
    let stored = store.get(&topic_prefix).unwrap().unwrap();
    assert_eq!(stored.codec, CodecFormat::Postcard);
}
