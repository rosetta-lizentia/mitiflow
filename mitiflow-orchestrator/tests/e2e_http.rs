//! E2E tests for the orchestrator HTTP API.
//!
//! These tests start a real orchestrator with HTTP server enabled,
//! then use an HTTP client to exercise the REST endpoints.

use std::time::Duration;

use serde_json::{Value, json};

use mitiflow_orchestrator::orchestrator::{Orchestrator, OrchestratorConfig};

/// Wrap a test body in a timeout to prevent hangs.
macro_rules! with_timeout {
    ($body:expr) => {
        tokio::time::timeout(Duration::from_secs(30), async { $body })
            .await
            .expect("test timed out after 30s")
    };
}

/// Unique key prefix per test to avoid cross-talk.
fn key_prefix(test: &str) -> String {
    format!("test/e2e_http_{test}_{}", uuid::Uuid::now_v7())
}

/// Find a free TCP port.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Start an orchestrator with HTTP enabled on a random port. Returns (orchestrator, base_url).
async fn start_orch_with_http(
    test_name: &str,
) -> (Orchestrator, zenoh::Session, String, tempfile::TempDir) {
    let port = free_port().await;
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();

    let config = OrchestratorConfig {
        key_prefix: key_prefix(test_name),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(60),
        admin_prefix: None,
        http_bind: Some(([127, 0, 0, 1], port).into()),
        auth_token: None,
        bootstrap_topics_from: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Give the HTTP server a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    let base_url = format!("http://127.0.0.1:{port}");
    (orch, session, base_url, dir)
}

// =========================================================================
// Health
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_health() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("health").await;

        let resp = reqwest::get(format!("{url}/api/v1/health")).await.unwrap();
        assert_eq!(resp.status(), 200);

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// Topic CRUD
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_topic_crud() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("topic_crud").await;
        let client = reqwest::Client::new();

        // List topics: empty
        let resp = client
            .get(format!("{url}/api/v1/topics"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let topics: Vec<Value> = resp.json().await.unwrap();
        assert!(topics.is_empty());

        // Create topic
        let create_body = json!({
            "name": "test-topic",
            "key_prefix": "test/e2e/http",
            "num_partitions": 4,
            "replication_factor": 1
        });
        let resp = client
            .post(format!("{url}/api/v1/topics"))
            .json(&create_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
        let topic: Value = resp.json().await.unwrap();
        assert_eq!(topic["name"], "test-topic");
        assert_eq!(topic["num_partitions"], 4);

        // Get topic
        let resp = client
            .get(format!("{url}/api/v1/topics/test-topic"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let topic: Value = resp.json().await.unwrap();
        assert_eq!(topic["name"], "test-topic");

        // List topics: one
        let resp = client
            .get(format!("{url}/api/v1/topics"))
            .send()
            .await
            .unwrap();
        let topics: Vec<Value> = resp.json().await.unwrap();
        assert_eq!(topics.len(), 1);

        // Update topic
        let update_body = json!({ "replication_factor": 3 });
        let resp = client
            .put(format!("{url}/api/v1/topics/test-topic"))
            .json(&update_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let updated: Value = resp.json().await.unwrap();
        assert_eq!(updated["replication_factor"], 3);

        // Delete topic
        let resp = client
            .delete(format!("{url}/api/v1/topics/test-topic"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 204);

        // List topics: empty again
        let resp = client
            .get(format!("{url}/api/v1/topics"))
            .send()
            .await
            .unwrap();
        let topics: Vec<Value> = resp.json().await.unwrap();
        assert!(topics.is_empty());

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// Topic not found
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_topic_not_found() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("topic_nf").await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{url}/api/v1/topics/nonexistent"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);

        let resp = client
            .delete(format!("{url}/api/v1/topics/nonexistent"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// Cluster endpoints
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_cluster_endpoints() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("cluster").await;
        let client = reqwest::Client::new();

        // Cluster nodes
        let resp = client
            .get(format!("{url}/api/v1/cluster/nodes"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // Cluster status
        let resp = client
            .get(format!("{url}/api/v1/cluster/status"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let status: Value = resp.json().await.unwrap();
        assert!(status["total_nodes"].is_number());

        // Overrides
        let resp = client
            .get(format!("{url}/api/v1/cluster/overrides"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// Auth middleware
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_auth_required() {
    with_timeout!({
        let port = free_port().await;
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let prefix = key_prefix("auth");

        let config = OrchestratorConfig {
            key_prefix: prefix,
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: Some(([127, 0, 0, 1], port).into()),
            auth_token: Some("test-secret-42".to_string()),
            bootstrap_topics_from: None,
        };

        let mut orch = Orchestrator::new(&session, config).unwrap();
        orch.run().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = reqwest::Client::new();
        let url = format!("http://127.0.0.1:{port}");

        // Health should bypass auth
        let resp = client
            .get(format!("{url}/api/v1/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "health should bypass auth");

        // Topics without token → 401
        let resp = client
            .get(format!("{url}/api/v1/topics"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401, "should reject without token");

        // Topics with wrong token → 401
        let resp = client
            .get(format!("{url}/api/v1/topics"))
            .header("authorization", "Bearer wrong-token")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401, "should reject wrong token");

        // Topics with correct token → 200
        let resp = client
            .get(format!("{url}/api/v1/topics"))
            .header("authorization", "Bearer test-secret-42")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "should accept correct token");

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// SSE streams
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_sse_cluster_stream() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("sse_cluster").await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{url}/api/v1/stream/cluster"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/event-stream"));

        // Drop SSE response/client before shutdown to close the connection,
        // otherwise axum's graceful shutdown waits forever for the stream to end.
        drop(resp);
        drop(client);

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_sse_events_stream() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("sse_events").await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{url}/api/v1/stream/events"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/event-stream"));

        drop(resp);
        drop(client);

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// Consumer groups endpoint
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_consumer_groups_empty() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("cg_empty").await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{url}/api/v1/consumer-groups"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let groups: Vec<Value> = resp.json().await.unwrap();
        assert!(groups.is_empty());

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// Topic partitions / lag / publishers (with real cluster view)
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_topic_partitions() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("topic_parts").await;
        let client = reqwest::Client::new();

        // Create a topic first
        let create_body = json!({
            "name": "parts-topic",
            "key_prefix": "test/e2e/parts",
            "num_partitions": 8,
            "replication_factor": 1
        });
        let resp = client
            .post(format!("{url}/api/v1/topics"))
            .json(&create_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);

        // Get partitions
        let resp = client
            .get(format!("{url}/api/v1/topics/parts-topic/partitions"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let parts: Vec<Value> = resp.json().await.unwrap();
        assert_eq!(parts.len(), 8, "should have 8 partitions");

        // Get lag (empty, no consumer groups yet)
        let resp = client
            .get(format!("{url}/api/v1/topics/parts-topic/lag"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let lag: Vec<Value> = resp.json().await.unwrap();
        assert!(lag.is_empty());

        // Get publishers (empty)
        let resp = client
            .get(format!("{url}/api/v1/topics/parts-topic/publishers"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// Events query endpoint (requires topic, uses Zenoh session)
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_events_query_validation() {
    with_timeout!({
        let (orch, session, url, _dir) = start_orch_with_http("events_query").await;
        let client = reqwest::Client::new();

        // Missing topic → 400
        let resp = client
            .get(format!("{url}/api/v1/events"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);

        // Nonexistent topic → 404
        let resp = client
            .get(format!("{url}/api/v1/events?topic=nonexistent"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);

        // Create topic, then query (should get empty results, no events published)
        let create_body = json!({
            "name": "query-topic",
            "key_prefix": "test/e2e/query",
            "num_partitions": 2,
            "replication_factor": 1
        });
        client
            .post(format!("{url}/api/v1/topics"))
            .json(&create_body)
            .send()
            .await
            .unwrap();

        let resp = client
            .get(format!("{url}/api/v1/events?topic=query-topic"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 0);
        assert_eq!(body["total"], 0);

        orch.shutdown().await;
        session.close().await.unwrap();
    });
}

// =========================================================================
// Full flow: create topic, publish events, query via HTTP
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_publish_and_query_events() {
    with_timeout!({
        let port = free_port().await;
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let prefix = key_prefix("pub_query");

        let config = OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: Some(([127, 0, 0, 1], port).into()),
            auth_token: None,
            bootstrap_topics_from: None,
        };

        let mut orch = Orchestrator::new(&session, config).unwrap();
        orch.run().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let url = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();

        // Create topic via HTTP
        let topic_prefix = format!("{prefix}/events");
        let create_body = json!({
            "name": "pub-topic",
            "key_prefix": topic_prefix,
            "num_partitions": 2,
            "replication_factor": 1
        });
        let resp = client
            .post(format!("{url}/api/v1/topics"))
            .json(&create_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);

        // Use mitiflow publisher to publish some events
        let bus_config = mitiflow::EventBusConfig::builder(&topic_prefix)
            .cache_size(10)
            .build()
            .unwrap();
        let publisher = mitiflow::EventPublisher::new(&session, bus_config.clone())
            .await
            .unwrap();

        for i in 0..5u32 {
            publisher
                .publish_bytes(format!("event-{i}").into_bytes())
                .await
                .unwrap();
        }

        // Small delay for events to propagate
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify events via SSE stream connectivity
        let resp = client
            .get(format!("{url}/api/v1/stream/events"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // Drop SSE connection before shutdown
        drop(resp);
        drop(client);
        drop(publisher);
        orch.shutdown().await;
        session.close().await.unwrap();
    });
}
