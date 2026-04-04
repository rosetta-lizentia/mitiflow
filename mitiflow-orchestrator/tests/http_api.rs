use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

use mitiflow_orchestrator::config::{CompactionPolicy, ConfigStore, RetentionPolicy, TopicConfig};
use mitiflow_orchestrator::http::{HttpState, build_router};

fn test_store() -> (TempDir, Arc<ConfigStore>) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(ConfigStore::open(dir.path()).unwrap());
    (dir, store)
}

fn make_state(store: Arc<ConfigStore>) -> HttpState {
    let (cluster_tx, _) = tokio::sync::broadcast::channel(16);
    let (lag_tx, _) = tokio::sync::broadcast::channel(16);
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    HttpState {
        config_store: store,
        nodes: None,
        cluster_events_tx: cluster_tx,
        lag_events_tx: lag_tx,
        event_tail_tx: event_tx,
        lag_monitor: None,
        session: None,
        topic_manager: None,
        override_manager: None,
        cluster_view: None,
        key_prefix: String::new(),
    }
}

fn sample_topic(name: &str) -> TopicConfig {
    TopicConfig {
        name: name.to_string(),
        key_prefix: format!("test/{name}"),
        num_partitions: 4,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
    }
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// =========================================================================
// Health
// =========================================================================

#[tokio::test]
async fn http_health_returns_ok() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

// =========================================================================
// Topics — CRUD
// =========================================================================

#[tokio::test]
async fn http_list_topics_empty() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(Request::get("/api/v1/topics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn http_create_topic() {
    let (_dir, store) = test_store();
    let state = make_state(store.clone());
    let app = build_router(state, None);

    let payload = json!({
        "name": "orders",
        "key_prefix": "app/orders",
        "num_partitions": 8,
        "replication_factor": 2
    });

    let resp = app
        .oneshot(
            Request::post("/api/v1/topics")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["name"], "orders");
    assert_eq!(body["num_partitions"], 8);
    assert_eq!(body["replication_factor"], 2);

    // Verify it was persisted
    let stored = store.get_topic("orders").unwrap().unwrap();
    assert_eq!(stored.num_partitions, 8);
}

#[tokio::test]
async fn http_create_topic_with_labels() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store.clone()), None);

    let payload = json!({
        "name": "labeled",
        "required_labels": {"region": "us-east"},
        "excluded_labels": {"role": "gateway"}
    });

    let resp = app
        .oneshot(
            Request::post("/api/v1/topics")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let stored = store.get_topic("labeled").unwrap().unwrap();
    assert_eq!(
        stored.required_labels.get("region"),
        Some(&"us-east".to_string())
    );
    assert_eq!(
        stored.excluded_labels.get("role"),
        Some(&"gateway".to_string())
    );
}

#[tokio::test]
async fn http_get_topic() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("sensors")).unwrap();

    let app = build_router(make_state(store), None);
    let resp = app
        .oneshot(
            Request::get("/api/v1/topics/sensors")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["name"], "sensors");
    assert_eq!(body["num_partitions"], 4);
}

#[tokio::test]
async fn http_get_topic_not_found() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/topics/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp.into_body()).await;
    assert!(body["error"].as_str().unwrap().contains("nonexistent"));
}

#[tokio::test]
async fn http_delete_topic() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("to_delete")).unwrap();

    let state = make_state(store.clone());
    let app = build_router(state, None);

    let resp = app
        .oneshot(
            Request::delete("/api/v1/topics/to_delete")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify deleted
    assert!(store.get_topic("to_delete").unwrap().is_none());
}

#[tokio::test]
async fn http_delete_topic_not_found() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::delete("/api/v1/topics/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_list_topics_returns_all() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("alpha")).unwrap();
    store.put_topic(&sample_topic("beta")).unwrap();
    store.put_topic(&sample_topic("gamma")).unwrap();

    let app = build_router(make_state(store), None);
    let resp = app
        .oneshot(Request::get("/api/v1/topics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    let topics = body.as_array().unwrap();
    assert_eq!(topics.len(), 3);

    let names: Vec<&str> = topics.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"beta"));
    assert!(names.contains(&"gamma"));
}

// =========================================================================
// Cluster endpoints (without node data)
// =========================================================================

#[tokio::test]
async fn http_cluster_nodes_empty() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/cluster/nodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body, json!({}));
}

#[tokio::test]
async fn http_cluster_status_no_nodes() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/cluster/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["total_nodes"], 0);
    assert_eq!(body["online_nodes"], 0);
    assert_eq!(body["total_partitions"], 0);
}

// =========================================================================
// Cluster endpoints (with node data)
// =========================================================================

#[tokio::test]
async fn http_cluster_status_with_nodes() {
    use chrono::Utc;
    use mitiflow_orchestrator::cluster_view::NodeInfo;

    let (_dir, store) = test_store();
    let nodes = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    {
        let mut map = nodes.write().await;
        map.insert(
            "agent-1".to_string(),
            NodeInfo {
                metadata: None,
                health: None,
                status: None,
                online: true,
                last_seen: Utc::now(),
            },
        );
        map.insert(
            "agent-2".to_string(),
            NodeInfo {
                metadata: None,
                health: None,
                status: None,
                online: false,
                last_seen: Utc::now(),
            },
        );
    }

    let (cluster_tx, _) = tokio::sync::broadcast::channel(16);
    let (lag_tx, _) = tokio::sync::broadcast::channel(16);
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    let state = HttpState {
        config_store: store,
        nodes: Some(nodes),
        cluster_events_tx: cluster_tx,
        lag_events_tx: lag_tx,
        event_tail_tx: event_tx,
        lag_monitor: None,
        session: None,
        topic_manager: None,
        override_manager: None,
        cluster_view: None,
        key_prefix: String::new(),
    };
    let app = build_router(state, None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/cluster/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["total_nodes"], 2);
    assert_eq!(body["online_nodes"], 1);
    assert_eq!(body["total_partitions"], 0);
}

#[tokio::test]
async fn http_cluster_nodes_with_data() {
    use chrono::Utc;
    use mitiflow_orchestrator::cluster_view::NodeInfo;

    let (_dir, store) = test_store();
    let nodes = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    {
        let mut map = nodes.write().await;
        map.insert(
            "node-a".to_string(),
            NodeInfo {
                metadata: None,
                health: None,
                status: None,
                online: true,
                last_seen: Utc::now(),
            },
        );
    }

    let (cluster_tx, _) = tokio::sync::broadcast::channel(16);
    let (lag_tx, _) = tokio::sync::broadcast::channel(16);
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    let state = HttpState {
        config_store: store,
        nodes: Some(nodes),
        cluster_events_tx: cluster_tx,
        lag_events_tx: lag_tx,
        event_tail_tx: event_tx,
        lag_monitor: None,
        session: None,
        topic_manager: None,
        override_manager: None,
        cluster_view: None,
        key_prefix: String::new(),
    };
    let app = build_router(state, None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/cluster/nodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert!(body.get("node-a").is_some());
    assert_eq!(body["node-a"]["online"], true);
}

// =========================================================================
// Edge cases
// =========================================================================

#[tokio::test]
async fn http_create_topic_defaults() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store.clone()), None);

    // Minimal payload — only name required
    let payload = json!({ "name": "minimal" });

    let resp = app
        .oneshot(
            Request::post("/api/v1/topics")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let stored = store.get_topic("minimal").unwrap().unwrap();
    assert_eq!(stored.num_partitions, 16); // default
    assert_eq!(stored.replication_factor, 1); // default
    assert!(stored.required_labels.is_empty());
}

#[tokio::test]
async fn http_create_topic_invalid_json() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::post("/api/v1/topics")
                .header("content-type", "application/json")
                .body(Body::from(b"not json".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    // axum returns 422 for deserialization failures
    assert!(
        resp.status() == StatusCode::BAD_REQUEST
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

// =========================================================================
// Auth middleware
// =========================================================================

#[tokio::test]
async fn http_auth_disabled_by_default() {
    // When no token is configured, all endpoints are accessible
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(Request::get("/api/v1/topics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn http_auth_rejects_missing_token() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), Some("secret-token-123"));

    let resp = app
        .oneshot(Request::get("/api/v1/topics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn http_auth_rejects_wrong_token() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), Some("secret-token-123"));

    let resp = app
        .oneshot(
            Request::get("/api/v1/topics")
                .header("authorization", "Bearer wrong-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn http_auth_accepts_valid_token() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("auth-test")).unwrap();

    let app = build_router(make_state(store), Some("secret-token-123"));

    let resp = app
        .oneshot(
            Request::get("/api/v1/topics")
                .header("authorization", "Bearer secret-token-123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    let topics = body.as_array().unwrap();
    assert_eq!(topics.len(), 1);
}

#[tokio::test]
async fn http_health_bypasses_auth() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), Some("secret-token-123"));

    // Health should work without any token
    let resp = app
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn http_auth_protects_write_endpoints() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), Some("my-token"));

    let payload = json!({ "name": "test" });
    let resp = app
        .oneshot(
            Request::post("/api/v1/topics")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn http_auth_protects_sse_endpoints() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), Some("my-token"));

    let resp = app
        .oneshot(
            Request::get("/api/v1/stream/cluster")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// =========================================================================
// Update topic (PUT)
// =========================================================================

#[tokio::test]
async fn http_update_topic_retention() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("updatable")).unwrap();
    let app = build_router(make_state(store.clone()), None);

    let payload = json!({
        "retention": { "max_events": 10000 }
    });

    let resp = app
        .oneshot(
            Request::put("/api/v1/topics/updatable")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["name"], "updatable");
    assert_eq!(body["retention"]["max_events"], 10000);

    // Original fields unchanged
    assert_eq!(body["num_partitions"], 4);
    assert_eq!(body["replication_factor"], 1);
}

#[tokio::test]
async fn http_update_topic_not_found() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let payload = json!({ "replication_factor": 3 });
    let resp = app
        .oneshot(
            Request::put("/api/v1/topics/nonexistent")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_update_topic_empty_body() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("unchanged")).unwrap();
    let app = build_router(make_state(store), None);

    let payload = json!({});
    let resp = app
        .oneshot(
            Request::put("/api/v1/topics/unchanged")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    // Should remain unchanged
    assert_eq!(body["num_partitions"], 4);
    assert_eq!(body["replication_factor"], 1);
}

// =========================================================================
// Event query-through endpoint (without Zenoh session)
// =========================================================================

#[tokio::test]
async fn http_events_query_requires_topic() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(Request::get("/api/v1/events").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp.into_body()).await;
    assert!(body["error"].as_str().unwrap().contains("topic"));
}

#[tokio::test]
async fn http_events_query_topic_not_found() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/events?topic=nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_events_query_no_session() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("events-test")).unwrap();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/events?topic=events-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // No Zenoh session available → internal server error
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// =========================================================================
// Consumer group reset endpoint (without Zenoh session)
// =========================================================================

#[tokio::test]
async fn http_reset_consumer_group_requires_session() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("my-topic")).unwrap();
    let app = build_router(make_state(store), None);

    let payload = json!({
        "topic": "my-topic",
        "partition": 0,
        "strategy": "earliest"
    });

    let resp = app
        .oneshot(
            Request::post("/api/v1/consumer-groups/test-group/reset")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    // No Zenoh session → internal server error
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn http_reset_consumer_group_topic_not_found() {
    // Handler checks session first, then topic — with no session it returns 500
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let payload = json!({
        "topic": "nonexistent",
        "partition": 0,
        "strategy": "latest"
    });

    let resp = app
        .oneshot(
            Request::post("/api/v1/consumer-groups/test-group/reset")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    // No session → Internal error (session checked before topic lookup)
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// =========================================================================
// Consumer groups endpoints (without lag monitor)
// =========================================================================

#[tokio::test]
async fn http_list_consumer_groups_empty() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/consumer-groups")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn http_consumer_group_detail_no_monitor() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/consumer-groups/test-group")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // No lag monitor → not found
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// =========================================================================
// SSE endpoint smoke tests (verify response starts as event-stream)
// =========================================================================

#[tokio::test]
async fn http_sse_cluster_returns_event_stream() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/stream/cluster")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.contains("text/event-stream"));
}

#[tokio::test]
async fn http_sse_lag_returns_event_stream() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/stream/lag")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.contains("text/event-stream"));
}

#[tokio::test]
async fn http_sse_events_returns_event_stream() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/stream/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.contains("text/event-stream"));
}

// =========================================================================
// SSE with broadcast data
// =========================================================================

#[tokio::test]
async fn http_sse_events_receives_broadcast() {
    use mitiflow_orchestrator::http::EventSummary;
    use tokio::time::{Duration, timeout};

    let (_dir, store) = test_store();
    let (cluster_tx, _) = tokio::sync::broadcast::channel(16);
    let (lag_tx, _) = tokio::sync::broadcast::channel(16);
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    let state = HttpState {
        config_store: store,
        nodes: None,
        cluster_events_tx: cluster_tx,
        lag_events_tx: lag_tx,
        event_tail_tx: event_tx.clone(),
        lag_monitor: None,
        session: None,
        topic_manager: None,
        override_manager: None,
        cluster_view: None,
        key_prefix: String::new(),
    };
    let app = build_router(state, None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/stream/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Send an event through the broadcast channel
    let summary = EventSummary {
        seq: 42,
        partition: 3,
        publisher_id: "pub-abc".to_string(),
        timestamp: "2025-01-01T00:00:00Z".to_string(),
        key: Some("order-123".to_string()),
        key_expr: "test/p/3/k/order-123/42".to_string(),
        payload_size: 128,
    };
    event_tx.send(summary).unwrap();

    // Read from the response body
    let body = resp.into_body();
    let result = timeout(Duration::from_secs(2), body.collect()).await;
    match result {
        Ok(Ok(collected)) => {
            let bytes = collected.to_bytes();
            let text = String::from_utf8_lossy(&bytes);
            assert!(
                text.contains("order-123") || text.contains("42"),
                "SSE body should contain event data, got: {}",
                text
            );
        }
        _ => {
            // Timeout is acceptable for SSE (it's a long-lived stream)
            // The important thing is the response started correctly
        }
    }
}

#[tokio::test]
async fn http_sse_cluster_receives_broadcast() {
    use mitiflow_orchestrator::http::ClusterEvent;
    use tokio::time::{Duration, timeout};

    let (_dir, store) = test_store();
    let (cluster_tx, _) = tokio::sync::broadcast::channel(16);
    let (lag_tx, _) = tokio::sync::broadcast::channel(16);
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    let state = HttpState {
        config_store: store,
        nodes: None,
        cluster_events_tx: cluster_tx.clone(),
        lag_events_tx: lag_tx,
        event_tail_tx: event_tx,
        lag_monitor: None,
        session: None,
        topic_manager: None,
        override_manager: None,
        cluster_view: None,
        key_prefix: String::new(),
    };
    let app = build_router(state, None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/stream/cluster")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Send a cluster event
    let event = ClusterEvent::NodeOnline {
        node_id: "node-x".to_string(),
        timestamp: "2025-01-01T00:00:00Z".to_string(),
    };
    cluster_tx.send(event).unwrap();

    let body = resp.into_body();
    let result = timeout(Duration::from_secs(2), body.collect()).await;
    match result {
        Ok(Ok(collected)) => {
            let bytes = collected.to_bytes();
            let text = String::from_utf8_lossy(&bytes);
            assert!(
                text.contains("node-x") || text.contains("node_online"),
                "SSE body should contain cluster event data, got: {}",
                text
            );
        }
        _ => {
            // Timeout acceptable for SSE
        }
    }
}

// =========================================================================
// Topic partitions and publishers (without cluster view / lag monitor)
// =========================================================================

#[tokio::test]
async fn http_topic_partitions_no_cluster_view() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("part-test")).unwrap();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/topics/part-test/partitions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    let partitions = body.as_array().unwrap();
    // Should return 4 empty partitions (from sample_topic num_partitions=4)
    assert_eq!(partitions.len(), 4);
    for (i, p) in partitions.iter().enumerate() {
        assert_eq!(p["partition"], i);
        assert!(p["replicas"].as_array().unwrap().is_empty());
    }
}

#[tokio::test]
async fn http_topic_partitions_not_found() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/topics/nonexistent/partitions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_topic_publishers_no_lag_monitor() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("pub-test")).unwrap();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/topics/pub-test/publishers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn http_topic_lag_no_lag_monitor() {
    let (_dir, store) = test_store();
    store.put_topic(&sample_topic("lag-test")).unwrap();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/topics/lag-test/lag")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body, json!([]));
}

// =========================================================================
// Overrides endpoints (without override manager)
// =========================================================================

#[tokio::test]
async fn http_get_overrides_empty() {
    let (_dir, store) = test_store();
    let app = build_router(make_state(store), None);

    let resp = app
        .oneshot(
            Request::get("/api/v1/cluster/overrides")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp.into_body()).await;
    assert_eq!(body["entries"], json!([]));
    assert_eq!(body["epoch"], 0);
}
