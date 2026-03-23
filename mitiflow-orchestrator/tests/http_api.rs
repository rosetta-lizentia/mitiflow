use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

use mitiflow_orchestrator::config::{
    CompactionPolicy, ConfigStore, RetentionPolicy, TopicConfig,
};
use mitiflow_orchestrator::http::{build_router, HttpState};

fn test_store() -> (TempDir, Arc<ConfigStore>) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(ConfigStore::open(dir.path()).unwrap());
    (dir, store)
}

fn make_state(store: Arc<ConfigStore>) -> HttpState {
    HttpState {
        config_store: store,
        nodes: None,
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
    let app = build_router(make_state(store));

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
    let app = build_router(make_state(store));

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
    let app = build_router(state);

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
    let app = build_router(make_state(store.clone()));

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

    let app = build_router(make_state(store));
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
    let app = build_router(make_state(store));

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
    let app = build_router(state);

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
    let app = build_router(make_state(store));

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

    let app = build_router(make_state(store));
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
    let app = build_router(make_state(store));

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
    let app = build_router(make_state(store));

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
    use mitiflow_orchestrator::cluster_view::NodeInfo;
    use chrono::Utc;

    let (_dir, store) = test_store();
    let nodes = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    {
        let mut map = nodes.write().await;
        map.insert("agent-1".to_string(), NodeInfo {
            metadata: None,
            health: None,
            status: None,
            online: true,
            last_seen: Utc::now(),
        });
        map.insert("agent-2".to_string(), NodeInfo {
            metadata: None,
            health: None,
            status: None,
            online: false,
            last_seen: Utc::now(),
        });
    }

    let state = HttpState {
        config_store: store,
        nodes: Some(nodes),
    };
    let app = build_router(state);

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
    use mitiflow_orchestrator::cluster_view::NodeInfo;
    use chrono::Utc;

    let (_dir, store) = test_store();
    let nodes = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    {
        let mut map = nodes.write().await;
        map.insert("node-a".to_string(), NodeInfo {
            metadata: None,
            health: None,
            status: None,
            online: true,
            last_seen: Utc::now(),
        });
    }

    let state = HttpState {
        config_store: store,
        nodes: Some(nodes),
    };
    let app = build_router(state);

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
    let app = build_router(make_state(store.clone()));

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
    let app = build_router(make_state(store));

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
