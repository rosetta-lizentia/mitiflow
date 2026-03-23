//! Embedded HTTP API for the orchestrator.
//!
//! Provides REST endpoints mirroring the Zenoh queryable admin API:
//!
//! - `GET  /api/v1/topics`           — list all topics
//! - `POST /api/v1/topics`           — create a topic
//! - `GET  /api/v1/topics/:name`     — get a specific topic
//! - `DELETE /api/v1/topics/:name`   — delete a topic
//! - `GET  /api/v1/cluster/nodes`    — list cluster nodes
//! - `GET  /api/v1/cluster/status`   — cluster status summary
//! - `GET  /api/v1/health`           — health check

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::cluster_view::NodeInfo;
use crate::config::{ConfigStore, TopicConfig};

/// Shared state for HTTP handlers.
#[derive(Clone)]
pub struct HttpState {
    pub config_store: Arc<ConfigStore>,
    pub nodes: Option<Arc<RwLock<std::collections::HashMap<String, NodeInfo>>>>,
}

/// Request body for creating a topic.
#[derive(Debug, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default = "default_partitions")]
    pub num_partitions: u32,
    #[serde(default = "default_rf")]
    pub replication_factor: u32,
    #[serde(default)]
    pub required_labels: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub excluded_labels: std::collections::HashMap<String, String>,
}

fn default_partitions() -> u32 { 16 }
fn default_rf() -> u32 { 1 }

/// Cluster status summary.
#[derive(Debug, Serialize)]
pub struct ClusterStatus {
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub total_partitions: usize,
}

/// Build the axum router with all API routes.
pub fn build_router(state: HttpState) -> Router {
    Router::new()
        .route("/api/v1/topics", get(list_topics).post(create_topic))
        .route("/api/v1/topics/{name}", get(get_topic).delete(delete_topic))
        .route("/api/v1/cluster/nodes", get(cluster_nodes))
        .route("/api/v1/cluster/status", get(cluster_status))
        .route("/api/v1/health", get(health))
        .with_state(state)
}

/// Start the HTTP server.
///
/// Returns a `JoinHandle` for the server task.
pub async fn start_http(
    config_store: Arc<ConfigStore>,
    nodes: Option<Arc<RwLock<std::collections::HashMap<String, NodeInfo>>>>,
    bind_addr: SocketAddr,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let state = HttpState {
        config_store,
        nodes,
    };

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    info!(addr = %bind_addr, "HTTP API started");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(cancel.cancelled_owned())
            .await
            .ok();
    })
}

// =========================================================================
// Handlers
// =========================================================================

async fn list_topics(
    State(state): State<HttpState>,
) -> Result<Json<Vec<TopicConfig>>, AppError> {
    let topics = state.config_store.list_topics()?;
    Ok(Json(topics))
}

async fn create_topic(
    State(state): State<HttpState>,
    Json(req): Json<CreateTopicRequest>,
) -> Result<(StatusCode, Json<TopicConfig>), AppError> {
    let cfg = TopicConfig {
        name: req.name,
        key_prefix: req.key_prefix,
        num_partitions: req.num_partitions,
        replication_factor: req.replication_factor,
        retention: crate::config::RetentionPolicy::default(),
        compaction: crate::config::CompactionPolicy::default(),
        required_labels: req.required_labels,
        excluded_labels: req.excluded_labels,
    };
    state.config_store.put_topic(&cfg)?;
    Ok((StatusCode::CREATED, Json(cfg)))
}

async fn get_topic(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<TopicConfig>, AppError> {
    match state.config_store.get_topic(&name)? {
        Some(cfg) => Ok(Json(cfg)),
        None => Err(AppError::NotFound(format!("topic '{name}' not found"))),
    }
}

async fn delete_topic(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    if state.config_store.delete_topic(&name)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound(format!("topic '{name}' not found")))
    }
}

async fn cluster_nodes(
    State(state): State<HttpState>,
) -> Result<impl IntoResponse, AppError> {
    match state.nodes {
        Some(ref nodes) => {
            let nodes = nodes.read().await;
            Ok(Json(serde_json::to_value(&*nodes)?))
        }
        None => Ok(Json(serde_json::json!({}))),
    }
}

async fn cluster_status(
    State(state): State<HttpState>,
) -> Result<Json<ClusterStatus>, AppError> {
    match state.nodes {
        Some(ref nodes) => {
            let nodes = nodes.read().await;
            let online = nodes.values().filter(|n| n.online).count();
            let total_partitions: usize = nodes
                .values()
                .filter_map(|n| n.status.as_ref())
                .map(|s| s.partitions.len())
                .sum();
            Ok(Json(ClusterStatus {
                total_nodes: nodes.len(),
                online_nodes: online,
                total_partitions,
            }))
        }
        None => Ok(Json(ClusterStatus {
            total_nodes: 0,
            online_nodes: 0,
            total_partitions: 0,
        })),
    }
}

async fn health() -> StatusCode {
    StatusCode::OK
}

// =========================================================================
// Error type
// =========================================================================

enum AppError {
    Internal(String),
    NotFound(String),
}

impl From<Box<dyn std::error::Error + Send + Sync>> for AppError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        AppError::Internal(e.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::Internal(e.to_string())
    }
}

impl From<fjall::Error> for AppError {
    fn from(e: fjall::Error) -> Self {
        AppError::Internal(e.to_string())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            AppError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
        }
    }
}
