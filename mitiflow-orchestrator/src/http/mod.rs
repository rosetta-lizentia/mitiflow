//! Embedded HTTP API for the orchestrator.
//!
//! ## REST endpoints
//!
//! **Read**
//! - `GET  /api/v1/health`                         — health check
//! - `GET  /api/v1/topics`                          — list topics
//! - `POST /api/v1/topics`                          — create topic
//! - `GET  /api/v1/topics/{name}`                   — get topic
//! - `PUT  /api/v1/topics/{name}`                   — update topic
//! - `DELETE /api/v1/topics/{name}`                  — delete topic
//! - `GET  /api/v1/topics/{name}/partitions`         — partition assignments
//! - `GET  /api/v1/topics/{name}/lag`                — consumer group lag for topic
//! - `GET  /api/v1/topics/{name}/publishers`         — active publishers
//! - `GET  /api/v1/topics/{name}/clients`            — live publishers & consumers (liveliness)
//! - `GET  /api/v1/clients`                           — live clients across all topics
//! - `GET  /api/v1/cluster/nodes`                    — list cluster nodes
//! - `GET  /api/v1/cluster/status`                   — cluster status summary
//! - `GET  /api/v1/cluster/overrides`                — current override table
//! - `GET  /api/v1/consumer-groups`                  — list consumer groups
//! - `GET  /api/v1/consumer-groups/{id}`             — consumer group detail
//! - `GET  /api/v1/events`                            — query events via Zenoh store
//!
//! **Write**
//! - `POST /api/v1/cluster/overrides`                — add override entries
//! - `DELETE /api/v1/cluster/overrides`               — clear overrides
//! - `POST /api/v1/cluster/nodes/{id}/drain`          — drain node
//! - `POST /api/v1/cluster/nodes/{id}/undrain`        — undrain node
//! - `POST /api/v1/consumer-groups/{id}/reset`        — reset group offsets
//!
//! **SSE**
//! - `GET /api/v1/stream/cluster`                     — node state changes
//! - `GET /api/v1/stream/lag`                         — lag reports (optional `?group=`)
//! - `GET /api/v1/stream/events`                      — live event tail

mod cluster;
mod consumer_groups;
mod events;
mod sse;
mod topics;
pub mod types;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::info;
use zenoh::Session;

use crate::cluster_view::{ClusterView, NodeInfo};
use crate::config::ConfigStore;
use crate::lag::LagReport;
use crate::override_manager::OverrideManager;
use crate::topic_manager::TopicManager;

// Re-export public types used by other crates/modules.
pub use types::{ClusterEvent, EventSummary};

// =========================================================================
// Shared state
// =========================================================================

/// Shared state for HTTP handlers (read + write + SSE).
#[derive(Clone)]
pub struct HttpState {
    pub config_store: Arc<ConfigStore>,
    pub nodes: Option<Arc<RwLock<HashMap<String, NodeInfo>>>>,
    // SSE broadcast senders
    pub cluster_events_tx: broadcast::Sender<ClusterEvent>,
    pub lag_events_tx: broadcast::Sender<LagReport>,
    pub event_tail_tx: broadcast::Sender<EventSummary>,
    // Subsystem handles for REST queries
    pub lag_monitor: Option<Arc<crate::lag::LagMonitor>>,
    pub session: Option<Session>,
    pub topic_manager: Option<Arc<RwLock<TopicManager>>>,
    pub override_manager: Option<Arc<OverrideManager>>,
    pub cluster_view: Option<Arc<ClusterView>>,
    pub key_prefix: String,
}

// =========================================================================
// Router
// =========================================================================

/// Build the axum router with all API routes.
///
/// If `auth_token` is `Some`, all API routes (except health) require
/// `Authorization: Bearer <token>`. Pass `None` to disable auth (local dev).
pub fn build_router(state: HttpState, auth_token: Option<&str>) -> Router {
    // Public routes (always accessible)
    let public = Router::new().route("/api/v1/health", get(health));

    // Protected API routes
    let api = Router::new()
        // Topics CRUD
        .route(
            "/api/v1/topics",
            get(topics::list_topics).post(topics::create_topic),
        )
        .route(
            "/api/v1/topics/{name}",
            get(topics::get_topic)
                .put(topics::update_topic)
                .delete(topics::delete_topic),
        )
        // Topic detail
        .route(
            "/api/v1/topics/{name}/partitions",
            get(topics::topic_partitions),
        )
        .route("/api/v1/topics/{name}/lag", get(topics::topic_lag))
        .route(
            "/api/v1/topics/{name}/publishers",
            get(topics::topic_publishers),
        )
        .route("/api/v1/topics/{name}/clients", get(topics::topic_clients))
        .route("/api/v1/clients", get(topics::all_clients))
        // Cluster
        .route("/api/v1/cluster/nodes", get(cluster::cluster_nodes))
        .route(
            "/api/v1/cluster/nodes/{id}/partitions",
            get(cluster::node_partitions),
        )
        .route("/api/v1/cluster/status", get(cluster::cluster_status))
        .route(
            "/api/v1/cluster/overrides",
            get(cluster::get_overrides)
                .post(cluster::add_overrides)
                .delete(cluster::clear_overrides),
        )
        // Drain / undrain
        .route(
            "/api/v1/cluster/nodes/{id}/drain",
            post(cluster::drain_node),
        )
        .route(
            "/api/v1/cluster/nodes/{id}/undrain",
            post(cluster::undrain_node),
        )
        // Consumer groups
        .route(
            "/api/v1/consumer-groups",
            get(consumer_groups::list_consumer_groups),
        )
        .route(
            "/api/v1/consumer-groups/{id}",
            get(consumer_groups::get_consumer_group),
        )
        .route(
            "/api/v1/consumer-groups/{id}/reset",
            post(consumer_groups::reset_consumer_group),
        )
        // Event query-through
        .route("/api/v1/events", get(events::query_events))
        // SSE streams
        .route("/api/v1/stream/cluster", get(sse::sse_cluster))
        .route("/api/v1/stream/lag", get(sse::sse_lag))
        .route("/api/v1/stream/events", get(sse::sse_events));

    // Apply bearer auth if configured
    let api = if let Some(token) = auth_token {
        let expected = format!("Bearer {token}");
        api.layer(middleware::from_fn(
            move |req: axum::http::Request<axum::body::Body>, next: Next| {
                let expected = expected.clone();
                async move {
                    match req.headers().get(header::AUTHORIZATION) {
                        Some(v) if v.as_bytes() == expected.as_bytes() => next.run(req).await,
                        _ => StatusCode::UNAUTHORIZED.into_response(),
                    }
                }
            },
        ))
    } else {
        api
    };

    let router = public.merge(api).with_state(state);

    // When `ui` feature is enabled, add SPA fallback
    #[cfg(feature = "ui")]
    let router = router.fallback(get(serve_ui));

    router
}

/// Start the HTTP server.
pub async fn start_http(
    state: HttpState,
    bind_addr: SocketAddr,
    cancel: CancellationToken,
    auth_token: Option<String>,
) -> tokio::task::JoinHandle<()> {
    let app = build_router(state, auth_token.as_deref());
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
// Health check
// =========================================================================

async fn health() -> StatusCode {
    StatusCode::OK
}

// =========================================================================
// Static SPA serving (behind `ui` feature)
// =========================================================================

#[cfg(feature = "ui")]
mod ui_assets {
    use rust_embed::Embed;

    #[derive(Embed)]
    #[folder = "../mitiflow-ui/build/"]
    pub struct Assets;
}

#[cfg(feature = "ui")]
async fn serve_ui(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try exact file first (JS, CSS, images, etc.)
    if let Some(file) = ui_assets::Assets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, mime.as_ref().to_string())],
            file.data.to_vec(),
        )
            .into_response();
    }

    // Fallback to index.html for SPA client-side routing
    match ui_assets::Assets::get("index.html") {
        Some(file) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/html".to_string())],
            file.data.to_vec(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// =========================================================================
// Error type
// =========================================================================

pub(crate) enum AppError {
    Internal(String),
    NotFound(String),
    BadRequest(String),
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
            AppError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
        }
    }
}
