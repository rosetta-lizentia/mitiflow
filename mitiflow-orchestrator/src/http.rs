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

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use zenoh::Session;

use crate::cluster_view::{ClusterView, NodeInfo};
use crate::config::{CompactionPolicy, ConfigStore, RetentionPolicy, TopicConfig};
use crate::lag::LagReport;
use crate::override_manager::OverrideManager;
use crate::topic_manager::TopicManager;

// =========================================================================
// Event types for SSE broadcasts
// =========================================================================

/// Events emitted by ClusterView for SSE streaming.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClusterEvent {
    NodeOnline {
        node_id: String,
        timestamp: String,
    },
    NodeOffline {
        node_id: String,
        timestamp: String,
    },
    NodeHealth {
        node_id: String,
        #[serde(flatten)]
        data: serde_json::Value,
    },
    NodeStatus {
        node_id: String,
        #[serde(flatten)]
        data: serde_json::Value,
    },
}

/// Lightweight event summary for the live tail SSE stream.
#[derive(Debug, Clone, Serialize)]
pub struct EventSummary {
    pub seq: u64,
    pub partition: u32,
    pub publisher_id: String,
    pub timestamp: String,
    pub key: Option<String>,
    pub key_expr: String,
    pub payload_size: usize,
}

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
// Request / response types
// =========================================================================

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
    pub required_labels: HashMap<String, String>,
    #[serde(default)]
    pub excluded_labels: HashMap<String, String>,
}

fn default_partitions() -> u32 {
    16
}
fn default_rf() -> u32 {
    1
}

/// Request body for updating a topic (partial).
#[derive(Debug, Deserialize)]
pub struct UpdateTopicRequest {
    pub replication_factor: Option<u32>,
    pub retention: Option<RetentionPolicy>,
    pub compaction: Option<CompactionPolicy>,
    pub required_labels: Option<HashMap<String, String>>,
    pub excluded_labels: Option<HashMap<String, String>>,
}

/// Cluster status summary.
#[derive(Debug, Serialize)]
pub struct ClusterStatus {
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub total_partitions: usize,
}

/// Partition info for topic detail.
#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub partition: u32,
    pub replicas: Vec<ReplicaInfo>,
}

/// Per-replica assignment info.
#[derive(Debug, Serialize)]
pub struct ReplicaInfo {
    pub replica: u32,
    pub node_id: String,
    pub state: String,
    pub source: String,
}

/// Publisher info derived from watermarks.
#[derive(Debug, Serialize)]
pub struct PublisherInfo {
    pub publisher_id: String,
    pub partitions: HashMap<u32, u64>,
}

/// Topic lag per consumer group.
#[derive(Debug, Serialize)]
pub struct TopicLagSummary {
    pub group_id: String,
    pub partitions: Vec<LagReport>,
    pub total_lag: u64,
}

/// Consumer group summary.
#[derive(Debug, Serialize)]
pub struct ConsumerGroupSummary {
    pub group_id: String,
    pub total_lag: u64,
}

/// Drain request.
#[derive(Debug, Deserialize)]
pub struct DrainRequest {
    #[serde(default = "default_rf")]
    pub replication_factor: u32,
}

/// Drain response.
#[derive(Debug, Serialize)]
pub struct DrainResponse {
    pub node_id: String,
    pub overrides: Vec<mitiflow_agent::OverrideEntry>,
}

/// Add overrides request.
#[derive(Debug, Deserialize)]
pub struct AddOverridesRequest {
    pub entries: Vec<OverrideEntryRequest>,
    pub ttl_seconds: Option<u64>,
}

/// Override entry in a request.
#[derive(Debug, Deserialize)]
pub struct OverrideEntryRequest {
    pub partition: u32,
    pub replica: u32,
    pub node_id: String,
    #[serde(default)]
    pub reason: String,
}

/// Query params for SSE lag stream.
#[derive(Debug, Deserialize)]
pub struct LagStreamParams {
    pub group: Option<String>,
}

/// Query params for SSE event stream.
#[derive(Debug, Deserialize)]
pub struct EventStreamParams {
    pub topic: Option<String>,
    pub partition: Option<u32>,
}

/// Query params for the event query-through endpoint.
#[derive(Debug, Deserialize)]
pub struct EventQueryParams {
    pub topic: Option<String>,
    pub partition: Option<u32>,
    pub after_seq: Option<u64>,
    pub before_seq: Option<u64>,
    pub after_time: Option<String>,
    pub before_time: Option<String>,
    pub publisher_id: Option<String>,
    pub key: Option<String>,
    pub limit: Option<usize>,
}

/// Request body for consumer group offset reset.
#[derive(Debug, Deserialize)]
pub struct ResetOffsetsRequest {
    pub topic: String,
    pub partition: u32,
    pub strategy: ResetStrategy,
}

/// Offset reset strategy.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetStrategy {
    Earliest,
    Latest,
    ToSeq(u64),
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
    let public = Router::new()
        .route("/api/v1/health", get(health));

    // Protected API routes
    let api = Router::new()
        // Topics CRUD
        .route("/api/v1/topics", get(list_topics).post(create_topic))
        .route(
            "/api/v1/topics/{name}",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
        // Topic detail
        .route("/api/v1/topics/{name}/partitions", get(topic_partitions))
        .route("/api/v1/topics/{name}/lag", get(topic_lag))
        .route("/api/v1/topics/{name}/publishers", get(topic_publishers))
        // Cluster
        .route("/api/v1/cluster/nodes", get(cluster_nodes))
        .route("/api/v1/cluster/status", get(cluster_status))
        .route(
            "/api/v1/cluster/overrides",
            get(get_overrides)
                .post(add_overrides)
                .delete(clear_overrides),
        )
        // Drain / undrain
        .route("/api/v1/cluster/nodes/{id}/drain", post(drain_node))
        .route("/api/v1/cluster/nodes/{id}/undrain", post(undrain_node))
        // Consumer groups
        .route("/api/v1/consumer-groups", get(list_consumer_groups))
        .route(
            "/api/v1/consumer-groups/{id}",
            get(get_consumer_group),
        )
        .route(
            "/api/v1/consumer-groups/{id}/reset",
            post(reset_consumer_group),
        )
        // Event query-through
        .route("/api/v1/events", get(query_events))
        // SSE streams
        .route("/api/v1/stream/cluster", get(sse_cluster))
        .route("/api/v1/stream/lag", get(sse_lag))
        .route("/api/v1/stream/events", get(sse_events));

    // Apply bearer auth if configured
    let api = if let Some(token) = auth_token {
        let expected = format!("Bearer {token}");
        api.layer(middleware::from_fn(move |req: axum::http::Request<axum::body::Body>, next: Next| {
            let expected = expected.clone();
            async move {
                match req.headers().get(header::AUTHORIZATION) {
                    Some(v) if v.as_bytes() == expected.as_bytes() => {
                        next.run(req).await
                    }
                    _ => StatusCode::UNAUTHORIZED.into_response(),
                }
            }
        }))
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
            [(
                axum::http::header::CONTENT_TYPE,
                "text/html".to_string(),
            )],
            file.data.to_vec(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// =========================================================================
// SSE handlers
// =========================================================================

async fn sse_cluster(
    State(state): State<HttpState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.cluster_events_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(event) => {
            let event_type = match &event {
                ClusterEvent::NodeOnline { .. } => "node_online",
                ClusterEvent::NodeOffline { .. } => "node_offline",
                ClusterEvent::NodeHealth { .. } => "node_health",
                ClusterEvent::NodeStatus { .. } => "node_status",
            };
            serde_json::to_string(&event)
                .ok()
                .map(|data| Ok(Event::default().event(event_type).data(data)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn sse_lag(
    State(state): State<HttpState>,
    Query(params): Query<LagStreamParams>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.lag_events_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
        Ok(report) => {
            if let Some(ref group) = params.group {
                if &report.group_id != group {
                    return None;
                }
            }
            serde_json::to_string(&report)
                .ok()
                .map(|data| Ok(Event::default().event("lag").data(data)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn sse_events(
    State(state): State<HttpState>,
    Query(params): Query<EventStreamParams>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.event_tail_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
        Ok(summary) => {
            if let Some(p) = params.partition {
                if summary.partition != p {
                    return None;
                }
            }
            serde_json::to_string(&summary)
                .ok()
                .map(|data| Ok(Event::default().event("message").data(data)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

// =========================================================================
// Read handlers
// =========================================================================

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn list_topics(State(state): State<HttpState>) -> Result<Json<Vec<TopicConfig>>, AppError> {
    let topics = state.config_store.list_topics()?;
    Ok(Json(topics))
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

async fn cluster_nodes(State(state): State<HttpState>) -> Result<impl IntoResponse, AppError> {
    match state.nodes {
        Some(ref nodes) => {
            let nodes = nodes.read().await;
            Ok(Json(serde_json::to_value(&*nodes)?))
        }
        None => Ok(Json(serde_json::json!({}))),
    }
}

async fn cluster_status(State(state): State<HttpState>) -> Result<Json<ClusterStatus>, AppError> {
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

/// Partition assignments for a topic.
async fn topic_partitions(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<PartitionInfo>>, AppError> {
    // Look up topic to get its key_prefix
    let topic = state
        .config_store
        .get_topic(&name)?
        .ok_or_else(|| AppError::NotFound(format!("topic '{name}' not found")))?;

    // Try per-topic cluster view first, fall back to global
    let assignments = if let Some(ref tm) = state.topic_manager {
        let tm = tm.read().await;
        if let Some(view) = tm.get_view(&name) {
            view.assignments().await
        } else if let Some(ref cv) = state.cluster_view {
            cv.assignments().await
        } else {
            HashMap::new()
        }
    } else if let Some(ref cv) = state.cluster_view {
        cv.assignments().await
    } else {
        HashMap::new()
    };

    // Group by partition
    let mut by_partition: HashMap<u32, Vec<ReplicaInfo>> = HashMap::new();
    for ((_p, _r), info) in &assignments {
        by_partition
            .entry(info.partition)
            .or_default()
            .push(ReplicaInfo {
                replica: info.replica,
                node_id: info.node_id.clone(),
                state: format!("{:?}", info.state),
                source: format!("{:?}", info.source),
            });
    }

    // Filter to partitions belonging to this topic (0..num_partitions)
    let mut result: Vec<PartitionInfo> = (0..topic.num_partitions)
        .map(|p| PartitionInfo {
            partition: p,
            replicas: by_partition.remove(&p).unwrap_or_default(),
        })
        .collect();
    result.sort_by_key(|p| p.partition);

    Ok(Json(result))
}

/// Consumer group lag for a specific topic.
async fn topic_lag(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<TopicLagSummary>>, AppError> {
    let topic = state
        .config_store
        .get_topic(&name)?
        .ok_or_else(|| AppError::NotFound(format!("topic '{name}' not found")))?;

    let Some(ref lag_monitor) = state.lag_monitor else {
        return Ok(Json(Vec::new()));
    };

    let groups = lag_monitor.known_groups().await;
    let mut result = Vec::new();

    for group_id in groups {
        let reports = lag_monitor.get_group_lag(&group_id).await;
        // Filter to this topic's partition range
        let topic_reports: Vec<LagReport> = reports
            .into_iter()
            .filter(|r| r.partition < topic.num_partitions)
            .collect();
        if topic_reports.is_empty() {
            continue;
        }
        let total_lag: u64 = topic_reports.iter().map(|r| r.total).sum();
        result.push(TopicLagSummary {
            group_id,
            partitions: topic_reports,
            total_lag,
        });
    }

    Ok(Json(result))
}

/// Active publishers for a topic (derived from watermark data).
async fn topic_publishers(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<PublisherInfo>>, AppError> {
    let topic = state
        .config_store
        .get_topic(&name)?
        .ok_or_else(|| AppError::NotFound(format!("topic '{name}' not found")))?;

    let Some(ref lag_monitor) = state.lag_monitor else {
        return Ok(Json(Vec::new()));
    };

    let raw_publishers = lag_monitor.get_publishers().await;
    let publishers: Vec<PublisherInfo> = raw_publishers
        .into_iter()
        .map(|(pub_id, partitions)| {
            let partition_map: HashMap<u32, u64> = partitions
                .into_iter()
                .filter(|p| *p < topic.num_partitions)
                .map(|p| (p, 0)) // lag per partition not available here
                .collect();
            PublisherInfo {
                publisher_id: pub_id.to_string(),
                partitions: partition_map,
            }
        })
        .filter(|p| !p.partitions.is_empty())
        .collect();
    Ok(Json(publishers))
}

// =========================================================================
// Write handlers
// =========================================================================

async fn create_topic(
    State(state): State<HttpState>,
    Json(req): Json<CreateTopicRequest>,
) -> Result<(StatusCode, Json<TopicConfig>), AppError> {
    let cfg = TopicConfig {
        name: req.name,
        key_prefix: req.key_prefix,
        num_partitions: req.num_partitions,
        replication_factor: req.replication_factor,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: req.required_labels,
        excluded_labels: req.excluded_labels,
    };

    // Persist
    state.config_store.put_topic(&cfg)?;

    // Publish config via Zenoh so agents discover it
    if let Some(ref session) = state.session {
        let key = format!("{}/_config/{}", state.key_prefix, cfg.name);
        if let Ok(bytes) = serde_json::to_vec(&cfg) {
            if let Err(e) = session.put(&key, bytes).await {
                warn!(topic = %cfg.name, "failed to publish topic config: {e}");
            }
        }
    }

    // Create per-topic cluster view
    if let Some(ref tm) = state.topic_manager {
        if !cfg.key_prefix.is_empty() {
            let mut tm = tm.write().await;
            if let Err(e) = tm.add_topic(&cfg.name, &cfg.key_prefix).await {
                warn!(topic = %cfg.name, "failed to create per-topic cluster view: {e}");
            }
        }
    }

    Ok((StatusCode::CREATED, Json(cfg)))
}

async fn update_topic(
    State(state): State<HttpState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateTopicRequest>,
) -> Result<Json<TopicConfig>, AppError> {
    let mut cfg = state
        .config_store
        .get_topic(&name)?
        .ok_or_else(|| AppError::NotFound(format!("topic '{name}' not found")))?;

    // Merge mutable fields
    if let Some(rf) = req.replication_factor {
        cfg.replication_factor = rf;
    }
    if let Some(ret) = req.retention {
        cfg.retention = ret;
    }
    if let Some(comp) = req.compaction {
        cfg.compaction = comp;
    }
    if let Some(labels) = req.required_labels {
        cfg.required_labels = labels;
    }
    if let Some(labels) = req.excluded_labels {
        cfg.excluded_labels = labels;
    }

    state.config_store.put_topic(&cfg)?;

    // Publish updated config
    if let Some(ref session) = state.session {
        let key = format!("{}/_config/{}", state.key_prefix, cfg.name);
        if let Ok(bytes) = serde_json::to_vec(&cfg) {
            let _ = session.put(&key, bytes).await;
        }
    }

    Ok(Json(cfg))
}

async fn delete_topic(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    if !state.config_store.delete_topic(&name)? {
        return Err(AppError::NotFound(format!("topic '{name}' not found")));
    }

    // Delete config key via Zenoh
    if let Some(ref session) = state.session {
        let key = format!("{}/_config/{}", state.key_prefix, name);
        let _ = session.delete(&key).await;
    }

    // Remove per-topic cluster view
    if let Some(ref tm) = state.topic_manager {
        let mut tm = tm.write().await;
        tm.remove_topic(&name).await;
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn drain_node(
    State(state): State<HttpState>,
    Path(id): Path<String>,
    Json(req): Json<DrainRequest>,
) -> Result<Json<DrainResponse>, AppError> {
    let cv = state
        .cluster_view
        .as_ref()
        .ok_or_else(|| AppError::Internal("cluster view not available".into()))?;
    let om = state
        .override_manager
        .as_ref()
        .ok_or_else(|| AppError::Internal("override manager not available".into()))?;

    let overrides = crate::drain::drain_node(&id, cv, om, req.replication_factor).await?;
    Ok(Json(DrainResponse {
        node_id: id,
        overrides,
    }))
}

async fn undrain_node(
    State(state): State<HttpState>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let om = state
        .override_manager
        .as_ref()
        .ok_or_else(|| AppError::Internal("override manager not available".into()))?;

    crate::drain::undrain_node(&id, om).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_overrides(State(state): State<HttpState>) -> Result<impl IntoResponse, AppError> {
    match state.override_manager {
        Some(ref om) => {
            let table = om.current().await;
            Ok(Json(serde_json::to_value(&table)?))
        }
        None => Ok(Json(serde_json::json!({"entries": [], "epoch": 0}))),
    }
}

async fn add_overrides(
    State(state): State<HttpState>,
    Json(req): Json<AddOverridesRequest>,
) -> Result<StatusCode, AppError> {
    let om = state
        .override_manager
        .as_ref()
        .ok_or_else(|| AppError::Internal("override manager not available".into()))?;

    let entries: Vec<mitiflow_agent::OverrideEntry> = req
        .entries
        .into_iter()
        .map(|e| mitiflow_agent::OverrideEntry {
            partition: e.partition,
            replica: e.replica,
            node_id: e.node_id,
            reason: e.reason,
        })
        .collect();
    let ttl = req.ttl_seconds.map(Duration::from_secs);
    om.add_entries(entries, ttl).await?;
    Ok(StatusCode::CREATED)
}

async fn clear_overrides(State(state): State<HttpState>) -> Result<StatusCode, AppError> {
    let om = state
        .override_manager
        .as_ref()
        .ok_or_else(|| AppError::Internal("override manager not available".into()))?;
    om.clear().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// List known consumer groups (derived from offset data in LagMonitor).
async fn list_consumer_groups(
    State(state): State<HttpState>,
) -> Result<Json<Vec<ConsumerGroupSummary>>, AppError> {
    let Some(ref lag_monitor) = state.lag_monitor else {
        return Ok(Json(Vec::new()));
    };

    let groups = lag_monitor.known_groups().await;
    let mut result = Vec::new();
    for group_id in groups {
        let reports = lag_monitor.get_group_lag(&group_id).await;
        let total_lag: u64 = reports.iter().map(|r| r.total).sum();
        result.push(ConsumerGroupSummary {
            group_id,
            total_lag,
        });
    }
    Ok(Json(result))
}

/// Get detail for a specific consumer group.
async fn get_consumer_group(
    State(state): State<HttpState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let Some(ref lag_monitor) = state.lag_monitor else {
        return Err(AppError::NotFound("lag monitor not available".into()));
    };

    let reports = lag_monitor.get_group_lag(&id).await;
    let total_lag: u64 = reports.iter().map(|r| r.total).sum();

    Ok(Json(serde_json::json!({
        "group_id": id,
        "lag": reports,
        "total_lag": total_lag,
    })))
}

/// Query events from the store via Zenoh query-through.
///
/// Proxies the request to the appropriate EventStore queryable for the given
/// topic and partition. The store returns events with attachment metadata.
///
/// Query parameters:
/// - `topic` (required) — topic name to look up key_prefix
/// - `partition` (optional) — specific partition, or all partitions
/// - `after_seq`, `before_seq` — sequence range filter
/// - `after_time`, `before_time` — time range filter (ISO 8601)
/// - `publisher_id` — filter by publisher UUID
/// - `key` — filter by application key
/// - `limit` — max results (default 50, max 1000)
async fn query_events(
    State(state): State<HttpState>,
    Query(params): Query<EventQueryParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    let topic_name = params
        .topic
        .as_deref()
        .ok_or_else(|| AppError::BadRequest("'topic' parameter is required".into()))?;

    // Look up topic config to get key_prefix
    let topic_config = state
        .config_store
        .get_topic(topic_name)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound(format!("topic '{}' not found", topic_name)))?;

    let session = state
        .session
        .as_ref()
        .ok_or_else(|| AppError::Internal("Zenoh session not available".into()))?;

    let limit = params.limit.unwrap_or(50).min(1000);

    // Build the Zenoh selector parameters
    let mut selector_params = Vec::new();
    if let Some(after_seq) = params.after_seq {
        selector_params.push(format!("after_seq={after_seq}"));
    }
    if let Some(before_seq) = params.before_seq {
        selector_params.push(format!("before_seq={before_seq}"));
    }
    if let Some(ref after_time) = params.after_time {
        selector_params.push(format!("after_time={after_time}"));
    }
    if let Some(ref before_time) = params.before_time {
        selector_params.push(format!("before_time={before_time}"));
    }
    if let Some(ref publisher_id) = params.publisher_id {
        selector_params.push(format!("publisher_id={publisher_id}"));
    }
    selector_params.push(format!("limit={limit}"));
    let params_str = selector_params.join("&");

    // Determine which partitions to query
    let partitions: Vec<u32> = if let Some(p) = params.partition {
        vec![p]
    } else {
        (0..topic_config.num_partitions).collect()
    };

    let store_prefix = format!("{}/_store", topic_config.key_prefix);
    let mut all_events = Vec::new();

    for partition in partitions {
        let selector = format!("{store_prefix}/{partition}?{params_str}");
        let replies = match tokio::time::timeout(
            Duration::from_secs(5),
            session
                .get(&selector)
                .consolidation(zenoh::query::ConsolidationMode::None)
                .accept_replies(zenoh::query::ReplyKeyExpr::Any),
        )
        .await
        {
            Ok(Ok(replies)) => replies,
            Ok(Err(e)) => {
                warn!(partition, error = %e, "store query failed");
                continue;
            }
            Err(_) => {
                warn!(partition, "store query timed out");
                continue;
            }
        };

        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.into_result() {
                let key_expr_str = sample.key_expr().as_keyexpr().as_str().to_string();
                let payload_bytes = sample.payload().to_bytes().to_vec();
                let payload_size = payload_bytes.len();

                // Try to decode metadata from attachment
                let meta = sample
                    .attachment()
                    .and_then(|a| mitiflow::attachment::decode_metadata(a).ok());

                let key = mitiflow::attachment::extract_key(&key_expr_str).map(String::from);

                // Optionally filter by application key
                if let Some(ref filter_key) = params.key {
                    if key.as_deref() != Some(filter_key.as_str()) {
                        continue;
                    }
                }

                // Encode payload as base64 and attempt UTF-8 text
                use base64::Engine;
                let payload_base64 =
                    base64::engine::general_purpose::STANDARD.encode(&payload_bytes);
                let payload_text = String::from_utf8(payload_bytes).ok();

                let event_json = serde_json::json!({
                    "seq": meta.as_ref().map(|m| m.seq).unwrap_or(0),
                    "partition": partition,
                    "publisher_id": meta.as_ref().map(|m| m.pub_id.to_string()).unwrap_or_default(),
                    "event_id": meta.as_ref().map(|m| m.event_id.to_string()).unwrap_or_default(),
                    "timestamp": meta.as_ref().map(|m| m.timestamp.to_rfc3339()).unwrap_or_default(),
                    "key": key,
                    "key_expr": key_expr_str,
                    "payload_size": payload_size,
                    "payload_base64": payload_base64,
                    "payload_text": payload_text,
                });
                all_events.push(event_json);
            }
        }
    }

    // Sort by (partition, seq)
    all_events.sort_by(|a, b| {
        let pa = a["partition"].as_u64().unwrap_or(0);
        let pb = b["partition"].as_u64().unwrap_or(0);
        let sa = a["seq"].as_u64().unwrap_or(0);
        let sb = b["seq"].as_u64().unwrap_or(0);
        (pa, sa).cmp(&(pb, sb))
    });

    let total = all_events.len();
    let has_more = total > limit;
    all_events.truncate(limit);

    Ok(Json(serde_json::json!({
        "events": all_events,
        "total": total.min(limit),
        "has_more": has_more,
    })))
}

/// Reset consumer group offsets.
///
/// Publishes an offset commit to `{key_prefix}/_offsets/{partition}/{group_id}`
/// with the target sequence number.
async fn reset_consumer_group(
    State(state): State<HttpState>,
    Path(id): Path<String>,
    Json(body): Json<ResetOffsetsRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = state
        .session
        .as_ref()
        .ok_or_else(|| AppError::Internal("Zenoh session not available".into()))?;

    let topic_name = &body.topic;
    let topic_config = state
        .config_store
        .get_topic(topic_name)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound(format!("topic '{}' not found", topic_name)))?;

    let target_seq = match &body.strategy {
        ResetStrategy::Earliest => 0u64,
        ResetStrategy::Latest => {
            // Retrieve the latest watermark — for now return u64::MAX as "latest"
            // which the store interprets as "current tip"
            u64::MAX
        }
        ResetStrategy::ToSeq(seq) => *seq,
    };

    let key_prefix = &topic_config.key_prefix;
    let offset_key = format!(
        "{key_prefix}/_offsets/{}/{id}",
        body.partition
    );

    // Publish a simple offset commit (seq as big-endian bytes)
    let payload = target_seq.to_be_bytes().to_vec();
    session
        .put(&offset_key, payload)
        .await
        .map_err(|e| AppError::Internal(format!("failed to publish offset reset: {e}")))?;

    Ok(Json(serde_json::json!({
        "group_id": id,
        "topic": topic_name,
        "partition": body.partition,
        "offset": target_seq,
    })))
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
