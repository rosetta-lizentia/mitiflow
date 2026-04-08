//! Cluster node, status, override, and drain handlers.

use std::time::Duration;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use super::types::*;
use super::{AppError, HttpState};

pub(super) async fn cluster_nodes(
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

/// Per-topic partition assignments for a single node, aggregated across all
/// per-topic ClusterViews.
pub(super) async fn node_partitions(
    State(state): State<HttpState>,
    Path(node_id): Path<String>,
) -> Result<Json<Vec<NodeTopicPartitions>>, AppError> {
    let mut result = Vec::new();

    if let Some(ref tm) = state.topic_manager {
        let tm = tm.read().await;
        let topics = state.config_store.list_topics()?;
        for topic in topics {
            if let Some(view) = tm.get_view(&topic.name) {
                let assignments = view.assignments().await;
                let mut partitions: Vec<NodePartitionEntry> = assignments
                    .into_iter()
                    .filter(|(_, a)| a.node_id == node_id || a.node_id.starts_with(&node_id))
                    .map(|(_, a)| NodePartitionEntry {
                        partition: a.partition,
                        replica: a.replica,
                        state: format!("{:?}", a.state),
                        source: format!("{:?}", a.source),
                    })
                    .collect();
                if partitions.is_empty() {
                    continue;
                }
                partitions.sort_by_key(|p| (p.partition, p.replica));
                result.push(NodeTopicPartitions {
                    topic: topic.name.clone(),
                    partitions,
                });
            }
        }
    }

    result.sort_by(|a, b| a.topic.cmp(&b.topic));
    Ok(Json(result))
}

pub(super) async fn cluster_status(
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

pub(super) async fn drain_node(
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

pub(super) async fn undrain_node(
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

pub(super) async fn get_overrides(
    State(state): State<HttpState>,
) -> Result<impl IntoResponse, AppError> {
    match state.override_manager {
        Some(ref om) => {
            let table = om.current().await;
            Ok(Json(serde_json::to_value(&table)?))
        }
        None => Ok(Json(serde_json::json!({"entries": [], "epoch": 0}))),
    }
}

pub(super) async fn add_overrides(
    State(state): State<HttpState>,
    Json(req): Json<AddOverridesRequest>,
) -> Result<StatusCode, AppError> {
    let om = state
        .override_manager
        .as_ref()
        .ok_or_else(|| AppError::Internal("override manager not available".into()))?;

    let entries: Vec<mitiflow_storage::OverrideEntry> = req
        .entries
        .into_iter()
        .map(|e| mitiflow_storage::OverrideEntry {
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

pub(super) async fn clear_overrides(
    State(state): State<HttpState>,
) -> Result<StatusCode, AppError> {
    let om = state
        .override_manager
        .as_ref()
        .ok_or_else(|| AppError::Internal("override manager not available".into()))?;
    om.clear().await?;
    Ok(StatusCode::NO_CONTENT)
}
