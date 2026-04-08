//! Topic CRUD and topic-detail handlers (partitions, lag, publishers, clients).

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use tracing::warn;
use zenoh::Session;

use super::types::*;
use super::{AppError, HttpState};
use crate::config::{RetentionPolicy, TopicConfig};

// =========================================================================
// Read handlers
// =========================================================================

pub(super) async fn list_topics(
    State(state): State<HttpState>,
) -> Result<Json<Vec<TopicConfig>>, AppError> {
    let topics = state.config_store.list_topics()?;
    Ok(Json(topics))
}

pub(super) async fn get_topic(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<TopicConfig>, AppError> {
    match state.config_store.get_topic(&name)? {
        Some(cfg) => Ok(Json(cfg)),
        None => Err(AppError::NotFound(format!("topic '{name}' not found"))),
    }
}

/// Partition assignments for a topic.
pub(super) async fn topic_partitions(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<PartitionInfo>>, AppError> {
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
pub(super) async fn topic_lag(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<TopicLagSummary>>, AppError> {
    let topic = state
        .config_store
        .get_topic(&name)?
        .ok_or_else(|| AppError::NotFound(format!("topic '{name}' not found")))?;

    // Try per-topic lag monitor first, fall back to global
    let per_topic_lm = if let Some(ref tm) = state.topic_manager {
        tm.read().await.get_lag_monitor(&name).cloned()
    } else {
        None
    };
    let Some(lm) = per_topic_lm.as_ref().or(state.lag_monitor.as_ref()) else {
        return Ok(Json(Vec::new()));
    };

    let groups = lm.known_groups().await;
    let mut result = Vec::new();

    for group_id in groups {
        let reports = lm.get_group_lag(&group_id).await;
        let topic_reports: Vec<_> = reports
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
pub(super) async fn topic_publishers(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<PublisherInfo>>, AppError> {
    let topic = state
        .config_store
        .get_topic(&name)?
        .ok_or_else(|| AppError::NotFound(format!("topic '{name}' not found")))?;

    // Try per-topic lag monitor first, fall back to global
    let per_topic_lm = if let Some(ref tm) = state.topic_manager {
        tm.read().await.get_lag_monitor(&name).cloned()
    } else {
        None
    };
    let Some(lm) = per_topic_lm.as_ref().or(state.lag_monitor.as_ref()) else {
        return Ok(Json(Vec::new()));
    };

    let raw_publishers = lm.get_publishers().await;
    let publishers: Vec<PublisherInfo> = raw_publishers
        .into_iter()
        .map(|(pub_id, partitions)| {
            let partition_map: HashMap<u32, u64> = partitions
                .into_iter()
                .filter(|p| *p < topic.num_partitions)
                .map(|p| (p, 0))
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

/// Query liveliness tokens for a specific topic to discover live publishers and consumers.
pub(super) async fn topic_clients(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<TopicClients>, AppError> {
    let topic = state
        .config_store
        .get_topic(&name)?
        .ok_or_else(|| AppError::NotFound(format!("topic '{name}' not found")))?;

    let Some(ref session) = state.session else {
        return Ok(Json(TopicClients {
            topic: name,
            publishers: Vec::new(),
            consumers: Vec::new(),
        }));
    };

    let prefix = if topic.key_prefix.is_empty() {
        &state.key_prefix
    } else {
        &topic.key_prefix
    };

    let (publishers, consumers) = query_liveliness_clients(session, prefix).await;

    Ok(Json(TopicClients {
        topic: name,
        publishers,
        consumers,
    }))
}

/// Query liveliness tokens across all topics.
pub(super) async fn all_clients(
    State(state): State<HttpState>,
) -> Result<Json<Vec<TopicClients>>, AppError> {
    let Some(ref session) = state.session else {
        return Ok(Json(Vec::new()));
    };

    let topics = state.config_store.list_topics()?;
    let mut result = Vec::new();

    for topic in topics {
        let prefix = if topic.key_prefix.is_empty() {
            &state.key_prefix
        } else {
            &topic.key_prefix
        };

        let (publishers, consumers) = query_liveliness_clients(session, prefix).await;

        result.push(TopicClients {
            topic: topic.name,
            publishers,
            consumers,
        });
    }

    result.sort_by(|a, b| a.topic.cmp(&b.topic));
    Ok(Json(result))
}

// =========================================================================
// Write handlers
// =========================================================================

pub(super) async fn create_topic(
    State(state): State<HttpState>,
    Json(req): Json<CreateTopicRequest>,
) -> Result<(StatusCode, Json<TopicConfig>), AppError> {
    let cfg = TopicConfig {
        name: req.name,
        key_prefix: req.key_prefix,
        num_partitions: req.num_partitions,
        replication_factor: req.replication_factor,
        retention: RetentionPolicy::default(),
        compaction: crate::config::CompactionPolicy::default(),
        required_labels: req.required_labels,
        excluded_labels: req.excluded_labels,
        codec: req.codec,
        key_format: req.key_format,
        schema_version: req.schema_version,
    };

    state.config_store.put_topic(&cfg)?;

    // Publish config via Zenoh so agents discover it
    if let Some(ref session) = state.session {
        let key = format!("{}/_config/{}", state.key_prefix, cfg.name);
        if let Ok(bytes) = serde_json::to_vec(&cfg)
            && let Err(e) = session.put(&key, bytes).await
        {
            warn!(topic = %cfg.name, "failed to publish topic config: {e}");
        }
    }

    // Create per-topic cluster view
    if let Some(ref tm) = state.topic_manager
        && !cfg.key_prefix.is_empty()
    {
        let mut tm = tm.write().await;
        if let Err(e) = tm.add_topic(&cfg.name, &cfg.key_prefix).await {
            warn!(topic = %cfg.name, "failed to create per-topic cluster view: {e}");
        }
    }

    Ok((StatusCode::CREATED, Json(cfg)))
}

pub(super) async fn update_topic(
    State(state): State<HttpState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateTopicRequest>,
) -> Result<Json<TopicConfig>, AppError> {
    let mut cfg = state
        .config_store
        .get_topic(&name)?
        .ok_or_else(|| AppError::NotFound(format!("topic '{name}' not found")))?;

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
    if let Some(codec) = req.codec {
        cfg.codec = codec;
    }
    if let Some(key_format) = req.key_format {
        cfg.key_format = key_format;
    }
    if let Some(schema_version) = req.schema_version {
        cfg.schema_version = schema_version;
    }

    state.config_store.put_topic(&cfg)?;

    if let Some(ref session) = state.session {
        let key = format!("{}/_config/{}", state.key_prefix, cfg.name);
        if let Ok(bytes) = serde_json::to_vec(&cfg) {
            let _ = session.put(&key, bytes).await;
        }
    }

    Ok(Json(cfg))
}

pub(super) async fn delete_topic(
    State(state): State<HttpState>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    if !state.config_store.delete_topic(&name)? {
        return Err(AppError::NotFound(format!("topic '{name}' not found")));
    }

    if let Some(ref session) = state.session {
        let key = format!("{}/_config/{}", state.key_prefix, name);
        let _ = session.delete(&key).await;
    }

    if let Some(ref tm) = state.topic_manager {
        let mut tm = tm.write().await;
        tm.remove_topic(&name).await;
    }

    Ok(StatusCode::NO_CONTENT)
}

// =========================================================================
// Liveliness helper
// =========================================================================

/// Helper: query liveliness for publishers (`_publishers/*`) and workers/consumers
/// (`_workers/**`) under a given key prefix.
pub(crate) async fn query_liveliness_clients(
    session: &Session,
    prefix: &str,
) -> (Vec<LiveClient>, Vec<LiveClient>) {
    let mut publishers = Vec::new();
    let mut consumers = Vec::new();

    // Discover publishers
    let pub_key = format!("{prefix}/_publishers/*");
    if let Ok(replies) = session.liveliness().get(&pub_key).await {
        let pub_prefix = format!("{prefix}/_publishers/");
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let key = sample.key_expr().as_str();
                if let Some(id) = key.strip_prefix(&pub_prefix) {
                    publishers.push(LiveClient {
                        id: id.to_string(),
                        role: ClientRole::Publisher,
                        group_id: None,
                    });
                }
            }
        }
    }

    // Discover consumers/workers (may be flat _workers/{id} or grouped _workers/{group}/{id})
    let worker_key = format!("{prefix}/_workers/**");
    if let Ok(replies) = session.liveliness().get(&worker_key).await {
        let worker_prefix = format!("{prefix}/_workers/");
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let key = sample.key_expr().as_str();
                if let Some(suffix) = key.strip_prefix(&worker_prefix) {
                    if let Some((group, member)) = suffix.split_once('/') {
                        consumers.push(LiveClient {
                            id: member.to_string(),
                            role: ClientRole::Consumer,
                            group_id: Some(group.to_string()),
                        });
                    } else {
                        consumers.push(LiveClient {
                            id: suffix.to_string(),
                            role: ClientRole::Consumer,
                            group_id: None,
                        });
                    }
                }
            }
        }
    }

    publishers.sort_by(|a, b| a.id.cmp(&b.id));
    consumers.sort_by(|a, b| a.id.cmp(&b.id));
    (publishers, consumers)
}
