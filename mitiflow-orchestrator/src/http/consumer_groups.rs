//! Consumer group list, detail, and offset-reset handlers.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, State};

use super::topics::query_liveliness_clients;
use super::types::*;
use super::{AppError, HttpState};

/// List known consumer groups.
///
/// Discovery combines two sources:
/// 1. **Lag monitors** (global + per-topic) — groups that have committed offsets
/// 2. **Liveliness tokens** — groups with live members (`_workers/{group}/{member}`)
pub(super) async fn list_consumer_groups(
    State(state): State<HttpState>,
) -> Result<Json<Vec<ConsumerGroupSummary>>, AppError> {
    let mut group_lag: HashMap<String, u64> = HashMap::new();

    // Global lag monitor
    if let Some(ref lm) = state.lag_monitor {
        for group_id in lm.known_groups().await {
            let total: u64 = lm
                .get_group_lag(&group_id)
                .await
                .iter()
                .map(|r| r.total)
                .sum();
            *group_lag.entry(group_id).or_default() += total;
        }
    }

    // Per-topic lag monitors
    let topics = state.config_store.list_topics().unwrap_or_default();
    if let Some(ref tm) = state.topic_manager {
        let tm = tm.read().await;
        for topic in &topics {
            if let Some(lm) = tm.get_lag_monitor(&topic.name) {
                for group_id in lm.known_groups().await {
                    let total: u64 = lm
                        .get_group_lag(&group_id)
                        .await
                        .iter()
                        .map(|r| r.total)
                        .sum();
                    *group_lag.entry(group_id).or_default() += total;
                }
            }
        }
    }

    // Also discover groups via liveliness tokens (catches groups that haven't committed yet)
    if let Some(ref session) = state.session {
        for topic in &topics {
            let prefix = if topic.key_prefix.is_empty() {
                &state.key_prefix
            } else {
                &topic.key_prefix
            };
            let (_, consumers) = query_liveliness_clients(session, prefix).await;
            for client in consumers {
                if let Some(group_id) = client.group_id {
                    group_lag.entry(group_id).or_insert(0);
                }
            }
        }
    }

    let mut result: Vec<ConsumerGroupSummary> = group_lag
        .into_iter()
        .map(|(group_id, total_lag)| ConsumerGroupSummary {
            group_id,
            total_lag,
        })
        .collect();
    result.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    Ok(Json(result))
}

/// Get detail for a specific consumer group.
pub(super) async fn get_consumer_group(
    State(state): State<HttpState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let mut reports = Vec::new();

    if let Some(ref lm) = state.lag_monitor {
        reports.extend(lm.get_group_lag(&id).await);
    }

    let topics = state.config_store.list_topics().unwrap_or_default();
    if let Some(ref tm) = state.topic_manager {
        let tm = tm.read().await;
        for topic in &topics {
            if let Some(lm) = tm.get_lag_monitor(&topic.name) {
                reports.extend(lm.get_group_lag(&id).await);
            }
        }
    }

    // Discover live members via liveliness
    let mut members: Vec<LiveClient> = Vec::new();
    if let Some(ref session) = state.session {
        for topic in &topics {
            let prefix = if topic.key_prefix.is_empty() {
                &state.key_prefix
            } else {
                &topic.key_prefix
            };
            let (_, consumers) = query_liveliness_clients(session, prefix).await;
            for client in consumers {
                if client.group_id.as_deref() == Some(&id) {
                    members.push(client);
                }
            }
        }
    }

    let total_lag: u64 = reports.iter().map(|r| r.total).sum();

    Ok(Json(serde_json::json!({
        "group_id": id,
        "lag": reports,
        "total_lag": total_lag,
        "members": members,
    })))
}

/// Reset consumer group offsets.
pub(super) async fn reset_consumer_group(
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
        ResetStrategy::Latest => u64::MAX,
        ResetStrategy::ToSeq(seq) => *seq,
    };

    let key_prefix = &topic_config.key_prefix;
    let offset_key = format!("{key_prefix}/_offsets/{}/{id}", body.partition);

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
