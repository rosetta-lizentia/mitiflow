//! Event query-through handler — proxies queries to EventStore via Zenoh.

use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use tracing::warn;

use super::types::EventQueryParams;
use super::{AppError, HttpState};

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
pub(super) async fn query_events(
    State(state): State<HttpState>,
    Query(params): Query<EventQueryParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    let topic_name = params
        .topic
        .as_deref()
        .ok_or_else(|| AppError::BadRequest("'topic' parameter is required".into()))?;

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

                let meta = sample
                    .attachment()
                    .and_then(|a| mitiflow::attachment::decode_metadata(a).ok());

                let key = mitiflow::attachment::extract_key(&key_expr_str).map(String::from);

                // Optionally filter by application key
                if let Some(ref filter_key) = params.key
                    && key.as_deref() != Some(filter_key.as_str())
                {
                    continue;
                }

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
