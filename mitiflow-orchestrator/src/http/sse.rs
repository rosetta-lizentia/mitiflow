//! Server-Sent Events (SSE) stream handlers.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use super::HttpState;
use super::types::{ClusterEvent, EventStreamParams, LagStreamParams};

pub(super) async fn sse_cluster(
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

pub(super) async fn sse_lag(
    State(state): State<HttpState>,
    Query(params): Query<LagStreamParams>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.lag_events_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
        Ok(report) => {
            if let Some(ref group) = params.group
                && &report.group_id != group
            {
                return None;
            }
            serde_json::to_string(&report)
                .ok()
                .map(|data| Ok(Event::default().event("lag").data(data)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

pub(super) async fn sse_events(
    State(state): State<HttpState>,
    Query(params): Query<EventStreamParams>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.event_tail_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
        Ok(summary) => {
            if let Some(p) = params.partition
                && summary.partition != p
            {
                return None;
            }
            serde_json::to_string(&summary)
                .ok()
                .map(|data| Ok(Event::default().event("message").data(data)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
