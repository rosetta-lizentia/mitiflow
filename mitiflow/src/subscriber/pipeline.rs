//! Event processing pipeline: gap detection, shard routing, heartbeat handling.
//!
//! Contains the single-shard fast path and multi-shard dispatcher + worker
//! spawning logic, extracted from the subscriber `mod.rs`.

use std::sync::Arc;

use tracing::{debug, trace, warn};
use zenoh::Session;

use crate::attachment::EventMeta;
use crate::config::{EventBusConfig, HeartbeatMode, RecoveryMode};
use crate::event::RawEvent;
use crate::types::PublisherId;

use super::event_id_dedup::EventIdDedup;
use super::forwarder::{DecodedSample, ForwarderControl, decode_heartbeat};
use super::gap_detector::{GapDetector, MissInfo, SampleResult, SequenceTracker};
use super::recovery::{RecoveryConfig, deliver_event, spawn_recovery};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Message routed to a specific processing shard.
pub(crate) enum ShardMsg {
    /// Deliver this event (gap detection already done in single-shard, or
    /// done by the dispatcher in multi-shard mode).
    Deliver {
        meta: EventMeta,
        key: String,
        payload: Vec<u8>,
    },
}

/// Route a publisher ID to the owning shard index.
#[inline]
pub(crate) fn shard_for(pub_id: &PublisherId, num_shards: usize) -> usize {
    let bytes = pub_id.to_bytes();
    let n = u128::from_le_bytes(bytes);
    (n % num_shards as u128) as usize
}

/// Process a gap detection result: deliver the current event and spawn
/// recovery for any detected gap.
pub(crate) fn handle_sample_result(
    result: SampleResult,
    meta: &EventMeta,
    key: &str,
    payload: &[u8],
    tx: &flume::Sender<RawEvent>,
    session: &Session,
    recovery_config: &Arc<RecoveryConfig>,
) {
    match result {
        SampleResult::Deliver => {
            deliver_event(
                tx,
                meta.pub_id,
                meta.seq,
                key,
                payload,
                meta.event_id,
                meta.timestamp,
            );
        }
        SampleResult::Duplicate => {
            trace!(seq = meta.seq, pub_id = %meta.pub_id, "duplicate, dropping");
        }
        SampleResult::Gap(miss) => {
            deliver_event(
                tx,
                meta.pub_id,
                meta.seq,
                key,
                payload,
                meta.event_id,
                meta.timestamp,
            );
            spawn_recovery(
                session.clone(),
                Arc::clone(recovery_config),
                miss,
                tx.clone(),
            );
        }
    }
}

/// Spawn heartbeat-triggered gap recovery for all detected misses.
pub(crate) fn handle_heartbeat_gaps(
    misses: Vec<MissInfo>,
    session: &Session,
    recovery_config: &Arc<RecoveryConfig>,
    tx: &flume::Sender<RawEvent>,
) {
    for miss in misses {
        spawn_recovery(
            session.clone(),
            Arc::clone(recovery_config),
            miss,
            tx.clone(),
        );
    }
}

/// Periodically evict stale publisher entries from the gap detector.
pub(crate) fn maybe_evict(
    gd: &mut GapDetector,
    sample_count: u64,
    publisher_ttl: Option<std::time::Duration>,
) {
    if let Some(ttl) = publisher_ttl
        && sample_count.is_multiple_of(10_000)
    {
        let evicted = gd.evict_older_than(ttl);
        if evicted > 0 {
            debug!(shard_evicted = evicted, "evicted stale publisher entries");
        }
    }
}

// ---------------------------------------------------------------------------
// Single-shard worker (fast path)
// ---------------------------------------------------------------------------

/// Spawn the single-shard combined gap-detection + heartbeat + offload task.
///
/// Returns the task handle. When offload is enabled, the worker integrates
/// with the `OffloadManager` to trigger and execute the offload cycle.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn spawn_single_shard_worker(
    session: &Session,
    config: &EventBusConfig,
    sample_rx: flume::Receiver<DecodedSample>,
    event_tx: flume::Sender<RawEvent>,
    cancel: tokio_util::sync::CancellationToken,
    key_filtered: bool,
    #[cfg(feature = "store")] forwarder_control: Option<ForwarderControl>,
    #[cfg(feature = "store")] offload_event_rx_slot: &mut Option<
        flume::Receiver<super::offload::OffloadEvent>,
    >,
) -> crate::error::Result<tokio::task::JoinHandle<()>> {
    let key_prefix = &config.key_prefix;
    let publisher_ttl = config.publisher_ttl;
    let recovery_enabled = matches!(
        config.recovery_mode,
        RecoveryMode::Heartbeat | RecoveryMode::Both
    );

    let tx = event_tx.clone();
    let sess = session.clone();
    let rc = Arc::new(RecoveryConfig::from_bus_config(config));

    let hb_sub = if config.heartbeat != HeartbeatMode::Disabled {
        Some(
            session
                .declare_subscriber(format!("{key_prefix}/_heartbeat/*"))
                .await?,
        )
    } else {
        None
    };
    let has_hb = hb_sub.is_some();

    // Offload support (single-shard only).
    #[cfg(feature = "store")]
    let offload_mgr = if config.offload.enabled {
        if let Some(ctrl) = forwarder_control {
            let (offload_tx, offload_rx) = flume::bounded(64);
            *offload_event_rx_slot = Some(offload_rx);
            Some(super::offload::OffloadManager::new(
                session.clone(),
                config.offload.clone(),
                config.resolved_store_key_prefix(),
                event_tx.clone(),
                offload_tx,
                vec![],
                config.event_channel_capacity,
                ctrl,
            ))
        } else {
            None
        }
    } else {
        None
    };

    let cancel_clone = cancel.clone();
    let dedup_capacity = config.dedup_capacity;
    let handle = tokio::spawn(async move {
        let mut gd = GapDetector::new();
        let mut dedup = EventIdDedup::new(dedup_capacity);
        let mut sample_count = 0u64;
        #[cfg(feature = "store")]
        let mut offload_mgr = offload_mgr;

        loop {
            tokio::select! {
                _ = cancel_clone.cancelled() => break,
                sample_result = sample_rx.recv_async() => {
                    match sample_result {
                        Ok((meta, key, payload)) => {
                            if key_filtered {
                                // Key-filtered passthrough: dedup by EventId, skip gap detection.
                                if dedup.is_new(meta.event_id) {
                                    deliver_event(
                                        &tx, meta.pub_id, meta.seq, &key, &payload,
                                        meta.event_id, meta.timestamp,
                                    );
                                } else {
                                    trace!(seq = meta.seq, pub_id = %meta.pub_id, "key-filtered duplicate, dropping");
                                }
                                continue;
                            }

                            let partition = crate::attachment::extract_partition(&key);
                            let result = gd.on_sample(&meta.pub_id, partition, meta.seq);

                            // Check offload lag before delivering.
                            #[cfg(feature = "store")]
                            if let Some(mgr) = offload_mgr.as_mut() {
                                mgr.update_lag(tx.len(), None);
                                if mgr.check_and_trigger() {
                                    handle_sample_result(result, &meta, &key, &payload, &tx, &sess, &rc);
                                    sample_count += 1;
                                    if let Err(e) = mgr.run_offload_cycle(&mut gd, &sample_rx).await {
                                        warn!("offload cycle failed: {e}");
                                    }
                                    continue;
                                }
                            }

                            handle_sample_result(result, &meta, &key, &payload, &tx, &sess, &rc);
                            sample_count += 1;
                            maybe_evict(&mut gd, sample_count, publisher_ttl);
                        }
                        Err(_) => break,
                    }
                }
                hb_result = async { hb_sub.as_ref().unwrap().recv_async().await }, if has_hb && !key_filtered => {
                    match hb_result {
                        Ok(sample) => {
                            if let Some(beacon) = decode_heartbeat(&sample) {
                                if recovery_enabled {
                                    let misses = gd.on_heartbeat(&beacon.pub_id, &beacon.partition_seqs);
                                    handle_heartbeat_gaps(misses, &sess, &rc, &tx);
                                }
                                // Feed heartbeat lag to offload manager.
                                #[cfg(feature = "store")]
                                if let Some(mgr) = offload_mgr.as_mut() {
                                    for (&partition, &pub_seq) in &beacon.partition_seqs {
                                        if let Some(our_seq) = gd.last_seen(&beacon.pub_id, partition)
                                            && pub_seq > our_seq {
                                            mgr.update_lag(tx.len(), Some((beacon.pub_id, partition, pub_seq - our_seq)));
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        debug!("combined dispatcher/shard task stopped");
    });

    Ok(handle)
}

// ---------------------------------------------------------------------------
// Multi-shard workers
// ---------------------------------------------------------------------------

/// Spawn the multi-shard dispatcher + N shard worker tasks.
///
/// Gap detection, heartbeat handling, recovery spawning, and offload all
/// run in the **dispatcher** task (which owns the single `GapDetector`).
/// Shard workers are pure delivery pipelines — they receive pre-classified
/// events and push them to `event_tx`.
///
/// Returns all task handles (shard workers + dispatcher).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn spawn_multi_shard_workers(
    session: &Session,
    config: &EventBusConfig,
    sample_rx: flume::Receiver<DecodedSample>,
    event_tx: flume::Sender<RawEvent>,
    cancel: tokio_util::sync::CancellationToken,
    key_filtered: bool,
    #[cfg(feature = "store")] forwarder_control: Option<ForwarderControl>,
    #[cfg(feature = "store")] offload_event_rx_slot: &mut Option<
        flume::Receiver<super::offload::OffloadEvent>,
    >,
) -> crate::error::Result<Vec<tokio::task::JoinHandle<()>>> {
    let num_shards = config.num_processing_shards;
    let key_prefix = &config.key_prefix;

    let mut tasks = Vec::new();
    let mut shard_txs: Vec<flume::Sender<ShardMsg>> = Vec::with_capacity(num_shards);
    let mut shard_rxs: Vec<flume::Receiver<ShardMsg>> = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        let (tx, rx) = flume::unbounded();
        shard_txs.push(tx);
        shard_rxs.push(rx);
    }

    // -- Shard worker tasks (pure delivery) --
    for shard_rx in shard_rxs {
        let tx = event_tx.clone();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    msg = shard_rx.recv_async() => {
                        match msg {
                            Ok(ShardMsg::Deliver { meta, key, payload }) => {
                                deliver_event(
                                    &tx, meta.pub_id, meta.seq, &key, &payload,
                                    meta.event_id, meta.timestamp,
                                );
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            debug!("shard worker stopped");
        });
        tasks.push(handle);
    }

    // -- Dispatcher task (gap detection + heartbeat + offload + routing) --
    let publisher_ttl = config.publisher_ttl;
    let recovery_enabled = matches!(
        config.recovery_mode,
        RecoveryMode::Heartbeat | RecoveryMode::Both
    );

    let sess = session.clone();
    let rc = Arc::new(RecoveryConfig::from_bus_config(config));

    let hb_sub = if config.heartbeat != HeartbeatMode::Disabled {
        Some(
            session
                .declare_subscriber(format!("{key_prefix}/_heartbeat/*"))
                .await?,
        )
    } else {
        None
    };
    let has_hb = hb_sub.is_some();

    // Offload support.
    #[cfg(feature = "store")]
    let offload_mgr = if config.offload.enabled {
        if let Some(ctrl) = forwarder_control {
            let (offload_tx, offload_rx) = flume::bounded(64);
            *offload_event_rx_slot = Some(offload_rx);
            Some(super::offload::OffloadManager::new(
                session.clone(),
                config.offload.clone(),
                config.resolved_store_key_prefix(),
                event_tx.clone(),
                offload_tx,
                vec![],
                config.event_channel_capacity,
                ctrl,
            ))
        } else {
            None
        }
    } else {
        None
    };

    let cancel_clone = cancel.clone();
    let tx = event_tx.clone();
    let dedup_capacity = config.dedup_capacity;
    let handle = tokio::spawn(async move {
        let mut gd = GapDetector::new();
        let mut dedup = EventIdDedup::new(dedup_capacity);
        let mut sample_count = 0u64;
        #[cfg(feature = "store")]
        let mut offload_mgr = offload_mgr;

        loop {
            tokio::select! {
                _ = cancel_clone.cancelled() => break,
                sample_result = sample_rx.recv_async() => {
                    match sample_result {
                        Ok((meta, key, payload)) => {
                            if key_filtered {
                                // Key-filtered passthrough: dedup by EventId, skip gap detection.
                                if dedup.is_new(meta.event_id) {
                                    let shard_idx = shard_for(&meta.pub_id, num_shards);
                                    let _ = shard_txs[shard_idx].send(ShardMsg::Deliver {
                                        meta: meta.clone(),
                                        key: key.to_string(),
                                        payload: payload.to_vec(),
                                    });
                                } else {
                                    trace!(seq = meta.seq, pub_id = %meta.pub_id, "key-filtered duplicate, dropping");
                                }
                                continue;
                            }

                            let partition = crate::attachment::extract_partition(&key);
                            let result = gd.on_sample(&meta.pub_id, partition, meta.seq);

                            // Check offload lag before delivering.
                            #[cfg(feature = "store")]
                            if let Some(mgr) = offload_mgr.as_mut() {
                                mgr.update_lag(tx.len(), None);
                                if mgr.check_and_trigger() {
                                    dispatch_sample_result(result, &meta, &key, &payload, &tx, &shard_txs, num_shards, &sess, &rc);
                                    sample_count += 1;
                                    if let Err(e) = mgr.run_offload_cycle(&mut gd, &sample_rx).await {
                                        warn!("offload cycle failed: {e}");
                                    }
                                    continue;
                                }
                            }

                            dispatch_sample_result(result, &meta, &key, &payload, &tx, &shard_txs, num_shards, &sess, &rc);
                            sample_count += 1;
                            maybe_evict(&mut gd, sample_count, publisher_ttl);
                        }
                        Err(_) => break,
                    }
                }
                hb_result = async { hb_sub.as_ref().unwrap().recv_async().await }, if has_hb && !key_filtered => {
                    match hb_result {
                        Ok(sample) => {
                            if let Some(beacon) = decode_heartbeat(&sample) {
                                if recovery_enabled {
                                    let misses = gd.on_heartbeat(&beacon.pub_id, &beacon.partition_seqs);
                                    handle_heartbeat_gaps(misses, &sess, &rc, &tx);
                                }
                                // Feed heartbeat lag to offload manager.
                                #[cfg(feature = "store")]
                                if let Some(mgr) = offload_mgr.as_mut() {
                                    for (&partition, &pub_seq) in &beacon.partition_seqs {
                                        if let Some(our_seq) = gd.last_seen(&beacon.pub_id, partition)
                                            && pub_seq > our_seq {
                                            mgr.update_lag(tx.len(), Some((beacon.pub_id, partition, pub_seq - our_seq)));
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        debug!("multi-shard dispatcher stopped");
    });
    tasks.push(handle);

    Ok(tasks)
}

/// Dispatch a gap-detected sample: deliver the event and route to shard,
/// spawning recovery for any detected gap.
#[allow(clippy::too_many_arguments)]
fn dispatch_sample_result(
    result: SampleResult,
    meta: &EventMeta,
    key: &str,
    payload: &[u8],
    event_tx: &flume::Sender<RawEvent>,
    shard_txs: &[flume::Sender<ShardMsg>],
    num_shards: usize,
    session: &Session,
    recovery_config: &Arc<RecoveryConfig>,
) {
    match result {
        SampleResult::Deliver => {
            let shard_idx = shard_for(&meta.pub_id, num_shards);
            let _ = shard_txs[shard_idx].send(ShardMsg::Deliver {
                meta: meta.clone(),
                key: key.to_string(),
                payload: payload.to_vec(),
            });
        }
        SampleResult::Duplicate => {
            trace!(seq = meta.seq, pub_id = %meta.pub_id, "duplicate, dropping");
        }
        SampleResult::Gap(miss) => {
            let shard_idx = shard_for(&meta.pub_id, num_shards);
            let _ = shard_txs[shard_idx].send(ShardMsg::Deliver {
                meta: meta.clone(),
                key: key.to_string(),
                payload: payload.to_vec(),
            });
            // Recovery pushes directly to event_tx (not via shard).
            spawn_recovery(
                session.clone(),
                Arc::clone(recovery_config),
                miss,
                event_tx.clone(),
            );
        }
    }
}
