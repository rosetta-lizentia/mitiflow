# Improvement idea


- configurable encoding/decoding format. (currently fix on serde_json) ✅
- shared subscriber in durable publish (currently create a new subscriber each time it called) ✅
  - NOTE: Must use broadcast channel (not mpsc/flume) so all concurrent `publish_durable` callers receive every watermark update. Flume only delivers to one receiver.

## Scalability Bottlenecks with Many Publishers

1. **Single event processing task** — all publishers' samples are serialized through one `tokio::spawn` loop. No parallelism across publishers. Fix: shard by `pub_id % N` into N worker tasks, each owning its own `GapDetector` slice.

2. **Write lock per sample** — `gd.write().await` on every sample, shared between main task and heartbeat task. Since the main task is already single-threaded, the lock only exists to share state with the heartbeat task. Fix: send heartbeat beacons into a channel → main task processes inline as `&mut GapDetector`, eliminating the `Arc<RwLock>` entirely.

3. **Dead publisher accumulation** — `HashMap<PublisherId, u64>` never shrinks. Ephemeral publishers (serverless, short-lived workers) accumulate stale entries → cache misses over time. Fix: track `last_activity: Instant` per publisher, evict entries idle past a TTL.

4. **`extract_event_meta` on every delivery** — full payload deserialization just to extract `id` + `timestamp`. Fix: either embed a fixed header (like the 24-byte transport pattern in FastBench) or accept that `id`/`timestamp` come from the attachment instead of inside the payload.

## Partition-Level Ordering

See [04_ordering.md](04_ordering.md) for the full design discussion and recommendation (Approach C: per-(partition, publisher) sequences).