# Improvement idea


- configurable encoding/decoding format. (currently fix on serde_json) ✅
- shared subscriber in durable publish (currently create a new subscriber each time it called) ✅
  - NOTE: Must use broadcast channel (not mpsc/flume) so all concurrent `publish_durable` callers receive every watermark update. Flume only delivers to one receiver.

## Scalability Bottlenecks with Many Publishers

1. **Single event processing task** — all publishers' samples are serialized through one `tokio::spawn` loop. No parallelism across publishers. Fix: shard by `pub_id % N` into N worker tasks, each owning its own `GapDetector` slice.

2. **Write lock per sample** — `gd.write().await` on every sample, shared between main task and heartbeat task. Since the main task is already single-threaded, the lock only exists to share state with the heartbeat task. Fix: send heartbeat beacons into a channel → main task processes inline as `&mut GapDetector`, eliminating the `Arc<RwLock>` entirely.

3. **Dead publisher accumulation** — `HashMap<PublisherId, u64>` never shrinks. Ephemeral publishers (serverless, short-lived workers) accumulate stale entries → cache misses over time. Fix: track `last_activity: Instant` per publisher, evict entries idle past a TTL.

4. **`extract_event_meta` on every delivery** — full payload deserialization just to extract `id` + `timestamp`. Fix: either embed a fixed header (like the 24-byte transport pattern in FastBench) or accept that `id`/`timestamp` come from the attachment instead of inside the payload.

## Partition-Level Ordering (multi-server / Kafka compat)

**Problem:** mitiflow sequences are per-publisher, not per-partition. If two servers both write to `myapp/events/p0`, there is no total order across their events (subscriber sees two independent seq streams). Kafka's partition ordering is maintained by the broker, not producers.

**Three approaches:**

1. **Gateway as broker (Kafka compat — current shape is already correct)**
   - `mitiflow-gateway` owns one `EventPublisher` per partition, serializing writes from multiple Kafka clients.
   - Achieves partition-level total order because there is a single `AtomicU64` per partition in the gateway process.
   - For HA: need partition leader election across gateway instances (Zenoh liveliness is suitable). Without this, only one gateway instance may own a given partition.

2. **Single publisher per partition (native multi-server)**
   - Extend `PartitionManager` to the producer side: each server claims partitions via liveliness and creates one `EventPublisher` per owned partition.
   - At most one server writes to each partition at any time — partition-level ordering holds.
   - Rebalancing on server join/leave re-assigns partitions (liveliness already handles this).

3. **Event Store as offset assigner (long-term)**
   - Decouple per-publisher seq (gap detection only) from consumer-visible offset (per-partition, assigned by Event Store on persist).
   - Matches Kafka's model exactly: producer seq = idempotency key, broker (Event Store) assigns partition offset.
   - Consumers query Event Store by partition offset; watermark carries partition offsets, not publisher seqs.
   - Higher complexity but enables true multi-writer partition ordering without routing constraints.