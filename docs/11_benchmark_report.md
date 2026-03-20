# Micro-Benchmark Report & Optimization Plan

> Generated from criterion benchmarks in `mitiflow/benches/`.
> All measurements taken on the development machine in `--release` mode.

---

## 1. Benchmark Results

### 1.1 Codec — Encode/Decode (per-event hot path)

| Codec    | Encode 64B | Encode 1KB | Encode 64KB | Decode 64B | Decode 1KB | Decode 64KB |
|----------|-----------|-----------|------------|-----------|-----------|------------|
| JSON     | ~250 ns   | ~2.9 µs   | ~165 µs    | ~700 ns   | ~6.4 µs   | ~378 µs    |
| MsgPack  | ~150 ns   | ~1.5 µs   | ~150 µs    | ~643 ns   | ~3.5 µs   | ~150 µs    |
| Postcard | **54 ns** | **477 ns**| **28 µs**  | **153 ns**| **477 ns**| **28 µs**  |

**Finding:** Postcard is **4–13× faster** than JSON (the current default) across all payload sizes. The codec sits on the absolute hottest path — every publish and every subscribe call.

### 1.2 Attachment Metadata — 50-byte fixed header

| Operation          | Time     |
|--------------------|----------|
| `encode_metadata`  | 25 ns    |
| `decode_metadata`  | 13 ns    |
| `extract_partition` | ~14 ns (×4 keys) |

**Finding:** Zero-copy 50-byte header is extremely fast. No action needed.

### 1.3 Gap Detector — Sequence tracking hot path

| Scenario                   | Time     |
|----------------------------|----------|
| Normal delivery            | 43 ns    |
| Gap detection              | 46 ns    |
| Duplicate filtering        | 35 ns    |
| 100 pre-seeded publishers  | 37 ns    |
| 1000 pre-seeded publishers | 37 ns    |
| Heartbeat / 4 partitions   | 144 ns   |
| Heartbeat / 16 partitions  | 602 ns   |
| Heartbeat / 64 partitions  | 2.3 µs   |

**Finding:** `on_sample()` is consistently <50 ns regardless of publisher count — well under the 1 µs target. HashMap lookup dominates; performance is scale-invariant. No action needed.

### 1.4 Partition — Hash Ring

| Operation                | Time     |
|--------------------------|----------|
| `partition_for` (64 partitions) | 8 ns |
| `worker_for` / 3 workers | 23 ns    |
| `worker_for` / 10 workers| 80 ns    |
| `worker_for` / 50 workers| 400 ns   |
| `assignments` / 3w × 64p | 2.3 µs  |
| `assignments` / 10w × 64p| 5.1 µs  |
| `assignments` / 50w × 64p| 22.9 µs |
| `assignments` / 10w × 256p| 18.9 µs|

**Finding:** `partition_for` is marginal (8 ns). `worker_for` scales linearly with worker count as expected from HRW — O(W) per partition. `assignments` (full recomputation) tops out at 23 µs for 50 workers × 64 partitions, acceptable since it runs only on rebalance. No action needed.

### 1.5 Event — Creation & Serialization

| Operation              | 64B    | 1KB    | 64KB   |
|------------------------|--------|--------|--------|
| `Event::new(payload)`  | 199 ns | 210 ns | 789 ns |
| `Event::to_bytes` (JSON) | 389 ns | 3.0 µs | 165 µs |
| `Event::from_bytes` (JSON) | 705 ns | 6.4 µs | 378 µs |

| `RawEvent::deserialize_with` | JSON: 1.7 µs | MsgPack: 643 ns | Postcard: 153 ns |

**Finding:** `Event::new()` costs ~200 ns, dominated by UUID v7 generation + `chrono::Utc::now()`. This is a fixed cost and unavoidable. The serialization cost is dominated by the codec — see §1.1.

### 1.6 Store — FjallBackend Operations

| Operation                    | Time       | Notes |
|------------------------------|------------|-------|
| Write single event           | 5.7 µs     | 256B payload |
| Write batch / 1              | 7.4 µs     | Vec overhead makes it *worse* |
| Write batch / 10             | 57 µs (5.7 µs/event) | Amortized ≈ single |
| Write batch / 100            | 637 µs (6.4 µs/event) | Slightly worse per-event |
| Query by seq / 1K events     | 151 µs     | |
| Query by seq / 10K events    | 1.1 ms     | |
| **Query by seq / 100K events** | **23.6 ms** | **Full keyspace scan** |
| Query by time / 10K events   | 54 µs      | |
| Query replay (HLC) / 10K     | 80 µs      | |
| **GC 100K events (50%)**     | **467 ms** | **One commit per event** |
| **Compact 10K×10 versions**  | **376 ms** | **Individual removes** |
| Watermarks / 10 publishers   | 260 ns     | |
| Watermarks / 100 publishers  | 2.5 µs    | |

### 1.7 Watermark — Serialization & Durability Checks

| Operation                        | Time     |
|----------------------------------|----------|
| Serialize / 10 publishers        | 648 ns   |
| Serialize / 100 publishers       | 4.5 µs   |
| Deserialize / 10 publishers      | 1.2 µs   |
| Deserialize / 100 publishers     | 10.0 µs  |
| `is_durable` (CommitWatermark)   | 8.6 ns   |
| `is_durable` / 0 gaps            | 1.0 ns   |
| `is_durable` / 10 gaps           | 2.4 ns   |
| `is_durable` / 100 gaps          | 8.4 ns   |

### 1.8 Checkpoint — SequenceCheckpoint (fjall)

| Operation          | Time    |
|--------------------|---------|
| `ack` (write)      | 926 ns  |
| `last_checkpoint`  | 109 ns  |
| `restore` / 100    | 3.5 ms  |
| `restore` / 1000   | 3.8 ms  |

**Finding:** `restore` cost is dominated by fjall DB open + iteration, not entry count (100 vs 1000 entries are nearly equal). This runs once on restart — acceptable.

---

## 2. Optimization Priority Matrix

### P0 — High Impact, Low Effort

#### 2a. Default codec: JSON → Postcard

| | JSON (current default) | Postcard |
|--|----------------------|----------|
| Decode 256B | 1,737 ns | 153 ns |
| Encode 64B  | 250 ns   | 54 ns   |
| **Speedup** | baseline | **4–11×** |

**Where it hurts:** The codec is on every `publish()` and every `recv()`. For a 1M msg/s target, switching saves ~1.6 µs/msg = **1.6 CPU-seconds per wall-second**. This is the single largest optimization opportunity in the entire hot path.

**Action:** Change `CodecFormat` default from `Json` to `Postcard`. JSON remains available for debugging/interop.

```rust
// codec.rs — one-line change
#[derive(Debug, Clone, Copy, Default)]
pub enum CodecFormat {
-   #[default]
    Json,
    MsgPack,
+   #[default]
    Postcard,
}
```

**Risk:** Breaking change for existing deployments that rely on default JSON encoding. Mitigate by documenting the change and providing an explicit `CodecFormat::Json` option.

---

#### 2b. Store `query()` → prefix/range scan

**Current:** `query()` iterates **every event** in the `events` keyspace (`store/backend.rs`, `StorageBackend::query()`):

```rust
fn query(&self, filters: &QueryFilters) -> Result<Vec<StoredEvent>> {
    for guard in self.events.iter() {   // ← full scan of ALL events
        // ... filter post-hoc ...
    }
}
```

**Impact:** 100K events → 23.6 ms. This is the recovery query path — a subscriber recovering from a gap calls this with `publisher_id` + `after_seq` filters.

**Keys are already sorted:** The event keyspace uses `[publisher_id(16) | seq(8 BE)]`. When `publisher_id` is specified, we can seek directly to the publisher's key range. When `after_seq` is also specified, we can start at `[publisher_id | after_seq]`.

**Action:** Use fjall's `range()` API to construct a bounded scan:

```rust
fn query(&self, filters: &QueryFilters) -> Result<Vec<StoredEvent>> {
    if let Some(ref pub_id) = filters.publisher_id {
        // Construct key range: [pub_id | after_seq+1 ..= pub_id | u64::MAX]
        let start_seq = filters.after_seq.map(|s| s + 1).unwrap_or(0);
        let start_key = encode_event_key(pub_id, start_seq);
        let end_seq = filters.before_seq.unwrap_or(u64::MAX);
        let end_key = encode_event_key(pub_id, end_seq);

        for guard in self.events.range(start_key..=end_key) {
            // Decode and apply remaining filters (time, limit)
        }
    } else {
        // No publisher filter — fall back to full scan (rare)
        for guard in self.events.iter() { ... }
    }
}
```

**Expected gain:** O(N) → O(result_set). For the common recovery case (100 events from 1 publisher in a 100K-event store), this would drop from **23.6 ms to ~150 µs** (~150×).

---

#### 2c. Store GC → batch all removals

**Current:** GC commits **one batch per deleted event** (`store/backend.rs`, `StorageBackend::gc()`):

```rust
for (event_key, key_expr, hlc_ts) in &to_remove {
    let mut batch = self.db.batch();   // ← new batch per event
    batch.remove(&self.events, ...);
    batch.remove(&self.keys, ...);
    batch.commit()?;                   // ← one fsync per event!
}
```

**Impact:** 50K removals × ~9 µs/commit = **467 ms**.

**Action:** Accumulate all removals into a single batch:

```rust
fn gc(&self, older_than: DateTime<Utc>) -> Result<usize> {
    // ... collect to_remove (unchanged) ...
    let mut batch = self.db.batch();
    for (event_key, key_expr, hlc_ts) in &to_remove {
        batch.remove(&self.events, event_key.as_slice());
        batch.remove(&self.keys, key_expr.as_bytes());
        if let (Some(hlc), Some((pub_id, seq))) = (hlc_ts, decode_event_key(event_key)) {
            batch.remove(&self.replay, encode_replay_key(hlc, &pub_id, seq));
        }
    }
    batch.commit()?;  // ← single commit for all removals
    Ok(to_remove.len())
}
```

**Expected gain:** ~100× (467 ms → ~5 ms for 50K events).

---

#### 2d. Store `compact()` → batch all removals

Same pattern as GC. Currently does individual `self.events.remove()` calls.

**Action:** Collect keys to remove, then batch-delete.

**Expected gain:** ~50× (376 ms → ~5-10 ms).

---

### P1 — Medium Impact, Low Effort

#### 2e. `PublisherWatermark::is_durable()` → binary search

**Current:** `gaps` is a `Vec<u64>` searched with `Vec::contains()` — O(n) linear scan:

```rust
pub fn is_durable(&self, seq: u64) -> bool {
    seq <= self.committed_seq && !self.gaps.contains(&seq)  // O(n)
}
```

**Current impact:** At 100 gaps: 8.4 ns (fast due to L1 cache). But gaps scale with network instability — 10K gaps would make every durable-publish confirmation O(10K).

**Action:** The store-side `BTreeSet` is already sorted. Preserve sort order through serialization and use `binary_search()`:

```rust
pub fn is_durable(&self, seq: u64) -> bool {
    seq <= self.committed_seq && self.gaps.binary_search(&seq).is_err()
}
```

**Expected gain:** O(n) → O(log n). Negligible for small gap counts, material at 1K+ gaps.

---

#### 2f. `store_batch()` → group events by publisher

**Current:** `store_batch()` calls `self.publisher_states.entry_sync(pub_id)` for **every event** in the batch, acquiring and releasing the scc::HashMap entry lock each time.

**Impact:** batch/100 is 6.4 µs/event vs 5.7 µs/event for single writes — the per-event entry_sync overhead accumulates.

**Action:** Pre-group events by `publisher_id`, then process each group with a single `entry_sync` call:

```rust
fn store_batch(&self, events: Vec<(String, Vec<u8>, EventMetadata)>) -> Result<()> {
    let mut batch = self.db.batch();
    // ... insert key/value pairs into batch (unchanged) ...

    // Group by publisher — one entry_sync per publisher, not per event
    let mut by_publisher: HashMap<PublisherId, Vec<u64>> = HashMap::new();
    for (_, _, metadata) in &events {
        by_publisher.entry(metadata.publisher_id).or_default().push(metadata.seq);
    }
    for (pub_id, seqs) in by_publisher {
        let mut entry = self.publisher_states.entry_sync(pub_id)...;
        for seq in seqs {
            // update gap state
        }
    }

    batch.commit()?;
    Ok(())
}
```

**Expected gain:** 10–30% improvement for mixed-publisher batches.

---

### P2 — Low Impact / Future

| Item | Current | Optimization | Gain |
|------|---------|-------------|------|
| Watermark serde (JSON) | 10 µs deserialize / 100 pubs | Switch to MsgPack or Postcard | 2–3× (but runs every 100ms — <0.1% CPU) |
| `Event::new()` UUID v7 | 199 ns | Pre-allocate or batch UUIDs | Marginal; correctness requires unique IDs |
| `query_replay()` | 80 µs / 10K | Already uses sorted HLC keys with early break | Good enough |
| Checkpoint restore | 3.5 ms / 100 entries | One-time cost on restart | No action |

---

## 3. Hot Path Cost Breakdown

### Publish path — single 256-byte event

**Current (JSON default):**

```
Event::new()          200 ns   ██████████
codec.encode(JSON)    250 ns   ████████████▌
encode_metadata()      25 ns   █▎
cache insert           ~5 ns   ▎
                       ─────
Total CPU             480 ns   (before Zenoh session.put)
```

**After Postcard default:**

```
Event::new()          200 ns   ██████████
codec.encode(Postcard) 54 ns   ██▋
encode_metadata()      25 ns   █▎
cache insert           ~5 ns   ▎
                       ─────
Total CPU             284 ns   (41% reduction)
```

### Subscribe path — single 256-byte event

**Current (JSON default):**

```
decode_metadata()      13 ns   ▋
gap_detector.on_sample 43 ns   ██▎
channel send           ~5 ns   ▎
codec.decode(JSON)   1737 ns   ████████████████████████████████████████████████████████████████████████████████████████
                       ─────
Total CPU            1798 ns   (codec is 97% of cost!)
```

**After Postcard default:**

```
decode_metadata()      13 ns   ▋
gap_detector.on_sample 43 ns   ██▎
channel send           ~5 ns   ▎
codec.decode(Postcard)153 ns   ████████
                       ─────
Total CPU             214 ns   (88% reduction)
```

---

## 4. Implementation Order

```
Phase 1 — Quick wins (P0, no API changes)
  ├── 2c. GC batch commit           ← trivial refactor, ~100× GC speedup
  ├── 2d. Compact batch commit      ← trivial refactor, ~50× compact speedup
  └── 2e. is_durable binary_search  ← one-line change

Phase 2 — Codec default (P0, API/wire change)
  └── 2a. Default codec → Postcard  ← one-line change + migration notice

Phase 3 — Store query optimization (P0, internal refactor)
  └── 2b. query() range scan        ← moderate refactor of query()

Phase 4 — Batch optimization (P1, internal refactor)
  └── 2f. store_batch publisher grouping  ← moderate refactor
```

---

## 5. Validation

After applying each optimization, re-run the corresponding benchmark suite and compare against the saved baseline:

```bash
# Save baseline before changes
cargo bench -p mitiflow --features full -- --save-baseline before

# Apply optimizations...

# Compare against baseline
cargo bench -p mitiflow --features full -- --baseline before
```

Criterion HTML reports are generated in `target/criterion/`.
