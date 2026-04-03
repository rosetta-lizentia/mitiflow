# Key-Based Publishing

Design for adding Kafka-style message keys to mitiflow's publish API —
embedding application-level keys in the Zenoh key expression so that
subscribers can filter by key natively.

**Status:** Implemented — all four phases complete (core keyed publish, store key index, log compaction, subscriber convenience).

**Related docs:**
[04_sequencing_and_replay.md](04_sequencing_and_replay.md),
[04_sequencing_and_replay.md](04_sequencing_and_replay.md),
[02_architecture.md](02_architecture.md),
[11_consumer_group_commits.md](11_consumer_group_commits.md)

---

## Part 1: Motivation

### What Kafka Keys Do

In Kafka, `ProducerRecord(topic, key, value)` uses the key for three things:

1. **Partition assignment.** `hash(key) % num_partitions` determines which
   partition the record goes to. All records with the same key land in the
   same partition, guaranteeing per-key ordering.

2. **Log compaction.** When compaction runs, Kafka retains only the latest
   record for each key within a partition. This enables "table" semantics —
   an event log that doubles as a materialised snapshot of current state.

3. **Consumer routing / filtering.** While Kafka itself doesn't filter by key
   server-side (consumers read entire partitions), downstream processors use
   the key to route events to the correct handler, state store, or join
   operator.

### What mitiflow Has Today

Currently, mitiflow's publish API is key-unaware at the application level:

```rust
// Publisher sends event to default partition 0
publisher.publish(&event).await?;

// Publisher sends to a specific Zenoh key expression
publisher.publish_to("myapp/events/p/2/42", &event).await?;
```

The Zenoh key expression encodes **partition and sequence** but not an
application-level key. The caller can specify a full key expression in
`publish_to`, but there is no structured "key" concept — it's a raw Zenoh
path. Partition assignment is either explicit (caller picks partition) or
implicit via `PartitionManager::partition_for(key)`, but that key is an
external input, not part of the published message metadata.

### Why Add Keys?

| Capability | Without keys | With keys |
|-----------|-------------|-----------|
| Partition routing | Caller manually maps key → partition, or uses `PartitionManager` externally | Automatic: `publish_keyed("order-123", &event)` → hash → partition |
| Per-key ordering | Possible if caller is disciplined | Guaranteed by construction (same key → same partition → same publisher stream) |
| Key-based subscribe | Subscriber gets all events in a partition, filters in application code | Subscriber uses Zenoh key expression to filter: `myapp/events/p/0/k/order-123/**` |
| Log compaction | Not possible — store doesn't know which events supersede others | Store retains latest per key (compacted view) |
| Key-based replay | Not possible — store query returns all events in partition | Store can return "latest value for each key" or "history for key X" |
| Consumer routing | Application deserialises event to determine type/entity | Key available in `RawEvent` without payload deserialisation |

The gap that matters most for mitiflow is **key-based subscribe**: because
Zenoh's key expression matching happens in the routing layer (zero-copy, before
delivery to application code), embedding the key in the topic structure enables
high-performance server-side filtering that no amount of application-level
filtering can match.

---

## Part 2: Is It Worth It?

### The Ordering Question

mitiflow uses **Approach C: Per-(Partition, Publisher) Sequences** from
[04_sequencing_and_replay.md](04_sequencing_and_replay.md). This means:

- Within a single publisher's stream for a single partition, events are
  **strictly ordered and contiguous**.
- Across publishers, events in the same partition have **no total order** —
  only approximately-physical-time ordering via HLC (see
  [04_sequencing_and_replay.md](04_sequencing_and_replay.md)).

Kafka, by contrast, provides **total order per partition** because the broker
assigns a monotonic offset to every record regardless of producer.

**Impact on key-based semantics:**

| Guarantee | Kafka | mitiflow |
|-----------|-------|----------|
| Per-key ordering (single producer) | Total | Strict (same: one producer + same partition = ordered stream) |
| Per-key ordering (multi-producer) | Total (broker-serialised) | Approximate (HLC-ordered, not causally ordered) |
| Log compaction correctness | Always correct (total order determines "latest") | Correct per producer; across producers, "latest" is HLC-max (may diverge from causal latest under clock skew) |
| Key-scoped subscribe | Not built-in (all events in partition delivered) | Native via Zenoh key expression matching |

**The honest assessment:** If multiple publishers write to the same key, mitiflow
cannot guarantee which event is "latest" in a causal sense — only which has the
highest HLC timestamp. For NTP-synchronised hosts in the same datacenter
(< 1ms skew), this is indistinguishable from correct. For cross-region
deployments with 10-50ms skew, an event from publisher P2 could appear "after"
an event from P1 that was causally later but had a lower physical clock.

However, this limitation already exists at the partition level — adding keys
doesn't make it worse. And the single-producer case (which is the vast majority
of real workloads: one service instance owns writes for an entity) has identical
guarantees to Kafka.

### Benefits vs. Costs

**Benefits:**

1. **Automatic partition affinity.** `publish_keyed("user-42", &event)` handles
   hashing internally. Removes a class of bugs where callers misconfigure
   partition routing and break per-entity ordering.

2. **Zenoh-native key filtering.** Subscribers requesting events for a specific
   key or key prefix get them filtered at the Zenoh routing mesh level — no
   application-level filtering, no wasted bandwidth. This is a capability Kafka
   doesn't have.

3. **Log compaction.** Enables "latest state per entity" queries from the store.
   Essential for event sourcing, CDC, and state recovery after restart. Without
   keys, every consumer must replay the full log.

4. **Key-scoped replay.** "Give me all events for order-123" becomes a store
   query by key — no partition scan needed.

5. **Simpler consumer group semantics.** Consumers already process by partition;
   keys give them structured routing within a partition without deserialising
   the payload.

6. **Aligns with user expectations.** Engineers familiar with Kafka, Pulsar,
   or Redis Streams expect key-value semantics. Reduces friction for adoption.

**Costs:**

1. **Key expression cardinality.** High-cardinality keys (millions of unique
   entity IDs) create large Zenoh key expression spaces. Zenoh handles this
   well (trie-based routing), but it's worth benchmarking.

2. **Compaction complexity.** Building a correct compaction strategy with
   per-(partition, publisher) ordering requires tombstone handling, retention
   policies, and interaction with the replay index. This is a significant
   implementation effort.

3. **Storage overhead.** Adding a key index to the store (in addition to the
   existing primary and replay indexes) increases write amplification.

4. **API surface expansion.** New publish/subscribe methods, config options,
   and store query patterns.

5. **Multi-producer compaction ambiguity.** As discussed above, "latest per key"
   across multiple producers is HLC-based, not causally ordered. Applications
   must be aware of this if they have multiple writers per key.

### Verdict

**Worth it.** The key-based filtering via Zenoh key expressions is a unique
advantage over Kafka — it's a feature Kafka cannot offer architecturally
because Kafka's brokers deliver whole partitions. Combined with automatic
partition affinity, key-scoped replay, and log compaction (even with the
weaker multi-producer ordering), keys make mitiflow significantly more useful
for event sourcing, CDC, and entity-centric streaming workloads.

The ordering limitation (no total order across producers for the same key) is
a known property of the brokerless architecture, not a regression from adding
keys. The workaround is the same as for partitions: use HLC for approximate
ordering, or design your system so that a given key is owned by a single
producer (which is the dominant pattern in practice).

---

## Part 3: Key Expression Layout

### Current Layout

```
{key_prefix}/p/{partition}/{seq}
```

Example: `myapp/events/p/2/42` means partition 2, sequence 42.

### Keyed Layout (decided)

```
{key_prefix}/p/{partition}/k/{key}/{seq}
```

Example: `myapp/events/p/2/k/order-123/42` means partition 2, key `order-123`,
sequence 42.

The `/k/` segment separates the key from the partition and sequence, keeping
the layout self-describing and parsable. The key sits between partition (used
for routing/assignment) and sequence (used for ordering/dedup).

**Keyless events** (backward compatible) retain the current layout:

```
{key_prefix}/p/{partition}/{seq}
```

Both layouts coexist. Subscribers use `{key_prefix}/p/{partition}/**` to
receive all events (keyed and unkeyed) in a partition.

### Zenoh Key Expression Matching Patterns

This layout enables powerful Zenoh-native filtering:

| Goal | Key expression | Matches |
|------|---------------|---------|
| All events, all partitions | `{prefix}/**` | Everything |
| All events in partition 2 | `{prefix}/p/2/**` | Keyed + unkeyed in partition 2 |
| Specific key, any partition | `{prefix}/p/*/k/order-123/*` | `order-123` regardless of partition |
| Specific key in partition 2 | `{prefix}/p/2/k/order-123/*` | `order-123` in partition 2 only |
| Key prefix (hierarchical) | `{prefix}/p/2/k/user/42/**` | All events under `user/42` subtree |
| All keyed events in partition | `{prefix}/p/{partition}/k/**` | All keyed events (excludes unkeyed) |

**Hierarchical keys** are natural: a key like `user/42/orders` becomes
`{prefix}/p/0/k/user/42/orders/{seq}`, and `{prefix}/p/0/k/user/42/**`
matches all events for user 42 across all sub-keys.

### Why `/k/` Instead of Inlining the Key Directly?

Three reasons:

1. **Parsing.** Without a sentinel, the subscriber can't distinguish
   `{prefix}/p/2/order-123/42` — is `order-123` a key or part of the prefix?
   The `/k/` segment makes it unambiguous.

2. **Mixed keyed/unkeyed.** `{prefix}/p/2/42` (unkeyed, seq=42) and
   `{prefix}/p/2/k/order-123/42` (keyed, seq=42) can coexist. Without `/k/`,
   `{prefix}/p/2/order-123/42` is ambiguous with a multi-segment unkeyed
   prefix.

3. **Filter separation.** `{prefix}/p/*/k/{key}/*` can match a specific key
   across all partitions. Without the `/k/` sentinel, there's no clean
   wildcard pattern that means "any partition, this specific key."

### Key Encoding

Keys are embedded verbatim in the Zenoh key expression. Since Zenoh key
expressions are `/`-separated and support `*` and `**` wildcards, the
following characters must be handled:

| Character | Handling |
|-----------|----------|
| `/` | Allowed — enables hierarchical keys (`user/42/orders`) |
| `*` | **Forbidden** in keys — conflicts with Zenoh wildcards |
| `$` | **Forbidden** — reserved in Zenoh for `$*` |
| `#` | Allowed |
| Spaces, UTF-8 | Allowed — Zenoh supports UTF-8 key expressions |

Validation at publish time:

```rust
fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidKey("key must not be empty"));
    }
    if key.contains('*') || key.contains('$') {
        return Err(Error::InvalidKey("key must not contain * or $"));
    }
    Ok(())
}
```

---

## Part 4: API Design

### Publisher API

New methods on `EventPublisher`:

```rust
/// Publish a keyed event. Partition is determined by hash(key).
pub async fn publish_keyed<T: Serialize>(
    &self,
    key: &str,
    event: &Event<T>,
) -> Result<u64>

/// Publish pre-serialised bytes with a key.
pub async fn publish_bytes_keyed(
    &self,
    key: &str,
    bytes: Vec<u8>,
) -> Result<u64>

/// Durable publish with key (waits for watermark confirmation).
pub async fn publish_keyed_durable<T: Serialize>(
    &self,
    key: &str,
    event: &Event<T>,
) -> Result<u64>
```

**Internal flow:**

```
publish_keyed("order-123", &event)
  → validate_key("order-123")
  → partition = hash("order-123") % num_partitions
  → seq = partition_seqs[partition].fetch_add(1)
  → key_expr = "{prefix}/p/{partition}/k/order-123/{seq}"
  → zenoh_put(key_expr, payload, attachment)
```

The existing `publish()` (unkeyed) and `publish_to()` (raw key expression)
remain unchanged for backward compatibility.

### Event Metadata

The `key` is extracted from the Zenoh key expression at zero cost — no
additional wire overhead. The fixed 50-byte binary attachment header remains
unchanged.

```rust
impl RawEvent {
    pub fn key(&self) -> Option<&str> {
        // key_expr = "{prefix}/p/{partition}/k/{key}/{seq}"
        // Find "/k/" and extract between it and the last "/"
        let k_pos = self.key_expr.find("/k/")?;
        let after_k = &self.key_expr[k_pos + 3..];
        let last_slash = after_k.rfind('/')?;
        Some(&after_k[..last_slash])
    }
}
```

This returns a `&str` slice into the already-owned key expression — no
allocation, no parsing beyond two `find` calls. If profiling reveals this is
hot, the parsed value can be cached lazily.

### Subscriber API

No API changes needed. Subscribers already use `{prefix}/**` or
`{prefix}/p/{partition}/**`, which matches the new layout. Key-filtered
subscriptions are just a different key expression:

```rust
// Subscribe to all events for a specific key
let config = EventBusConfig::builder("myapp/events").build()?;
let sub = EventSubscriber::new_with_key_expr(
    &session,
    config,
    "myapp/events/p/*/k/order-123/*",  // Zenoh handles the filtering
).await?;
```

Or with a helper:

```rust
impl EventBusConfig {
    /// Key expression matching a specific key across all partitions.
    pub fn key_expr_for_key(&self, key: &str) -> String {
        format!("{}/p/*/k/{}/*", self.key_prefix, key)
    }

    /// Key expression matching a key prefix across all partitions.
    pub fn key_expr_for_key_prefix(&self, key_prefix: &str) -> String {
        format!("{}/p/*/k/{}/**", self.key_prefix, key_prefix)
    }
}
```

---

## Part 5: Store Integration

### Storage Key Layout

Add a key-based index to the `FjallBackend`, alongside the existing primary
(publisher_id + seq) and replay (HLC) indexes.

**Key index keyspace:**

```
Keyspace: keys
Key:   [key_hash: 8 bytes][hlc_physical: 8 bytes BE][hlc_logical: 4 bytes BE][publisher_id: 16 bytes]
Value: [publisher_id: 16 bytes][seq: 8 bytes BE]  (pointer to primary index)
```

The `key_hash` is a stable hash of the application key (e.g., xxHash64 of
`"order-123"`). Using a hash instead of the raw key avoids variable-length
prefixes in the LSM key, keeping range scans efficient.

The raw key string is stored in the event metadata (already present via
`key_expr` in `EventMetadata`).

**Why key_hash first?** Grouping by key_hash in the LSM tree makes key-scoped
queries (all events for key X, latest event for key X) efficient prefix scans.

### Key-Scoped Queries

New query methods on `StorageBackend`:

```rust
/// Return all events for a given key, in HLC order.
fn query_by_key(&self, key: &str, limit: Option<usize>) -> Result<Vec<StoredEvent>>;

/// Return the latest event for each distinct key (compacted view).
fn query_latest_by_keys(&self, keys: &[&str]) -> Result<Vec<StoredEvent>>;
```

### Log Compaction

Log compaction retains only the latest event per key within the store.

**Approach: Background compaction** (Kafka model). A periodic background task
scans the key index and deletes superseded entries:

```
For each unique key_hash:
  Find the entry with the highest HLC timestamp
  Delete all other entries for that key_hash from all three indexes
  (primary, replay, and key indexes)
```

**When to run:** Configurable interval or when storage exceeds a threshold.

**Tombstones:** A keyed event with a `None`/empty payload is a tombstone — it
signals deletion. After compaction, tombstones are retained for a configurable
retention period, then removed ("tombstone GC").

This approach keeps the write path fast (no read-before-write) and is naturally
suited to fjall's LSM architecture, which already performs background
compaction at the storage layer. Space is reclaimed asynchronously — the
multi-producer "latest" ambiguity is resolved by HLC max.

### Multi-Producer Compaction Correctness

When multiple producers write to the same key, "latest" is determined by HLC
timestamp. This is correct under the assumption that:

1. Clocks are NTP-synchronised (< 1ms skew in same datacenter).
2. If two events are truly concurrent (no causal relationship), either one is
   a valid "latest" — the application doesn't depend on which wins.

For applications that need strict last-writer-wins with multiple producers,
they should embed a logical version or vector clock in the payload. This is
the same tradeoff as any eventually-consistent system — mitiflow doesn't
pretend to be strongly consistent across producers.

---

## Part 6: Interaction with Existing Systems

### Gap Detection

No change. Gap detection operates on `(partition, publisher, seq)` triples.
The key is orthogonal — it's a routing/filtering concept, not an ordering
concept. The gap detector doesn't need to know about keys.

### Watermark Protocol

No change. Watermarks track `committed_seq` per `(partition, publisher)`. The
key doesn't affect durability confirmation.

### Consumer Groups

No change to partition assignment. `PartitionManager` still assigns whole
partitions to consumers. Within a partition, consumers see all keys (keyed
and unkeyed).

Key-filtered subscription within a consumer group is an application concern:
a consumer assigned partition 2 could further filter to `prefix/p/2/k/user/**`
if it only cares about user events. This is a Zenoh subscription filter, not
a mitiflow abstraction.

### HLC Replay Index

The replay index remains unchanged — it's ordered by
`(hlc, publisher_id, seq)` and doesn't reference keys. Key-ordered replay
uses the key index instead.

### Publisher Cache

The publisher cache stores `CachedSample` with the full key expression,
including the key segment. Recovery queries via `_cache/{publisher_id}` return
events with their original key expressions — no changes needed.

---

## Part 7: Comparison with Kafka

| Dimension | Kafka | mitiflow (with keys) |
|-----------|-------|---------------------|
| Key → partition routing | Built-in (DefaultPartitioner) | Built-in (`publish_keyed` hashes key) |
| Per-key ordering (1 producer) | Total | Strict (equivalent) |
| Per-key ordering (N producers) | Total (broker-serialised) | HLC-approximate (within NTP skew) |
| Key-based consumer filtering | Not available (consumers read full partition) | **Zenoh-native key expression filtering** |
| Hierarchical key filtering | Not available | `prefix/p/*/k/user/42/**` |
| Log compaction | Built-in, per partition | Implementable, per partition |
| Compaction correctness | Always correct (total order) | Correct per producer; HLC-based across producers |
| Key in wire protocol | Yes (explicit field in ProducerRecord) | Embedded in Zenoh key expression (zero overhead) |
| Broker involvement | Broker assigns offset, routes to partition leader | None — publisher hashes key and publishes directly |

**mitiflow's unique advantage:** key-based subscribe filtering at the Zenoh
mesh layer. A Kafka consumer must read every record in its assigned partitions
and filter in application code. A mitiflow subscriber can subscribe to
`prefix/p/*/k/user/42/*` and receive only events for that key — the Zenoh
router drops non-matching events before they reach the application. This is
particularly valuable for:

- **Microservices** that care about a subset of entities (e.g., a service that
  handles orders for a specific region).
- **Real-time dashboards** that display data for a specific entity.
- **Event-driven triggers** that fire only for specific keys.

---

## Part 8: Implementation Plan

### Phase 1: Core keyed publish ✅

1. **Key validation** — `validate_key()` rejects `*`, `$`, empty keys (`attachment.rs`)
2. **`publish_keyed()` family** — `publish_keyed`, `publish_bytes_keyed`,
   `publish_keyed_durable`, `publish_bytes_keyed_durable` (`publisher/mod.rs`)
3. **Key expression construction** — `{prefix}/p/{partition}/k/{key}/{seq}`
4. **`RawEvent::key()` accessor** — zero-alloc extraction from key expression (`event.rs`)
5. **`extract_partition()` update** — handles both keyed and unkeyed layouts
6. **Config helpers** — `key_expr_for_key()`, `key_expr_for_key_prefix()` (`config.rs`)
7. **13 tests** in `tests/keyed_publish.rs`

### Phase 2: Store key index ✅

1. **`query_by_key()`** and **`query_latest_by_keys()`** on `StorageBackend` trait
2. **`FjallBackend`** implements both via filtered scan on `meta.key`
3. **`EventMetadata.key`** field populated from key expression

### Phase 3: Log compaction ✅

1. **`compact_keyed()`** on `FjallBackend` — keeps latest per application key
2. **`compaction_interval`** config for automatic periodic compaction
3. **Replay index cleanup** during keyed compaction

### Phase 4: Subscriber convenience ✅

1. **`EventSubscriber::new_keyed()`** — subscribe to a specific key
2. **`EventSubscriber::new_key_prefix()`** — subscribe to a key prefix
3. **Example** — `examples/keyed_pubsub.rs`

---

## Part 9: Resolved Questions

1. **Key in attachment vs. key expression only?** Key lives in the Zenoh key
   expression only. `EventMetadata.key` is populated by parsing the key
   expression — no wire overhead. Resolved in Phase 2.

2. **Maximum key length?** No hard limit enforced. Zenoh handles large key
   expression spaces well (trie-based routing).

3. **Key expression collision with internal keys.** No collision — the `/k/`
   segment is distinct from `_` prefixed internal keys.

4. **Compaction and the replay index.** Yes — `compact_keyed()` cleans up
   replay index entries for deleted events. Implemented.

5. **Interaction with WAL.** Deferred — WAL feature (`wal`) not yet
   implemented.
