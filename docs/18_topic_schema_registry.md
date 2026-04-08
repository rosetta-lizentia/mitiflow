# Topic Schema Registry

Design for a distributed topic schema registry that guarantees deterministic
per-topic schema discovery without startup-order races, while preserving the
original goal of storage-served durability.

**Status:** Revised design — Phases 1-2 implemented, Phase 3 planned
**Date:** 2026-04-07

**Related docs:**
[16_dx_and_multi_topic.md](16_dx_and_multi_topic.md),
[13_distributed_storage.md](13_distributed_storage.md),
[02_architecture.md](02_architecture.md),
[configuration.md](configuration.md)

---

## 1. Problem Statement

The original schema registry implementation correctly identified the wire-level
problem to solve, but coupled correctness to a best-effort pub/sub path.

Publishers and subscribers perform exact per-topic schema lookups:

```
session.get("{key_prefix}/_schema")
```

That contract is sound and should remain unchanged.

The failure came from how the system served and propagated schemas:

1. The orchestrator published schemas once at startup and expected storage
   agents to catch them.
2. Storage agents subscribed after startup and sometimes missed those puts.
3. The orchestrator's queryable did not match the exact per-topic query keys.

The result was a startup-order race where schema queries timed out if storage
agents had not already received and persisted the schema before the first
consumer tried to fetch it.

The redesign keeps the same client-facing query key and validation model, but
changes the system invariant:

**Exact-key query is the correctness path. Pub/sub is only a propagation path.**

### 1.1 What Failed In The Initial Implementation

The previous implementation failed in three distinct ways that interacted badly:

| Problem | What happened | Effect |
|---------|----------------|--------|
| Queryable key mismatch | The orchestrator declared a single root `_schema` queryable instead of exact per-topic queryables | Exact per-topic `get()` calls timed out whenever storage agents had not already persisted the schema |
| Orchestrator-to-storage race | Startup `publish_all_schemas()` ran before storage topic workers subscribed | Late-starting storage agents missed schema publishes entirely |
| `schema_version = 0` default | Bootstrapped topics defaulted to version `0` and storage treated repeated publishes as stale | Restarts produced misleading stale-version behavior and complicated recovery |

The critical dependency chain:

```
root queryable mismatch
   -> orchestrator cannot answer exact per-topic schema queries
   -> only storage agents can answer
   -> storage startup race leaves stores empty
   -> no respondent replies to fetch_schema()
   -> consumer receives TopicSchemaNotFound
```

---

## 2. Design Goals

The revised design optimizes for the following:

1. Exact per-topic schema discovery must work regardless of startup order.
2. When an orchestrator exists, it remains the authoritative writer.
3. Storage agents remain the durable steady-state responders.
4. Schema propagation must be idempotent across restarts and redeployments.
5. A missing schema on one topic must not affect other topics.
6. Existing client APIs and query semantics should remain stable.

Non-goals:

- Introducing a new external schema registry service
- Changing the public query key format away from `{key_prefix}/_schema`
- Making application workers self-register schemas in orchestrator-managed
  deployments by default

---

## 3. Chosen Architecture

The selected design is:

**Authoritative writes, exact-key reads, storage-served steady state**

Responsibilities are split as follows:

| Component | Responsibility |
|-----------|----------------|
| Orchestrator | Authoritative writer, per-topic exact-key schema responder, bootstrap source for cold storage agents |
| Storage agents | Durable schema cache, live schema subscriber, startup puller, steady-state exact-key responder |
| Publishers / Subscribers | Exact-key schema fetchers; validate or auto-configure from first valid reply |
| CLI / first publisher | Optional bootstrap path only for deployments without an orchestrator |

### Why this design

- It fixes the race at the root cause instead of masking it.
- It preserves the original operational model: storage agents still serve
  schemas after warm-up and during orchestrator downtime.
- It avoids introducing a new dependency or changing client behavior.
- It keeps orchestration policy in the orchestrator, not in application workers.

### System view

```
                     exact get {topic_prefix}/_schema
           +------------------------------------------------+
           |                                                |
           v                                                |
  +-------------------+        notify put                   |
  | Publisher /       | <-----------------------------+     |
  | Subscriber        |                               |     |
  | Validate /        |                               |     |
  | AutoConfig        |                               |     |
  +-------------------+                               |     |
           ^                                          |     |
           | first valid reply                        |     |
           |                                          |     |
  +-------------------+        bootstrap get          |     |
  | Storage Agent     | ------------------------------+     |
  | local schema      |                                     |
  | store + queryable | <-----------------------------------+
  +-------------------+            exact get {topic_prefix}/_schema
           ^
           |
           | authoritative write + exact-key reply
  +-------------------+
  | Orchestrator      |
  | topic config +    |
  | schema metadata   |
  +-------------------+
```

---

## 4. Core Invariants

The redesign depends on a small set of explicit rules.

### 4.1 Query is the correctness path

Clients and storage agents must assume that the only reliable way to discover
the current schema is an exact-key query to `{key_prefix}/_schema`.

Pub/sub delivery is best-effort. It is useful for convergence and cache warm-up,
but correctness cannot depend on it.

### 4.2 Responders answer exact per-topic keys only

There is no root-level schema queryable such as `{cluster_prefix}/_schema`.
Every responder answers the exact topic key:

```
myapp/orders/_schema
myapp/events/_schema
myapp/metrics/_schema
```

This matches the existing client contract and avoids wildcard parsing or key
intersection ambiguity.

### 4.3 Version zero is invalid

`schema_version = 0` is reserved for "unset" and must never be published as a
valid schema version.

The first real schema version is `1`.

### 4.4 Same version must be idempotent

A repeated publish of the same schema version must be handled as follows:

- Same version, same wire contract: accept as a no-op
- Same version, different wire contract: reject as a conflict
- Lower version: reject as stale
- Higher version: accept and persist

This makes schema propagation safe across restarts and repeated bootstrap.

### 4.5 Schema metadata must be stable

The orchestrator must not synthesize a new `created_at` or `updated_at`
timestamp on every reply.

Repeated replies for the same schema version must be stable. The orchestrator
therefore needs persisted schema metadata, not a fresh in-memory reconstruction
using "now" each time.

### 4.6 Per-topic failure isolation

Schema lookup failure on one topic should not cascade to other topics or
unrelated application functionality. `TopicSchemaNotFound` is a per-topic,
recoverable error.

---

## 5. Data Model

The public `TopicSchema` shape stays the same. The redesign changes how the
orchestrator persists and reconstructs it.

```rust
pub struct TopicSchema {
    pub name: String,
    pub key_prefix: String,
    pub codec: CodecFormat,
    pub num_partitions: u32,
    pub key_format: KeyFormat,
    pub schema_version: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

### 5.1 Orchestrator persistence

The lowest-migration implementation is:

- Keep `TopicConfig` as the control-plane record
- Add persisted schema timestamps to `TopicConfig`
- Derive `TopicSchema` from those persisted fields when replying or publishing

That is sufficient to make replies stable without introducing a second schema
database in the orchestrator.

Suggested additional persisted fields:

```rust
pub struct TopicConfig {
    // existing fields...
    pub schema_version: u32,
    pub schema_created_at: DateTime<Utc>,
    pub schema_updated_at: DateTime<Utc>,
}
```

If schema lifecycle logic later becomes more complex, a dedicated orchestrator
schema keyspace can be introduced. It is not required for the first fix.

### 5.2 Storage-side idempotence

The storage agent should compare incoming schemas on the wire-contract fields:

- `name`
- `key_prefix`
- `codec`
- `num_partitions`
- `key_format`
- `schema_version`

Timestamps should not be allowed to create false conflicts for an otherwise
identical schema.

---

## 6. Protocol

### 6.1 Exact-key query protocol

All readers use the same query shape:

```rust
session.get("{key_prefix}/_schema")
```

Rules:

1. Responders reply on the same exact key.
2. Clients accept the first valid schema reply.
3. If no responder answers before timeout, clients return `TopicSchemaNotFound`.
4. No wildcard query is required for normal schema discovery.

This preserves the existing API in the core crate.

### 6.2 Authoritative write protocol

When the orchestrator creates or updates a topic:

1. Validate the requested evolution
2. Assign the canonical schema version
3. Persist the updated `TopicConfig` including stable schema timestamps
4. Ensure the per-topic schema queryable exists for that topic
5. Publish the schema to `{key_prefix}/_schema` as a notification

The publish is a cache update signal for storage agents and late readers. It is
not the only durable source of truth.

### 6.3 Storage agent warm-up protocol

Each topic worker follows this lifecycle:

```
Topic worker start
  -> subscribe to {key_prefix}/_schema for live updates
  -> read local schema store
       -> if present: serve immediately
       -> if missing: enter bootstrap loop
             -> exact get {key_prefix}/_schema
             -> if reply: persist locally and serve
             -> if no reply: retry with backoff until shutdown
  -> keep subscriber running for newer schema versions
```

Important consequences:

- A storage agent that starts late no longer depends on having caught the
  orchestrator's startup publish.
- If the orchestrator is unavailable but another storage agent already has the
  schema, bootstrap still succeeds.
- If no responder has the schema, the topic worker logs a clear "schema missing"
  state instead of silently timing out forever.

### 6.4 Startup publish remains optional warm-up

`publish_all_schemas()` may remain as a startup best-effort cache warm-up for
backward compatibility and faster convergence.

However, the system must be correct even if every one of those publishes is
missed.

---

## 7. Component Behavior

### 7.1 Orchestrator

The orchestrator must declare one exact-key schema queryable per topic.

That queryable set changes with topic lifecycle:

- On startup: create queryables for all persisted topics
- On topic create: create a queryable for the new topic
- On topic delete: remove the queryable for that topic
- On topic update: keep the same queryable, update the persisted schema it serves

This is preferred over a single wildcard queryable because topic key prefixes
are already modeled as per-topic values, and clients already query exact keys.

### 7.2 Storage agents

Storage agents remain the durable steady-state responders.

Their responsibilities are:

- Persist schemas locally in fjall
- Serve exact-key queries from the local store
- Subscribe for live schema updates
- Perform startup pull if the local store is empty

Storage agents do not become authoritative writers in orchestrator-managed
production. They mirror and serve the canonical schema.

### 7.3 Publishers and subscribers

Client behavior stays stable:

- `Disabled`: no schema lookup
- `Validate`: fetch exact schema and compare local config
- `AutoConfig`: fetch exact schema and overwrite wire-level config
- `RegisterOrValidate`: only for deployments without authoritative control plane

`RegisterOrValidate` remains useful for local development and orchestrator-less
deployments, but should not be the default for orchestrator-managed production.

---

## 8. Failure Behavior

The revised design intentionally separates control-plane correctness from
startup timing.

| Situation | Expected behavior |
|-----------|-------------------|
| Orchestrator up, storage cold | Clients succeed via orchestrator; storage agents bootstrap by exact query and then serve locally |
| Orchestrator down, storage warm | Clients succeed via storage agents |
| Orchestrator down, one storage warm, one storage cold | Cold storage agent bootstraps from warm storage agent |
| Orchestrator up, startup schema publish missed | No correctness impact; storage agent still bootstraps by query |
| No responder has schema | `Validate` and `AutoConfig` fail with `TopicSchemaNotFound` |

This eliminates startup order as a correctness variable.

---

## 9. Consumer Integration Guidance

Applications consuming multiple Mitiflow topics should handle schema lookup
failures gracefully rather than treating them as fatal.

### 9.1 Recommended policy for orchestrator-managed topics

For topics managed by the orchestrator:

- Use `Validate` mode in publisher and subscriber configs
- Do not switch to `RegisterOrValidate` as a workaround for registry bugs

The registry should be fixed at the infrastructure level.

### 9.2 Failure containment

Applications that initialize multiple publishers or subscribers should do so
independently per topic:

- A failed schema lookup on one topic should disable that topic's publisher or
  subscriber, not collapse the entire application
- Initialization should be capability-based: each topic either succeeds or
  enters a retry/degraded state independently

This is an application-level concern, but the design should make it natural:

- `TopicSchemaNotFound` is a recoverable, per-topic error
- Applications can retry individual topic lookups with backoff
- Unaffected topics should continue operating normally

---

## 10. Migration Strategy

The migration should be staged so that each step improves correctness without
requiring a flag day.

### Stage 1: Orchestrator becomes a correct responder

Ship the per-topic exact-key queryable change first.

After this stage:

- Clients can fetch schemas before any storage agent has warmed up
- Existing storage agents continue to work unchanged
- The startup race is no longer fatal for readers

### Stage 2: Storage agents become self-warming

Add the bootstrap query loop and idempotent same-version handling.

After this stage:

- Storage agents no longer depend on startup publish timing
- Restarting an agent with an existing volume becomes safe and quiet
- Schema durability works as originally intended

### Stage 3: Consumers stop cascading failures

Consuming applications adopt per-topic failure containment so missing Mitiflow
capabilities do not terminate unrelated functionality.

After this stage:

- A transient schema outage becomes degraded functionality instead of a fatal error
- Observability improves because the first transport error is logged in context

### Stage 4: Cleanup and hardening

After the correctness path is fixed, add:

- better schema readiness diagnostics
- startup-order integration tests
- rollout docs and operational guidance

---

## 11. Implementation Plan

The concrete rollout below is designed for the current code layout.

### Phase 1: Fix orchestrator schema serving

Primary goal: make exact per-topic query work before any storage agent starts.

Files:

- `mitiflow-orchestrator/src/orchestrator.rs`
- `mitiflow-orchestrator/src/config.rs`

Changes:

1. Replace the single `{cluster_prefix}/_schema` queryable with one queryable
   per topic key prefix.
2. Manage queryable lifecycle on startup, topic create, and topic delete.
3. Persist stable schema timestamps in `TopicConfig` so repeated replies for the
   same schema version are deterministic.
4. Change `schema_version` defaults from `0` to `1`, or reject `0` during
   bootstrap normalization.
5. Keep `publish_all_schemas()` only as a warm-up helper.

Acceptance criteria:

- Querying `{topic_key_prefix}/_schema` succeeds with only the orchestrator running.
- Repeated queries return stable schema metadata for the same version.
- Bootstrapped topics do not publish or serve version `0`.

### Phase 2: Fix storage bootstrap and idempotence

Primary goal: make late-starting storage agents converge without relying on
startup pub/sub timing.

Files:

- `mitiflow-storage/src/topic_worker.rs`
- `mitiflow-storage/src/schema_store.rs`

Changes:

1. On topic worker startup, subscribe to live `_schema` updates and then load
   the local schema store.
2. If no local schema exists, run an exact-key bootstrap fetch loop with backoff.
3. Persist fetched schema before serving it locally.
4. Replace strict `incoming <= current` rejection with idempotent same-version
   handling and explicit equal-version conflict detection.
5. Emit clear logs for `schema missing`, `bootstrap retry`, `schema warmed`, and
   `equal-version conflict`.

Acceptance criteria:

- A storage agent started after the orchestrator still persists all topic schemas.
- Restarting a storage agent with an existing volume does not log false stale-version warnings.
- Same-version different-content publishes are rejected explicitly.

### Phase 3: Consumer failure containment (application-level)

Primary goal: turn schema outages into degraded behavior instead of fatal errors
in applications that consume multiple Mitiflow topics.

This phase is application-specific. Mitiflow provides the building blocks (per-topic
`TopicSchemaNotFound` errors, retryable `fetch_schema_with_timeout`), but the
containment logic lives in the consuming application.

Recommended changes for multi-topic consumers:

1. Initialize publishers, subscribers, and consumer groups independently per topic.
2. Let each topic enter a retry/degraded state on schema lookup failure.
3. Do not disable unrelated topics when one topic's schema is unavailable.

Acceptance criteria:

- One failed topic schema lookup does not disable the entire application transport.
- Unaffected topics continue operating normally.
- Application roles that do not depend on the unavailable topic keep running.

### Phase 4: Test matrix and rollout

Primary goal: prove the redesign under real startup permutations.

Areas to cover:

- Orchestrator-only schema fetch
- Storage-late bootstrap
- Restart with existing schema volume
- Same-version idempotent republish
- Same-version conflicting republish
- Multi-topic consumer with partial schema availability

Suggested rollout order:

1. Deploy orchestrator exact-key fix
2. Deploy storage bootstrap fix
3. Update consuming applications to use per-topic failure containment
4. Remove any temporary operational workarounds tied to startup order

---

## 12. Summary

The schema registry should continue to look simple from the client side:

```
get({key_prefix}/_schema)
```

The redesign makes that simplicity real.

The core shift is architectural, not cosmetic:

- exact query becomes the only correctness path
- storage agents bootstrap by pull, not by timing luck
- orchestrator serves exact per-topic schemas directly
- per-topic schema failure is isolated, not cascading

That preserves the original design intent while removing the race that blocked
schema discovery under real startup conditions.