# Topic Schema Registry

Design for a distributed topic schema registry that ensures publishers and
subscribers agree on wire-level configuration (codec, partition count, key
format) before exchanging events — and enables automatic configuration
loading at runtime.

**Status:** Implemented (phases 1–5).

**Related docs:**
[16_dx_and_multi_topic.md](16_dx_and_multi_topic.md),
[13_distributed_storage.md](13_distributed_storage.md),
[02_architecture.md](02_architecture.md),
[configuration.md](configuration.md)

---

## 1. The Problem

Today, each publisher and subscriber constructs its own `EventBusConfig`
independently via the builder pattern. **Nothing validates that participants
on the same topic agree on critical wire-level properties.** Mismatches cause:

| Mismatch | Failure mode |
|----------|-------------|
| Codec (e.g., publisher sends Postcard, subscriber expects JSON) | Silent deserialization failure or garbage data |
| Partition count | Hash ring divergence — key-based routing sends events to wrong partitions |
| Key format (keyed vs. unkeyed) | Subscriber key filters match nothing, or unkeyed subscriber gets unexpected key expressions |

These failures are **silent at connection time** and only surface as runtime
errors deep in the processing pipeline, making them extremely difficult to
diagnose in distributed deployments.

### What We Want

1. **Pre-flight validation** — pub/sub checks its local config against a
   registered schema before starting, catching mismatches at creation time
   with a clear error message.
2. **Auto-configuration** — pub/sub can discover and load a topic's canonical
   configuration at runtime, eliminating manual config duplication across
   services.
3. **No new SPOF** — the schema registry should not add a hard dependency on
   a component that isn't already required.

---

## 2. Design: Storage-Served, Orchestrator-Managed

Separate **write authority** from **persistence and serving**.

| Concern | Owner |
|---------|-------|
| Schema creation & validated evolution | Orchestrator (when present) or CLI / first publisher (when absent) |
| Schema persistence & serving | Storage agents (durable, replicated, co-located with event data) |
| Schema querying | Any pub/sub client via `session.get("{key_prefix}/_schema")` |

```
                          ┌─────────────────────────┐
                          │     Orchestrator         │
                          │  (validated writer)      │
                          │                          │
                          │  POST /api/v1/topics     │
                          │   → validates schema     │
                          │   → put _schema key      │
                          └───────────┬──────────────┘
                                      │ zenoh put
                                      ▼
┌──────────────┐   get    ┌─────────────────────────┐
│  Publisher /  │ ◄──────►│   Storage Agent(s)       │
│  Subscriber  │         │  ┌────────────────────┐  │
│              │         │  │ fjall "schemas"    │  │
│  validate or │         │  │ keyspace           │  │
│  auto-config │         │  └────────────────────┘  │
└──────────────┘         │  queryable: _schema      │
                          └─────────────────────────┘
                           (N agents = N respondents)
```

### Why Storage Agents?

- **Already required** for durable topics — no new dependency
- **Already have fjall** — adding a keyspace is trivial
- **Naturally replicated** — with $R$ replicas, $R$ agents serve the schema
- **Co-located with data** — schema describes the data the agent stores
- **Survives orchestrator downtime** — new pub/sub instances can still
  discover schemas from storage agents

### Feature-Flag Independence

`TopicSchema`, `KeyFormat`, `TopicSchemaMode`, and all validation logic
live in `mitiflow/src/schema.rs` — **not behind `#[cfg(feature = "store")]`**.
Schema fetch and validation use only `session.get()` / `session.put()`
(always available in Zenoh). The `store` feature controls whether an
`EventStore` persists event data; topic schema validation is orthogonal.

This means:
- `schema_mode` is always available on `EventBusConfigBuilder`
- `EventBusConfig::from_topic()` works regardless of feature flags
- A publisher compiled without `store` can still validate against (or
  auto-configure from) a schema served by storage agents

### Any Component with Fjall Can Serve Schemas

Both storage agents and the orchestrator have fjall databases. Both can
persist and serve schemas via `{key_prefix}/_schema` queryables. This
gives a clean durability gradient:

| Infrastructure present | Schema durable? | Served by |
|------------------------|-----------------|----------|
| Orchestrator + Storage agents | Yes (both persist) | Storage agents + orchestrator |
| Orchestrator only (no storage) | Yes (orchestrator fjall) | Orchestrator |
| Storage agents only (no orchestrator) | Yes (storage fjall) | Storage agents |
| Neither (pub/sub only) | **No** — ephemeral in registrar publisher memory | Registrar publisher (if `RegisterOrValidate`) |

The key insight: **schema durability follows from whichever fjall-backed
component is present**, not from a specific component. The orchestrator
already persists `TopicConfig` in its `ConfigStore` — extending it to
also respond to `_schema` queries is a small addition.

---

## 3. TopicSchema Model

A shared struct in the core `mitiflow` crate, used by all components:

```rust
/// Wire-level contract for a topic. All publishers and subscribers
/// on the same key_prefix must agree on these fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicSchema {
    /// Topic name (matches orchestrator TopicConfig.name).
    pub name: String,
    /// Zenoh key prefix (e.g., "myapp/events").
    pub key_prefix: String,
    /// Serialization codec. All participants must use the same codec.
    pub codec: CodecFormat,
    /// Number of partitions. Must match across all participants for
    /// consistent hash-ring routing.
    pub num_partitions: u32,
    /// Key format — whether events carry application-level keys.
    pub key_format: KeyFormat,
    /// Monotonically increasing version. Only forward-version writes
    /// are accepted, preventing accidental downgrades.
    pub schema_version: u32,
    /// When this schema was first created.
    pub created_at: DateTime<Utc>,
    /// When this schema was last modified.
    pub updated_at: DateTime<Utc>,
}

/// Key format for events on a topic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyFormat {
    /// Events have no application key: `{prefix}/p/{partition}/{seq}`
    Unkeyed,
    /// Events carry an exact key: `{prefix}/p/{partition}/k/{key}/{seq}`
    Keyed,
    /// Events carry a hierarchical key prefix: `{prefix}/p/{partition}/k/{prefix}/**`
    KeyPrefix,
}
```

### Zenoh Key

Schema is stored and queried at:

```
{key_prefix}/_schema
```

This follows the existing `_` prefix convention for internal channels
(`_store`, `_watermark`, `_heartbeat`, etc.).

### Validated Fields

When a publisher or subscriber checks its local `EventBusConfig` against a
`TopicSchema`, the following fields are compared:

| Field | Mismatch severity | Behavior |
|-------|-------------------|----------|
| `codec` | **Fatal** — data corruption | Return `Error::TopicSchemaMismatch` |
| `num_partitions` | **Fatal** — routing divergence | Return `Error::TopicSchemaMismatch` |
| `key_format` | **Warning** — may work but unexpected | Log warning; fatal in strict mode |

Other `EventBusConfig` fields (cache size, heartbeat mode, recovery mode,
offload config, etc.) are **local tuning** and are intentionally *not*
validated against the schema.

---

## 4. Schema Lifecycle

### 4.1 Creation

Three paths, depending on deployment mode:

**Path A — Orchestrator creates topic (production):**
```
User → POST /api/v1/topics { name, codec, num_partitions, key_format, ... }
     → Orchestrator validates fields
     → config_store.put_topic(topic_config)
     → session.put("{key_prefix}/_schema", schema_json)
     → Storage agents receive put, persist in fjall "schemas" keyspace
```

**Path B — CLI creates schema directly (no orchestrator):**
```
User → mitiflow ctl schema register --topic orders --codec postcard --partitions 16
     → session.put("{key_prefix}/_schema", schema_json)
     → Storage agents receive put, persist
```

**Path C — First publisher registers (dev/test):**
```
Publisher starts with TopicSchemaMode::RegisterOrValidate
     → session.get("{key_prefix}/_schema") → timeout, no reply
     → Publisher constructs TopicSchema from its EventBusConfig
     → session.put("{key_prefix}/_schema", schema_json)
     → Publisher also declares queryable on _schema (ephemeral, for peers
       before storage agents pick it up)
```

### 4.2 Persistence (Storage Agents)

Storage agents subscribe to `{key_prefix}/_schema` as part of their
existing topic lifecycle. On receiving a schema put:

1. Deserialize incoming `TopicSchema`
2. Check monotonic version: if `incoming.schema_version <= persisted.schema_version`, reject (log warning, do not overwrite)
3. Persist to fjall `"schemas"` keyspace keyed by `key_prefix`
4. Update in-memory cache

On startup, storage agents load persisted schemas from fjall and declare
queryables immediately — no orchestrator round-trip needed.

### 4.3 Serving (Queryable)

Each storage agent declares a Zenoh queryable on `{key_prefix}/_schema`
for every topic it manages. When a publisher or subscriber queries:

```rust
session.get("{key_prefix}/_schema")
    .timeout(Duration::from_secs(2))
    .await
```

Any storage agent (or orchestrator, or registrar publisher) can respond.
The client uses the **first valid reply**.

### 4.4 Schema Evolution

Schema updates follow a monotonic version protocol:

1. Writer (orchestrator or CLI) increments `schema_version`
2. Writer puts updated schema to `{key_prefix}/_schema`
3. Storage agents accept only if new version > current version
4. Running publishers/subscribers are **not** affected (they validated at
   startup). New instances will validate against the updated schema.

**Breaking changes** (codec change, partition count change) require:

1. Stop all publishers and subscribers for the topic
2. Update the schema with a new version
3. Restart participants with matching configs

The orchestrator (when present) can enforce evolution rules:

| Change | Policy |
|--------|--------|
| Codec change | Requires version bump, rejected if active publishers exist |
| Partition count increase | Allowed (existing data stays, new partitions are empty) |
| Partition count decrease | Rejected (would orphan data) |
| Key format change | Requires version bump |

---

## 5. Client Integration

### 5.1 TopicSchemaMode

A new enum on `EventBusConfig` controls schema behavior:

```rust
pub enum TopicSchemaMode {
    /// No schema validation (current behavior, backward-compatible default).
    Disabled,
    /// Validate local config against registry, fail on mismatch.
    Validate,
    /// Load full config from registry (auto-configuration).
    AutoConfig,
    /// Register schema if absent, validate if present (dev/test).
    RegisterOrValidate,
}
```

**Default:** `Disabled` — fully backward-compatible. Existing code works
without changes.

### 5.2 Validation Flow

On `EventPublisher::new()` or `EventSubscriber::new()`:

```
match config.schema_mode {
    Disabled => proceed (current behavior),

    Validate => {
        schema = fetch_schema(session, key_prefix).await?;
        // Returns Error::TopicSchemaNotFound if no respondent
        validate(config, schema)?;
        // Returns Error::TopicSchemaMismatch { field, expected, actual }
    }

    AutoConfig => {
        schema = fetch_schema(session, key_prefix).await?;
        config = build_config_from_schema(schema);
        // Local overrides (cache_size, heartbeat, etc.) still apply
    }

    RegisterOrValidate => {
        match fetch_schema(session, key_prefix).timeout(1s).await {
            Ok(schema) => validate(config, schema)?,
            Err(Timeout) => {
                schema = TopicSchema::from_config(config);
                register_schema(session, key_prefix, schema).await?;
            }
        }
    }
}
```

### 5.3 Auto-Configuration API

```rust
// Full auto-config: load everything from the registry
let config = EventBusConfig::from_topic(&session, "myapp/events", "orders").await?;
let publisher = EventPublisher::new(&session, config).await?;

// Validate mode: build locally, check against registry
let config = EventBusConfig::builder("myapp/events/orders")
    .codec(CodecFormat::Postcard)
    .num_partitions(16)
    .schema_mode(TopicSchemaMode::Validate)
    .build()?;
let subscriber = EventSubscriber::new(&session, config).await?;

// Dev mode: register or validate automatically
let config = EventBusConfig::builder("myapp/events/orders")
    .codec(CodecFormat::Postcard)
    .num_partitions(16)
    .schema_mode(TopicSchemaMode::RegisterOrValidate)
    .build()?;
let publisher = EventPublisher::new(&session, config).await?;
```

### 5.4 Error Types

```rust
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
    // ... existing variants ...

    #[error("topic schema not found for '{key_prefix}' (no storage agents or orchestrator responded)")]
    TopicSchemaNotFound { key_prefix: String },

    #[error("topic schema mismatch on field '{field}': local={local}, registered={registered}")]
    TopicSchemaMismatch {
        field: String,
        local: String,
        registered: String,
    },

    #[error("topic schema version conflict: local={local}, registered={registered}")]
    TopicSchemaVersionConflict { local: u32, registered: u32 },
}
```

---

## 6. Deployment Modes

The schema registry adapts to all existing deployment modes:

### With Orchestrator + Storage Agents (Production)

```
                Orchestrator
               (creates topics,
                validates evolution)
                     │
                     ▼ put _schema
              Storage Agents ×N
             (persist & serve)
                     ▲
                     │ get _schema
              Publishers / Subscribers
             (Validate or AutoConfig mode)
```

- Orchestrator is the **validated writer**
- Storage agents are the **durable, redundant servers**
- Pub/sub clients query storage agents (orchestrator is optional respondent)

### Storage Agents Only (No Orchestrator)

```
              CLI: mitiflow ctl schema register
                     │
                     ▼ put _schema
              Storage Agents ×N
             (persist & serve)
                     ▲
                     │ get _schema
              Publishers / Subscribers
             (Validate or AutoConfig mode)
```

- Schema created via CLI or first publisher
- Storage agents persist and serve
- No governance — monotonic version is the only guard

### Orchestrator Only (No Storage Agents)

```
              Orchestrator
             (creates topics,
              persists in ConfigStore,
              serves _schema queryable)
                     ▲
                     │ get _schema
              Publishers / Subscribers
             (Validate or AutoConfig mode)
```

- Orchestrator persists schema in its own fjall `ConfigStore`
- Orchestrator declares `_schema` queryable for each topic
- No storage agents needed — useful for lightweight deployments where
  event durability is not required but schema governance is

### Pub/Sub Only (No Storage, No Orchestrator)

```
              First Publisher
             (RegisterOrValidate mode)
                     │
                     ▼ put _schema + ephemeral queryable
              Other Publishers / Subscribers
             (RegisterOrValidate mode)
                     │
                     ▼ get _schema → first publisher responds
```

- First publisher registers schema and serves it via ephemeral queryable
- Other participants validate against it
- Schema is **not durable** — lost when the registrar publisher exits
- Suitable for dev/test and ephemeral workloads
- **Limitation:** if the registrar publisher restarts, it re-registers
  from its own config. Peers that start between the exit and restart
  will get `TopicSchemaNotFound` unless another peer also registered

### Response Matrix

| `schema_mode` | Orch + Storage | Orch only | Storage only | Neither |
|---------------|---------------|-----------|--------------|----------|
| `Disabled` | No validation | No validation | No validation | No validation |
| `Validate` | Query either | Query orch | Query storage | Fail: `TopicSchemaNotFound` |
| `AutoConfig` | Query either | Query orch | Query storage | Fail: `TopicSchemaNotFound` |
| `RegisterOrValidate` | Validate against either | Validate against orch | Validate against storage | First pub registers (ephemeral) |

---

## 7. Storage Agent Changes

### 7.1 New Fjall Keyspace

Each `TopicWorker` in the storage agent gains access to a shared
`SchemaStore` backed by a `"schemas"` fjall keyspace in the agent's
data directory:

```rust
pub struct SchemaStore {
    keyspace: fjall::Keyspace,
}

impl SchemaStore {
    pub fn get(&self, key_prefix: &str) -> Result<Option<TopicSchema>>;
    pub fn put(&self, schema: &TopicSchema) -> Result<()>;
    /// Put only if incoming version > current. Returns false if rejected.
    pub fn put_if_newer(&self, schema: &TopicSchema) -> Result<bool>;
}
```

The keyspace is shared across all topics on the node (keyed by
`key_prefix`), avoiding one database per topic.

### 7.2 Schema Subscriber

The `TopicWorker` subscribes to `{key_prefix}/_schema` alongside its
existing event subscriptions. On receiving a schema put:

```rust
if schema_store.put_if_newer(&incoming_schema)? {
    info!(topic = %schema.name, version = schema.schema_version, "schema updated");
} else {
    warn!(topic = %schema.name, "rejected stale schema version");
}
```

### 7.3 Schema Queryable

Each `TopicWorker` declares a queryable on `{key_prefix}/_schema`:

```rust
let queryable = session.declare_queryable("{key_prefix}/_schema").await?;
// On query: reply with persisted TopicSchema (or empty if none)
```

On agent startup, schemas are loaded from fjall and queryables are
declared immediately — before any orchestrator contact.

---

## 8. Orchestrator Changes

### 8.1 Extended TopicConfig

Add wire-level fields to the existing `TopicConfig`:

```rust
pub struct TopicConfig {
    // ... existing fields (name, key_prefix, num_partitions, replication_factor, retention, compaction, labels) ...

    /// Wire codec for this topic. All publishers and subscribers must agree.
    pub codec: CodecFormat,
    /// Key format for events.
    pub key_format: KeyFormat,
    /// Schema version (monotonically increasing).
    pub schema_version: u32,
}
```

### 8.2 Schema Queryable

The orchestrator declares a queryable on `{topic_key_prefix}/_schema`
for every topic it manages. This mirrors the storage agent's queryable
and ensures schemas are served even when no storage agents are running:

```rust
// For each topic with a non-empty key_prefix:
let schema_queryable = session
    .declare_queryable(format!("{}/_schema", topic.key_prefix))
    .await?;
// On query: reply with TopicSchema derived from persisted TopicConfig
```

On startup, the orchestrator iterates all persisted topics and declares
schema queryables alongside the existing `_config` queryables.

### 8.3 Schema Publication on Topic Create/Update

When the orchestrator creates or updates a topic, it also publishes the
`TopicSchema` to `{key_prefix}/_schema`:

```rust
pub async fn create_topic(&mut self, config: TopicConfig) -> Result<()> {
    // Existing: persist config, publish to _config/{name}
    self.config_store.put_topic(&config)?;
    let config_key = format!("{}/_config/{}", self.config.key_prefix, config.name);
    self.session.put(&config_key, serde_json::to_vec(&config)?).await?;

    // NEW: publish schema (storage agents subscribe and persist)
    let schema = TopicSchema::from_topic_config(&config);
    let schema_key = format!("{}/_schema", config.key_prefix);
    self.session.put(&schema_key, serde_json::to_vec(&schema)?).await?;

    // NEW: declare schema queryable for this topic
    self.declare_schema_queryable(&config).await?;

    Ok(())
}
```

### 8.4 Schema Evolution Validation

On topic update, the orchestrator validates the transition:

```rust
pub async fn update_topic(&mut self, name: &str, update: TopicUpdate) -> Result<()> {
    let current = self.config_store.get_topic(name)?.ok_or(TopicNotFound)?;

    // Validate evolution rules
    if let Some(new_partitions) = update.num_partitions {
        if new_partitions < current.num_partitions {
            return Err(Error::InvalidSchemaEvolution(
                "partition count cannot decrease".into()
            ));
        }
    }

    // Bump schema version
    let new_version = current.schema_version + 1;
    // ... apply update, persist, publish schema ...
}
```

---

## 9. CLI Integration

```bash
# Register a schema (no orchestrator needed — direct Zenoh put)
mitiflow ctl schema register \
    --key-prefix myapp/events \
    --topic orders \
    --codec postcard \
    --partitions 16 \
    --key-format keyed

# Inspect a schema (queries storage agents / orchestrator)
mitiflow ctl schema inspect --key-prefix myapp/events
# → TopicSchema { name: "orders", codec: Postcard, num_partitions: 16,
#                  key_format: Keyed, schema_version: 1, ... }

# Validate local config against registry
mitiflow ctl schema validate --key-prefix myapp/events \
    --codec postcard --partitions 16

# Via orchestrator HTTP API (with governance)
mitiflow ctl topic create --name orders \
    --key-prefix myapp/events \
    --codec postcard \
    --partitions 16 \
    --key-format keyed \
    --replication-factor 2
```

---

## 10. Implementation Plan

### Phase 1: Schema Model (core crate) ✅

- `TopicSchema`, `KeyFormat`, `TopicSchemaMode` in `mitiflow/src/schema.rs`
- `TopicSchemaMismatch`, `TopicSchemaNotFound` error variants in `error.rs`
- `schema_mode` field on `EventBusConfigBuilder`
- `EventBusConfig::from_topic()` convenience constructor
- 10 unit tests + 7 integration tests for validation logic

### Phase 2: Storage Agent Schema Store ✅

- `SchemaStore` (fjall keyspace) in `mitiflow-storage/src/schema_store.rs`
- `_schema` subscriber + queryable wired into `TopicWorker`
- `TopicSupervisor` opens shared `SchemaStore` at `{data_dir}/_schemas`
- 6 unit tests for `SchemaStore` (open, put, get, put_if_newer, version rejection)

### Phase 3: Orchestrator Extension ✅

- `TopicConfig` extended with `codec`, `key_format`, `schema_version` (all `#[serde(default)]`)
- `create_topic()` publishes `TopicSchema` to `{key_prefix}/_schema`
- `run_schema_queryable()` serves schemas from persisted `TopicConfig`
- `publish_all_schemas()` on startup alongside `publish_all_configs()`
- HTTP API `CreateTopicRequest` and `UpdateTopicRequest` include schema fields

### Phase 4: Publisher/Subscriber Validation ✅

- `fetch_schema()` and `register_schema()` in core crate (Zenoh `get`/`put` with timeout)
- `resolve_schema()` dispatches on `TopicSchemaMode` in `EventPublisher::new()` and `EventSubscriber::init()`
- `RegisterOrValidate` path: publisher declares ephemeral `_schema` queryable
- Integration tests: mismatch detection, auto-config, register-or-validate

### Phase 5: CLI ✅

- `mitiflow ctl schema inspect|register|validate` subcommands
- Inspect: fetches and pretty-prints schema JSON
- Register: parses codec/key_format strings, creates TopicSchema, registers via Zenoh
- Validate: fetches schema, compares codec and partitions, reports mismatches
- UI integration deferred (see ROADMAP)
