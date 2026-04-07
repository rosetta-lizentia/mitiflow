//! Unified `mitiflow` CLI binary.
//!
//! Subcommands:
//!   storage       — Run the storage service (multi-topic capable)
//!   orchestrator  — Run the orchestrator control plane
//!   ctl           — Admin CLI (topics, cluster, drain)
//!   dev           — All-in-one dev mode (orchestrator + agent + embedded Zenoh)

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "mitiflow",
    about = "Brokerless event streaming platform",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the storage service (multi-topic capable).
    Storage {
        /// Path to YAML configuration file.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },

    /// Run the orchestrator control plane.
    Orchestrator {
        /// Path to YAML configuration file.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },

    /// Admin CLI for cluster management.
    Ctl {
        #[command(subcommand)]
        command: CtlCommands,
    },

    /// All-in-one development mode.
    ///
    /// Co-locates orchestrator + single-node agent in one process.
    Dev {
        /// Topics to create as `name:partitions:rf` (comma-separated).
        /// Example: `events:4:1,logs:2:1`
        #[arg(long)]
        topics: Option<String>,

        /// Path to YAML configuration file (alternative to --topics).
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Global Zenoh key prefix.
        #[arg(long, default_value = "mitiflow")]
        prefix: String,

        /// Data directory.
        #[arg(long, default_value = "/tmp/mitiflow-dev")]
        data_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum CtlCommands {
    /// Topic management.
    Topics {
        #[command(subcommand)]
        command: TopicCtlCommands,
    },
    /// Cluster management.
    Cluster {
        #[command(subcommand)]
        command: ClusterCtlCommands,
    },
    /// Run connectivity and health diagnostics.
    Diagnose {
        /// Global Zenoh key prefix.
        #[arg(long, default_value = "mitiflow")]
        prefix: String,

        /// Timeout for queries in seconds.
        #[arg(long, default_value = "5")]
        timeout: u64,
    },
    /// Schema registry operations.
    Schema {
        #[command(subcommand)]
        command: SchemaCtlCommands,
    },
}

#[derive(Subcommand)]
enum TopicCtlCommands {
    /// List all topics.
    List,
    /// Get topic details.
    Get {
        /// Topic name.
        name: String,
    },
}

#[derive(Subcommand)]
enum ClusterCtlCommands {
    /// List cluster nodes.
    Nodes,
    /// Drain a node (move all partitions away).
    Drain {
        /// Node ID to drain.
        node_id: String,
    },
}

#[derive(Subcommand)]
enum SchemaCtlCommands {
    /// Inspect the schema registered for a key prefix.
    Inspect {
        /// Key prefix to query (e.g. "myapp/events").
        key_prefix: String,
    },
    /// Register a schema for a key prefix.
    Register {
        /// Key prefix.
        key_prefix: String,
        /// Topic name.
        #[arg(long)]
        name: String,
        /// Number of partitions.
        #[arg(long)]
        partitions: u32,
        /// Wire codec (json, msgpack, postcard).
        #[arg(long, default_value = "postcard")]
        codec: String,
        /// Key format (unkeyed, keyed).
        #[arg(long, default_value = "unkeyed")]
        key_format: String,
    },
    /// Validate local settings against the registered schema.
    Validate {
        /// Key prefix to query.
        key_prefix: String,
        /// Expected codec.
        #[arg(long)]
        codec: String,
        /// Expected partition count.
        #[arg(long)]
        partitions: u32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Storage { config } => run_storage(config).await,
        Commands::Orchestrator { config } => run_orchestrator(config).await,
        Commands::Ctl { command } => run_ctl(command).await,
        Commands::Dev {
            topics,
            config,
            prefix,
            data_dir,
        } => run_dev(topics, config, prefix, data_dir).await,
    }
}

async fn run_storage(config_path: Option<PathBuf>) -> anyhow::Result<()> {
    let session = zenoh::open(zenoh::Config::default())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut agent = if let Some(path) = config_path {
        let yaml = mitiflow_storage::AgentYamlConfig::from_file(&path)?;
        let agent_config = yaml.into_agent_config()?;
        mitiflow_storage::StorageAgent::start_multi(&session, agent_config).await?
    } else {
        // Env-var fallback
        let key_prefix = std::env::var("MITIFLOW_KEY_PREFIX").unwrap_or_else(|_| "mitiflow".into());
        let data_dir =
            std::env::var("MITIFLOW_DATA_DIR").unwrap_or_else(|_| "/tmp/mitiflow-storage".into());
        let node_id = std::env::var("MITIFLOW_NODE_ID").ok();
        let num_partitions: u32 = std::env::var("MITIFLOW_NUM_PARTITIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);
        let replication_factor: u32 = std::env::var("MITIFLOW_REPLICATION_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let capacity: u32 = std::env::var("MITIFLOW_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let bus_config = mitiflow::EventBusConfig::builder(&key_prefix).build()?;
        let mut builder =
            mitiflow_storage::StorageAgentConfig::builder(PathBuf::from(&data_dir), bus_config)
                .capacity(capacity)
                .num_partitions(num_partitions)
                .replication_factor(replication_factor);
        if let Some(id) = node_id {
            builder = builder.node_id(id);
        }
        mitiflow_storage::StorageAgent::start(&session, builder.build()?).await?
    };

    tracing::info!("storage agent running, press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;

    agent.shutdown().await?;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

async fn run_orchestrator(config_path: Option<PathBuf>) -> anyhow::Result<()> {
    let session = zenoh::open(zenoh::Config::default())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let orch_config = if let Some(path) = config_path {
        let content = std::fs::read_to_string(&path)?;
        serde_yaml::from_str::<OrchestratorYamlConfig>(&content)?.into_orch_config()
    } else {
        let key_prefix = std::env::var("MITIFLOW_KEY_PREFIX").unwrap_or_else(|_| "mitiflow".into());
        let data_dir = std::env::var("MITIFLOW_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./orchestrator_data"));
        let lag_interval_ms: u64 = std::env::var("MITIFLOW_LAG_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        mitiflow_orchestrator::orchestrator::OrchestratorConfig {
            key_prefix,
            data_dir,
            lag_interval: Duration::from_millis(lag_interval_ms),
            admin_prefix: None,
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: std::env::var("MITIFLOW_BOOTSTRAP_TOPICS_FROM")
                .ok()
                .map(PathBuf::from),
        }
    };

    let mut orchestrator = mitiflow_orchestrator::Orchestrator::new(&session, orch_config)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    orchestrator
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    tracing::info!("orchestrator running, press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;

    orchestrator.shutdown().await;
    Ok(())
}

async fn run_ctl(command: CtlCommands) -> anyhow::Result<()> {
    let key_prefix = std::env::var("MITIFLOW_KEY_PREFIX").unwrap_or_else(|_| "mitiflow".into());
    let session = zenoh::open(zenoh::Config::default())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    match command {
        CtlCommands::Topics { command } => match command {
            TopicCtlCommands::List => {
                let key = format!("{key_prefix}/_config/**");
                let replies = session
                    .get(&key)
                    .consolidation(zenoh::query::ConsolidationMode::None)
                    .accept_replies(zenoh::query::ReplyKeyExpr::Any)
                    .timeout(Duration::from_secs(5))
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;

                let mut topics = Vec::new();
                while let Ok(reply) = replies.recv_async().await {
                    if let Ok(sample) = reply.result() {
                        let payload = sample.payload().to_bytes();
                        if let Ok(cfg) = serde_json::from_slice::<serde_json::Value>(&payload) {
                            topics.push(cfg);
                        }
                    }
                }

                if topics.is_empty() {
                    println!("No topics found.");
                } else {
                    for t in &topics {
                        println!(
                            "  {} (partitions={}, rf={})",
                            t["name"].as_str().unwrap_or("?"),
                            t["num_partitions"].as_u64().unwrap_or(0),
                            t["replication_factor"].as_u64().unwrap_or(0),
                        );
                    }
                }
            }
            TopicCtlCommands::Get { name } => {
                let key = format!("{key_prefix}/_config/{name}");
                let replies = session
                    .get(&key)
                    .consolidation(zenoh::query::ConsolidationMode::None)
                    .accept_replies(zenoh::query::ReplyKeyExpr::Any)
                    .timeout(Duration::from_secs(5))
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;

                let mut found = false;
                while let Ok(reply) = replies.recv_async().await {
                    if let Ok(sample) = reply.result() {
                        let payload = sample.payload().to_bytes();
                        if let Ok(cfg) = serde_json::from_slice::<serde_json::Value>(&payload) {
                            println!("{}", serde_json::to_string_pretty(&cfg)?);
                            found = true;
                        }
                    }
                }
                if !found {
                    println!("Topic '{name}' not found.");
                }
            }
        },
        CtlCommands::Cluster { command } => match command {
            ClusterCtlCommands::Nodes => {
                let admin = std::env::var("MITIFLOW_ADMIN_PREFIX")
                    .unwrap_or_else(|_| format!("{key_prefix}/_admin"));
                let key = format!("{admin}/cluster/nodes");
                let replies = session
                    .get(&key)
                    .timeout(Duration::from_secs(5))
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;

                while let Ok(reply) = replies.recv_async().await {
                    if let Ok(sample) = reply.result() {
                        let payload = sample.payload().to_bytes();
                        if let Ok(nodes) = serde_json::from_slice::<serde_json::Value>(&payload) {
                            println!("{}", serde_json::to_string_pretty(&nodes)?);
                        }
                    }
                }
            }
            ClusterCtlCommands::Drain { node_id } => {
                println!("Draining node {node_id}...");
                // In a real implementation this would hit the admin API.
                // For now, just print instruction:
                println!("Use the orchestrator HTTP API to drain: POST /admin/drain/{node_id}");
            }
        },
        CtlCommands::Diagnose { prefix, timeout } => {
            run_diagnose(&session, &prefix, timeout).await?;
        }
        CtlCommands::Schema { command } => {
            run_schema_ctl(&session, &key_prefix, command).await?;
        }
    }

    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

async fn run_diagnose(
    session: &zenoh::Session,
    prefix: &str,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(timeout_secs);
    let mut checks_passed = 0u32;
    let checks_failed = 0u32;
    let mut warnings = 0u32;

    println!("Mitiflow Diagnostics");
    println!("====================\n");

    // 1. Zenoh connectivity
    print!("[check] Zenoh session ... ");
    let info = session.info();
    let zid = info.zid().await;
    println!("OK (zid: {zid})");
    checks_passed += 1;

    // 2. Discover topics via orchestrator config queryable
    print!("[check] Topic discovery ({prefix}/_config/**) ... ");
    let key = format!("{prefix}/_config/**");
    let replies = session
        .get(&key)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .timeout(timeout)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut topics: Vec<serde_json::Value> = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let payload = sample.payload().to_bytes();
            if let Ok(cfg) = serde_json::from_slice::<serde_json::Value>(&payload) {
                topics.push(cfg);
            }
        }
    }

    if topics.is_empty() {
        println!("WARN (no topics found — orchestrator may not be running)");
        warnings += 1;
    } else {
        println!("OK ({} topic(s))", topics.len());
        checks_passed += 1;
        for t in &topics {
            let name = t["name"].as_str().unwrap_or("?");
            let partitions = t["num_partitions"].as_u64().unwrap_or(0);
            let rf = t["replication_factor"].as_u64().unwrap_or(0);
            println!("       - {name} (partitions={partitions}, rf={rf})");
        }
    }

    // 3. Check for live agents
    print!("[check] Agent liveliness ... ");
    let mut agent_count = 0u32;
    // Try each discovered topic's prefix for liveliness
    let mut checked_prefixes = std::collections::HashSet::new();
    for t in &topics {
        if let Some(kp) = t["key_prefix"].as_str()
            && checked_prefixes.insert(kp.to_string())
        {
            let token_key = format!("{kp}/_agents/*");
            if let Ok(sub) = session.liveliness().get(&token_key).timeout(timeout).await {
                while let Ok(reply) = sub.recv_async().await {
                    if reply.result().is_ok() {
                        agent_count += 1;
                    }
                }
            }
        }
    }

    if agent_count == 0 {
        if topics.is_empty() {
            println!("SKIP (no topics to check)");
        } else {
            println!("WARN (no agents detected for any topic)");
            warnings += 1;
        }
    } else {
        println!("OK ({agent_count} agent token(s) across topics)");
        checks_passed += 1;
    }

    // 4. Check orchestrator admin queryable (if configured)
    let admin_prefix =
        std::env::var("MITIFLOW_ADMIN_PREFIX").unwrap_or_else(|_| format!("{prefix}/_admin"));
    print!("[check] Orchestrator admin ({admin_prefix}/cluster/nodes) ... ");
    let admin_key = format!("{admin_prefix}/cluster/nodes");
    let replies = session
        .get(&admin_key)
        .timeout(timeout)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut got_admin = false;
    while let Ok(reply) = replies.recv_async().await {
        if reply.result().is_ok() {
            got_admin = true;
        }
    }

    if got_admin {
        println!("OK");
        checks_passed += 1;
    } else {
        println!("WARN (no response — admin queryable may not be enabled)");
        warnings += 1;
    }

    // Summary
    println!("\n--------------------");
    println!(
        "Results: {} passed, {} warnings, {} failed",
        checks_passed, warnings, checks_failed
    );

    if checks_failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_schema_ctl(
    session: &zenoh::Session,
    _global_prefix: &str,
    command: SchemaCtlCommands,
) -> anyhow::Result<()> {
    match command {
        SchemaCtlCommands::Inspect { key_prefix } => {
            match mitiflow::schema::fetch_schema(session, &key_prefix).await {
                Ok(schema) => {
                    println!("{}", serde_json::to_string_pretty(&schema)?);
                }
                Err(e) => {
                    eprintln!("No schema found for '{key_prefix}': {e}");
                    std::process::exit(1);
                }
            }
        }
        SchemaCtlCommands::Register {
            key_prefix,
            name,
            partitions,
            codec,
            key_format,
        } => {
            let codec: mitiflow::codec::CodecFormat = match codec.to_lowercase().as_str() {
                "json" => mitiflow::codec::CodecFormat::Json,
                "msgpack" | "messagepack" => mitiflow::codec::CodecFormat::MsgPack,
                "postcard" => mitiflow::codec::CodecFormat::Postcard,
                other => anyhow::bail!("unknown codec: {other}"),
            };
            let key_format: mitiflow::schema::KeyFormat = match key_format.to_lowercase().as_str() {
                "unkeyed" => mitiflow::schema::KeyFormat::Unkeyed,
                "keyed" => mitiflow::schema::KeyFormat::Keyed,
                other => anyhow::bail!("unknown key_format: {other}"),
            };
            let schema = mitiflow::schema::TopicSchema::new(
                &key_prefix,
                name,
                partitions,
                codec,
                key_format,
                1,
            );
            mitiflow::schema::register_schema(session, &schema).await?;
            println!("Schema registered for '{key_prefix}'");
        }
        SchemaCtlCommands::Validate {
            key_prefix,
            codec,
            partitions,
        } => {
            let schema = mitiflow::schema::fetch_schema(session, &key_prefix).await?;
            let mut mismatches = Vec::new();
            let expected_codec = match codec.to_lowercase().as_str() {
                "json" => mitiflow::codec::CodecFormat::Json,
                "msgpack" | "messagepack" => mitiflow::codec::CodecFormat::MsgPack,
                "postcard" => mitiflow::codec::CodecFormat::Postcard,
                other => anyhow::bail!("unknown codec: {other}"),
            };
            if schema.codec != expected_codec {
                mismatches.push(format!(
                    "codec: registry={:?}, local={:?}",
                    schema.codec, expected_codec
                ));
            }
            if schema.num_partitions != partitions {
                mismatches.push(format!(
                    "partitions: registry={}, local={}",
                    schema.num_partitions, partitions
                ));
            }
            if mismatches.is_empty() {
                println!("OK — local settings match registered schema");
            } else {
                eprintln!("Schema validation failed:");
                for m in &mismatches {
                    eprintln!("  - {m}");
                }
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

async fn run_dev(
    topics_arg: Option<String>,
    _config_path: Option<PathBuf>,
    prefix: String,
    data_dir: PathBuf,
) -> anyhow::Result<()> {
    // Parse topics from CLI arg
    let topic_specs = if let Some(ref topics_str) = topics_arg {
        parse_topic_specs(topics_str)?
    } else {
        vec![("default".into(), 4u32, 1u32)]
    };

    let session = zenoh::open(zenoh::Config::default())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Start orchestrator
    let orch_dir = data_dir.join("orchestrator");
    std::fs::create_dir_all(&orch_dir)?;
    let orch_config = mitiflow_orchestrator::orchestrator::OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: orch_dir,
        lag_interval: Duration::from_secs(1),
        admin_prefix: Some(format!("{prefix}/_admin")),
        http_bind: Some("0.0.0.0:8080".parse().unwrap()),
        auth_token: None,
        bootstrap_topics_from: None,
    };

    let mut orchestrator = mitiflow_orchestrator::Orchestrator::new(&session, orch_config)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    orchestrator
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Create topics
    for (name, partitions, rf) in &topic_specs {
        let tc = mitiflow_orchestrator::config::TopicConfig {
            name: name.clone(),
            key_prefix: format!("{prefix}/{name}"),
            num_partitions: *partitions,
            replication_factor: *rf,
            retention: mitiflow_orchestrator::config::RetentionPolicy::default(),
            compaction: mitiflow_orchestrator::config::CompactionPolicy::default(),
            required_labels: Default::default(),
            excluded_labels: Default::default(),
            codec: Default::default(),
            key_format: Default::default(),
            schema_version: 0,
        };
        orchestrator
            .create_topic(tc)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    // Start agent in auto-discover mode
    let agent_dir = data_dir.join("agent");
    std::fs::create_dir_all(&agent_dir)?;
    let agent_config = mitiflow_storage::AgentConfig::builder(agent_dir)
        .global_prefix(&prefix)
        .auto_discover_topics(true)
        .build()?;

    let mut agent = mitiflow_storage::StorageAgent::start_multi(&session, agent_config).await?;

    tracing::info!(
        topics = ?topic_specs.iter().map(|(n,p,r)| format!("{n}:{p}:{r}")).collect::<Vec<_>>(),
        prefix = %prefix,
        "dev mode running (orchestrator + agent), press Ctrl+C to stop"
    );

    tokio::signal::ctrl_c().await?;

    agent.shutdown().await?;
    orchestrator.shutdown().await;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Parse `name:partitions:rf` specs (comma-separated).
fn parse_topic_specs(s: &str) -> anyhow::Result<Vec<(String, u32, u32)>> {
    let mut specs = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let fields: Vec<&str> = part.split(':').collect();
        match fields.len() {
            1 => specs.push((fields[0].to_string(), 4, 1)),
            2 => {
                let p: u32 = fields[1].parse()?;
                specs.push((fields[0].to_string(), p, 1));
            }
            3 => {
                let p: u32 = fields[1].parse()?;
                let r: u32 = fields[2].parse()?;
                specs.push((fields[0].to_string(), p, r));
            }
            _ => anyhow::bail!("invalid topic spec: '{part}' (expected name:partitions:rf)"),
        }
    }
    Ok(specs)
}

/// YAML config for orchestrator (used by `mitiflow orchestrator --config`).
#[derive(serde::Deserialize)]
struct OrchestratorYamlConfig {
    #[serde(default = "default_key_prefix")]
    key_prefix: String,
    #[serde(default = "default_orch_data_dir")]
    data_dir: PathBuf,
    #[serde(default = "default_lag_interval")]
    lag_interval_ms: u64,
    admin_prefix: Option<String>,
    /// HTTP API bind address (e.g. "0.0.0.0:8080"). Also settable via `MITIFLOW_HTTP_BIND`.
    http_bind: Option<String>,
    /// Path to a YAML file with `topics` to bootstrap on startup.
    /// Also settable via `MITIFLOW_BOOTSTRAP_TOPICS_FROM`.
    bootstrap_topics_from: Option<PathBuf>,
}

fn default_key_prefix() -> String {
    "mitiflow".into()
}
fn default_orch_data_dir() -> PathBuf {
    PathBuf::from("./orchestrator_data")
}
fn default_lag_interval() -> u64 {
    1000
}

impl OrchestratorYamlConfig {
    fn into_orch_config(self) -> mitiflow_orchestrator::orchestrator::OrchestratorConfig {
        // Env var takes precedence over YAML config
        let http_bind = std::env::var("MITIFLOW_HTTP_BIND")
            .ok()
            .or(self.http_bind)
            .and_then(|s| s.parse().ok());

        let bootstrap_topics_from = std::env::var("MITIFLOW_BOOTSTRAP_TOPICS_FROM")
            .ok()
            .map(PathBuf::from)
            .or(self.bootstrap_topics_from);

        mitiflow_orchestrator::orchestrator::OrchestratorConfig {
            key_prefix: self.key_prefix,
            data_dir: self.data_dir,
            lag_interval: Duration::from_millis(self.lag_interval_ms),
            admin_prefix: self.admin_prefix,
            http_bind,
            auth_token: None,
            bootstrap_topics_from,
        }
    }
}
