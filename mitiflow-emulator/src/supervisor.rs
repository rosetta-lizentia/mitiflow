//! Supervisor — orchestrates startup, shutdown, and lifecycle of all components.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::backend::{ComponentHandle, ComponentSpec, ExecutionBackend};
use crate::config::{
    ComponentDef, ComponentKind, IsolationMode, TopologyConfig,
};
use crate::log_aggregator::LogAggregator;
use crate::role_config::{
    ConsumerGroupRoleConfig, ConsumerRoleConfig, OrchestratorRoleConfig, OutputRoleConfig,
    PayloadRoleConfig, ProcessorRoleConfig, ProducerRoleConfig, StorageAgentRoleConfig,
    TopicRegistration, ZenohRoleConfig, encode_config,
};
use crate::validation::{resolve_component_config, validate};

/// Manages the complete lifecycle of an emulator topology.
pub struct Supervisor {
    config: TopologyConfig,
    backend: Box<dyn ExecutionBackend>,
    container_backend: Option<Box<dyn ExecutionBackend>>,
    handles: Vec<(ComponentDef, Vec<Box<dyn ComponentHandle>>)>,
    log_aggregator: Option<LogAggregator>,
    cancel: CancellationToken,
    duration: Option<Duration>,
    dry_run: bool,
    chaos_enabled: Option<bool>,
}

impl Supervisor {
    pub fn new(
        config: TopologyConfig,
        backend: Box<dyn ExecutionBackend>,
        container_backend: Option<Box<dyn ExecutionBackend>>,
    ) -> Self {
        Self {
            config,
            backend,
            container_backend,
            handles: Vec::new(),
            log_aggregator: None,
            cancel: CancellationToken::new(),
            duration: None,
            dry_run: false,
            chaos_enabled: None,
        }
    }

    pub fn with_duration(mut self, d: Duration) -> Self {
        self.duration = Some(d);
        self
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    pub fn with_chaos_override(mut self, enabled: Option<bool>) -> Self {
        self.chaos_enabled = enabled;
        self
    }

    /// Run the topology.
    pub async fn run(&mut self) -> crate::error::Result<()> {
        // Validate first.
        let result = validate(&self.config)?;
        for w in &result.warnings {
            warn!("{}", w);
        }

        if self.dry_run {
            self.print_plan();
            return Ok(());
        }

        // Start log aggregator.
        let log_agg = LogAggregator::new(&self.config.logging);

        // Build topic registry for config resolution.
        let topic_map: HashMap<&str, &crate::config::TopicDef> = self
            .config
            .topics
            .iter()
            .map(|t| (t.name.as_str(), t))
            .collect();

        // Group components by tier and sort.
        let mut tiers: Vec<Vec<&ComponentDef>> = vec![Vec::new(); 6]; // tiers 1-5
        for comp in &self.config.components {
            let tier = comp.kind.tier() as usize;
            tiers[tier].push(comp);
        }

        // Spawn components tier by tier.
        for tier in &tiers {
            for comp in tier {
                let topic = match comp.kind {
                    ComponentKind::Orchestrator => None,
                    ComponentKind::Processor => comp
                        .input_topic
                        .as_deref()
                        .and_then(|t| topic_map.get(t).copied()),
                    _ => comp
                        .topic
                        .as_deref()
                        .and_then(|t| topic_map.get(t).copied()),
                };

                let resolved =
                    resolve_component_config(comp, topic, &self.config.defaults);

                for instance in 0..comp.instances {
                    let spec = self.build_spec(comp, instance as usize, &resolved, &topic_map)?;

                    info!(
                        "Spawning {}:{} ({})",
                        comp.name,
                        instance,
                        comp.kind.binary_name()
                    );

                    let isolation = comp
                        .isolation
                        .unwrap_or(self.config.defaults.isolation);

                    let backend: &dyn ExecutionBackend = match isolation {
                        IsolationMode::Process => self.backend.as_ref(),
                        IsolationMode::Container => {
                            self.container_backend.as_deref().unwrap_or(self.backend.as_ref())
                        }
                    };

                    let mut handle = backend.spawn(&spec).await?;

                    // Wire up log streams.
                    if let Some(stdout) = handle.take_stdout() {
                        log_agg.add_stdout(&comp.name, instance as usize, stdout);
                    }
                    if let Some(stderr) = handle.take_stderr() {
                        log_agg.add_stderr(&comp.name, instance as usize, stderr);
                    }

                    // Store handle grouped by component.
                    if let Some(entry) = self
                        .handles
                        .iter_mut()
                        .find(|(c, _)| c.name == comp.name)
                    {
                        entry.1.push(handle);
                    } else {
                        self.handles.push(((*comp).clone(), vec![handle]));
                    }
                }

                // Readiness gate for storage agents.
                if comp.kind == ComponentKind::StorageAgent {
                    // Small delay to let storage agents start.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        self.log_aggregator = Some(log_agg);

        // Start chaos scheduler if enabled.
        let chaos_enabled = self
            .chaos_enabled
            .unwrap_or(self.config.chaos.enabled);

        let _chaos_cancel = self.cancel.clone();
        let _chaos_enabled = if chaos_enabled && !self.config.chaos.schedule.is_empty() {
            info!("Chaos engineering enabled with {} events", self.config.chaos.schedule.len());
            true
        } else {
            false
        };

        // Wait for shutdown signal.
        let cancel = self.cancel.clone();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, shutting down...");
            }
            _ = async {
                if let Some(d) = self.duration {
                    tokio::time::sleep(d).await;
                    info!("Duration elapsed ({:?}), shutting down...", d);
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
            _ = cancel.cancelled() => {
                info!("Shutdown requested");
            }
        }

        // Graceful shutdown.
        self.shutdown().await;

        Ok(())
    }

    /// Build a ComponentSpec for a given component/instance.
    fn build_spec(
        &self,
        comp: &ComponentDef,
        instance: usize,
        resolved: &crate::validation::ResolvedComponentConfig,
        topic_map: &HashMap<&str, &crate::config::TopicDef>,
    ) -> crate::error::Result<ComponentSpec> {
        let mut env = HashMap::new();

        // Build zenoh config.
        let zenoh_config = ZenohRoleConfig {
            mode: match self.config.zenoh.mode {
                crate::config::ZenohMode::Peer => "peer".into(),
                crate::config::ZenohMode::Client => "client".into(),
                crate::config::ZenohMode::Router => "router".into(),
            },
            listen: self.config.zenoh.listen.clone(),
            connect: self.config.zenoh.connect.clone(),
        };
        env.insert(
            "MITIFLOW_ZENOH_CONFIG".into(),
            encode_config(&zenoh_config)?,
        );

        // Build role-specific config.
        let role_config = match comp.kind {
            ComponentKind::Producer => {
                let payload = comp
                    .payload
                    .as_ref()
                    .map(PayloadRoleConfig::from)
                    .unwrap_or(PayloadRoleConfig {
                        generator: crate::config::GeneratorType::RandomJson,
                        size_bytes: 256,
                        schema: HashMap::new(),
                        content: None,
                        prefix: None,
                    });

                let cfg = ProducerRoleConfig {
                    key_prefix: resolved.key_prefix.clone(),
                    codec: resolved.codec,
                    cache_size: resolved.cache_size,
                    heartbeat_ms: resolved.heartbeat_ms,
                    recovery_mode: self.config.defaults.recovery_mode,
                    durable: comp.durable,
                    urgency_ms: comp.urgency_ms,
                    rate: comp.rate,
                    rate_per_instance: comp.rate_per_instance,
                    ramp_up_sec: comp.ramp_up_sec,
                    ramp_start_rate: comp.ramp_start_rate,
                    burst_factor: comp.burst_factor,
                    payload,
                    num_partitions: resolved.num_partitions,
                };
                encode_config(&cfg)?
            }

            ComponentKind::Consumer => {
                let cfg = ConsumerRoleConfig {
                    key_prefix: resolved.key_prefix.clone(),
                    codec: resolved.codec,
                    cache_size: resolved.cache_size,
                    heartbeat_ms: resolved.heartbeat_ms,
                    recovery_mode: self.config.defaults.recovery_mode,
                    num_partitions: resolved.num_partitions,
                    consumer_group: comp
                        .consumer_group
                        .as_ref()
                        .map(ConsumerGroupRoleConfig::from),
                    output: OutputRoleConfig::from(comp.output.as_ref()),
                };
                encode_config(&cfg)?
            }

            ComponentKind::Processor => {
                let output_topic = comp
                    .output_topic
                    .as_deref()
                    .and_then(|t| topic_map.get(t).copied());

                let output_resolved = resolve_component_config(
                    comp,
                    output_topic,
                    &self.config.defaults,
                );

                let processing = comp.processing.as_ref();

                let cfg = ProcessorRoleConfig {
                    input_key_prefix: resolved.key_prefix.clone(),
                    output_key_prefix: output_resolved.key_prefix,
                    codec: resolved.codec,
                    cache_size: resolved.cache_size,
                    heartbeat_ms: resolved.heartbeat_ms,
                    recovery_mode: self.config.defaults.recovery_mode,
                    num_partitions: resolved.num_partitions,
                    processing_mode: processing
                        .map(|p| p.mode)
                        .unwrap_or(crate::config::ProcessingMode::Passthrough),
                    delay_ms: processing.and_then(|p| p.delay_ms),
                    drop_probability: processing.and_then(|p| p.drop_probability),
                    consumer_group: comp
                        .consumer_group
                        .as_ref()
                        .map(ConsumerGroupRoleConfig::from),
                };
                encode_config(&cfg)?
            }

            ComponentKind::StorageAgent => {
                let data_dir = comp
                    .data_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("/tmp/mitiflow-emu"));
                let instance_dir = data_dir.join(format!("{}-{}", comp.name, instance));

                let cfg = StorageAgentRoleConfig {
                    key_prefix: resolved.key_prefix.clone(),
                    data_dir: instance_dir,
                    num_partitions: resolved.num_partitions,
                    replication_factor: resolved.replication_factor,
                    capacity: comp.capacity.unwrap_or(100),
                    node_id: Some(format!("{}-{}", comp.name, instance)),
                    codec: resolved.codec,
                    cache_size: resolved.cache_size,
                    heartbeat_ms: resolved.heartbeat_ms,
                    recovery_mode: self.config.defaults.recovery_mode,
                    log_level: comp.log_level.clone(),
                };
                encode_config(&cfg)?
            }

            ComponentKind::Orchestrator => {
                let data_dir = comp
                    .data_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("/tmp/mitiflow-emu-orchestrator"));
                let instance_dir = data_dir.join(format!("{}-{}", comp.name, instance));

                // Collect all topics for auto-registration.
                let topics: Vec<TopicRegistration> = self
                    .config
                    .topics
                    .iter()
                    .map(|t| TopicRegistration {
                        name: t.name.clone(),
                        key_prefix: t.key_prefix.clone(),
                        num_partitions: t.num_partitions,
                        replication_factor: t.replication_factor,
                    })
                    .collect();

                // Use the first topic's key_prefix as the orchestrator prefix,
                // or a default if no topics.
                let key_prefix = self
                    .config
                    .topics
                    .first()
                    .map(|t| t.key_prefix.clone())
                    .unwrap_or_else(|| "mitiflow".into());

                let cfg = OrchestratorRoleConfig {
                    key_prefix,
                    data_dir: instance_dir,
                    lag_interval_ms: comp.lag_interval_ms.unwrap_or(1000),
                    topics,
                };
                encode_config(&cfg)?
            }
        };

        env.insert("MITIFLOW_EMU_CONFIG".into(), role_config);

        // Set RUST_LOG for child process.
        // Component-level log_level overrides the global logging.level.
        let log_level = comp.log_level.clone().unwrap_or_else(|| {
            match self.config.logging.level {
                crate::config::LogLevel::Trace => "trace",
                crate::config::LogLevel::Debug => "debug",
                crate::config::LogLevel::Info => "info",
                crate::config::LogLevel::Warn => "warn",
                crate::config::LogLevel::Error => "error",
            }
            .into()
        });
        env.insert("RUST_LOG".into(), log_level);

        // Resolve binary path: look next to this executable first, then fall back to PATH.
        let bin_name = comp.kind.binary_name();
        let binary = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.join(bin_name)))
            .filter(|p| p.exists())
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| bin_name.into());

        Ok(ComponentSpec {
            name: comp.name.clone(),
            instance,
            binary,
            env,
            work_dir: None,
        })
    }

    /// Graceful shutdown in reverse tier order.
    async fn shutdown(&mut self) {
        info!("Starting graceful shutdown...");

        let _shutdown_timeout = Duration::from_secs(5);

        // Group handles by tier (reverse order).
        let mut by_tier: Vec<(u8, usize)> = self
            .handles
            .iter()
            .enumerate()
            .map(|(i, (comp, _))| (comp.kind.tier(), i))
            .collect();
        by_tier.sort_by(|a, b| b.0.cmp(&a.0));

        for (_, idx) in by_tier {
            let (comp, handles) = &self.handles[idx];
            info!("Stopping {} ({} instances)...", comp.name, handles.len());

            for handle in handles {
                if let Err(e) = handle.stop().await {
                    warn!("Failed to stop {}: {}", handle.id(), e);
                }
            }

            // Wait a bit for graceful exit.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Force kill any remaining.
        for (_, handles) in &self.handles {
            for handle in handles {
                let _ = handle.kill().await;
            }
        }

        // Shutdown log aggregator.
        if let Some(agg) = self.log_aggregator.take() {
            agg.shutdown().await;
        }

        info!("All components stopped.");
    }

    /// Print what would be spawned (dry-run mode).
    fn print_plan(&self) {
        println!("=== Dry Run — Spawn Plan ===\n");

        let _topic_map: HashMap<&str, &crate::config::TopicDef> = self
            .config
            .topics
            .iter()
            .map(|t| (t.name.as_str(), t))
            .collect();

        let mut tiers: Vec<Vec<&ComponentDef>> = vec![Vec::new(); 6];
        for comp in &self.config.components {
            tiers[comp.kind.tier() as usize].push(comp);
        }

        let tier_names = ["", "Orchestrator", "Storage Agents", "Producers", "Processors", "Consumers"];

        for (tier_idx, tier) in tiers.iter().enumerate() {
            if tier.is_empty() {
                continue;
            }
            println!("Tier {} — {}:", tier_idx, tier_names[tier_idx]);
            for comp in tier {
                let topic_name = comp
                    .topic
                    .as_deref()
                    .or(comp.input_topic.as_deref())
                    .unwrap_or("-");
                println!(
                    "  {} (kind={:?}, instances={}, topic={})",
                    comp.name,
                    comp.kind,
                    comp.instances,
                    topic_name
                );
            }
            println!();
        }
    }
}
