//! Producer role binary — spawned by the supervisor.
//!
//! Reads configuration from MITIFLOW_EMU_CONFIG / MITIFLOW_ZENOH_CONFIG
//! environment variables, creates a Zenoh session and EventPublisher,
//! generates payloads at the configured rate using lightbench's rate controller.

use std::time::Duration;

use lightbench::{Benchmark, BenchmarkWork, WorkResult};
use mitiflow::config::{EventBusConfig, HeartbeatMode, RecoveryMode};
use mitiflow::codec::CodecFormat;
use mitiflow::publisher::EventPublisher;
use mitiflow_emulator::config::{PayloadConfig, RecoveryModeConfig};
use mitiflow_emulator::generator::PayloadGenerator;
use mitiflow_emulator::role_config::{decode_config, ProducerRoleConfig, ZenohRoleConfig};

// ---------------------------------------------------------------------------
// BenchmarkWork implementation for lightbench rate control
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct EmulatorPublishWork {
    session: zenoh::Session,
    bus_config: EventBusConfig,
    payload_config: PayloadConfig,
    durable: bool,
}

struct EmulatorPublishState {
    publisher: EventPublisher,
    generator: PayloadGenerator,
}

impl BenchmarkWork for EmulatorPublishWork {
    type State = EmulatorPublishState;

    async fn init(&self) -> Self::State {
        let publisher = EventPublisher::new(&self.session, self.bus_config.clone())
            .await
            .expect("failed to create publisher");
        let generator = PayloadGenerator::from_config(&self.payload_config);
        EmulatorPublishState {
            publisher,
            generator,
        }
    }

    async fn work(&self, state: &mut Self::State) -> WorkResult {
        let payload = state.generator.generate();
        let result = if self.durable {
            state.publisher.publish_bytes_durable(payload).await
        } else {
            state.publisher.publish_bytes(payload).await
        };
        match result {
            Ok(_) => WorkResult::success(0),
            Err(e) => WorkResult::error(e.to_string()),
        }
    }

    async fn cleanup(&self, state: Self::State) {
        state.publisher.shutdown().await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env(),
        )
        .init();

    let zenoh_b64 = std::env::var("MITIFLOW_ZENOH_CONFIG")
        .map_err(|_| anyhow::anyhow!("MITIFLOW_ZENOH_CONFIG not set"))?;
    let config_b64 = std::env::var("MITIFLOW_EMU_CONFIG")
        .map_err(|_| anyhow::anyhow!("MITIFLOW_EMU_CONFIG not set"))?;

    let zenoh_cfg: ZenohRoleConfig = decode_config(&zenoh_b64)?;
    let config: ProducerRoleConfig = decode_config(&config_b64)?;

    // Build zenoh config.
    let mut zc = zenoh::Config::default();
    let me = |e: Box<dyn std::error::Error + Send + Sync>| anyhow::anyhow!("{e}");
    match zenoh_cfg.mode.as_str() {
        "client" => { zc.insert_json5("mode", r#""client""#).map_err(&me)?; }
        "router" => { zc.insert_json5("mode", r#""router""#).map_err(&me)?; }
        _ => { zc.insert_json5("mode", r#""peer""#).map_err(&me)?; }
    }
    if !zenoh_cfg.listen.is_empty() {
        let json = serde_json::to_string(&zenoh_cfg.listen)?;
        zc.insert_json5("listen/endpoints", &json).map_err(&me)?;
    }
    if !zenoh_cfg.connect.is_empty() {
        let json = serde_json::to_string(&zenoh_cfg.connect)?;
        zc.insert_json5("connect/endpoints", &json).map_err(&me)?;
    }

    let session = zenoh::open(zc).await.map_err(&me)?;

    // Build EventBusConfig.
    let codec = match config.codec {
        mitiflow_emulator::config::CodecConfig::Json => CodecFormat::Json,
        mitiflow_emulator::config::CodecConfig::Msgpack => CodecFormat::MsgPack,
        mitiflow_emulator::config::CodecConfig::Postcard => CodecFormat::Postcard,
    };

    let recovery_mode = match config.recovery_mode {
        RecoveryModeConfig::Heartbeat => RecoveryMode::Heartbeat,
        RecoveryModeConfig::PeriodicQuery => {
            RecoveryMode::PeriodicQuery(Duration::from_secs(1))
        }
        RecoveryModeConfig::Both => RecoveryMode::Both,
    };

    let mut bus_builder = EventBusConfig::builder(&config.key_prefix)
        .codec(codec)
        .cache_size(config.cache_size)
        .heartbeat(HeartbeatMode::Sporadic(Duration::from_millis(
            config.heartbeat_ms,
        )))
        .recovery_mode(recovery_mode)
        .num_partitions(config.num_partitions);

    if let Some(urgency) = config.urgency_ms {
        bus_builder = bus_builder.durable_urgency(Duration::from_millis(urgency));
    }

    let bus_config = bus_builder.build()?;
    let payload_config = PayloadConfig::from(&config.payload);

    tracing::info!(
        "Producer started: prefix={}, rate={:?}, ramp_up={:?}, burst={:?}",
        config.key_prefix,
        config.rate_per_instance.or(config.rate),
        config.ramp_up_sec,
        config.burst_factor,
    );

    // Build lightbench BenchmarkConfig for rate control.
    let bench_config = lightbench::BenchmarkConfig {
        rate: if config.rate_per_instance.is_some() {
            None
        } else {
            config.rate
        },
        rate_per_worker: config.rate_per_instance,
        workers: 1,
        duration: 86400, // Effectively infinite; supervisor SIGTERM handles shutdown.
        ramp_up: config.ramp_up_sec.map(|s| s as u64),
        ramp_start_rate: config.ramp_start_rate.unwrap_or(0.0),
        burst_factor: config.burst_factor.unwrap_or(1.0),
        csv: None,
        no_progress: true,
        hide_ramp_progress: true,
        drain_timeout: 0,
    };

    let work = EmulatorPublishWork {
        session: session.clone(),
        bus_config,
        payload_config,
        durable: config.durable,
    };

    // Handle SIGTERM / Ctrl+C for early termination.
    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        cancel_clone.cancel();
    });
    #[cfg(unix)]
    {
        let cancel_sig = cancel.clone();
        tokio::spawn(async move {
            let mut sig =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            sig.recv().await;
            cancel_sig.cancel();
        });
    }

    // Run lightbench rate-controlled benchmark; SIGTERM cancels early.
    tokio::select! {
        _ = Benchmark::from_config(bench_config).work(work).run() => {}
        _ = cancel.cancelled() => {}
    }

    tracing::info!("Producer shutting down");
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
