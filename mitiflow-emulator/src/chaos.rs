//! Chaos scheduler — timed fault injection against running components.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngExt;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::weighted::WeightedIndex;
use rand::rngs::ChaCha8Rng;
use rand_distr::Exp;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::backend::ComponentHandle;
use crate::config::{ChaosAction, ChaosEventDef};
use crate::network_fault::{FaultGuard, NetworkFaultInjector};
use crate::restart_channel::{RestartRequest, RestartSender};

/// Generate random chaos events from config using a seeded PRNG.
///
/// Returns a sorted `Vec<ChaosEventDef>` with `at` fields set to absolute times.
/// All random decisions happen in this function — the output is pure data.
pub fn generate_random_events(
    config: &crate::config::RandomChaosConfig,
    seed: u64,
) -> crate::error::Result<Vec<ChaosEventDef>> {
    use crate::config::RandomActionConfig;

    let mut master = ChaCha8Rng::seed_from_u64(seed);

    let mut timing_rng = ChaCha8Rng::from_rng(&mut master);
    let mut action_rng = ChaCha8Rng::from_rng(&mut master);
    let mut target_rng = ChaCha8Rng::from_rng(&mut master);
    let mut param_rng = ChaCha8Rng::from_rng(&mut master);

    // Map action names to ChaosAction + their configs.
    let mut sorted_keys: Vec<&String> = config.actions.keys().collect();
    sorted_keys.sort();

    let actions: Vec<(ChaosAction, &RandomActionConfig)> = sorted_keys
        .into_iter()
        .filter_map(|name| {
            let cfg = &config.actions[name];
            let action = match name.as_str() {
                "kill" => Some(ChaosAction::Kill),
                "pause" => Some(ChaosAction::Pause),
                "restart" => Some(ChaosAction::Restart),
                "slow" => Some(ChaosAction::Slow),
                "network_partition" => Some(ChaosAction::NetworkPartition),
                "kill_random" => Some(ChaosAction::Kill),
                _ => None,
            };
            action.map(|a| (a, cfg))
        })
        .collect();

    if actions.is_empty() {
        return Err(crate::error::EmulatorError::Chaos(
            "no valid actions in random chaos config".into(),
        ));
    }

    // Build weighted index.
    let weights: Vec<f64> = actions.iter().map(|(_, c)| c.probability).collect();
    let dist = WeightedIndex::new(&weights)
        .map_err(|e| crate::error::EmulatorError::Chaos(format!("invalid weights: {e}")))?;

    // Poisson process: sample inter-arrival times.
    let exp = Exp::new(config.fault_rate)
        .map_err(|e| crate::error::EmulatorError::Chaos(format!("invalid fault_rate: {e}")))?;

    let mut events: Vec<ChaosEventDef> = Vec::new();
    let mut t = Duration::ZERO;
    let mut active_until: Vec<Duration> = Vec::new();

    loop {
        let dt_secs: f64 = exp.sample(&mut timing_rng);
        let dt = Duration::from_secs_f64(dt_secs);
        t = t.saturating_add(dt);
        if t > config.duration {
            break;
        }

        // Enforce min_interval: push time forward if too close to last event.
        if let Some(last) = events.last() {
            let last_at = last.at.unwrap_or(Duration::ZERO);
            if t.saturating_sub(last_at) < config.min_interval {
                t = last_at.saturating_add(config.min_interval);
                if t > config.duration {
                    break;
                }
            }
        }

        // Select action type.
        let action_idx: usize = dist.sample(&mut action_rng);
        let (action, action_config) = &actions[action_idx];

        if *action == ChaosAction::NetworkPartition && action_config.pool.len() < 2 {
            continue;
        }

        // Select target from pool.
        let target = if !action_config.pool.is_empty() {
            let idx: usize = target_rng.random_range(0..action_config.pool.len());
            Some(action_config.pool[idx].clone())
        } else {
            None
        };

        // Sample action-specific parameters within configured ranges.
        let event_def = build_event_def(*action, t, target.clone(), action_config, &mut param_rng);

        // Enforce max_concurrent_faults: compute when this fault expires,
        // count currently active faults, push time forward if at limit.
        let fault_active = fault_active_duration(&event_def);
        let this_expires = t.saturating_add(fault_active);

        // Prune expired faults from tracking.
        active_until.retain(|&expiry| expiry > t);

        if active_until.len() >= config.max_concurrent_faults {
            // Push to when the earliest fault expires.
            if let Some(&earliest) = active_until.iter().min() {
                t = earliest;
                if t > config.duration {
                    break;
                }
                let event_def =
                    build_event_def(*action, t, target.clone(), action_config, &mut param_rng);
                let fault_active = fault_active_duration(&event_def);
                let this_expires = t.saturating_add(fault_active);
                active_until.retain(|&expiry| expiry > t);
                active_until.push(this_expires);
                events.push(event_def);
            }
            continue;
        }

        active_until.push(this_expires);
        events.push(event_def);
    }

    // Events are already sorted by time (Poisson process is sequential).
    Ok(events)
}

/// Build a [`ChaosEventDef`] from action type, time, target, and config ranges.
///
/// Samples action-specific parameters from configured ranges using the provided PRNG.
fn build_event_def(
    action: ChaosAction,
    at: Duration,
    target: Option<String>,
    config: &crate::config::RandomActionConfig,
    rng: &mut ChaCha8Rng,
) -> ChaosEventDef {
    let restart_after = config
        .restart_after
        .map(|[min, max]| rng.random_range(min..=max));
    let delay_ms = config
        .delay_ms
        .map(|[min, max]| rng.random_range(min..=max));
    let heal_after = config
        .heal_after
        .map(|[min, max]| rng.random_range(min..=max));
    let duration = config
        .pause_duration
        .map(|[min, max]| rng.random_range(min..=max));

    let partition_from = if action == ChaosAction::NetworkPartition {
        config
            .pool
            .iter()
            .filter(|p| target.as_ref().is_none_or(|t| p != &t))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };

    ChaosEventDef {
        at: Some(at),
        every: None, // Random events are one-shot.
        action,
        target,
        instance: None,
        restart_after,
        duration,
        delay_ms,
        partition_from,
        heal_after,
        loss_percent: None,
        bandwidth: None,
        jitter_ms: None,
        pool: config.pool.clone(),
    }
}

/// Compute how long a fault remains "active" based on its action-specific duration fields.
fn fault_active_duration(event: &ChaosEventDef) -> Duration {
    event
        .restart_after
        .or(event.duration)
        .or(event.heal_after)
        .unwrap_or(Duration::ZERO)
}

/// A runtime chaos event ready for scheduling.
struct ScheduledEvent {
    def: ChaosEventDef,
    next_fire: Duration,
    fired: bool,
    source: crate::metrics::ChaosEventSource,
}

/// Chaos scheduler that executes fault injection events on a timeline.
pub struct ChaosScheduler {
    events: Vec<ScheduledEvent>,
    restart_tx: RestartSender,
    fault_injector: Option<Arc<dyn NetworkFaultInjector>>,
    rng: Option<ChaCha8Rng>,
    active_faults: Vec<FaultGuard>,
    manifest_writer: Option<crate::metrics::ChaosManifestWriter>,
    seed: Option<u64>,
    has_random: bool,
}

impl ChaosScheduler {
    /// Build a scheduler from chaos event definitions.
    pub fn new(defs: &[ChaosEventDef], restart_tx: RestartSender) -> Self {
        let events = defs
            .iter()
            .map(|def| {
                let next_fire = def.at.unwrap_or(def.every.unwrap_or(Duration::ZERO));
                ScheduledEvent {
                    def: def.clone(),
                    next_fire,
                    fired: false,
                    source: crate::metrics::ChaosEventSource::Fixed,
                }
            })
            .collect();
        Self {
            events,
            restart_tx,
            fault_injector: None,
            rng: None,
            active_faults: Vec::new(),
            manifest_writer: None,
            seed: None,
            has_random: false,
        }
    }

    /// Build a scheduler from a fixed schedule + optional random chaos config.
    ///
    /// Random events are pre-generated using the seeded PRNG and merged into
    /// the timeline alongside fixed events. The run loop handles them uniformly.
    pub fn new_unified(
        fixed_schedule: &[ChaosEventDef],
        random_config: Option<&crate::config::RandomChaosConfig>,
        seed: u64,
        restart_tx: RestartSender,
    ) -> crate::error::Result<Self> {
        let has_random = random_config.is_some();
        let fixed_count = fixed_schedule.len();

        let mut all_events: Vec<ChaosEventDef> = fixed_schedule.to_vec();
        if let Some(random) = random_config {
            let random_events = generate_random_events(random, seed)?;
            all_events.extend(random_events);
        }

        let mut tagged: Vec<ScheduledEvent> = all_events
            .into_iter()
            .enumerate()
            .map(|(i, def)| {
                let next_fire = def.at.unwrap_or(def.every.unwrap_or(Duration::ZERO));
                ScheduledEvent {
                    def,
                    next_fire,
                    fired: false,
                    source: if i < fixed_count {
                        crate::metrics::ChaosEventSource::Fixed
                    } else {
                        crate::metrics::ChaosEventSource::Random
                    },
                }
            })
            .collect();
        tagged.sort_by_key(|e| e.next_fire);

        Ok(Self {
            events: tagged,
            restart_tx,
            fault_injector: None,
            rng: None,
            active_faults: Vec::new(),
            manifest_writer: None,
            seed: Some(seed),
            has_random,
        })
    }

    /// Attach a network fault injector (required for `Slow` and `NetworkPartition` actions).
    pub fn with_fault_injector(mut self, injector: Arc<dyn NetworkFaultInjector>) -> Self {
        self.fault_injector = Some(injector);
        self
    }

    /// Attach a seeded PRNG for deterministic random actions (KillRandom).
    pub fn with_rng(mut self, rng: ChaCha8Rng) -> Self {
        self.rng = Some(rng);
        self
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn with_random_flag(mut self, has_random: bool) -> Self {
        self.has_random = has_random;
        self
    }

    pub fn with_chaos_manifest(mut self, writer: crate::metrics::ChaosManifestWriter) -> Self {
        self.manifest_writer = Some(writer);
        self
    }

    /// Run the chaos schedule.
    ///
    /// `lookup` resolves a component name + optional instance to a handle.
    /// The scheduler runs until `cancel` is triggered.
    pub async fn run<F>(&mut self, cancel: CancellationToken, start: Instant, mut lookup: F)
    where
        F: FnMut(&str, Option<usize>) -> Option<HandleRef>,
    {
        loop {
            // Find the next event to fire.
            let next = self
                .events
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    if e.fired && e.def.every.is_none() {
                        return false; // One-shot already fired.
                    }
                    true
                })
                .min_by_key(|(_, e)| e.next_fire);

            let Some((idx, _)) = next else {
                // No more events.
                info!("Chaos scheduler: no more events to schedule");
                return;
            };

            let wait_until = self.events[idx].next_fire;
            let elapsed = start.elapsed();

            if wait_until > elapsed {
                let delay = wait_until - elapsed;
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = cancel.cancelled() => {
                        info!("Chaos scheduler cancelled");
                        return;
                    }
                }
            }

            // Fire the event.
            let event = &self.events[idx];
            let def = event.def.clone();
            let at_ms = self.events[idx].next_fire.as_millis() as u64;

            info!(
                "Chaos: firing {:?} on target={:?} instance={:?}",
                def.action, def.target, def.instance
            );

            let actual_target = self.execute_action(&def, &mut lookup).await;

            if let Some(writer) = &mut self.manifest_writer {
                let entry = crate::metrics::ChaosManifestEntry {
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    elapsed_ms: at_ms,
                    seed: self.seed,
                    sequence: 0,
                    source: self.events[idx].source,
                    action: format!("{:?}", def.action).to_lowercase(),
                    target: actual_target.or(def.target.clone()).unwrap_or_default(),
                    instance: def.instance,
                    parameters: serde_json::json!({
                        "at_ms": at_ms,
                        "restart_after_ms": def.restart_after.map(|d| d.as_millis()),
                        "duration_ms": def.duration.map(|d| d.as_millis()),
                        "delay_ms": def.delay_ms,
                        "heal_after_ms": def.heal_after.map(|d| d.as_millis()),
                        "partition_from": if def.partition_from.is_empty() { serde_json::Value::Null } else { serde_json::json!(def.partition_from) },
                        "loss_percent": def.loss_percent,
                        "bandwidth": def.bandwidth,
                        "jitter_ms": def.jitter_ms,
                        "pool": if def.pool.is_empty() { serde_json::Value::Null } else { serde_json::json!(def.pool) },
                    }),
                };
                if let Err(e) = writer.write_entry(&entry) {
                    error!("Failed to write chaos manifest entry: {e}");
                }
            }

            // Update scheduling state.
            let event = &mut self.events[idx];
            event.fired = true;
            if let Some(interval) = event.def.every {
                event.next_fire += interval;
            }
        }
    }

    async fn execute_action<F>(&mut self, def: &ChaosEventDef, lookup: &mut F) -> Option<String>
    where
        F: FnMut(&str, Option<usize>) -> Option<HandleRef>,
    {
        match def.action {
            ChaosAction::Kill => {
                if let Some(target) = &def.target {
                    if let Some(handle) = lookup(target, def.instance) {
                        if let Err(e) = handle.kill().await {
                            error!("Chaos kill failed for {}: {}", target, e);
                        }

                        // Restart after delay if specified.
                        if let Some(restart_delay) = def.restart_after {
                            let target_name = target.clone();
                            let request = RestartRequest {
                                component: target_name.clone(),
                                instance: def.instance,
                                delay: restart_delay,
                            };

                            if let Err(e) = self.restart_tx.send(request).await {
                                error!(
                                    "Chaos: failed to enqueue restart for {}:{:?}: {}",
                                    target_name, def.instance, e
                                );
                            }
                        }
                    } else {
                        warn!("Chaos: target not found: {}:{:?}", target, def.instance);
                    }
                }
                def.target.clone()
            }

            ChaosAction::Pause => {
                if let Some(target) = &def.target
                    && let Some(handle) = lookup(target, def.instance)
                {
                    if let Err(e) = handle.pause().await {
                        error!("Chaos pause failed for {}: {}", target, e);
                    }

                    if let Some(duration) = def.duration {
                        let target = target.clone();
                        let handle_ref = lookup(&target, def.instance);
                        if let Some(h) = handle_ref {
                            let dur = duration;
                            tokio::spawn(async move {
                                tokio::time::sleep(dur).await;
                                info!("Chaos: resuming {} after pause", target);
                                if let Err(e) = h.resume().await {
                                    error!("Chaos: failed to resume {}: {}", target, e);
                                }
                            });
                        }
                    }
                }
                def.target.clone()
            }

            ChaosAction::Restart => {
                if let Some(target) = &def.target
                    && let Some(handle) = lookup(target, def.instance)
                    && let Err(e) = handle.stop().await
                {
                    error!("Chaos restart stop failed for {}: {}", target, e);
                }
                def.target.clone()
            }

            ChaosAction::Slow => {
                if let Some(target) = &def.target {
                    let delay_ms = def.delay_ms.unwrap_or(100) as u32;
                    let loss_pct = def.loss_percent.unwrap_or(0.0);
                    let Some(injector) = self.fault_injector.as_ref() else {
                        error!("Slow action requires a fault injector but none configured");
                        return None;
                    };
                    match injector.add_latency(target, delay_ms, loss_pct).await {
                        Ok(guard) => {
                            if let Some(duration) = def.duration {
                                let guard = Arc::new(Mutex::new(Some(guard)));
                                let g = guard.clone();
                                let target_name = target.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(duration).await;
                                    info!("Removing slow fault for {}", target_name);
                                    let _ = g.lock().await.take();
                                });
                            } else {
                                self.active_faults.push(guard);
                            }
                        }
                        Err(e) => error!("Slow action failed for {}: {}", target, e),
                    }
                }
                def.target.clone()
            }

            ChaosAction::KillRandom => {
                if !def.pool.is_empty() {
                    let target_name = {
                        let idx: usize = self
                            .rng
                            .as_mut()
                            .map(|rng| rng.random_range(0..def.pool.len()))
                            .unwrap_or(0);
                        def.pool[idx].clone()
                    };
                    info!("Chaos: kill_random selected {}", target_name);

                    if let Some(handle) = lookup(&target_name, None)
                        && let Err(e) = handle.kill().await
                    {
                        error!("Chaos kill_random failed for {}: {}", target_name, e);
                    }
                    Some(target_name)
                } else {
                    None
                }
            }

            ChaosAction::NetworkPartition => {
                if let Some(target) = &def.target {
                    if def.partition_from.is_empty() {
                        error!("NetworkPartition requires non-empty partition_from");
                        return None;
                    }
                    let Some(injector) = self.fault_injector.as_ref() else {
                        error!("NetworkPartition requires a fault injector");
                        return None;
                    };
                    match injector.partition(target, &def.partition_from).await {
                        Ok(guard) => {
                            if let Some(heal_after) = def.heal_after {
                                let guard = Arc::new(Mutex::new(Some(guard)));
                                let g = guard.clone();
                                let target_name = target.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(heal_after).await;
                                    info!("Healing network partition for {}", target_name);
                                    let _ = g.lock().await.take();
                                });
                            } else {
                                self.active_faults.push(guard);
                            }
                        }
                        Err(e) => error!("NetworkPartition failed for {}: {}", target, e),
                    }
                }
                def.target.clone()
            }
        }
    }
}

/// A reference to a component handle that the chaos scheduler can operate on.
/// This is a thin wrapper to avoid lifetime issues with the handle lookup.
pub type HandleRef = &'static dyn ComponentHandle;

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::backend::ComponentExitStatus;
    use crate::restart_channel::create_restart_channel;

    struct MockHandle {
        kill_calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ComponentHandle for MockHandle {
        fn id(&self) -> &str {
            "mock"
        }

        async fn stop(&self) -> crate::error::Result<()> {
            Ok(())
        }

        async fn kill(&self) -> crate::error::Result<()> {
            self.kill_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn pause(&self) -> crate::error::Result<()> {
            Ok(())
        }

        async fn resume(&self) -> crate::error::Result<()> {
            Ok(())
        }

        async fn wait(&mut self) -> crate::error::Result<ComponentExitStatus> {
            Ok(ComponentExitStatus { code: Some(0) })
        }

        async fn restart(&mut self) -> crate::error::Result<()> {
            Ok(())
        }

        fn take_stdout(
            &mut self,
        ) -> Option<tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>> {
            None
        }

        fn take_stderr(
            &mut self,
        ) -> Option<tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStderr>>> {
            None
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sends_restart_request_for_kill_with_restart_after() {
        let (restart_tx, mut restart_rx) = create_restart_channel();
        let mut scheduler = ChaosScheduler::new(
            &[ChaosEventDef {
                at: Some(Duration::ZERO),
                every: None,
                action: ChaosAction::Kill,
                target: Some("producer-a".to_string()),
                instance: Some(1),
                restart_after: Some(Duration::from_millis(50)),
                duration: None,
                delay_ms: None,
                partition_from: Vec::new(),
                heal_after: None,
                loss_percent: None,
                bandwidth: None,
                jitter_ms: None,
                pool: Vec::new(),
            }],
            restart_tx,
        );

        let kill_calls = Arc::new(AtomicUsize::new(0));
        let handle: HandleRef = Box::leak(Box::new(MockHandle {
            kill_calls: kill_calls.clone(),
        }));

        scheduler
            .run(
                CancellationToken::new(),
                Instant::now(),
                move |name, instance| {
                    if name == "producer-a" && instance == Some(1) {
                        Some(handle)
                    } else {
                        None
                    }
                },
            )
            .await;

        let received = tokio::time::timeout(Duration::from_secs(1), restart_rx.recv())
            .await
            .expect("restart request should be sent")
            .expect("restart request should be available");

        assert_eq!(received.component, "producer-a");
        assert_eq!(received.instance, Some(1));
        assert_eq!(received.delay, Duration::from_millis(50));
        assert_eq!(kill_calls.load(Ordering::SeqCst), 1);
    }

    struct MockInjector {
        partition_calls: Arc<AtomicUsize>,
        last_target: Arc<std::sync::Mutex<Option<String>>>,
        last_from: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl MockInjector {
        fn new() -> Self {
            Self {
                partition_calls: Arc::new(AtomicUsize::new(0)),
                last_target: Arc::new(std::sync::Mutex::new(None)),
                last_from: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl NetworkFaultInjector for MockInjector {
        async fn add_latency(
            &self,
            _target: &str,
            _delay_ms: u32,
            _loss_percent: f64,
        ) -> crate::error::Result<FaultGuard> {
            Ok(FaultGuard::new(|| Box::pin(async { Ok(()) })))
        }

        async fn partition(
            &self,
            target: &str,
            from: &[String],
        ) -> crate::error::Result<FaultGuard> {
            self.partition_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_target.lock().expect("lock") = Some(target.to_string());
            *self.last_from.lock().expect("lock") = from.to_vec();
            Ok(FaultGuard::new(|| Box::pin(async { Ok(()) })))
        }

        async fn cleanup(&self) -> crate::error::Result<()> {
            Ok(())
        }
    }

    struct MockFaultInjector {
        latency_calls: Arc<AtomicUsize>,
        partition_calls: Arc<AtomicUsize>,
        cleanup_tx: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    }

    #[async_trait::async_trait]
    impl NetworkFaultInjector for MockFaultInjector {
        async fn add_latency(
            &self,
            _target: &str,
            _delay_ms: u32,
            _loss_percent: f64,
        ) -> crate::error::Result<FaultGuard> {
            self.latency_calls.fetch_add(1, Ordering::SeqCst);
            let tx = self.cleanup_tx.lock().await.take();
            Ok(FaultGuard::new(move || {
                Box::pin(async move {
                    if let Some(tx) = tx {
                        let _ = tx.send(());
                    }
                    Ok(())
                })
            }))
        }

        async fn partition(
            &self,
            _target: &str,
            _from: &[String],
        ) -> crate::error::Result<FaultGuard> {
            self.partition_calls.fetch_add(1, Ordering::SeqCst);
            let tx = self.cleanup_tx.lock().await.take();
            Ok(FaultGuard::new(move || {
                Box::pin(async move {
                    if let Some(tx) = tx {
                        let _ = tx.send(());
                    }
                    Ok(())
                })
            }))
        }

        async fn cleanup(&self) -> crate::error::Result<()> {
            Ok(())
        }
    }

    fn make_slow_def(duration: Option<Duration>) -> ChaosEventDef {
        ChaosEventDef {
            at: Some(Duration::ZERO),
            every: None,
            action: ChaosAction::Slow,
            target: Some("node-a".to_string()),
            instance: None,
            restart_after: None,
            duration,
            delay_ms: Some(100),
            partition_from: Vec::new(),
            heal_after: None,
            loss_percent: Some(0.5),
            bandwidth: None,
            jitter_ms: None,
            pool: Vec::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_action_calls_injector() {
        let (restart_tx, _rx) = create_restart_channel();
        let injector = Arc::new(MockFaultInjector {
            latency_calls: Arc::new(AtomicUsize::new(0)),
            partition_calls: Arc::new(AtomicUsize::new(0)),
            cleanup_tx: Arc::new(tokio::sync::Mutex::new(None)),
        });
        let latency_calls = injector.latency_calls.clone();

        let mut scheduler =
            ChaosScheduler::new(&[make_slow_def(None)], restart_tx).with_fault_injector(injector);

        scheduler
            .run(CancellationToken::new(), Instant::now(), |_, _| None)
            .await;

        assert_eq!(latency_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_duration_cleanup() {
        let (restart_tx, _rx) = create_restart_channel();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let injector = Arc::new(MockFaultInjector {
            latency_calls: Arc::new(AtomicUsize::new(0)),
            partition_calls: Arc::new(AtomicUsize::new(0)),
            cleanup_tx: Arc::new(tokio::sync::Mutex::new(Some(tx))),
        });

        let mut scheduler = ChaosScheduler::new(
            &[make_slow_def(Some(Duration::from_millis(50)))],
            restart_tx,
        )
        .with_fault_injector(injector);

        scheduler
            .run(CancellationToken::new(), Instant::now(), |_, _| None)
            .await;

        tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("slow fault cleanup should fire within 1s")
            .expect("cleanup signal received");
    }

    fn make_partition_def(
        target: Option<&str>,
        partition_from: Vec<String>,
        heal_after: Option<Duration>,
    ) -> ChaosEventDef {
        ChaosEventDef {
            at: Some(Duration::ZERO),
            every: None,
            action: ChaosAction::NetworkPartition,
            target: target.map(str::to_string),
            instance: None,
            restart_after: None,
            duration: None,
            delay_ms: None,
            partition_from,
            heal_after,
            loss_percent: None,
            bandwidth: None,
            jitter_ms: None,
            pool: Vec::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn network_partition_action_calls_injector() {
        let (restart_tx, _rx) = create_restart_channel();
        let injector = Arc::new(MockInjector::new());
        let partition_calls = injector.partition_calls.clone();
        let last_target = injector.last_target.clone();
        let last_from = injector.last_from.clone();

        let mut scheduler = ChaosScheduler::new(
            &[make_partition_def(
                Some("node-a"),
                vec!["node-b".to_string(), "node-c".to_string()],
                None,
            )],
            restart_tx,
        )
        .with_fault_injector(injector);

        scheduler
            .run(CancellationToken::new(), Instant::now(), |_, _| None)
            .await;

        assert_eq!(partition_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *last_target.lock().expect("lock"),
            Some("node-a".to_string())
        );
        assert_eq!(
            *last_from.lock().expect("lock"),
            vec!["node-b".to_string(), "node-c".to_string()]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_empty_from_logs_error() {
        let (restart_tx, _rx) = create_restart_channel();
        let injector = Arc::new(MockInjector::new());
        let partition_calls = injector.partition_calls.clone();

        let mut scheduler = ChaosScheduler::new(
            &[make_partition_def(Some("node-a"), Vec::new(), None)],
            restart_tx,
        )
        .with_fault_injector(injector);

        scheduler
            .run(CancellationToken::new(), Instant::now(), |_, _| None)
            .await;

        assert_eq!(
            partition_calls.load(Ordering::SeqCst),
            0,
            "injector must not be called when partition_from is empty"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_heal_after_drops_guard() {
        let (restart_tx, _rx) = create_restart_channel();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let tx = Arc::new(std::sync::Mutex::new(Some(tx)));

        struct SignallingInjector {
            tx: Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
        }

        #[async_trait::async_trait]
        impl NetworkFaultInjector for SignallingInjector {
            async fn add_latency(
                &self,
                _target: &str,
                _delay_ms: u32,
                _loss_percent: f64,
            ) -> crate::error::Result<FaultGuard> {
                Ok(FaultGuard::new(|| Box::pin(async { Ok(()) })))
            }

            async fn partition(
                &self,
                _target: &str,
                _from: &[String],
            ) -> crate::error::Result<FaultGuard> {
                let tx = self.tx.clone();
                Ok(FaultGuard::new(move || {
                    Box::pin(async move {
                        if let Some(sender) = tx.lock().expect("lock").take() {
                            let _ = sender.send(());
                        }
                        Ok(())
                    })
                }))
            }

            async fn cleanup(&self) -> crate::error::Result<()> {
                Ok(())
            }
        }

        let injector = Arc::new(SignallingInjector { tx });

        let mut scheduler = ChaosScheduler::new(
            &[make_partition_def(
                Some("node-a"),
                vec!["node-b".to_string()],
                Some(Duration::from_millis(50)),
            )],
            restart_tx,
        )
        .with_fault_injector(injector);

        scheduler
            .run(CancellationToken::new(), Instant::now(), |_, _| None)
            .await;

        tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("guard cleanup should fire within 2s")
            .expect("cleanup signal received");
    }

    #[test]
    fn kill_random_uses_provided_rng_deterministic() {
        use rand::{RngExt, SeedableRng, rngs::ChaCha8Rng};

        let pool = ["a".to_string(), "b".to_string(), "c".to_string()];

        let mut selections1 = Vec::new();
        let mut rng1 = ChaCha8Rng::seed_from_u64(42);
        for _ in 0..20 {
            let idx = rng1.random_range(0..pool.len());
            selections1.push(pool[idx].clone());
        }

        let mut selections2 = Vec::new();
        let mut rng2 = ChaCha8Rng::seed_from_u64(42);
        for _ in 0..20 {
            let idx = rng2.random_range(0..pool.len());
            selections2.push(pool[idx].clone());
        }

        assert_eq!(
            selections1, selections2,
            "same seed must produce identical selections"
        );
        let unique: std::collections::HashSet<_> = selections1.into_iter().collect();
        assert!(
            unique.len() > 1,
            "RNG should produce varied selections, got all same"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kill_random_works_without_seeded_rng() {
        let (restart_tx, _rx) = create_restart_channel();

        let kill_calls = Arc::new(AtomicUsize::new(0));
        let handle: HandleRef = Box::leak(Box::new(MockHandle {
            kill_calls: kill_calls.clone(),
        }));

        let mut scheduler = ChaosScheduler::new(
            &[ChaosEventDef {
                at: Some(Duration::ZERO),
                every: None,
                action: ChaosAction::KillRandom,
                target: None,
                instance: None,
                restart_after: None,
                duration: None,
                delay_ms: None,
                partition_from: Vec::new(),
                heal_after: None,
                loss_percent: None,
                bandwidth: None,
                jitter_ms: None,
                pool: vec!["x".to_string()],
            }],
            restart_tx,
        );

        scheduler
            .run(
                CancellationToken::new(),
                Instant::now(),
                move |name, _inst| {
                    if name == "x" { Some(handle) } else { None }
                },
            )
            .await;

        assert_eq!(kill_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn kill_random_normalized_to_kill_in_random_generation() {
        use crate::config::{RandomActionConfig, RandomChaosConfig};

        let config = RandomChaosConfig {
            fault_rate: 10.0,
            duration: Duration::from_secs(5),
            max_concurrent_faults: 10,
            min_interval: Duration::from_millis(10),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill_random".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["a".into(), "b".into(), "c".into()],
                        restart_after: Some([Duration::from_millis(100), Duration::from_secs(1)]),
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 42).expect("generate");
        assert!(!events.is_empty(), "should produce events");

        for event in &events {
            assert_eq!(
                event.action,
                ChaosAction::Kill,
                "kill_random should be normalized to Kill in random generation"
            );
            assert!(
                event.target.is_some(),
                "kill_random should have a concrete target after normalization"
            );
            assert!(
                event
                    .pool
                    .iter()
                    .any(|p| p == event.target.as_ref().unwrap()),
                "target should come from the configured pool"
            );
        }
    }

    // --- T3: Seeded PRNG tests ---

    #[test]
    fn seeded_rng_determinism() {
        use crate::config::{RandomActionConfig, RandomChaosConfig};

        let config = RandomChaosConfig {
            fault_rate: 1.0,
            duration: Duration::from_secs(10),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(100),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["a".into(), "b".into(), "c".into()],
                        restart_after: Some([Duration::from_secs(1), Duration::from_secs(5)]),
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events1 = super::generate_random_events(&config, 42).expect("gen1");
        let events2 = super::generate_random_events(&config, 42).expect("gen2");

        assert_eq!(
            events1.len(),
            events2.len(),
            "same seed must produce same count"
        );
        for (a, b) in events1.iter().zip(events2.iter()) {
            assert_eq!(a.at, b.at, "same seed must produce same times");
            assert_eq!(a.target, b.target, "same seed must produce same targets");
            assert_eq!(
                a.restart_after, b.restart_after,
                "same seed must produce same params"
            );
        }
    }

    #[test]
    fn seeded_rng_different_seeds() {
        use crate::config::{RandomActionConfig, RandomChaosConfig};

        let config = RandomChaosConfig {
            fault_rate: 1.0,
            duration: Duration::from_secs(10),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(100),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["a".into(), "b".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events1 = super::generate_random_events(&config, 42).expect("gen1");
        let events2 = super::generate_random_events(&config, 99).expect("gen2");

        let ats1: Vec<_> = events1.iter().map(|e| e.at).collect();
        let ats2: Vec<_> = events2.iter().map(|e| e.at).collect();
        assert_ne!(
            ats1, ats2,
            "different seeds must produce different sequences"
        );
    }

    #[test]
    fn poisson_timing() {
        use crate::config::{RandomActionConfig, RandomChaosConfig};

        let config = RandomChaosConfig {
            fault_rate: 0.5,
            duration: Duration::from_secs(200),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(100),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["x".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 12345).expect("gen");

        if events.len() > 1 {
            let total_time = events.last().expect("last").at.expect("at").as_secs_f64();
            let mean = total_time / (events.len() - 1) as f64;
            assert!(
                (mean - 2.0).abs() < 0.3,
                "mean inter-arrival {mean:.3}s should be ≈2.0s (fault_rate=0.5)"
            );
        }
    }

    #[test]
    fn weighted_action_selection() {
        use crate::config::{RandomActionConfig, RandomChaosConfig};

        let config = RandomChaosConfig {
            fault_rate: 5.0,
            duration: Duration::from_secs(20),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(10),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 0.7,
                        pool: vec!["x".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m.insert(
                    "pause".into(),
                    RandomActionConfig {
                        probability: 0.3,
                        pool: vec!["x".into()],
                        pause_duration: Some([
                            Duration::from_millis(100),
                            Duration::from_millis(200),
                        ]),
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 42).expect("gen");

        let kills = events
            .iter()
            .filter(|e| matches!(e.action, ChaosAction::Kill))
            .count();
        let pauses = events
            .iter()
            .filter(|e| matches!(e.action, ChaosAction::Pause))
            .count();
        let total = kills + pauses;

        assert!(
            total > 50,
            "need sufficient events for statistical test, got {total}"
        );

        let kill_pct = kills as f64 / total as f64;
        assert!(
            (kill_pct - 0.7).abs() < 0.1,
            "kill percentage {kill_pct:.2} should be ≈0.70 (±0.10)"
        );
    }

    #[test]
    fn generate_random_events_respects_duration() {
        use crate::config::{RandomActionConfig, RandomChaosConfig};

        let config = RandomChaosConfig {
            fault_rate: 2.0,
            duration: Duration::from_secs(5),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(50),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["x".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 42).expect("gen");

        for event in &events {
            let at = event.at.expect("all random events should have at set");
            assert!(
                at <= config.duration,
                "event at {at:?} exceeds duration {:?}",
                config.duration
            );
        }
    }

    #[test]
    fn parameter_ranges() {
        use crate::config::{RandomActionConfig, RandomChaosConfig};

        let config = RandomChaosConfig {
            fault_rate: 2.0,
            duration: Duration::from_secs(10),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(50),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["x".into()],
                        restart_after: Some([Duration::from_secs(2), Duration::from_secs(10)]),
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 42).expect("gen");

        for event in &events {
            if let Some(ra) = event.restart_after {
                assert!(
                    ra >= Duration::from_secs(2) && ra <= Duration::from_secs(10),
                    "restart_after {ra:?} outside range [2s, 10s]"
                );
            }
        }
    }

    // --- T5: Unified timeline tests ---

    fn make_random_config() -> crate::config::RandomChaosConfig {
        use crate::config::RandomActionConfig;
        crate::config::RandomChaosConfig {
            fault_rate: 1.0,
            duration: Duration::from_secs(10),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(100),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["target-a".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        }
    }

    #[test]
    fn unified_timeline_fixed_only() {
        let (restart_tx, _) = create_restart_channel();
        let fixed = vec![ChaosEventDef {
            at: Some(Duration::from_secs(5)),
            every: None,
            action: ChaosAction::Kill,
            target: Some("x".into()),
            instance: None,
            restart_after: None,
            duration: None,
            delay_ms: None,
            partition_from: Vec::new(),
            heal_after: None,
            loss_percent: None,
            bandwidth: None,
            jitter_ms: None,
            pool: Vec::new(),
        }];

        let scheduler = ChaosScheduler::new_unified(&fixed, None, 42, restart_tx).expect("unified");
        assert_eq!(scheduler.events.len(), 1);
    }

    #[test]
    fn unified_timeline_random_only() {
        let (restart_tx, _) = create_restart_channel();
        let config = make_random_config();

        let scheduler =
            ChaosScheduler::new_unified(&[], Some(&config), 42, restart_tx).expect("unified");
        assert!(
            !scheduler.events.is_empty(),
            "random config should generate events"
        );
    }

    #[test]
    fn unified_timeline_merges_both() {
        let (restart_tx, _) = create_restart_channel();
        let fixed = vec![ChaosEventDef {
            at: Some(Duration::from_secs(5)),
            every: None,
            action: ChaosAction::Kill,
            target: Some("x".into()),
            instance: None,
            restart_after: None,
            duration: None,
            delay_ms: None,
            partition_from: Vec::new(),
            heal_after: None,
            loss_percent: None,
            bandwidth: None,
            jitter_ms: None,
            pool: Vec::new(),
        }];
        let config = make_random_config();

        let only_fixed =
            ChaosScheduler::new_unified(&fixed, None, 42, restart_tx.clone()).expect("fixed");
        let fixed_count = only_fixed.events.len();

        let merged =
            ChaosScheduler::new_unified(&fixed, Some(&config), 42, restart_tx).expect("merged");
        assert!(
            merged.events.len() > fixed_count,
            "merged should have more events than fixed-only"
        );
    }

    // --- T6: Guardrails tests ---

    #[test]
    fn min_interval_enforced() {
        use crate::config::RandomActionConfig;

        let config = crate::config::RandomChaosConfig {
            fault_rate: 10.0,
            duration: Duration::from_secs(10),
            max_concurrent_faults: 100,
            min_interval: Duration::from_secs(2),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["x".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 42).expect("gen");

        for window in events.windows(2) {
            let t0 = window[0].at.expect("at");
            let t1 = window[1].at.expect("at");
            let gap = t1.saturating_sub(t0);
            assert!(
                gap >= config.min_interval,
                "events at {t0:?} and {t1:?} are {gap:?} apart, below min_interval {:?}",
                config.min_interval
            );
        }
    }

    #[test]
    fn max_concurrent_faults_enforced() {
        use crate::config::RandomActionConfig;

        let config = crate::config::RandomChaosConfig {
            fault_rate: 5.0,
            duration: Duration::from_secs(10),
            max_concurrent_faults: 2,
            min_interval: Duration::from_millis(10),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["x".into()],
                        restart_after: Some([Duration::from_secs(1), Duration::from_secs(3)]),
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 42).expect("gen");

        for (i, event) in events.iter().enumerate() {
            let t = event.at.expect("at");
            let active = super::fault_active_duration(event);
            let expires = t.saturating_add(active);
            let concurrent = events
                .iter()
                .enumerate()
                .filter(|(j, e)| {
                    *j != i
                        && e.at.expect("at") <= t
                        && e.at
                            .expect("at")
                            .saturating_add(super::fault_active_duration(e))
                            > t
                })
                .count();
            assert!(
                concurrent < config.max_concurrent_faults,
                "event {i} at {t:?} has {concurrent} concurrent faults (max {})",
                config.max_concurrent_faults
            );
            let _ = expires;
        }
    }

    #[test]
    fn guardrails_defaults() {
        let config = crate::config::RandomChaosConfig {
            fault_rate: 1.0,
            duration: Duration::from_secs(5),
            max_concurrent_faults: 3,
            min_interval: Duration::from_secs(1),
            actions: HashMap::new(),
        };
        assert_eq!(config.max_concurrent_faults, 3);
        assert_eq!(config.min_interval, Duration::from_secs(1));
    }

    // --- T8: Full determinism tests ---

    #[test]
    fn full_determinism_same_seed() {
        use crate::config::RandomActionConfig;

        let config = crate::config::RandomChaosConfig {
            fault_rate: 1.0,
            duration: Duration::from_secs(30),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(100),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 0.3,
                        pool: vec!["a".into(), "b".into()],
                        restart_after: Some([Duration::from_secs(1), Duration::from_secs(5)]),
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m.insert(
                    "pause".into(),
                    RandomActionConfig {
                        probability: 0.3,
                        pool: vec!["c".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: Some([Duration::from_millis(500), Duration::from_secs(2)]),
                    },
                );
                m.insert(
                    "slow".into(),
                    RandomActionConfig {
                        probability: 0.2,
                        pool: vec!["d".into()],
                        restart_after: None,
                        delay_ms: Some([50, 200]),
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m.insert(
                    "network_partition".into(),
                    RandomActionConfig {
                        probability: 0.2,
                        pool: vec!["e".into(), "f".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: Some([Duration::from_secs(2), Duration::from_secs(10)]),
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events1 = super::generate_random_events(&config, 42).expect("gen1");
        let events2 = super::generate_random_events(&config, 42).expect("gen2");

        assert_eq!(
            events1.len(),
            events2.len(),
            "same seed must produce same count"
        );
        for (i, (a, b)) in events1.iter().zip(events2.iter()).enumerate() {
            assert_eq!(a.at, b.at, "event {i}: at mismatch");
            assert_eq!(a.action, b.action, "event {i}: action mismatch");
            assert_eq!(a.target, b.target, "event {i}: target mismatch");
            assert_eq!(
                a.restart_after, b.restart_after,
                "event {i}: restart_after mismatch"
            );
            assert_eq!(a.duration, b.duration, "event {i}: duration mismatch");
            assert_eq!(a.delay_ms, b.delay_ms, "event {i}: delay_ms mismatch");
            assert_eq!(a.heal_after, b.heal_after, "event {i}: heal_after mismatch");
        }
    }

    #[test]
    fn full_determinism_different_seed() {
        use crate::config::RandomActionConfig;

        let config = crate::config::RandomChaosConfig {
            fault_rate: 2.0,
            duration: Duration::from_secs(10),
            max_concurrent_faults: 3,
            min_interval: Duration::from_millis(100),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["x".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events1 = super::generate_random_events(&config, 42).expect("gen1");
        let events2 = super::generate_random_events(&config, 99).expect("gen2");

        let ats1: Vec<_> = events1.iter().map(|e| e.at).collect();
        let ats2: Vec<_> = events2.iter().map(|e| e.at).collect();
        assert_ne!(
            ats1, ats2,
            "different seeds must produce different sequences"
        );
    }

    #[test]
    fn network_partition_uses_pool_as_partition_from() {
        use crate::config::RandomActionConfig;

        let config = crate::config::RandomChaosConfig {
            fault_rate: 10.0,
            duration: Duration::from_secs(5),
            max_concurrent_faults: 100,
            min_interval: Duration::from_millis(10),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "network_partition".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["a".into(), "b".into(), "c".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: Some([Duration::from_secs(1), Duration::from_secs(2)]),
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 42).expect("gen");

        assert!(!events.is_empty(), "should generate events");
        for event in &events {
            assert!(
                !event.partition_from.is_empty(),
                "network_partition must have non-empty partition_from"
            );
            assert!(
                !event
                    .partition_from
                    .contains(&event.target.clone().unwrap_or_default()),
                "partition_from must not contain the target itself"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unified_timeline_interleaved() {
        let (restart_tx, _) = create_restart_channel();

        let fixed_at = Duration::from_secs(5);
        let fixed = vec![ChaosEventDef {
            at: Some(fixed_at),
            every: None,
            action: ChaosAction::Kill,
            target: Some("fixed-target".into()),
            instance: None,
            restart_after: None,
            duration: None,
            delay_ms: None,
            partition_from: Vec::new(),
            heal_after: None,
            loss_percent: None,
            bandwidth: None,
            jitter_ms: None,
            pool: Vec::new(),
        }];

        let random_config = crate::config::RandomChaosConfig {
            fault_rate: 1.0,
            duration: Duration::from_secs(10),
            max_concurrent_faults: 100,
            min_interval: Duration::from_millis(100),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "kill".into(),
                    crate::config::RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["random-target".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: None,
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let scheduler = ChaosScheduler::new_unified(&fixed, Some(&random_config), 42, restart_tx)
            .expect("unified");

        let times: Vec<Duration> = scheduler.events.iter().map(|e| e.next_fire).collect();
        let mut sorted = times.clone();
        sorted.sort();
        assert_eq!(times, sorted, "events must be in chronological order");

        let has_fixed = scheduler
            .events
            .iter()
            .any(|e| e.def.target.as_deref() == Some("fixed-target"));
        let has_random = scheduler
            .events
            .iter()
            .any(|e| e.def.target.as_deref() == Some("random-target"));
        assert!(has_fixed, "should contain the fixed event");
        assert!(has_random, "should contain random events");

        assert!(scheduler.has_random, "has_random flag should be set");
        assert_eq!(scheduler.seed, Some(42), "seed should be stored");
    }

    #[test]
    fn network_partition_skips_single_node_pool() {
        use crate::config::RandomActionConfig;

        let config = crate::config::RandomChaosConfig {
            fault_rate: 5.0,
            duration: Duration::from_secs(5),
            max_concurrent_faults: 100,
            min_interval: Duration::from_millis(10),
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "network_partition".into(),
                    RandomActionConfig {
                        probability: 1.0,
                        pool: vec!["only-node".into()],
                        restart_after: None,
                        delay_ms: None,
                        heal_after: Some([Duration::from_secs(1), Duration::from_secs(2)]),
                        pause_duration: None,
                    },
                );
                m
            },
        };

        let events = super::generate_random_events(&config, 42).expect("gen");
        assert!(
            events.is_empty(),
            "single-node pool should produce no network_partition events"
        );
    }
}
