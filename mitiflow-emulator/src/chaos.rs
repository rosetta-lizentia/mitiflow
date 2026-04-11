//! Chaos scheduler — timed fault injection against running components.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngExt;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::backend::ComponentHandle;
use crate::config::{ChaosAction, ChaosEventDef};
use crate::network_fault::{FaultGuard, NetworkFaultInjector};
use crate::restart_channel::{RestartRequest, RestartSender};

/// A runtime chaos event ready for scheduling.
struct ScheduledEvent {
    def: ChaosEventDef,
    /// Absolute time of next firing (relative to topology start).
    next_fire: Duration,
    /// Whether this event has already fired (for one-shot events).
    fired: bool,
}

/// Chaos scheduler that executes fault injection events on a timeline.
pub struct ChaosScheduler {
    events: Vec<ScheduledEvent>,
    restart_tx: RestartSender,
    /// Optional network fault injector for Slow/NetworkPartition actions.
    fault_injector: Option<Arc<dyn NetworkFaultInjector>>,
    /// Guards for persistent (non-timed) active faults. Dropped on scheduler drop.
    active_faults: Vec<FaultGuard>,
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
                }
            })
            .collect();
        Self {
            events,
            restart_tx,
            fault_injector: None,
            active_faults: Vec::new(),
        }
    }

    /// Attach a network fault injector (required for `Slow` and `NetworkPartition` actions).
    pub fn with_fault_injector(mut self, injector: Arc<dyn NetworkFaultInjector>) -> Self {
        self.fault_injector = Some(injector);
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

            info!(
                "Chaos: firing {:?} on target={:?} instance={:?}",
                def.action, def.target, def.instance
            );

            self.execute_action(&def, &mut lookup).await;

            // Update scheduling state.
            let event = &mut self.events[idx];
            event.fired = true;
            if let Some(interval) = event.def.every {
                event.next_fire += interval;
            }
        }
    }

    async fn execute_action<F>(&mut self, def: &ChaosEventDef, lookup: &mut F)
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
            }

            ChaosAction::Pause => {
                if let Some(target) = &def.target
                    && let Some(handle) = lookup(target, def.instance)
                {
                    if let Err(e) = handle.pause().await {
                        error!("Chaos pause failed for {}: {}", target, e);
                    }

                    // Resume after duration.
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
            }

            ChaosAction::Restart => {
                if let Some(target) = &def.target
                    && let Some(handle) = lookup(target, def.instance)
                    && let Err(e) = handle.stop().await
                {
                    error!("Chaos restart stop failed for {}: {}", target, e);
                }
                // The supervisor should detect the exit and handle restart.
            }

            ChaosAction::Slow => {
                if let Some(target) = &def.target {
                    let delay_ms = def.delay_ms.unwrap_or(100) as u32;
                    let loss_pct = def.loss_percent.unwrap_or(0.0);
                    let Some(injector) = self.fault_injector.as_ref() else {
                        error!("Slow action requires a fault injector but none configured");
                        return;
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
            }

            ChaosAction::KillRandom => {
                if !def.pool.is_empty() {
                    let target_name = {
                        let mut rng = rand::rng();
                        let idx = rng.random_range(0..def.pool.len());
                        def.pool[idx].clone()
                    };
                    info!("Chaos: kill_random selected {}", target_name);

                    if let Some(handle) = lookup(&target_name, None)
                        && let Err(e) = handle.kill().await
                    {
                        error!("Chaos kill_random failed for {}: {}", target_name, e);
                    }
                }
            }

            ChaosAction::NetworkPartition => {
                if let Some(target) = &def.target {
                    if def.partition_from.is_empty() {
                        error!("NetworkPartition requires non-empty partition_from");
                        return;
                    }
                    let Some(injector) = self.fault_injector.as_ref() else {
                        error!("NetworkPartition requires a fault injector");
                        return;
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
}
