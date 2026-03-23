//! Chaos scheduler — timed fault injection against running components.

use std::time::{Duration, Instant};

use rand::Rng;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::backend::ComponentHandle;
use crate::config::{ChaosAction, ChaosEventDef};

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
}

impl ChaosScheduler {
    /// Build a scheduler from chaos event definitions.
    pub fn new(defs: &[ChaosEventDef]) -> Self {
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
        Self { events }
    }

    /// Run the chaos schedule.
    ///
    /// `lookup` resolves a component name + optional instance to a handle.
    /// The scheduler runs until `cancel` is triggered.
    pub async fn run<F>(
        &mut self,
        cancel: CancellationToken,
        start: Instant,
        mut lookup: F,
    ) where
        F: FnMut(&str, Option<usize>) -> Option<HandleRef>,
    {
        loop {
            // Find the next event to fire.
            let next = self.events.iter().enumerate().filter(|(_, e)| {
                if e.fired && e.def.every.is_none() {
                    return false; // One-shot already fired.
                }
                true
            }).min_by_key(|(_, e)| e.next_fire);

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
                event.next_fire = event.next_fire + interval;
            }
        }
    }

    async fn execute_action<F>(&self, def: &ChaosEventDef, lookup: &mut F)
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
                            let target = target.clone();
                            let _instance = def.instance;
                            tokio::spawn(async move {
                                tokio::time::sleep(restart_delay).await;
                                info!("Chaos: restarting {} after kill", target);
                                // Note: restart requires mutable handle, handled by supervisor
                            });
                        }
                    } else {
                        warn!("Chaos: target not found: {}:{:?}", target, def.instance);
                    }
                }
            }

            ChaosAction::Pause => {
                if let Some(target) = &def.target {
                    if let Some(handle) = lookup(target, def.instance) {
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
                                    let _ = h.resume().await;
                                });
                            }
                        }
                    }
                }
            }

            ChaosAction::Restart => {
                if let Some(target) = &def.target {
                    if let Some(handle) = lookup(target, def.instance) {
                        if let Err(e) = handle.stop().await {
                            warn!("Chaos restart stop failed for {}: {}", target, e);
                        }
                        // The supervisor should detect the exit and handle restart.
                    }
                }
            }

            ChaosAction::Slow => {
                // Network slowdown is not implementable via process signals alone.
                // Log a warning — this would require tc/netem for processes or
                // Docker network manipulation for containers.
                warn!(
                    "Chaos: 'slow' action not yet implemented (requires tc/netem). \
                     target={:?}, delay_ms={:?}",
                    def.target, def.delay_ms
                );
            }

            ChaosAction::KillRandom => {
                if !def.pool.is_empty() {
                    let mut rng = rand::thread_rng();
                    let idx = rng.gen_range(0..def.pool.len());
                    let target = &def.pool[idx];
                    info!("Chaos: kill_random selected {}", target);

                    if let Some(handle) = lookup(target, None) {
                        if let Err(e) = handle.kill().await {
                            error!("Chaos kill_random failed for {}: {}", target, e);
                        }
                    }
                }
            }
        }
    }
}

/// A reference to a component handle that the chaos scheduler can operate on.
/// This is a thin wrapper to avoid lifetime issues with the handle lookup.
pub type HandleRef = &'static dyn ComponentHandle;
