//! Publisher lifecycle state machine.
//!
//! Manages the lifecycle of publisher state in the store to prevent unbounded
//! growth of watermark entries and gap tracker memory.
//!
//! Uses multi-signal liveness detection:
//! - Zenoh liveliness tokens provide fast death hints
//! - Event/heartbeat arrival provides ground-truth activity signals
//! - Inactivity timeout prevents false eviction on network partition
//!
//! See [08_replay_ordering.md](../../docs/08_replay_ordering.md) § Publisher Lifecycle.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::types::PublisherId;

/// Publisher lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublisherState {
    /// Publisher is live (liveliness token present or events arriving).
    Active,
    /// Liveliness token revoked but inactivity timeout not yet elapsed.
    /// Publisher may be behind a temporary network partition.
    Suspected,
    /// Inactivity timeout elapsed while suspected. Publisher is considered dead.
    /// Grace period allows in-flight events to land.
    Draining,
    /// Drain grace period expired. State frozen, removed from watermark.
    Archived,
}

/// Per-publisher liveness tracking.
#[derive(Debug)]
pub struct PublisherLiveness {
    pub state: PublisherState,
    /// Last time an event or heartbeat was received from this publisher.
    pub last_activity: Instant,
    /// Whether the Zenoh liveliness token is currently present.
    pub liveliness_live: bool,
    /// When SUSPECTED transitions to DRAINING (if no activity arrives).
    pub suspicion_deadline: Option<Instant>,
    /// When DRAINING transitions to ARCHIVED (if no activity arrives).
    pub drain_deadline: Option<Instant>,
}

impl PublisherLiveness {
    fn new_active() -> Self {
        Self {
            state: PublisherState::Active,
            last_activity: Instant::now(),
            liveliness_live: true,
            suspicion_deadline: None,
            drain_deadline: None,
        }
    }
}

/// Configuration for the lifecycle state machine.
#[derive(Debug, Clone)]
pub struct LifecycleConfig {
    /// How long to wait after liveliness loss before declaring DRAINING.
    pub inactivity_timeout: Duration,
    /// How long DRAINING lasts before ARCHIVED.
    pub drain_grace_period: Duration,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            inactivity_timeout: Duration::from_secs(300), // 5 minutes
            drain_grace_period: Duration::from_secs(120), // 2 minutes
        }
    }
}

/// Manages publisher lifecycle states and epoch tracking.
///
/// The epoch counter increments on every state transition, allowing consumers
/// to detect changes to the publisher set.
pub struct LifecycleManager {
    publishers: HashMap<PublisherId, PublisherLiveness>,
    config: LifecycleConfig,
    epoch: u64,
}

impl LifecycleManager {
    pub fn new(config: LifecycleConfig) -> Self {
        Self {
            publishers: HashMap::new(),
            config,
            epoch: 0,
        }
    }

    /// Current epoch (increments on each state transition).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Record activity from a publisher (event or heartbeat received).
    ///
    /// Creates the publisher entry if it doesn't exist.
    /// Cancels any suspicion/draining state — the publisher is clearly alive.
    pub fn on_activity(&mut self, pub_id: PublisherId) {
        let entry = self
            .publishers
            .entry(pub_id)
            .or_insert_with(|| {
                self.epoch += 1;
                PublisherLiveness::new_active()
            });

        entry.last_activity = Instant::now();

        match entry.state {
            PublisherState::Suspected | PublisherState::Draining => {
                entry.state = PublisherState::Active;
                entry.suspicion_deadline = None;
                entry.drain_deadline = None;
                self.epoch += 1;
            }
            PublisherState::Archived => {
                // Publisher recovered after being archived (very long partition).
                entry.state = PublisherState::Active;
                entry.suspicion_deadline = None;
                entry.drain_deadline = None;
                self.epoch += 1;
                tracing::warn!(
                    %pub_id,
                    "publisher recovered from ARCHIVED state — re-activating"
                );
            }
            PublisherState::Active => {}
        }
    }

    /// Handle liveliness token revocation for a publisher.
    pub fn on_liveliness_revoked(&mut self, pub_id: PublisherId) {
        if let Some(entry) = self.publishers.get_mut(&pub_id) {
            entry.liveliness_live = false;
            if entry.state == PublisherState::Active {
                entry.state = PublisherState::Suspected;
                entry.suspicion_deadline =
                    Some(Instant::now() + self.config.inactivity_timeout);
                self.epoch += 1;
            }
        }
    }

    /// Handle liveliness token restoration for a publisher.
    pub fn on_liveliness_restored(&mut self, pub_id: PublisherId) {
        if let Some(entry) = self.publishers.get_mut(&pub_id) {
            entry.liveliness_live = true;
            if entry.state == PublisherState::Suspected {
                entry.state = PublisherState::Active;
                entry.suspicion_deadline = None;
                self.epoch += 1;
            }
        }
    }

    /// Advance the state machine based on elapsed time.
    ///
    /// Call this periodically (e.g., on each watermark tick).
    /// Returns the list of publishers that transitioned to ARCHIVED.
    pub fn tick(&mut self) -> Vec<PublisherId> {
        let now = Instant::now();
        let mut newly_archived = Vec::new();

        for (pub_id, entry) in &mut self.publishers {
            match entry.state {
                PublisherState::Active => {
                    // If liveliness is gone AND no recent activity, transition to SUSPECTED.
                    if !entry.liveliness_live
                        && now.duration_since(entry.last_activity) > self.config.inactivity_timeout
                    {
                        entry.state = PublisherState::Draining;
                        entry.drain_deadline =
                            Some(now + self.config.drain_grace_period);
                        self.epoch += 1;
                    }
                }
                PublisherState::Suspected => {
                    if let Some(deadline) = entry.suspicion_deadline {
                        if now >= deadline {
                            entry.state = PublisherState::Draining;
                            entry.drain_deadline =
                                Some(now + self.config.drain_grace_period);
                            entry.suspicion_deadline = None;
                            self.epoch += 1;
                        }
                    }
                }
                PublisherState::Draining => {
                    if let Some(deadline) = entry.drain_deadline {
                        if now >= deadline {
                            entry.state = PublisherState::Archived;
                            entry.drain_deadline = None;
                            self.epoch += 1;
                            newly_archived.push(*pub_id);
                        }
                    }
                }
                PublisherState::Archived => {}
            }
        }

        newly_archived
    }

    /// Returns the set of publisher IDs that should be included in watermark
    /// broadcasts (ACTIVE, SUSPECTED, or DRAINING).
    pub fn active_publishers(&self) -> Vec<PublisherId> {
        self.publishers
            .iter()
            .filter(|(_, entry)| {
                matches!(
                    entry.state,
                    PublisherState::Active
                        | PublisherState::Suspected
                        | PublisherState::Draining
                )
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Check whether a publisher is in a state where it should be tracked
    /// in the watermark.
    pub fn is_watermark_active(&self, pub_id: &PublisherId) -> bool {
        self.publishers
            .get(pub_id)
            .is_some_and(|e| {
                matches!(
                    e.state,
                    PublisherState::Active
                        | PublisherState::Suspected
                        | PublisherState::Draining
                )
            })
    }

    /// Get the state of a specific publisher.
    pub fn state(&self, pub_id: &PublisherId) -> Option<PublisherState> {
        self.publishers.get(pub_id).map(|e| e.state)
    }

    /// Remove all ARCHIVED publishers from tracking.
    /// Called by GC to free memory.
    pub fn gc_archived(&mut self) -> usize {
        let before = self.publishers.len();
        self.publishers
            .retain(|_, entry| entry.state != PublisherState::Archived);
        before - self.publishers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_config() -> LifecycleConfig {
        LifecycleConfig {
            inactivity_timeout: Duration::from_millis(50),
            drain_grace_period: Duration::from_millis(30),
        }
    }

    #[test]
    fn new_publisher_on_activity() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        assert_eq!(mgr.epoch(), 0);
        mgr.on_activity(pub_id);
        assert_eq!(mgr.epoch(), 1);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Active));
        assert!(mgr.is_watermark_active(&pub_id));
    }

    #[test]
    fn liveliness_revoked_transitions_to_suspected() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        mgr.on_activity(pub_id);
        mgr.on_liveliness_revoked(pub_id);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Suspected));
        // Still included in watermark
        assert!(mgr.is_watermark_active(&pub_id));
    }

    #[test]
    fn activity_cancels_suspicion() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        mgr.on_activity(pub_id);
        mgr.on_liveliness_revoked(pub_id);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Suspected));

        // Event arrives — back to ACTIVE
        mgr.on_activity(pub_id);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Active));
    }

    #[test]
    fn liveliness_restored_cancels_suspicion() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        mgr.on_activity(pub_id);
        mgr.on_liveliness_revoked(pub_id);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Suspected));

        mgr.on_liveliness_restored(pub_id);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Active));
    }

    #[test]
    fn suspected_to_draining_on_timeout() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        mgr.on_activity(pub_id);
        mgr.on_liveliness_revoked(pub_id);

        // Wait for inactivity timeout
        std::thread::sleep(Duration::from_millis(60));
        mgr.tick();
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Draining));
        // Still in watermark during draining
        assert!(mgr.is_watermark_active(&pub_id));
    }

    #[test]
    fn draining_to_archived_on_grace_expiry() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        mgr.on_activity(pub_id);
        mgr.on_liveliness_revoked(pub_id);

        // Wait for inactivity timeout
        std::thread::sleep(Duration::from_millis(60));
        mgr.tick();
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Draining));

        // Wait for drain grace period
        std::thread::sleep(Duration::from_millis(40));
        let archived = mgr.tick();
        assert_eq!(archived, vec![pub_id]);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Archived));
        // No longer in watermark
        assert!(!mgr.is_watermark_active(&pub_id));
    }

    #[test]
    fn activity_during_draining_reactivates() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        mgr.on_activity(pub_id);
        mgr.on_liveliness_revoked(pub_id);

        std::thread::sleep(Duration::from_millis(60));
        mgr.tick();
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Draining));

        // Late event arrives
        mgr.on_activity(pub_id);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Active));
    }

    #[test]
    fn archived_publisher_reactivates_on_event() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        mgr.on_activity(pub_id);
        mgr.on_liveliness_revoked(pub_id);

        // Fast-forward through suspected → draining → archived
        std::thread::sleep(Duration::from_millis(60));
        mgr.tick();
        std::thread::sleep(Duration::from_millis(40));
        mgr.tick();
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Archived));

        // Publisher comes back after long partition
        mgr.on_activity(pub_id);
        assert_eq!(mgr.state(&pub_id), Some(PublisherState::Active));
    }

    #[test]
    fn gc_removes_archived() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_a = PublisherId::new();
        let pub_b = PublisherId::new();

        mgr.on_activity(pub_a);
        mgr.on_activity(pub_b);

        // Archive pub_a
        mgr.on_liveliness_revoked(pub_a);
        std::thread::sleep(Duration::from_millis(60));
        mgr.tick();
        std::thread::sleep(Duration::from_millis(40));
        mgr.tick();

        assert_eq!(mgr.state(&pub_a), Some(PublisherState::Archived));
        assert_eq!(mgr.state(&pub_b), Some(PublisherState::Active));

        let removed = mgr.gc_archived();
        assert_eq!(removed, 1);
        assert_eq!(mgr.state(&pub_a), None);
        assert_eq!(mgr.state(&pub_b), Some(PublisherState::Active));
    }

    #[test]
    fn epoch_increments_on_transitions() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_id = PublisherId::new();

        let e0 = mgr.epoch();
        mgr.on_activity(pub_id); // +1 (new publisher)
        let e1 = mgr.epoch();
        mgr.on_liveliness_revoked(pub_id); // +1 (ACTIVE → SUSPECTED)
        let e2 = mgr.epoch();
        mgr.on_activity(pub_id); // +1 (SUSPECTED → ACTIVE)
        let e3 = mgr.epoch();

        assert!(e1 > e0);
        assert!(e2 > e1);
        assert!(e3 > e2);
    }

    #[test]
    fn active_publishers_filters_correctly() {
        let mut mgr = LifecycleManager::new(fast_config());
        let pub_a = PublisherId::new();
        let pub_b = PublisherId::new();
        let pub_c = PublisherId::new();

        mgr.on_activity(pub_a);
        mgr.on_activity(pub_b);
        mgr.on_activity(pub_c);

        // Archive pub_c
        mgr.on_liveliness_revoked(pub_c);
        std::thread::sleep(Duration::from_millis(60));
        mgr.tick();
        std::thread::sleep(Duration::from_millis(40));
        mgr.tick();

        let active = mgr.active_publishers();
        assert!(active.contains(&pub_a));
        assert!(active.contains(&pub_b));
        assert!(!active.contains(&pub_c));
    }
}
