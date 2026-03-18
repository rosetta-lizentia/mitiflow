//! Dead Letter Queue (DLQ) for poison message isolation.
//!
//! When an event cannot be processed after a configurable number of retries,
//! it is routed to a DLQ topic for out-of-band inspection and reprocessing.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use zenoh::Session;

use crate::error::Result;
use crate::event::RawEvent;
use crate::types::EventId;

/// Backoff strategy for retry delays between processing attempts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// Fixed delay between retries.
    Fixed(Duration),
    /// Exponential backoff: delay doubles each time, capped at `max`.
    Exponential {
        /// Initial delay for the first retry.
        base: Duration,
        /// Maximum delay cap.
        max: Duration,
    },
}

impl BackoffStrategy {
    /// Compute the delay for the given attempt number (0-indexed).
    pub fn delay_for(&self, attempt: u32) -> Duration {
        match self {
            BackoffStrategy::Fixed(d) => *d,
            BackoffStrategy::Exponential { base, max } => {
                let multiplier = 2u64.saturating_pow(attempt);
                let delay = base.saturating_mul(multiplier as u32);
                if delay > *max { *max } else { delay }
            }
        }
    }
}

/// Configuration for the Dead Letter Queue.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Maximum number of processing attempts before routing to DLQ.
    pub max_retries: u32,
    /// Key prefix for the DLQ topic. Events are published to `{dlq_key_prefix}/{event_id}`.
    pub dlq_key_prefix: String,
    /// Backoff strategy between retries.
    pub retry_backoff: BackoffStrategy,
}

impl DlqConfig {
    /// Create a DLQ config with default backoff (exponential 100ms base, 30s max).
    pub fn new(dlq_key_prefix: impl Into<String>, max_retries: u32) -> Self {
        Self {
            max_retries,
            dlq_key_prefix: dlq_key_prefix.into(),
            retry_backoff: BackoffStrategy::Exponential {
                base: Duration::from_millis(100),
                max: Duration::from_secs(30),
            },
        }
    }
}

/// Tracks retry counts and routes exhausted events to a DLQ topic.
pub struct DeadLetterQueue {
    /// Configuration.
    config: DlqConfig,
    /// Per-event retry counters: `event_id → attempt_count`.
    retries: HashMap<EventId, u32>,
    /// Zenoh session for publishing to the DLQ topic.
    session: Session,
}

/// Outcome of a DLQ retry check.
#[derive(Debug, PartialEq, Eq)]
pub enum RetryOutcome {
    /// The event should be retried after the given delay.
    Retry { attempt: u32, delay: Duration },
    /// The event has exhausted its retries and was sent to the DLQ.
    SentToDlq { attempts: u32 },
}

impl DeadLetterQueue {
    /// Create a new DLQ instance.
    pub fn new(session: &Session, config: DlqConfig) -> Self {
        Self {
            config,
            retries: HashMap::new(),
            session: session.clone(),
        }
    }

    /// Record a failed processing attempt for an event.
    ///
    /// Returns whether the event should be retried or was sent to the DLQ.
    /// If the event has exceeded `max_retries`, it is published to the DLQ
    /// topic and the retry counter is cleared.
    pub async fn on_failure(&mut self, event: &RawEvent) -> Result<RetryOutcome> {
        let attempt = self.retries.entry(event.id).or_insert(0);
        *attempt += 1;

        if *attempt >= self.config.max_retries {
            let attempts = *attempt;
            self.send_to_dlq(event).await?;
            self.retries.remove(&event.id);
            Ok(RetryOutcome::SentToDlq { attempts })
        } else {
            let current_attempt = *attempt;
            let delay = self.config.retry_backoff.delay_for(current_attempt - 1);
            Ok(RetryOutcome::Retry {
                attempt: current_attempt,
                delay,
            })
        }
    }

    /// Check how many attempts have been recorded for an event.
    pub fn attempt_count(&self, event_id: &EventId) -> u32 {
        self.retries.get(event_id).copied().unwrap_or(0)
    }

    /// Clear the retry counter for an event (e.g., after successful processing).
    pub fn ack(&mut self, event_id: &EventId) {
        self.retries.remove(event_id);
    }

    /// Publish the event payload to the DLQ topic.
    async fn send_to_dlq(&self, event: &RawEvent) -> Result<()> {
        let dlq_key = format!("{}/{}", self.config.dlq_key_prefix, event.id);
        warn!(
            event_id = %event.id,
            seq = event.seq,
            publisher = %event.publisher_id,
            dlq_key = %dlq_key,
            "routing event to DLQ after max retries"
        );

        self.session.put(&dlq_key, event.payload.clone()).await?;

        debug!(event_id = %event.id, "event published to DLQ");
        Ok(())
    }

    /// Number of events currently being tracked for retries.
    pub fn pending_count(&self) -> usize {
        self.retries.len()
    }

    /// Configuration reference.
    pub fn config(&self) -> &DlqConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_backoff() {
        let strategy = BackoffStrategy::Fixed(Duration::from_secs(1));
        assert_eq!(strategy.delay_for(0), Duration::from_secs(1));
        assert_eq!(strategy.delay_for(5), Duration::from_secs(1));
        assert_eq!(strategy.delay_for(100), Duration::from_secs(1));
    }

    #[test]
    fn exponential_backoff() {
        let strategy = BackoffStrategy::Exponential {
            base: Duration::from_millis(100),
            max: Duration::from_secs(10),
        };
        assert_eq!(strategy.delay_for(0), Duration::from_millis(100)); // 100 * 2^0
        assert_eq!(strategy.delay_for(1), Duration::from_millis(200)); // 100 * 2^1
        assert_eq!(strategy.delay_for(2), Duration::from_millis(400)); // 100 * 2^2
        assert_eq!(strategy.delay_for(3), Duration::from_millis(800)); // 100 * 2^3
    }

    #[test]
    fn exponential_backoff_capped() {
        let strategy = BackoffStrategy::Exponential {
            base: Duration::from_secs(1),
            max: Duration::from_secs(10),
        };
        // 1 * 2^4 = 16s → capped to 10s
        assert_eq!(strategy.delay_for(4), Duration::from_secs(10));
        assert_eq!(strategy.delay_for(20), Duration::from_secs(10));
    }

    #[test]
    fn dlq_config_defaults() {
        let cfg = DlqConfig::new("myapp/dlq", 3);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.dlq_key_prefix, "myapp/dlq");
        match cfg.retry_backoff {
            BackoffStrategy::Exponential { base, max } => {
                assert_eq!(base, Duration::from_millis(100));
                assert_eq!(max, Duration::from_secs(30));
            }
            _ => panic!("expected exponential backoff"),
        }
    }
}
