//! Dead letter queue (DLQ) example — retry logic and poison message isolation.
//!
//! This example demonstrates:
//!   1. Processing events from a mitiflow subscriber with simulated failures.
//!   2. Retrying failed events with exponential backoff via `DeadLetterQueue`.
//!   3. Routing events that exhaust their retry budget to a DLQ topic.
//!   4. Monitoring the DLQ topic with a separate Zenoh subscriber to inspect
//!      poison messages out-of-band.
//!
//! Three event behaviors are exercised:
//!   - Normal:   processed successfully on the first attempt.
//!   - Flaky:    fails N times before succeeding (transient errors).
//!   - Poison:   always fails; eventually routed to the DLQ.
//!
//! Run with: `cargo run -p mitiflow --example dead_letter_queue --no-default-features`

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{
    BackoffStrategy, DeadLetterQueue, DlqConfig, Event, EventBusConfig, EventPublisher,
    EventSubscriber, HeartbeatMode, RetryOutcome,
};

// ---------------------------------------------------------------------------
// Application domain types
// ---------------------------------------------------------------------------

/// Whether this transaction is expected to succeed, fail transiently, or always fail.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Behavior {
    /// Always succeeds on the first attempt.
    Normal,
    /// Fails `fails_before_success` times (simulates transient errors).
    Flaky { fails_before_success: u32 },
    /// Always fails — a poison message.
    Poison,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Transaction {
    tx_id: String,
    amount_cents: i64,
    behavior: Behavior,
}

// ---------------------------------------------------------------------------
// Processing logic — returns `true` on success, `false` on failure.
// ---------------------------------------------------------------------------

/// Simulates processing a transaction.
///
/// `attempt` is the 0-based attempt index for this event; used to decide
/// whether a `Flaky` event should succeed yet.
fn try_process(tx: &Transaction, attempt: u32) -> bool {
    match &tx.behavior {
        Behavior::Normal => true,
        Behavior::Flaky {
            fails_before_success,
        } => attempt >= *fails_before_success,
        Behavior::Poison => false,
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("mitiflow=warn,dead_letter_queue=info")
        .init();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    const DLQ_PREFIX: &str = "demo/txn/_dlq";

    let config = EventBusConfig::builder("demo/txn")
        .cache_size(100)
        .heartbeat(HeartbeatMode::Disabled)
        .build()?;

    // -------------------------------------------------------------------------
    // Phase 1: Subscribe to the DLQ topic for out-of-band monitoring.
    //
    // In production this would run in a separate service (an "alert pipeline"
    // or "reprocessing worker").  Here we use a raw Zenoh subscriber so we can
    // observe DLQ messages independently of the main subscriber.
    // -------------------------------------------------------------------------
    let dlq_monitor = session
        .declare_subscriber(format!("{DLQ_PREFIX}/**"))
        .await
        .unwrap();

    // -------------------------------------------------------------------------
    // Phase 2: Set up the main subscriber, publisher, and DLQ.
    //
    // DLQ configuration:
    //   - max_retries = 3  (4th attempt routes to DLQ)
    //   - Exponential backoff: 30 ms → 60 ms → 120 ms, capped at 200 ms
    // -------------------------------------------------------------------------
    let subscriber = EventSubscriber::new(&session, config.clone()).await?;
    let publisher = EventPublisher::new(&session, config).await?;

    let dlq_config = DlqConfig {
        max_retries: 3,
        dlq_key_prefix: DLQ_PREFIX.to_string(),
        retry_backoff: BackoffStrategy::Exponential {
            base: Duration::from_millis(30),
            max: Duration::from_millis(200),
        },
    };
    let mut dlq = DeadLetterQueue::new(&session, dlq_config);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // -------------------------------------------------------------------------
    // Phase 3: Publish a mix of normal, flaky, and poison transactions.
    // -------------------------------------------------------------------------
    let transactions: &[(&str, i64, Behavior)] = &[
        ("TXN-001", 25000, Behavior::Normal),
        ("TXN-002", -500, Behavior::Poison), // negative amount — always invalid
        ("TXN-003", 12000, Behavior::Normal),
        (
            "TXN-004",
            9999,
            Behavior::Flaky {
                fails_before_success: 2,
            },
        ), // fails twice, then ok
        ("TXN-005", 0, Behavior::Normal),
        ("TXN-006", -100, Behavior::Poison),
        ("TXN-007", 30000, Behavior::Normal),
        (
            "TXN-008",
            7550,
            Behavior::Flaky {
                fails_before_success: 5,
            },
        ), // exceeds max_retries → DLQ
        ("TXN-009", 18000, Behavior::Normal),
        ("TXN-010", 5500, Behavior::Normal),
    ];

    println!("Publishing {} transactions...\n", transactions.len());
    for (tx_id, amount_cents, behavior) in transactions {
        let event = Event::new(Transaction {
            tx_id: tx_id.to_string(),
            amount_cents: *amount_cents,
            behavior: behavior.clone(),
        });
        publisher.publish(&event).await?;
    }

    // -------------------------------------------------------------------------
    // Phase 4: Processing loop with retry/DLQ handling.
    //
    // For each event we track the local attempt count independently of the DLQ's
    // internal counter.  `dlq.on_failure(&raw)` increments the DLQ's counter and
    // decides whether to retry or route to DLQ.  `dlq.ack(&event_id)` clears it
    // on success.
    //
    // Calling convention:
    //   1. Receive raw event.
    //   2. Deserialize and attempt processing.
    //   3. On success  → ack, move on.
    //   4. On failure  → call on_failure; match RetryOutcome:
    //        Retry { delay }  → sleep, then re-attempt the SAME raw event.
    //        SentToDlq        → abandon, move on to next event.
    // -------------------------------------------------------------------------
    println!("Processing loop:\n");

    // Track per-event attempt index (0-based) for `try_process` simulation.
    let mut attempt_map: HashMap<mitiflow::EventId, u32> = HashMap::new();

    let total = transactions.len();
    let mut received = 0;

    while received < total {
        // Receive the next raw event.
        let raw = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_raw())
            .await
            .expect("timed out waiting for event")?;

        received += 1;

        // Deserialize once to inspect the payload.
        let event: Event<Transaction> = raw.deserialize()?;
        let tx = &event.payload;
        let seq = raw.seq;

        println!(
            "  ┌─ seq={seq:02}  {}  ${:.2}",
            tx.tx_id,
            tx.amount_cents as f64 / 100.0
        );

        // Inner retry loop for this event.
        loop {
            let attempt = *attempt_map.entry(raw.id).or_insert(0);

            if try_process(tx, attempt) {
                // Success — acknowledge in DLQ (clears retry counter).
                dlq.ack(&raw.id);
                attempt_map.remove(&raw.id);
                println!("  └─ ✓  processed on attempt {attempt}");
                break;
            }

            // Record a failure.
            *attempt_map.entry(raw.id).or_insert(0) += 1;
            let outcome = dlq.on_failure(&raw).await?;

            match outcome {
                RetryOutcome::Retry {
                    attempt: att,
                    delay,
                } => {
                    println!("  │  ↺  attempt {att} failed — retrying in {delay:.0?}");
                    tokio::time::sleep(delay).await;
                    // Loop again with incremented attempt counter.
                }
                RetryOutcome::SentToDlq { attempts } => {
                    println!("  └─ ✗  routed to DLQ after {attempts} attempts");
                    attempt_map.remove(&raw.id);
                    break;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 5: Drain the DLQ monitor to show what landed in the dead letter queue.
    //
    // Poll non-blockingly — some DLQ messages might still be in-flight.
    // -------------------------------------------------------------------------
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\nDLQ monitor:");
    let mut dlq_count = 0;
    while let Ok(Some(sample)) = dlq_monitor.try_recv() {
        dlq_count += 1;
        // The DLQ payload is the raw Event<T> JSON exactly as published.
        if let Ok(event) =
            serde_json::from_slice::<Event<Transaction>>(&sample.payload().to_bytes())
        {
            println!(
                "  [DLQ #{dlq_count}]  key={}  tx_id={}  amount=${:.2}  behavior={:?}",
                sample.key_expr(),
                event.payload.tx_id,
                event.payload.amount_cents as f64 / 100.0,
                event.payload.behavior,
            );
        } else {
            println!("  [DLQ #{dlq_count}]  (could not deserialize)");
        }
    }
    if dlq_count == 0 {
        println!("  (empty — all events processed successfully)");
    }

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------
    println!("\nDLQ pending count (should be 0): {}", dlq.pending_count());

    // Cleanup
    drop(dlq_monitor);
    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();

    println!("Done.");
    Ok(())
}
