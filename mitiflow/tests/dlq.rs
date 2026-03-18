//! Integration tests for the Dead Letter Queue.

use std::time::Duration;

use mitiflow::DeadLetterQueue;
use mitiflow::dlq::{BackoffStrategy, DlqConfig, RetryOutcome};
use mitiflow::event::RawEvent;
use mitiflow::types::{EventId, PublisherId};

fn make_raw_event() -> RawEvent {
    RawEvent {
        id: EventId::new(),
        seq: 0,
        publisher_id: PublisherId::new(),
        key_expr: "test/key".to_string(),
        payload: b"test payload".to_vec(),
        timestamp: chrono::Utc::now(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dlq_retry_then_send() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = DlqConfig::new("test/dlq", 3);
    let mut dlq = DeadLetterQueue::new(&session, config);

    let event = make_raw_event();

    // First failure → retry (attempt 1).
    let outcome = dlq.on_failure(&event).await.unwrap();
    match outcome {
        RetryOutcome::Retry { attempt, .. } => assert_eq!(attempt, 1),
        _ => panic!("expected retry"),
    }

    // Second failure → retry (attempt 2).
    let outcome = dlq.on_failure(&event).await.unwrap();
    match outcome {
        RetryOutcome::Retry { attempt, .. } => assert_eq!(attempt, 2),
        _ => panic!("expected retry"),
    }

    // Third failure → sent to DLQ (max_retries = 3).
    let outcome = dlq.on_failure(&event).await.unwrap();
    assert_eq!(outcome, RetryOutcome::SentToDlq { attempts: 3 });

    // Counter is cleared — next failure starts fresh.
    assert_eq!(dlq.attempt_count(&event.id), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dlq_ack_clears_counter() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = DlqConfig::new("test/dlq_ack", 5);
    let mut dlq = DeadLetterQueue::new(&session, config);

    let event = make_raw_event();

    dlq.on_failure(&event).await.unwrap();
    dlq.on_failure(&event).await.unwrap();
    assert_eq!(dlq.attempt_count(&event.id), 2);

    dlq.ack(&event.id);
    assert_eq!(dlq.attempt_count(&event.id), 0);
    assert_eq!(dlq.pending_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dlq_backoff_delays() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = DlqConfig {
        max_retries: 10,
        dlq_key_prefix: "test/dlq_bo".to_string(),
        retry_backoff: BackoffStrategy::Exponential {
            base: Duration::from_millis(50),
            max: Duration::from_secs(5),
        },
    };
    let mut dlq = DeadLetterQueue::new(&session, config);

    let event = make_raw_event();

    // Attempt 1 → delay = 50ms * 2^0 = 50ms.
    let outcome = dlq.on_failure(&event).await.unwrap();
    match outcome {
        RetryOutcome::Retry { delay, .. } => assert_eq!(delay, Duration::from_millis(50)),
        _ => panic!("expected retry"),
    }

    // Attempt 2 → delay = 50ms * 2^1 = 100ms.
    let outcome = dlq.on_failure(&event).await.unwrap();
    match outcome {
        RetryOutcome::Retry { delay, .. } => assert_eq!(delay, Duration::from_millis(100)),
        _ => panic!("expected retry"),
    }

    // Attempt 3 → delay = 50ms * 2^2 = 200ms.
    let outcome = dlq.on_failure(&event).await.unwrap();
    match outcome {
        RetryOutcome::Retry { delay, .. } => assert_eq!(delay, Duration::from_millis(200)),
        _ => panic!("expected retry"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dlq_routes_to_zenoh_topic() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Subscribe to DLQ topic to verify the event arrives.
    let dlq_sub = session
        .declare_subscriber("test/dlq_route/**")
        .await
        .unwrap();

    let config = DlqConfig::new("test/dlq_route", 1); // max_retries=1 → first failure goes to DLQ.
    let mut dlq = DeadLetterQueue::new(&session, config);

    let event = make_raw_event();
    let outcome = dlq.on_failure(&event).await.unwrap();
    assert_eq!(outcome, RetryOutcome::SentToDlq { attempts: 1 });

    // Verify the event was published to the DLQ topic.
    let sample = tokio::time::timeout(Duration::from_secs(2), dlq_sub.recv_async())
        .await
        .expect("timeout waiting for DLQ event")
        .expect("DLQ subscriber error");

    let received_payload = sample.payload().to_bytes().to_vec();
    assert_eq!(received_payload, event.payload);
}
