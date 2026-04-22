#![cfg(feature = "fjall-backend")]

mod common;

use std::time::Duration;

use common::TestPayload;
use mitiflow::store::FjallBackend;
use mitiflow::store::backend::{EventMetadata, HlcTimestamp, StorageBackend};
use mitiflow::subscriber::replay::EventReplayer;
use mitiflow::types::{EventId, PublisherId};
use mitiflow::{
    Event, EventBusConfig, EventPublisher, EventStore, HeartbeatMode, ReplayEnd, ReplayPosition,
};

fn temp_dir(name: &str) -> tempfile::TempDir {
    common::temp_dir(name)
}

fn replay_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(100)
        .heartbeat(HeartbeatMode::Disabled)
        .history_on_subscribe(false)
        .watermark_interval(Duration::from_millis(50))
        .num_partitions(1)
        .build()
        .unwrap()
}

/// Open a Zenoh session with timestamping enabled so that published samples
/// carry HLC timestamps, which are required for the replay index.
async fn open_session() -> zenoh::Session {
    let mut cfg = zenoh::Config::default();
    cfg.insert_json5("timestamping/enabled", "true").unwrap();
    zenoh::open(cfg).await.unwrap()
}

async fn publish_and_wait(publisher: &EventPublisher, count: u64) {
    for i in 0..count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_all_events_bounded() {
    let session = open_session().await;
    let dir = temp_dir("replay_all");
    let config = replay_config("replay_all");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    publish_and_wait(&publisher, 5).await;

    let mut replayer = EventReplayer::builder(&session, config.clone())
        .partition(0)
        .start(ReplayPosition::Earliest)
        .end(ReplayEnd::Bounded { limit: 100 })
        .batch_size(10)
        .build()
        .await
        .unwrap();

    let mut received = Vec::new();
    loop {
        match replayer.recv::<TestPayload>().await {
            Ok(event) => received.push(event),
            Err(mitiflow::Error::EndOfReplay) => break,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    assert_eq!(received.len(), 5, "should replay all 5 events");
    for (i, event) in received.iter().enumerate() {
        assert_eq!(event.payload.value, i as u64);
    }

    replayer.shutdown().await;
    drop(publisher);
    store.shutdown();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_after_hlc_skips_earlier() {
    let session = open_session().await;
    let dir = temp_dir("replay_after_hlc");
    let config = replay_config("replay_after_hlc");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    publish_and_wait(&publisher, 10).await;

    let mut replayer = EventReplayer::builder(&session, config.clone())
        .partition(0)
        .batch_size(10)
        .build()
        .await
        .unwrap();

    let batch = replayer.poll().await.unwrap();
    assert!(batch.len() >= 5, "should have at least 5 events");
    let hlc_at_5 = batch[4].metadata.hlc_timestamp.unwrap();

    let mut replayer2 = EventReplayer::builder(&session, config.clone())
        .partition(0)
        .start(ReplayPosition::AfterHlc(hlc_at_5))
        .end(ReplayEnd::Bounded { limit: 100 })
        .batch_size(10)
        .build()
        .await
        .unwrap();

    let mut received = Vec::new();
    loop {
        match replayer2.recv::<TestPayload>().await {
            Ok(event) => received.push(event),
            Err(mitiflow::Error::EndOfReplay) => break,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    assert_eq!(received.len(), 5);
    assert_eq!(received[0].payload.value, 5);
    assert_eq!(received[4].payload.value, 9);

    replayer.shutdown().await;
    replayer2.shutdown().await;
    drop(publisher);
    store.shutdown();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_bounded_limit() {
    let session = open_session().await;
    let dir = temp_dir("replay_bounded");
    let config = replay_config("replay_bounded");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    publish_and_wait(&publisher, 20).await;

    let mut replayer = EventReplayer::builder(&session, config.clone())
        .partition(0)
        .end(ReplayEnd::Bounded { limit: 7 })
        .batch_size(3)
        .build()
        .await
        .unwrap();

    let mut count = 0;
    loop {
        match replayer.recv::<TestPayload>().await {
            Ok(_) => count += 1,
            Err(mitiflow::Error::EndOfReplay) => break,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    assert_eq!(count, 7, "should stop after exactly 7 events");

    replayer.shutdown().await;
    drop(publisher);
    store.shutdown();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_commit_and_resume() {
    let session = open_session().await;
    let dir = temp_dir("replay_commit");
    let config = replay_config("replay_commit");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    publish_and_wait(&publisher, 10).await;

    let mut replayer = EventReplayer::builder(&session, config.clone())
        .partition(0)
        .end(ReplayEnd::Bounded { limit: 5 })
        .build()
        .await
        .unwrap();

    let mut first_batch = Vec::new();
    loop {
        match replayer.recv::<TestPayload>().await {
            Ok(event) => first_batch.push(event),
            Err(mitiflow::Error::EndOfReplay) => break,
            Err(e) => panic!("unexpected: {e}"),
        }
    }
    assert_eq!(first_batch.len(), 5);
    replayer.commit("test-group").await.unwrap();
    let committed_cursor = *replayer.cursor();
    replayer.shutdown().await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut replayer2 = EventReplayer::builder(&session, config.clone())
        .partition(0)
        .start(ReplayPosition::CommittedOffset {
            group_id: "test-group".into(),
        })
        .end(ReplayEnd::Bounded { limit: 100 })
        .build()
        .await
        .unwrap();

    assert_eq!(*replayer2.cursor(), committed_cursor);

    let mut second_batch = Vec::new();
    loop {
        match replayer2.recv::<TestPayload>().await {
            Ok(event) => second_batch.push(event),
            Err(mitiflow::Error::EndOfReplay) => break,
            Err(e) => panic!("unexpected: {e}"),
        }
    }

    assert_eq!(second_batch.len(), 5);
    assert_eq!(second_batch[0].payload.value, 5);

    replayer2.shutdown().await;
    drop(publisher);
    store.shutdown();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_local_backend() {
    use std::sync::Arc;

    let session = open_session().await;
    let dir = temp_dir("replay_local");
    let config = replay_config("replay_local");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let pub_id = PublisherId::new();

    for seq in 0..5u64 {
        let hlc = HlcTimestamp {
            physical_ns: 1000 + seq,
            logical: 0,
        };
        let payload = config.codec.encode(&TestPayload { value: seq }).unwrap();
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: chrono::Utc::now(),
            key_expr: format!("test/replay_local/p/0/k/item/{seq}"),
            key: Some("item".to_string()),
            hlc_timestamp: Some(hlc),
        };
        backend.store(&meta.key_expr.clone(), &payload, meta).unwrap();
    }

    let backend_arc: Arc<dyn StorageBackend> = Arc::new(backend);

    let mut replayer = EventReplayer::builder(&session, config)
        .partition(0)
        .local_backend(backend_arc)
        .end(ReplayEnd::Bounded { limit: 100 })
        .build()
        .await
        .unwrap();

    let mut received = Vec::new();
    loop {
        match replayer.recv::<TestPayload>().await {
            Ok(event) => received.push(event),
            Err(mitiflow::Error::EndOfReplay) => break,
            Err(e) => panic!("unexpected: {e}"),
        }
    }

    assert_eq!(received.len(), 5);
    for (i, event) in received.iter().enumerate() {
        assert_eq!(event.payload.value, i as u64);
    }

    replayer.shutdown().await;
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_then_live_seamless() {
    let session = open_session().await;
    let dir = temp_dir("replay_then_live");
    let config = replay_config("replay_then_live");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    publish_and_wait(&publisher, 5).await;

    let mut replayer = EventReplayer::builder(&session, config.clone())
        .partition(0)
        .end(ReplayEnd::ThenLive)
        .build()
        .await
        .unwrap();

    let mut received = Vec::new();
    for _ in 0..5 {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), replayer.recv())
                .await
                .expect("timed out")
                .expect("recv failed");
        received.push(event);
    }
    assert_eq!(received.len(), 5);

    for i in 5..8u64 {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    for expected_val in 5..8u64 {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), replayer.recv())
                .await
                .expect("timed out waiting for live event")
                .expect("recv failed");
        assert_eq!(event.payload.value, expected_val);
    }

    replayer.shutdown().await;
    drop(publisher);
    store.shutdown();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_into_live() {
    let session = open_session().await;
    let dir = temp_dir("replay_into_live");
    let config = replay_config("replay_into_live");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    publish_and_wait(&publisher, 5).await;

    let mut replayer = EventReplayer::builder(&session, config.clone())
        .partition(0)
        .end(ReplayEnd::Bounded { limit: 100 })
        .build()
        .await
        .unwrap();

    loop {
        match replayer.recv::<TestPayload>().await {
            Ok(_) => {}
            Err(mitiflow::Error::EndOfReplay) => break,
            Err(e) => panic!("unexpected: {e}"),
        }
    }

    let live_sub = replayer.into_live(&session).await.unwrap();

    for i in 5..8u64 {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    for expected_val in 5..8u64 {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), live_sub.recv())
                .await
                .expect("timed out")
                .expect("recv failed");
        assert_eq!(event.payload.value, expected_val);
    }

    live_sub.shutdown().await;
    drop(publisher);
    store.shutdown();
    session.close().await.unwrap();
}
