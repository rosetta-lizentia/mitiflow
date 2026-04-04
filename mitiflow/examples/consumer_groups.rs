//! Consumer groups with offset commits and rebalancing.
//!
//! This example demonstrates:
//!   1. Creating a `ConsumerGroupSubscriber` that joins a consumer group.
//!   2. Automatic partition assignment via rendezvous (HRW) hashing.
//!   3. Publishing keyed events routed to partitions by user_id.
//!   4. Manual offset commits (`commit_sync`).
//!   5. Auto-commit mode for hands-off offset management.
//!   6. Rebalancing when the `PartitionManager` is used directly.
//!
//!   cargo run -p mitiflow --features full --example consumer_groups

mod inner {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use mitiflow::{
        CommitMode, ConsumerGroupConfig, ConsumerGroupSubscriber, Event, EventBusConfig,
        EventPublisher, EventStore, FjallBackend, HeartbeatMode, OffsetReset, PartitionManager,
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct UserAction {
        user_id: String,
        action: String,
    }

    fn base_config(num_partitions: u32) -> mitiflow::Result<EventBusConfig> {
        EventBusConfig::builder("demo/consumer_group")
            .cache_size(200)
            .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
            .num_partitions(num_partitions)
            .build()
    }

    fn worker_config(worker_id: &str, num_partitions: u32) -> mitiflow::Result<EventBusConfig> {
        EventBusConfig::builder("demo/consumer_group")
            .cache_size(200)
            .heartbeat(HeartbeatMode::Disabled)
            .num_partitions(num_partitions)
            .worker_id(worker_id)
            .build()
    }

    /// Phase 1: ConsumerGroupSubscriber with manual commit.
    async fn phase_consumer_group(session: &zenoh::Session) -> mitiflow::Result<()> {
        println!("=== Phase 1: ConsumerGroupSubscriber with manual commit ===\n");
        const NUM_PARTITIONS: u32 = 4;

        // Start an EventStore so offset commits have somewhere to persist.
        let store_dir = tempfile::tempdir().unwrap();
        let backend =
            FjallBackend::open(store_dir.path(), 0).expect("failed to open fjall backend");
        let store_config = EventBusConfig::builder("demo/consumer_group")
            .num_partitions(NUM_PARTITIONS)
            .build()?;
        let mut store = EventStore::new(session, backend, store_config.clone());
        store.run().await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create a publisher.
        let pub_config = base_config(NUM_PARTITIONS)?;
        let publisher = EventPublisher::new(session, pub_config).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create a consumer group subscriber with manual commits.
        let group_config = ConsumerGroupConfig {
            group_id: "demo-group".into(),
            member_id: "member-1".into(),
            commit_mode: CommitMode::Manual,
            offset_reset: OffsetReset::Earliest,
        };
        let consumer_config = base_config(NUM_PARTITIONS)?;
        let consumer = ConsumerGroupSubscriber::new(session, consumer_config, group_config).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let partitions = consumer.assigned_partitions().await;
        println!(
            "  member-1 assigned {} partitions: {:?}",
            partitions.len(),
            partitions
        );
        println!("  generation: {}", consumer.current_generation());

        // Publish some keyed events.
        let users = ["alice", "bob", "carol", "dave"];
        for user_id in &users {
            let event = Event::new(UserAction {
                user_id: user_id.to_string(),
                action: "login".into(),
            });
            let seq = publisher.publish_keyed(user_id, &event).await?;
            println!("  published: user={user_id}, seq={seq}");
        }

        // Receive events.
        println!("\n  Receiving events...");
        for _ in 0..users.len() {
            let event: Event<UserAction> =
                tokio::time::timeout(Duration::from_secs(5), consumer.recv())
                    .await
                    .expect("timed out")?;
            println!(
                "  received: user={}, action={}, seq={}",
                event.payload.user_id,
                event.payload.action,
                event.seq.unwrap_or(0),
            );
        }

        // Manually commit offsets.
        consumer.commit_sync().await?;
        println!("\n  Offsets committed (manual).");

        consumer.shutdown().await;
        drop(publisher);
        store.shutdown_gracefully().await;
        drop(store_dir);
        Ok(())
    }

    /// Phase 2: Auto-commit mode.
    async fn phase_auto_commit(session: &zenoh::Session) -> mitiflow::Result<()> {
        println!("\n=== Phase 2: Auto-commit mode ===\n");
        const NUM_PARTITIONS: u32 = 4;

        let store_dir = tempfile::tempdir().unwrap();
        let backend =
            FjallBackend::open(store_dir.path(), 0).expect("failed to open fjall backend");
        let store_config = EventBusConfig::builder("demo/consumer_group_auto")
            .num_partitions(NUM_PARTITIONS)
            .build()?;
        let mut store = EventStore::new(session, backend, store_config.clone());
        store.run().await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let pub_config = EventBusConfig::builder("demo/consumer_group_auto")
            .cache_size(200)
            .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
            .num_partitions(NUM_PARTITIONS)
            .build()?;
        let publisher = EventPublisher::new(session, pub_config).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Auto-commit every 1 second.
        let group_config = ConsumerGroupConfig {
            group_id: "auto-group".into(),
            member_id: "auto-member-1".into(),
            commit_mode: CommitMode::Auto {
                interval: Duration::from_secs(1),
            },
            offset_reset: OffsetReset::Earliest,
        };
        let consumer_config = EventBusConfig::builder("demo/consumer_group_auto")
            .num_partitions(NUM_PARTITIONS)
            .build()?;
        let consumer = ConsumerGroupSubscriber::new(session, consumer_config, group_config).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        println!(
            "  Auto-commit interval: 1s, offset_reset: Earliest, partitions: {:?}",
            consumer.assigned_partitions().await
        );

        // Publish and receive.
        let event = Event::new(UserAction {
            user_id: "eve".into(),
            action: "signup".into(),
        });
        publisher.publish_keyed("eve", &event).await?;
        let received: Event<UserAction> =
            tokio::time::timeout(Duration::from_secs(5), consumer.recv())
                .await
                .expect("timed out")?;
        println!(
            "  received: user={}, action={}",
            received.payload.user_id, received.payload.action
        );

        // Wait for auto-commit to fire.
        println!("  waiting for auto-commit...");
        tokio::time::sleep(Duration::from_millis(1500)).await;
        println!("  offsets auto-committed.");

        consumer.shutdown().await;
        drop(publisher);
        store.shutdown_gracefully().await;
        drop(store_dir);
        Ok(())
    }

    /// Phase 3: PartitionManager rebalancing demo.
    async fn phase_rebalancing(session: &zenoh::Session) -> mitiflow::Result<()> {
        println!("\n=== Phase 3: PartitionManager rebalancing ===\n");
        const NUM_PARTITIONS: u32 = 12;

        let worker_a =
            PartitionManager::new(session, worker_config("worker-a", NUM_PARTITIONS)?).await?;
        let worker_b =
            PartitionManager::new(session, worker_config("worker-b", NUM_PARTITIONS)?).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;

        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        println!("  2 workers, {NUM_PARTITIONS} partitions:");
        println!("    worker-a ({} partitions): {:?}", parts_a.len(), parts_a);
        println!("    worker-b ({} partitions): {:?}", parts_b.len(), parts_b);

        // A third worker joins.
        let rebalance_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let log_clone = Arc::clone(&rebalance_log);
        worker_a
            .on_rebalance(move |gained, lost| {
                log_clone
                    .lock()
                    .unwrap()
                    .push(format!("worker-a  gained={gained:?}  lost={lost:?}"));
            })
            .await;

        let worker_c =
            PartitionManager::new(session, worker_config("worker-c", NUM_PARTITIONS)?).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        let parts_c = worker_c.my_partitions().await;
        println!("\n  After worker-c joins (3 workers):");
        println!("    worker-a ({} partitions): {:?}", parts_a.len(), parts_a);
        println!("    worker-b ({} partitions): {:?}", parts_b.len(), parts_b);
        println!("    worker-c ({} partitions): {:?}", parts_c.len(), parts_c);

        println!("\n  Rebalance callbacks:");
        for msg in rebalance_log.lock().unwrap().iter() {
            println!("    {msg}");
        }

        // worker-c leaves.
        println!("\n  worker-c leaves...");
        drop(worker_c);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        println!("    worker-a ({} partitions): {:?}", parts_a.len(), parts_a);
        println!("    worker-b ({} partitions): {:?}", parts_b.len(), parts_b);

        drop(worker_a);
        drop(worker_b);
        Ok(())
    }

    pub async fn run() -> mitiflow::Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter("mitiflow=info,consumer_groups=info")
            .init();

        let session = zenoh::open(zenoh::Config::default()).await.unwrap();

        phase_consumer_group(&session).await?;
        phase_auto_commit(&session).await?;
        phase_rebalancing(&session).await?;

        session.close().await.unwrap();
        println!("\nConsumer group example complete.");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    inner::run().await
}
