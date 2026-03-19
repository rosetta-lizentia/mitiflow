//! Consumer groups with partitioned subscriptions.
//!
//! This example demonstrates:
//!   1. Creating multiple workers, each with a `PartitionManager`.
//!   2. Stable partition assignment using rendezvous (HRW) hashing.
//!   3. Publishing events to partition-specific key expressions.
//!   4. Rebalancing when new workers join or existing ones leave.
//!   5. Generating partition-filtered Zenoh subscription key expressions.
//!
//! Rendezvous hashing guarantees minimal partition movement on topology changes:
//! only the partitions owned by the leaving/joining worker are reassigned.
//!
//! Requires the `partition` feature:
//!   cargo run -p mitiflow --example consumer_groups --features partition

// When the `partition` feature is disabled, emit a helpful error and exit.
#[cfg(not(feature = "partition"))]
fn main() {
    eprintln!(
        "This example requires the `partition` feature.\n\
         Run with:\n  cargo run -p mitiflow --example consumer_groups --features partition"
    );
}

#[cfg(feature = "partition")]
mod inner {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use mitiflow::{Event, EventBusConfig, EventPublisher, HeartbeatMode, PartitionManager};

    /// An event carrying a user action, routed to a partition by `user_id`.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct UserAction {
        user_id: String,
        action: String,
    }

    /// Build an `EventBusConfig` for the given worker identity.
    fn worker_config(worker_id: &str, num_partitions: u32) -> mitiflow::Result<EventBusConfig> {
        EventBusConfig::builder("demo/user_actions")
            .cache_size(200)
            .heartbeat(HeartbeatMode::Disabled)
            .num_partitions(num_partitions)
            .worker_id(worker_id)
            .build()
    }

    /// Phase 1: Start two workers and print initial partition assignment.
    async fn phase_initial_assignment(
        session: &zenoh::Session,
        num_partitions: u32,
    ) -> mitiflow::Result<(PartitionManager, PartitionManager)> {
        let worker_a =
            PartitionManager::new(session, worker_config("worker-a", num_partitions)?).await?;
        let worker_b =
            PartitionManager::new(session, worker_config("worker-b", num_partitions)?).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;

        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        println!("--- Initial assignment: 2 workers, {num_partitions} partitions ---");
        println!("  worker-a ({} partitions): {:?}", parts_a.len(), parts_a);
        println!("  worker-b ({} partitions): {:?}", parts_b.len(), parts_b);
        assert_eq!(
            parts_a.len() + parts_b.len(),
            num_partitions as usize,
            "all partitions must be assigned"
        );
        Ok((worker_a, worker_b))
    }

    /// Phase 2: Publish events routed to partitions by user_id.
    async fn phase_publish_events(
        session: &zenoh::Session,
        worker_a: &PartitionManager,
        worker_b: &PartitionManager,
        num_partitions: u32,
    ) -> mitiflow::Result<()> {
        const KEY_PREFIX: &str = "demo/user_actions";
        let publisher = EventPublisher::new(
            session,
            EventBusConfig::builder(KEY_PREFIX)
                .cache_size(200)
                .heartbeat(HeartbeatMode::Disabled)
                .num_partitions(num_partitions)
                .worker_id("publisher")
                .build()?,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let user_ids = [
            "alice", "bob", "carol", "dave", "eve", "frank", "grace", "henry",
        ];
        println!("\n--- Event routing to partitions ---");
        for user_id in &user_ids {
            let partition = worker_a.partition_for(user_id);
            let key = format!("{KEY_PREFIX}/{partition}/{user_id}");
            let event = Event::new(UserAction {
                user_id: user_id.to_string(),
                action: "login".to_string(),
            });
            publisher.publish_to(&key, &event).await?;
            let owner = if worker_a.my_partitions().await.contains(&partition) {
                "worker-a"
            } else {
                "worker-b"
            };
            println!(
                "  user={user_id:6}  partition={partition:2}  owner={owner}  key_suffix=…/{partition}/{user_id}"
            );
        }
        Ok(())
    }

    /// Phase 3: A third worker joins — observe rebalance.
    async fn phase_worker_joins(
        session: &zenoh::Session,
        worker_a: &PartitionManager,
        worker_b: &PartitionManager,
        rebalance_log: &Arc<Mutex<Vec<String>>>,
        num_partitions: u32,
    ) -> mitiflow::Result<PartitionManager> {
        let log_for_a = Arc::clone(rebalance_log);
        worker_a
            .on_rebalance(move |gained, lost| {
                log_for_a
                    .lock()
                    .unwrap()
                    .push(format!("worker-a  gained={gained:?}  lost={lost:?}"));
            })
            .await;

        let worker_c =
            PartitionManager::new(session, worker_config("worker-c", num_partitions)?).await?;

        let log_for_c = Arc::clone(rebalance_log);
        worker_c
            .on_rebalance(move |gained, lost| {
                log_for_c
                    .lock()
                    .unwrap()
                    .push(format!("worker-c  gained={gained:?}  lost={lost:?}"));
            })
            .await;

        tokio::time::sleep(Duration::from_millis(500)).await;

        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        let parts_c = worker_c.my_partitions().await;
        println!("\n--- After worker-c joins: 3 workers ---");
        println!("  worker-a ({} partitions): {:?}", parts_a.len(), parts_a);
        println!("  worker-b ({} partitions): {:?}", parts_b.len(), parts_b);
        println!("  worker-c ({} partitions): {:?}", parts_c.len(), parts_c);
        assert_eq!(
            parts_a.len() + parts_b.len() + parts_c.len(),
            num_partitions as usize,
            "all partitions must remain assigned after rebalance"
        );

        println!("\nRebalance callbacks:");
        for msg in rebalance_log.lock().unwrap().iter() {
            println!("  {msg}");
        }
        rebalance_log.lock().unwrap().clear();

        Ok(worker_c)
    }

    /// Phase 4: Show partition-filtered subscription key expressions.
    async fn phase_show_key_exprs(
        worker_a: &PartitionManager,
        worker_b: &PartitionManager,
        worker_c: &PartitionManager,
    ) {
        const KEY_PREFIX: &str = "demo/user_actions";
        println!("\n--- Partition-filtered subscription key expressions ---");
        println!(
            "  worker-a: {}",
            worker_a.subscription_key_expr(KEY_PREFIX).await
        );
        println!(
            "  worker-b: {}",
            worker_b.subscription_key_expr(KEY_PREFIX).await
        );
        println!(
            "  worker-c: {}",
            worker_c.subscription_key_expr(KEY_PREFIX).await
        );
    }

    /// Phase 5: worker-c leaves — partitions rebalance back to 2 workers.
    async fn phase_worker_leaves(
        worker_a: &PartitionManager,
        worker_b: &PartitionManager,
        worker_c: PartitionManager,
        rebalance_log: &Arc<Mutex<Vec<String>>>,
        num_partitions: u32,
    ) {
        println!("\n--- worker-c leaves ---");
        drop(worker_c);
        tokio::time::sleep(Duration::from_millis(500)).await;

        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        println!("  worker-a ({} partitions): {:?}", parts_a.len(), parts_a);
        println!("  worker-b ({} partitions): {:?}", parts_b.len(), parts_b);
        assert_eq!(
            parts_a.len() + parts_b.len(),
            num_partitions as usize,
            "all partitions must remain assigned after worker-c leaves"
        );

        println!("\nRebalance callbacks:");
        for msg in rebalance_log.lock().unwrap().iter() {
            println!("  {msg}");
        }
    }

    pub async fn run() -> mitiflow::Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter("mitiflow=info,consumer_groups=info")
            .init();

        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        const NUM_PARTITIONS: u32 = 12;

        let (worker_a, worker_b) = phase_initial_assignment(&session, NUM_PARTITIONS).await?;
        phase_publish_events(&session, &worker_a, &worker_b, NUM_PARTITIONS).await?;

        let rebalance_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let worker_c = phase_worker_joins(
            &session,
            &worker_a,
            &worker_b,
            &rebalance_log,
            NUM_PARTITIONS,
        )
        .await?;
        phase_show_key_exprs(&worker_a, &worker_b, &worker_c).await;
        phase_worker_leaves(&worker_a, &worker_b, worker_c, &rebalance_log, NUM_PARTITIONS).await;

        drop(worker_a);
        drop(worker_b);
        session.close().await.unwrap();

        println!("\nConsumer group example complete.");
        Ok(())
    }
}

#[cfg(feature = "partition")]
#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    inner::run().await
}
