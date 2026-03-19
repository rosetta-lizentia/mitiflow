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

    pub async fn run() -> mitiflow::Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter("mitiflow=info,consumer_groups=info")
            .init();

        let session = zenoh::open(zenoh::Config::default()).await.unwrap();

        const KEY_PREFIX: &str = "demo/user_actions";
        const NUM_PARTITIONS: u32 = 12;

        // -------------------------------------------------------------------------
        // Phase 1: Start two workers and observe the initial partition assignment.
        //
        // Each PartitionManager declares a Zenoh liveliness token so all other
        // workers on the same key prefix can discover it.  On construction it
        // queries existing tokens, computes a full assignment via rendezvous
        // hashing, and starts a background task that reacts to membership changes.
        // -------------------------------------------------------------------------
        let worker_a = PartitionManager::new(&session, worker_config("worker-a", NUM_PARTITIONS)?)
            .await?;
        let worker_b = PartitionManager::new(&session, worker_config("worker-b", NUM_PARTITIONS)?)
            .await?;

        // Allow liveliness discovery to propagate between workers.
        tokio::time::sleep(Duration::from_millis(400)).await;

        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        println!(
            "--- Initial assignment: 2 workers, {NUM_PARTITIONS} partitions ---"
        );
        println!(
            "  worker-a ({} partitions): {:?}",
            parts_a.len(),
            parts_a
        );
        println!(
            "  worker-b ({} partitions): {:?}",
            parts_b.len(),
            parts_b
        );
        assert_eq!(
            parts_a.len() + parts_b.len(),
            NUM_PARTITIONS as usize,
            "all partitions must be assigned"
        );

        // -------------------------------------------------------------------------
        // Phase 2: Publish events — the publisher routes each event to the
        // partition that owns its routing key (e.g., user_id).
        //
        // `partition_for(key)` uses the same HRW hash as the PartitionManager so
        // each worker knows deterministically which events it should process.
        // -------------------------------------------------------------------------
        let publisher = EventPublisher::new(
            &session,
            EventBusConfig::builder(KEY_PREFIX)
                .cache_size(200)
                .heartbeat(HeartbeatMode::Disabled)
                .num_partitions(NUM_PARTITIONS)
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
            // Route using a key of the form `{prefix}/{partition}/{routing_key}`.
            let key = format!("{KEY_PREFIX}/{partition}/{user_id}");
            let event = Event::new(UserAction {
                user_id: user_id.to_string(),
                action: "login".to_string(),
            });
            publisher.publish_to(&key, &event).await?;

            // Determine the owning worker from the current assignment.
            let owner = if worker_a.my_partitions().await.contains(&partition) {
                "worker-a"
            } else {
                "worker-b"
            };
            println!(
                "  user={user_id:6}  partition={partition:2}  owner={owner}  key_suffix=…/{partition}/{user_id}"
            );
        }

        // -------------------------------------------------------------------------
        // Phase 3: A third worker joins — the partition ring rebalances.
        //
        // Register rebalance callbacks on worker-a and worker-c before the
        // topology change so we can observe which partitions move.
        // -------------------------------------------------------------------------
        let rebalance_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let log_for_a = Arc::clone(&rebalance_log);
        worker_a
            .on_rebalance(move |gained, lost| {
                let msg = format!("worker-a  gained={gained:?}  lost={lost:?}");
                log_for_a.lock().unwrap().push(msg);
            })
            .await;

        let worker_c =
            PartitionManager::new(&session, worker_config("worker-c", NUM_PARTITIONS)?).await?;

        let log_for_c = Arc::clone(&rebalance_log);
        worker_c
            .on_rebalance(move |gained, lost| {
                let msg = format!("worker-c  gained={gained:?}  lost={lost:?}");
                log_for_c.lock().unwrap().push(msg);
            })
            .await;

        // Allow liveliness propagation and background rebalance tasks to run.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        let parts_c = worker_c.my_partitions().await;
        println!("\n--- After worker-c joins: 3 workers ---");
        println!(
            "  worker-a ({} partitions): {:?}",
            parts_a.len(),
            parts_a
        );
        println!(
            "  worker-b ({} partitions): {:?}",
            parts_b.len(),
            parts_b
        );
        println!(
            "  worker-c ({} partitions): {:?}",
            parts_c.len(),
            parts_c
        );
        assert_eq!(
            parts_a.len() + parts_b.len() + parts_c.len(),
            NUM_PARTITIONS as usize,
            "all partitions must remain assigned after rebalance"
        );

        // Print rebalance callbacks that fired.
        println!("\nRebalance callbacks:");
        for msg in rebalance_log.lock().unwrap().iter() {
            println!("  {msg}");
        }
        rebalance_log.lock().unwrap().clear();

        // -------------------------------------------------------------------------
        // Phase 4: Show subscription key expressions.
        //
        // `subscription_key_expr` returns a Zenoh key expression that matches
        // exactly this worker's assigned partitions, e.g.:
        //   "demo/user_actions/0/**|demo/user_actions/3/**|demo/user_actions/7/**"
        //
        // This can be passed directly to `session.declare_subscriber(...)` so
        // Zenoh itself does the partition filtering — no application-level routing
        // overhead.
        // -------------------------------------------------------------------------
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

        // -------------------------------------------------------------------------
        // Phase 5: worker-c leaves — partitions rebalance back to 2 workers.
        // -------------------------------------------------------------------------
        println!("\n--- worker-c leaves ---");
        drop(worker_c);

        // Allow liveliness expiry to propagate.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let parts_a = worker_a.my_partitions().await;
        let parts_b = worker_b.my_partitions().await;
        println!(
            "  worker-a ({} partitions): {:?}",
            parts_a.len(),
            parts_a
        );
        println!(
            "  worker-b ({} partitions): {:?}",
            parts_b.len(),
            parts_b
        );
        assert_eq!(
            parts_a.len() + parts_b.len(),
            NUM_PARTITIONS as usize,
            "all partitions must remain assigned after worker-c leaves"
        );

        println!("\nRebalance callbacks:");
        for msg in rebalance_log.lock().unwrap().iter() {
            println!("  {msg}");
        }

        // Cleanup
        drop(worker_a);
        drop(worker_b);
        drop(publisher);
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
