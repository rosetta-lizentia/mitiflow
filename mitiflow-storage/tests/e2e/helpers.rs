//! TestCluster — manages N StorageAgent instances for E2E tests.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow::attachment::decode_metadata;
use mitiflow::{EventBusConfig, EventPublisher};
use mitiflow_storage::{StorageAgent, StorageAgentConfigBuilder};

/// A managed cluster of StorageAgent instances for testing.
pub struct TestCluster {
    pub test_name: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
    agents: Vec<Option<AgentSlot>>,
    node_labels: Vec<HashMap<String, String>>,
}

struct AgentSlot {
    agent: StorageAgent,
    session: zenoh::Session,
    _tmp: tempfile::TempDir,
}

impl TestCluster {
    /// Create a new cluster description without starting any agents.
    pub fn new(test_name: &str, num_nodes: usize, num_partitions: u32, rf: u32) -> Self {
        Self {
            test_name: test_name.to_string(),
            num_partitions,
            replication_factor: rf,
            agents: (0..num_nodes).map(|_| None).collect(),
            node_labels: vec![HashMap::new(); num_nodes],
        }
    }

    /// Set labels for a specific node slot (must be called before start_agent).
    #[allow(dead_code)]
    pub fn set_labels(&mut self, idx: usize, labels: HashMap<String, String>) {
        self.node_labels[idx] = labels;
    }

    /// Start agent at the given slot index.
    pub async fn start_agent(&mut self, idx: usize) {
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let bus_config = EventBusConfig::builder(format!("test/{}", self.test_name))
            .cache_size(100)
            .build()
            .unwrap();
        let config = StorageAgentConfigBuilder::new(tmp.path().to_path_buf(), bus_config)
            .node_id(format!("node-{idx}"))
            .num_partitions(self.num_partitions)
            .replication_factor(self.replication_factor)
            .drain_grace_period(Duration::from_millis(200))
            .health_interval(Duration::from_secs(60))
            .labels(self.node_labels[idx].clone())
            .build()
            .unwrap();

        let agent = StorageAgent::start(&session, config).await.unwrap();
        self.agents[idx] = Some(AgentSlot {
            agent,
            session,
            _tmp: tmp,
        });

        // Allow time for discovery.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    /// Gracefully stop agent at the given slot.
    pub async fn stop_agent(&mut self, idx: usize) {
        if let Some(mut slot) = self.agents[idx].take() {
            slot.agent.shutdown().await.unwrap();
            slot.session.close().await.unwrap();
        }
    }

    /// Simulate a crash by closing the session without graceful agent shutdown.
    /// This still sends a transport close which allows liveliness detection.
    pub async fn kill_agent(&mut self, idx: usize) {
        if let Some(slot) = self.agents[idx].take() {
            // Close session (sends transport close → liveliness delete propagates),
            // but skip graceful agent shutdown (no drain, no de-registration).
            let _ = slot.session.close().await;
        }
    }

    /// Wait for the cluster to stabilize, then recompute all agents.
    pub async fn wait_for_stable(&self, timeout: Duration) {
        tokio::time::sleep(timeout).await;
        for s in self.agents.iter().flatten() {
            let _ = s.agent.recompute_and_reconcile().await;
        }
        // Additional settle time after recomputation.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    /// Poll until the total partition count across all agents reaches `expected`,
    /// recomputing on each iteration. Returns the actual total.
    pub async fn wait_for_coverage(&self, expected: usize, deadline: Duration) -> usize {
        let start = tokio::time::Instant::now();
        let mut total = 0;
        while start.elapsed() < deadline {
            for s in self.agents.iter().flatten() {
                let _ = s.agent.recompute_and_reconcile().await;
            }
            total = self.total_assignments().await;
            if total >= expected {
                return total;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        total
    }

    /// Get the full assignment map: for each running agent, its assigned (partition, replica) set.
    pub async fn get_assignment_snapshot(&self) -> HashMap<String, Vec<(u32, u32)>> {
        let mut map = HashMap::new();
        for (i, slot) in self.agents.iter().enumerate() {
            if let Some(s) = slot {
                let parts = s.agent.assigned_partitions().await;
                map.insert(format!("node-{i}"), parts);
            }
        }
        map
    }

    /// Verify every (partition, replica) has at least one owner.
    pub async fn verify_full_coverage(&self) {
        let snapshot = self.get_assignment_snapshot().await;
        let all_assignments: Vec<(u32, u32)> = snapshot.values().flatten().copied().collect();

        for partition in 0..self.num_partitions {
            for replica in 0..self.replication_factor {
                assert!(
                    all_assignments.contains(&(partition, replica)),
                    "partition {partition} replica {replica} is unassigned. Full snapshot: {snapshot:?}"
                );
            }
        }
    }

    /// Verify no (partition, replica) is owned by more than one node.
    pub async fn verify_no_overlap(&self) {
        let snapshot = self.get_assignment_snapshot().await;
        let mut seen: HashMap<(u32, u32), String> = HashMap::new();
        for (node, parts) in &snapshot {
            for &pr in parts {
                if let Some(other) = seen.get(&pr) {
                    panic!(
                        "partition {:?} owned by both {} and {}, snapshot: {:?}",
                        pr, other, node, snapshot
                    );
                }
                seen.insert(pr, node.clone());
            }
        }
    }

    /// Check if there's any overlap without panicking.
    async fn has_overlap(&self) -> bool {
        let snapshot = self.get_assignment_snapshot().await;
        let mut seen: HashMap<(u32, u32), String> = HashMap::new();
        for (node, parts) in &snapshot {
            for &pr in parts {
                if seen.contains_key(&pr) {
                    return true;
                }
                seen.insert(pr, node.clone());
            }
        }
        false
    }

    /// Poll until there is no partition overlap, recomputing on each iteration.
    pub async fn wait_for_no_overlap(&self, deadline: Duration) {
        let start = tokio::time::Instant::now();
        while start.elapsed() < deadline {
            for s in self.agents.iter().flatten() {
                let _ = s.agent.recompute_and_reconcile().await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            if !self.has_overlap().await {
                return;
            }
        }
        // Final assert to get a nice error message.
        self.verify_no_overlap().await;
    }

    /// Count total assignments across all agents.
    pub async fn total_assignments(&self) -> usize {
        let snapshot = self.get_assignment_snapshot().await;
        snapshot.values().map(|v| v.len()).sum()
    }

    /// Shutdown all remaining agents.
    pub async fn shutdown_all(&mut self) {
        for i in 0..self.agents.len() {
            self.stop_agent(i).await;
        }
    }

    /// Get agent reference for direct interaction.
    #[allow(dead_code)]
    pub fn agent(&self, idx: usize) -> Option<&StorageAgent> {
        self.agents[idx].as_ref().map(|s| &s.agent)
    }

    /// Get a Zenoh session from a running agent (for publishing/querying).
    #[allow(dead_code)]
    pub fn session(&self, idx: usize) -> Option<&zenoh::Session> {
        self.agents[idx].as_ref().map(|s| &s.session)
    }

    /// Create an `EventPublisher` bound to this cluster's key prefix.
    #[allow(dead_code)]
    pub async fn create_publisher(&self, session: &zenoh::Session) -> EventPublisher {
        let bus_config = EventBusConfig::builder(format!("test/{}", self.test_name))
            .cache_size(100)
            .build()
            .unwrap();
        EventPublisher::new(session, bus_config).await.unwrap()
    }

    /// Publish `n` events with sequential payload `"msg-0"`, `"msg-1"`, … to the given partition.
    #[allow(dead_code)]
    pub async fn publish_events(&self, publisher: &EventPublisher, partition: u32, count: usize) {
        let key = format!("test/{}/p/{partition}/data", self.test_name);
        for i in 0..count {
            let payload = format!("msg-{i}");
            publisher
                .publish_bytes_to(&key, payload.as_bytes().to_vec())
                .await
                .unwrap();
        }
    }

    /// Query a store partition via Zenoh and return the number of events found.
    #[allow(dead_code)]
    pub async fn query_store_count(&self, session: &zenoh::Session, partition: u32) -> usize {
        let query_key = format!("test/{}/_store/{partition}", self.test_name);
        let mut count = 0;

        if let Ok(replies) = session
            .get(&query_key)
            .consolidation(zenoh::query::ConsolidationMode::None)
            .accept_replies(zenoh::query::ReplyKeyExpr::Any)
            .timeout(Duration::from_secs(3))
            .await
        {
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result()
                    && sample.attachment().is_some()
                {
                    count += 1;
                }
            }
        }

        count
    }

    /// Query a store partition and return the sequence numbers of stored events.
    #[allow(dead_code)]
    pub async fn query_store_seqs(&self, session: &zenoh::Session, partition: u32) -> Vec<u64> {
        let query_key = format!("test/{}/_store/{partition}", self.test_name);
        let mut seqs = Vec::new();

        if let Ok(replies) = session
            .get(&query_key)
            .consolidation(zenoh::query::ConsolidationMode::None)
            .accept_replies(zenoh::query::ReplyKeyExpr::Any)
            .timeout(Duration::from_secs(3))
            .await
        {
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result()
                    && let Some(attachment) = sample.attachment()
                    && let Ok(meta) = decode_metadata(attachment)
                {
                    seqs.push(meta.seq);
                }
            }
        }

        seqs.sort();
        seqs
    }

    /// Wait until a store partition has at least `min_count` events, polling periodically.
    #[allow(dead_code)]
    pub async fn wait_for_events(
        &self,
        session: &zenoh::Session,
        partition: u32,
        min_count: usize,
        deadline: Duration,
    ) -> usize {
        let start = tokio::time::Instant::now();
        let mut count = 0;
        while start.elapsed() < deadline {
            count = self.query_store_count(session, partition).await;
            if count >= min_count {
                return count;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        count
    }
}
