//! TestCluster — manages N StorageAgent instances for E2E tests.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow::EventBusConfig;
use mitiflow_agent::{StorageAgent, StorageAgentConfigBuilder};

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
    pub fn new(
        test_name: &str,
        num_nodes: usize,
        num_partitions: u32,
        rf: u32,
    ) -> Self {
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
        if let Some(slot) = self.agents[idx].take() {
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
        for slot in &self.agents {
            if let Some(s) = slot {
                let _ = s.agent.recompute_and_reconcile().await;
            }
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
            for slot in &self.agents {
                if let Some(s) = slot {
                    let _ = s.agent.recompute_and_reconcile().await;
                }
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
        let all_assignments: Vec<(u32, u32)> =
            snapshot.values().flatten().copied().collect();

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
            for slot in &self.agents {
                if let Some(s) = slot {
                    let _ = s.agent.recompute_and_reconcile().await;
                }
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
}
