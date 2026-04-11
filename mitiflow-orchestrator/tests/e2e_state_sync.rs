//! Comprehensive E2E tests for orchestrator state synchronization.
//!
//! These tests target edge cases discovered from the UI (mitiflow-ui):
//! - Storage agent restart with new identity → phantom nodes in ClusterView
//! - Phantom node accumulation (nodes never evicted from ClusterView)
//! - Drain overrides orphaned after agent restart with new ID
//! - Lag/watermark staleness after agent death
//! - Orchestrator restart and state recovery
//! - SSE event ordering consistency
//!
//! Each test uses a unique UUID-v7 key prefix to avoid cross-test interference.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow::EventBusConfig;
use mitiflow::store::OffsetCommit;
use mitiflow::store::watermark::{CommitWatermark, PublisherWatermark};
use mitiflow::types::PublisherId;
use mitiflow_orchestrator::orchestrator::{Orchestrator, OrchestratorConfig};
use mitiflow_storage::{
    NodeStatus, PartitionStatus, StorageAgent, StorageAgentConfigBuilder, StoreState,
};
use serde_json::Value;

/// Wrap a test body in a timeout to prevent hangs.
macro_rules! with_timeout {
    ($body:expr) => {
        tokio::time::timeout(Duration::from_secs(60), async { $body })
            .await
            .expect("test timed out after 60s")
    };
}

/// Unique key prefix per test to avoid cross-talk.
fn key_prefix(test: &str) -> String {
    format!("test/e2e_sync_{test}_{}", uuid::Uuid::now_v7())
}

/// Find a free TCP port.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

// ── Test Cluster Helper ──────────────────────────────────────────────────────

/// Slot holding a running agent.
struct AgentSlot {
    agent: StorageAgent,
    session: zenoh::Session,
    node_id: String,
    _tmp: tempfile::TempDir,
}

/// Extended test cluster for state-sync edge case testing.
struct StateSyncCluster {
    prefix: String,
    num_partitions: u32,
    replication_factor: u32,
    agents: Vec<Option<AgentSlot>>,
    orchestrator: Option<Orchestrator>,
    orch_session: Option<zenoh::Session>,
    _orch_dir: Option<tempfile::TempDir>,
    /// HTTP base URL (if HTTP enabled).
    http_url: Option<String>,
    /// Track all node_ids that have ever been used (for phantom detection).
    all_node_ids: Vec<String>,
}

impl StateSyncCluster {
    fn new(test_name: &str, num_slots: usize, num_partitions: u32, rf: u32) -> Self {
        Self {
            prefix: key_prefix(test_name),
            num_partitions,
            replication_factor: rf,
            agents: (0..num_slots).map(|_| None).collect(),
            orchestrator: None,
            orch_session: None,
            _orch_dir: None,
            http_url: None,
            all_node_ids: Vec::new(),
        }
    }

    /// Start an agent at the given slot index with a specific node_id.
    async fn start_agent_with_id(&mut self, idx: usize, node_id: &str) {
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let bus_config = EventBusConfig::builder(&self.prefix)
            .cache_size(100)
            .build()
            .unwrap();
        let config = StorageAgentConfigBuilder::new(tmp.path().to_path_buf(), bus_config)
            .node_id(node_id.to_string())
            .num_partitions(self.num_partitions)
            .replication_factor(self.replication_factor)
            .drain_grace_period(Duration::from_millis(200))
            .health_interval(Duration::from_secs(60))
            .build()
            .unwrap();

        let agent = StorageAgent::start(&session, config).await.unwrap();
        if !self.all_node_ids.contains(&node_id.to_string()) {
            self.all_node_ids.push(node_id.to_string());
        }
        self.agents[idx] = Some(AgentSlot {
            agent,
            session,
            node_id: node_id.to_string(),
            _tmp: tmp,
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    /// Start an agent at the given slot with the default "node-{idx}" ID.
    async fn start_agent(&mut self, idx: usize) {
        let node_id = format!("node-{idx}");
        self.start_agent_with_id(idx, &node_id).await;
    }

    /// Start an agent with a randomly generated UUID node_id (simulates default restart).
    async fn start_agent_auto_id(&mut self, idx: usize) -> String {
        let node_id = uuid::Uuid::now_v7().to_string();
        self.start_agent_with_id(idx, &node_id).await;
        node_id
    }

    /// Stop an agent gracefully.
    async fn stop_agent(&mut self, idx: usize) {
        if let Some(mut slot) = self.agents[idx].take() {
            slot.agent.shutdown().await.unwrap();
            slot.session.close().await.unwrap();
        }
    }

    /// Kill an agent abruptly (no graceful drain).
    async fn kill_agent(&mut self, idx: usize) {
        if let Some(slot) = self.agents[idx].take() {
            let _ = slot.session.close().await;
        }
    }

    /// Start orchestrator without HTTP.
    async fn start_orchestrator(&mut self) {
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = OrchestratorConfig {
            key_prefix: self.prefix.clone(),
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(1),
            admin_prefix: Some(format!("{}/_admin", self.prefix)),
            http_bind: None,
            auth_token: None,
            bootstrap_topics_from: None,
        };
        let mut orch = Orchestrator::new(&session, config).unwrap();
        orch.run().await.unwrap();
        self.orchestrator = Some(orch);
        self.orch_session = Some(session);
        self._orch_dir = Some(dir);
    }

    /// Start orchestrator with HTTP enabled on a random port.
    async fn start_orchestrator_with_http(&mut self) {
        let port = free_port().await;
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = OrchestratorConfig {
            key_prefix: self.prefix.clone(),
            data_dir: dir.path().to_path_buf(),
            lag_interval: Duration::from_secs(1),
            admin_prefix: Some(format!("{}/_admin", self.prefix)),
            http_bind: Some(([127, 0, 0, 1], port).into()),
            auth_token: None,
            bootstrap_topics_from: None,
        };
        let mut orch = Orchestrator::new(&session, config).unwrap();
        orch.run().await.unwrap();
        self.orchestrator = Some(orch);
        self.orch_session = Some(session);
        self._orch_dir = Some(dir);
        self.http_url = Some(format!("http://127.0.0.1:{port}"));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    async fn stop_orchestrator(&mut self) {
        if let Some(orch) = self.orchestrator.take() {
            orch.shutdown().await;
        }
        if let Some(s) = self.orch_session.take() {
            let _ = s.close().await;
        }
        self._orch_dir = None;
        self.http_url = None;
    }

    // ── ClusterView queries ──────────────────────────────────────────────

    /// Wait for the orchestrator's ClusterView to see `expected` online nodes.
    async fn wait_for_online_count(&self, expected: usize, timeout: Duration) -> usize {
        let start = tokio::time::Instant::now();
        let orch = self.orchestrator.as_ref().unwrap();
        let cv = orch.cluster_view().unwrap();
        loop {
            let count = cv.online_count().await;
            if count >= expected {
                return count;
            }
            if start.elapsed() > timeout {
                return count;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait until a specific node_id goes offline in ClusterView.
    async fn wait_for_node_offline(&self, node_id: &str, timeout: Duration) -> bool {
        let start = tokio::time::Instant::now();
        let orch = self.orchestrator.as_ref().unwrap();
        let cv = orch.cluster_view().unwrap();
        loop {
            if !cv.is_node_online(node_id).await {
                // Node is known and offline (or unknown)
                let nodes = cv.nodes().await;
                if nodes.contains_key(node_id) {
                    return true; // Known and offline
                }
            }
            if start.elapsed() > timeout {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait until a specific node_id goes online in ClusterView.
    async fn wait_for_node_online(&self, node_id: &str, timeout: Duration) -> bool {
        let start = tokio::time::Instant::now();
        let orch = self.orchestrator.as_ref().unwrap();
        let cv = orch.cluster_view().unwrap();
        loop {
            if cv.is_node_online(node_id).await {
                return true;
            }
            if start.elapsed() > timeout {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Get all nodes from ClusterView (includes offline).
    async fn get_all_nodes(&self) -> HashMap<String, bool> {
        let orch = self.orchestrator.as_ref().unwrap();
        let cv = orch.cluster_view().unwrap();
        let nodes = cv.nodes().await;
        nodes
            .into_iter()
            .map(|(id, info)| (id, info.online))
            .collect()
    }

    /// Count offline nodes in ClusterView.
    async fn count_offline_nodes(&self) -> usize {
        let nodes = self.get_all_nodes().await;
        nodes.values().filter(|online| !**online).count()
    }

    /// Count online nodes in ClusterView.
    async fn count_online_nodes(&self) -> usize {
        let nodes = self.get_all_nodes().await;
        nodes.values().filter(|online| **online).count()
    }

    // ── Agent assignment helpers ─────────────────────────────────────────

    /// Force all live agents to recompute assignments.
    async fn recompute_all(&self) {
        for s in self.agents.iter().flatten() {
            let _ = s.agent.recompute_and_reconcile().await;
        }
    }

    /// Publish each agent's current assigned partitions to the ClusterView.
    async fn publish_agent_statuses(&self) {
        for slot in self.agents.iter().flatten() {
            let parts = slot.agent.assigned_partitions().await;
            let statuses: Vec<PartitionStatus> = parts
                .iter()
                .map(|&(p, r)| PartitionStatus {
                    partition: p,
                    replica: r,
                    state: StoreState::Active,
                    event_count: 0,
                    watermark_seq: HashMap::new(),
                })
                .collect();
            let status = NodeStatus {
                node_id: slot.node_id.clone(),
                partitions: statuses,
                timestamp: chrono::Utc::now(),
            };
            if let Ok(bytes) = serde_json::to_vec(&status) {
                let key = format!("{}/_cluster/status/{}", self.prefix, slot.node_id);
                let _ = slot.session.put(&key, bytes).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    /// Get total partition assignments across all live agents.
    async fn total_assignments(&self) -> usize {
        let mut total = 0;
        for s in self.agents.iter().flatten() {
            total += s.agent.assigned_partitions().await.len();
        }
        total
    }

    /// Wait until agents reach `expected` total assignments.
    async fn wait_for_coverage(&self, expected: usize, timeout: Duration) -> usize {
        let start = tokio::time::Instant::now();
        loop {
            self.recompute_all().await;
            let total = self.total_assignments().await;
            if total >= expected {
                return total;
            }
            if start.elapsed() > timeout {
                return total;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Verify every (partition, replica) has at least one owner.
    async fn verify_full_coverage(&self) {
        let mut all = Vec::new();
        for slot in self.agents.iter().flatten() {
            let parts = slot.agent.assigned_partitions().await;
            all.extend(parts);
        }
        for p in 0..self.num_partitions {
            for r in 0..self.replication_factor {
                assert!(all.contains(&(p, r)), "p{p}r{r} unassigned. All: {all:?}");
            }
        }
    }

    // ── HTTP helpers ─────────────────────────────────────────────────────

    fn http_url(&self) -> &str {
        self.http_url.as_deref().expect("HTTP not enabled")
    }

    /// GET /api/v1/cluster/nodes via HTTP.
    async fn http_get_cluster_nodes(&self) -> HashMap<String, Value> {
        let url = format!("{}/api/v1/cluster/nodes", self.http_url());
        let resp = reqwest::get(&url).await.unwrap();
        assert_eq!(resp.status(), 200);
        resp.json().await.unwrap()
    }

    /// GET /api/v1/cluster/status via HTTP.
    async fn http_get_cluster_status(&self) -> Value {
        let url = format!("{}/api/v1/cluster/status", self.http_url());
        let resp = reqwest::get(&url).await.unwrap();
        assert_eq!(resp.status(), 200);
        resp.json().await.unwrap()
    }

    // ── Lag simulation helpers ───────────────────────────────────────────

    /// Publish a watermark to simulate a storage agent reporting durability progress.
    async fn publish_watermark(
        &self,
        partition: u32,
        publisher_id: PublisherId,
        committed_seq: u64,
    ) {
        let session = self.orch_session.as_ref().unwrap();
        let wm = CommitWatermark {
            partition,
            publishers: {
                let mut map = HashMap::new();
                map.insert(
                    publisher_id,
                    PublisherWatermark {
                        committed_seq,
                        gaps: vec![],
                    },
                );
                map
            },
            timestamp: chrono::Utc::now(),
            epoch: 0,
        };
        let bytes = serde_json::to_vec(&wm).unwrap();
        let key = format!("{}/_watermark/{}", self.prefix, partition);
        session.put(&key, bytes).await.unwrap();
    }

    /// Publish an offset commit to simulate a consumer group checkpoint.
    async fn publish_offset(
        &self,
        group_id: &str,
        partition: u32,
        publisher_id: PublisherId,
        offset_seq: u64,
    ) {
        let session = self.orch_session.as_ref().unwrap();
        let commit = OffsetCommit {
            group_id: group_id.to_string(),
            member_id: "test-member".to_string(),
            partition,
            offsets: {
                let mut map = HashMap::new();
                map.insert(publisher_id, offset_seq);
                map
            },
            generation: 1,
            timestamp: chrono::Utc::now(),
        };
        let bytes = serde_json::to_vec(&commit).unwrap();
        let key = format!("{}/_offsets/{}/{}", self.prefix, partition, group_id);
        session.put(&key, bytes).await.unwrap();
    }

    // ── Shutdown ─────────────────────────────────────────────────────────

    async fn shutdown_all(&mut self) {
        for i in 0..self.agents.len() {
            self.stop_agent(i).await;
        }
        self.stop_orchestrator().await;
    }
}

// ==========================================================================
// Phase 2: Storage Identity & Phantom Node Tests
// ==========================================================================

/// Storage agent restart with auto-generated ID creates a phantom offline node
/// in ClusterView (old ID stays, new ID added).
///
/// **Expected: EXPOSES BUG** — ClusterView accumulates phantom entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_storage_restart_new_id_creates_phantom_node() {
    with_timeout!({
        let mut c = StateSyncCluster::new("phantom_node", 3, 6, 1);
        c.start_orchestrator_with_http().await;

        // Start 3 agents with explicit IDs
        for i in 0..3 {
            c.start_agent(i).await;
        }

        let seen = c.wait_for_online_count(3, Duration::from_secs(5)).await;
        assert_eq!(seen, 3, "should see 3 online nodes initially");

        // Stop agent-1 gracefully
        c.stop_agent(1).await;

        // Wait for node-1 to go offline in ClusterView
        let offline = c
            .wait_for_node_offline("node-1", Duration::from_secs(10))
            .await;
        assert!(offline, "node-1 should appear offline after stop");

        // Restart agent-1 with a NEW random ID (simulates default restart without MITIFLOW_NODE_ID)
        let new_id = c.start_agent_auto_id(1).await;
        eprintln!("agent-1 restarted with new ID: {new_id}");

        // Wait for the new node to appear online
        let new_online = c
            .wait_for_node_online(&new_id, Duration::from_secs(5))
            .await;
        assert!(new_online, "new agent should appear online");

        // Now check: ClusterView should have 4 entries (node-0, node-1[offline], new_id, node-2)
        let all_nodes = c.get_all_nodes().await;
        eprintln!("ClusterView nodes after restart: {all_nodes:?}");

        // The phantom node detection: total nodes > running agents
        let total_nodes = all_nodes.len();
        let online_count = all_nodes.values().filter(|o| **o).count();
        let offline_count = all_nodes.values().filter(|o| !**o).count();

        // BUG: old "node-1" should still be in the map, marked offline
        assert!(
            all_nodes.contains_key("node-1"),
            "phantom: old node-1 should still exist in ClusterView"
        );
        assert_eq!(
            all_nodes.get("node-1"),
            Some(&false),
            "phantom: old node-1 should be offline"
        );

        // There should be 4 total entries: node-0 (online), node-1 (offline/phantom),
        // new_id (online), node-2 (online)
        assert_eq!(
            total_nodes, 4,
            "BUG CONFIRMED: ClusterView has {total_nodes} entries (expected 4 = 3 running + 1 phantom). Nodes: {all_nodes:?}"
        );
        assert_eq!(online_count, 3, "should have 3 online nodes");
        assert_eq!(offline_count, 1, "should have 1 phantom offline node");

        // Verify via HTTP (same as what the UI sees)
        let http_nodes = c.http_get_cluster_nodes().await;
        assert_eq!(
            http_nodes.len(),
            4,
            "HTTP API exposes phantom node to UI. Nodes: {http_nodes:?}"
        );

        let http_status = c.http_get_cluster_status().await;
        eprintln!("HTTP cluster status: {http_status}");

        c.shutdown_all().await;
    });
}

/// Storage agent restart with SAME explicit node_id preserves identity.
/// ClusterView should have exactly 3 entries, all online.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_storage_restart_same_id_preserves_identity() {
    with_timeout!({
        let mut c = StateSyncCluster::new("same_id", 3, 6, 1);
        c.start_orchestrator().await;

        for i in 0..3 {
            c.start_agent(i).await;
        }

        let seen = c.wait_for_online_count(3, Duration::from_secs(5)).await;
        assert_eq!(seen, 3);

        // Stop agent-1
        c.stop_agent(1).await;

        // Wait for offline
        let offline = c
            .wait_for_node_offline("node-1", Duration::from_secs(10))
            .await;
        assert!(offline, "node-1 should go offline");

        // Restart agent-1 with SAME node_id
        c.start_agent(1).await;

        // Wait for it to come back online
        let online = c
            .wait_for_node_online("node-1", Duration::from_secs(5))
            .await;
        assert!(online, "node-1 should come back online with same ID");

        // ClusterView should have exactly 3 entries, all online
        let all_nodes = c.get_all_nodes().await;
        assert_eq!(
            all_nodes.len(),
            3,
            "should have exactly 3 nodes (no phantoms). Nodes: {all_nodes:?}"
        );
        assert_eq!(c.count_online_nodes().await, 3, "all 3 should be online");
        assert_eq!(
            c.count_offline_nodes().await,
            0,
            "no nodes should be offline"
        );

        c.shutdown_all().await;
    });
}

/// Multiple restarts with auto-generated IDs accumulate phantom nodes.
///
/// **Expected: EXPOSES BUG** — N restarts = N phantom offline entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_multiple_restarts_accumulate_phantoms() {
    with_timeout!({
        let mut c = StateSyncCluster::new("multi_phantom", 1, 4, 1);
        c.start_orchestrator_with_http().await;

        // Start 1 agent with auto-ID
        let id1 = c.start_agent_auto_id(0).await;
        eprintln!("restart 0: id = {id1}");
        c.wait_for_online_count(1, Duration::from_secs(5)).await;

        // Restart 3 times, each with a new auto-id
        let mut old_ids = vec![id1];
        for restart in 1..=3 {
            c.stop_agent(0).await;
            let old_id = &old_ids[old_ids.len() - 1];
            c.wait_for_node_offline(old_id, Duration::from_secs(10))
                .await;

            let new_id = c.start_agent_auto_id(0).await;
            eprintln!("restart {restart}: id = {new_id}");
            c.wait_for_node_online(&new_id, Duration::from_secs(5))
                .await;
            old_ids.push(new_id);
        }

        // Wait for state to settle
        tokio::time::sleep(Duration::from_millis(500)).await;

        let all_nodes = c.get_all_nodes().await;
        eprintln!("ClusterView after 3 restarts: {all_nodes:?}");

        // Should have 4 entries: 3 phantom offline + 1 online
        let online_count = c.count_online_nodes().await;
        let offline_count = c.count_offline_nodes().await;

        assert_eq!(
            online_count, 1,
            "should have exactly 1 online node after restarts"
        );
        assert_eq!(
            offline_count, 3,
            "BUG CONFIRMED: {offline_count} phantom offline nodes accumulated (expected 3)"
        );
        assert_eq!(
            all_nodes.len(),
            4,
            "total node count = 4 (3 phantoms + 1 active)"
        );

        // Verify via HTTP — this is what the UI Nodes page shows
        let http_status = c.http_get_cluster_status().await;
        let total = http_status["total_nodes"].as_u64().unwrap_or(0);
        let online = http_status["online_nodes"].as_u64().unwrap_or(0);
        eprintln!("HTTP status: total={total}, online={online}");
        assert_eq!(
            total, 4,
            "HTTP reports 4 total nodes (phantom accumulation)"
        );
        assert_eq!(online, 1, "HTTP reports 1 online node");

        c.shutdown_all().await;
    });
}

/// Rapid restart with same ID before liveliness token fully expires.
/// Should not create duplicate entries.
///
/// **Known flaky**: Under parallel test load, Zenoh's internal routing mutexes
/// get poisoned by concurrent session teardown + creation with the same node ID.
/// This exposes a real edge case: rapid restart can corrupt Zenoh's routing table,
/// causing PoisonError panics in `face.rs`. Run in isolation to reproduce reliably.
#[ignore = "flaky under parallel Zenoh load — exposes real rapid-restart race condition"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_rapid_restart_same_id_no_duplicate() {
    with_timeout!({
        let mut c = StateSyncCluster::new("rapid_restart", 3, 6, 1);
        c.start_orchestrator().await;

        for i in 0..3 {
            c.start_agent(i).await;
        }
        c.wait_for_online_count(3, Duration::from_secs(5)).await;

        // Kill agent-1 abruptly and wait briefly for session cleanup
        c.kill_agent(1).await;
        // Wait just long enough for the Zenoh session to close (but not for
        // full liveliness expiry — we want to test the rapid restart path)
        tokio::time::sleep(Duration::from_millis(1000)).await;
        c.start_agent(1).await;

        // Wait for node-1 to be online (may take time for old token to expire + new to register)
        let online = c
            .wait_for_node_online("node-1", Duration::from_secs(20))
            .await;
        assert!(online, "node-1 should be online after rapid restart");

        // Should have exactly 3 entries, no duplicates
        let all_nodes = c.get_all_nodes().await;
        let node1_entries: Vec<_> = all_nodes
            .iter()
            .filter(|(id, _)| id.as_str() == "node-1")
            .collect();
        assert_eq!(
            node1_entries.len(),
            1,
            "no duplicate entries for node-1. All: {all_nodes:?}"
        );
        assert_eq!(all_nodes.len(), 3, "exactly 3 nodes");

        c.shutdown_all().await;
    });
}

/// Verify full partition coverage is maintained after restart with same ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_restart_agent_partition_coverage_maintained() {
    with_timeout!({
        let mut c = StateSyncCluster::new("coverage_restart", 3, 12, 1);
        c.start_orchestrator().await;

        for i in 0..3 {
            c.start_agent(i).await;
        }

        let expected = (c.num_partitions * c.replication_factor) as usize;
        let total = c.wait_for_coverage(expected, Duration::from_secs(10)).await;
        assert!(total >= expected, "initial coverage: {total}/{expected}");

        // Verify full coverage before restart
        c.verify_full_coverage().await;

        // Stop agent-1, restart with same ID
        c.stop_agent(1).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        c.start_agent(1).await;

        // Wait for agents to settle and recompute
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            c.recompute_all().await;
        }

        // Verify full coverage after restart
        let total_after = c.total_assignments().await;
        assert!(
            total_after >= expected,
            "coverage after restart: {total_after}/{expected}"
        );
        c.verify_full_coverage().await;

        c.shutdown_all().await;
    });
}

// ==========================================================================
// Phase 3: Drain + Restart Interaction Tests
// ==========================================================================

/// Drain a node, then restart it with a new ID.
/// Old drain overrides reference the old node_id — the new agent ignores them.
///
/// **Expected: EXPOSES BUG** — stale overrides for dead node-id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_drain_then_restart_new_id_orphans_overrides() {
    with_timeout!({
        let mut c = StateSyncCluster::new("drain_new_id", 3, 6, 1);
        c.start_orchestrator().await;

        for i in 0..3 {
            c.start_agent(i).await;
        }

        let expected = (c.num_partitions * c.replication_factor) as usize;
        c.wait_for_coverage(expected, Duration::from_secs(5)).await;
        c.wait_for_online_count(3, Duration::from_secs(3)).await;

        // Publish statuses so ClusterView has assignment data for drain
        c.recompute_all().await;
        c.publish_agent_statuses().await;

        // Drain node-2
        {
            let orch = c.orchestrator.as_ref().unwrap();
            let overrides = orch
                .drain_node("node-2", c.replication_factor)
                .await
                .unwrap();
            assert!(
                !overrides.is_empty(),
                "drain should produce overrides for node-2"
            );
            eprintln!("drain overrides: {overrides:?}");
        }

        // Wait for drain to propagate
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            c.recompute_all().await;
        }

        // Now stop node-2 and restart with a NEW random ID
        c.stop_agent(2).await;
        c.wait_for_node_offline("node-2", Duration::from_secs(10))
            .await;

        let new_id = c.start_agent_auto_id(2).await;
        eprintln!("node-2 restarted as: {new_id}");

        c.wait_for_node_online(&new_id, Duration::from_secs(5))
            .await;

        // Recompute
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            c.recompute_all().await;
        }

        // Check: overrides still reference "node-2" but it's now a phantom
        let current = {
            let om = c.orchestrator.as_ref().unwrap().override_manager().unwrap();
            om.current().await
        };
        let stale_overrides: Vec<_> = current
            .entries
            .iter()
            .filter(|e| e.node_id == "node-2" || e.reason.contains("node-2"))
            .collect();
        eprintln!("stale overrides referencing node-2: {stale_overrides:?}");

        // The old drain moved partitions AWAY from node-2 to other nodes.
        // The overrides say "partition X should go to node-0/node-1" (not node-2).
        // But now node-2 is gone and {new_id} exists. The new agent participates
        // in HRW normally, but the old drain overrides are stale because:
        // - If someone tries to undrain "node-2", it refers to a phantom.
        // - The new agent with {new_id} wasn't drained, so it may pick up partitions
        //   that are supposed to be drained.

        // Verify the new agent has joined the cluster
        let all_nodes = c.get_all_nodes().await;
        assert!(
            all_nodes.contains_key(&new_id),
            "new agent should be in ClusterView"
        );
        assert!(
            all_nodes.contains_key("node-2"),
            "phantom node-2 should still exist (stale)"
        );

        eprintln!("all nodes: {all_nodes:?}");

        c.shutdown_all().await;
    });
}

/// Undrain a node that has been restarted with a new ID.
/// The undrain references the old node-id — should clean up overrides anyway.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_undrain_after_restart_new_id_cleans_up() {
    with_timeout!({
        let mut c = StateSyncCluster::new("undrain_new_id", 3, 6, 1);
        c.start_orchestrator().await;

        for i in 0..3 {
            c.start_agent(i).await;
        }

        let expected = (c.num_partitions * c.replication_factor) as usize;
        c.wait_for_coverage(expected, Duration::from_secs(5)).await;
        c.wait_for_online_count(3, Duration::from_secs(3)).await;
        c.recompute_all().await;
        c.publish_agent_statuses().await;

        // Drain node-2
        {
            let orch = c.orchestrator.as_ref().unwrap();
            let overrides = orch
                .drain_node("node-2", c.replication_factor)
                .await
                .unwrap();
            assert!(!overrides.is_empty());
        }

        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            c.recompute_all().await;
        }

        // Restart node-2 with new ID
        c.stop_agent(2).await;
        let _new_id = c.start_agent_auto_id(2).await;

        // Undrain the old "node-2" — should remove drain-related overrides
        let result = c
            .orchestrator
            .as_ref()
            .unwrap()
            .undrain_node("node-2")
            .await;
        assert!(
            result.is_ok(),
            "undrain should succeed even for phantom node"
        );

        // After undrain, overrides should be cleared
        let om = c.orchestrator.as_ref().unwrap().override_manager().unwrap();
        let current = om.current().await;
        let drain_overrides: Vec<_> = current
            .entries
            .iter()
            .filter(|e| e.reason.contains("node-2"))
            .collect();
        assert!(
            drain_overrides.is_empty(),
            "undrain should remove all drain overrides for node-2. Remaining: {drain_overrides:?}"
        );

        c.shutdown_all().await;
    });
}

/// Kill a node while drain is propagating.
/// Remaining nodes should eventually cover all partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_drain_active_node_killed_during_propagation() {
    with_timeout!({
        let mut c = StateSyncCluster::new("drain_kill", 3, 6, 1);
        c.start_orchestrator().await;

        for i in 0..3 {
            c.start_agent(i).await;
        }

        let expected = (c.num_partitions * c.replication_factor) as usize;
        c.wait_for_coverage(expected, Duration::from_secs(5)).await;
        c.wait_for_online_count(3, Duration::from_secs(3)).await;
        c.recompute_all().await;
        c.publish_agent_statuses().await;

        // Drain node-1
        {
            let orch = c.orchestrator.as_ref().unwrap();
            let overrides = orch
                .drain_node("node-1", c.replication_factor)
                .await
                .unwrap();
            assert!(!overrides.is_empty());
        }

        // Kill node-1 while drain is propagating (don't wait for full convergence)
        c.kill_agent(1).await;

        // Wait for remaining 2 nodes to stabilize
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            c.recompute_all().await;
        }

        // node-0 and node-2 should cover all partitions
        let mut all = Vec::new();
        for slot in c.agents.iter().flatten() {
            all.extend(slot.agent.assigned_partitions().await);
        }
        let total = all.len();
        assert!(
            total >= expected,
            "remaining nodes should cover all {expected} partitions, got {total}. Assignments: {all:?}"
        );

        c.shutdown_all().await;
    });
}

// ==========================================================================
// Phase 4: Lag & Watermark Staleness Tests
// ==========================================================================

/// Lag reports remain stale after agent death — watermarks never expire.
///
/// **Expected: EXPOSES BUG** — LagMonitor keeps stale watermarks indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_lag_reports_stale_after_agent_death() {
    with_timeout!({
        let mut c = StateSyncCluster::new("lag_stale", 1, 4, 1);
        c.start_orchestrator().await;
        c.start_agent(0).await;
        c.wait_for_online_count(1, Duration::from_secs(5)).await;

        let pub_id = PublisherId::new();

        // Simulate watermark (agent reporting durability progress)
        c.publish_watermark(0, pub_id, 100).await;

        // Simulate offset commit (consumer group checkpoint)
        c.publish_offset("test-group", 0, pub_id, 50).await;

        // Wait for lag monitor to ingest
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify lag exists
        {
            let orch = c.orchestrator.as_ref().unwrap();
            let lag = orch.get_group_lag("test-group").await;
            assert!(!lag.is_empty(), "should have lag reports");
            let initial_lag: u64 = lag.iter().map(|r| r.total).sum();
            assert_eq!(initial_lag, 50, "lag should be 100 - 50 = 50");
            eprintln!("initial lag: {initial_lag}");
        }

        // Kill the storage agent
        c.kill_agent(0).await;

        // Wait to confirm offline
        c.wait_for_node_offline("node-0", Duration::from_secs(10))
            .await;

        // Wait some more — lag should ideally be marked stale or cleared
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Query lag again — BUG: still returns stale data
        let lag_after = c
            .orchestrator
            .as_ref()
            .unwrap()
            .get_group_lag("test-group")
            .await;
        let stale_lag: u64 = lag_after.iter().map(|r| r.total).sum();
        eprintln!("lag after agent death: {stale_lag}");

        // The lag is still 50 even though the agent is dead.
        // This is the staleness bug — the UI shows this stale lag indefinitely.
        assert_eq!(
            stale_lag, 50,
            "BUG CONFIRMED: lag reports are stale after agent death (expected 50, got {stale_lag})"
        );

        assert!(!lag_after.is_empty(), "stale lag reports still present");

        c.shutdown_all().await;
    });
}

/// Watermarks resume correctly after agent restart with same ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_watermark_resumes_after_agent_restart() {
    with_timeout!({
        let mut c = StateSyncCluster::new("wm_resume", 1, 4, 1);
        c.start_orchestrator().await;
        c.start_agent(0).await;
        c.wait_for_online_count(1, Duration::from_secs(5)).await;

        let pub_id = PublisherId::new();

        // Publish initial watermark + offset
        c.publish_watermark(0, pub_id, 100).await;
        c.publish_offset("test-group", 0, pub_id, 50).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        {
            let orch = c.orchestrator.as_ref().unwrap();
            let lag_before = orch.get_group_lag("test-group").await;
            let total_before: u64 = lag_before.iter().map(|r| r.total).sum();
            assert_eq!(total_before, 50, "initial lag should be 50");
        }

        // Restart agent
        c.stop_agent(0).await;
        c.wait_for_node_offline("node-0", Duration::from_secs(10))
            .await;
        c.start_agent(0).await;
        c.wait_for_node_online("node-0", Duration::from_secs(5))
            .await;

        // Publish updated watermark (more events processed)
        c.publish_watermark(0, pub_id, 200).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Lag should now be 200 - 50 = 150
        let lag_after = c
            .orchestrator
            .as_ref()
            .unwrap()
            .get_group_lag("test-group")
            .await;
        let total_after: u64 = lag_after.iter().map(|r| r.total).sum();
        assert_eq!(
            total_after, 150,
            "lag should update to 150 after watermark advance"
        );

        c.shutdown_all().await;
    });
}

/// Lag monitor handles multiple publishers correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_lag_monitor_handles_publisher_restart() {
    with_timeout!({
        let mut c = StateSyncCluster::new("pub_restart", 1, 4, 1);
        c.start_orchestrator().await;
        c.start_agent(0).await;
        c.wait_for_online_count(1, Duration::from_secs(5)).await;

        let pub1 = PublisherId::new();
        let pub2 = PublisherId::new();

        // Publisher 1 watermark
        c.publish_watermark(0, pub1, 100).await;
        c.publish_offset("test-group", 0, pub1, 30).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let orch = c.orchestrator.as_ref().unwrap();
        let lag1 = orch.get_group_lag("test-group").await;
        let total1: u64 = lag1.iter().map(|r| r.total).sum();
        assert_eq!(total1, 70, "lag from pub1: 100 - 30 = 70");

        // "New" publisher (simulates publisher restart with new PublisherId)
        // Both watermarks arrive on the same partition
        c.publish_watermark(0, pub2, 50).await;
        c.publish_offset("test-group", 0, pub2, 10).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let lag2 = orch.get_group_lag("test-group").await;
        let total2: u64 = lag2.iter().map(|r| r.total).sum();
        // Should be (100-30) + (50-10) = 70 + 40 = 110
        assert_eq!(
            total2, 110,
            "lag should include both publishers: 70 + 40 = 110, got {total2}"
        );

        // Verify both publishers appear in lag report
        let partition0_report = lag2.iter().find(|r| r.partition == 0);
        assert!(
            partition0_report.is_some(),
            "should have report for partition 0"
        );
        assert_eq!(
            partition0_report.unwrap().publishers.len(),
            2,
            "should track 2 publishers"
        );

        c.shutdown_all().await;
    });
}

// ==========================================================================
// Phase 5: Orchestrator Restart & State Recovery Tests
// ==========================================================================

/// Orchestrator restart rediscovers all agents via liveliness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_orchestrator_restart_rediscovers_all_agents() {
    with_timeout!({
        let mut c = StateSyncCluster::new("orch_restart_disco", 3, 6, 1);

        // Start agents first
        for i in 0..3 {
            c.start_agent(i).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start orchestrator
        c.start_orchestrator_with_http().await;
        let seen = c.wait_for_online_count(3, Duration::from_secs(5)).await;
        assert_eq!(seen, 3, "should see 3 nodes before restart");

        // Stop orchestrator (agents keep running)
        c.stop_orchestrator().await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Restart orchestrator (with HTTP again)
        c.start_orchestrator_with_http().await;

        // Should rediscover all 3 agents
        let seen = c.wait_for_online_count(3, Duration::from_secs(5)).await;
        assert_eq!(seen, 3, "restarted orchestrator should rediscover 3 agents");

        // Verify via HTTP
        let http_nodes = c.http_get_cluster_nodes().await;
        let online_via_http = http_nodes
            .values()
            .filter(|v| v["online"].as_bool().unwrap_or(false))
            .count();
        assert_eq!(online_via_http, 3, "HTTP should show 3 online nodes");

        c.shutdown_all().await;
    });
}

/// Orchestrator restart preserves topic config (fjall persistence).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_orchestrator_restart_preserves_topic_config() {
    with_timeout!({
        let prefix = key_prefix("orch_persist");
        let port = free_port().await;
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        // Start orchestrator with HTTP
        let config = OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: data_dir.clone(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: Some(([127, 0, 0, 1], port).into()),
            auth_token: None,
            bootstrap_topics_from: None,
        };
        let mut orch = Orchestrator::new(&session, config).unwrap();
        orch.run().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = reqwest::Client::new();
        let base_url = format!("http://127.0.0.1:{port}");

        // Create 3 topics
        for i in 0..3 {
            let body = serde_json::json!({
                "name": format!("topic-{i}"),
                "num_partitions": 4,
                "replication_factor": 1
            });
            let resp = client
                .post(format!("{base_url}/api/v1/topics"))
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 201, "should create topic-{i}");
        }

        // Verify 3 topics exist
        let resp = client
            .get(format!("{base_url}/api/v1/topics"))
            .send()
            .await
            .unwrap();
        let topics: Vec<Value> = resp.json().await.unwrap();
        assert_eq!(topics.len(), 3, "should have 3 topics before restart");

        // Shutdown orchestrator
        orch.shutdown().await;
        session.close().await.unwrap();

        // Restart with same data_dir but new port
        let port2 = free_port().await;
        let session2 = zenoh::open(zenoh::Config::default()).await.unwrap();
        let config2 = OrchestratorConfig {
            key_prefix: prefix.clone(),
            data_dir: data_dir.clone(),
            lag_interval: Duration::from_secs(60),
            admin_prefix: None,
            http_bind: Some(([127, 0, 0, 1], port2).into()),
            auth_token: None,
            bootstrap_topics_from: None,
        };
        let mut orch2 = Orchestrator::new(&session2, config2).unwrap();
        orch2.run().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let base_url2 = format!("http://127.0.0.1:{port2}");

        // Verify all 3 topics persisted
        let resp = client
            .get(format!("{base_url2}/api/v1/topics"))
            .send()
            .await
            .unwrap();
        let topics: Vec<Value> = resp.json().await.unwrap();
        assert_eq!(
            topics.len(),
            3,
            "topics should persist across orchestrator restart"
        );

        // Verify individual topics
        for i in 0..3 {
            let resp = client
                .get(format!("{base_url2}/api/v1/topics/topic-{i}"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "topic-{i} should exist after restart");
        }

        orch2.shutdown().await;
        session2.close().await.unwrap();
    });
}

/// Orchestrator restart: lag monitor starts fresh (no stale pre-restart data).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_orchestrator_restart_lag_monitor_recovers() {
    with_timeout!({
        let mut c = StateSyncCluster::new("orch_lag_recover", 1, 4, 1);
        c.start_orchestrator().await;
        c.start_agent(0).await;
        c.wait_for_online_count(1, Duration::from_secs(5)).await;

        let pub_id = PublisherId::new();

        // Populate lag data
        c.publish_watermark(0, pub_id, 100).await;
        c.publish_offset("test-group", 0, pub_id, 50).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        {
            let orch = c.orchestrator.as_ref().unwrap();
            let lag = orch.get_group_lag("test-group").await;
            assert!(!lag.is_empty(), "should have lag before restart");
        }

        // Stop orchestrator (agent keeps running)
        c.stop_orchestrator().await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Restart orchestrator
        c.start_orchestrator().await;

        // Immediately after restart, lag should be empty (in-memory state was wiped)
        {
            let orch2 = c.orchestrator.as_ref().unwrap();
            let lag_fresh = orch2.get_group_lag("test-group").await;
            assert!(
                lag_fresh.is_empty(),
                "lag should be empty immediately after restart (in-memory state wiped). Got: {lag_fresh:?}"
            );
        }

        // Publish new watermark + offset → lag should resume
        c.publish_watermark(0, pub_id, 200).await;
        c.publish_offset("test-group", 0, pub_id, 100).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let lag_resumed = c
            .orchestrator
            .as_ref()
            .unwrap()
            .get_group_lag("test-group")
            .await;
        let total: u64 = lag_resumed.iter().map(|r| r.total).sum();
        assert_eq!(total, 100, "lag should resume: 200 - 100 = 100");

        c.shutdown_all().await;
    });
}

// ==========================================================================
// Phase 6: SSE Event Ordering & HTTP Consistency Tests
// ==========================================================================

/// SSE cluster events reflect agent lifecycle correctly.
/// Start → online event, stop → offline event, restart → online again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_sse_cluster_events_reflect_agent_lifecycle() {
    with_timeout!({
        let mut c = StateSyncCluster::new("sse_lifecycle", 3, 4, 1);
        c.start_orchestrator_with_http().await;

        let url = c.http_url().to_string();
        let client = reqwest::Client::new();

        // Start SSE connection
        let resp = client
            .get(format!("{url}/api/v1/stream/cluster"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/event-stream"), "should be SSE stream");

        // Drop SSE — we can't easily parse it in reqwest without streaming.
        // Instead, verify that after lifecycle events, HTTP state is consistent.
        drop(resp);

        // Start agent
        c.start_agent(0).await;
        let online = c
            .wait_for_node_online("node-0", Duration::from_secs(5))
            .await;
        assert!(online, "node-0 should be online");

        // Verify HTTP reflects online state
        let nodes = c.http_get_cluster_nodes().await;
        assert!(nodes.contains_key("node-0"), "HTTP should show node-0");
        assert_eq!(
            nodes["node-0"]["online"].as_bool(),
            Some(true),
            "HTTP should show node-0 as online"
        );

        // Stop agent
        c.stop_agent(0).await;
        let went_offline = c
            .wait_for_node_offline("node-0", Duration::from_secs(10))
            .await;
        assert!(went_offline, "node-0 should go offline");

        // Verify HTTP reflects offline state
        let nodes = c.http_get_cluster_nodes().await;
        assert!(nodes.contains_key("node-0"), "node-0 entry persists");
        assert_eq!(
            nodes["node-0"]["online"].as_bool(),
            Some(false),
            "HTTP should show node-0 as offline"
        );

        // Restart with same ID
        c.start_agent(0).await;
        let back_online = c
            .wait_for_node_online("node-0", Duration::from_secs(5))
            .await;
        assert!(back_online, "node-0 should be back online");

        // Final HTTP snapshot
        let nodes = c.http_get_cluster_nodes().await;
        assert_eq!(
            nodes["node-0"]["online"].as_bool(),
            Some(true),
            "HTTP should show node-0 online after restart"
        );

        // Should still be exactly 1 node entry (no phantom)
        assert_eq!(
            nodes.len(),
            1,
            "should have exactly 1 node entry (same ID restart). Got: {nodes:?}"
        );

        c.shutdown_all().await;
    });
}

/// HTTP cluster/nodes is consistent: REST snapshot agrees with ClusterView state.
/// Test with multiple agent starts/stops/restarts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_cluster_nodes_consistent_with_cluster_view() {
    with_timeout!({
        let mut c = StateSyncCluster::new("http_cv_consistent", 3, 6, 1);
        c.start_orchestrator_with_http().await;

        // Start all agents
        for i in 0..3 {
            c.start_agent(i).await;
        }
        c.wait_for_online_count(3, Duration::from_secs(5)).await;

        // Verify HTTP matches ClusterView
        let cv_nodes = c.get_all_nodes().await;
        let http_nodes = c.http_get_cluster_nodes().await;
        assert_eq!(
            cv_nodes.len(),
            http_nodes.len(),
            "HTTP and ClusterView should have same node count"
        );

        // Stop one agent
        c.stop_agent(1).await;
        c.wait_for_node_offline("node-1", Duration::from_secs(10))
            .await;

        // Both should agree
        let cv_nodes = c.get_all_nodes().await;
        let http_nodes = c.http_get_cluster_nodes().await;
        assert_eq!(cv_nodes.len(), http_nodes.len());

        let cv_online = cv_nodes.values().filter(|o| **o).count();
        let http_online = http_nodes
            .values()
            .filter(|v| v["online"].as_bool().unwrap_or(false))
            .count();
        assert_eq!(
            cv_online, http_online,
            "online counts should match: CV={cv_online}, HTTP={http_online}"
        );

        // Restart with new ID → phantom appears in both
        let new_id = c.start_agent_auto_id(1).await;
        c.wait_for_node_online(&new_id, Duration::from_secs(5))
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let cv_nodes = c.get_all_nodes().await;
        let http_nodes = c.http_get_cluster_nodes().await;
        assert_eq!(
            cv_nodes.len(),
            http_nodes.len(),
            "after phantom: HTTP and ClusterView should still agree. CV: {cv_nodes:?}, HTTP: {http_nodes:?}"
        );

        c.shutdown_all().await;
    });
}

/// Verify that ClusterView node count reported by HTTP /cluster/status matches
/// the actual /cluster/nodes endpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_http_status_consistent_with_nodes_endpoint() {
    with_timeout!({
        let mut c = StateSyncCluster::new("http_status_consistent", 3, 4, 1);
        c.start_orchestrator_with_http().await;

        for i in 0..3 {
            c.start_agent(i).await;
        }
        c.wait_for_online_count(3, Duration::from_secs(5)).await;

        let nodes = c.http_get_cluster_nodes().await;
        let status = c.http_get_cluster_status().await;

        let nodes_total = nodes.len() as u64;
        let nodes_online = nodes
            .values()
            .filter(|v| v["online"].as_bool().unwrap_or(false))
            .count() as u64;

        let status_total = status["total_nodes"].as_u64().unwrap_or(0);
        let status_online = status["online_nodes"].as_u64().unwrap_or(0);

        assert_eq!(
            nodes_total, status_total,
            "total_nodes should match nodes endpoint count"
        );
        assert_eq!(
            nodes_online, status_online,
            "online_nodes should match nodes endpoint online count"
        );

        // Now create a phantom and verify consistency
        c.stop_agent(2).await;
        c.wait_for_node_offline("node-2", Duration::from_secs(10))
            .await;
        let _new_id = c.start_agent_auto_id(2).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let nodes = c.http_get_cluster_nodes().await;
        let status = c.http_get_cluster_status().await;

        let nodes_total = nodes.len() as u64;
        let nodes_online = nodes
            .values()
            .filter(|v| v["online"].as_bool().unwrap_or(false))
            .count() as u64;

        let status_total = status["total_nodes"].as_u64().unwrap_or(0);
        let status_online = status["online_nodes"].as_u64().unwrap_or(0);

        assert_eq!(nodes_total, status_total);
        assert_eq!(nodes_online, status_online);

        eprintln!(
            "After phantom: total={nodes_total} (vs status {status_total}), online={nodes_online} (vs status {status_online})"
        );

        c.shutdown_all().await;
    });
}
