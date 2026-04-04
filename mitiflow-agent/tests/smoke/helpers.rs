//! Helpers for multi-process smoke tests.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};

/// Path to the built agent binary.
fn agent_binary() -> PathBuf {
    // cargo test builds into target/debug
    let mut path = std::env::current_exe()
        .unwrap()
        .parent() // deps/
        .unwrap()
        .parent() // debug/
        .unwrap()
        .to_path_buf();
    path.push("mitiflow-agent");
    path
}

/// A managed agent subprocess.
pub struct AgentProcess {
    pub child: Child,
    pub node_id: String,
    #[allow(dead_code)]
    pub data_dir: tempfile::TempDir,
}

impl AgentProcess {
    /// Spawn an agent binary with environment-based configuration.
    pub fn spawn(
        node_id: &str,
        key_prefix: &str,
        num_partitions: u32,
        replication_factor: u32,
    ) -> Self {
        let data_dir = tempfile::tempdir().unwrap();
        let child = Command::new(agent_binary())
            .env("MITIFLOW_NODE_ID", node_id)
            .env("MITIFLOW_KEY_PREFIX", key_prefix)
            .env("MITIFLOW_DATA_DIR", data_dir.path())
            .env("MITIFLOW_NUM_PARTITIONS", num_partitions.to_string())
            .env(
                "MITIFLOW_REPLICATION_FACTOR",
                replication_factor.to_string(),
            )
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("failed to spawn mitiflow-agent binary");

        Self {
            child,
            node_id: node_id.to_string(),
            data_dir,
        }
    }

    /// Send SIGTERM for graceful shutdown.
    pub fn terminate(&self) {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;
        if let Some(pid) = self.child.id() {
            let _ = signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
        }
    }

    /// Kill the process immediately (SIGKILL).
    pub fn kill(&mut self) {
        let _ = self.child.start_kill();
    }

    /// Wait for the process to exit, with a timeout.
    pub async fn wait_exit(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(Ok(status)) => Some(status),
            _ => None,
        }
    }
}

/// A cluster of agent subprocesses.
pub struct SmokeCluster {
    pub key_prefix: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub agents: Vec<AgentProcess>,
}

impl SmokeCluster {
    pub fn new(test_name: &str, num_partitions: u32, rf: u32) -> Self {
        Self {
            key_prefix: format!("smoke/{test_name}"),
            num_partitions,
            replication_factor: rf,
            agents: Vec::new(),
        }
    }

    /// Spawn and register a new agent process.
    pub fn spawn_agent(&mut self, node_id: &str) {
        let agent = AgentProcess::spawn(
            node_id,
            &self.key_prefix,
            self.num_partitions,
            self.replication_factor,
        );
        self.agents.push(agent);
    }

    /// Wait for agents to discover each other by subscribing to their health keys.
    pub async fn wait_healthy(&self, session: &zenoh::Session, timeout: Duration) -> bool {
        let start = tokio::time::Instant::now();
        let mut healthy: HashMap<String, bool> = HashMap::new();
        for a in &self.agents {
            healthy.insert(a.node_id.clone(), false);
        }

        // Subscribe to health for all agents via wildcard.
        let health_pattern = format!("{}/_cluster/health/*", self.key_prefix);
        let subscriber = session.declare_subscriber(&health_pattern).await.unwrap();

        while start.elapsed() < timeout {
            tokio::select! {
                sample = subscriber.recv_async() => {
                    if let Ok(sample) = sample {
                        let key = sample.key_expr().as_str();
                        if let Some(node_id) = key.rsplit('/').next() {
                            healthy.insert(node_id.to_string(), true);
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
            if healthy.values().all(|&v| v) {
                return true;
            }
        }
        false
    }

    /// Check which agents are alive via liveliness tokens.
    pub async fn get_live_agents(&self, session: &zenoh::Session) -> Vec<String> {
        let pattern = format!("{}/_agents/*", self.key_prefix);
        let mut live = Vec::new();
        if let Ok(replies) = session.liveliness().get(&pattern).await {
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result() {
                    let key = sample.key_expr().as_str();
                    if let Some(node_id) = key.rsplit('/').next()
                        && !node_id.contains('/')
                    {
                        live.push(node_id.to_string());
                    }
                }
            }
        }
        live
    }

    /// Kill all agent processes.
    pub async fn kill_all(&mut self) {
        for a in &mut self.agents {
            a.kill();
            let _ = a.wait_exit(Duration::from_secs(3)).await;
        }
    }
}
