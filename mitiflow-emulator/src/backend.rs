//! Execution backend traits for process and container spawning.

use std::collections::HashMap;
use std::path::PathBuf;

/// Specification for spawning a component.
#[derive(Debug, Clone)]
pub struct ComponentSpec {
    /// Component name.
    pub name: String,
    /// Instance index (0-based).
    pub instance: usize,
    /// Binary path (for process backend) or role name (for container backend).
    pub binary: String,
    /// Environment variables to set.
    pub env: HashMap<String, String>,
    /// Working directory.
    pub work_dir: Option<PathBuf>,
}

/// Exit status of a component.
#[derive(Debug, Clone, Copy)]
pub struct ComponentExitStatus {
    pub code: Option<i32>,
}

impl ComponentExitStatus {
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }
}

/// Handle to a spawned component for lifecycle control.
#[async_trait::async_trait]
pub trait ComponentHandle: Send + Sync {
    /// Component identifier.
    fn id(&self) -> &str;

    /// Send graceful stop signal (SIGTERM / docker stop).
    async fn stop(&self) -> crate::error::Result<()>;

    /// Force kill (SIGKILL / docker kill).
    async fn kill(&self) -> crate::error::Result<()>;

    /// Pause (SIGSTOP / docker pause).
    async fn pause(&self) -> crate::error::Result<()>;

    /// Resume (SIGCONT / docker unpause).
    async fn resume(&self) -> crate::error::Result<()>;

    /// Wait for the process/container to exit.
    async fn wait(&mut self) -> crate::error::Result<ComponentExitStatus>;

    /// Restart with the same config.
    async fn restart(&mut self) -> crate::error::Result<()>;

    /// Take the stdout reader (can only be called once).
    fn take_stdout(
        &mut self,
    ) -> Option<tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>>;

    /// Take the stderr reader (can only be called once).
    fn take_stderr(
        &mut self,
    ) -> Option<tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStderr>>>;
}

/// Backend for spawning components.
#[async_trait::async_trait]
pub trait ExecutionBackend: Send + Sync {
    /// Spawn a component. Returns a handle for lifecycle control.
    async fn spawn(&self, spec: &ComponentSpec) -> crate::error::Result<Box<dyn ComponentHandle>>;
}
