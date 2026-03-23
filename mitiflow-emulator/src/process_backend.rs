//! Process execution backend — spawns components as OS processes.

use std::process::Stdio;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use crate::backend::{ComponentExitStatus, ComponentHandle, ComponentSpec, ExecutionBackend};

/// Backend that spawns components as OS processes.
pub struct ProcessBackend;

#[async_trait::async_trait]
impl ExecutionBackend for ProcessBackend {
    async fn spawn(&self, spec: &ComponentSpec) -> crate::error::Result<Box<dyn ComponentHandle>> {
        let handle = ProcessHandle::spawn(spec).await?;
        Ok(Box::new(handle))
    }
}

/// Handle to a spawned OS process.
pub struct ProcessHandle {
    id: String,
    spec: ComponentSpec,
    child: Option<Child>,
    stdout: Option<tokio::io::Lines<BufReader<tokio::process::ChildStdout>>>,
    stderr: Option<tokio::io::Lines<BufReader<tokio::process::ChildStderr>>>,
}

impl ProcessHandle {
    async fn spawn(spec: &ComponentSpec) -> crate::error::Result<Self> {
        let mut cmd = Command::new(&spec.binary);
        cmd.envs(&spec.env)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        if let Some(ref wd) = spec.work_dir {
            cmd.current_dir(wd);
        }

        let mut child = cmd.spawn().map_err(|e| {
            crate::error::EmulatorError::Process(format!("failed to spawn {}: {}", spec.binary, e))
        })?;

        let stdout = child.stdout.take().map(|s| BufReader::new(s).lines());
        let stderr = child.stderr.take().map(|s| BufReader::new(s).lines());

        let id = format!("{}:{}", spec.name, spec.instance);
        Ok(Self {
            id,
            spec: spec.clone(),
            child: Some(child),
            stdout,
            stderr,
        })
    }

    fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|c| c.id())
    }

    fn send_signal(&self, sig: Signal) -> crate::error::Result<()> {
        if let Some(pid) = self.pid() {
            signal::kill(Pid::from_raw(pid as i32), sig).map_err(|e| {
                crate::error::EmulatorError::Process(format!(
                    "failed to send {:?} to {}: {}",
                    sig, self.id, e
                ))
            })
        } else {
            Ok(()) // Process already exited.
        }
    }
}

#[async_trait::async_trait]
impl ComponentHandle for ProcessHandle {
    fn id(&self) -> &str {
        &self.id
    }

    async fn stop(&self) -> crate::error::Result<()> {
        self.send_signal(Signal::SIGTERM)
    }

    async fn kill(&self) -> crate::error::Result<()> {
        self.send_signal(Signal::SIGKILL)
    }

    async fn pause(&self) -> crate::error::Result<()> {
        self.send_signal(Signal::SIGSTOP)
    }

    async fn resume(&self) -> crate::error::Result<()> {
        self.send_signal(Signal::SIGCONT)
    }

    async fn wait(&mut self) -> crate::error::Result<ComponentExitStatus> {
        if let Some(ref mut child) = self.child {
            let status = child.wait().await?;
            Ok(ComponentExitStatus {
                code: status.code(),
            })
        } else {
            Ok(ComponentExitStatus { code: None })
        }
    }

    async fn restart(&mut self) -> crate::error::Result<()> {
        // Stop existing process.
        if self.child.is_some() {
            let _ = self.stop().await;
            // Give it a moment, then force kill.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if self.pid().is_some() {
                let _ = self.kill().await;
            }
            if let Some(ref mut child) = self.child {
                let _ = child.wait().await;
            }
        }

        // Respawn.
        let new = ProcessHandle::spawn(&self.spec).await?;
        self.child = new.child;
        self.stdout = new.stdout;
        self.stderr = new.stderr;
        Ok(())
    }

    fn take_stdout(&mut self) -> Option<tokio::io::Lines<BufReader<tokio::process::ChildStdout>>> {
        self.stdout.take()
    }

    fn take_stderr(&mut self) -> Option<tokio::io::Lines<BufReader<tokio::process::ChildStderr>>> {
        self.stderr.take()
    }
}
