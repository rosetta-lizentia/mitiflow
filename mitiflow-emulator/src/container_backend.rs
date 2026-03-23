//! Container execution backend — spawns components via Docker/Podman CLI.

use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::backend::{ComponentExitStatus, ComponentHandle, ComponentSpec, ExecutionBackend};

/// Backend that spawns components as Docker/Podman containers.
pub struct ContainerBackend {
    /// Container runtime command (docker or podman).
    runtime: String,
    /// Docker image name.
    image: String,
}

impl ContainerBackend {
    pub fn new(runtime: impl Into<String>, image: impl Into<String>) -> Self {
        Self {
            runtime: runtime.into(),
            image: image.into(),
        }
    }

    /// Check if the image exists locally, build if not.
    pub async fn ensure_image(&self, dockerfile_dir: &str) -> crate::error::Result<()> {
        let status = Command::new(&self.runtime)
            .args(["image", "inspect", &self.image])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;

        if !status.success() {
            tracing::info!("Building {} image (first time)...", self.image);
            let build_status = Command::new(&self.runtime)
                .args([
                    "build",
                    "-t",
                    &self.image,
                    "-f",
                    &format!("{}/Dockerfile", dockerfile_dir),
                    ".",
                ])
                .status()
                .await?;

            if !build_status.success() {
                return Err(crate::error::EmulatorError::Container(
                    "failed to build container image".into(),
                ));
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExecutionBackend for ContainerBackend {
    async fn spawn(&self, spec: &ComponentSpec) -> crate::error::Result<Box<dyn ComponentHandle>> {
        let container_name = format!("mitiflow-emu-{}-{}", spec.name, spec.instance);

        // Build docker run command.
        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--name".to_string(),
            container_name.clone(),
            "--entrypoint".to_string(),
            spec.binary.clone(),
        ];

        // Add environment variables.
        for (key, val) in &spec.env {
            args.push("-e".to_string());
            args.push(format!("{}={}", key, val));
        }

        args.push(self.image.clone());

        let output = Command::new(&self.runtime)
            .args(&args)
            .output()
            .await
            .map_err(|e| {
                crate::error::EmulatorError::Container(format!("failed to start container: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(crate::error::EmulatorError::Container(format!(
                "docker run failed: {}",
                stderr.trim()
            )));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();

        // Start log streaming via docker logs -f.
        let mut log_cmd = Command::new(&self.runtime);
        log_cmd
            .args(["logs", "-f", &container_name])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut log_child = log_cmd.spawn().map_err(|e| {
            crate::error::EmulatorError::Container(format!("failed to stream logs: {}", e))
        })?;

        let stdout = log_child.stdout.take().map(|s| BufReader::new(s).lines());
        let stderr = log_child.stderr.take().map(|s| BufReader::new(s).lines());

        let handle = ContainerHandle {
            id: format!("{}:{}", spec.name, spec.instance),
            container_name,
            container_id,
            runtime: self.runtime.clone(),
            image: self.image.clone(),
            spec: spec.clone(),
            log_child: Some(log_child),
            stdout,
            stderr,
        };

        Ok(Box::new(handle))
    }
}

/// Handle to a Docker/Podman container.
#[allow(dead_code)]
pub struct ContainerHandle {
    id: String,
    container_name: String,
    container_id: String,
    runtime: String,
    image: String,
    spec: ComponentSpec,
    log_child: Option<tokio::process::Child>,
    stdout: Option<tokio::io::Lines<BufReader<tokio::process::ChildStdout>>>,
    stderr: Option<tokio::io::Lines<BufReader<tokio::process::ChildStderr>>>,
}

impl ContainerHandle {
    async fn docker_cmd(&self, args: &[&str]) -> crate::error::Result<()> {
        let status = Command::new(&self.runtime)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;

        if !status.success() {
            return Err(crate::error::EmulatorError::Container(format!(
                "{} {:?} failed for {}",
                self.runtime,
                args,
                self.container_name
            )));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ComponentHandle for ContainerHandle {
    fn id(&self) -> &str {
        &self.id
    }

    async fn stop(&self) -> crate::error::Result<()> {
        self.docker_cmd(&["stop", "-t", "5", &self.container_name])
            .await
    }

    async fn kill(&self) -> crate::error::Result<()> {
        self.docker_cmd(&["kill", &self.container_name]).await
    }

    async fn pause(&self) -> crate::error::Result<()> {
        self.docker_cmd(&["pause", &self.container_name]).await
    }

    async fn resume(&self) -> crate::error::Result<()> {
        self.docker_cmd(&["unpause", &self.container_name]).await
    }

    async fn wait(&mut self) -> crate::error::Result<ComponentExitStatus> {
        let output = Command::new(&self.runtime)
            .args(["wait", &self.container_name])
            .output()
            .await?;

        let code_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let code = code_str.parse::<i32>().ok();

        // Clean up the container.
        let _ = Command::new(&self.runtime)
            .args(["rm", "-f", &self.container_name])
            .output()
            .await;

        // Stop log streaming.
        if let Some(ref mut child) = self.log_child {
            let _ = child.kill().await;
        }

        Ok(ComponentExitStatus { code })
    }

    async fn restart(&mut self) -> crate::error::Result<()> {
        self.docker_cmd(&["restart", &self.container_name]).await
    }

    fn take_stdout(
        &mut self,
    ) -> Option<tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>> {
        self.stdout.take()
    }

    fn take_stderr(
        &mut self,
    ) -> Option<tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStderr>>> {
        self.stderr.take()
    }
}
