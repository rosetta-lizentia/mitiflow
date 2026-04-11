//! Network fault injection primitives for emulator backends.

use std::future::Future;
use std::pin::Pin;

use nix::unistd;
use tokio::process::Command;

use crate::error::{EmulatorError, Result};

#[async_trait::async_trait]
pub trait NetworkFaultInjector: Send + Sync {
    async fn add_latency(
        &self,
        target: &str,
        delay_ms: u32,
        loss_percent: f64,
    ) -> Result<FaultGuard>;
    async fn partition(&self, target: &str, from: &[String]) -> Result<FaultGuard>;
    async fn cleanup(&self) -> Result<()>;
}

type CleanupFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type CleanupFn = Box<dyn FnOnce() -> CleanupFuture + Send>;

/// Check if the current process has CAP_NET_ADMIN capability.
/// Returns `true` if the process likely has network administration privileges.
pub fn has_net_admin_capability() -> bool {
    // Root always has all capabilities
    if unistd::geteuid().is_root() {
        return true;
    }

    // Check /proc/self/status for CapEff (effective capabilities)
    // CAP_NET_ADMIN is bit 12
    match std::fs::read_to_string("/proc/self/status") {
        Ok(status) => parse_cap_eff(&status, 12),
        Err(_) => false, // Can't determine — assume unprivileged
    }
}

/// Parse the CapEff line from /proc/self/status and check if a specific capability bit is set.
fn parse_cap_eff(status: &str, cap_bit: u32) -> bool {
    for line in status.lines() {
        if let Some(hex_str) = line.strip_prefix("CapEff:\t") {
            let hex_str = hex_str.trim();
            if let Ok(caps) = u64::from_str_radix(hex_str, 16) {
                return caps & (1u64 << cap_bit) != 0;
            }
        }
    }
    false
}

fn no_op_guard() -> FaultGuard {
    FaultGuard::new(|| Box::pin(async { Ok(()) }))
}

pub struct FaultGuard {
    cleanup_fn: Option<CleanupFn>,
}

impl FaultGuard {
    pub fn new(cleanup: impl FnOnce() -> CleanupFuture + Send + 'static) -> Self {
        Self {
            cleanup_fn: Some(Box::new(cleanup)),
        }
    }
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        if let Some(cleanup_fn) = self.cleanup_fn.take() {
            // Spawn a dedicated thread to ensure cleanup runs even if the
            // tokio runtime is shutting down. A minimal runtime is created
            // inside the thread to drive the async cleanup.
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                let result = match rt {
                    Ok(rt) => rt.block_on(cleanup_fn()),
                    Err(_) => {
                        tracing::warn!("failed to create cleanup runtime for network fault");
                        return;
                    }
                };
                if let Err(err) = result {
                    tracing::warn!(error = %err, "failed to clean up network fault");
                }
            });
        }
    }
}

pub struct ContainerNetworkFaultInjector {
    runtime: String,
}

impl ContainerNetworkFaultInjector {
    pub fn new() -> Self {
        Self {
            runtime: "docker".to_string(),
        }
    }

    async fn run_command(&self, args: &[&str], op: &str) -> Result<String> {
        let output = Command::new(&self.runtime)
            .args(args)
            .output()
            .await
            .map_err(|err| EmulatorError::NetworkFault(format!("{}: {}", op, err)))?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            Err(EmulatorError::NetworkFault(format!("{}: {}", op, stderr)))
        }
    }

    async fn discover_network(&self, target: &str) -> Result<String> {
        let inspect_output = self
            .run_command(
                &[
                    "inspect",
                    target,
                    "--format",
                    "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
                ],
                "failed to inspect container networks",
            )
            .await?;

        inspect_output
            .split_whitespace()
            .next()
            .map(str::to_string)
            .ok_or_else(|| {
                EmulatorError::NetworkFault(format!(
                    "failed to discover network for container {}",
                    target
                ))
            })
    }
}

impl Default for ContainerNetworkFaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl NetworkFaultInjector for ContainerNetworkFaultInjector {
    async fn add_latency(
        &self,
        target: &str,
        delay_ms: u32,
        loss_percent: f64,
    ) -> Result<FaultGuard> {
        if !has_net_admin_capability() {
            tracing::warn!(
                target,
                "skipping network latency fault injection: missing CAP_NET_ADMIN"
            );
            return Ok(no_op_guard());
        }

        self.run_command(
            &[
                "exec",
                target,
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "root",
                "netem",
                "delay",
                &format!("{}ms", delay_ms),
                "loss",
                &format!("{}%", loss_percent),
            ],
            "failed to add latency fault",
        )
        .await?;

        let runtime = self.runtime.clone();
        let target = target.to_string();
        Ok(FaultGuard::new(move || {
            Box::pin(async move {
                let output = Command::new(&runtime)
                    .args([
                        "exec", &target, "tc", "qdisc", "del", "dev", "eth0", "root", "netem",
                    ])
                    .output()
                    .await
                    .map_err(|err| {
                        EmulatorError::NetworkFault(format!(
                            "failed to remove latency fault for {}: {}",
                            target, err
                        ))
                    })?;

                if output.status.success() {
                    Ok(())
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                    Err(EmulatorError::NetworkFault(format!(
                        "failed to remove latency fault for {}: {}",
                        target, stderr
                    )))
                }
            })
        }))
    }

    async fn partition(&self, target: &str, _from: &[String]) -> Result<FaultGuard> {
        if !has_net_admin_capability() {
            tracing::warn!(
                target,
                "skipping network partition fault injection: missing CAP_NET_ADMIN"
            );
            return Ok(no_op_guard());
        }

        let network = self.discover_network(target).await?;

        self.run_command(
            &["network", "disconnect", &network, target],
            "failed to disconnect container from network",
        )
        .await?;

        let runtime = self.runtime.clone();
        let target = target.to_string();
        let network = network.to_string();
        Ok(FaultGuard::new(move || {
            Box::pin(async move {
                let output = Command::new(&runtime)
                    .args(["network", "connect", &network, &target])
                    .output()
                    .await
                    .map_err(|err| {
                        EmulatorError::NetworkFault(format!(
                            "failed to reconnect container {} to {}: {}",
                            target, network, err
                        ))
                    })?;

                if output.status.success() {
                    Ok(())
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                    Err(EmulatorError::NetworkFault(format!(
                        "failed to reconnect container {} to {}: {}",
                        target, network, stderr
                    )))
                }
            })
        }))
    }

    async fn cleanup(&self) -> Result<()> {
        Ok(())
    }
}

pub struct ProcessNetworkFaultInjector;

#[async_trait::async_trait]
impl NetworkFaultInjector for ProcessNetworkFaultInjector {
    async fn add_latency(
        &self,
        _target: &str,
        _delay_ms: u32,
        _loss_percent: f64,
    ) -> Result<FaultGuard> {
        tracing::warn!(
            "Network fault injection not supported in process mode. Use container isolation for network faults."
        );
        Ok(FaultGuard::new(|| Box::pin(async { Ok(()) })))
    }

    async fn partition(&self, _target: &str, _from: &[String]) -> Result<FaultGuard> {
        tracing::warn!(
            "Network fault injection not supported in process mode. Use container isolation for network faults."
        );
        Ok(FaultGuard::new(|| Box::pin(async { Ok(()) })))
    }

    async fn cleanup(&self) -> Result<()> {
        Ok(())
    }
}

pub fn create_fault_injector(container_mode: bool) -> Box<dyn NetworkFaultInjector> {
    if container_mode {
        Box::new(ContainerNetworkFaultInjector::new())
    } else {
        Box::new(ProcessNetworkFaultInjector)
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};

    use super::*;

    #[test]
    fn privilege_detection_does_not_panic() {
        let _ = has_net_admin_capability();
    }

    #[test]
    fn parse_cap_eff_with_admin() {
        let status = "Name:\ttest\nCapEff:\t0000000000001000\n";
        assert!(parse_cap_eff(status, 12));
    }

    #[test]
    fn parse_cap_eff_without_admin() {
        let status = "Name:\ttest\nCapEff:\t0000000000000001\n";
        assert!(!parse_cap_eff(status, 12));
    }

    #[test]
    fn parse_cap_eff_missing_line() {
        let status = "Name:\ttest\nUid:\t1000\t1000\t1000\t1000\n";
        assert!(!parse_cap_eff(status, 12));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fault_guard_runs_cleanup_on_drop() {
        let (tx, rx) = oneshot::channel();
        let guard = FaultGuard::new(move || {
            Box::pin(async move {
                let _ = tx.send(());
                Ok(())
            })
        });

        drop(guard);

        let got_signal = timeout(Duration::from_secs(1), rx).await;
        assert!(got_signal.is_ok());
        assert!(got_signal.expect("timeout waiting for cleanup").is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_injector_operations_are_noop_and_non_failing() {
        let injector = ProcessNetworkFaultInjector;
        let _guard = injector
            .add_latency("any", 100, 0.5)
            .await
            .expect("process-mode add_latency should be no-op");
        let _guard = injector
            .partition("any", &[])
            .await
            .expect("process-mode partition should be no-op");
        injector.cleanup().await.expect("cleanup should be no-op");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn factory_returns_process_implementation_for_non_container_mode() {
        let injector = create_fault_injector(false);
        let _guard = injector
            .add_latency("any", 50, 0.0)
            .await
            .expect("non-container factory should return process no-op injector");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires Docker container runtime and daemon"]
    async fn container_add_latency_command_path() {
        let injector = ContainerNetworkFaultInjector::new();
        let result = injector
            .add_latency("mitiflow-emu-missing-0", 100, 1.0)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires Docker container runtime and daemon"]
    async fn container_partition_command_path() {
        let injector = ContainerNetworkFaultInjector::new();
        let result = injector.partition("mitiflow-emu-missing-0", &[]).await;
        assert!(result.is_err());
    }
}
