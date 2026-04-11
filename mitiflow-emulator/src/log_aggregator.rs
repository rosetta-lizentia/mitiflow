//! Log aggregation — line-buffered multiplexer with colored prefixes.

use tokio::sync::mpsc;

use crate::config::{LogFormat, LogMode, LoggingConfig};

/// A single aggregated log line.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LogLine {
    pub ts: String,
    pub component: String,
    pub instance: usize,
    pub line: String,
}

/// Aggregates log output from multiple components into a single ordered stream.
pub struct LogAggregator {
    #[allow(dead_code)]
    config: LoggingConfig,
    tx: mpsc::UnboundedSender<LogLine>,
    writer_task: Option<tokio::task::JoinHandle<()>>,
}

impl LogAggregator {
    /// Create a new log aggregator.
    pub fn new(config: &LoggingConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let config_clone = config.clone();

        let writer_task = tokio::spawn(async move {
            Self::writer_loop(rx, &config_clone).await;
        });

        Self {
            config: config.clone(),
            tx,
            writer_task: Some(writer_task),
        }
    }

    /// Add a stdout source from a component.
    pub fn add_stdout(
        &self,
        name: &str,
        instance: usize,
        mut lines: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
    ) {
        let tx = self.tx.clone();
        let component = name.to_string();
        tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                let log_line = LogLine {
                    ts: chrono::Utc::now().to_rfc3339(),
                    component: component.clone(),
                    instance,
                    line,
                };
                if tx.send(log_line).is_err() {
                    break;
                }
            }
        });
    }

    /// Add a stderr source from a component.
    pub fn add_stderr(
        &self,
        name: &str,
        instance: usize,
        mut lines: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStderr>>,
    ) {
        let tx = self.tx.clone();
        let component = name.to_string();
        tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                let log_line = LogLine {
                    ts: chrono::Utc::now().to_rfc3339(),
                    component: component.clone(),
                    instance,
                    line,
                };
                if tx.send(log_line).is_err() {
                    break;
                }
            }
        });
    }

    /// Shutdown the aggregator — flush remaining lines.
    pub async fn shutdown(mut self) {
        drop(self.tx);
        if let Some(task) = self.writer_task.take() {
            let _ = task.await;
        }
    }

    async fn writer_loop(mut rx: mpsc::UnboundedReceiver<LogLine>, config: &LoggingConfig) {
        use tokio::io::AsyncWriteExt;

        let mut file = match config.mode {
            LogMode::File | LogMode::Both => {
                let dir = &config.directory;
                let _ = tokio::fs::create_dir_all(dir).await;
                let path = dir.join("emulator.log");
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .await
                    .ok()
            }
            LogMode::Stdout => None,
        };

        while let Some(log_line) = rx.recv().await {
            let formatted = match config.format {
                LogFormat::Json => serde_json::to_string(&log_line).unwrap_or_default() + "\n",
                LogFormat::Text => {
                    format!(
                        "[{}:{}] {}\n",
                        log_line.component, log_line.instance, log_line.line
                    )
                }
            };

            match config.mode {
                LogMode::Stdout => {
                    print!("{}", formatted);
                }
                LogMode::File => {
                    if let Some(ref mut f) = file {
                        let _ = f.write_all(formatted.as_bytes()).await;
                    }
                }
                LogMode::Both => {
                    print!("{}", formatted);
                    if let Some(ref mut f) = file {
                        let _ = f.write_all(formatted.as_bytes()).await;
                    }
                }
            }
        }

        // Final flush.
        if let Some(ref mut f) = file {
            let _ = f.flush().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::process::Stdio;

    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    use super::*;
    use crate::config::{LogFormat, LogLevel, LogMode, LoggingConfig};

    fn test_logging_config() -> LoggingConfig {
        LoggingConfig {
            level: LogLevel::Info,
            mode: LogMode::Stdout,
            format: LogFormat::Text,
            directory: std::path::PathBuf::from("/tmp"),
            per_component: false,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_add_stdout_streams_dynamically() {
        let config = test_logging_config();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<LogLine>();

        let agg = LogAggregator {
            config,
            tx,
            writer_task: None,
        };

        let mut child1 = Command::new("echo")
            .arg("hello-from-first")
            .stdout(Stdio::piped())
            .spawn()
            .expect("echo must be available");
        let stdout1 = child1.stdout.take().unwrap();
        agg.add_stdout("comp-a", 0, BufReader::new(stdout1).lines());

        let line1 = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("should receive within timeout")
            .expect("channel should not be closed");
        assert_eq!(line1.component, "comp-a");
        assert_eq!(line1.instance, 0);
        assert_eq!(line1.line, "hello-from-first");

        let mut child2 = Command::new("echo")
            .arg("hello-from-second")
            .stdout(Stdio::piped())
            .spawn()
            .expect("echo must be available");
        let stdout2 = child2.stdout.take().unwrap();
        agg.add_stdout("comp-a", 0, BufReader::new(stdout2).lines());

        let line2 = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("should receive within timeout")
            .expect("channel should not be closed");
        assert_eq!(line2.component, "comp-a");
        assert_eq!(line2.instance, 0);
        assert_eq!(line2.line, "hello-from-second");

        let _ = child1.wait().await;
        let _ = child2.wait().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_log_rewire_after_restart() {
        let config = test_logging_config();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<LogLine>();

        let agg = LogAggregator {
            config,
            tx,
            writer_task: None,
        };

        let mut initial = Command::new("echo")
            .arg("initial-run")
            .stdout(Stdio::piped())
            .spawn()
            .expect("echo must be available");
        let stdout_initial = initial.stdout.take().unwrap();
        agg.add_stdout("restarted-comp", 0, BufReader::new(stdout_initial).lines());

        let mut other = Command::new("echo")
            .arg("other-comp-line")
            .stdout(Stdio::piped())
            .spawn()
            .expect("echo must be available");
        let stdout_other = other.stdout.take().unwrap();
        agg.add_stdout("other-comp", 1, BufReader::new(stdout_other).lines());

        let mut received = Vec::new();
        for _ in 0..2 {
            let line = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .expect("should receive line within timeout")
                .expect("channel should not be closed");
            received.push(line);
        }

        let initial_line = received
            .iter()
            .find(|l| l.component == "restarted-comp" && l.line == "initial-run");
        assert!(
            initial_line.is_some(),
            "should have received initial-run from restarted-comp"
        );
        assert_eq!(initial_line.unwrap().instance, 0);

        let other_line = received
            .iter()
            .find(|l| l.component == "other-comp" && l.line == "other-comp-line");
        assert!(
            other_line.is_some(),
            "other-comp should not be affected by rewiring"
        );
        assert_eq!(other_line.unwrap().instance, 1);

        let _ = initial.wait().await;

        let mut restarted = Command::new("echo")
            .arg("after-restart")
            .stdout(Stdio::piped())
            .spawn()
            .expect("echo must be available");
        let stdout_restarted = restarted.stdout.take().unwrap();
        agg.add_stdout(
            "restarted-comp",
            0,
            BufReader::new(stdout_restarted).lines(),
        );

        let restarted_line = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("should receive after-restart line within timeout")
            .expect("channel should not be closed");
        assert_eq!(restarted_line.component, "restarted-comp");
        assert_eq!(restarted_line.instance, 0);
        assert_eq!(restarted_line.line, "after-restart");

        let _ = restarted.wait().await;
        let _ = other.wait().await;
    }
}
