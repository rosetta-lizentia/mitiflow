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
                LogFormat::Json => {
                    serde_json::to_string(&log_line).unwrap_or_default() + "\n"
                }
                LogFormat::Text => {
                    format!(
                        "[{}:{}] {}\n",
                        log_line.component,
                        log_line.instance,
                        log_line.line
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
