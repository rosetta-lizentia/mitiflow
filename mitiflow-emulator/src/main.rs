//! CLI entry point for mitiflow-emulator.
//!
//! Subcommands:
//!   run      — parse topology YAML, validate, spawn components
//!   validate — parse + validate only, report result

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use mitiflow_emulator::config::TopologyConfig;
use mitiflow_emulator::validation::validate;
use mitiflow_emulator::process_backend::ProcessBackend;
use mitiflow_emulator::container_backend::ContainerBackend;
use mitiflow_emulator::supervisor::Supervisor;

#[derive(Parser)]
#[command(name = "mitiflow-emulator", about = "YAML-driven topology runner for mitiflow")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a topology from a YAML file.
    Run {
        /// Path to the topology YAML file.
        topology: PathBuf,

        /// Run duration in seconds (0 = indefinite).
        #[arg(long, default_value_t = 0)]
        duration: u64,

        /// Enable chaos even if disabled in YAML.
        #[arg(long, conflicts_with = "no_chaos")]
        chaos: bool,

        /// Disable chaos even if enabled in YAML.
        #[arg(long, conflicts_with = "chaos")]
        no_chaos: bool,

        /// Override log level (trace, debug, info, warn, error).
        #[arg(long)]
        log_level: Option<String>,

        /// Print spawn plan without starting processes.
        #[arg(long)]
        dry_run: bool,

        /// Container image name (enables container backend).
        #[arg(long)]
        container_image: Option<String>,

        /// Container runtime (docker or podman).
        #[arg(long, default_value = "docker")]
        container_runtime: String,
    },

    /// Validate a topology YAML file without running it.
    Validate {
        /// Path to the topology YAML file.
        topology: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            topology,
            duration,
            chaos,
            no_chaos,
            log_level,
            dry_run,
            container_image,
            container_runtime,
        } => {
            // Initialize tracing.
            let filter = log_level
                .as_deref()
                .unwrap_or("info");
            tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::try_new(filter)?)
                .init();

            let config = TopologyConfig::from_file(&topology)?;

            let process_backend = Box::new(ProcessBackend);

            let container_backend = container_image.map(|image| {
                Box::new(ContainerBackend::new(container_runtime, image))
                    as Box<dyn mitiflow_emulator::backend::ExecutionBackend>
            });

            let chaos_override = if chaos {
                Some(true)
            } else if no_chaos {
                Some(false)
            } else {
                None
            };

            let mut supervisor = Supervisor::new(config, process_backend, container_backend);

            if duration > 0 {
                supervisor = supervisor.with_duration(Duration::from_secs(duration));
            }

            supervisor = supervisor
                .with_dry_run(dry_run)
                .with_chaos_override(chaos_override);

            supervisor.run().await?;
        }

        Commands::Validate { topology } => {
            tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::try_new("warn")?)
                .init();

            let config = TopologyConfig::from_file(&topology)?;
            match validate(&config) {
                Ok(result) => {
                    if result.warnings.is_empty() {
                        println!("Topology is valid.");
                    } else {
                        println!("Topology is valid with {} warning(s):", result.warnings.len());
                        for w in &result.warnings {
                            println!("  {}", w);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Validation failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
