//! mitiflow-ctl — CLI for cluster management via Zenoh queryable endpoints.

use std::time::Duration;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "mitiflow-ctl", about = "Mitiflow cluster management CLI")]
struct Cli {
    /// Zenoh key prefix (default: "mitiflow")
    #[arg(long, default_value = "mitiflow")]
    prefix: String,

    /// Admin API prefix (default: "{prefix}/_admin")
    #[arg(long)]
    admin_prefix: Option<String>,

    /// Query timeout in seconds
    #[arg(long, default_value = "5")]
    timeout: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Topic management
    Topics {
        #[command(subcommand)]
        action: TopicActions,
    },
    /// Cluster management
    Cluster {
        #[command(subcommand)]
        action: ClusterActions,
    },
}

#[derive(Subcommand)]
enum TopicActions {
    /// List all topics
    List,
    /// Get a specific topic
    Get {
        /// Topic name
        name: String,
    },
}

#[derive(Subcommand)]
enum ClusterActions {
    /// Show all nodes with health and assignment counts
    Nodes,
    /// Show the full partition assignment table
    Assignments,
    /// Drain a node (move all partitions off it)
    Drain {
        /// Node ID to drain
        node_id: String,
    },
    /// Undrain a node (remove drain overrides)
    Undrain {
        /// Node ID to undrain
        node_id: String,
    },
    /// Show current override table
    Overrides,
    /// Show cluster-wide health summary
    Status,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    let admin_prefix = cli
        .admin_prefix
        .unwrap_or_else(|| format!("{}/_admin", cli.prefix));
    let timeout = Duration::from_secs(cli.timeout);

    let session = zenoh::open(zenoh::Config::default()).await?;

    let route = match &cli.command {
        Commands::Topics { action } => match action {
            TopicActions::List => "topics".to_string(),
            TopicActions::Get { name } => format!("topics/{name}"),
        },
        Commands::Cluster { action } => match action {
            ClusterActions::Nodes => "cluster/nodes".to_string(),
            ClusterActions::Assignments => "cluster/assignments".to_string(),
            ClusterActions::Drain { node_id } => format!("cluster/drain/{node_id}"),
            ClusterActions::Undrain { node_id } => format!("cluster/undrain/{node_id}"),
            ClusterActions::Overrides => "cluster/overrides".to_string(),
            ClusterActions::Status => "cluster/status".to_string(),
        },
    };

    let query_key = format!("{admin_prefix}/{route}");
    let replies = session.get(&query_key).timeout(timeout).await?;

    let mut received = false;
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
                let bytes = sample.payload().to_bytes();
                // Try pretty-print as JSON, fall back to raw string
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else {
                    println!("{}", String::from_utf8_lossy(&bytes));
                }
                received = true;
            }
            Err(err) => {
                eprintln!("Error: {err}");
            }
        }
    }

    if !received {
        eprintln!("No reply received. Is the orchestrator running?");
        std::process::exit(1);
    }

    session.close().await?;
    Ok(())
}
