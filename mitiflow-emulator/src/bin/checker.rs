//! Post-run invariant checker for mitiflow-emulator chaos tests.
//!
//! Usage: cargo run -p mitiflow-emulator --bin checker -- --manifest-dir ./manifests/scenario-1

use std::path::PathBuf;
use std::process;

use clap::Parser;
use mitiflow_emulator::invariant_checker::{InvariantChecker, InvariantVerdict};

#[derive(Parser)]
#[command(name = "mitiflow-checker", about = "Post-run invariant checker")]
struct Cli {
    /// Directory containing JSONL manifest files
    #[arg(long)]
    manifest_dir: PathBuf,

    /// Output format: human (default), json
    #[arg(long, default_value = "human")]
    format: String,

    /// Allow ordering violations (don't affect verdict)
    #[arg(long, default_value_t = true)]
    allow_reorder: bool,
}

fn main() {
    let exit_code = match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("Error: {error}");
            2
        }
    };

    process::exit(exit_code);
}

fn run() -> Result<i32, Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let report = InvariantChecker::check(&cli.manifest_dir)?;

    match cli.format.as_str() {
        "json" => {
            let output = serde_json::to_string_pretty(&report)?;
            println!("{output}");
        }
        _ => {
            println!("=== Invariant Check Report ===");
            println!("Total published: {}", report.total_published);
            println!("Total received:  {}", report.total_received);
            println!("Missing events:  {}", report.missing_events.len());
            println!("Duplicate events:{}", report.duplicate_events.len());
            println!("Ordering issues: {}", report.ordering_violations.len());
            println!("Checksum errors: {}", report.checksum_mismatches.len());
            println!("Verdict:         {:?}", report.verdict);

            if !report.missing_events.is_empty() {
                println!("\nMissing events (first 10):");
                for evt in report.missing_events.iter().take(10) {
                    println!(
                        "  - {} (publisher={}, seq={})",
                        evt.event_id, evt.publisher_id, evt.seq
                    );
                }
            }
        }
    }

    Ok(match report.verdict {
        InvariantVerdict::Pass => 0,
        InvariantVerdict::Fail => 1,
    })
}
