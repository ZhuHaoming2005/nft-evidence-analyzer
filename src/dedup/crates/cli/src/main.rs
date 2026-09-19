mod pipeline;
mod progress;
mod report;
mod sample_images;

use clap::{Parser, Subcommand};
use dedup_core::DedupError;
use pipeline::{RunConfig, SampleMetadataConfig};
use progress::{ProgressMode, ProgressReporter};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "dedup", version, about = "In-memory NFT deduplicator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
struct CommonArgs {
    #[arg(long = "input", required = true)]
    inputs: Vec<PathBuf>,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, value_delimiter = ',')]
    chains: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    evm_chains: Vec<String>,
    #[arg(
        long,
        help = "Name similarity percentage; omission disables Name dedup"
    )]
    name_threshold: Option<f64>,
    #[arg(long, default_value_t = 0.6)]
    metadata_threshold: f64,
    #[arg(
        long,
        help = "Metadata anchors per contract; omission keeps every valid NFT metadata record"
    )]
    metadata_anchors: Option<usize>,
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    #[arg(long, default_value_t = 1_000)]
    progress_interval_ms: u64,
    #[arg(
        long,
        value_parser = parse_positive_usize,
        help = "Worker threads; omission uses Rayon's system default"
    )]
    threads: Option<usize>,
}

#[derive(Debug, Parser)]
struct SampleMetadataArgs {
    #[arg(long = "input", required = true)]
    inputs: Vec<PathBuf>,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, value_delimiter = ',')]
    chains: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    evm_chains: Vec<String>,
    #[arg(long, default_value_t = 0.6)]
    metadata_threshold: f64,
    #[arg(
        long,
        help = "Metadata anchors per contract; omission keeps every valid NFT metadata record"
    )]
    metadata_anchors: Option<usize>,
    #[arg(
        long,
        value_parser = parse_positive_usize,
        help = "Complete media pairs required per pool; balanced across chains or unordered chain pairs"
    )]
    sample_pairs: usize,
    #[arg(
        long,
        help = "Sampling seed; omission uses fresh operating-system randomness"
    )]
    seed: Option<u64>,
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    #[arg(long, default_value_t = 1_000)]
    progress_interval_ms: u64,
    #[arg(
        long,
        value_parser = parse_positive_usize,
        help = "Worker threads used while loading and preparing Metadata"
    )]
    threads: Option<usize>,
}

#[derive(Debug, Subcommand)]
enum Command {
    All(CommonArgs),
    RunName(CommonArgs),
    RunUri(CommonArgs),
    RunMetadata(CommonArgs),
    SampleMetadata(SampleMetadataArgs),
}

fn main() {
    if let Err(error) = run() {
        match error {
            DedupError::Interrupted => {
                eprintln!("interrupted");
                std::process::exit(130);
            }
            other => {
                eprintln!("{other}");
                std::process::exit(1);
            }
        }
    }
}

fn run() -> Result<(), DedupError> {
    let cli = Cli::parse();
    if let Command::SampleMetadata(args) = cli.command {
        configure_threads(args.threads)?;
        let mut reporter = ProgressReporter::start(args.progress, args.progress_interval_ms);
        install_cancel_handler(&reporter);
        let result = pipeline::sample_metadata(
            SampleMetadataConfig {
                inputs: args.inputs,
                output_dir: args.output_dir,
                chains: args.chains,
                evm_chains: args.evm_chains,
                metadata_threshold: args.metadata_threshold,
                metadata_anchors: args.metadata_anchors,
                sample_pairs: args.sample_pairs,
                sample_seed: args.seed,
            },
            &reporter,
        );
        reporter.finish();
        return result;
    }
    let (args, run_name, run_uri, run_metadata) = match cli.command {
        Command::All(args) => (args, true, true, true),
        Command::RunName(args) => (args, true, false, false),
        Command::RunUri(args) => (args, false, true, false),
        Command::RunMetadata(args) => (args, false, false, true),
        Command::SampleMetadata(_) => unreachable!("sample command returned above"),
    };
    let progress_mode = args.progress;
    let progress_interval_ms = args.progress_interval_ms;
    configure_threads(args.threads)?;
    let config = RunConfig {
        inputs: args.inputs,
        output_dir: args.output_dir,
        chains: args.chains,
        evm_chains: args.evm_chains,
        name_threshold: args.name_threshold,
        metadata_threshold: args.metadata_threshold,
        metadata_anchors: args.metadata_anchors,
        threads: rayon::current_num_threads(),
        run_name,
        run_uri,
        run_metadata,
    };

    let mut reporter = ProgressReporter::start(progress_mode, progress_interval_ms);
    install_cancel_handler(&reporter);

    let result = pipeline::run(config, &reporter);
    reporter.finish();
    result
}

fn configure_threads(threads: Option<usize>) -> Result<(), DedupError> {
    if let Some(threads) = threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .map_err(|error| {
                DedupError::Message(format!(
                    "failed to configure {threads} worker threads: {error}"
                ))
            })?;
    }
    Ok(())
}

fn install_cancel_handler(reporter: &ProgressReporter) {
    let cancel = reporter.cancel_handle();
    let _ = ctrlc::set_handler(move || {
        cancel.request_cancel();
    });
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| "must be a positive integer".to_owned())?;
    if parsed == 0 {
        Err("must be greater than zero".to_owned())
    } else {
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_positive_usize;

    #[test]
    fn thread_count_must_be_positive() {
        assert_eq!(parse_positive_usize("64").unwrap(), 64);
        assert!(parse_positive_usize("0").is_err());
        assert!(parse_positive_usize("invalid").is_err());
    }
}
