use analysis_cli::pipeline::{RunConfig, RunDedupConfig, run as run_pipeline, run_dedup};
use analysis_cli::progress::{ProgressMode, ProgressReporter};
use analysis_core::{
    AnalysisError, ApiKeys, INTERMEDIATE_DIR, PaperConfig, ProgressObserver, ProviderEndpoints,
    SeedNftDownloadOptions, SelectSeedsOptions, select_seeds, write_seed_outputs,
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "analysis",
    version,
    about = "In-memory NFT evidence analysis pipeline"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Shared flags for `run` and `run-dedup`.
#[derive(Debug, Parser)]
struct RunArgs {
    /// Input Parquet snapshot (repeatable).
    #[arg(long = "input", required = true)]
    inputs: Vec<PathBuf>,

    /// Seed manifest JSON path.
    #[arg(long)]
    seeds: PathBuf,

    /// Output directory for reports.
    #[arg(long)]
    output_dir: PathBuf,

    /// Chains to include (comma-separated).
    #[arg(long, value_delimiter = ',')]
    chains: Vec<String>,

    /// EVM chains among `--chains` (comma-separated).
    #[arg(long, value_delimiter = ',')]
    evm_chains: Vec<String>,

    /// Enable Name deduplication with this Jaro-Winkler threshold.
    ///
    /// If omitted, Name indexes and Name duplicate queries are skipped.
    #[arg(long)]
    name_threshold: Option<f64>,

    /// Metadata BM25 threshold, default 0.6.
    #[arg(long, default_value_t = 0.6)]
    metadata_threshold: f64,

    /// Alchemy API key (required for uncached EVM seed snapshots; otherwise optional).
    #[arg(long)]
    alchemy_api_key: Option<String>,

    /// Etherscan API key (optional).
    #[arg(long)]
    etherscan_api_key: Option<String>,

    /// Helius API key (required for uncached Solana seed snapshots; otherwise optional).
    #[arg(long)]
    helius_api_key: Option<String>,

    /// OpenSea API key (optional).
    #[arg(long)]
    opensea_api_key: Option<String>,

    /// Rayon thread pool size.
    #[arg(long)]
    rayon_threads: Option<usize>,

    /// Per-provider HTTP concurrency (Alchemy / OpenSea / Helius / Etherscan each
    /// get an independent pool of this size). Saturating Alchemy does not block
    /// other providers. Keep modest: each candidate fans out to many nested RPCs.
    /// Default 12 avoids mass Alchemy timeouts.
    #[arg(long, default_value_t = 12)]
    http_concurrency: usize,

    /// Ignore raw API responses cached before this run and refresh each exact
    /// request once; newly successful responses are reused within the run.
    #[arg(long)]
    refresh_api_cache: bool,

    /// Path for durable dedup cache (default: `<output-dir>/intermediate/dedup_cache.json`).
    /// Written after dedup on `run`. Compatible cache is **auto-reused** on later runs.
    #[arg(long)]
    dedup_cache: Option<PathBuf>,

    /// Path for durable evidence cache (default: `<output-dir>/intermediate/evidence_cache.json`).
    /// Written during enrich. Compatible cache is **auto-resumed** on later runs.
    #[arg(long)]
    evidence_cache: Option<PathBuf>,

    /// Compressed complete seed-NFT cache directory (default:
    /// `<output-dir>/intermediate/seed_nfts`).
    #[arg(long)]
    seed_nft_cache_dir: Option<PathBuf>,

    /// Ignore reusable compressed seed-NFT caches and download again.
    #[arg(long)]
    refresh_seed_nfts: bool,

    /// Progress reporter mode.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
}

#[derive(Debug, Parser)]
struct SelectSeedsArgs {
    /// Output directory for `seeds.json` and `seeds.audit.json`.
    #[arg(long)]
    output_dir: PathBuf,

    /// Chains to select seeds for (comma-separated).
    #[arg(long, value_delimiter = ',')]
    chains: Vec<String>,

    /// Top-N seeds per chain (default 25).
    #[arg(long, default_value_t = 25)]
    seeds_per_chain: usize,

    /// OpenSea API key (required for EVM seed ranking).
    #[arg(long)]
    opensea_api_key: Option<String>,

    /// NFTScan API key (required for Solana 30-day ranking).
    #[arg(long)]
    nftscan_api_key: Option<String>,

    /// Per-provider HTTP concurrency (independent pools for each API provider).
    #[arg(long, default_value_t = 32)]
    http_concurrency: usize,

    /// Progress reporter mode.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Build per-chain top-N seed list + audit JSON.
    SelectSeeds(SelectSeedsArgs),
    /// End-to-end: load → dedup → enrich → analyze → reports.
    Run(RunArgs),
    /// Load snapshots, match seeds, and write hit/candidate reports.
    RunDedup(RunArgs),
}

fn main() {
    if let Err(error) = run() {
        match error {
            AnalysisError::Cancelled => {
                eprintln!("cancelled");
                std::process::exit(130);
            }
            other => {
                eprintln!("{other}");
                std::process::exit(1);
            }
        }
    }
}

fn with_progress<F>(mode: ProgressMode, f: F) -> Result<(), AnalysisError>
where
    F: FnOnce(&ProgressReporter) -> Result<(), AnalysisError>,
{
    let reporter = ProgressReporter::start(mode, 500);
    let result = f(&reporter);
    reporter.finish();
    result
}

fn run() -> Result<(), AnalysisError> {
    let cli = Cli::parse();
    match cli.command {
        Command::SelectSeeds(args) => with_progress(args.progress, |progress| {
            let chains = if args.chains.is_empty() {
                SelectSeedsOptions::default().chains
            } else {
                args.chains.clone()
            };
            progress.begin_phase("select-seeds", Some(chains.len() as u64));
            let (seeds, audit) = select_seeds(&SelectSeedsOptions {
                chains: chains.clone(),
                seeds_per_chain: args.seeds_per_chain,
                opensea_api_key: args.opensea_api_key.clone(),
                nftscan_api_key: args.nftscan_api_key.clone(),
                http_concurrency: args.http_concurrency,
                ..SelectSeedsOptions::default()
            })?;
            write_seed_outputs(&args.output_dir, &seeds, &audit)?;
            progress.add_completed(chains.len() as u64);
            eprintln!(
                "select-seeds: wrote {} seeds to {}",
                seeds.len(),
                args.output_dir.join("seeds.json").display()
            );
            Ok(())
        }),
        Command::Run(args) => with_progress(args.progress, |progress| {
            let api_keys = ApiKeys {
                alchemy: args.alchemy_api_key.clone(),
                etherscan: args.etherscan_api_key.clone(),
                helius: args.helius_api_key.clone(),
                opensea: args.opensea_api_key.clone(),
            };
            let seed_nft_download = SeedNftDownloadOptions {
                cache_dir: args
                    .seed_nft_cache_dir
                    .clone()
                    .unwrap_or_else(|| args.output_dir.join(INTERMEDIATE_DIR).join("seed_nfts")),
                api_keys: api_keys.clone(),
                endpoints: ProviderEndpoints::default(),
                concurrency: args.http_concurrency,
                retries: 3,
                refresh: args.refresh_seed_nfts,
            };
            run_pipeline(
                &RunConfig {
                    inputs: args.inputs,
                    seeds: args.seeds,
                    output_dir: args.output_dir,
                    chains: args.chains,
                    evm_chains: args.evm_chains,
                    name_threshold: args.name_threshold,
                    metadata_threshold: args.metadata_threshold,
                    metadata_anchors: None,
                    rayon_threads: args.rayon_threads,
                    api_keys,
                    http_concurrency: args.http_concurrency,
                    refresh_api_cache: args.refresh_api_cache,
                    paper: PaperConfig::default(),
                    enrich_override: None,
                    dedup_cache_path: args.dedup_cache,
                    evidence_cache_path: args.evidence_cache,
                    seed_nft_download: Some(seed_nft_download),
                },
                progress,
            )
        }),
        Command::RunDedup(args) => with_progress(args.progress, |progress| {
            let api_keys = ApiKeys {
                alchemy: args.alchemy_api_key.clone(),
                etherscan: args.etherscan_api_key.clone(),
                helius: args.helius_api_key.clone(),
                opensea: args.opensea_api_key.clone(),
            };
            let seed_nft_download = SeedNftDownloadOptions {
                cache_dir: args
                    .seed_nft_cache_dir
                    .clone()
                    .unwrap_or_else(|| args.output_dir.join(INTERMEDIATE_DIR).join("seed_nfts")),
                api_keys,
                endpoints: ProviderEndpoints::default(),
                concurrency: args.http_concurrency,
                retries: 3,
                refresh: args.refresh_seed_nfts,
            };
            run_dedup(
                &RunDedupConfig {
                    inputs: args.inputs,
                    seeds: args.seeds,
                    output_dir: args.output_dir,
                    chains: args.chains,
                    evm_chains: args.evm_chains,
                    name_threshold: args.name_threshold,
                    metadata_threshold: args.metadata_threshold,
                    metadata_anchors: None,
                    rayon_threads: args.rayon_threads,
                    seed_nft_download: Some(seed_nft_download),
                },
                progress,
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required_run_args() -> Vec<&'static str> {
        vec![
            "analysis",
            "--input",
            "snapshot.parquet",
            "--seeds",
            "seeds.json",
            "--output-dir",
            "out",
        ]
    }

    #[test]
    fn name_threshold_is_disabled_when_omitted() {
        let args = RunArgs::try_parse_from(required_run_args()).unwrap();
        assert_eq!(args.name_threshold, None);
    }

    #[test]
    fn name_threshold_is_enabled_when_explicit() {
        let mut argv = required_run_args();
        argv.extend(["--name-threshold", "0.97"]);
        let args = RunArgs::try_parse_from(argv).unwrap();
        assert_eq!(args.name_threshold, Some(0.97));
    }

    #[test]
    fn api_cache_refresh_is_opt_in() {
        let args = RunArgs::try_parse_from(required_run_args()).unwrap();
        assert!(!args.refresh_api_cache);

        let mut argv = required_run_args();
        argv.push("--refresh-api-cache");
        let args = RunArgs::try_parse_from(argv).unwrap();
        assert!(args.refresh_api_cache);
    }

    #[test]
    fn obsolete_reuse_flags_are_removed() {
        for flag in ["--reuse-dedup", "--reuse-evidence"] {
            let mut argv = required_run_args();
            argv.push(flag);
            assert!(
                RunArgs::try_parse_from(argv).is_err(),
                "{flag} must no longer be accepted"
            );
        }
        let mut argv = required_run_args();
        argv.extend(["--metadata-anchors", "8"]);
        assert!(RunArgs::try_parse_from(argv).is_err());
    }
}
