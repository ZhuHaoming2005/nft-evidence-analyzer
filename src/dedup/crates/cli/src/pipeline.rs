use crate::progress::ProgressReporter;
use crate::report::{
    PhaseTiming, ReportPartition, ReportRequest, StageTiming, write_partition_reports,
    write_reports,
};
use crate::sample_images::StreamingDownloadSession;
use dedup_core::{
    DedupError, Dimension, LoadOptions, ProgressObserver, SummaryAccumulator,
    load_entities_with_options, run_metadata, run_name, run_uri,
    sample_metadata as sample_metadata_core,
};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: Option<f64>,
    pub metadata_threshold: f64,
    pub metadata_anchors: Option<usize>,
    pub threads: usize,
    pub run_name: bool,
    pub run_uri: bool,
    pub run_metadata: bool,
}

#[derive(Clone, Debug)]
pub struct SampleMetadataConfig {
    pub inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub metadata_threshold: f64,
    pub metadata_anchors: Option<usize>,
    pub sample_pairs: usize,
    pub sample_seed: Option<u64>,
}

pub fn run(config: RunConfig, progress: &ProgressReporter) -> Result<(), DedupError> {
    let started = Instant::now();
    let allowed = config
        .chains
        .iter()
        .map(|c| c.trim().to_ascii_lowercase())
        .filter(|c| !c.is_empty())
        .collect::<Vec<_>>();
    let evm_names = config
        .evm_chains
        .iter()
        .map(|c| c.trim().to_ascii_lowercase())
        .filter(|c| !c.is_empty())
        .collect::<Vec<_>>();
    let mut load_options = LoadOptions::new(
        allowed,
        evm_names.iter().cloned(),
        if config.run_metadata {
            config.metadata_anchors
        } else {
            Some(0)
        },
    );
    load_options.load_names = config.run_name && config.name_threshold.is_some();
    load_options.load_token_uris = config.run_uri;
    load_options.load_image_uris = config.run_uri;
    load_options.load_metadata = config.run_metadata;
    let stage_started = Instant::now();
    let mut store = load_entities_with_options(&config.inputs, &load_options, progress)?;
    let interned_strings = store.strings.len();
    let token_uri_postings = store.token_uri_postings.len();
    let image_uri_postings = store.image_uri_postings.len();
    let mut stage_timings = vec![StageTiming {
        stage: "load",
        elapsed_secs: stage_started.elapsed().as_secs_f64(),
    }];

    let mut acc = SummaryAccumulator::default();
    if let (true, Some(name_threshold)) = (config.run_name, config.name_threshold) {
        let stage_started = Instant::now();
        run_name(&store, name_threshold / 100.0, &mut acc, progress)?;
        stage_timings.push(StageTiming {
            stage: "name",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
        write_completed_partition(
            &config.output_dir,
            &store,
            &acc,
            ReportPartition::Name,
            "name",
            progress,
        )?;
        acc.seal_dimension(Dimension::Name);
    }
    if config.run_uri {
        let stage_started = Instant::now();
        run_uri(&store, &mut acc, progress)?;
        stage_timings.push(StageTiming {
            stage: "uri",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
        write_completed_partition(
            &config.output_dir,
            &store,
            &acc,
            ReportPartition::Uri,
            "uri",
            progress,
        )?;
        acc.seal_dimension(Dimension::TokenUri);
        acc.seal_dimension(Dimension::ImageUri);
    }
    store.release_completed_dimension_data();
    let mut metadata_stats = None;
    if config.run_metadata {
        let stage_started = Instant::now();
        let evm: std::collections::HashSet<String> = config
            .evm_chains
            .iter()
            .map(|c| c.trim().to_ascii_lowercase())
            .collect();
        let result = run_metadata(
            &mut store,
            &evm,
            config.metadata_anchors,
            config.metadata_threshold,
            &mut acc,
            progress,
        )?;
        metadata_stats = Some(result.stats);
        stage_timings.push(StageTiming {
            stage: "metadata",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
    }

    let phase_timings = progress
        .phase_timings()
        .into_iter()
        .map(|timing| PhaseTiming {
            stage: timing.stage,
            phase: timing.phase,
            elapsed_secs: timing.elapsed.as_secs_f64(),
        })
        .collect();
    progress.set_stage("report");
    progress.begin_phase("write", Some(3));
    write_reports(
        &config.output_dir,
        ReportRequest {
            store: &store,
            accumulator: &acc,
            inputs: &config.inputs,
            chains: &config.chains,
            evm_chains: &config.evm_chains,
            name_threshold: config.name_threshold,
            metadata_threshold: config.metadata_threshold,
            metadata_anchors: config.metadata_anchors,
            threads: config.threads,
            interned_strings,
            token_uri_postings,
            image_uri_postings,
            metadata_direct: metadata_stats,
            stage_timings,
            phase_timings,
            elapsed: started.elapsed(),
        },
    )
    .map_err(|error| DedupError::Message(error.to_string()))?;
    progress.add_completed(3);
    Ok(())
}

pub fn sample_metadata(
    config: SampleMetadataConfig,
    progress: &ProgressReporter,
) -> Result<(), DedupError> {
    let allowed = config
        .chains
        .iter()
        .map(|chain| chain.trim().to_ascii_lowercase())
        .filter(|chain| !chain.is_empty())
        .collect::<Vec<_>>();
    let evm_names = config
        .evm_chains
        .iter()
        .map(|chain| chain.trim().to_ascii_lowercase())
        .filter(|chain| !chain.is_empty())
        .collect::<Vec<_>>();
    let mut load_options =
        LoadOptions::new(allowed, evm_names.iter().cloned(), config.metadata_anchors);
    load_options.load_names = false;
    load_options.load_token_uris = false;
    load_options.load_image_uris = true;
    load_options.load_metadata = true;
    let mut downloads = StreamingDownloadSession::new(
        &config.output_dir,
        config.sample_pairs,
        std::sync::Arc::new(progress.clone()),
    )
    .map_err(|error| DedupError::Message(error.to_string()))?;
    let mut store = load_entities_with_options(&config.inputs, &load_options, progress)?;
    let evm = evm_names.into_iter().collect();
    let result = sample_metadata_core(
        &mut store,
        &evm,
        config.metadata_anchors,
        config.metadata_threshold,
        config.sample_pairs,
        config.sample_seed,
        progress,
        &mut downloads,
    )?;
    if result.intra_chain.len() != config.sample_pairs
        || result.cross_chain.len() != config.sample_pairs
    {
        return Err(DedupError::Message(format!(
            "random Metadata candidate search exhausted {}/{} randomized profile visits and performed {} profile-pair evaluations with {} of {} intra-chain and {} of {} cross-chain complete media pairs ({})",
            result.profile_visits,
            result.planned_profile_visits,
            result.scored_profile_pairs,
            result.intra_chain.len(),
            config.sample_pairs,
            result.cross_chain.len(),
            config.sample_pairs,
            downloads.summary(),
        )));
    }
    downloads.set_sampling_seed(result.sample_seed);
    downloads
        .finish()
        .map_err(|error| DedupError::Message(error.to_string()))?;
    Ok(())
}

fn write_completed_partition(
    output_dir: &std::path::Path,
    store: &dedup_core::EntityStore,
    accumulator: &SummaryAccumulator,
    partition: ReportPartition,
    stage: &str,
    progress: &ProgressReporter,
) -> Result<(), DedupError> {
    progress.set_stage("report");
    progress.begin_phase(stage, Some(2));
    write_partition_reports(output_dir, store, accumulator, partition)
        .map_err(|error| DedupError::Message(error.to_string()))?;
    progress.add_completed(2);
    Ok(())
}
