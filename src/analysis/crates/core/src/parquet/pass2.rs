//! Pass 2: metadata_json projection into descending anchors.

use ahash::{AHashMap, AHashSet};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::fs::File;

use crate::AnalysisError;
use crate::entity::{
    ContractId, MetadataRecord, ResidentStore, SourceOrder, compare_token_ids_desc,
    normalized_evm_token_slice,
};
use crate::parquet::LoadOptions;
use crate::parquet::metadata::validated_metadata;
use crate::parquet::pass1::{
    ProjectedUtf8Columns, RowGroupChunk, normalized_chain, row_group_chunks,
};
use crate::parquet::validate::{PASS2_COLUMNS, ValidatedInput};
use crate::progress::ProgressObserver;

/// Per-file metadata records. `metadata_anchors = None` keeps all valid rows.
#[derive(Default)]
struct ShardAnchors {
    by_chain: AHashMap<String, AHashMap<String, Vec<MetadataRecord>>>,
    stats: Pass2Stats,
}

#[derive(Clone, Copy, Debug, Default)]
struct Pass2Stats {
    rows: u64,
    chain_rejected: u64,
    structural_rejected: u64,
    metadata_rejected: u64,
    selection_rejected: u64,
    json_parsed: u64,
    json_valid: u64,
}

impl Pass2Stats {
    fn merge(&mut self, other: Self) {
        self.rows += other.rows;
        self.chain_rejected += other.chain_rejected;
        self.structural_rejected += other.structural_rejected;
        self.metadata_rejected += other.metadata_rejected;
        self.selection_rejected += other.selection_rejected;
        self.json_parsed += other.json_parsed;
        self.json_valid += other.json_valid;
    }
}

/// Exact subset needed by the seed-scoped metadata alignment algorithm.
pub struct MetadataQuerySelection<'a> {
    seed_contracts: AHashMap<String, AHashSet<String>>,
    evm_seed_count: usize,
    evm_seed_indices_by_token: AHashMap<&'a str, Vec<usize>>,
}

impl<'a> MetadataQuerySelection<'a> {
    pub fn new(store: &'a ResidentStore, seeds: &[ContractId]) -> Self {
        let mut seed_contracts = AHashMap::<String, AHashSet<String>>::new();
        let mut evm_seed_indices_by_token = AHashMap::<&str, Vec<usize>>::new();
        let mut evm_seed_count = 0;
        for &seed in seeds {
            let contract = &store.contracts[seed as usize];
            let chain = store.chain_name(contract.chain_id);
            seed_contracts
                .entry(chain.to_owned())
                .or_default()
                .insert(contract.address.clone());
            if store.is_evm_chain(chain) {
                for &nft in store.nfts_for_contract(seed) {
                    let token = normalized_evm_token_slice(
                        store.string(store.nfts[nft as usize].token_id_id),
                    );
                    let seed_indices = evm_seed_indices_by_token.entry(token).or_default();
                    if seed_indices.last().copied() != Some(evm_seed_count) {
                        seed_indices.push(evm_seed_count);
                    }
                }
                evm_seed_count += 1;
            }
        }
        Self {
            seed_contracts,
            evm_seed_count,
            evm_seed_indices_by_token,
        }
    }

    fn is_seed_contract(&self, chain: &str, contract: &str) -> bool {
        self.seed_contracts
            .get(chain)
            .is_some_and(|contracts| contracts.contains(contract))
    }

    fn is_shared_evm_token(&self, token_id: &str) -> bool {
        self.evm_seed_indices_by_token
            .contains_key(normalized_evm_token_slice(token_id))
    }
}

impl ShardAnchors {
    /// Cheap lossless gate before strict JSON parsing/canonicalization. A row
    /// can be skipped only when already-valid anchors prove it cannot affect
    /// the bounded/alignment selection.
    fn could_accept(
        &self,
        chain: &str,
        contract_address: &str,
        token_id: &str,
        options: &LoadOptions,
        selection: Option<&MetadataQuerySelection<'_>>,
    ) -> bool {
        let Some(anchors) = self
            .by_chain
            .get(chain)
            .and_then(|contracts| contracts.get(contract_address))
        else {
            return true;
        };
        if options.metadata_anchors.is_none() && selection.is_none() {
            return true;
        }
        let is_seed_contract =
            selection.is_some_and(|selection| selection.is_seed_contract(chain, contract_address));
        let is_evm = options.evm_chains.contains(chain);
        if options.metadata_anchors.is_none()
            && let Some(selection) = selection
            && !is_seed_contract
        {
            let shared = is_evm && selection.is_shared_evm_token(token_id);
            if !shared
                && let Some(prior) = anchors
                    .iter()
                    .find(|record| !is_evm || !selection.is_shared_evm_token(&record.token_id))
            {
                return compare_token_ids_desc(token_id, &prior.token_id, is_evm)
                    == std::cmp::Ordering::Less;
            }
        }
        if anchors.iter().any(|record| record.token_id == token_id) {
            return false;
        }
        let insert_at = anchors
            .binary_search_by(|record| compare_token_ids_desc(&record.token_id, token_id, is_evm))
            .unwrap_or_else(|position| position);
        !options
            .metadata_anchors
            .is_some_and(|limit| insert_at >= limit && anchors.len() >= limit)
    }

    #[allow(clippy::too_many_arguments)] // Fields are moved directly into a bounded metadata record.
    fn insert(
        &mut self,
        chain: &str,
        contract_address: &str,
        token_id: String,
        canonical_json: String,
        source_order: SourceOrder,
        options: &LoadOptions,
        selection: Option<&MetadataQuerySelection<'_>>,
    ) {
        let is_seed_contract =
            selection.is_some_and(|selection| selection.is_seed_contract(chain, contract_address));
        let anchors = self
            .by_chain
            .entry(chain.to_owned())
            .or_default()
            .entry(contract_address.to_owned())
            .or_default();
        if options.metadata_anchors.is_none() && selection.is_none() {
            anchors.push(MetadataRecord {
                token_id,
                canonical_json,
                source_order,
            });
            return;
        }
        let is_evm = options.evm_chains.contains(chain);
        if options.metadata_anchors.is_none()
            && let Some(selection) = selection
            && !is_seed_contract
        {
            let shared = is_evm && selection.is_shared_evm_token(&token_id);
            if !shared {
                let prior_non_shared = anchors
                    .iter()
                    .position(|record| !is_evm || !selection.is_shared_evm_token(&record.token_id));
                if let Some(position) = prior_non_shared {
                    if compare_token_ids_desc(&token_id, &anchors[position].token_id, is_evm)
                        == std::cmp::Ordering::Less
                    {
                        anchors[position] = MetadataRecord {
                            token_id,
                            canonical_json,
                            source_order,
                        };
                        anchors.sort_unstable_by(|left, right| {
                            compare_token_ids_desc(&left.token_id, &right.token_id, is_evm)
                                .then_with(|| left.source_order.cmp(&right.source_order))
                        });
                    }
                    return;
                }
            }
        }
        // Same token id: keep first valid in source order.
        if anchors.iter().any(|record| record.token_id == token_id) {
            return;
        }
        let insert_at = anchors
            .binary_search_by(|record| compare_token_ids_desc(&record.token_id, &token_id, is_evm))
            .unwrap_or_else(|position| position);
        if let Some(limit) = options.metadata_anchors
            && insert_at >= limit
            && anchors.len() >= limit
        {
            return;
        }
        anchors.insert(
            insert_at,
            MetadataRecord {
                token_id,
                canonical_json,
                source_order,
            },
        );
        if let Some(limit) = options.metadata_anchors
            && anchors.len() > limit
        {
            anchors.pop();
        }
    }

    fn merge_ordered(
        &mut self,
        mut other: Self,
        options: &LoadOptions,
        selection: Option<&MetadataQuerySelection<'_>>,
    ) {
        self.stats.merge(other.stats);
        for (chain, contracts) in other.by_chain.drain() {
            for (contract_address, records) in contracts {
                for record in records {
                    self.insert(
                        &chain,
                        &contract_address,
                        record.token_id,
                        record.canonical_json,
                        record.source_order,
                        options,
                        selection,
                    );
                }
            }
        }
    }

    fn prune_to_alignment_anchors(
        &mut self,
        options: &LoadOptions,
        selection: &MetadataQuerySelection<'_>,
    ) {
        for (chain, contracts) in &mut self.by_chain {
            for (contract, anchors) in contracts {
                if options.metadata_anchors.is_some()
                    || selection.is_seed_contract(chain, contract)
                    || !options.evm_chains.contains(chain)
                    || anchors.len() <= 1
                {
                    continue;
                }
                let mut unresolved = vec![true; selection.evm_seed_count];
                let mut unresolved_count = unresolved.len();
                let mut retained = Vec::with_capacity(unresolved.len().saturating_add(1));
                for (position, record) in anchors.drain(..).enumerate() {
                    let token = normalized_evm_token_slice(&record.token_id);
                    let mut required = position == 0;
                    if let Some(seed_indices) = selection.evm_seed_indices_by_token.get(token) {
                        for &seed_index in seed_indices {
                            if unresolved[seed_index] {
                                unresolved[seed_index] = false;
                                unresolved_count -= 1;
                                required = true;
                            }
                        }
                    }
                    if required {
                        retained.push(record);
                    }
                    if unresolved_count == 0 {
                        break;
                    }
                }
                *anchors = retained;
            }
        }
    }
}

/// Collected pass-2 anchors prior to store ingestion.
#[derive(Default)]
pub struct CollectedPass2Anchors {
    by_chain: AHashMap<String, AHashMap<String, Vec<MetadataRecord>>>,
}

/// Scan + merge pass-2 metadata without touching the resident store.
pub fn collect_pass2_anchors(
    inputs: &[ValidatedInput],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
    selection: Option<&MetadataQuerySelection<'_>>,
) -> Result<CollectedPass2Anchors, AnalysisError> {
    let shard_results: Vec<Result<ShardAnchors, AnalysisError>> = inputs
        .par_iter()
        .map(|input| scan_file_pass2(input, options, progress, selection))
        .collect();

    // Merge in an ordered parallel tree. This preserves first-source-row
    // semantics without serially replaying every intermediate file shard.
    let mut shard = merge_anchor_shards_ordered(shard_results, options, selection)?;
    if let Some(selection) = selection {
        shard.prune_to_alignment_anchors(options, selection);
    }
    let retained = shard
        .by_chain
        .values()
        .flat_map(|contracts| contracts.values())
        .map(Vec::len)
        .sum::<usize>();
    eprintln!(
        "metadata/pass2: rows={} chain_rejected={} structural_rejected={} metadata_rejected={} selection_rejected={} json_parsed={} json_valid={} retained={}",
        shard.stats.rows,
        shard.stats.chain_rejected,
        shard.stats.structural_rejected,
        shard.stats.metadata_rejected,
        shard.stats.selection_rejected,
        shard.stats.json_parsed,
        shard.stats.json_valid,
        retained,
    );
    Ok(CollectedPass2Anchors {
        by_chain: shard.by_chain,
    })
}

/// Ingest previously collected pass-2 anchors into the resident store.
pub fn apply_pass2_anchors(
    store: &mut ResidentStore,
    anchors: CollectedPass2Anchors,
) -> Result<(), AnalysisError> {
    for (chain, contracts) in anchors.by_chain {
        for (contract_address, records) in contracts {
            store.ingest_metadata_records(&chain, &contract_address, records)?;
        }
    }
    Ok(())
}

fn scan_file_pass2(
    input: &ValidatedInput,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
    selection: Option<&MetadataQuerySelection<'_>>,
) -> Result<ShardAnchors, AnalysisError> {
    let chunks = row_group_chunks(input);

    // Parse contiguous row-group chunks in parallel, then merge in source order
    // so duplicate token ids still keep the first valid source row.
    let chunk_results: Vec<Result<ShardAnchors, AnalysisError>> = chunks
        .par_iter()
        .map(|chunk| scan_row_group_chunk_pass2(input, chunk, options, progress, selection))
        .collect();
    merge_anchor_shards_ordered(chunk_results, options, selection)
}

fn merge_anchor_shards_ordered(
    mut shards: Vec<Result<ShardAnchors, AnalysisError>>,
    options: &LoadOptions,
    selection: Option<&MetadataQuerySelection<'_>>,
) -> Result<ShardAnchors, AnalysisError> {
    match shards.len() {
        0 => Ok(ShardAnchors::default()),
        1 => shards.pop().expect("one anchor shard is present"),
        _ => {
            let right = shards.split_off(shards.len() / 2);
            let (left, right) = rayon::join(
                || merge_anchor_shards_ordered(shards, options, selection),
                || merge_anchor_shards_ordered(right, options, selection),
            );
            let mut left = left?;
            left.merge_ordered(right?, options, selection);
            Ok(left)
        }
    }
}

fn scan_row_group_chunk_pass2(
    input: &ValidatedInput,
    chunk: &RowGroupChunk,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
    selection: Option<&MetadataQuerySelection<'_>>,
) -> Result<ShardAnchors, AnalysisError> {
    progress.check_cancelled()?;
    let file = File::open(&input.path)
        .map_err(|error| AnalysisError::parquet(format!("{}: {error}", input.path.display())))?;
    let mask = ProjectionMask::roots(
        input.metadata.metadata().file_metadata().schema_descr(),
        input.pass2_projection.iter().copied(),
    );
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, input.metadata.clone())
        .with_projection(mask)
        .with_row_groups(chunk.row_groups.clone().collect())
        .with_batch_size(8 * 1024)
        .build()
        .map_err(|error| AnalysisError::parquet(format!("{}: {error}", input.path.display())))?;
    let mut row_offset = 0_u64;
    let mut shard = ShardAnchors::default();
    for batch in reader {
        let batch = batch.map_err(|error| {
            AnalysisError::parquet(format!("{}: {error}", input.path.display()))
        })?;
        let columns = ProjectedUtf8Columns::new(&batch, &input.path, &PASS2_COLUMNS)?;
        for row_index in 0..batch.num_rows() {
            shard.stats.rows += 1;
            let chain = normalized_chain(columns.value_at(0, row_index));
            let contract_address = columns.value_at(1, row_index).trim();
            let token_id = columns.value_at(2, row_index).trim();
            let metadata_raw = columns.value_at(3, row_index).trim();
            let source_order = SourceOrder {
                file_ordinal: input.file_ordinal,
                file_row_number: chunk.row_start + row_offset,
            };
            row_offset += 1;
            if !options.allowed_chains.is_empty()
                && !options.allowed_chains.contains(chain.as_ref())
            {
                shard.stats.chain_rejected += 1;
                continue;
            }
            if chain.is_empty() || contract_address.is_empty() || token_id.is_empty() {
                shard.stats.structural_rejected += 1;
                continue;
            }
            // Cheap reject before full JSON parse+canonicalize.
            if metadata_raw.is_empty()
                || metadata_raw == "{}"
                || !matches!(metadata_raw.as_bytes().first(), Some(b'{') | Some(b'['))
            {
                shard.stats.metadata_rejected += 1;
                continue;
            }
            if !shard.could_accept(
                chain.as_ref(),
                contract_address,
                token_id,
                options,
                selection,
            ) {
                shard.stats.selection_rejected += 1;
                continue;
            }
            shard.stats.json_parsed += 1;
            let Some(canonical_json) = validated_metadata(metadata_raw) else {
                continue;
            };
            shard.stats.json_valid += 1;
            shard.insert(
                chain.as_ref(),
                contract_address,
                token_id.to_owned(),
                canonical_json,
                source_order,
                options,
                selection,
            );
        }
        progress.add_completed(batch.num_rows() as u64);
    }
    Ok(shard)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(shard: &mut ShardAnchors, token: &str, selection: &MetadataQuerySelection<'_>) {
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], None);
        shard.insert(
            "ethereum",
            "0xcandidate",
            token.to_owned(),
            format!(r#"{{"token":"{token}"}}"#),
            SourceOrder {
                file_ordinal: 0,
                file_row_number: token.parse().unwrap(),
            },
            &options,
            Some(selection),
        );
    }

    #[test]
    fn seed_selection_keeps_shared_tokens_and_only_the_largest_fallback() {
        let shared = "2";
        let selection = MetadataQuerySelection {
            seed_contracts: AHashMap::new(),
            evm_seed_count: 1,
            evm_seed_indices_by_token: AHashMap::from([(shared, vec![0])]),
        };
        let mut shard = ShardAnchors::default();
        for token in ["1", "2", "3", "4"] {
            record(&mut shard, token, &selection);
        }
        let anchors = &shard.by_chain["ethereum"]["0xcandidate"];
        assert_eq!(
            anchors
                .iter()
                .map(|record| record.token_id.as_str())
                .collect::<Vec<_>>(),
            ["4", "2"]
        );
    }

    #[test]
    fn bounded_selection_rejects_losing_rows_before_json_validation() {
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], Some(2));
        let mut shard = ShardAnchors::default();
        for token in ["10", "9"] {
            shard.insert(
                "ethereum",
                "0xcandidate",
                token.to_owned(),
                format!(r#"{{"token":"{token}"}}"#),
                SourceOrder {
                    file_ordinal: 0,
                    file_row_number: token.parse().unwrap(),
                },
                &options,
                None,
            );
        }

        assert!(!shard.could_accept("ethereum", "0xcandidate", "1", &options, None));
        assert!(!shard.could_accept("ethereum", "0xcandidate", "9", &options, None));
        assert!(shard.could_accept("ethereum", "0xcandidate", "11", &options, None));
    }

    #[test]
    fn ordered_merge_discards_row_group_local_fallbacks() {
        let shared = "2";
        let selection = MetadataQuerySelection {
            seed_contracts: AHashMap::new(),
            evm_seed_count: 1,
            evm_seed_indices_by_token: AHashMap::from([(shared, vec![0])]),
        };
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], None);
        let mut left = ShardAnchors::default();
        record(&mut left, "5", &selection);
        let mut right = ShardAnchors::default();
        record(&mut right, "2", &selection);
        record(&mut right, "7", &selection);
        left.merge_ordered(right, &options, Some(&selection));
        let anchors = &left.by_chain["ethereum"]["0xcandidate"];
        assert_eq!(
            anchors
                .iter()
                .map(|record| record.token_id.as_str())
                .collect::<Vec<_>>(),
            ["7", "2"]
        );
    }

    #[test]
    fn final_prune_keeps_only_largest_shared_token_per_seed() {
        let selection = MetadataQuerySelection {
            seed_contracts: AHashMap::new(),
            evm_seed_count: 2,
            evm_seed_indices_by_token: AHashMap::from([
                ("2", vec![0]),
                ("4", vec![0]),
                ("3", vec![1]),
                ("5", vec![1]),
            ]),
        };
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], None);
        let mut shard = ShardAnchors::default();
        for token in ["1", "2", "3", "4", "5", "6"] {
            record(&mut shard, token, &selection);
        }
        shard.prune_to_alignment_anchors(&options, &selection);
        let anchors = &shard.by_chain["ethereum"]["0xcandidate"];
        assert_eq!(
            anchors
                .iter()
                .map(|record| record.token_id.as_str())
                .collect::<Vec<_>>(),
            ["6", "5", "4"]
        );
    }
}
