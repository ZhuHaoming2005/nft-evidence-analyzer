//! Name query-to-index engine (EVM representative + Solana NFT→collection).

mod bounds;
mod representative;

pub use bounds::CandidateBounds;
pub use representative::{is_usable_name, select_evm_representatives};

use ahash::{AHashMap, AHashSet};
use rapidfuzz::distance::jaro_winkler::{Args, BatchComparator};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::dedup::hits::{Dimension, HitEdge, HitGraph};
use crate::entity::{ChainId, ContractId, ResidentStore, StringId};
use crate::error::AnalysisError;
use crate::progress::{NoopProgress, ProgressObserver};

use self::representative::is_usable_name as usable;

/// Default JW similarity threshold (fraction).
pub const DEFAULT_NAME_THRESHOLD: f64 = 0.98;

const DENSE_NAME_SEEN_BUDGET_BYTES: usize = 4 * 1024 * 1024 * 1024;
const PARALLEL_NAME_QUERY_CHUNK: usize = 8;

fn visit_occurrence_tokens<E>(
    sorted_chars: &[char],
    mut visit: impl FnMut(u64) -> Result<(), E>,
) -> Result<(), E> {
    let mut previous = None;
    let mut rank = 0_u32;
    for &character in sorted_chars {
        if previous == Some(character) {
            rank = rank
                .checked_add(1)
                .expect("name occurrence rank cannot exceed the slice length");
        } else {
            previous = Some(character);
            rank = 0;
        }
        visit((u64::from(character as u32) << 32) | u64::from(rank))?;
    }
    Ok(())
}

enum NameCandidateSeen {
    Dense {
        generations: Vec<u16>,
        generation: u16,
    },
    Sparse(AHashSet<u32>),
}

impl NameCandidateSeen {
    fn begin_query(&mut self, name_count: usize) {
        match self {
            Self::Dense {
                generations,
                generation,
            } => {
                if generations.len() != name_count {
                    generations.clear();
                    generations.resize(name_count, 0);
                    *generation = 0;
                }
                *generation = generation.wrapping_add(1);
                if *generation == 0 {
                    generations.fill(0);
                    *generation = 1;
                }
            }
            Self::Sparse(seen) => seen.clear(),
        }
    }

    fn insert(&mut self, candidate: u32) -> bool {
        match self {
            Self::Dense {
                generations,
                generation,
            } => {
                let slot = &mut generations[candidate as usize];
                if *slot == *generation {
                    false
                } else {
                    *slot = *generation;
                    true
                }
            }
            Self::Sparse(seen) => seen.insert(candidate),
        }
    }
}

/// Reusable per-worker Name query buffers.
pub struct NameQueryScratch {
    query_ids: Vec<StringId>,
    query_id_seen: AHashSet<StringId>,
    query_chars: Vec<char>,
    query_sorted: Vec<char>,
    candidates: Vec<u32>,
    candidate_seen: NameCandidateSeen,
    emitted: AHashSet<(ContractId, ChainId)>,
}

impl NameQueryScratch {
    pub fn for_worker_pool(name_count: usize, worker_count: usize) -> Self {
        let dense = name_count
            .checked_mul(std::mem::size_of::<u16>())
            .and_then(|bytes| bytes.checked_mul(worker_count.max(1)))
            .is_some_and(|bytes| bytes <= DENSE_NAME_SEEN_BUDGET_BYTES);
        Self {
            query_ids: Vec::new(),
            query_id_seen: AHashSet::new(),
            query_chars: Vec::new(),
            query_sorted: Vec::new(),
            candidates: Vec::new(),
            candidate_seen: if dense {
                NameCandidateSeen::Dense {
                    generations: vec![0; name_count],
                    generation: 0,
                }
            } else {
                NameCandidateSeen::Sparse(AHashSet::new())
            },
            emitted: AHashSet::new(),
        }
    }

    fn sparse() -> Self {
        Self {
            query_ids: Vec::new(),
            query_id_seen: AHashSet::new(),
            query_chars: Vec::new(),
            query_sorted: Vec::new(),
            candidates: Vec::new(),
            candidate_seen: NameCandidateSeen::Sparse(AHashSet::new()),
            emitted: AHashSet::new(),
        }
    }
}

/// Build EVM representatives, Solana NFT name CSR, and length-sorted name keys.
pub fn finalize_name_index(store: &mut ResidentStore) -> Result<(), AnalysisError> {
    finalize_name_index_with_progress(store, &NoopProgress)
}

pub(crate) fn finalize_name_index_with_progress(
    store: &mut ResidentStore,
    progress: &dyn ProgressObserver,
) -> Result<(), AnalysisError> {
    if store.contract_nft_csr.is_empty() && !store.nfts.is_empty() {
        store.rebuild_contract_nft_csr();
    }
    for contract in &mut store.contracts {
        contract.name_id = None;
    }

    progress.begin_phase("name_representatives", Some(1));
    let reps = select_evm_representatives(store);
    progress.add_completed(1);
    for &(contract_id, name_id) in &reps {
        store.contracts[contract_id as usize].name_id = Some(name_id);
    }

    let mut contract_pairs: Vec<(u32, u32)> = reps
        .iter()
        .map(|&(contract_id, name_id)| (name_id, contract_id))
        .collect();
    contract_pairs.sort_unstable();
    store.name_contract_csr = crate::entity::CsrIndex::from_sorted_pairs(&contract_pairs);

    progress.begin_phase("name_nft_postings", Some(store.nfts.len() as u64));
    let mut nft_pairs: Vec<(u32, u32)> = store
        .nfts
        .par_iter()
        .enumerate()
        .filter_map(|(nft_id, nft)| {
            let name_id = nft.name_id?;
            let contract = &store.contracts[nft.contract_id as usize];
            let chain = store.chain_name(contract.chain_id);
            (!store.is_evm_chain(chain) && usable(store.string(name_id)))
                .then_some((name_id, nft_id as u32))
        })
        .collect();
    nft_pairs.par_sort_unstable();
    progress.add_completed(store.nfts.len() as u64);
    store.name_nft_csr = crate::entity::CsrIndex::from_sorted_pairs(&nft_pairs);

    // Unique name keys from both CSRs, sorted by char length then text for windows.
    let mut keys =
        Vec::with_capacity(store.name_contract_csr.keys.len() + store.name_nft_csr.keys.len());
    keys.extend_from_slice(&store.name_contract_csr.keys);
    keys.extend_from_slice(&store.name_nft_csr.keys);
    keys.par_sort_unstable();
    keys.dedup();
    let mut keyed: Vec<(usize, StringId)> = keys
        .into_par_iter()
        .map(|id| (char_len_fast(store.string(id)), id))
        .collect();
    keyed.par_sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| store.string(a.1).cmp(store.string(b.1)))
    });
    progress.begin_phase("name_profiles", Some(keyed.len() as u64));
    let profile_chunks = keyed
        .par_chunks(1_024)
        .map(|chunk| {
            progress.check_cancelled()?;
            let profiles = chunk
                .iter()
                .map(|&(char_len, id)| {
                    let mut sorted_chars = store.string(id).chars().collect::<Vec<_>>();
                    sorted_chars.sort_unstable();
                    (char_len, id, sorted_chars)
                })
                .collect::<Vec<_>>();
            progress.add_completed(chunk.len() as u64);
            Ok::<_, AnalysisError>(profiles)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let profiles = profile_chunks.into_iter().flatten().collect::<Vec<_>>();

    store.name_keys_by_len.clear();
    store.name_key_char_lens.clear();
    store.name_sorted_char_offsets.clear();
    store.name_sorted_chars.clear();
    store.name_key_positions.clear();
    store.name_key_positions.reserve(profiles.len());
    store.name_occurrence_token_offsets.clear();
    store.name_occurrence_token_offsets.push(0);
    store.name_occurrence_tokens.clear();
    store.name_occurrence_postings = crate::entity::CsrIndex::new();
    store.name_keys_by_len.reserve(profiles.len());
    store.name_key_char_lens.reserve(profiles.len());
    store.name_sorted_char_offsets.reserve(profiles.len() + 1);
    store.name_sorted_char_offsets.push(0);
    store.name_occurrence_token_offsets.reserve(profiles.len());

    u32::try_from(profiles.len())
        .map_err(|_| AnalysisError::invalid("too many indexed names for u32"))?;
    progress.begin_phase("name_profile_layout", Some(profiles.len() as u64));
    let mut pending_progress = 0_u64;
    for (key_index, (char_len, id, sorted_chars)) in profiles.iter().enumerate() {
        if key_index % 1_024 == 0 {
            progress.check_cancelled()?;
        }
        let key_index = key_index as u32;
        let char_len = u32::try_from(*char_len)
            .map_err(|_| AnalysisError::invalid("indexed name is too long for u32"))?;
        store.name_keys_by_len.push(*id);
        store.name_key_char_lens.push(char_len);
        store.name_key_positions.insert(*id, key_index);
        store.name_sorted_chars.extend_from_slice(sorted_chars);
        store
            .name_sorted_char_offsets
            .push(store.name_sorted_chars.len() as u64);
        pending_progress += 1;
        if pending_progress == 1_024 {
            progress.add_completed(pending_progress);
            pending_progress = 0;
        }
    }
    if pending_progress > 0 {
        progress.add_completed(pending_progress);
    }

    // Occurrence tokens encode `(character, occurrence_rank)`. A sorted,
    // deterministic raw-token catalog removes the former serial global hash
    // insertion from the character hot path.
    progress.begin_phase("name_occurrence_catalog", Some(profiles.len() as u64));
    let raw_chunks = profiles
        .par_chunks(1_024)
        .map(|chunk| {
            progress.check_cancelled()?;
            let mut raw = AHashSet::new();
            for (_, _, sorted_chars) in chunk {
                visit_occurrence_tokens(sorted_chars, |token| {
                    raw.insert(token);
                    Ok::<_, std::convert::Infallible>(())
                })
                .expect("infallible occurrence-token collector");
            }
            progress.add_completed(chunk.len() as u64);
            Ok::<_, AnalysisError>(raw.into_iter().collect::<Vec<_>>())
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let mut occurrence_keys = raw_chunks.into_iter().flatten().collect::<Vec<_>>();
    u32::try_from(occurrence_keys.len())
        .map_err(|_| AnalysisError::invalid("too many name occurrence postings"))?;
    occurrence_keys.par_sort_unstable();
    occurrence_keys.dedup();
    u32::try_from(occurrence_keys.len())
        .map_err(|_| AnalysisError::invalid("too many name occurrence tokens"))?;
    let occurrence_ids = occurrence_keys
        .iter()
        .enumerate()
        .map(|(token, &raw)| (raw, token as u32))
        .collect::<AHashMap<_, _>>();

    progress.begin_phase("name_occurrence_tokens", Some(profiles.len() as u64));
    let token_chunks = profiles
        .par_chunks(1_024)
        .map(|chunk| {
            progress.check_cancelled()?;
            let names = chunk
                .iter()
                .map(|(_, _, sorted_chars)| {
                    let mut tokens = Vec::with_capacity(sorted_chars.len());
                    visit_occurrence_tokens(sorted_chars, |raw| {
                        let token = occurrence_ids
                            .get(&raw)
                            .copied()
                            .expect("catalogued occurrence token");
                        tokens.push(token);
                        Ok::<_, std::convert::Infallible>(())
                    })
                    .expect("infallible occurrence-token mapper");
                    tokens
                })
                .collect::<Vec<_>>();
            progress.add_completed(chunk.len() as u64);
            Ok::<_, AnalysisError>(names)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let mut name_tokens = token_chunks.into_iter().flatten().collect::<Vec<_>>();
    let posting_counts = (0..occurrence_keys.len())
        .map(|_| AtomicU32::new(0))
        .collect::<Vec<_>>();
    name_tokens.par_iter().for_each(|tokens| {
        for &token in tokens {
            posting_counts[token as usize].fetch_add(1, Ordering::Relaxed);
        }
    });
    let posting_counts = posting_counts
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect::<Vec<_>>();
    name_tokens.par_iter_mut().for_each(|tokens| {
        tokens.sort_unstable_by(|&left, &right| {
            posting_counts[left as usize]
                .cmp(&posting_counts[right as usize])
                .then_with(|| left.cmp(&right))
        });
    });

    progress.begin_phase("name_occurrence_postings", Some(1));
    let posting_count = name_tokens.iter().map(Vec::len).sum::<usize>();
    u32::try_from(posting_count)
        .map_err(|_| AnalysisError::invalid("too many name occurrence postings"))?;
    let mut posting_pairs = name_tokens
        .par_iter()
        .enumerate()
        .flat_map_iter(|(key_index, tokens)| {
            tokens.iter().map(move |&token| (token, key_index as u32))
        })
        .collect::<Vec<_>>();
    posting_pairs.par_sort_unstable();
    store
        .name_occurrence_token_offsets
        .reserve(name_tokens.len());
    for tokens in name_tokens {
        store.name_occurrence_tokens.extend_from_slice(&tokens);
        store
            .name_occurrence_token_offsets
            .push(store.name_occurrence_tokens.len() as u64);
    }
    store.name_occurrence_postings = crate::entity::CsrIndex::from_sorted_pairs(&posting_pairs);
    debug_assert_eq!(
        store.name_occurrence_postings.key_count(),
        occurrence_keys.len()
    );
    progress.add_completed(1);
    Ok(())
}

/// Query Name for `seed` against the finalized index; emit whole-collection edges.
///
/// - EVM seed: one representative string query.
/// - Solana seed: each NFT name; any hit marks the whole seed→candidate collection
///   (`candidate_nft: None` so HitGraph expands all candidate NFTs).
pub fn query_name_for_seed(
    store: &ResidentStore,
    seed: ContractId,
    threshold: f64,
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
) -> Result<(), AnalysisError> {
    let mut scratch =
        NameQueryScratch::for_worker_pool(store.name_keys_by_len.len(), /*worker_count=*/ 1);
    query_name_for_seed_with_scratch(store, seed, threshold, graph, progress, &mut scratch)
}

pub fn query_name_for_seed_with_scratch(
    store: &ResidentStore,
    seed: ContractId,
    threshold: f64,
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
    scratch: &mut NameQueryScratch,
) -> Result<(), AnalysisError> {
    progress.set_stage("name");
    progress.check_cancelled()?;

    let seed_usize = seed as usize;
    if seed_usize >= store.contracts.len() {
        return Err(AnalysisError::invalid(format!(
            "unknown seed contract id {seed}"
        )));
    }
    let seed_chain = store.contracts[seed_usize].chain_id;
    let seed_chain_name = store.chain_name(seed_chain);
    let seed_is_evm = store.is_evm_chain(seed_chain_name);

    scratch.query_ids.clear();
    scratch.query_id_seen.clear();
    if seed_is_evm {
        if let Some(name_id) = store.contracts[seed_usize].name_id {
            scratch.query_ids.push(name_id);
        }
    } else {
        for &nft_id in store.contract_nft_csr.values_for(seed).unwrap_or(&[]) {
            let nft = &store.nfts[nft_id as usize];
            let Some(name_id) = nft.name_id else {
                continue;
            };
            if usable(store.string(name_id)) && scratch.query_id_seen.insert(name_id) {
                scratch.query_ids.push(name_id);
            }
        }
        // Standalone dedup atomizes each canonical Name once. A Solana
        // collection can contain thousands of NFTs with the same Name, so
        // scoring duplicate query ids again is pure repeated work.
    }

    progress.begin_phase("name_query", Some(scratch.query_ids.len() as u64));
    scratch.emitted.clear();

    if scratch.query_ids.len() > PARALLEL_NAME_QUERY_CHUNK
        && crate::dedup::inner_query_parallel_allowed()
        && rayon::current_num_threads() > 1
    {
        let chunk_graphs = scratch
            .query_ids
            .par_chunks(PARALLEL_NAME_QUERY_CHUNK)
            .map_init(NameQueryScratch::sparse, |worker, query_ids| {
                let mut graph = HitGraph::new();
                worker.emitted.clear();
                for &query_id in query_ids {
                    progress.check_cancelled()?;
                    emit_for_query(
                        store,
                        seed,
                        seed_chain,
                        query_id,
                        store.string(query_id),
                        threshold,
                        &mut graph,
                        worker,
                    );
                    progress.add_completed(1);
                }
                Ok::<_, AnalysisError>(graph)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        // Rayon preserves indexed chunk order. Deduping in that order keeps the
        // previous "first query id wins" score selection deterministic.
        for chunk_graph in chunk_graphs {
            for edge in chunk_graph.into_edges() {
                if scratch
                    .emitted
                    .insert((edge.candidate_contract, edge.secondary_chain))
                {
                    graph.push(edge);
                }
            }
        }
        return Ok(());
    }

    for query_index in 0..scratch.query_ids.len() {
        let query_id = scratch.query_ids[query_index];
        progress.check_cancelled()?;
        emit_for_query(
            store,
            seed,
            seed_chain,
            query_id,
            store.string(query_id),
            threshold,
            graph,
            scratch,
        );
        progress.add_completed(1);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Hot path: grouping these borrowed scratch values adds no clarity.
fn emit_for_query(
    store: &ResidentStore,
    seed: ContractId,
    seed_chain: ChainId,
    query_id: StringId,
    query_text: &str,
    threshold: f64,
    graph: &mut HitGraph,
    scratch: &mut NameQueryScratch,
) {
    // Prefer finalize-time profiles when the query name is already indexed.
    let query_key_index = store.name_key_positions.get(&query_id).map(|&i| i as usize);
    let (query_len, query_sorted_slice) = if let Some(index) = query_key_index {
        let len = store.name_key_char_lens[index] as usize;
        let start = store.name_sorted_char_offsets[index] as usize;
        let end = store.name_sorted_char_offsets[index + 1] as usize;
        (len, Some(&store.name_sorted_chars[start..end]))
    } else {
        let len = char_len_fast(query_text);
        scratch.query_sorted.clear();
        scratch.query_sorted.extend(query_text.chars());
        scratch.query_sorted.sort_unstable();
        (len, None)
    };

    // BatchComparator still needs the original character sequence (not sorted).
    scratch.query_chars.clear();
    scratch.query_chars.extend(query_text.chars());

    // Byte-equal short circuit via CSR exact key.
    if let Some(contracts) = store.name_contract_csr.values_for(query_id) {
        for &cand in contracts {
            push_collection_edge(
                store,
                seed,
                seed_chain,
                cand,
                1.0,
                graph,
                &mut scratch.emitted,
            );
        }
    }
    if let Some(nfts) = store.name_nft_csr.values_for(query_id) {
        for &nft_id in nfts {
            let cand = store.nfts[nft_id as usize].contract_id;
            push_collection_edge(
                store,
                seed,
                seed_chain,
                cand,
                1.0,
                graph,
                &mut scratch.emitted,
            );
        }
    }

    if threshold > 1.0 || threshold.is_nan() {
        return;
    }
    if store.name_keys_by_len.is_empty() {
        return;
    }

    let args = Args::default().score_cutoff(threshold.clamp(0.0, 1.0));
    let prepared = BatchComparator::new(scratch.query_chars.iter().copied());

    // Allocation-free length window over sorted unique names.
    let keys = &store.name_keys_by_len;
    let lengths = &store.name_key_char_lens;
    debug_assert_eq!(keys.len(), lengths.len());
    let lo = lengths.partition_point(|&cand_len| {
        let cand_len = cand_len as usize;
        cand_len < query_len && !CandidateBounds::lengths_can_reach(query_len, cand_len, threshold)
    });
    let hi = lengths.partition_point(|&cand_len| {
        let cand_len = cand_len as usize;
        cand_len <= query_len || CandidateBounds::lengths_can_reach(query_len, cand_len, threshold)
    });

    let query_tokens = query_key_index.and_then(|index| {
        let start = *store.name_occurrence_token_offsets.get(index)? as usize;
        let end = *store.name_occurrence_token_offsets.get(index + 1)? as usize;
        store.name_occurrence_tokens.get(start..end)
    });

    scratch
        .candidate_seen
        .begin_query(store.name_keys_by_len.len());
    let mut group_start = lo;
    while group_start < hi {
        let cand_len = lengths[group_start] as usize;
        let group_end = lengths[group_start..hi]
            .partition_point(|&length| length as usize == cand_len)
            + group_start;
        let required = CandidateBounds::minimum_multiset_overlap(query_len, cand_len, threshold);
        scratch.candidates.clear();
        if required == 0 || query_tokens.is_none() {
            scratch
                .candidates
                .extend((group_start..group_end).map(|index| index as u32));
        } else if let Some(query_tokens) = query_tokens {
            let prefix_len = query_len
                .saturating_sub(required)
                .saturating_add(1)
                .min(query_tokens.len());
            for &token in &query_tokens[..prefix_len] {
                let Some(posting) = store.name_occurrence_postings.values_for(token) else {
                    continue;
                };
                let start = posting.partition_point(|&candidate| candidate < group_start as u32);
                let end = posting.partition_point(|&candidate| candidate < group_end as u32);
                for &candidate in &posting[start..end] {
                    if scratch.candidate_seen.insert(candidate) {
                        scratch.candidates.push(candidate);
                    }
                }
            }
        }
        scratch.candidates.sort_unstable();

        for &key_index in &scratch.candidates {
            let key_index = key_index as usize;
            let name_id = keys[key_index];
            if name_id == query_id {
                continue;
            }
            let cand_text = store.string(name_id);
            let start = store.name_sorted_char_offsets[key_index] as usize;
            let end = store.name_sorted_char_offsets[key_index + 1] as usize;
            let cand_sorted = &store.name_sorted_chars[start..end];
            let query_sorted = query_sorted_slice.unwrap_or(scratch.query_sorted.as_slice());
            if !sorted_overlap_at_least(query_sorted, cand_sorted, required) {
                continue;
            }
            let Some(score) = prepared.similarity_with_args(cand_text.chars(), &args) else {
                continue;
            };
            if let Some(contracts) = store.name_contract_csr.values_for(name_id) {
                for &cand in contracts {
                    push_collection_edge(
                        store,
                        seed,
                        seed_chain,
                        cand,
                        score,
                        graph,
                        &mut scratch.emitted,
                    );
                }
            }
            if let Some(nfts) = store.name_nft_csr.values_for(name_id) {
                for &nft_id in nfts {
                    let cand = store.nfts[nft_id as usize].contract_id;
                    push_collection_edge(
                        store,
                        seed,
                        seed_chain,
                        cand,
                        score,
                        graph,
                        &mut scratch.emitted,
                    );
                }
            }
        }
        group_start = group_end;
    }
}

/// Prefer O(1) byte length for pure-ASCII names (the common NFT case).
#[inline]
fn char_len_fast(text: &str) -> usize {
    if text.is_ascii() {
        text.len()
    } else {
        text.chars().count()
    }
}

fn push_collection_edge(
    store: &ResidentStore,
    seed: ContractId,
    seed_chain: ChainId,
    candidate: ContractId,
    score: f64,
    graph: &mut HitGraph,
    seen: &mut AHashSet<(ContractId, ChainId)>,
) {
    if candidate == seed {
        return;
    }
    let secondary = store.contracts[candidate as usize].chain_id;
    if !seen.insert((candidate, secondary)) {
        return;
    }
    graph.push(HitEdge {
        seed_contract: seed,
        candidate_contract: candidate,
        candidate_nft: None, // whole-collection Name hit
        dimension: Dimension::Name,
        score,
        primary_chain: seed_chain,
        secondary_chain: secondary,
    });
}

fn sorted_overlap_at_least(left: &[char], right: &[char], required: usize) -> bool {
    if required == 0 {
        return true;
    }
    let mut i = 0usize;
    let mut j = 0usize;
    let mut overlap = 0usize;
    while i < left.len() && j < right.len() {
        if overlap + (left.len() - i).min(right.len() - j) < required {
            return false;
        }
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Equal => {
                overlap += 1;
                if overlap >= required {
                    return true;
                }
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => {
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                j += 1;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::hits::ScopeKind;
    use crate::entity::{IdentityRow, NftId, SourceOrder};
    use crate::progress::NoopProgress;
    use crate::reporting::count_scope_nfts;
    use ahash::{AHashMap, AHashSet};

    fn row(chain: &str, contract: &str, token: &str, name: &str, n: u64) -> IdentityRow {
        IdentityRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: name.to_owned(),
            token_uri_norm: String::new(),
            image_uri_norm: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: n,
            },
        }
    }

    fn prepared(evm: &[&str], rows: impl IntoIterator<Item = IdentityRow>) -> ResidentStore {
        let evm_set = evm.iter().map(|c| (*c).to_owned()).collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(Some(8), &evm_set);
        for r in rows {
            store.ingest_identity_row(r).unwrap();
        }
        finalize_name_index(&mut store).unwrap();
        store
    }

    fn cid(store: &ResidentStore, chain: &str, address: &str) -> ContractId {
        store
            .contract_id(chain, address)
            .expect("contract must exist")
    }

    fn nft_map(store: &ResidentStore) -> AHashMap<ContractId, Vec<NftId>> {
        let mut map: AHashMap<ContractId, Vec<NftId>> = AHashMap::new();
        for (nft_id, nft) in store.nfts.iter().enumerate() {
            map.entry(nft.contract_id)
                .or_default()
                .push(nft_id as NftId);
        }
        map
    }

    #[test]
    fn solana_one_nft_hit_marks_whole_candidate_collection() {
        // Seed collection A has one NFT name matching one NFT in collection B;
        // B has 3 NFTs total → Name count expands to all 3.
        let store = prepared(
            &["ethereum"],
            [
                row("solana", "col-a", "mint-1", "SharedName", 1),
                row("solana", "col-a", "mint-2", "OtherUnique", 2),
                row("solana", "col-b", "mint-x", "SharedName", 3),
                row("solana", "col-b", "mint-y", "NoiseOne", 4),
                row("solana", "col-b", "mint-z", "NoiseTwo", 5),
            ],
        );
        let seed = cid(&store, "solana", "col-a");
        let cand = cid(&store, "solana", "col-b");
        let mut graph = HitGraph::new();
        query_name_for_seed(
            &store,
            seed,
            DEFAULT_NAME_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();

        let edge = graph
            .edges()
            .iter()
            .find(|e| e.candidate_contract == cand)
            .expect("candidate collection edge");
        assert_eq!(edge.candidate_nft, None);
        assert_eq!(edge.dimension, Dimension::Name);
        assert_eq!(edge.score, 1.0);

        let sol = store.chain_ids["solana"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::IntraChain,
            sol,
            None,
            &nft_map(&store),
        );
        assert_eq!(counts.name, 3, "whole candidate collection NFT count");
    }

    #[test]
    fn solana_jw_near_match_also_expands_collection() {
        // "CoolCatzzz" vs "CoolCatzzz!" — high JW; one NFT hit → all candidate NFTs.
        let store = prepared(
            &["ethereum"],
            [
                row("solana", "col-a", "m1", "CoolCatCollection", 1),
                row("solana", "col-b", "m1", "CoolCatCollectiom", 2), // last char differs
                row("solana", "col-b", "m2", "UnrelatedNameXYZ", 3),
            ],
        );
        let seed = cid(&store, "solana", "col-a");
        let cand = cid(&store, "solana", "col-b");
        let mut graph = HitGraph::new();
        query_name_for_seed(&store, seed, 0.90, &mut graph, &NoopProgress).unwrap();

        assert!(
            graph
                .edges()
                .iter()
                .any(|e| e.candidate_contract == cand && e.candidate_nft.is_none()),
            "expected whole-collection Name edge"
        );
        let sol = store.chain_ids["solana"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::IntraChain,
            sol,
            None,
            &nft_map(&store),
        );
        assert_eq!(counts.name, 2);
    }

    #[test]
    fn evm_representative_exact_cross_chain() {
        let store = prepared(
            &["ethereum", "base"],
            [
                row("ethereum", "0xa", "1", "Alpha", 1),
                row("ethereum", "0xa", "2", "Alpha", 2),
                row("base", "0xb", "1", "Alpha", 3),
                row("base", "0xb", "2", "Alpha", 4),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "base", "0xb");
        assert_eq!(
            store.string(store.contracts[seed as usize].name_id.unwrap()),
            "Alpha"
        );
        let mut graph = HitGraph::new();
        query_name_for_seed(
            &store,
            seed,
            DEFAULT_NAME_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        let eth = store.chain_ids["ethereum"];
        let base = store.chain_ids["base"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::ChainMatrix,
            eth,
            Some(base),
            &nft_map(&store),
        );
        assert_eq!(counts.name, 2);
        assert!(graph.edges().iter().any(|e| {
            e.candidate_contract == cand && e.candidate_nft.is_none() && e.score == 1.0
        }));
    }

    #[test]
    fn self_hit_excluded() {
        let store = prepared(
            &["ethereum"],
            [
                row("ethereum", "0xa", "1", "Solo", 1),
                row("ethereum", "0xa", "2", "Solo", 2),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let mut graph = HitGraph::new();
        query_name_for_seed(
            &store,
            seed,
            DEFAULT_NAME_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        assert!(graph.is_empty());
    }

    #[test]
    fn cached_length_window_matches_exhaustive_jaro_winkler() {
        let names = [
            "a",
            "ab",
            "abc",
            "abcd",
            "abcde",
            "abcdf",
            "xbcde",
            "abcdefghij",
            "abcdefghijk",
            "zzzzzzzzzzzzzzzz",
            "αβγδε",
        ];
        let rows = names.iter().enumerate().map(|(index, name)| {
            row(
                "ethereum",
                &format!("0x{index:02x}"),
                "1",
                name,
                index as u64,
            )
        });
        let store = prepared(&["ethereum"], rows);

        assert_eq!(
            store.name_sorted_char_offsets.len(),
            store.name_keys_by_len.len() + 1
        );
        assert_eq!(store.name_key_char_lens.len(), store.name_keys_by_len.len());
        assert_eq!(
            store.name_occurrence_token_offsets.len(),
            store.name_keys_by_len.len() + 1
        );

        for threshold in [0.8, 0.98, 1.0] {
            for (seed_index, seed_name) in names.iter().enumerate() {
                let seed = cid(&store, "ethereum", &format!("0x{seed_index:02x}"));
                let mut graph = HitGraph::new();
                query_name_for_seed(&store, seed, threshold, &mut graph, &NoopProgress).unwrap();
                let actual: AHashSet<ContractId> = graph
                    .edges()
                    .iter()
                    .map(|edge| edge.candidate_contract)
                    .collect();
                let expected: AHashSet<ContractId> = names
                    .iter()
                    .enumerate()
                    .filter(|(candidate_index, candidate_name)| {
                        *candidate_index != seed_index
                            && rapidfuzz::distance::jaro_winkler::similarity(
                                seed_name.chars(),
                                candidate_name.chars(),
                            ) >= threshold
                    })
                    .map(|(candidate_index, _)| {
                        cid(&store, "ethereum", &format!("0x{candidate_index:02x}"))
                    })
                    .collect();
                assert_eq!(
                    actual, expected,
                    "seed={seed_name:?}, threshold={threshold}"
                );
            }
        }
    }

    #[test]
    fn parallel_large_seed_query_matches_single_thread_order_and_hits() {
        let mut rows = Vec::new();
        for index in 0..20 {
            let name = format!("SharedCollectionName{index:02}");
            rows.push(row(
                "solana",
                "seed-collection",
                &format!("seed-{index}"),
                &name,
                index as u64,
            ));
            rows.push(row(
                "solana",
                &format!("candidate-{index:02}"),
                &format!("candidate-mint-{index}"),
                &name,
                100 + index as u64,
            ));
        }
        let store = prepared(&["ethereum"], rows);
        let seed = cid(&store, "solana", "seed-collection");

        let run = |threads| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap()
                .install(|| {
                    let mut graph = HitGraph::new();
                    query_name_for_seed(
                        &store,
                        seed,
                        DEFAULT_NAME_THRESHOLD,
                        &mut graph,
                        &NoopProgress,
                    )
                    .unwrap();
                    graph.into_edges()
                })
        };
        assert_eq!(run(1), run(4));
    }
}
