//! Metadata query-to-index engine (descending anchors + BM25).
//!
//! Anchors are selected at load (Task 4). Finalize prepares BM25 documents and a
//! term→document inverted index for lossless rare-prefix candidate probes.
//! Query aligns one document pair per seed↔candidate (largest shared / max each
//! side), then exact canonical match or BM25 cosine (default threshold 0.6).
//! No template / MinHash / LSH / quotas.

mod align;
mod bm25;

pub use align::{AnchorRef, select_documents};
pub use bm25::{
    PreparedDocument, ThresholdDecision, cosine_similarity, similarity_at_least,
    similarity_score_if_at_least,
};

use ahash::AHashMap;
use rayon::prelude::*;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::dedup::hits::{Dimension, HitEdge, HitGraph};
use crate::entity::{ChainId, ContractId, ResidentStore, normalized_evm_token_slice};
use crate::error::AnalysisError;
use crate::progress::{NoopProgress, ProgressObserver};

use self::align::select_documents as align_pair;
use self::bm25::{lossless_prefix_len, visit_tokens};

/// Default BM25 cosine threshold.
pub const DEFAULT_METADATA_THRESHOLD: f64 = 0.6;
const PARALLEL_METADATA_CANDIDATE_CHUNK: usize = 64;
const MAX_METADATA_CANDIDATE_CHUNK: usize = 4 * 1024;

fn metadata_candidate_chunk(candidate_count: usize) -> usize {
    let target_chunks = rayon::current_num_threads().saturating_mul(4).max(1);
    candidate_count.div_ceil(target_chunks).clamp(
        PARALLEL_METADATA_CANDIDATE_CHUNK,
        MAX_METADATA_CANDIDATE_CHUNK,
    )
}

#[derive(Clone, Debug)]
struct DenseCsr<T> {
    offsets: Vec<u32>,
    values: Vec<T>,
}

impl<T> Default for DenseCsr<T> {
    fn default() -> Self {
        Self {
            offsets: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl<T> DenseCsr<T> {
    fn values_for(&self, key: u32) -> Option<&[T]> {
        let index = key as usize;
        let start = *self.offsets.get(index)? as usize;
        let end = *self.offsets.get(index + 1)? as usize;
        self.values.get(start..end)
    }

    fn get(&self, key: usize) -> Option<&[T]> {
        self.values_for(key as u32)
    }

    fn iter(&self) -> impl Iterator<Item = &[T]> {
        self.offsets
            .windows(2)
            .map(|pair| &self.values[pair[0] as usize..pair[1] as usize])
    }
}

impl DenseCsr<ContractId> {
    fn from_first_and_extras(
        first: Vec<ContractId>,
        mut extras: AHashMap<u32, Vec<ContractId>>,
    ) -> Result<Self, AnalysisError> {
        let total = first
            .len()
            .saturating_add(extras.values().map(Vec::len).sum::<usize>());
        let mut offsets = Vec::with_capacity(first.len() + 1);
        let mut values = Vec::with_capacity(total);
        offsets.push(0);
        for (document_id, first_contract) in first.into_iter().enumerate() {
            values.push(first_contract);
            if let Some(mut contracts) = extras.remove(&(document_id as u32)) {
                values.append(&mut contracts);
            }
            offsets.push(
                u32::try_from(values.len())
                    .map_err(|_| AnalysisError::invalid("dense CSR exceeds u32 capacity"))?,
            );
        }
        Ok(Self { offsets, values })
    }
}

impl<T> std::ops::Index<usize> for DenseCsr<T> {
    type Output = [T];

    fn index(&self, index: usize) -> &Self::Output {
        self.values_for(index as u32).expect("dense CSR index")
    }
}

#[derive(Debug, Default)]
struct DenseAtomicCsr {
    offsets: Vec<u32>,
    values: Vec<AtomicU32>,
}

impl Clone for DenseAtomicCsr {
    fn clone(&self) -> Self {
        Self {
            offsets: self.offsets.clone(),
            values: self
                .values
                .iter()
                .map(|value| AtomicU32::new(value.load(Ordering::Relaxed)))
                .collect(),
        }
    }
}

impl DenseAtomicCsr {
    fn values_for(&self, key: u32) -> Option<&[AtomicU32]> {
        let index = key as usize;
        let start = *self.offsets.get(index)? as usize;
        let end = *self.offsets.get(index + 1)? as usize;
        self.values.get(start..end)
    }

    fn len_for(&self, key: u32) -> u32 {
        let index = key as usize;
        self.offsets
            .get(index..=index + 1)
            .map(|pair| pair[1] - pair[0])
            .unwrap_or(0)
    }
}

/// Reusable per-worker Metadata candidate buffers.
#[derive(Default)]
pub struct MetadataQueryScratch {
    /// Dense bit marks replace per-seed hash-set insertion. Contract ids are
    /// contiguous; one bit per contract remains bounded even with one scratch
    /// per Rayon worker.
    candidate_bits: Vec<u64>,
    ordered_terms: Vec<(u32, u32, u32)>,
    frequencies: Vec<u32>,
    seed_documents: Vec<u32>,
    output: Vec<ContractId>,
    /// Cached BM25 decisions for aligned `(left_doc, right_doc)` pairs this seed.
    /// `None` means scored below threshold.
    score_cache: AHashMap<(u32, u32), Option<f64>>,
}

impl MetadataQueryScratch {
    fn begin_candidates(&mut self, contract_count: usize) {
        for contract_id in self.output.drain(..) {
            let index = contract_id as usize;
            self.candidate_bits[index / 64] &= !(1_u64 << (index % 64));
        }
        let words = contract_count.div_ceil(64);
        if self.candidate_bits.len() < words {
            self.candidate_bits.resize(words, 0);
        }
    }
}

fn mark_candidate(bits: &mut [u64], output: &mut Vec<ContractId>, contract_id: ContractId) {
    let index = contract_id as usize;
    let mask = 1_u64 << (index % 64);
    let word = &mut bits[index / 64];
    if *word & mask == 0 {
        *word |= mask;
        output.push(contract_id);
    }
}

struct MetadataSeedQuery<'a> {
    store: &'a ResidentStore,
    index: &'a MetadataIndex,
    seed: ContractId,
    seed_chain: ChainId,
    seed_is_evm: bool,
    seed_anchors: &'a [AnchorRef],
    threshold: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct MetadataQueryStats {
    exact_pairs: u64,
    pair_cache_hits: u64,
    pairs_scored: u64,
}

impl MetadataQueryStats {
    fn merge(&mut self, other: Self) {
        self.exact_pairs += other.exact_pairs;
        self.pair_cache_hits += other.pair_cache_hits;
        self.pairs_scored += other.pairs_scored;
    }
}

impl MetadataSeedQuery<'_> {
    fn edge_for_candidate(
        &self,
        candidate: ContractId,
        score_cache: &mut AHashMap<(u32, u32), Option<f64>>,
        stats: &mut MetadataQueryStats,
    ) -> Option<HitEdge> {
        if candidate == self.seed {
            return None;
        }
        let candidate_index = candidate as usize;
        let candidate_anchors = self.index.contract_anchors.get(candidate_index)?;
        if candidate_anchors.is_empty() {
            return None;
        }
        let candidate_is_evm = self.index.contract_is_evm[candidate_index];
        let (left_doc, right_doc) = align_pair(
            self.seed_is_evm,
            self.seed_anchors,
            candidate_is_evm,
            candidate_anchors,
        )?;
        let score = if left_doc == right_doc {
            stats.exact_pairs += 1;
            1.0
        } else {
            let cache_key = if left_doc <= right_doc {
                (left_doc, right_doc)
            } else {
                (right_doc, left_doc)
            };
            if let Some(cached) = score_cache.get(&cache_key) {
                stats.pair_cache_hits += 1;
                (*cached)?
            } else {
                stats.pairs_scored += 1;
                let left = &self.index.documents[left_doc as usize];
                let right = &self.index.documents[right_doc as usize];
                let left_terms = self.index.document_terms(left_doc);
                let right_terms = self.index.document_terms(right_doc);
                let score = similarity_score_if_at_least(
                    left,
                    left_terms,
                    right,
                    right_terms,
                    self.threshold,
                );
                score_cache.insert(cache_key, score);
                score?
            }
        };
        Some(HitEdge {
            seed_contract: self.seed,
            candidate_contract: candidate,
            candidate_nft: None,
            dimension: Dimension::Metadata,
            score,
            primary_chain: self.seed_chain,
            secondary_chain: self.store.contracts[candidate_index].chain_id,
        })
    }
}

/// Prepared BM25 documents + inverted term postings + per-contract anchor refs.
#[derive(Clone, Debug, Default)]
pub struct MetadataIndex {
    documents: Vec<PreparedDocument>,
    terms: Vec<(u32, u32)>,
    /// `document_id` → contracts that hold this canonical document as an anchor.
    doc_contracts: DenseCsr<ContractId>,
    /// Parallel to `ResidentStore::contracts`.
    contract_anchors: DenseCsr<AnchorRef>,
    contract_is_evm: Vec<bool>,
    /// term_id → sorted unique document ids (full inverted index).
    term_postings: DenseAtomicCsr,
}

impl MetadataIndex {
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    fn document_terms(&self, document_id: u32) -> &[(u32, u32)] {
        self.documents[document_id as usize].terms(&self.terms)
    }

    #[cfg(test)]
    fn cosine_between(&self, left_doc: u32, right_doc: u32) -> f64 {
        cosine_similarity(
            &self.documents[left_doc as usize],
            self.document_terms(left_doc),
            &self.documents[right_doc as usize],
            self.document_terms(right_doc),
        )
    }
}

struct RawMetadataDocument<'a> {
    /// Unique terms in first-token occurrence order. Keeping this order lets
    /// the global catalog retain the exact historical term-id assignment.
    terms: Vec<(&'a str, u32)>,
}

const INLINE_METADATA_TERMS: usize = 16;

const TERM_INTERNER_SHARDS: usize = 256;

/// Read-mostly sharded term catalog. Common JSON keys are looked up under a
/// shared read lock, while genuinely new terms only serialize within one of
/// 256 independent shards.
struct ParallelTermInterner<'a> {
    shards: Box<[RwLock<AHashMap<&'a str, u32>>]>,
    hash_builder: ahash::RandomState,
    next_id: AtomicU32,
}

impl<'a> ParallelTermInterner<'a> {
    fn new() -> Self {
        let shards = (0..TERM_INTERNER_SHARDS)
            .map(|_| RwLock::new(AHashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            hash_builder: ahash::RandomState::with_seeds(1, 2, 3, 4),
            next_id: AtomicU32::new(0),
        }
    }

    fn intern(&self, term: &'a str) -> Result<u32, AnalysisError> {
        debug_assert!(self.shards.len().is_power_of_two());
        let shard_index = self.hash_builder.hash_one(term) as usize & (self.shards.len() - 1);
        let shard = &self.shards[shard_index];
        if let Some(&id) = shard
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(term)
        {
            return Ok(id);
        }
        let mut terms = shard
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(&id) = terms.get(term) {
            return Ok(id);
        }
        let id = self
            .next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                (next < u32::MAX).then_some(next + 1)
            })
            .map_err(|_| AnalysisError::invalid("too many BM25 terms for u32"))?;
        terms.insert(term, id);
        Ok(id)
    }

    fn len(&self) -> usize {
        self.next_id.load(Ordering::Relaxed) as usize
    }

    fn into_indexed_values(self) -> Result<Vec<&'a str>, AnalysisError> {
        let mut values = vec![None; self.len()];
        for shard in self.shards {
            let entries = shard
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (value, id) in entries {
                values[id as usize] = Some(value);
            }
        }
        values
            .into_iter()
            .map(|value| value.ok_or_else(|| AnalysisError::invalid("metadata interner id gap")))
            .collect()
    }
}

struct InternedMetadataDocument {
    document: PreparedDocument,
    terms: Vec<(u32, u32)>,
}

fn tokenize_and_intern_metadata_document<'a>(
    canonical: &'a str,
    interner: &ParallelTermInterner<'a>,
) -> Result<InternedMetadataDocument, AnalysisError> {
    let raw = tokenize_metadata_document(canonical);
    let mut terms = Vec::with_capacity(raw.terms.len());
    for (term, frequency) in raw.terms {
        terms.push((interner.intern(term)?, frequency));
    }
    terms.sort_unstable_by_key(|&(term, _)| term);
    let document = PreparedDocument::from_compact_terms(&terms);
    Ok(InternedMetadataDocument { document, terms })
}

fn tokenize_metadata_document(canonical: &str) -> RawMetadataDocument<'_> {
    // Most canonical metadata anchors are small. Avoid a separate hash-table
    // allocation until a document actually exceeds the inline linear-scan
    // window; larger documents retain O(1) frequency updates.
    let mut positions = None::<AHashMap<&str, usize>>;
    let mut terms = Vec::<(&str, u32)>::new();
    visit_tokens(canonical, |term| {
        let existing = positions
            .as_ref()
            .and_then(|positions| positions.get(term).copied())
            .or_else(|| {
                positions
                    .is_none()
                    .then(|| terms.iter().position(|&(candidate, _)| candidate == term))
                    .flatten()
            });
        if let Some(position) = existing {
            terms[position].1 = terms[position].1.saturating_add(1);
            return Ok::<_, std::convert::Infallible>(());
        }
        if terms.len() == INLINE_METADATA_TERMS {
            positions = Some(
                terms
                    .iter()
                    .enumerate()
                    .map(|(position, &(candidate, _))| (candidate, position))
                    .collect(),
            );
        }
        if let Some(positions) = &mut positions {
            positions.insert(term, terms.len());
        }
        terms.push((term, 1));
        Ok::<_, std::convert::Infallible>(())
    })
    .expect("infallible metadata tokenizer");
    RawMetadataDocument { terms }
}

/// Build BM25 prepared documents and lossless-capable term postings from anchors.
pub fn finalize_metadata_index(store: &mut ResidentStore) -> Result<(), AnalysisError> {
    finalize_metadata_index_with_progress(store, &NoopProgress)
}

/// Progress-aware metadata finalize used by the full load pipeline.
pub fn finalize_metadata_index_with_progress(
    store: &mut ResidentStore,
    progress: &dyn ProgressObserver,
) -> Result<(), AnalysisError> {
    const PROGRESS_BATCH: usize = 1 << 10;
    const CATALOG_BATCH: usize = 4 * 1024;
    const TOKENIZE_BATCH: usize = 4 * 1024;

    let n_contracts = store.contracts.len();
    let anchor_count: usize = store
        .contracts
        .iter()
        .map(|contract| contract.metadata_by_token.len())
        .sum();
    // Sharded borrowed-string catalogs let the expensive canonical/token hash
    // work run in parallel without duplicating the resident strings.
    let canonical_interner = ParallelTermInterner::new();
    let token_interner = ParallelTermInterner::new();
    // Most canonical documents occur in one contract. Store that common case
    // inline and allocate a side vector only for actual cross-contract reuse.
    let mut doc_first_contract = Vec::<ContractId>::new();
    let mut doc_extra_contracts = AHashMap::<u32, Vec<ContractId>>::new();

    let mut contract_anchor_offsets = Vec::with_capacity(n_contracts + 1);
    let mut contract_anchor_values = Vec::with_capacity(anchor_count);
    contract_anchor_offsets.push(0);
    let mut contract_is_evm: Vec<bool> = vec![false; n_contracts];

    progress.begin_phase("metadata_catalog", Some(n_contracts as u64));
    for contract_batch in store.contracts.chunks(CATALOG_BATCH) {
        progress.check_cancelled()?;
        let catalog_batch = contract_batch
            .par_iter()
            .map(|contract| {
                let chain = store.chain_name(contract.chain_id);
                let is_evm = store.is_evm_chain(chain);
                let mut anchors = Vec::with_capacity(contract.metadata_by_token.len());
                for record in &contract.metadata_by_token {
                    let document_id = canonical_interner.intern(&record.canonical_json)?;
                    let token = if is_evm {
                        normalized_evm_token_slice(&record.token_id)
                    } else {
                        record.token_id.as_str()
                    };
                    anchors.push(AnchorRef {
                        token_key: token_interner.intern(token)?,
                        document_id,
                    });
                }
                Ok::<_, AnalysisError>((contract.id, is_evm, anchors))
            })
            .collect::<Vec<_>>();

        doc_first_contract.resize(canonical_interner.len(), ContractId::MAX);
        for result in catalog_batch {
            let (contract_id, is_evm, anchors) = result?;
            contract_is_evm[contract_id as usize] = is_evm;
            for anchor in &anchors {
                let first = &mut doc_first_contract[anchor.document_id as usize];
                if *first == ContractId::MAX {
                    *first = contract_id;
                } else if *first != contract_id {
                    let contracts = doc_extra_contracts.entry(anchor.document_id).or_default();
                    if contracts.last().copied() != Some(contract_id) {
                        // Batches are consumed in contract-id order, preserving
                        // sorted document postings without a later sort.
                        contracts.push(contract_id);
                    }
                }
            }
            contract_anchor_values.extend(anchors);
            contract_anchor_offsets.push(
                u32::try_from(contract_anchor_values.len())
                    .map_err(|_| AnalysisError::invalid("too many metadata anchors for u32"))?,
            );
        }
        progress.add_completed(contract_batch.len() as u64);
    }
    drop(token_interner);
    let canonical_documents = canonical_interner.into_indexed_values()?;

    // Tokenize a bounded parallel batch, immediately intern and compact it,
    // then release its borrowed/raw term vectors. Keeping raw tokenization for
    // every document until a second pass made peak RSS grow with the complete
    // metadata corpus (tens of millions of documents in production).
    progress.begin_phase("metadata_tokenize", Some(canonical_documents.len() as u64));
    let term_interner = ParallelTermInterner::new();
    let mut documents = Vec::with_capacity(canonical_documents.len());
    let mut terms = Vec::new();
    let mut document_frequency = Vec::<u32>::new();
    for canonical_batch in canonical_documents.chunks(TOKENIZE_BATCH) {
        progress.check_cancelled()?;
        let prepared_batch = canonical_batch
            .par_iter()
            .map(|&canonical| tokenize_and_intern_metadata_document(canonical, &term_interner))
            .collect::<Vec<_>>();
        document_frequency.resize(term_interner.len(), 0);
        for prepared in prepared_batch {
            let InternedMetadataDocument {
                mut document,
                terms: document_terms,
            } = prepared?;
            let term_start = u32::try_from(terms.len())
                .map_err(|_| AnalysisError::invalid("too many metadata terms for u32"))?;
            document.set_term_start(term_start);
            for &(term, _) in &document_terms {
                document_frequency[term as usize] =
                    document_frequency[term as usize].saturating_add(1);
            }
            terms.extend(document_terms);
            documents.push(document);
        }
        progress.add_completed(canonical_batch.len() as u64);
    }
    drop(term_interner);
    drop(canonical_documents);
    store.clear_metadata_anchors();

    let shared_documents = doc_extra_contracts.len();
    let extra_contract_refs = doc_extra_contracts.values().map(Vec::len).sum::<usize>();
    let total_postings = document_frequency
        .iter()
        .map(|&frequency| u64::from(frequency))
        .sum::<u64>();
    let max_document_frequency = document_frequency.iter().copied().max().unwrap_or(0);
    progress.begin_phase("metadata_postings", Some(documents.len() as u64));
    let term_postings = build_term_postings(
        &documents,
        &terms,
        &document_frequency,
        progress,
        PROGRESS_BATCH,
    )?;
    eprintln!(
        "metadata/index: contracts={} anchors={} documents={} shared_documents={} extra_contract_refs={} terms={} postings={} max_df={}",
        n_contracts,
        anchor_count,
        documents.len(),
        shared_documents,
        extra_contract_refs,
        document_frequency.len(),
        total_postings,
        max_document_frequency,
    );

    store.metadata_index = MetadataIndex {
        documents,
        terms,
        doc_contracts: DenseCsr::from_first_and_extras(doc_first_contract, doc_extra_contracts)?,
        contract_anchors: DenseCsr {
            offsets: contract_anchor_offsets,
            values: contract_anchor_values,
        },
        contract_is_evm,
        term_postings,
    };
    Ok(())
}

fn build_term_postings(
    documents: &[PreparedDocument],
    terms: &[(u32, u32)],
    document_frequency: &[u32],
    progress: &dyn ProgressObserver,
    progress_batch: usize,
) -> Result<DenseAtomicCsr, AnalysisError> {
    let term_count = u32::try_from(document_frequency.len())
        .map_err(|_| AnalysisError::invalid("too many BM25 terms for u32"))?;
    let mut offsets = Vec::with_capacity(document_frequency.len() + 1);
    offsets.push(0_u32);
    let mut total = 0_u64;
    for &frequency in document_frequency {
        total = total.saturating_add(u64::from(frequency));
        offsets.push(
            u32::try_from(total)
                .map_err(|_| AnalysisError::invalid("too many metadata postings for u32"))?,
        );
    }

    let cursors = offsets[..document_frequency.len()]
        .iter()
        .map(|&offset| AtomicU32::new(offset))
        .collect::<Vec<_>>();
    // Atomic values remain the final index storage, so parallel construction
    // needs no equally large conversion buffer.
    let values = (0..total).map(|_| AtomicU32::new(0)).collect::<Vec<_>>();
    if documents.len() >= progress_batch && rayon::current_num_threads() > 1 {
        documents
            .par_chunks(progress_batch)
            .enumerate()
            .try_for_each(|(chunk_index, chunk)| {
                progress.check_cancelled()?;
                let document_start = chunk_index * progress_batch;
                for (offset, document) in chunk.iter().enumerate() {
                    let document_id = u32::try_from(document_start + offset).map_err(|_| {
                        AnalysisError::invalid("too many metadata documents for u32")
                    })?;
                    for &(term, _) in document.terms(terms) {
                        let position =
                            cursors[term as usize].fetch_add(1, Ordering::Relaxed) as usize;
                        values[position].store(document_id, Ordering::Relaxed);
                    }
                }
                progress.add_completed(chunk.len() as u64);
                Ok::<_, AnalysisError>(())
            })?;
        debug_assert_eq!(offsets.len(), term_count as usize + 1);
        return Ok(DenseAtomicCsr { offsets, values });
    }

    let mut pending_progress = 0_u64;
    for (document_id, document) in documents.iter().enumerate() {
        if document_id % progress_batch == 0 {
            progress.check_cancelled()?;
        }
        let document_id = u32::try_from(document_id)
            .map_err(|_| AnalysisError::invalid("too many metadata documents for u32"))?;
        for &(term, _) in document.terms(terms) {
            let position = cursors[term as usize].fetch_add(1, Ordering::Relaxed) as usize;
            values[position].store(document_id, Ordering::Relaxed);
        }
        pending_progress += 1;
        if pending_progress as usize == progress_batch {
            progress.add_completed(pending_progress);
            pending_progress = 0;
        }
    }
    if pending_progress > 0 {
        progress.add_completed(pending_progress);
    }

    debug_assert_eq!(offsets.len(), term_count as usize + 1);
    Ok(DenseAtomicCsr { offsets, values })
}

/// Query Metadata for `seed` against the finalized index; emit whole-contract edges.
///
/// Hits use `candidate_nft: None` so scope helpers expand all candidate NFTs.
pub fn query_metadata_for_seed(
    store: &ResidentStore,
    seed: ContractId,
    threshold: f64,
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
) -> Result<(), AnalysisError> {
    let mut scratch = MetadataQueryScratch::default();
    query_metadata_for_seed_with_scratch(store, seed, threshold, graph, progress, &mut scratch)
}

pub fn query_metadata_for_seed_with_scratch(
    store: &ResidentStore,
    seed: ContractId,
    threshold: f64,
    graph: &mut HitGraph,
    progress: &dyn ProgressObserver,
    scratch: &mut MetadataQueryScratch,
) -> Result<(), AnalysisError> {
    progress.set_stage("metadata");
    progress.check_cancelled()?;

    let seed_usize = seed as usize;
    if seed_usize >= store.contracts.len() {
        return Err(AnalysisError::invalid(format!(
            "unknown seed contract id {seed}"
        )));
    }
    let index = &store.metadata_index;
    if index.is_empty() {
        return Ok(());
    }
    let seed_anchors = index.contract_anchors.get(seed_usize).unwrap_or(&[]);
    if seed_anchors.is_empty() {
        return Ok(());
    }

    let seed_chain = store.contracts[seed_usize].chain_id;
    let seed_is_evm = index.contract_is_evm[seed_usize];

    collect_candidates(index, seed, seed_anchors, threshold, scratch);
    scratch.score_cache.clear();
    progress.begin_phase("metadata_query", Some(scratch.output.len() as u64));
    let query = MetadataSeedQuery {
        store,
        index,
        seed,
        seed_chain,
        seed_is_evm,
        seed_anchors,
        threshold,
    };

    if scratch.output.len() > PARALLEL_METADATA_CANDIDATE_CHUNK
        && crate::dedup::inner_query_parallel_allowed()
        && rayon::current_num_threads() > 1
    {
        let chunk_size = metadata_candidate_chunk(scratch.output.len());
        let chunks = scratch
            .output
            .par_chunks(chunk_size)
            .map(|candidates| {
                progress.check_cancelled()?;
                let mut edges = Vec::new();
                let mut local_cache = AHashMap::with_capacity(candidates.len());
                let mut stats = MetadataQueryStats::default();
                for &candidate in candidates {
                    if let Some(edge) =
                        query.edge_for_candidate(candidate, &mut local_cache, &mut stats)
                    {
                        edges.push(edge);
                    }
                }
                progress.add_completed(candidates.len() as u64);
                Ok::<_, AnalysisError>((edges, stats))
            })
            .collect::<Vec<_>>();
        // Indexed chunks are collected in candidate order, preserving stable
        // edge order across thread counts.
        let mut stats = MetadataQueryStats::default();
        let mut emitted = 0_u64;
        for chunk in chunks {
            let (edges, chunk_stats) = chunk?;
            stats.merge(chunk_stats);
            emitted += edges.len() as u64;
            for edge in edges {
                graph.push(edge);
            }
        }
        eprintln!(
            "metadata/query: seed={} candidates={} exact_pairs={} pair_cache_hits={} pairs_scored={} emitted={}",
            seed,
            scratch.output.len(),
            stats.exact_pairs,
            stats.pair_cache_hits,
            stats.pairs_scored,
            emitted,
        );
        return Ok(());
    }

    let mut stats = MetadataQueryStats::default();
    let mut emitted = 0_u64;
    for (position, &candidate) in scratch.output.iter().enumerate() {
        if position % 256 == 0 {
            progress.check_cancelled()?;
        }
        if let Some(edge) =
            query.edge_for_candidate(candidate, &mut scratch.score_cache, &mut stats)
        {
            graph.push(edge);
            emitted += 1;
        }
        if position % 256 == 255 {
            progress.add_completed(256);
        }
    }
    let remainder = scratch.output.len() % 256;
    if remainder > 0 {
        progress.add_completed(remainder as u64);
    }
    eprintln!(
        "metadata/query: seed={} candidates={} exact_pairs={} pair_cache_hits={} pairs_scored={} emitted={}",
        seed,
        scratch.output.len(),
        stats.exact_pairs,
        stats.pair_cache_hits,
        stats.pairs_scored,
        emitted,
    );
    Ok(())
}

fn collect_candidates(
    index: &MetadataIndex,
    seed: ContractId,
    seed_anchors: &[AnchorRef],
    threshold: f64,
    scratch: &mut MetadataQueryScratch,
) {
    scratch.begin_candidates(index.contract_is_evm.len());
    scratch.seed_documents.clear();
    for anchor in seed_anchors {
        if !scratch.seed_documents.contains(&anchor.document_id) {
            scratch.seed_documents.push(anchor.document_id);
        }
    }

    // Exact document reuse is always a candidate (byte-identical canonical JSON).
    for &document_id in &scratch.seed_documents {
        if let Some(contracts) = index.doc_contracts.values_for(document_id) {
            for &contract_id in contracts {
                if contract_id != seed {
                    mark_candidate(
                        &mut scratch.candidate_bits,
                        &mut scratch.output,
                        contract_id,
                    );
                }
            }
        }
    }

    if threshold.is_nan() || threshold > 1.0 {
        scratch.output.sort_unstable();
        return;
    }

    if threshold <= 0.0 {
        // Every other contract with anchors can match.
        for (contract_id, anchors) in index.contract_anchors.iter().enumerate() {
            if contract_id as ContractId != seed && !anchors.is_empty() {
                mark_candidate(
                    &mut scratch.candidate_bits,
                    &mut scratch.output,
                    contract_id as ContractId,
                );
            }
        }
        scratch.output.sort_unstable();
        return;
    }

    // Lossless rare-prefix probe: any BM25≥threshold pair shares a seed prefix term.
    for &document_id in &scratch.seed_documents {
        let doc_terms = index.document_terms(document_id);
        if doc_terms.is_empty() {
            continue;
        }
        scratch.ordered_terms.clear();
        scratch
            .ordered_terms
            .extend(doc_terms.iter().map(|&(term, frequency)| {
                let df = index.term_postings.len_for(term);
                (df, term, frequency)
            }));
        scratch.ordered_terms.sort_unstable();
        scratch.frequencies.clear();
        scratch.frequencies.extend(
            scratch
                .ordered_terms
                .iter()
                .map(|(_, _, frequency)| *frequency),
        );
        let prefix_len = lossless_prefix_len(&scratch.frequencies, threshold);
        for &(_, term, _) in scratch.ordered_terms.iter().take(prefix_len) {
            if let Some(docs) = index.term_postings.values_for(term) {
                for document_id in docs {
                    let document_id = document_id.load(Ordering::Relaxed);
                    if let Some(contracts) = index.doc_contracts.values_for(document_id) {
                        for &contract_id in contracts {
                            if contract_id != seed {
                                mark_candidate(
                                    &mut scratch.candidate_bits,
                                    &mut scratch.output,
                                    contract_id,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    scratch.output.sort_unstable();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::hits::ScopeKind;
    use crate::entity::{IdentityRow, SourceOrder};
    use crate::progress::NoopProgress;
    use crate::reporting::count_scope_nfts;
    use ahash::{AHashMap, AHashSet};

    fn row(chain: &str, contract: &str, token: &str, n: u64) -> IdentityRow {
        IdentityRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: String::new(),
            token_uri_norm: String::new(),
            image_uri_norm: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: n,
            },
        }
    }

    fn anchor(
        store: &mut ResidentStore,
        chain: &str,
        contract: &str,
        token: &str,
        canonical: &str,
        n: u64,
    ) {
        store
            .ingest_metadata_anchor(
                chain,
                contract,
                token,
                canonical.to_owned(),
                SourceOrder {
                    file_ordinal: 0,
                    file_row_number: n,
                },
            )
            .unwrap();
    }

    fn prepared(
        evm: &[&str],
        k: usize,
        rows: impl IntoIterator<Item = IdentityRow>,
        anchors: impl IntoIterator<Item = (&'static str, &'static str, &'static str, &'static str, u64)>,
    ) -> ResidentStore {
        let evm_set = evm.iter().map(|c| (*c).to_owned()).collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(Some(k), &evm_set);
        for r in rows {
            store.ingest_identity_row(r).unwrap();
        }
        for (chain, contract, token, canonical, n) in anchors {
            anchor(&mut store, chain, contract, token, canonical, n);
        }
        finalize_metadata_index(&mut store).unwrap();
        store
    }

    fn cid(store: &ResidentStore, chain: &str, address: &str) -> ContractId {
        store
            .contract_id(chain, address)
            .expect("contract must exist")
    }

    fn nft_map(store: &ResidentStore) -> AHashMap<ContractId, Vec<crate::entity::NftId>> {
        let mut map: AHashMap<ContractId, Vec<_>> = AHashMap::new();
        for (nft_id, nft) in store.nfts.iter().enumerate() {
            map.entry(nft.contract_id).or_default().push(nft_id as u32);
        }
        map
    }

    fn prepare_parts(texts: &[&str]) -> Vec<(PreparedDocument, Vec<(u32, u32)>)> {
        let mut term_ids: AHashMap<String, u32> = AHashMap::new();
        texts
            .iter()
            .map(|text| {
                let parts = PreparedDocument::try_new(text, |term| {
                    if let Some(&id) = term_ids.get(term) {
                        return Ok::<_, std::convert::Infallible>(id);
                    }
                    let id = term_ids.len() as u32;
                    term_ids.insert(term.to_owned(), id);
                    Ok(id)
                })
                .unwrap();
                (parts.document, parts.terms)
            })
            .collect()
    }

    #[test]
    fn descending_anchors_from_load_order_are_largest_first() {
        // Mirrors Task 4: tokens 1,2,10 with k=2 → descending [10, 2].
        let store = prepared(
            &["ethereum"],
            2,
            [
                row("ethereum", "0xa", "1", 1),
                row("ethereum", "0xa", "2", 2),
                row("ethereum", "0xa", "10", 3),
            ],
            [
                ("ethereum", "0xa", "1", r#"{"name":"t1"}"#, 1),
                ("ethereum", "0xa", "2", r#"{"name":"t2"}"#, 2),
                ("ethereum", "0xa", "10", r#"{"name":"t10"}"#, 3),
            ],
        );
        let refs = &store.metadata_index.contract_anchors[0];
        assert_eq!(refs.len(), 2);
        assert_eq!(
            refs.iter()
                .map(|anchor| anchor.document_id)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert!(store.contracts[0].metadata_by_token.is_empty());
    }

    #[test]
    fn borrowed_evm_token_normalization_matches_owned_contract() {
        assert_eq!(normalized_evm_token_slice("010"), "10");
        assert_eq!(normalized_evm_token_slice("000"), "0");
        assert_eq!(
            normalized_evm_token_slice("000123456789012345678901234567890"),
            "123456789012345678901234567890"
        );
        assert_eq!(normalized_evm_token_slice(" token "), " token ");
        assert_eq!(normalized_evm_token_slice("   "), "   ");
    }

    #[test]
    fn bm25_threshold_match_and_mismatch_oracle() {
        let docs = prepare_parts(&[
            "alpha beta gamma delta epsilon zeta eta theta",
            "alpha beta gamma delta epsilon zeta eta theta",
            "alpha beta gamma delta epsilon zeta eta theta iota",
            "completely unrelated vocabulary one two three four",
        ]);
        assert!(
            similarity_at_least(
                &docs[0].0,
                &docs[0].1,
                &docs[1].0,
                &docs[1].1,
                DEFAULT_METADATA_THRESHOLD,
            )
            .matched
        );
        assert!(cosine_similarity(&docs[0].0, &docs[0].1, &docs[1].0, &docs[1].1) > 0.99);

        let near_score = cosine_similarity(&docs[0].0, &docs[0].1, &docs[2].0, &docs[2].1);
        assert!(near_score > 0.0 && near_score < 1.0);
        assert!(
            similarity_at_least(
                &docs[0].0,
                &docs[0].1,
                &docs[2].0,
                &docs[2].1,
                near_score - 1e-9,
            )
            .matched
        );
        assert!(
            !similarity_at_least(
                &docs[0].0,
                &docs[0].1,
                &docs[2].0,
                &docs[2].1,
                near_score + 0.05,
            )
            .matched
        );

        let far = similarity_at_least(
            &docs[0].0,
            &docs[0].1,
            &docs[3].0,
            &docs[3].1,
            DEFAULT_METADATA_THRESHOLD,
        );
        assert!(!far.matched);
        assert!(far.zero_overlap_pruned || far.upper_bound_pruned);
    }

    #[test]
    fn exact_canonical_hit_expands_whole_candidate_contract() {
        let shared = r#"{"name":"CoolCats","desc":"shared metadata body"}"#;
        let store = prepared(
            &["ethereum"],
            8,
            [
                row("ethereum", "0xa", "10", 1),
                row("ethereum", "0xa", "2", 2),
                row("ethereum", "0xb", "10", 3),
                row("ethereum", "0xb", "9", 4),
                row("ethereum", "0xb", "8", 5),
            ],
            [
                ("ethereum", "0xa", "10", shared, 1),
                ("ethereum", "0xa", "2", r#"{"name":"other-a"}"#, 2),
                ("ethereum", "0xb", "10", shared, 3),
                ("ethereum", "0xb", "9", r#"{"name":"other-b1"}"#, 4),
                ("ethereum", "0xb", "8", r#"{"name":"other-b2"}"#, 5),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "ethereum", "0xb");
        let mut graph = HitGraph::new();
        query_metadata_for_seed(
            &store,
            seed,
            DEFAULT_METADATA_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();

        let edge = graph
            .edges()
            .iter()
            .find(|e| e.candidate_contract == cand)
            .expect("metadata edge");
        assert_eq!(edge.candidate_nft, None);
        assert_eq!(edge.dimension, Dimension::Metadata);
        assert_eq!(edge.score, 1.0);

        let eth = store.chain_ids["ethereum"];
        let counts = count_scope_nfts(
            &graph,
            seed,
            ScopeKind::IntraChain,
            eth,
            None,
            &nft_map(&store),
        );
        assert_eq!(counts.metadata, 3, "whole candidate NFT expansion");
    }

    #[test]
    fn alignment_uses_largest_shared_not_smallest() {
        // Shared tokens 1 and 10; largest shared is 10. Docs at 10 match; docs at 1 do not.
        let store = prepared(
            &["ethereum"],
            8,
            [
                row("ethereum", "0xa", "1", 1),
                row("ethereum", "0xa", "10", 2),
                row("ethereum", "0xb", "1", 3),
                row("ethereum", "0xb", "10", 4),
            ],
            [
                ("ethereum", "0xa", "1", r#"{"name":"placeholder a"}"#, 1),
                (
                    "ethereum",
                    "0xa",
                    "10",
                    r#"{"name":"real collection shared body"}"#,
                    2,
                ),
                (
                    "ethereum",
                    "0xb",
                    "1",
                    r#"{"name":"placeholder b totally different"}"#,
                    3,
                ),
                (
                    "ethereum",
                    "0xb",
                    "10",
                    r#"{"name":"real collection shared body"}"#,
                    4,
                ),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "ethereum", "0xb");
        let mut graph = HitGraph::new();
        query_metadata_for_seed(
            &store,
            seed,
            DEFAULT_METADATA_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        assert!(
            graph
                .edges()
                .iter()
                .any(|e| e.candidate_contract == cand && e.score == 1.0),
            "largest shared token 10 should exact-match"
        );
    }

    #[test]
    fn evm_leading_zero_token_ids_share_alignment() {
        // Without bigint normalize, "10" vs "010" would not share; max-each-side
        // would compare unrelated docs at 20 vs 30 and miss the shared body.
        let shared = r#"{"name":"aligned shared metadata body"}"#;
        let store = prepared(
            &["ethereum"],
            8,
            [
                row("ethereum", "0xa", "20", 1),
                row("ethereum", "0xa", "10", 2),
                row("ethereum", "0xb", "30", 3),
                row("ethereum", "0xb", "010", 4),
            ],
            [
                ("ethereum", "0xa", "20", r#"{"name":"max-a unrelated"}"#, 1),
                ("ethereum", "0xa", "10", shared, 2),
                ("ethereum", "0xb", "30", r#"{"name":"max-b different"}"#, 3),
                ("ethereum", "0xb", "010", shared, 4),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "ethereum", "0xb");
        let seed_anchors = &store.metadata_index.contract_anchors[seed as usize];
        let cand_anchors = &store.metadata_index.contract_anchors[cand as usize];
        assert_eq!(seed_anchors.len(), 2);
        assert_eq!(cand_anchors.len(), 2);
        // Descending: [20, 10] and [30, 010]; shared key is the second entry.
        assert_eq!(seed_anchors[1].token_key, cand_anchors[1].token_key);
        assert_ne!(seed_anchors[0].token_key, cand_anchors[0].token_key);

        let mut graph = HitGraph::new();
        query_metadata_for_seed(
            &store,
            seed,
            DEFAULT_METADATA_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        let edge = graph
            .edges()
            .iter()
            .find(|e| e.candidate_contract == cand && e.candidate_nft.is_none())
            .expect("shared-token exact hit");
        assert_eq!(edge.dimension, Dimension::Metadata);
        assert_eq!(edge.score, 1.0);
    }

    #[test]
    fn bm25_near_match_emits_whole_contract_edge() {
        // Non-identical high-overlap JSON; query at a threshold below the true score.
        let store = prepared(
            &["ethereum", "base"],
            8,
            [
                row("ethereum", "0xa", "1", 1),
                row("base", "0xb", "1", 2),
                row("base", "0xb", "2", 3),
            ],
            [
                (
                    "ethereum",
                    "0xa",
                    "1",
                    r#"{"description":"alpha beta gamma delta epsilon zeta eta theta","name":"CoolCats"}"#,
                    1,
                ),
                (
                    "base",
                    "0xb",
                    "1",
                    r#"{"description":"alpha beta gamma delta epsilon zeta eta theta iota","name":"CoolCats"}"#,
                    2,
                ),
            ],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let cand = cid(&store, "base", "0xb");
        assert_eq!(store.metadata_index.document_count(), 2);

        let left_doc = store.metadata_index.contract_anchors[seed as usize][0].document_id;
        let right_doc = store.metadata_index.contract_anchors[cand as usize][0].document_id;
        assert_ne!(left_doc, right_doc);
        let score = store.metadata_index.cosine_between(left_doc, right_doc);
        assert!(
            score > 0.0 && score < 1.0,
            "expected non-exact BM25 score, got {score}"
        );
        let threshold = score * 0.9;

        let mut graph = HitGraph::new();
        query_metadata_for_seed(&store, seed, threshold, &mut graph, &NoopProgress).unwrap();
        let edge = graph
            .edges()
            .iter()
            .find(|e| e.candidate_contract == cand && e.candidate_nft.is_none())
            .expect("BM25 whole-contract edge");
        assert_eq!(edge.dimension, Dimension::Metadata);
        assert!((edge.score - score).abs() < 1e-9);

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
        assert_eq!(counts.metadata, 2);
    }

    #[test]
    fn parallel_candidate_chunks_match_single_thread_order_and_hits() {
        let run = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| {
                let evm = ["ethereum".to_owned()].into_iter().collect::<AHashSet<_>>();
                let mut store = ResidentStore::with_options(Some(1), &evm);
                // Exceeds both the candidate-chunk and posting-build
                // thresholds, covering parallel finalize and query.
                for index in 0..1_050_u64 {
                    let contract = if index == 0 {
                        "seed".to_owned()
                    } else {
                        format!("candidate-{index}")
                    };
                    store
                        .ingest_identity_row(row("ethereum", &contract, "1", index))
                        .unwrap();
                    let canonical =
                        format!(r#"{{"description":"shared collection body token {index}"}}"#);
                    anchor(&mut store, "ethereum", &contract, "1", &canonical, index);
                }
                finalize_metadata_index(&mut store).unwrap();
                let seed = cid(&store, "ethereum", "seed");
                let mut graph = HitGraph::new();
                query_metadata_for_seed(&store, seed, 0.0, &mut graph, &NoopProgress).unwrap();
                graph.into_edges()
            })
        };
        assert_eq!(run(1), run(4));
    }

    #[test]
    fn self_hit_excluded() {
        let store = prepared(
            &["ethereum"],
            8,
            [row("ethereum", "0xa", "1", 1)],
            [("ethereum", "0xa", "1", r#"{"name":"solo"}"#, 1)],
        );
        let seed = cid(&store, "ethereum", "0xa");
        let mut graph = HitGraph::new();
        query_metadata_for_seed(
            &store,
            seed,
            DEFAULT_METADATA_THRESHOLD,
            &mut graph,
            &NoopProgress,
        )
        .unwrap();
        assert!(graph.is_empty());
    }
}
