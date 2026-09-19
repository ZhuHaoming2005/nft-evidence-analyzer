use crate::entity::{ChainId, ContractId, Dimension, EntityStore, NftId, ScopeKind, UriPosting};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::scope::{ScopeCounts, ScopeKey};
use crate::stats::SummaryAccumulator;
use ahash::{AHashMap, AHashSet};
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};

const URI_PROGRESS_BATCH: u64 = 64;
const URI_TASKS_PER_THREAD: usize = 8;

#[derive(Default)]
struct ScopeAggregate {
    contracts: AHashSet<ContractId>,
    nft_rows: u64,
}

#[derive(Default)]
struct LocalUriAccumulator {
    scopes: AHashMap<ScopeKey, ScopeAggregate>,
}

impl LocalUriAccumulator {
    fn add(
        &mut self,
        posting: &UriPosting,
        nft_count: u64,
        dimension: Dimension,
        kind: ScopeKind,
        secondary_chain: Option<ChainId>,
    ) {
        let aggregate = self
            .scopes
            .entry(ScopeKey {
                kind,
                primary_chain: posting.chain_id,
                secondary_chain,
                dimension,
            })
            .or_default();
        aggregate.contracts.insert(posting.contract_id);
        aggregate.nft_rows = aggregate.nft_rows.saturating_add(nft_count);
    }

    fn merge(&mut self, other: Self) {
        for (key, mut source) in other.scopes {
            let target = self.scopes.entry(key).or_default();
            if target.contracts.len() < source.contracts.len() {
                std::mem::swap(&mut target.contracts, &mut source.contracts);
            }
            target.contracts.extend(source.contracts);
            target.nft_rows = target.nft_rows.saturating_add(source.nft_rows);
        }
    }

    fn add_group(
        &mut self,
        primary_chain: ChainId,
        contracts: &[ContractId],
        nft_rows: u64,
        dimension: Dimension,
        kind: ScopeKind,
        secondary_chain: Option<ChainId>,
    ) {
        if nft_rows == 0 {
            return;
        }
        let aggregate = self
            .scopes
            .entry(ScopeKey {
                kind,
                primary_chain,
                secondary_chain,
                dimension,
            })
            .or_default();
        aggregate.contracts.extend(contracts.iter().copied());
        aggregate.nft_rows = aggregate.nft_rows.saturating_add(nft_rows);
    }

    fn flush(self, acc: &mut SummaryAccumulator, dimension: Dimension) {
        acc.merge_completed_dimension_counts(
            dimension,
            self.scopes
                .into_iter()
                .map(|(key, aggregate)| {
                    (
                        key,
                        ScopeCounts {
                            duplicate_contract_count: aggregate.contracts.len() as u64,
                            duplicate_nft_count: aggregate.nft_rows,
                        },
                    )
                })
                .collect(),
        );
    }
}

struct TokenWorker {
    uri: LocalUriAccumulator,
    intra: Vec<NftId>,
    cross_summary: Vec<NftId>,
    matrix: MatrixWorkerHits,
}

enum MatrixWorkerHits {
    Compact(Vec<(NftId, u64)>),
    Wide(Vec<(NftId, ChainId)>),
}

impl TokenWorker {
    fn new(chain_count: usize) -> Self {
        Self {
            uri: LocalUriAccumulator::default(),
            intra: Vec::new(),
            cross_summary: Vec::new(),
            matrix: if chain_count <= u64::BITS as usize {
                MatrixWorkerHits::Compact(Vec::new())
            } else {
                MatrixWorkerHits::Wide(Vec::new())
            },
        }
    }

    fn add_intra(&mut self, posting: &UriPosting) {
        self.intra.extend_from_slice(&posting.nft_ids);
        self.uri.add(
            posting,
            posting.nft_ids.len() as u64,
            Dimension::TokenUri,
            ScopeKind::IntraChain,
            None,
        );
    }

    fn add_cross_summary(&mut self, posting: &UriPosting) {
        self.cross_summary.extend_from_slice(&posting.nft_ids);
        self.uri.add(
            posting,
            posting.nft_ids.len() as u64,
            Dimension::TokenUri,
            ScopeKind::CrossChainSummary,
            None,
        );
    }

    fn add_matrix(&mut self, posting: &UriPosting, peer_chains: &[ChainId]) {
        for &secondary in peer_chains {
            if secondary != posting.chain_id {
                self.uri.add(
                    posting,
                    posting.nft_ids.len() as u64,
                    Dimension::TokenUri,
                    ScopeKind::ChainMatrix,
                    Some(secondary),
                );
            }
        }
        match &mut self.matrix {
            MatrixWorkerHits::Compact(entries) => {
                let peer_mask = peer_chains.iter().fold(0_u64, |mask, &chain| {
                    if chain == posting.chain_id {
                        mask
                    } else {
                        mask | (1_u64 << usize::from(chain))
                    }
                });
                entries.extend(posting.nft_ids.iter().map(|&nft_id| (nft_id, peer_mask)));
            }
            MatrixWorkerHits::Wide(entries) => {
                for &secondary in peer_chains {
                    if secondary != posting.chain_id {
                        entries.extend(posting.nft_ids.iter().map(|&nft_id| (nft_id, secondary)));
                    }
                }
            }
        }
    }

    fn merge(&mut self, mut other: Self) {
        self.uri.merge(other.uri);
        self.intra.append(&mut other.intra);
        self.cross_summary.append(&mut other.cross_summary);
        match (&mut self.matrix, &mut other.matrix) {
            (MatrixWorkerHits::Compact(left), MatrixWorkerHits::Compact(right)) => {
                left.append(right);
            }
            (MatrixWorkerHits::Wide(left), MatrixWorkerHits::Wide(right)) => {
                left.append(right);
            }
            _ => unreachable!("all URI workers use the same chain width"),
        }
    }
}

struct TokenHits {
    intra: DenseNftSet,
    cross_summary: DenseNftSet,
    matrix: MatrixNftSet,
}

enum MatrixNftSet {
    Compact(Vec<u64>),
    Wide(AHashSet<(NftId, ChainId)>),
}

impl MatrixNftSet {
    fn new(nft_count: usize, chain_count: usize) -> Self {
        if chain_count <= u64::BITS as usize {
            Self::Compact(vec![0; nft_count])
        } else {
            Self::Wide(AHashSet::new())
        }
    }

    fn insert(&mut self, nft_id: NftId, chain_id: ChainId) {
        match self {
            Self::Compact(masks) => {
                masks[nft_id as usize] |= 1_u64 << usize::from(chain_id);
            }
            Self::Wide(entries) => {
                entries.insert((nft_id, chain_id));
            }
        }
    }

    fn contains(&self, nft_id: NftId, chain_id: ChainId) -> bool {
        match self {
            Self::Compact(masks) => masks[nft_id as usize] & (1_u64 << usize::from(chain_id)) != 0,
            Self::Wide(entries) => entries.contains(&(nft_id, chain_id)),
        }
    }
}

struct DenseNftSet {
    words: Vec<u64>,
}

impl DenseNftSet {
    fn with_capacity(nft_count: usize) -> Self {
        Self {
            words: vec![0; nft_count.div_ceil(64)],
        }
    }

    fn insert(&mut self, nft_id: NftId) {
        let nft_id = nft_id as usize;
        self.words[nft_id / 64] |= 1_u64 << (nft_id % 64);
    }

    fn contains(&self, nft_id: NftId) -> bool {
        let nft_id = nft_id as usize;
        self.words
            .get(nft_id / 64)
            .is_some_and(|word| word & (1_u64 << (nft_id % 64)) != 0)
    }
}

impl TokenHits {
    fn new(nft_count: usize, chain_count: usize) -> Self {
        Self {
            intra: DenseNftSet::with_capacity(nft_count),
            cross_summary: DenseNftSet::with_capacity(nft_count),
            matrix: MatrixNftSet::new(nft_count, chain_count),
        }
    }
}

pub fn run_uri(
    store: &EntityStore,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<(), DedupError> {
    progress.set_stage("uri");
    let token_groups = group_count(&store.token_uri_postings);
    progress.begin_phase("token_uri", Some(token_groups));
    let cancelled = AtomicBool::new(false);
    let token_worker = process_group_chunks(
        &store.token_uri_postings,
        || TokenWorker::new(store.chains.len()),
        |worker, members| accumulate_token_scope_hits(members, worker),
        |mut left, right| {
            left.merge(right);
            left
        },
        progress,
        &cancelled,
    );
    if cancelled.load(Ordering::Relaxed) {
        return Err(DedupError::Interrupted);
    }
    let TokenWorker {
        uri,
        intra,
        cross_summary,
        matrix,
    } = token_worker;
    let mut token_hits = TokenHits::new(store.nfts.len(), store.chains.len());
    for nft_id in intra {
        token_hits.intra.insert(nft_id);
    }
    for nft_id in cross_summary {
        token_hits.cross_summary.insert(nft_id);
    }
    match matrix {
        MatrixWorkerHits::Compact(entries) => {
            let MatrixNftSet::Compact(masks) = &mut token_hits.matrix else {
                unreachable!("compact URI worker requires compact token hits");
            };
            for (nft_id, peer_mask) in entries {
                masks[nft_id as usize] |= peer_mask;
            }
        }
        MatrixWorkerHits::Wide(entries) => {
            for (nft_id, chain_id) in entries {
                token_hits.matrix.insert(nft_id, chain_id);
            }
        }
    }
    uri.flush(acc, Dimension::TokenUri);

    let image_groups = group_count(&store.image_uri_postings);
    progress.begin_phase("image_uri", Some(image_groups));
    let image_acc = process_group_chunks(
        &store.image_uri_postings,
        LocalUriAccumulator::default,
        |local, members| {
            accumulate_image_scope_hits(members, &token_hits, store.chains.len(), local)
        },
        |mut left, right| {
            left.merge(right);
            left
        },
        progress,
        &cancelled,
    );
    if cancelled.load(Ordering::Relaxed) {
        return Err(DedupError::Interrupted);
    }
    image_acc.flush(acc, Dimension::ImageUri);
    Ok(())
}

fn group_count(postings: &[UriPosting]) -> u64 {
    if postings.is_empty() {
        return 0;
    }
    1 + postings
        .par_windows(2)
        .filter(|pair| pair[0].uri_id != pair[1].uri_id)
        .count() as u64
}

fn process_group_chunks<W, New, Process, Merge>(
    postings: &[UriPosting],
    new_worker: New,
    process: Process,
    merge: Merge,
    progress: &dyn ProgressObserver,
    cancelled: &AtomicBool,
) -> W
where
    W: Send,
    New: Fn() -> W + Send + Sync,
    Process: Fn(&mut W, &[UriPosting]) + Send + Sync,
    Merge: Fn(W, W) -> W + Send + Sync,
{
    if postings.is_empty() {
        return new_worker();
    }
    let target_tasks = rayon::current_num_threads()
        .saturating_mul(URI_TASKS_PER_THREAD)
        .max(1);
    let chunk_size = postings.len().div_ceil(target_tasks).max(1);
    let task_count = postings.len().div_ceil(chunk_size);
    (0..task_count)
        .into_par_iter()
        .map(|task| {
            let mut worker = new_worker();
            if progress.check_cancelled().is_err() {
                cancelled.store(true, Ordering::Relaxed);
                return worker;
            }
            let chunk_start = task * chunk_size;
            let chunk_end = (chunk_start + chunk_size).min(postings.len());
            let mut start = chunk_start;
            if start > 0 && postings[start - 1].uri_id == postings[start].uri_id {
                let continued_uri = postings[start].uri_id;
                start +=
                    postings[start..].partition_point(|posting| posting.uri_id == continued_uri);
            }
            let mut pending = 0_u64;
            while start < chunk_end && !cancelled.load(Ordering::Relaxed) {
                let uri_id = postings[start].uri_id;
                let mut end = start + 1;
                while end < postings.len() && postings[end].uri_id == uri_id {
                    end += 1;
                }
                process(&mut worker, &postings[start..end]);
                pending += 1;
                if pending >= URI_PROGRESS_BATCH {
                    progress.add_completed(pending);
                    pending = 0;
                    if progress.check_cancelled().is_err() {
                        cancelled.store(true, Ordering::Relaxed);
                    }
                }
                start = end;
            }
            if pending != 0 {
                progress.add_completed(pending);
                if progress.check_cancelled().is_err() {
                    cancelled.store(true, Ordering::Relaxed);
                }
            }
            worker
        })
        .reduce(&new_worker, merge)
}

fn postings_by_chain(members: &[UriPosting]) -> AHashMap<ChainId, Vec<&UriPosting>> {
    let mut by_chain: AHashMap<ChainId, Vec<&UriPosting>> = AHashMap::new();
    for member in members {
        by_chain.entry(member.chain_id).or_default().push(member);
    }
    by_chain
}

fn accumulate_token_scope_hits(members: &[UriPosting], worker: &mut TokenWorker) {
    if members.len() < 2 {
        return;
    }
    if matches!(&worker.matrix, MatrixWorkerHits::Compact(_)) {
        let mut posting_counts = [0_usize; u64::BITS as usize];
        let mut present_mask = 0_u64;
        for posting in members {
            posting_counts[usize::from(posting.chain_id)] += 1;
            present_mask |= 1_u64 << usize::from(posting.chain_id);
        }
        let mut chains = Vec::with_capacity(present_mask.count_ones() as usize);
        let mut remaining = present_mask;
        while remaining != 0 {
            let chain = remaining.trailing_zeros() as ChainId;
            chains.push(chain);
            remaining &= remaining - 1;
        }
        for posting in members {
            if posting_counts[usize::from(posting.chain_id)] >= 2 {
                worker.add_intra(posting);
            }
            if chains.len() >= 2 {
                worker.add_cross_summary(posting);
                worker.add_matrix(posting, &chains);
            }
        }
        return;
    }

    let by_chain = postings_by_chain(members);
    let chains: Vec<ChainId> = by_chain.keys().copied().collect();
    for (&chain, postings) in &by_chain {
        if postings.len() >= 2 {
            for posting in postings {
                worker.add_intra(posting);
            }
        }
        if chains.len() >= 2 {
            for posting in postings {
                worker.add_cross_summary(posting);
            }
        }
        if chains.len() >= 2 {
            for posting in postings {
                debug_assert_eq!(posting.chain_id, chain);
                worker.add_matrix(posting, &chains);
            }
        }
    }
}

fn accumulate_image_scope_hits(
    members: &[UriPosting],
    token_hits: &TokenHits,
    chain_count: usize,
    output: &mut LocalUriAccumulator,
) {
    if members.len() < 2 {
        return;
    }
    if chain_count <= u64::BITS as usize {
        accumulate_image_scope_hits_compact(members, token_hits, output);
    } else {
        accumulate_image_scope_hits_wide(members, token_hits, output);
    }
}

#[derive(Default)]
struct GroupScopeAggregate {
    contracts: Vec<ContractId>,
    nft_rows: u64,
    posting_count: usize,
}

impl GroupScopeAggregate {
    fn add_posting(&mut self, contract_id: ContractId, nft_rows: u64) {
        if nft_rows == 0 {
            return;
        }
        self.contracts.push(contract_id);
        self.nft_rows = self.nft_rows.saturating_add(nft_rows);
        self.posting_count += 1;
    }
}

fn accumulate_image_scope_hits_compact(
    members: &[UriPosting],
    token_hits: &TokenHits,
    output: &mut LocalUriAccumulator,
) {
    let mut present_mask = 0_u64;
    for posting in members {
        present_mask |= 1_u64 << usize::from(posting.chain_id);
    }
    let mut chains = Vec::with_capacity(present_mask.count_ones() as usize);
    let mut remaining = present_mask;
    while remaining != 0 {
        let chain = remaining.trailing_zeros() as ChainId;
        chains.push(chain);
        remaining &= remaining - 1;
    }
    if chains.len() == 1 {
        let mut intra = GroupScopeAggregate::default();
        for posting in members {
            let nft_rows = posting
                .nft_ids
                .iter()
                .filter(|&&nft_id| !token_hits.intra.contains(nft_id))
                .count() as u64;
            intra.add_posting(posting.contract_id, nft_rows);
        }
        if intra.posting_count >= 2 {
            output.add_group(
                chains[0],
                &intra.contracts,
                intra.nft_rows,
                Dimension::ImageUri,
                ScopeKind::IntraChain,
                None,
            );
        }
        return;
    }
    let mut local_index = [usize::MAX; u64::BITS as usize];
    for (index, &chain) in chains.iter().enumerate() {
        local_index[usize::from(chain)] = index;
    }
    let width = chains.len();
    let mut intra = (0..width)
        .map(|_| GroupScopeAggregate::default())
        .collect::<Vec<_>>();
    let mut cross = (0..width)
        .map(|_| GroupScopeAggregate::default())
        .collect::<Vec<_>>();
    let mut matrix = (0..width.saturating_mul(width))
        .map(|_| GroupScopeAggregate::default())
        .collect::<Vec<_>>();

    for posting in members {
        let primary = local_index[usize::from(posting.chain_id)];
        let mut intra_count = 0_u64;
        let mut cross_count = 0_u64;
        let mut matrix_counts = [0_u64; u64::BITS as usize];
        for &nft_id in &posting.nft_ids {
            if !token_hits.intra.contains(nft_id) {
                intra_count += 1;
            }
            if !token_hits.cross_summary.contains(nft_id) {
                cross_count += 1;
            }
            for (secondary, &secondary_chain) in chains.iter().enumerate() {
                if secondary != primary && !token_hits.matrix.contains(nft_id, secondary_chain) {
                    matrix_counts[secondary] += 1;
                }
            }
        }
        intra[primary].add_posting(posting.contract_id, intra_count);
        cross[primary].add_posting(posting.contract_id, cross_count);
        for secondary in 0..width {
            if secondary != primary {
                matrix[primary * width + secondary]
                    .add_posting(posting.contract_id, matrix_counts[secondary]);
            }
        }
    }

    for (primary, aggregate) in intra.iter().enumerate() {
        if aggregate.posting_count >= 2 {
            output.add_group(
                chains[primary],
                &aggregate.contracts,
                aggregate.nft_rows,
                Dimension::ImageUri,
                ScopeKind::IntraChain,
                None,
            );
        }
    }

    if cross
        .iter()
        .filter(|aggregate| aggregate.nft_rows != 0)
        .count()
        >= 2
    {
        for (primary, aggregate) in cross.iter().enumerate() {
            output.add_group(
                chains[primary],
                &aggregate.contracts,
                aggregate.nft_rows,
                Dimension::ImageUri,
                ScopeKind::CrossChainSummary,
                None,
            );
        }
    }

    for primary in 0..width {
        for secondary in 0..width {
            if primary == secondary {
                continue;
            }
            let aggregate = &matrix[primary * width + secondary];
            let reciprocal = &matrix[secondary * width + primary];
            if aggregate.nft_rows != 0 && reciprocal.nft_rows != 0 {
                output.add_group(
                    chains[primary],
                    &aggregate.contracts,
                    aggregate.nft_rows,
                    Dimension::ImageUri,
                    ScopeKind::ChainMatrix,
                    Some(chains[secondary]),
                );
            }
        }
    }
}

fn accumulate_image_scope_hits_wide(
    members: &[UriPosting],
    token_hits: &TokenHits,
    output: &mut LocalUriAccumulator,
) {
    let by_chain = postings_by_chain(members);
    let chains: Vec<ChainId> = by_chain.keys().copied().collect();

    for (&chain, postings) in &by_chain {
        let intra = filtered_posting_counts(postings, |nft_id| !token_hits.intra.contains(nft_id));
        if intra.len() >= 2 {
            for (posting, nft_count) in intra {
                output.add(
                    posting,
                    nft_count,
                    Dimension::ImageUri,
                    ScopeKind::IntraChain,
                    None,
                );
            }
        }

        let primary_cross = filtered_posting_counts(postings, |nft_id| {
            !token_hits.cross_summary.contains(nft_id)
        });
        let has_other_cross = chains.iter().any(|&other_chain| {
            other_chain != chain
                && by_chain[&other_chain].iter().any(|posting| {
                    posting
                        .nft_ids
                        .iter()
                        .any(|&nft_id| !token_hits.cross_summary.contains(nft_id))
                })
        });
        if !primary_cross.is_empty() && has_other_cross {
            for (posting, nft_count) in primary_cross {
                output.add(
                    posting,
                    nft_count,
                    Dimension::ImageUri,
                    ScopeKind::CrossChainSummary,
                    None,
                );
            }
        }

        for &other_chain in &chains {
            if other_chain == chain {
                continue;
            }
            let primary_matrix = filtered_posting_counts(postings, |nft_id| {
                !token_hits.matrix.contains(nft_id, other_chain)
            });
            let other_has_match = by_chain[&other_chain].iter().any(|posting| {
                posting
                    .nft_ids
                    .iter()
                    .any(|&nft_id| !token_hits.matrix.contains(nft_id, chain))
            });
            if !primary_matrix.is_empty() && other_has_match {
                for (posting, nft_count) in primary_matrix {
                    output.add(
                        posting,
                        nft_count,
                        Dimension::ImageUri,
                        ScopeKind::ChainMatrix,
                        Some(other_chain),
                    );
                }
            }
        }
    }
}

fn filtered_posting_counts<'a>(
    postings: &[&'a UriPosting],
    keep: impl Fn(NftId) -> bool,
) -> Vec<(&'a UriPosting, u64)> {
    postings
        .iter()
        .filter_map(|posting| {
            let nft_count = posting
                .nft_ids
                .iter()
                .copied()
                .filter(|&nft_id| keep(nft_id))
                .count() as u64;
            (nft_count != 0).then_some((*posting, nft_count))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{InputRow, SourceOrder};
    use crate::progress::NoopProgress;

    fn row(chain: &str, contract: &str, token: &str, token_uri: &str, image_uri: &str) -> InputRow {
        InputRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: String::new(),
            token_uri_norm: token_uri.to_owned(),
            image_uri_norm: image_uri.to_owned(),
            metadata_json: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: token.parse().unwrap_or(0),
            },
        }
    }

    #[test]
    fn compact_image_chain_masks_match_wide_reference() {
        let postings = vec![
            UriPosting {
                contract_id: 0,
                chain_id: 0,
                uri_id: 0,
                nft_ids: vec![0, 1],
            },
            UriPosting {
                contract_id: 1,
                chain_id: 0,
                uri_id: 0,
                nft_ids: vec![2],
            },
            UriPosting {
                contract_id: 2,
                chain_id: 1,
                uri_id: 0,
                nft_ids: vec![3, 4],
            },
            UriPosting {
                contract_id: 3,
                chain_id: 2,
                uri_id: 0,
                nft_ids: vec![5],
            },
        ];
        let mut compact = TokenHits::new(6, 3);
        let mut wide = TokenHits {
            intra: DenseNftSet::with_capacity(6),
            cross_summary: DenseNftSet::with_capacity(6),
            matrix: MatrixNftSet::Wide(AHashSet::new()),
        };
        for nft_id in [1, 4] {
            compact.intra.insert(nft_id);
            wide.intra.insert(nft_id);
        }
        for nft_id in [2, 5] {
            compact.cross_summary.insert(nft_id);
            wide.cross_summary.insert(nft_id);
        }
        for (nft_id, chain_id) in [(0, 1), (3, 0), (4, 2)] {
            compact.matrix.insert(nft_id, chain_id);
            wide.matrix.insert(nft_id, chain_id);
        }

        let mut actual = LocalUriAccumulator::default();
        accumulate_image_scope_hits_compact(&postings, &compact, &mut actual);
        let mut expected = LocalUriAccumulator::default();
        accumulate_image_scope_hits_wide(&postings, &wide, &mut expected);

        assert_eq!(actual.scopes.len(), expected.scopes.len());
        for (key, actual) in actual.scopes {
            let expected = &expected.scopes[&key];
            assert_eq!(actual.nft_rows, expected.nft_rows, "scope {key:?}");
            assert_eq!(actual.contracts, expected.contracts, "scope {key:?}");
        }
    }

    #[test]
    fn boundary_aware_group_chunks_process_every_uri_once() {
        let mut postings = (0..50)
            .map(|contract_id| UriPosting {
                contract_id,
                chain_id: 0,
                uri_id: 0,
                nft_ids: vec![contract_id],
            })
            .collect::<Vec<_>>();
        postings.extend((1..=46).map(|uri_id| UriPosting {
            contract_id: 49 + uri_id,
            chain_id: 0,
            uri_id,
            nft_ids: vec![49 + uri_id],
        }));
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let cancelled = AtomicBool::new(false);
        let mut groups = pool.install(|| {
            process_group_chunks(
                &postings,
                Vec::<u32>::new,
                |seen, members| seen.push(members[0].uri_id),
                |mut left, mut right| {
                    left.append(&mut right);
                    left
                },
                &NoopProgress,
                &cancelled,
            )
        });
        groups.sort_unstable();

        assert!(!cancelled.load(Ordering::Relaxed));
        assert_eq!(group_count(&postings), 47);
        assert_eq!(groups, (0..=46).collect::<Vec<_>>());
    }

    fn prepared(rows: impl IntoIterator<Item = InputRow>) -> EntityStore {
        let mut store = EntityStore::default();
        for row in rows {
            store.ingest_row(row);
        }
        store.rebuild_uri_postings();
        store
    }

    fn counts(
        store: &EntityStore,
        acc: &SummaryAccumulator,
        chain: &str,
        kind: ScopeKind,
        secondary: Option<&str>,
        dimension: Dimension,
    ) -> u64 {
        let primary = store.chain_ids[chain];
        let key = crate::scope::ScopeKey {
            kind,
            primary_chain: primary,
            secondary_chain: secondary.map(|name| store.chain_ids[name]),
            dimension,
        };
        acc.counts()
            .get(&key)
            .map(|value| value.duplicate_nft_count)
            .unwrap_or(0)
    }

    #[test]
    fn intra_chain_token_uri_counts_two_contracts() {
        let store = prepared([
            row("ethereum", "a", "1", "ipfs://x", ""),
            row("ethereum", "b", "1", "ipfs://x", ""),
        ]);
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        assert_eq!(
            counts(
                &store,
                &acc,
                "ethereum",
                ScopeKind::IntraChain,
                None,
                Dimension::TokenUri
            ),
            2
        );
    }

    #[test]
    fn rerunning_uri_on_the_same_accumulator_is_idempotent() {
        let store = prepared([
            row("ethereum", "a", "1", "same", "image"),
            row("ethereum", "b", "2", "same", "image"),
            row("base", "c", "3", "same", "image"),
        ]);
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        let once = acc.counts().clone();

        run_uri(&store, &mut acc, &NoopProgress).unwrap();

        assert_eq!(acc.counts(), &once);
    }

    #[test]
    fn cross_summary_nft_not_double_counted_across_peers() {
        let store = prepared([
            row("ethereum", "a", "1", "ipfs://x", ""),
            row("base", "b", "1", "ipfs://x", ""),
            row("polygon", "c", "1", "ipfs://x", ""),
        ]);
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        assert_eq!(
            counts(
                &store,
                &acc,
                "ethereum",
                ScopeKind::CrossChainSummary,
                None,
                Dimension::TokenUri
            ),
            1
        );
    }

    #[test]
    fn image_and_not_is_scope_specific() {
        let store = prepared([
            row("ethereum", "a", "1", "token://same", "image://same"),
            row("ethereum", "b", "1", "token://same", "image://other"),
            row("base", "c", "1", "token://base-only", "image://same"),
        ]);
        let mut acc = SummaryAccumulator::default();
        run_uri(&store, &mut acc, &NoopProgress).unwrap();
        assert_eq!(
            counts(
                &store,
                &acc,
                "ethereum",
                ScopeKind::IntraChain,
                None,
                Dimension::ImageUri
            ),
            0
        );
        assert_eq!(
            counts(
                &store,
                &acc,
                "ethereum",
                ScopeKind::ChainMatrix,
                Some("base"),
                Dimension::ImageUri
            ),
            1
        );
    }

    #[test]
    fn interleaved_uri_rows_merge_postings() {
        let store = prepared([
            row("ethereum", "a", "1", "ipfs://x", ""),
            row("ethereum", "a", "2", "ipfs://y", ""),
            row("ethereum", "a", "3", "ipfs://x", ""),
            row("ethereum", "b", "1", "ipfs://x", ""),
        ]);
        assert_eq!(store.token_uri_postings.len(), 3);
        let x = store.string_id("ipfs://x").unwrap();
        let posting = store
            .token_uri_postings
            .iter()
            .find(|posting| posting.contract_id == 0 && posting.uri_id == x)
            .unwrap();
        assert_eq!(posting.nft_count(), 2);
    }
}
