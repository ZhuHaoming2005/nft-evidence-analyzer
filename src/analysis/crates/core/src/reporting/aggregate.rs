//! Scope counters and duplicate-scale rows for offline reports.

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use crate::dedup::hits::{Dimension, HitGraph, ScopeKind};
use crate::entity::{ChainId, ContractId, NftId, ResidentStore};

/// Per-dimension and union NFT numerator counts for one seed scope.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeNftCounts {
    pub name: u64,
    pub token_uri: u64,
    pub image_uri: u64,
    pub metadata: u64,
    /// Four-dimension set-union size (not the sum of the rows above).
    pub total: u64,
}

impl ScopeNftCounts {
    pub fn from_sets(
        name: usize,
        token_uri: usize,
        image_uri: usize,
        metadata: usize,
        total: usize,
    ) -> Self {
        Self {
            name: name as u64,
            token_uri: token_uri as u64,
            image_uri: image_uri as u64,
            metadata: metadata as u64,
            total: total as u64,
        }
    }
}

/// One duplicate-scale category row (`token_uri` / `image_uri` / `metadata` / `name` / `total`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DuplicateScaleRow {
    pub category: String,
    pub duplicate_nft_count: u64,
    pub duplicate_nft_ratio: Option<f64>,
    pub duplicate_nft_ratio_numerator: u64,
    pub duplicate_nft_ratio_denominator: u64,
    pub duplicate_contract_count: u64,
    pub duplicate_contract_ratio: Option<f64>,
    pub duplicate_contract_ratio_numerator: u64,
    pub duplicate_contract_ratio_denominator: u64,
}

/// Directional chain-matrix block for one secondary chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChainMatrixBlock {
    pub secondary_chain: String,
    pub rows: Vec<DuplicateScaleRow>,
}

/// Per-seed duplicate-scale sections.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SeedDuplicateScale {
    pub intra_chain: Vec<DuplicateScaleRow>,
    pub chain_matrix: Vec<ChainMatrixBlock>,
    pub cross_chain_summary: Vec<DuplicateScaleRow>,
}

/// Build `contract_id → nft_ids` map used by HitGraph expansion helpers.
///
/// Copies CSR slices once so callers can keep the map after optional index
/// drops. Prefer [`ResidentStore::nfts_for_contract`] when the store is live.
pub fn build_contract_nft_map(store: &ResidentStore) -> AHashMap<ContractId, Vec<NftId>> {
    let mut map = AHashMap::with_capacity(store.contracts.len());
    for contract in &store.contracts {
        let ids = store.nfts_for_contract(contract.id);
        if !ids.is_empty() {
            // The resident CSR is already ordered by `(contract_id, nft_id)`.
            map.insert(contract.id, ids.to_vec());
        }
    }
    map
}

/// Copy CSR membership only for contracts referenced by contract-wide hit
/// edges. NFT-specific edges already carry their NFT id and need no expansion.
pub fn build_contract_nft_map_for_graphs<'a>(
    store: &ResidentStore,
    graphs: impl IntoIterator<Item = &'a HitGraph>,
) -> AHashMap<ContractId, Vec<NftId>> {
    let contracts: AHashSet<ContractId> = graphs
        .into_iter()
        .flat_map(HitGraph::edges)
        .filter(|edge| edge.candidate_nft.is_none())
        .map(|edge| edge.candidate_contract)
        .collect();
    let mut map = AHashMap::with_capacity(contracts.len());
    for contract_id in contracts {
        let ids = store.nfts_for_contract(contract_id);
        if !ids.is_empty() {
            map.insert(contract_id, ids.to_vec());
        }
    }
    map
}

/// Count NFT numerators for one `seed` under `scope`: per-dimension rows + set-union `total`.
///
/// Only edges with `edge.seed_contract == seed` are counted. `image_uri` excludes NFTs already
/// counted under `token_uri` for the same seed/scope (other seeds must not contaminate).
pub fn count_scope_nfts(
    graph: &HitGraph,
    seed: ContractId,
    scope: ScopeKind,
    primary_chain: ChainId,
    matrix_secondary: Option<ChainId>,
    contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
) -> ScopeNftCounts {
    // Single edge scan fills all four dimension sets + union.
    let mut name = AHashSet::new();
    let mut token_uri = AHashSet::new();
    let mut image_uri = AHashSet::new();
    let mut metadata = AHashSet::new();
    let mut total = AHashSet::new();

    for edge in graph.edges() {
        if edge.seed_contract != seed {
            continue;
        }
        if !HitGraph::edge_in_scope(edge, scope, primary_chain, matrix_secondary) {
            continue;
        }
        let expand = |out: &mut AHashSet<NftId>| match edge.candidate_nft {
            Some(nft) => {
                out.insert(nft);
            }
            None => {
                if let Some(nfts) = contract_nfts.get(&edge.candidate_contract) {
                    out.extend(nfts.iter().copied());
                }
            }
        };
        match edge.dimension {
            Dimension::Name => expand(&mut name),
            Dimension::TokenUri => expand(&mut token_uri),
            Dimension::ImageUri => expand(&mut image_uri),
            Dimension::Metadata => expand(&mut metadata),
        }
        expand(&mut total);
    }
    // Image is supplemental to token_uri within the same seed/scope.
    image_uri.retain(|nft| !token_uri.contains(nft));

    ScopeNftCounts::from_sets(
        name.len(),
        token_uri.len(),
        image_uri.len(),
        metadata.len(),
        total.len(),
    )
}

fn ratio(numer: u64, denom: u64) -> Option<f64> {
    if denom == 0 {
        None
    } else {
        Some(numer as f64 / denom as f64)
    }
}

fn scale_row(
    category: &str,
    nft: u64,
    contract: u64,
    nft_denom: u64,
    contract_denom: u64,
) -> DuplicateScaleRow {
    DuplicateScaleRow {
        category: category.to_owned(),
        duplicate_nft_count: nft,
        duplicate_nft_ratio: ratio(nft, nft_denom),
        duplicate_nft_ratio_numerator: nft,
        duplicate_nft_ratio_denominator: nft_denom,
        duplicate_contract_count: contract,
        duplicate_contract_ratio: ratio(contract, contract_denom),
        duplicate_contract_ratio_numerator: contract,
        duplicate_contract_ratio_denominator: contract_denom,
    }
}

fn contracts_for_nfts(nfts: &AHashSet<NftId>, store: &ResidentStore) -> AHashSet<ContractId> {
    let mut out = AHashSet::new();
    for &nft in nfts {
        if let Some(rec) = store.nfts.get(nft as usize) {
            out.insert(rec.contract_id);
        }
    }
    out
}

fn dimension_sets(
    graph: &HitGraph,
    seed: ContractId,
    scope: ScopeKind,
    primary: ChainId,
    secondary: Option<ChainId>,
    contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
) -> [(Dimension, AHashSet<NftId>); 4] {
    [
        (
            Dimension::TokenUri,
            graph.dimension_candidate_nfts(
                seed,
                Dimension::TokenUri,
                scope,
                primary,
                secondary,
                contract_nfts,
            ),
        ),
        (
            Dimension::ImageUri,
            graph.dimension_candidate_nfts(
                seed,
                Dimension::ImageUri,
                scope,
                primary,
                secondary,
                contract_nfts,
            ),
        ),
        (
            Dimension::Metadata,
            graph.dimension_candidate_nfts(
                seed,
                Dimension::Metadata,
                scope,
                primary,
                secondary,
                contract_nfts,
            ),
        ),
        (
            Dimension::Name,
            graph.dimension_candidate_nfts(
                seed,
                Dimension::Name,
                scope,
                primary,
                secondary,
                contract_nfts,
            ),
        ),
    ]
}

fn category_name(dim: Dimension) -> &'static str {
    match dim {
        Dimension::TokenUri => "token_uri",
        Dimension::ImageUri => "image_uri",
        Dimension::Metadata => "metadata",
        Dimension::Name => "name",
    }
}

#[derive(Default)]
struct DimensionNftSets {
    token_uri: AHashSet<NftId>,
    image_uri: AHashSet<NftId>,
    metadata: AHashSet<NftId>,
    name: AHashSet<NftId>,
}

impl DimensionNftSets {
    fn insert_edge(
        &mut self,
        dimension: Dimension,
        candidate_nft: Option<NftId>,
        candidate_contract: ContractId,
        contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
    ) {
        let target = match dimension {
            Dimension::TokenUri => &mut self.token_uri,
            Dimension::ImageUri => &mut self.image_uri,
            Dimension::Metadata => &mut self.metadata,
            Dimension::Name => &mut self.name,
        };
        match candidate_nft {
            Some(nft) => {
                target.insert(nft);
            }
            None => {
                if let Some(nfts) = contract_nfts.get(&candidate_contract) {
                    target.extend(nfts.iter().copied());
                }
            }
        }
    }
}

fn rows_from_dimension_sets(
    store: &ResidentStore,
    denominator_chains: impl IntoIterator<Item = ChainId>,
    mut sets: DimensionNftSets,
) -> Vec<DuplicateScaleRow> {
    // Image URI is supplemental to token URI within the same reporting scope.
    sets.image_uri.retain(|nft| !sets.token_uri.contains(nft));
    let mut nft_denom = 0u64;
    let mut contract_denom = 0u64;
    for chain in denominator_chains {
        let totals = store.totals.get(&chain).cloned().unwrap_or_default();
        nft_denom += totals.nfts;
        contract_denom += totals.contracts;
    }
    let mut rows = Vec::with_capacity(5);
    let mut union_nfts = AHashSet::new();
    for (dimension, nfts) in [
        (Dimension::TokenUri, &sets.token_uri),
        (Dimension::ImageUri, &sets.image_uri),
        (Dimension::Metadata, &sets.metadata),
        (Dimension::Name, &sets.name),
    ] {
        union_nfts.extend(nfts.iter().copied());
        let contracts = contracts_for_nfts(nfts, store);
        rows.push(scale_row(
            category_name(dimension),
            nfts.len() as u64,
            contracts.len() as u64,
            nft_denom,
            contract_denom,
        ));
    }
    let union_contracts = contracts_for_nfts(&union_nfts, store);
    rows.push(scale_row(
        "total",
        union_nfts.len() as u64,
        union_contracts.len() as u64,
        nft_denom,
        contract_denom,
    ));
    rows
}

/// Build five-category duplicate-scale rows for one seed/scope.
pub fn build_duplicate_scale_rows(
    store: &ResidentStore,
    graph: &HitGraph,
    seed: ContractId,
    scope: ScopeKind,
    primary_chain: ChainId,
    matrix_secondary: Option<ChainId>,
    contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
) -> Vec<DuplicateScaleRow> {
    let denominator_chains: Vec<ChainId> = match scope {
        ScopeKind::IntraChain => vec![primary_chain],
        ScopeKind::ChainMatrix => matrix_secondary.into_iter().collect(),
        ScopeKind::CrossChainSummary => (0..store.chains.len())
            .map(|idx| idx as ChainId)
            .filter(|chain| *chain != primary_chain)
            .collect(),
    };
    let (nft_denom, contract_denom) =
        denominator_chains
            .into_iter()
            .fold((0u64, 0u64), |(nfts, contracts), chain| {
                let totals = store.totals.get(&chain).cloned().unwrap_or_default();
                (nfts + totals.nfts, contracts + totals.contracts)
            });

    let dims = dimension_sets(
        graph,
        seed,
        scope,
        primary_chain,
        matrix_secondary,
        contract_nfts,
    );
    let mut rows = Vec::with_capacity(5);
    let mut union_nfts = AHashSet::new();
    for (dim, nfts) in &dims {
        for &nft in nfts {
            union_nfts.insert(nft);
        }
        let contracts = contracts_for_nfts(nfts, store);
        rows.push(scale_row(
            category_name(*dim),
            nfts.len() as u64,
            contracts.len() as u64,
            nft_denom,
            contract_denom,
        ));
    }
    let union_contracts = contracts_for_nfts(&union_nfts, store);
    rows.push(scale_row(
        "total",
        union_nfts.len() as u64,
        union_contracts.len() as u64,
        nft_denom,
        contract_denom,
    ));
    rows
}

/// Build intra / matrix / cross duplicate-scale sections for one seed.
pub fn build_seed_duplicate_scale(
    store: &ResidentStore,
    graph: &HitGraph,
    seed: ContractId,
    contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
) -> SeedDuplicateScale {
    let primary = store.contracts[seed as usize].chain_id;
    let mut by_secondary: Vec<DimensionNftSets> = (0..store.chains.len())
        .map(|_| DimensionNftSets::default())
        .collect();
    let mut cross = DimensionNftSets::default();

    // A seed-local graph is scanned once. The old path rescanned it once per
    // dimension and scope (including an extra token scan for image fallback).
    for edge in graph.edges() {
        if edge.seed_contract != seed || edge.primary_chain != primary {
            continue;
        }
        if let Some(scope) = by_secondary.get_mut(edge.secondary_chain as usize) {
            scope.insert_edge(
                edge.dimension,
                edge.candidate_nft,
                edge.candidate_contract,
                contract_nfts,
            );
        }
        if edge.secondary_chain != primary {
            cross.insert_edge(
                edge.dimension,
                edge.candidate_nft,
                edge.candidate_contract,
                contract_nfts,
            );
        }
    }

    let intra_chain = rows_from_dimension_sets(
        store,
        std::iter::once(primary),
        std::mem::take(&mut by_secondary[primary as usize]),
    );

    let mut chain_matrix = Vec::new();
    for (idx, _name) in store.chains.iter().enumerate() {
        let secondary = idx as ChainId;
        if secondary == primary {
            continue;
        }
        let rows = rows_from_dimension_sets(
            store,
            std::iter::once(secondary),
            std::mem::take(&mut by_secondary[idx]),
        );
        chain_matrix.push(ChainMatrixBlock {
            secondary_chain: store.chain_name(secondary).to_owned(),
            rows,
        });
    }
    chain_matrix.sort_by(|a, b| a.secondary_chain.cmp(&b.secondary_chain));

    let cross_chain_summary = rows_from_dimension_sets(
        store,
        (0..store.chains.len())
            .map(|idx| idx as ChainId)
            .filter(|chain| *chain != primary),
        cross,
    );

    SeedDuplicateScale {
        intra_chain,
        chain_matrix,
        cross_chain_summary,
    }
}

/// Minimal relation view for batch-level scale aggregation (any scope).
#[derive(Clone, Copy, Debug)]
pub struct AllChainsRelationRef<'a> {
    pub seed_chain: &'a str,
    pub seed_address: &'a str,
    pub candidate_chain: &'a str,
    pub candidate_address: &'a str,
    pub dimensions: &'a [String],
    pub nft_ids: &'a [u32],
}

/// Which seed→candidate relations count toward a batch-level scope rollup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeScaleFilter<'a> {
    /// All relations (all_chains).
    All,
    /// Same-chain only.
    Intra,
    /// Different-chain only (cross_chain_summary).
    Cross,
    /// One directional primary → secondary matrix cell.
    Matrix {
        primary_chain: &'a str,
        secondary_chain: &'a str,
    },
}

fn relation_matches(rel: &AllChainsRelationRef<'_>, filter: ScopeScaleFilter<'_>) -> bool {
    let same = rel.seed_chain.eq_ignore_ascii_case(rel.candidate_chain);
    match filter {
        ScopeScaleFilter::All => true,
        ScopeScaleFilter::Intra => same,
        ScopeScaleFilter::Cross => !same,
        ScopeScaleFilter::Matrix {
            primary_chain,
            secondary_chain,
        } => {
            !same
                && rel.seed_chain.eq_ignore_ascii_case(primary_chain)
                && rel.candidate_chain.eq_ignore_ascii_case(secondary_chain)
        }
    }
}

fn snapshot_denominators(
    store: &ResidentStore,
    denominator_chains: &AHashSet<String>,
) -> (u64, u64) {
    let mut nft_denom = 0u64;
    let mut contract_denom = 0u64;
    for name in denominator_chains {
        if let Some(&cid) = store.chain_ids.get(name) {
            let totals = store.totals.get(&cid).cloned().unwrap_or_default();
            nft_denom += totals.nfts;
            contract_denom += totals.contracts;
        }
    }
    (nft_denom, contract_denom)
}

fn denominator_chains(
    store: &ResidentStore,
    primary_chains: &AHashSet<String>,
    filter: ScopeScaleFilter<'_>,
) -> AHashSet<String> {
    match filter {
        ScopeScaleFilter::All => store
            .chains
            .iter()
            .map(|chain| chain.to_ascii_lowercase())
            .collect(),
        ScopeScaleFilter::Intra => primary_chains.clone(),
        ScopeScaleFilter::Cross => store
            .chains
            .iter()
            .filter(|candidate| {
                primary_chains
                    .iter()
                    .any(|primary| !candidate.eq_ignore_ascii_case(primary))
            })
            .map(|chain| chain.to_ascii_lowercase())
            .collect(),
        ScopeScaleFilter::Matrix {
            secondary_chain, ..
        } => std::iter::once(secondary_chain.to_ascii_lowercase()).collect(),
    }
}

/// Batch-level duplicate scale for one reporting scope (set-union numerators).
pub fn build_scope_duplicate_scale<'a>(
    store: &ResidentStore,
    relations: impl IntoIterator<Item = AllChainsRelationRef<'a>>,
    filter: ScopeScaleFilter<'_>,
) -> Vec<DuplicateScaleRow> {
    let relations: Vec<_> = relations.into_iter().collect();
    let primary_chains = relations
        .iter()
        .map(|rel| rel.seed_chain.to_ascii_lowercase())
        .collect();
    build_scope_duplicate_scale_for_chains(store, relations, filter, &primary_chains)
}

/// Batch-level duplicate scale with an explicit formal-primary universe.
///
/// Denominators are independent of positive hits. Numerators always identify
/// candidate-side NFTs/contracts, so cross/matrix denominators use candidate
/// chain snapshot totals.
pub fn build_scope_duplicate_scale_for_chains<'a>(
    store: &ResidentStore,
    relations: impl IntoIterator<Item = AllChainsRelationRef<'a>>,
    filter: ScopeScaleFilter<'_>,
    primary_chains: &AHashSet<String>,
) -> Vec<DuplicateScaleRow> {
    let mut filtered: Vec<AllChainsRelationRef<'a>> = Vec::new();
    for rel in relations {
        if relation_matches(&rel, filter) {
            filtered.push(rel);
        }
    }
    let denominator_chains = denominator_chains(store, primary_chains, filter);
    let (nft_denom, contract_denom) = snapshot_denominators(store, &denominator_chains);

    let mut token_uri_nfts = AHashSet::new();
    let mut image_uri_nfts = AHashSet::new();
    let mut metadata_nfts = AHashSet::new();
    let mut name_nfts = AHashSet::new();
    let mut total_nfts = AHashSet::new();

    let mut token_uri_contracts = AHashSet::new();
    let mut image_uri_contracts = AHashSet::new();
    let mut metadata_contracts = AHashSet::new();
    let mut name_contracts = AHashSet::new();
    let mut total_contracts = AHashSet::new();

    for rel in filtered {
        let cand_key = format!("{}:{}", rel.candidate_chain, rel.candidate_address);
        total_contracts.insert(cand_key.clone());
        for &nft in rel.nft_ids.iter() {
            total_nfts.insert(nft);
        }

        let has_token = rel.dimensions.iter().any(|d| d == "token_uri");
        let has_image = rel.dimensions.iter().any(|d| d == "image_uri");
        let has_metadata = rel.dimensions.iter().any(|d| d == "metadata");
        let has_name = rel.dimensions.iter().any(|d| d == "name");

        if has_token {
            token_uri_contracts.insert(cand_key.clone());
            token_uri_nfts.extend(rel.nft_ids.iter().copied());
        }
        if has_image {
            image_uri_contracts.insert(cand_key.clone());
            image_uri_nfts.extend(rel.nft_ids.iter().copied());
        }
        if has_metadata {
            metadata_contracts.insert(cand_key.clone());
            metadata_nfts.extend(rel.nft_ids.iter().copied());
        }
        if has_name {
            name_contracts.insert(cand_key);
            name_nfts.extend(rel.nft_ids.iter().copied());
        }
    }

    image_uri_nfts.retain(|nft| !token_uri_nfts.contains(nft));

    vec![
        scale_row(
            "token_uri",
            token_uri_nfts.len() as u64,
            token_uri_contracts.len() as u64,
            nft_denom,
            contract_denom,
        ),
        scale_row(
            "image_uri",
            image_uri_nfts.len() as u64,
            image_uri_contracts.len() as u64,
            nft_denom,
            contract_denom,
        ),
        scale_row(
            "metadata",
            metadata_nfts.len() as u64,
            metadata_contracts.len() as u64,
            nft_denom,
            contract_denom,
        ),
        scale_row(
            "name",
            name_nfts.len() as u64,
            name_contracts.len() as u64,
            nft_denom,
            contract_denom,
        ),
        scale_row(
            "total",
            total_nfts.len() as u64,
            total_contracts.len() as u64,
            nft_denom,
            contract_denom,
        ),
    ]
}

/// Batch-level `all_chains` duplicate scale (fourth export dimension).
pub fn build_all_chains_duplicate_scale<'a>(
    store: &ResidentStore,
    relations: impl IntoIterator<Item = AllChainsRelationRef<'a>>,
) -> Vec<DuplicateScaleRow> {
    build_scope_duplicate_scale(store, relations, ScopeScaleFilter::All)
}

/// Distinct secondary chains appearing in cross-chain relations (sorted).
pub fn matrix_secondary_chains<'a>(
    relations: impl IntoIterator<Item = AllChainsRelationRef<'a>>,
) -> Vec<String> {
    let mut set = AHashSet::new();
    for rel in relations {
        if !rel.seed_chain.eq_ignore_ascii_case(rel.candidate_chain) {
            set.insert(rel.candidate_chain.to_ascii_lowercase());
        }
    }
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort();
    v
}
