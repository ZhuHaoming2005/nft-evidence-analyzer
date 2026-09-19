//! Candidate registry derived from a HitGraph.

use ahash::{AHashMap, AHashSet};
use std::sync::Arc;

use crate::entity::{ContractId, NftId};

use super::hits::{Dimension, HitGraph};

/// One seed→candidate contract relation with merged dimensions and NFT union.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeedCandidateRelation {
    pub seed_contract: ContractId,
    pub candidate_contract: ContractId,
    pub dimensions: Vec<Dimension>,
    pub nft_ids: Arc<[NftId]>,
}

#[derive(Debug, Default)]
enum PendingNfts {
    #[default]
    Explicit,
    ExplicitSet(AHashSet<NftId>),
    WholeContract,
}

impl PendingNfts {
    fn insert_explicit(&mut self, nft: NftId) {
        match self {
            Self::Explicit => {
                *self = Self::ExplicitSet(AHashSet::from([nft]));
            }
            Self::ExplicitSet(nfts) => {
                nfts.insert(nft);
            }
            Self::WholeContract => {}
        }
    }
}

/// Unique candidates plus per-seed relations (multi-seed hits keep separate relations).
#[derive(Clone, Debug, Default)]
pub struct CandidateRegistry {
    candidate_contracts: Vec<ContractId>,
    relations: Vec<SeedCandidateRelation>,
    relations_by_seed: AHashMap<ContractId, Vec<usize>>,
}

impl CandidateRegistry {
    pub fn from_hit_graph(
        graph: &HitGraph,
        contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
    ) -> Self {
        Self::from_hit_graphs(std::iter::once(graph), contract_nfts)
    }

    /// Build one registry directly from seed-local graphs.
    ///
    /// Keeping the graphs seed-local avoids constructing a second global edge
    /// buffer and lets reporting scan only the selected seed's edges.
    pub fn from_hit_graphs<'a>(
        graphs: impl IntoIterator<Item = &'a HitGraph>,
        contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
    ) -> Self {
        // (seed, candidate) → (dimensions, nfts)
        let mut pair_dims: AHashMap<(ContractId, ContractId), AHashSet<Dimension>> =
            AHashMap::new();
        let mut pair_nfts: AHashMap<(ContractId, ContractId), PendingNfts> = AHashMap::new();
        let mut candidates: AHashSet<ContractId> = AHashSet::new();

        for graph in graphs {
            for edge in graph.edges() {
                let key = (edge.seed_contract, edge.candidate_contract);
                candidates.insert(edge.candidate_contract);
                pair_dims.entry(key).or_default().insert(edge.dimension);
                match edge.candidate_nft {
                    Some(nft) => {
                        pair_nfts.entry(key).or_default().insert_explicit(nft);
                    }
                    None => {
                        // Keep whole-contract hits symbolic during aggregation;
                        // expanding once per candidate avoids one large set per
                        // seed relation and preserves the exact final id list.
                        pair_nfts.insert(key, PendingNfts::WholeContract);
                    }
                }
            }
        }

        let mut candidate_contracts: Vec<ContractId> = candidates.into_iter().collect();
        candidate_contracts.sort_unstable();

        let mut keys: Vec<(ContractId, ContractId)> = pair_dims.keys().copied().collect();
        keys.sort_unstable();

        let mut relations = Vec::with_capacity(keys.len());
        let mut relations_by_seed: AHashMap<ContractId, Vec<usize>> = AHashMap::new();
        let mut whole_contract_nfts: AHashMap<ContractId, Arc<[NftId]>> = AHashMap::new();

        for key in keys {
            let (seed_contract, candidate_contract) = key;
            let mut dimensions: Vec<Dimension> = pair_dims
                .remove(&key)
                .unwrap_or_default()
                .into_iter()
                .collect();
            dimensions.sort_by_key(|d| dimension_ord(*d));
            let nft_ids = match pair_nfts.remove(&key).unwrap_or_default() {
                PendingNfts::WholeContract => whole_contract_nfts
                    .entry(candidate_contract)
                    .or_insert_with(|| {
                        let mut ids = contract_nfts
                            .get(&candidate_contract)
                            .cloned()
                            .unwrap_or_default();
                        ids.sort_unstable();
                        Arc::from(ids)
                    })
                    .clone(),
                PendingNfts::ExplicitSet(nfts) => {
                    let mut ids: Vec<NftId> = nfts.into_iter().collect();
                    ids.sort_unstable();
                    Arc::from(ids)
                }
                PendingNfts::Explicit => Arc::from([]),
            };

            let idx = relations.len();
            relations.push(SeedCandidateRelation {
                seed_contract,
                candidate_contract,
                dimensions,
                nft_ids,
            });
            relations_by_seed
                .entry(seed_contract)
                .or_default()
                .push(idx);
        }

        Self {
            candidate_contracts,
            relations,
            relations_by_seed,
        }
    }

    pub fn candidate_contracts(&self) -> &[ContractId] {
        &self.candidate_contracts
    }

    pub fn relations(&self) -> &[SeedCandidateRelation] {
        &self.relations
    }

    pub fn relations_for_seed(&self, seed: ContractId) -> Vec<&SeedCandidateRelation> {
        self.relations_by_seed
            .get(&seed)
            .into_iter()
            .flatten()
            .filter_map(|idx| self.relations.get(*idx))
            .collect()
    }

    pub fn candidate_contract_count(&self) -> usize {
        self.candidate_contracts.len()
    }

    /// Sub-registry containing only `keep` candidates and their seed relations.
    ///
    /// Used when reusing an evidence cache so HTTP enrich only runs for missing
    /// candidates (legit prefilter still sees the relevant seed relations).
    pub fn filter_candidates(&self, keep: &AHashSet<ContractId>) -> Self {
        if keep.is_empty() {
            return Self::default();
        }
        let mut candidate_contracts: Vec<ContractId> = self
            .candidate_contracts
            .iter()
            .copied()
            .filter(|id| keep.contains(id))
            .collect();
        candidate_contracts.sort_unstable();

        let mut relations = Vec::new();
        let mut relations_by_seed: AHashMap<ContractId, Vec<usize>> = AHashMap::new();
        for rel in &self.relations {
            if !keep.contains(&rel.candidate_contract) {
                continue;
            }
            let idx = relations.len();
            relations_by_seed
                .entry(rel.seed_contract)
                .or_default()
                .push(idx);
            relations.push(rel.clone());
        }

        Self {
            candidate_contracts,
            relations,
            relations_by_seed,
        }
    }
}

fn dimension_ord(d: Dimension) -> u8 {
    match d {
        Dimension::Name => 0,
        Dimension::TokenUri => 1,
        Dimension::ImageUri => 2,
        Dimension::Metadata => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::hits::{Dimension, HitEdge, HitGraph, ScopeKind};
    use crate::entity::ChainId;
    use crate::reporting::aggregate::count_scope_nfts;

    fn edge(
        seed: ContractId,
        cand: ContractId,
        nft: Option<NftId>,
        dim: Dimension,
        primary: ChainId,
        secondary: ChainId,
    ) -> HitEdge {
        HitEdge {
            seed_contract: seed,
            candidate_contract: cand,
            candidate_nft: nft,
            dimension: dim,
            score: 1.0,
            primary_chain: primary,
            secondary_chain: secondary,
        }
    }

    #[test]
    fn registry_unique_candidates_and_per_seed_relations() {
        let mut g = HitGraph::new();
        g.push(edge(1, 10, Some(100), Dimension::TokenUri, 0, 1));
        g.push(edge(1, 10, Some(101), Dimension::ImageUri, 0, 1));
        g.push(edge(1, 11, None, Dimension::Name, 0, 1));
        g.push(edge(2, 10, Some(100), Dimension::TokenUri, 0, 1));

        let mut contract_nfts = AHashMap::new();
        contract_nfts.insert(11, vec![200, 201]);

        let reg = CandidateRegistry::from_hit_graph(&g, &contract_nfts);
        assert_eq!(reg.candidate_contract_count(), 2);
        assert_eq!(reg.candidate_contracts(), &[10, 11]);

        let seed1 = reg.relations_for_seed(1);
        assert_eq!(seed1.len(), 2);
        let seed2 = reg.relations_for_seed(2);
        assert_eq!(seed2.len(), 1);
        assert_eq!(seed2[0].candidate_contract, 10);
        // Multi-seed on candidate 10: two relations, enrich once per unique candidate

        let only_11 = [11].into_iter().collect::<AHashSet<_>>();
        let filtered = reg.filter_candidates(&only_11);
        assert_eq!(filtered.candidate_contracts(), &[11]);
        assert_eq!(filtered.relations().len(), 1);
        assert_eq!(filtered.relations()[0].candidate_contract, 11);
        assert_eq!(reg.relations().len(), 3);
    }

    #[test]
    fn whole_contract_nft_ids_are_shared_across_seed_relations() {
        let mut graph = HitGraph::new();
        graph.push(edge(1, 10, None, Dimension::Name, 0, 1));
        graph.push(edge(2, 10, None, Dimension::Metadata, 0, 1));
        let contract_nfts = AHashMap::from([(10, vec![102, 100, 101])]);

        let registry = CandidateRegistry::from_hit_graph(&graph, &contract_nfts);
        let relations = registry.relations();
        assert_eq!(&*relations[0].nft_ids, &[100, 101, 102]);
        assert!(Arc::ptr_eq(&relations[0].nft_ids, &relations[1].nft_ids));
    }

    #[test]
    fn scope_counts_total_is_union_not_sum() {
        let mut g = HitGraph::new();
        g.push(edge(1, 2, Some(1), Dimension::TokenUri, 0, 0));
        g.push(edge(1, 2, Some(1), Dimension::ImageUri, 0, 0));
        g.push(edge(1, 2, Some(2), Dimension::ImageUri, 0, 0));
        g.push(edge(1, 3, None, Dimension::Name, 0, 0));

        let mut contract_nfts = AHashMap::new();
        contract_nfts.insert(3, vec![2, 3]); // overlaps image nft 2

        let counts = count_scope_nfts(&g, 1, ScopeKind::IntraChain, 0, None, &contract_nfts);
        assert_eq!(counts.token_uri, 1);
        assert_eq!(counts.image_uri, 1); // nft 1 excluded (already token_uri); nft 2 remains
        assert_eq!(counts.name, 2);
        assert_eq!(counts.metadata, 0);
        assert_eq!(counts.total, 3); // {1,2,3} — not 1+1+2
    }

    #[test]
    fn scope_counts_ignore_other_seed_edges_including_image_uri() {
        // Shared candidate NFT 100: seed A image-only, seed B token_uri — must not cross-exclude.
        // Distinct NFTs: seed A token 1; seed B image 2 / name expands 3,4.
        let mut g = HitGraph::new();
        g.push(edge(1, 10, Some(1), Dimension::TokenUri, 0, 0));
        g.push(edge(1, 10, Some(100), Dimension::ImageUri, 0, 0));
        g.push(edge(2, 10, Some(100), Dimension::TokenUri, 0, 0));
        g.push(edge(2, 10, Some(2), Dimension::ImageUri, 0, 0));
        g.push(edge(2, 11, None, Dimension::Name, 0, 0));

        let mut contract_nfts = AHashMap::new();
        contract_nfts.insert(11, vec![3, 4]);

        let a = count_scope_nfts(&g, 1, ScopeKind::IntraChain, 0, None, &contract_nfts);
        assert_eq!(a.token_uri, 1); // {1}
        assert_eq!(a.image_uri, 1); // {100} — not excluded by seed B's token_uri
        assert_eq!(a.name, 0);
        assert_eq!(a.metadata, 0);
        assert_eq!(a.total, 2); // {1, 100}

        let b = count_scope_nfts(&g, 2, ScopeKind::IntraChain, 0, None, &contract_nfts);
        assert_eq!(b.token_uri, 1); // {100}
        assert_eq!(b.image_uri, 1); // {2} — 100 excluded by seed B's own token_uri
        assert_eq!(b.name, 2); // {3, 4}
        assert_eq!(b.metadata, 0);
        assert_eq!(b.total, 4); // {100, 2, 3, 4} — ignores seed A edges
    }
}
