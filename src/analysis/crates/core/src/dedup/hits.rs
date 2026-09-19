//! Hit edges and HitGraph for seed→candidate dedup relations.

use ahash::{AHashMap, AHashSet};

use crate::entity::{ChainId, ContractId, NftId};

/// Dedup evidence dimension.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dimension {
    Name,
    TokenUri,
    ImageUri,
    Metadata,
}

/// Reporting / counting scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    IntraChain,
    ChainMatrix,
    CrossChainSummary,
}

/// One direct seed→candidate hit.
///
/// `candidate_nft`: `None` expands to the whole candidate contract; `Some` is that NFT only.
#[derive(Clone, Debug, PartialEq)]
pub struct HitEdge {
    pub seed_contract: ContractId,
    pub candidate_contract: ContractId,
    pub candidate_nft: Option<NftId>,
    pub dimension: Dimension,
    pub score: f64,
    pub primary_chain: ChainId,
    pub secondary_chain: ChainId,
}

/// Compact collection of accepted hit edges.
#[derive(Clone, Debug, Default)]
pub struct HitGraph {
    edges: Vec<HitEdge>,
}

impl HitGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    pub fn edges(&self) -> &[HitEdge] {
        &self.edges
    }

    pub(crate) fn into_edges(self) -> Vec<HitEdge> {
        self.edges
    }

    /// Push an edge. Seed self-hits (`seed_contract == candidate_contract`) are excluded.
    pub fn push(&mut self, edge: HitEdge) -> bool {
        if edge.seed_contract == edge.candidate_contract {
            return false;
        }
        self.edges.push(edge);
        true
    }

    /// Append already-validated edges from another seed-local graph without cloning.
    pub fn append(&mut self, other: &mut Self) {
        self.edges.append(&mut other.edges);
    }

    /// Whether `edge` participates in `scope` for the given primary (/ optional matrix secondary).
    pub fn edge_in_scope(
        edge: &HitEdge,
        scope: ScopeKind,
        primary_chain: ChainId,
        matrix_secondary: Option<ChainId>,
    ) -> bool {
        if edge.primary_chain != primary_chain {
            return false;
        }
        match scope {
            ScopeKind::IntraChain => edge.secondary_chain == primary_chain,
            ScopeKind::ChainMatrix => matrix_secondary.is_some_and(|sec| {
                edge.secondary_chain == sec && edge.secondary_chain != primary_chain
            }),
            ScopeKind::CrossChainSummary => edge.secondary_chain != primary_chain,
        }
    }

    /// Expand a single edge into candidate NFT ids.
    ///
    /// `None` = whole candidate contract (all NFTs in `contract_nfts`); `Some(nft)` = that NFT only
    /// (including Name/Metadata when engines attach a specific NFT).
    pub fn expand_edge_nfts(
        edge: &HitEdge,
        contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
    ) -> Vec<NftId> {
        match edge.candidate_nft {
            Some(nft) => vec![nft],
            None => contract_nfts
                .get(&edge.candidate_contract)
                .cloned()
                .unwrap_or_default(),
        }
    }

    /// Union of candidate NFTs for one `seed` under `scope` (four-dimension set union).
    pub fn union_candidate_nfts(
        &self,
        seed: ContractId,
        scope: ScopeKind,
        primary_chain: ChainId,
        matrix_secondary: Option<ChainId>,
        contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
    ) -> AHashSet<NftId> {
        let mut out = AHashSet::new();
        for edge in &self.edges {
            if edge.seed_contract != seed {
                continue;
            }
            if !Self::edge_in_scope(edge, scope, primary_chain, matrix_secondary) {
                continue;
            }
            match edge.candidate_nft {
                Some(nft) => {
                    out.insert(nft);
                }
                None => {
                    if let Some(nfts) = contract_nfts.get(&edge.candidate_contract) {
                        out.extend(nfts.iter().copied());
                    }
                }
            }
        }
        out
    }

    /// NFTs contributing to a single dimension for one `seed` under `scope`.
    ///
    /// `ImageUri` is supplemental: an NFT already hit by `TokenUri` for the same seed/scope
    /// is excluded from the ImageUri set (and therefore from ImageUri-only numerators).
    pub fn dimension_candidate_nfts(
        &self,
        seed: ContractId,
        dimension: Dimension,
        scope: ScopeKind,
        primary_chain: ChainId,
        matrix_secondary: Option<ChainId>,
        contract_nfts: &AHashMap<ContractId, Vec<NftId>>,
    ) -> AHashSet<NftId> {
        let mut out = AHashSet::new();
        for edge in &self.edges {
            if edge.seed_contract != seed {
                continue;
            }
            if edge.dimension != dimension {
                continue;
            }
            if !Self::edge_in_scope(edge, scope, primary_chain, matrix_secondary) {
                continue;
            }
            match edge.candidate_nft {
                Some(nft) => {
                    out.insert(nft);
                }
                None => {
                    if let Some(nfts) = contract_nfts.get(&edge.candidate_contract) {
                        out.extend(nfts.iter().copied());
                    }
                }
            }
        }
        if dimension == Dimension::ImageUri {
            let token_uri = self.dimension_candidate_nfts(
                seed,
                Dimension::TokenUri,
                scope,
                primary_chain,
                matrix_secondary,
                contract_nfts,
            );
            out.retain(|nft| !token_uri.contains(nft));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn self_hit_excluded() {
        let mut g = HitGraph::new();
        assert!(!g.push(edge(1, 1, Some(10), Dimension::TokenUri, 0, 0)));
        assert!(g.is_empty());
        assert!(g.push(edge(1, 2, Some(11), Dimension::TokenUri, 0, 0)));
        assert_eq!(g.len(), 1);
    }

    #[test]
    fn four_dimension_union_does_not_double_count_nft() {
        let mut g = HitGraph::new();
        let nft = 42u32;
        let cand = 7u32;
        // URI-level hits on the same NFT
        g.push(edge(1, cand, Some(nft), Dimension::TokenUri, 0, 1));
        g.push(edge(1, cand, Some(nft), Dimension::ImageUri, 0, 1));
        // Contract-level Name + Metadata expand to the same NFT via contract map
        g.push(edge(1, cand, None, Dimension::Name, 0, 1));
        g.push(edge(1, cand, None, Dimension::Metadata, 0, 1));

        let mut contract_nfts = AHashMap::new();
        contract_nfts.insert(cand, vec![nft, 43]);

        let union = g.union_candidate_nfts(1, ScopeKind::ChainMatrix, 0, Some(1), &contract_nfts);
        // Name/Metadata expand to {42, 43}; URI hits add 42 — union size is 2, not 4+
        assert_eq!(union.len(), 2);
        assert!(union.contains(&nft));
        assert!(union.contains(&43));
    }

    #[test]
    fn image_uri_supplemental_not_double_counted_with_token_uri() {
        let mut g = HitGraph::new();
        let shared = 100u32;
        let image_only = 101u32;
        g.push(edge(1, 2, Some(shared), Dimension::TokenUri, 0, 0));
        g.push(edge(1, 2, Some(shared), Dimension::ImageUri, 0, 0));
        g.push(edge(1, 2, Some(image_only), Dimension::ImageUri, 0, 0));

        let contract_nfts = AHashMap::new();
        let scope = ScopeKind::IntraChain;
        let total = g.union_candidate_nfts(1, scope, 0, None, &contract_nfts);
        assert_eq!(total.len(), 2);

        let token =
            g.dimension_candidate_nfts(1, Dimension::TokenUri, scope, 0, None, &contract_nfts);
        let image =
            g.dimension_candidate_nfts(1, Dimension::ImageUri, scope, 0, None, &contract_nfts);
        assert_eq!(token, AHashSet::from([shared]));
        // shared already counted under token_uri → excluded from image_uri numerator
        assert_eq!(image, AHashSet::from([image_only]));
        assert_eq!(token.len() + image.len(), total.len());
    }
}
