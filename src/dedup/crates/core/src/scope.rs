use crate::entity::{ChainId, Dimension, ScopeKind};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeKey {
    pub kind: ScopeKind,
    pub primary_chain: ChainId,
    pub secondary_chain: Option<ChainId>,
    pub dimension: Dimension,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeCounts {
    pub duplicate_contract_count: u64,
    pub duplicate_nft_count: u64,
}

impl ScopeCounts {
    pub fn add_contract(&mut self, nft_count: u64) {
        self.duplicate_contract_count += 1;
        self.duplicate_nft_count += nft_count;
    }

    pub fn add_nfts(&mut self, nft_count: u64) {
        self.duplicate_nft_count += nft_count;
    }
}
