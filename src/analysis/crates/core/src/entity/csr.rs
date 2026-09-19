//! Compact sparse-row posting index and chain-grouped URI postings.

use super::ids::{ChainId, ContractId, NftId, StringId};

/// CSR postings: sorted keys with contiguous value slices via offsets.
///
/// Layout: `values[offsets[i]..offsets[i + 1]]` belongs to `keys[i]`.
#[derive(Clone, Debug, Default)]
pub struct CsrIndex {
    pub keys: Vec<u32>,
    pub offsets: Vec<u32>,
    pub values: Vec<u32>,
}

impl CsrIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Binary-search `key` and return its value slice, if present.
    pub fn values_for(&self, key: u32) -> Option<&[u32]> {
        let index = self.keys.binary_search(&key).ok()?;
        let start = self.offsets.get(index).copied()? as usize;
        let end = self.offsets.get(index + 1).copied()? as usize;
        self.values.get(start..end)
    }

    /// Release backing storage (used after a dimension stage finishes).
    pub fn clear(&mut self) {
        self.keys.clear();
        self.keys.shrink_to_fit();
        self.offsets.clear();
        self.offsets.shrink_to_fit();
        self.values.clear();
        self.values.shrink_to_fit();
    }

    /// Build CSR from sorted `(key, value)` pairs (already grouped by key).
    pub fn from_sorted_pairs(pairs: &[(u32, u32)]) -> Self {
        if pairs.is_empty() {
            return Self {
                keys: Vec::new(),
                offsets: vec![0],
                values: Vec::new(),
            };
        }
        let mut keys = Vec::new();
        let mut offsets = vec![0_u32];
        let mut values = Vec::with_capacity(pairs.len());
        let mut index = 0;
        while index < pairs.len() {
            let key = pairs[index].0;
            let start = index;
            index += 1;
            while index < pairs.len() && pairs[index].0 == key {
                index += 1;
            }
            keys.push(key);
            for &(_, value) in &pairs[start..index] {
                values.push(value);
            }
            offsets.push(values.len() as u32);
        }
        Self {
            keys,
            offsets,
            values,
        }
    }
}

/// One contiguous chain run inside a URI's NFT postings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UriChainRun {
    pub chain_id: ChainId,
    pub start: u32,
    pub end: u32,
}

/// URI → NFT postings pre-grouped by chain for O(hit-chains) query expansion.
///
/// For each URI key, `values[value_offsets[i]..value_offsets[i+1]]` holds all
/// member NFT ids sorted by `(chain_id, nft_id)`. `runs` subdivides that range
/// into per-chain slices so queries avoid re-walking `nft → contract → chain`.
#[derive(Clone, Debug, Default)]
pub struct UriChainIndex {
    pub keys: Vec<u32>,
    pub value_offsets: Vec<u32>,
    pub values: Vec<u32>,
    pub run_offsets: Vec<u32>,
    pub runs: Vec<UriChainRun>,
}

impl UriChainIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    pub fn clear(&mut self) {
        self.keys.clear();
        self.keys.shrink_to_fit();
        self.value_offsets.clear();
        self.value_offsets.shrink_to_fit();
        self.values.clear();
        self.values.shrink_to_fit();
        self.run_offsets.clear();
        self.run_offsets.shrink_to_fit();
        self.runs.clear();
        self.runs.shrink_to_fit();
    }

    /// All NFT members for `key` (across chains), if present.
    pub fn values_for(&self, key: u32) -> Option<&[u32]> {
        let index = self.keys.binary_search(&key).ok()?;
        let start = self.value_offsets.get(index).copied()? as usize;
        let end = self.value_offsets.get(index + 1).copied()? as usize;
        self.values.get(start..end)
    }

    /// Per-chain runs for `key`.
    pub fn runs_for(&self, key: u32) -> Option<&[UriChainRun]> {
        let index = self.keys.binary_search(&key).ok()?;
        let start = self.run_offsets.get(index).copied()? as usize;
        let end = self.run_offsets.get(index + 1).copied()? as usize;
        self.runs.get(start..end)
    }

    /// Fill `by_chain[chain]` with NFT members for `key` (clears each bucket first).
    pub fn fill_by_chain(&self, key: u32, by_chain: &mut [Vec<u32>]) -> bool {
        for bucket in by_chain.iter_mut() {
            bucket.clear();
        }
        let Some(runs) = self.runs_for(key) else {
            return false;
        };
        for run in runs {
            let chain = run.chain_id as usize;
            if chain >= by_chain.len() {
                continue;
            }
            let start = run.start as usize;
            let end = run.end as usize;
            by_chain[chain].extend_from_slice(&self.values[start..end]);
        }
        true
    }

    /// Build from sorted `(uri_id, chain_id, nft_id)` triples.
    pub fn from_sorted_triples(triples: &[(u32, u16, u32)]) -> Self {
        if triples.is_empty() {
            return Self {
                keys: Vec::new(),
                value_offsets: vec![0],
                values: Vec::new(),
                run_offsets: vec![0],
                runs: Vec::new(),
            };
        }
        let mut keys = Vec::new();
        let mut value_offsets = vec![0_u32];
        let mut values = Vec::with_capacity(triples.len());
        let mut run_offsets = vec![0_u32];
        let mut runs = Vec::new();

        let mut index = 0;
        while index < triples.len() {
            let uri = triples[index].0;
            let uri_start = index;
            index += 1;
            while index < triples.len() && triples[index].0 == uri {
                index += 1;
            }
            keys.push(uri);

            let mut run_at = uri_start;
            while run_at < index {
                let chain = triples[run_at].1;
                let run_start_values = values.len() as u32;
                while run_at < index && triples[run_at].1 == chain {
                    values.push(triples[run_at].2);
                    run_at += 1;
                }
                runs.push(UriChainRun {
                    chain_id: chain,
                    start: run_start_values,
                    end: values.len() as u32,
                });
            }
            value_offsets.push(values.len() as u32);
            run_offsets.push(runs.len() as u32);
        }

        Self {
            keys,
            value_offsets,
            values,
            run_offsets,
            runs,
        }
    }
}

/// URI posting group identity (contract-scoped), for future builders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UriPostingKey {
    pub uri_id: StringId,
    pub contract_id: ContractId,
}

/// Name posting payload stub (contract- or NFT-level members).
#[derive(Clone, Debug, Default)]
pub struct NamePostingStub {
    pub name_id: Option<StringId>,
    pub contract_ids: Vec<ContractId>,
    pub nft_ids: Vec<NftId>,
}
