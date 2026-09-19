use crate::error::DedupError;
use crate::radix::sort_u32_triples;
use ahash::{AHashMap, AHashSet};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

pub type ContractId = u32;
pub type ChainId = u16;
pub type NftId = u32;
pub type StringId = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceOrder {
    pub file_ordinal: u32,
    pub file_row_number: u64,
}

#[derive(Clone, Debug)]
pub struct InputRow {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name_norm: String,
    pub token_uri_norm: String,
    pub image_uri_norm: String,
    pub metadata_json: String,
    pub source_order: SourceOrder,
}

#[derive(Clone, Debug)]
pub struct MetadataRecord {
    pub token_id: String,
    pub canonical_json: String,
    pub source_order: SourceOrder,
}

#[derive(Clone, Debug)]
pub struct Contract {
    pub id: ContractId,
    pub chain_id: ChainId,
    pub address: String,
    pub nft_count: u64,
    /// Sorted metadata records. A configured limit keeps only the first records.
    pub metadata_by_token: Vec<MetadataRecord>,
}

#[derive(Clone, Debug)]
pub struct Nft {
    pub id: NftId,
    pub contract_id: ContractId,
    pub token_id: String,
    /// Per-NFT normalized name. EVM Name analysis chooses a contract-level
    /// representative while Solana cross-chain analysis keeps this NFT unit.
    pub name_id: Option<StringId>,
    pub token_uri_id: Option<StringId>,
    pub image_uri_id: Option<StringId>,
    pub source_order: SourceOrder,
}

#[derive(Clone, Debug)]
pub struct UriPosting {
    pub contract_id: ContractId,
    pub chain_id: ChainId,
    pub uri_id: StringId,
    pub nft_ids: Vec<NftId>,
}

impl UriPosting {
    pub fn nft_count(&self) -> u64 {
        self.nft_ids.len() as u64
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChainTotals {
    pub contracts: u64,
    pub nfts: u64,
}

#[derive(Clone, Debug)]
pub struct EntityStore {
    pub chains: Vec<String>,
    pub chain_ids: AHashMap<String, ChainId>,
    pub contracts: Vec<Contract>,
    pub contract_index: AHashMap<(ChainId, String), ContractId>,
    pub nfts: Vec<Nft>,
    nft_index: AHashMap<(ContractId, String), NftId>,
    pub strings: Vec<String>,
    string_ids: AHashMap<String, StringId>,
    pub token_uri_postings: Vec<UriPosting>,
    pub image_uri_postings: Vec<UriPosting>,
    pub totals: AHashMap<ChainId, ChainTotals>,
    pub rows_loaded: u64,
    metadata_anchor_limit: Option<usize>,
    defer_metadata_sort: bool,
    evm_chains: AHashSet<String>,
}

impl Default for EntityStore {
    fn default() -> Self {
        Self::with_options(None, &AHashSet::default())
    }
}

impl EntityStore {
    pub fn with_options(
        metadata_anchor_limit: impl Into<Option<usize>>,
        evm_chains: &AHashSet<String>,
    ) -> Self {
        let metadata_anchor_limit = metadata_anchor_limit.into();
        Self {
            chains: Vec::new(),
            chain_ids: AHashMap::new(),
            contracts: Vec::new(),
            contract_index: AHashMap::new(),
            nfts: Vec::new(),
            nft_index: AHashMap::new(),
            strings: Vec::new(),
            string_ids: AHashMap::new(),
            token_uri_postings: Vec::new(),
            image_uri_postings: Vec::new(),
            totals: AHashMap::new(),
            rows_loaded: 0,
            metadata_anchor_limit,
            defer_metadata_sort: false,
            evm_chains: evm_chains.clone(),
        }
    }

    pub(crate) fn defer_unbounded_metadata_sort(&mut self) {
        self.defer_metadata_sort = self.metadata_anchor_limit.is_none();
    }

    pub fn finalize_metadata_anchors(&mut self) {
        if !self.defer_metadata_sort {
            return;
        }
        let chains = &self.chains;
        let evm_chains = &self.evm_chains;
        self.contracts.par_iter_mut().for_each(|contract| {
            let is_evm = evm_chains.contains(&chains[contract.chain_id as usize]);
            contract.metadata_by_token.sort_unstable_by(|left, right| {
                compare_token_ids(&left.token_id, &right.token_id, is_evm)
                    .then_with(|| left.token_id.cmp(&right.token_id))
                    .then_with(|| left.source_order.cmp(&right.source_order))
            });
            contract
                .metadata_by_token
                .dedup_by(|right, left| right.token_id == left.token_id);
        });
        self.defer_metadata_sort = false;
    }

    pub fn chain_name(&self, id: ChainId) -> &str {
        &self.chains[id as usize]
    }

    pub fn string(&self, id: StringId) -> &str {
        &self.strings[id as usize]
    }

    pub fn string_id(&self, value: &str) -> Option<StringId> {
        self.string_ids.get(value).copied()
    }

    pub(crate) fn nft_id(&self, contract_id: ContractId, token_id: &str) -> Option<NftId> {
        self.nft_index
            .get(&(contract_id, token_id.to_owned()))
            .copied()
    }

    pub fn is_solana_chain(&self, id: ChainId) -> bool {
        self.chain_name(id) == "solana"
    }

    pub fn ensure_chain(&mut self, chain: &str) -> Result<ChainId, DedupError> {
        if let Some(id) = self.chain_ids.get(chain) {
            return Ok(*id);
        }
        let id = ChainId::try_from(self.chains.len())
            .map_err(|_| DedupError::invalid("load", "too many chains for ChainId"))?;
        self.chains.push(chain.to_owned());
        self.chain_ids.insert(chain.to_owned(), id);
        self.totals.insert(id, ChainTotals::default());
        Ok(id)
    }

    fn intern_string(&mut self, value: &str) -> Result<StringId, DedupError> {
        if let Some(&id) = self.string_ids.get(value) {
            return Ok(id);
        }
        let id = StringId::try_from(self.strings.len())
            .map_err(|_| DedupError::invalid("load", "too many interned strings"))?;
        self.strings.push(value.to_owned());
        self.string_ids.insert(value.to_owned(), id);
        Ok(id)
    }

    /// Convenience entry point used by small unit fixtures.
    pub fn ingest_row(&mut self, row: InputRow) {
        self.try_ingest_row(row)
            .expect("fixture row must satisfy the snapshot contract");
    }

    pub fn try_ingest_row(&mut self, row: InputRow) -> Result<(), DedupError> {
        if row.chain.is_empty() || row.contract_address.is_empty() || row.token_id.is_empty() {
            return Ok(());
        }
        let chain_id = self.ensure_chain(&row.chain)?;
        let contract_key = (chain_id, row.contract_address.clone());
        let contract_id = if let Some(id) = self.contract_index.get(&contract_key).copied() {
            id
        } else {
            let id = ContractId::try_from(self.contracts.len())
                .map_err(|_| DedupError::invalid("load", "too many contracts for ContractId"))?;
            self.contracts.push(Contract {
                id,
                chain_id,
                address: row.contract_address.clone(),
                nft_count: 0,
                metadata_by_token: Vec::new(),
            });
            self.contract_index.insert(contract_key, id);
            self.totals.entry(chain_id).or_default().contracts += 1;
            id
        };

        if let Some(canonical_json) = validated_metadata(&row.metadata_json) {
            self.insert_metadata_anchor(
                contract_id,
                &row.chain,
                row.token_id.clone(),
                canonical_json,
                row.source_order,
            );
        }

        let nft_key = (contract_id, row.token_id.clone());
        if let Some(&nft_id) = self.nft_index.get(&nft_key) {
            self.merge_duplicate_nft(nft_id, &row)?;
            return Ok(());
        }

        let token_uri_id = if row.token_uri_norm.is_empty() {
            None
        } else {
            Some(self.intern_string(&row.token_uri_norm)?)
        };
        let image_uri_id = if row.image_uri_norm.is_empty() {
            None
        } else {
            Some(self.intern_string(&row.image_uri_norm)?)
        };
        let name_id = if row.name_norm.trim().is_empty() {
            None
        } else {
            Some(self.intern_string(&row.name_norm)?)
        };
        let nft_id = NftId::try_from(self.nfts.len())
            .map_err(|_| DedupError::invalid("load", "too many NFTs for NftId"))?;
        self.nfts.push(Nft {
            id: nft_id,
            contract_id,
            token_id: row.token_id.clone(),
            name_id,
            token_uri_id,
            image_uri_id,
            source_order: row.source_order,
        });
        self.nft_index.insert(nft_key, nft_id);
        self.contracts[contract_id as usize].nft_count += 1;
        self.totals.entry(chain_id).or_default().nfts += 1;
        self.rows_loaded += 1;
        Ok(())
    }

    fn merge_duplicate_nft(&mut self, nft_id: NftId, row: &InputRow) -> Result<(), DedupError> {
        let existing_name = self.nfts[nft_id as usize].name_id;
        let existing_token = self.nfts[nft_id as usize].token_uri_id;
        let existing_image = self.nfts[nft_id as usize].image_uri_id;
        let name_id = if existing_name.is_none() && !row.name_norm.trim().is_empty() {
            Some(self.intern_string(&row.name_norm)?)
        } else {
            existing_name
        };
        let token_uri_id =
            self.merge_uri_value(existing_token, &row.token_uri_norm, "token_uri_norm", row)?;
        let image_uri_id =
            self.merge_uri_value(existing_image, &row.image_uri_norm, "image_uri_norm", row)?;
        let nft = &mut self.nfts[nft_id as usize];
        nft.name_id = name_id;
        nft.token_uri_id = token_uri_id;
        nft.image_uri_id = image_uri_id;
        Ok(())
    }

    fn merge_uri_value(
        &mut self,
        existing: Option<StringId>,
        incoming: &str,
        field: &str,
        row: &InputRow,
    ) -> Result<Option<StringId>, DedupError> {
        if incoming.is_empty() {
            return Ok(existing);
        }
        if let Some(id) = existing {
            if self.string(id) != incoming {
                return Err(DedupError::invalid(
                    "load",
                    format!(
                        "snapshot conflict for ({}, {}, {}): distinct {field} values",
                        row.chain, row.contract_address, row.token_id
                    ),
                ));
            }
            return Ok(existing);
        }
        self.intern_string(incoming).map(Some)
    }

    fn insert_metadata_anchor(
        &mut self,
        contract_id: ContractId,
        chain: &str,
        token_id: String,
        canonical_json: String,
        source_order: SourceOrder,
    ) {
        if self.metadata_anchor_limit == Some(0) {
            return;
        }
        if self.defer_metadata_sort {
            self.contracts[contract_id as usize]
                .metadata_by_token
                .push(MetadataRecord {
                    token_id,
                    canonical_json,
                    source_order,
                });
            return;
        }
        let is_evm = self.evm_chains.contains(chain);
        let anchors = &mut self.contracts[contract_id as usize].metadata_by_token;
        if anchors.iter().any(|record| record.token_id == token_id) {
            return;
        }
        let insert_at = anchors
            .binary_search_by(|record| compare_token_ids(&record.token_id, &token_id, is_evm))
            .unwrap_or_else(|position| position);
        if let Some(limit) = self.metadata_anchor_limit
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
        if let Some(limit) = self.metadata_anchor_limit
            && anchors.len() > limit
        {
            anchors.pop();
        }
    }

    pub fn rebuild_uri_postings(&mut self) {
        let (token_uri_postings, image_uri_postings) = rayon::join(
            || build_uri_postings(&self.contracts, &self.nfts, true),
            || build_uri_postings(&self.contracts, &self.nfts, false),
        );
        self.token_uri_postings = token_uri_postings;
        self.image_uri_postings = image_uri_postings;
    }

    pub(crate) fn release_metadata(&mut self) {
        self.release_metadata_payloads();
        self.release_contract_addresses();
    }

    pub(crate) fn release_metadata_payloads(&mut self) {
        self.release_metadata_records();
        self.nfts
            .par_iter_mut()
            .for_each(|nft| nft.token_id = String::new());
        self.nft_index = AHashMap::new();
    }

    pub(crate) fn release_metadata_records(&mut self) {
        self.contracts.par_iter_mut().for_each(|contract| {
            contract.metadata_by_token = Vec::new();
        });
    }

    pub(crate) fn release_contract_addresses(&mut self) {
        self.contracts
            .par_iter_mut()
            .for_each(|contract| contract.address = String::new());
    }

    pub fn release_completed_dimension_data(&mut self) {
        self.release_completed_dimension_data_with_image_retention(false);
    }

    pub fn release_completed_dimension_data_preserving_image_uris(&mut self) {
        self.release_completed_dimension_data_with_image_retention(true);
    }

    fn release_completed_dimension_data_with_image_retention(&mut self, preserve_images: bool) {
        self.token_uri_postings = Vec::new();
        self.image_uri_postings = Vec::new();
        if preserve_images {
            let mut remap = AHashMap::<StringId, StringId>::new();
            let mut image_strings = Vec::new();
            for nft in &mut self.nfts {
                let Some(old_id) = nft.image_uri_id else {
                    continue;
                };
                let new_id = if let Some(&id) = remap.get(&old_id) {
                    id
                } else {
                    let id = StringId::try_from(image_strings.len())
                        .expect("image URI count was already bounded by StringId");
                    image_strings.push(self.strings[old_id as usize].clone());
                    remap.insert(old_id, id);
                    id
                };
                nft.image_uri_id = Some(new_id);
            }
            self.strings = image_strings;
        } else {
            self.strings = Vec::new();
        }
        self.string_ids = AHashMap::new();
        self.chain_ids = AHashMap::new();
        self.contract_index = AHashMap::new();
        self.nfts.par_iter_mut().for_each(|nft| {
            nft.name_id = None;
            nft.token_uri_id = None;
            if !preserve_images {
                nft.image_uri_id = None;
            }
        });
    }

    pub fn merge_shard(&mut self, shard: EntityStore) -> Result<(), DedupError> {
        let EntityStore {
            chains,
            contracts,
            nfts,
            strings,
            ..
        } = shard;

        let mut chain_map = Vec::with_capacity(chains.len());
        for chain in &chains {
            chain_map.push(self.ensure_chain(chain)?);
        }
        let mut string_map = Vec::with_capacity(strings.len());
        for value in &strings {
            string_map.push(self.intern_string(value)?);
        }

        let mut contract_map = vec![0; contracts.len()];
        for contract in contracts {
            let chain_id = chain_map[contract.chain_id as usize];
            let key = (chain_id, contract.address.clone());
            let contract_id = if let Some(&existing) = self.contract_index.get(&key) {
                existing
            } else {
                let id = ContractId::try_from(self.contracts.len()).map_err(|_| {
                    DedupError::invalid("load", "too many contracts for ContractId")
                })?;
                self.contracts.push(Contract {
                    id,
                    chain_id,
                    address: contract.address.clone(),
                    nft_count: 0,
                    metadata_by_token: Vec::new(),
                });
                self.contract_index.insert(key, id);
                self.totals.entry(chain_id).or_default().contracts += 1;
                id
            };
            contract_map[contract.id as usize] = contract_id;
            let chain_name = self.chains[chain_id as usize].clone();
            for record in contract.metadata_by_token {
                self.insert_metadata_anchor(
                    contract_id,
                    &chain_name,
                    record.token_id,
                    record.canonical_json,
                    record.source_order,
                );
            }
        }

        for nft in nfts {
            let contract_id = contract_map[nft.contract_id as usize];
            let name_id = nft.name_id.map(|id| string_map[id as usize]);
            let token_uri_id = nft.token_uri_id.map(|id| string_map[id as usize]);
            let image_uri_id = nft.image_uri_id.map(|id| string_map[id as usize]);
            let nft_key = (contract_id, nft.token_id.clone());
            if let Some(&existing_id) = self.nft_index.get(&nft_key) {
                let existing = &mut self.nfts[existing_id as usize];
                if existing.name_id.is_none() {
                    existing.name_id = name_id;
                }
                existing.token_uri_id = merge_mapped_uri(
                    existing.token_uri_id,
                    token_uri_id,
                    "token_uri_norm",
                    contract_id,
                    &nft.token_id,
                )?;
                existing.image_uri_id = merge_mapped_uri(
                    existing.image_uri_id,
                    image_uri_id,
                    "image_uri_norm",
                    contract_id,
                    &nft.token_id,
                )?;
                continue;
            }
            let nft_id = NftId::try_from(self.nfts.len())
                .map_err(|_| DedupError::invalid("load", "too many NFTs for NftId"))?;
            self.nfts.push(Nft {
                id: nft_id,
                contract_id,
                token_id: nft.token_id.clone(),
                name_id,
                token_uri_id,
                image_uri_id,
                source_order: nft.source_order,
            });
            self.nft_index.insert(nft_key, nft_id);
            let chain_id = self.contracts[contract_id as usize].chain_id;
            self.contracts[contract_id as usize].nft_count += 1;
            self.totals.entry(chain_id).or_default().nfts += 1;
            self.rows_loaded += 1;
        }
        Ok(())
    }

    /// Keep only listed chains. An empty `allowed` means keep all; no matches means keep none.
    pub fn retain_chains(&mut self, allowed: &std::collections::BTreeSet<String>) {
        if allowed.is_empty() {
            return;
        }
        let evm = self.evm_chains.clone();
        let mut replacement = Self::with_options(self.metadata_anchor_limit, &evm);
        let old = std::mem::replace(self, Self::with_options(self.metadata_anchor_limit, &evm));
        for nft in &old.nfts {
            let contract = &old.contracts[nft.contract_id as usize];
            let chain = old.chain_name(contract.chain_id);
            if !allowed.contains(chain) {
                continue;
            }
            let metadata = contract
                .metadata_by_token
                .iter()
                .find(|record| record.token_id == nft.token_id)
                .map(|record| record.canonical_json.clone())
                .unwrap_or_default();
            replacement
                .try_ingest_row(InputRow {
                    chain: chain.to_owned(),
                    contract_address: contract.address.clone(),
                    token_id: nft.token_id.clone(),
                    name_norm: nft
                        .name_id
                        .map(|id| old.string(id).to_owned())
                        .unwrap_or_default(),
                    token_uri_norm: nft
                        .token_uri_id
                        .map(|id| old.string(id).to_owned())
                        .unwrap_or_default(),
                    image_uri_norm: nft
                        .image_uri_id
                        .map(|id| old.string(id).to_owned())
                        .unwrap_or_default(),
                    metadata_json: metadata,
                    source_order: nft.source_order,
                })
                .expect("retaining a validated store cannot introduce conflicts");
        }
        replacement.rebuild_uri_postings();
        *self = replacement;
    }
}

fn merge_mapped_uri(
    existing: Option<StringId>,
    incoming: Option<StringId>,
    field: &str,
    contract_id: ContractId,
    token_id: &str,
) -> Result<Option<StringId>, DedupError> {
    match (existing, incoming) {
        (None, value) => Ok(value),
        (value, None) => Ok(value),
        (Some(existing), Some(incoming)) if existing == incoming => Ok(Some(existing)),
        (Some(_), Some(_)) => Err(DedupError::invalid(
            "load",
            format!(
                "snapshot conflict for contract {contract_id}, token {token_id}: distinct {field} values"
            ),
        )),
    }
}

fn build_uri_postings(
    contracts: &[Contract],
    nfts: &[Nft],
    token_dimension: bool,
) -> Vec<UriPosting> {
    let mut tuples: Vec<(StringId, ContractId, NftId)> = nfts
        .iter()
        .filter_map(|nft| {
            let uri = if token_dimension {
                nft.token_uri_id
            } else {
                nft.image_uri_id
            }?;
            Some((uri, nft.contract_id, nft.id))
        })
        .collect();
    sort_u32_triples(&mut tuples);
    let mut postings = Vec::new();
    let mut index = 0;
    while index < tuples.len() {
        let (uri_id, contract_id, _) = tuples[index];
        let mut end = index + 1;
        while end < tuples.len() && tuples[end].0 == uri_id && tuples[end].1 == contract_id {
            end += 1;
        }
        postings.push(UriPosting {
            contract_id,
            chain_id: contracts[contract_id as usize].chain_id,
            uri_id,
            nft_ids: tuples[index..end].iter().map(|tuple| tuple.2).collect(),
        });
        index = end;
    }
    postings
}

pub fn compare_token_ids(left: &str, right: &str, is_evm: bool) -> Ordering {
    if is_evm {
        match (decimal_magnitude(left), decimal_magnitude(right)) {
            (Some(a), Some(b)) => a.len().cmp(&b.len()).then_with(|| a.cmp(b)),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => left.cmp(right),
        }
    } else {
        left.cmp(right)
    }
}

fn decimal_magnitude(token: &str) -> Option<&str> {
    let trimmed = token.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let magnitude = trimmed.trim_start_matches('0');
    Some(if magnitude.is_empty() { "0" } else { magnitude })
}

pub fn is_valid_metadata(content: &str) -> bool {
    validated_metadata(content).is_some()
}

fn validated_metadata(content: &str) -> Option<String> {
    let trimmed = content.trim();
    if trimmed.is_empty() || trimmed.len() > 64 * 1024 {
        return None;
    }
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return None;
    }
    let canonical = crate::metadata::canonicalize_json_strict(trimmed)?;
    (canonical != "{}").then_some(canonical)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dimension {
    Name,
    TokenUri,
    ImageUri,
    Metadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeKind {
    IntraChain,
    CrossChainSummary,
    ChainMatrix,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(token_id: &str, token_uri: &str, metadata: &str) -> InputRow {
        InputRow {
            chain: "ethereum".to_owned(),
            contract_address: "0xabc".to_owned(),
            token_id: token_id.to_owned(),
            name_norm: "collection".to_owned(),
            token_uri_norm: token_uri.to_owned(),
            image_uri_norm: String::new(),
            metadata_json: metadata.to_owned(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: token_id.parse().unwrap_or(0),
            },
        }
    }

    #[test]
    fn duplicate_nft_key_counts_once() {
        let mut store = EntityStore::default();
        store.try_ingest_row(row("1", "uri://one", "")).unwrap();
        store.try_ingest_row(row("1", "uri://one", "")).unwrap();
        assert_eq!(store.rows_loaded, 1);
        assert_eq!(store.contracts[0].nft_count, 1);
        assert_eq!(store.nfts.len(), 1);
    }

    #[test]
    fn completed_stage_data_releases_owned_strings_and_lookup_indexes() {
        let mut store = EntityStore::default();
        store
            .try_ingest_row(row("1", "uri://one", r#"{"name":"one"}"#))
            .unwrap();

        store.release_completed_dimension_data();
        assert!(store.strings.is_empty());
        assert!(store.string_ids.is_empty());
        assert!(store.chain_ids.is_empty());
        assert!(store.contract_index.is_empty());
        assert!(store.nfts[0].name_id.is_none());
        assert!(store.nfts[0].token_uri_id.is_none());

        store.release_metadata();
        assert!(store.nft_index.is_empty());
        assert!(store.contracts[0].metadata_by_token.is_empty());
        assert!(store.contracts[0].address.is_empty());
        assert!(store.nfts[0].token_id.is_empty());
    }

    #[test]
    fn duplicate_nft_conflicting_uri_is_rejected() {
        let mut store = EntityStore::default();
        store.try_ingest_row(row("1", "uri://one", "")).unwrap();
        let error = store
            .try_ingest_row(row("1", "uri://different", ""))
            .unwrap_err();
        assert!(error.to_string().contains("snapshot conflict"));
    }

    #[test]
    fn per_nft_names_survive_shard_merge() {
        let mut higher = row("1", "", "");
        higher.name_norm = "pokemon trading card game - scar".to_owned();
        let mut lower = row("2", "", "");
        lower.name_norm = "1999 # charmander cgc 10 pristin".to_owned();

        let mut left = EntityStore::default();
        left.try_ingest_row(higher).unwrap();
        let mut right = EntityStore::default();
        right.try_ingest_row(lower).unwrap();
        left.merge_shard(right).unwrap();

        assert_eq!(
            left.string(left.nfts[0].name_id.unwrap()),
            "pokemon trading card game - scar"
        );
        assert_eq!(
            left.string(left.nfts[1].name_id.unwrap()),
            "1999 # charmander cgc 10 pristin"
        );
    }

    #[test]
    fn direct_shard_merge_preserves_unique_rows_and_fills_missing_uri() {
        let mut left = EntityStore::default();
        left.try_ingest_row(row("1", "", r#"{"name":"one"}"#))
            .unwrap();
        let mut right = EntityStore::default();
        right
            .try_ingest_row(row("1", "uri://one", r#"{"name":"one"}"#))
            .unwrap();
        right
            .try_ingest_row(row("2", "uri://two", r#"{"name":"two"}"#))
            .unwrap();

        left.merge_shard(right).unwrap();
        left.rebuild_uri_postings();

        assert_eq!(left.rows_loaded, 2);
        assert_eq!(left.contracts.len(), 1);
        assert_eq!(left.contracts[0].nft_count, 2);
        assert_eq!(left.nfts.len(), 2);
        assert_eq!(left.string(left.nfts[0].token_uri_id.unwrap()), "uri://one");
        assert_eq!(left.token_uri_postings.len(), 2);
    }

    #[test]
    fn metadata_anchors_are_bounded_and_numeric() {
        let evm_chains = ["ethereum".to_owned()].into_iter().collect();
        let mut store = EntityStore::with_options(2, &evm_chains);
        for token in ["10", "2", "1"] {
            store
                .try_ingest_row(row(token, "", &format!(r#"{{"name":"{token}"}}"#)))
                .unwrap();
        }
        let tokens = store.contracts[0]
            .metadata_by_token
            .iter()
            .map(|record| record.token_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(tokens, ["1", "2"]);
    }

    #[test]
    fn omitted_metadata_anchor_limit_keeps_every_valid_nft() {
        let evm_chains = ["ethereum".to_owned()].into_iter().collect();
        let mut store = EntityStore::with_options(None, &evm_chains);
        for token in 0..32 {
            store
                .try_ingest_row(row(
                    &token.to_string(),
                    "",
                    &format!(r#"{{"name":"{token}"}}"#),
                ))
                .unwrap();
        }

        assert_eq!(store.contracts[0].metadata_by_token.len(), 32);
    }

    #[test]
    fn deferred_unbounded_metadata_is_sorted_and_deduplicated_once() {
        let evm_chains = ["ethereum".to_owned()].into_iter().collect();
        let mut store = EntityStore::with_options(None, &evm_chains);
        store.defer_unbounded_metadata_sort();
        for token in ["10", "2", "1", "2"] {
            store
                .try_ingest_row(row(token, "", &format!(r#"{{"name":"{token}"}}"#)))
                .unwrap();
        }

        store.finalize_metadata_anchors();

        let tokens = store.contracts[0]
            .metadata_by_token
            .iter()
            .map(|record| record.token_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(tokens, ["1", "2", "10"]);
    }

    #[test]
    fn evm_token_comparison_avoids_precision_and_leading_zero_changes() {
        assert_eq!(compare_token_ids("0002", "2", true), Ordering::Equal);
        assert_eq!(compare_token_ids("9", "10", true), Ordering::Less);
        assert_eq!(
            compare_token_ids(
                "999999999999999999999999",
                "1000000000000000000000000",
                true
            ),
            Ordering::Less
        );
        assert_eq!(
            compare_token_ids("not-a-number", "10", true),
            Ordering::Greater
        );
    }

    #[test]
    fn duplicate_json_keys_do_not_consume_anchor_budget() {
        let evm_chains = ["ethereum".to_owned()].into_iter().collect();
        let mut store = EntityStore::with_options(1, &evm_chains);
        store
            .try_ingest_row(row("1", "", r#"{"name":"a","name":"b"}"#))
            .unwrap();
        store
            .try_ingest_row(row("2", "", r#"{"name":"valid"}"#))
            .unwrap();
        assert_eq!(store.contracts[0].metadata_by_token.len(), 1);
        assert_eq!(store.contracts[0].metadata_by_token[0].token_id, "2");
    }

    #[test]
    fn empty_metadata_object_does_not_consume_anchor_budget() {
        let evm_chains = ["ethereum".to_owned()].into_iter().collect();
        let mut store = EntityStore::with_options(1, &evm_chains);
        store.try_ingest_row(row("1", "", " { } ")).unwrap();
        store
            .try_ingest_row(row("2", "", r#"{"name":"valid"}"#))
            .unwrap();
        assert_eq!(store.contracts[0].metadata_by_token.len(), 1);
        assert_eq!(store.contracts[0].metadata_by_token[0].token_id, "2");
    }

    #[test]
    fn retaining_unknown_chain_clears_store() {
        let mut store = EntityStore::default();
        store.try_ingest_row(row("1", "", "")).unwrap();
        store.rebuild_uri_postings();
        store.retain_chains(&["missing".to_owned()].into_iter().collect());
        assert!(store.contracts.is_empty());
        assert!(store.nfts.is_empty());
    }
}
