//! ID aliases and core entity shapes.

use std::cmp::Ordering;

use num_bigint::BigUint;

pub type ChainId = u16;
pub type ContractId = u32;
pub type NftId = u32;
pub type StringId = u32;

/// Stable source ordering from multi-file Parquet input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceOrder {
    pub file_ordinal: u32,
    pub file_row_number: u64,
}

/// Per-chain denominator totals over the full snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChainTotals {
    pub contracts: u64,
    pub nfts: u64,
}

/// Valid Metadata anchor retained on a contract (descending token-id order).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRecord {
    pub token_id: String,
    pub canonical_json: String,
    pub source_order: SourceOrder,
}

/// Contract / collection identity plus lightweight stats.
#[derive(Clone, Debug)]
pub struct Contract {
    pub id: ContractId,
    pub chain_id: ChainId,
    pub address: String,
    pub nft_count: u64,
    /// EVM representative Name (filled by name finalize); Solana leaves this unset.
    pub name_id: Option<StringId>,
    /// Valid metadata records by token id descending.
    pub metadata_by_token: Vec<MetadataRecord>,
}

/// Per-NFT identity and interned dimension values.
#[derive(Clone, Debug)]
pub struct Nft {
    pub contract_id: ContractId,
    pub token_id_id: StringId,
    pub name_id: Option<StringId>,
    pub token_uri_id: Option<StringId>,
    pub image_uri_id: Option<StringId>,
}

/// Canonical EVM token-id string for shared-key alignment (bigint, no leading zeros).
///
/// Decimal digit strings normalize via `BigUint` (`"010"` / `"10"` → `"10"`).
/// Non-decimal tokens are returned unchanged (same fallback spirit as strip-zeros).
pub fn normalized_evm_token(token: &str) -> String {
    let trimmed = token.trim();
    match BigUint::parse_bytes(trimmed.as_bytes(), 10) {
        Some(n) => n.to_string(),
        None => token.to_owned(),
    }
}

/// Borrowed canonical decimal token id used by hot-path equality indexes.
/// Non-decimal values retain their original spelling.
pub fn normalized_evm_token_slice(token: &str) -> &str {
    let trimmed = token.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        return token;
    }
    let without_zeroes = trimmed.trim_start_matches('0');
    if without_zeroes.is_empty() {
        &trimmed[trimmed.len() - 1..]
    } else {
        without_zeroes
    }
}

/// Compare token ids for ascending order (EVM bigint; Solana lex).
pub fn compare_token_ids(left: &str, right: &str, is_evm: bool) -> Ordering {
    if is_evm {
        match (
            BigUint::parse_bytes(left.trim().as_bytes(), 10),
            BigUint::parse_bytes(right.trim().as_bytes(), 10),
        ) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => left.cmp(right),
        }
    } else {
        left.cmp(right)
    }
}

/// Descending token-id order used for metadata anchors.
pub fn compare_token_ids_desc(left: &str, right: &str, is_evm: bool) -> Ordering {
    compare_token_ids(right, left, is_evm)
}
