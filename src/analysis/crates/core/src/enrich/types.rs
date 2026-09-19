//! Evidence bundle types for candidate enrichment.

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::entity::ContractId;

/// Distinct quality states for provider fetches.
#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStatus {
    #[default]
    NotRequested,
    Complete,
    Empty,
    Truncated,
    Failed,
}

/// Per-field quality for one candidate evidence bundle.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EvidenceQuality {
    pub transfers: EvidenceStatus,
    pub sales: EvidenceStatus,
    pub holders: EvidenceStatus,
    pub prices: EvidenceStatus,
    pub assets: EvidenceStatus,
    pub histories: EvidenceStatus,
    pub gas: EvidenceStatus,
    pub value_flows: EvidenceStatus,
    /// The resident identity was authoritatively resolved by the provider as
    /// fungible rather than NFT. Such candidates stay auditable in detail and
    /// cache files, but are outside the NFT analysis population.
    pub excluded_non_nft: bool,
    pub identity_exclusion_reason: Option<String>,
    pub failures: Vec<String>,
    /// Stable machine-readable reasons for Truncated evidence families.
    pub truncation_reasons: Vec<String>,
}

/// Provenance row for one provider request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceObservation {
    pub source: String,
    pub request_key: String,
    pub observed_at: i64,
    pub status: EvidenceStatus,
}

/// Classification of a native / value-flow edge around operators.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueFlowKind {
    Funding,
    Withdrawal,
    Cashout,
    RevenueBackflow,
}

/// Native value movement related to candidate controllers / operators.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValueFlowEdge {
    pub tx_hash: String,
    /// Provider-stable event identity within the transaction (instruction,
    /// trace, or transfer id). Required to distinguish multiple real transfers
    /// sharing the same transaction and endpoints.
    #[serde(default)]
    pub event_id: Option<String>,
    pub from: String,
    pub to: String,
    pub kind: ValueFlowKind,
    pub native_amount: Option<f64>,
    pub usd_amount: Option<f64>,
    pub timestamp: Option<i64>,
    /// Transaction fee paid for this money-flow transaction, when decoded.
    #[serde(default)]
    pub gas_native: Option<f64>,
    /// Fee payer used to decide whether the fee is an operator cost.
    #[serde(default)]
    pub fee_payer: Option<String>,
}

/// Normalized NFT transfer / mint event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferEvent {
    pub tx_hash: String,
    pub token_id: String,
    pub from: String,
    pub to: String,
    pub timestamp: Option<i64>,
    pub block_number: Option<u64>,
    pub is_mint: bool,
    pub gas_native: Option<f64>,
    /// Address that paid the transaction fee when known (EVM `from` / Solana fee payer).
    #[serde(default)]
    pub fee_payer: Option<String>,
    /// Native payment attached to a paid mint (same tx), when known.
    #[serde(default)]
    pub mint_payment_native: Option<f64>,
    /// USD conversion of [`mint_payment_native`] via run-time spot prices.
    #[serde(default)]
    pub mint_payment_usd: Option<f64>,
    /// Payment recipient for the paid mint, when the same transaction exposes it.
    #[serde(default)]
    pub mint_payment_receiver: Option<String>,
}

/// Normalized NFT sale / market activity event.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SaleEvent {
    pub tx_hash: String,
    pub token_id: String,
    pub seller: String,
    pub buyer: String,
    pub timestamp: Option<i64>,
    pub block_number: Option<u64>,
    pub marketplace: Option<String>,
    pub native_amount: Option<f64>,
    pub usd_amount: Option<f64>,
    pub currency_symbol: Option<String>,
    /// Payment-token contract/mint when the provider exposes it.
    ///
    /// `None` is valid for a chain-native payment. Non-native symbols without
    /// this identity are priced only when the symbol quote is unambiguous.
    #[serde(default)]
    pub currency_address: Option<String>,
    /// Exact payment amount in the payment token's smallest unit when supplied
    /// by the provider. Used for price-sensitive ERC-2981 `royaltyInfo` calls.
    #[serde(default)]
    pub sale_price_raw: Option<String>,
    /// Seller net proceeds when provider fee breakdown is available.
    #[serde(default)]
    pub seller_proceeds_native: Option<f64>,
    #[serde(default)]
    pub seller_proceeds_usd: Option<f64>,
    /// Marketplace/protocol fee split when supplied by the sale provider.
    #[serde(default)]
    pub marketplace_fee_native: Option<f64>,
    #[serde(default)]
    pub marketplace_fee_usd: Option<f64>,
    #[serde(default)]
    pub marketplace_fee_currency_symbol: Option<String>,
    #[serde(default)]
    pub marketplace_fee_currency_address: Option<String>,
    /// Creator royalty split and recipient when provable.
    #[serde(default)]
    pub royalty_fee_native: Option<f64>,
    #[serde(default)]
    pub royalty_fee_usd: Option<f64>,
    #[serde(default)]
    pub royalty_fee_currency_symbol: Option<String>,
    #[serde(default)]
    pub royalty_fee_currency_address: Option<String>,
    #[serde(default)]
    pub royalty_recipient: Option<String>,
    /// Receipt fee for the sale transaction.
    #[serde(default)]
    pub gas_native: Option<f64>,
    #[serde(default)]
    pub fee_payer: Option<String>,
}

/// Contract-creation transaction evidence used for Setup gas and lifecycle timing.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeploymentEvent {
    pub tx_hash: String,
    pub timestamp: Option<i64>,
    pub gas_native: Option<f64>,
    pub fee_payer: Option<String>,
}

/// Holder balance for one token (or owner for Solana mint).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HolderRecord {
    pub token_id: String,
    pub owner: String,
    pub balance: Option<i64>,
}

/// Alchemy Prices UTC-day bucket.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceBucket {
    pub chain: String,
    pub day_utc: i64,
    pub symbol: String,
    #[serde(default)]
    pub token_address: Option<String>,
    pub usd_per_native: f64,
}

/// Optional official-relation signals used by deep analysis `legit` marking.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LegitSignals {
    /// The seed collection permits unrestricted reuse (CC0 / public domain).
    pub seed_open_license: bool,
    pub verified_migration: bool,
    pub authorized_reissue: bool,
    pub official_controller_continuity: bool,
    pub official_collection_relation: bool,
    /// Seed↔candidate on-chain NFT interaction (holds seed NFT / transfer counterparty).
    pub seed_nft_interaction: bool,
    pub evidence_keys: Vec<String>,
    /// Whether verification was attempted to completion (false → incomplete probe).
    pub verification_complete: bool,
}

impl LegitSignals {
    pub fn is_legit_duplicate(&self) -> bool {
        self.seed_open_license
            || self.verified_migration
            || self.authorized_reissue
            || self.official_controller_continuity
            || self.official_collection_relation
            || self.seed_nft_interaction
    }

    /// Merge another relation's signals (OR flags, union evidence keys).
    pub fn merge_or(&mut self, other: &LegitSignals) {
        self.seed_open_license |= other.seed_open_license;
        self.verified_migration |= other.verified_migration;
        self.authorized_reissue |= other.authorized_reissue;
        self.official_controller_continuity |= other.official_controller_continuity;
        self.official_collection_relation |= other.official_collection_relation;
        self.seed_nft_interaction |= other.seed_nft_interaction;
        self.verification_complete |= other.verification_complete;
        for key in &other.evidence_keys {
            if !self.evidence_keys.iter().any(|k| k == key) {
                self.evidence_keys.push(key.clone());
            }
        }
    }
}

/// Finalize top-level `LegitSignals` after relation probes (or when none ran).
pub fn finalize_legit_signals(bundle: &mut EvidenceBundle) {
    if !bundle.relation_legit.is_empty() {
        let mut merged = LegitSignals::default();
        let mut any_complete = false;
        for signals in bundle.relation_legit.values() {
            merged.merge_or(signals);
            any_complete |= signals.verification_complete;
        }
        bundle.legit = merged;
        if bundle.legit.is_legit_duplicate() || any_complete {
            bundle.legit.verification_complete = true;
        }
        return;
    }
    if bundle.legit.is_legit_duplicate() {
        bundle.legit.verification_complete = true;
        return;
    }
    bundle.legit.verification_complete = false;
}

/// Per-candidate enrichment product consumed by deep analysis (Task 12).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct EvidenceBundle {
    pub contract_id: ContractId,
    pub chain: String,
    pub address: String,
    pub transfers: Vec<TransferEvent>,
    pub sales: Vec<SaleEvent>,
    pub holders: Vec<HolderRecord>,
    /// Current NFT/token-id population for the whole candidate contract.
    /// Present only when the provider completed the same collection/holder
    /// universe used by holder-based stuck-NFT attribution.
    pub collection_nft_count: Option<u64>,
    pub collection_nft_count_complete: bool,
    pub prices: Vec<PriceBucket>,
    /// Native funding / withdrawal / cashout edges (populated by enrich E2–E4).
    pub value_flows: Vec<ValueFlowEdge>,
    pub quality: EvidenceQuality,
    pub provenance: Vec<EvidenceObservation>,
    /// Known controllers / collection authorities (operator seeds for attribution).
    pub controllers: Vec<String>,
    pub deployment: Option<DeploymentEvent>,
    pub deployment_timestamp: Option<i64>,
    pub legit: LegitSignals,
    /// Per-seed relation signals keyed by `"chain:address"`.
    #[serde(default)]
    pub relation_legit: std::collections::BTreeMap<String, LegitSignals>,
}

impl Default for EvidenceBundle {
    fn default() -> Self {
        Self::empty(0, "", "")
    }
}

impl EvidenceBundle {
    pub fn empty(
        contract_id: ContractId,
        chain: impl Into<String>,
        address: impl Into<String>,
    ) -> Self {
        Self {
            contract_id,
            chain: chain.into(),
            address: address.into(),
            transfers: Vec::new(),
            sales: Vec::new(),
            holders: Vec::new(),
            collection_nft_count: None,
            collection_nft_count_complete: false,
            prices: Vec::new(),
            value_flows: Vec::new(),
            quality: EvidenceQuality::default(),
            provenance: Vec::new(),
            controllers: Vec::new(),
            deployment: None,
            deployment_timestamp: None,
            legit: LegitSignals::default(),
            relation_legit: std::collections::BTreeMap::new(),
        }
    }

    /// Drop fields kept only for disk provenance / debug after cache write.
    ///
    /// Safe before `analyze_candidate` (detectors use transfers/sales/holders/
    /// value_flows/controllers, not provenance).
    pub fn strip_for_analysis_memory(&mut self) {
        self.provenance.clear();
        self.provenance.shrink_to_fit();
    }

    /// Clone contract-wide evidence for a reporting scope and retain only that
    /// scope's seed-relation classifications.
    ///
    /// Duplicate detection is contract-level: once a candidate contract is hit
    /// in a scope, every NFT in that contract has equal standing in deep
    /// analysis, so only seed-relation keys are needed here.
    pub fn filtered_for_analysis(&self, seed_keys: &ahash::AHashSet<String>) -> Self {
        let mut scoped = self.clone();
        scoped
            .relation_legit
            .retain(|seed_key, _| seed_keys.contains(seed_key));
        scoped.legit = LegitSignals::default();
        finalize_legit_signals(&mut scoped);
        scoped
    }
}

/// Chain-aware address normalization.
///
/// EVM addresses are case-insensitive; Solana base58 public keys are not.
pub fn normalize_chain_address(chain: &str, address: &str) -> String {
    let address = address.trim();
    if chain.trim().eq_ignore_ascii_case("solana") {
        address.to_owned()
    } else {
        address.to_ascii_lowercase()
    }
}

/// Normalize transaction identifiers without corrupting case-sensitive Solana
/// signatures. EVM hashes are hexadecimal and compare case-insensitively.
pub fn normalize_chain_transaction(chain: &str, tx_hash: &str) -> String {
    if chain.trim().eq_ignore_ascii_case("solana") {
        tx_hash.trim().to_owned()
    } else {
        tx_hash.trim().to_ascii_lowercase()
    }
}

pub fn chain_addresses_equal(chain: &str, left: &str, right: &str) -> bool {
    normalize_chain_address(chain, left) == normalize_chain_address(chain, right)
}

/// API keys for enrichment providers. Empty / missing → `not_requested`.
#[derive(Clone, Debug, Default)]
pub struct ApiKeys {
    pub alchemy: Option<String>,
    pub etherscan: Option<String>,
    pub helius: Option<String>,
    pub opensea: Option<String>,
}

impl ApiKeys {
    pub fn alchemy(&self) -> Option<&str> {
        nonempty(self.alchemy.as_deref())
    }

    pub fn etherscan(&self) -> Option<&str> {
        nonempty(self.etherscan.as_deref())
    }

    pub fn helius(&self) -> Option<&str> {
        nonempty(self.helius.as_deref())
    }

    pub fn opensea(&self) -> Option<&str> {
        nonempty(self.opensea.as_deref())
    }
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|s| !s.is_empty())
}

/// Injected provider base URLs (tests override with httpmock).
#[derive(Clone, Debug)]
pub struct ProviderEndpoints {
    pub alchemy_rpc_template: String,
    pub alchemy_nft_template: String,
    pub alchemy_prices: String,
    pub etherscan: String,
    pub helius: String,
    pub opensea: String,
    pub alchemy_networks: AHashMap<String, String>,
}

impl Default for ProviderEndpoints {
    fn default() -> Self {
        let mut alchemy_networks = AHashMap::new();
        alchemy_networks.insert("ethereum".into(), "eth-mainnet".into());
        alchemy_networks.insert("base".into(), "base-mainnet".into());
        alchemy_networks.insert("polygon".into(), "polygon-mainnet".into());
        Self {
            // `{network}` + `{key}` placeholders
            alchemy_rpc_template: "https://{network}.g.alchemy.com/v2/{key}".into(),
            alchemy_nft_template: "https://{network}.g.alchemy.com/nft/v3/{key}/{method}".into(),
            alchemy_prices: "https://api.g.alchemy.com/prices/v1".into(),
            etherscan: "https://api.etherscan.io/v2/api".into(),
            helius: "https://mainnet.helius-rpc.com/".into(),
            opensea: "https://api.opensea.io".into(),
            alchemy_networks,
        }
    }
}

impl ProviderEndpoints {
    pub fn alchemy_rpc(&self, chain: &str, api_key: &str) -> Option<String> {
        let network = self.alchemy_networks.get(chain)?;
        Some(
            self.alchemy_rpc_template
                .replace("{network}", network)
                .replace("{key}", api_key),
        )
    }

    pub fn alchemy_nft(&self, chain: &str, api_key: &str, method: &str) -> Option<String> {
        let network = self.alchemy_networks.get(chain)?;
        Some(
            self.alchemy_nft_template
                .replace("{network}", network)
                .replace("{key}", api_key)
                .replace("{method}", method),
        )
    }
}

/// Bounded concurrency / pagination / retry knobs.
#[derive(Clone, Debug)]
pub struct HttpLimits {
    pub concurrency: usize,
    pub retries: usize,
    pub max_transfer_pages: usize,
    pub max_holder_pages: usize,
    pub max_sale_pages: usize,
    pub max_solana_assets: usize,
    pub max_history_assets: usize,
    pub max_signatures_per_asset: usize,
    /// Durable raw JSON cache for every fully successful provider request.
    /// API secrets are excluded from cache identities.
    pub success_response_cache_dir: Option<std::path::PathBuf>,
    /// Ignore entries older than this Unix timestamp while still storing and
    /// reusing successful responses produced during the current run.
    pub success_response_cache_min_unix: Option<i64>,
    /// Candidate-scoped controller/collection identity checkpoint. Unlike the
    /// raw response cache, this remains reusable when batch boundaries change.
    pub candidate_identity_cache_path: Option<std::path::PathBuf>,
    pub endpoints: ProviderEndpoints,
}

impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            // Prefer modest per-provider concurrency: Alchemy timeouts explode
            // under 32×N nested candidate tasks on large holder/sales pages.
            // OpenSea/Helius/Etherscan use independent pools of the same size.
            concurrency: 8,
            retries: 3,
            max_transfer_pages: 5,
            max_holder_pages: 5,
            max_sale_pages: 5,
            max_solana_assets: 200,
            max_history_assets: 20,
            max_signatures_per_asset: 50,
            success_response_cache_dir: None,
            success_response_cache_min_unix: None,
            candidate_identity_cache_path: None,
            endpoints: ProviderEndpoints::default(),
        }
    }
}

pub(crate) fn status_from_count(count: usize, truncated: bool) -> EvidenceStatus {
    if truncated {
        EvidenceStatus::Truncated
    } else if count == 0 {
        EvidenceStatus::Empty
    } else {
        EvidenceStatus::Complete
    }
}

pub(crate) fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

pub(crate) fn day_bucket(timestamp: i64) -> i64 {
    timestamp.div_euclid(86_400) * 86_400
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalize_legit_keeps_default_incomplete_without_signals() {
        let mut bundle = EvidenceBundle::empty(1, "ethereum", "0xabc");
        finalize_legit_signals(&mut bundle);
        assert!(!bundle.legit.is_legit_duplicate());
        assert!(!bundle.legit.verification_complete);
    }

    #[test]
    fn finalize_legit_marks_complete_when_positive_signal_present() {
        let mut bundle = EvidenceBundle::empty(1, "ethereum", "0xabc");
        bundle.legit.official_collection_relation = true;
        bundle
            .legit
            .evidence_keys
            .push("collection:official".into());
        finalize_legit_signals(&mut bundle);
        assert!(bundle.legit.is_legit_duplicate());
        assert!(bundle.legit.verification_complete);
    }

    #[test]
    fn filtered_analysis_keeps_contract_wide_nft_evidence_and_scope_relations() {
        let mut bundle = EvidenceBundle::empty(1, "ethereum", "0xcand");
        let transfer = |tx: &str, token: &str| TransferEvent {
            tx_hash: tx.into(),
            token_id: token.into(),
            from: "0xa".into(),
            to: "0xb".into(),
            timestamp: Some(1),
            block_number: Some(1),
            is_mint: false,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        };
        bundle.transfers = vec![transfer("hit-tx", "1"), transfer("other-tx", "2")];
        bundle.sales = ["1", "2"]
            .into_iter()
            .map(|token_id| SaleEvent {
                tx_hash: format!("sale-{token_id}"),
                token_id: token_id.into(),
                seller: "0xseller".into(),
                buyer: "0xbuyer".into(),
                native_amount: Some(1.0),
                ..SaleEvent::default()
            })
            .collect();
        bundle.holders = vec![
            HolderRecord {
                token_id: "1".into(),
                owner: "0xb".into(),
                balance: Some(1),
            },
            HolderRecord {
                token_id: "2".into(),
                owner: "0xc".into(),
                balance: Some(1),
            },
        ];
        bundle.value_flows = vec![
            ValueFlowEdge {
                tx_hash: "hit-tx".into(),
                event_id: None,
                from: "0xa".into(),
                to: "0xb".into(),
                kind: ValueFlowKind::RevenueBackflow,
                native_amount: Some(1.0),
                usd_amount: Some(2_000.0),
                timestamp: Some(1),
                gas_native: None,
                fee_payer: None,
            },
            ValueFlowEdge {
                tx_hash: "unrelated".into(),
                event_id: None,
                from: "0xa".into(),
                to: "0xc".into(),
                kind: ValueFlowKind::Withdrawal,
                native_amount: Some(9.0),
                usd_amount: Some(18_000.0),
                timestamp: Some(2),
                gas_native: None,
                fee_payer: None,
            },
        ];
        bundle.relation_legit.insert(
            "ethereum:0xseed".into(),
            LegitSignals {
                verified_migration: true,
                verification_complete: true,
                ..Default::default()
            },
        );
        bundle
            .relation_legit
            .insert("base:0xother".into(), LegitSignals::default());

        let filtered =
            bundle.filtered_for_analysis(&["ethereum:0xseed".to_owned()].into_iter().collect());
        // A contract-level hit includes every NFT event/holder in the candidate,
        // even when only token 1 produced the original duplicate edge.
        assert_eq!(filtered.transfers.len(), 2);
        assert_eq!(filtered.sales.len(), 2);
        assert_eq!(filtered.holders.len(), 2);
        // Candidate-wide money flows are copied into every related seed scope;
        // the run summary de-duplicates them by on-chain event.
        assert_eq!(filtered.value_flows.len(), 2);
        assert_eq!(filtered.relation_legit.len(), 1);
        assert!(filtered.legit.is_legit_duplicate());
    }
}
