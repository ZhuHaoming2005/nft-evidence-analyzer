//! Full `run` report builders: candidate JSON, seed analysis sections, summary aggregates.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::analysis::{
    AddressRole, BehaviorKind, CandidateAnalysis, EconomicContributionKind, GasStage,
};
use crate::dedup::candidates::CandidateRegistry;
use crate::enrich::{normalize_chain_address, normalize_chain_transaction};
use crate::entity::{ContractId, ResidentStore};
use crate::error::AnalysisError;

use super::json::{
    DedupRunParams, SeedDedupReport, SeedRecord, rebuild_seed_duplicate_scale, seed_dir_name,
    write_json,
};
use super::layout::{
    DETAIL_CANDIDATES_REL, SCOPE_CHAIN_MATRIX, SCOPE_INTRA_CHAIN, SCOPE_LABEL_ALL_CHAINS,
    SCOPE_LABEL_CROSS_CHAIN, ensure_output_layout, intermediate_path, seed_report_dir,
};
use super::manifest::{
    FailureRecord, RunManifest, RunManifestSeeds, count_failed_seeds, write_failures_jsonl,
};
use super::markdown;

/// Candidate analyses rebuilt from the same cached evidence for every reporting
/// scope. Matrix entries are keyed by `(primary, secondary)`.
#[derive(Clone, Debug, Default)]
pub struct ScopeAnalysisSets {
    pub intra_chain: Vec<Arc<CandidateAnalysis>>,
    pub intra_chain_by_chain: BTreeMap<String, Vec<Arc<CandidateAnalysis>>>,
    pub cross_chain: Vec<Arc<CandidateAnalysis>>,
    pub cross_chain_by_primary: BTreeMap<String, Vec<Arc<CandidateAnalysis>>>,
    pub chain_matrix: BTreeMap<(String, String), Vec<Arc<CandidateAnalysis>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunSummaryScope<'a> {
    All,
    Intra,
    IntraChain {
        chain: &'a str,
    },
    Cross,
    CrossPrimary {
        primary_chain: &'a str,
    },
    Matrix {
        primary_chain: &'a str,
        secondary_chain: &'a str,
    },
}

impl RunSummaryScope<'_> {
    fn seed_matches(self, seed_chain: &str) -> bool {
        match self {
            Self::IntraChain { chain } => seed_chain.eq_ignore_ascii_case(chain),
            Self::CrossPrimary { primary_chain } | Self::Matrix { primary_chain, .. } => {
                seed_chain.eq_ignore_ascii_case(primary_chain)
            }
            _ => true,
        }
    }

    fn relation_matches(self, seed_chain: &str, candidate_chain: &str) -> bool {
        let same = seed_chain.eq_ignore_ascii_case(candidate_chain);
        match self {
            Self::All => true,
            Self::Intra => same,
            Self::IntraChain { chain } => {
                same && seed_chain.eq_ignore_ascii_case(chain)
                    && candidate_chain.eq_ignore_ascii_case(chain)
            }
            Self::Cross => !same,
            Self::CrossPrimary { primary_chain } => {
                !same && seed_chain.eq_ignore_ascii_case(primary_chain)
            }
            Self::Matrix {
                primary_chain,
                secondary_chain,
            } => {
                !same
                    && seed_chain.eq_ignore_ascii_case(primary_chain)
                    && candidate_chain.eq_ignore_ascii_case(secondary_chain)
            }
        }
    }
}

/// Per-seed analysis rollup attached to the seed report after deep analysis.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedAnalysisRollup {
    pub analyzed_candidate_count: u64,
    pub suspected_duplicate_contract_count: u64,
    pub legit_duplicate_contract_count: u64,
    pub infringing_nft_count: u64,
    pub malicious_address_count: u64,
    pub honest_address_count: u64,
    #[serde(default)]
    pub overlapping_role_address_count: u64,
    pub economics_usd: EconomicsUsdRollup,
    pub candidate_refs: Vec<CandidateRef>,
}

/// Cross-chain / multi-candidate economics rollup.
///
/// Monetary fields exposed by reports are USD only, valued with run-time prices.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EconomicsUsdRollup {
    pub operator_output_usd: f64,
    /// Operator output for the same contracts included in the ratio denominator.
    pub ratio_operator_output_usd: f64,
    #[serde(rename = "honest_paid_exposure_usd")]
    pub honest_loss_usd: f64,
    #[serde(rename = "secondary_sale_paid_exposure_usd")]
    pub secondary_sale_loss_usd: f64,
    #[serde(rename = "paid_mint_exposure_usd")]
    pub paid_mint_loss_usd: f64,
    #[serde(rename = "gross_sales_volume_usd")]
    pub gross_revenue_usd: f64,
    pub marketplace_fee_usd: f64,
    pub royalty_fee_usd: f64,
    pub operator_royalty_usd: f64,
    pub setup_gas_usd: f64,
    pub lure_gas_usd: f64,
    pub exit_gas_usd: f64,
    pub total_gas_usd: f64,
    pub funding_usd: f64,
    pub revenue_backflow_usd: f64,
    pub withdrawal_usd: f64,
    pub stuck_nft_count: u64,
    /// Contracts with a defined same-unit `output_input_ratio`.
    pub output_input_ratio_count: u64,
    pub output_input_ratio_ge1_count: u64,
    pub output_input_ratio_lt1_count: u64,
    /// Same ratio fields restricted to candidates whose required evidence is complete.
    pub complete_output_input_ratio_count: u64,
    pub complete_output_input_ratio_ge1_count: u64,
    pub complete_output_input_ratio_lt1_count: u64,
    /// Sum of observed attacker gas input for contracts with a priced USD denominator.
    pub attacker_input_usd: f64,
    /// Attacker gas input for the same contracts included in the ratio numerator.
    pub ratio_attacker_input_usd: f64,
    pub complete_ratio_operator_output_usd: f64,
    pub complete_ratio_attacker_input_usd: f64,
    pub sale_count: u64,
    pub priced_sale_count: u64,
    pub unpriced_sale_count: u64,
    pub amountless_sale_count: u64,
    pub assumed_stablecoin_peg_sale_count: u64,
    pub priced_value_flow_count: u64,
    pub unpriced_value_flow_count: u64,
    pub operator_sale_count: u64,
    pub priced_operator_sale_proceeds_count: u64,
    pub unpriced_operator_sale_proceeds_count: u64,
    pub unknown_operator_sale_proceeds_count: u64,
    pub unknown_royalty_recipient_count: u64,
    pub paid_mint_payment_count: u64,
    pub operator_paid_mint_payment_count: u64,
    pub priced_operator_paid_mint_payment_count: u64,
    pub unpriced_operator_paid_mint_payment_count: u64,
    pub unknown_paid_mint_receiver_count: u64,
    #[serde(rename = "honest_paid_mint_exposure_count")]
    pub honest_paid_mint_loss_count: u64,
    #[serde(rename = "priced_honest_paid_mint_exposure_count")]
    pub priced_honest_paid_mint_loss_count: u64,
    #[serde(rename = "unpriced_honest_paid_mint_exposure_count")]
    pub unpriced_honest_paid_mint_loss_count: u64,
    pub gas_cost_contract_count: u64,
    pub priced_gas_cost_contract_count: u64,
    pub unpriced_gas_cost_contract_count: u64,
}

/// Pointer from a seed report to a streamed candidate artifact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateRef {
    pub chain: String,
    pub address: String,
    pub is_legit_duplicate: bool,
    pub path: String,
}

/// Full per-seed report written by `run` (dedup + analysis).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedFullReport {
    #[serde(flatten)]
    pub dedup: SeedDedupReport,
    /// True when dedup completed for all configured secondary chains (four-scope complete).
    pub scopes_complete: bool,
    /// True when every related candidate finished analysis successfully.
    pub analysis_complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analysis: Option<SeedAnalysisRollup>,
}

/// Whether this seed's chain-matrix covers every non-primary chain in the store.
pub fn scopes_complete_for_seed(store: &ResidentStore, report: &SeedDedupReport) -> bool {
    let primary = report.seed.chain.as_str();
    let expected: AHashSet<&str> = store
        .chains
        .iter()
        .map(String::as_str)
        .filter(|c| *c != primary)
        .collect();
    let present: AHashSet<&str> = report
        .duplicate_scale
        .chain_matrix
        .iter()
        .map(|b| b.secondary_chain.as_str())
        .collect();
    expected == present
}

pub fn candidate_file_name(chain: &str, address: &str) -> String {
    format!("{chain}__{address}.json")
}

/// Relative path for one candidate analysis JSON under `detail/candidates/`.
pub fn candidate_json_rel_path(chain: &str, address: &str) -> String {
    format!(
        "{DETAIL_CANDIDATES_REL}/{}",
        candidate_file_name(chain, address)
    )
}

/// Write one candidate analysis JSON under `output_dir/detail/candidates/`.
pub fn write_candidate_json(
    output_dir: &Path,
    analysis: &CandidateAnalysis,
) -> Result<String, AnalysisError> {
    let rel = candidate_json_rel_path(&analysis.chain, &analysis.address);
    let path = output_dir.join(&rel);
    write_json(&path, analysis)?;
    Ok(rel)
}

/// Serialize candidate analysis to compact JSON bytes (CPU work; safe on Rayon).
pub fn serialize_candidate_json(analysis: &CandidateAnalysis) -> Result<Vec<u8>, AnalysisError> {
    serde_json::to_vec(analysis)
        .map_err(|e| AnalysisError::invalid(format!("serialize candidate analysis: {e}")))
}

/// Write pre-serialized candidate JSON bytes to `output_dir/rel`.
pub fn write_candidate_json_bytes(
    output_dir: &Path,
    rel: &str,
    body: &[u8],
) -> Result<(), AnalysisError> {
    let path = output_dir.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, body)?;
    Ok(())
}

fn economics_usd_from(analysis: &CandidateAnalysis) -> EconomicsUsdRollup {
    let facts = &analysis.economics;
    let mut roll = EconomicsUsdRollup {
        operator_output_usd: facts.operator_output_usd,
        ratio_operator_output_usd: 0.0,
        honest_loss_usd: facts.honest_loss_usd,
        secondary_sale_loss_usd: facts.secondary_sale_loss_usd,
        paid_mint_loss_usd: facts.paid_mint_loss_usd,
        gross_revenue_usd: facts.gross_revenue_usd,
        marketplace_fee_usd: facts.marketplace_fee_usd,
        royalty_fee_usd: facts.royalty_fee_usd,
        operator_royalty_usd: facts.operator_royalty_usd,
        setup_gas_usd: facts.setup_gas_usd,
        lure_gas_usd: facts.lure_gas_usd,
        exit_gas_usd: facts.exit_gas_usd,
        total_gas_usd: facts.total_gas_usd,
        funding_usd: facts.funding_usd,
        revenue_backflow_usd: facts.revenue_backflow_usd,
        withdrawal_usd: facts.withdrawal_usd,
        stuck_nft_count: facts.stuck_nft_count,
        output_input_ratio_count: 0,
        output_input_ratio_ge1_count: 0,
        output_input_ratio_lt1_count: 0,
        complete_output_input_ratio_count: 0,
        complete_output_input_ratio_ge1_count: 0,
        complete_output_input_ratio_lt1_count: 0,
        attacker_input_usd: facts
            .attacker_input_usd
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(0.0),
        ratio_attacker_input_usd: 0.0,
        complete_ratio_operator_output_usd: 0.0,
        complete_ratio_attacker_input_usd: 0.0,
        sale_count: facts.sale_count,
        priced_sale_count: facts.priced_sale_count,
        unpriced_sale_count: facts.unpriced_sale_count,
        amountless_sale_count: facts.amountless_sale_count,
        assumed_stablecoin_peg_sale_count: facts.assumed_stablecoin_peg_sale_count,
        priced_value_flow_count: facts.priced_value_flow_count,
        unpriced_value_flow_count: facts.unpriced_value_flow_count,
        operator_sale_count: facts.operator_sale_count,
        priced_operator_sale_proceeds_count: facts.priced_operator_sale_proceeds_count,
        unpriced_operator_sale_proceeds_count: facts.unpriced_operator_sale_proceeds_count,
        unknown_operator_sale_proceeds_count: facts.unknown_operator_sale_proceeds_count,
        unknown_royalty_recipient_count: facts.unknown_royalty_recipient_count,
        paid_mint_payment_count: facts.paid_mint_payment_count,
        operator_paid_mint_payment_count: facts.operator_paid_mint_payment_count,
        priced_operator_paid_mint_payment_count: facts.priced_operator_paid_mint_payment_count,
        unpriced_operator_paid_mint_payment_count: facts.unpriced_operator_paid_mint_payment_count,
        unknown_paid_mint_receiver_count: facts.unknown_paid_mint_receiver_count,
        honest_paid_mint_loss_count: facts.honest_paid_mint_loss_count,
        priced_honest_paid_mint_loss_count: facts.priced_honest_paid_mint_loss_count,
        unpriced_honest_paid_mint_loss_count: facts.unpriced_honest_paid_mint_loss_count,
        gas_cost_contract_count: u64::from(facts.gas_cost_observed),
        priced_gas_cost_contract_count: u64::from(facts.gas_cost_priced),
        unpriced_gas_cost_contract_count: u64::from(
            facts.gas_cost_observed && !facts.gas_cost_priced,
        ),
    };
    // Only count ge1/lt1 and sum input when the per-contract ratio is USD/USD.
    if facts.output_input_ratio_is_usd {
        roll.ratio_operator_output_usd = facts.operator_output_usd;
        if let Some(ratio) = facts.output_input_ratio {
            roll.output_input_ratio_count = 1;
            if ratio >= 1.0 {
                roll.output_input_ratio_ge1_count = 1;
            } else {
                roll.output_input_ratio_lt1_count = 1;
            }
        }
        if let Some(input_usd) = facts
            .attacker_input_usd
            .filter(|v| v.is_finite() && *v > 0.0)
        {
            roll.ratio_attacker_input_usd = input_usd;
        }
    }
    if analysis.has_complete_evidence() && facts.output_input_ratio_is_usd {
        roll.complete_ratio_operator_output_usd = facts.operator_output_usd;
        if let Some(ratio) = facts.output_input_ratio {
            roll.complete_output_input_ratio_count = 1;
            if ratio >= 1.0 {
                roll.complete_output_input_ratio_ge1_count = 1;
            } else {
                roll.complete_output_input_ratio_lt1_count = 1;
            }
        }
        if let Some(input_usd) = facts
            .attacker_input_usd
            .filter(|value| value.is_finite() && *value > 0.0)
        {
            roll.complete_ratio_attacker_input_usd = input_usd;
        }
    }
    roll
}

fn merge_usd(dst: &mut EconomicsUsdRollup, src: &EconomicsUsdRollup) {
    dst.operator_output_usd += src.operator_output_usd;
    dst.ratio_operator_output_usd += src.ratio_operator_output_usd;
    dst.honest_loss_usd += src.honest_loss_usd;
    dst.secondary_sale_loss_usd += src.secondary_sale_loss_usd;
    dst.paid_mint_loss_usd += src.paid_mint_loss_usd;
    dst.gross_revenue_usd += src.gross_revenue_usd;
    dst.marketplace_fee_usd += src.marketplace_fee_usd;
    dst.royalty_fee_usd += src.royalty_fee_usd;
    dst.operator_royalty_usd += src.operator_royalty_usd;
    dst.setup_gas_usd += src.setup_gas_usd;
    dst.lure_gas_usd += src.lure_gas_usd;
    dst.exit_gas_usd += src.exit_gas_usd;
    dst.total_gas_usd += src.total_gas_usd;
    dst.funding_usd += src.funding_usd;
    dst.revenue_backflow_usd += src.revenue_backflow_usd;
    dst.withdrawal_usd += src.withdrawal_usd;
    dst.stuck_nft_count += src.stuck_nft_count;
    dst.output_input_ratio_count += src.output_input_ratio_count;
    dst.output_input_ratio_ge1_count += src.output_input_ratio_ge1_count;
    dst.output_input_ratio_lt1_count += src.output_input_ratio_lt1_count;
    dst.complete_output_input_ratio_count += src.complete_output_input_ratio_count;
    dst.complete_output_input_ratio_ge1_count += src.complete_output_input_ratio_ge1_count;
    dst.complete_output_input_ratio_lt1_count += src.complete_output_input_ratio_lt1_count;
    dst.attacker_input_usd += src.attacker_input_usd;
    dst.ratio_attacker_input_usd += src.ratio_attacker_input_usd;
    dst.complete_ratio_operator_output_usd += src.complete_ratio_operator_output_usd;
    dst.complete_ratio_attacker_input_usd += src.complete_ratio_attacker_input_usd;
    dst.sale_count += src.sale_count;
    dst.priced_sale_count += src.priced_sale_count;
    dst.unpriced_sale_count += src.unpriced_sale_count;
    dst.amountless_sale_count += src.amountless_sale_count;
    dst.assumed_stablecoin_peg_sale_count += src.assumed_stablecoin_peg_sale_count;
    dst.priced_value_flow_count += src.priced_value_flow_count;
    dst.unpriced_value_flow_count += src.unpriced_value_flow_count;
    dst.operator_sale_count += src.operator_sale_count;
    dst.priced_operator_sale_proceeds_count += src.priced_operator_sale_proceeds_count;
    dst.unpriced_operator_sale_proceeds_count += src.unpriced_operator_sale_proceeds_count;
    dst.unknown_operator_sale_proceeds_count += src.unknown_operator_sale_proceeds_count;
    dst.unknown_royalty_recipient_count += src.unknown_royalty_recipient_count;
    dst.paid_mint_payment_count += src.paid_mint_payment_count;
    dst.operator_paid_mint_payment_count += src.operator_paid_mint_payment_count;
    dst.priced_operator_paid_mint_payment_count += src.priced_operator_paid_mint_payment_count;
    dst.unpriced_operator_paid_mint_payment_count += src.unpriced_operator_paid_mint_payment_count;
    dst.unknown_paid_mint_receiver_count += src.unknown_paid_mint_receiver_count;
    dst.honest_paid_mint_loss_count += src.honest_paid_mint_loss_count;
    dst.priced_honest_paid_mint_loss_count += src.priced_honest_paid_mint_loss_count;
    dst.unpriced_honest_paid_mint_loss_count += src.unpriced_honest_paid_mint_loss_count;
    dst.gas_cost_contract_count += src.gas_cost_contract_count;
    dst.priced_gas_cost_contract_count += src.priced_gas_cost_contract_count;
    dst.unpriced_gas_cost_contract_count += src.unpriced_gas_cost_contract_count;
}

type EconomicEventKey = (
    String,
    String,
    String,
    String,
    String,
    String,
    EconomicContributionKind,
    u64,
);

fn collect_economic_events(
    events: &mut AHashMap<EconomicEventKey, f64>,
    analysis: &CandidateAnalysis,
) {
    for (index, contribution) in analysis.economics.economic_contributions.iter().enumerate() {
        let tx_hash = normalize_chain_transaction(&analysis.chain, &contribution.tx_hash);
        let tx_hash = if tx_hash.is_empty() {
            format!(
                "missing:{}:{}",
                normalize_chain_address(&analysis.chain, &analysis.address),
                index
            )
        } else {
            tx_hash
        };
        let key = (
            analysis.chain.trim().to_ascii_lowercase(),
            normalize_chain_address(&analysis.chain, &analysis.address),
            contribution.token_id.clone(),
            tx_hash,
            normalize_chain_address(&analysis.chain, &contribution.from),
            normalize_chain_address(&analysis.chain, &contribution.to),
            contribution.kind,
            contribution.usd.to_bits(),
        );
        events.entry(key).or_insert(contribution.usd);
    }
}

fn apply_deduped_economic_events(
    economics: &mut EconomicsUsdRollup,
    events: AHashMap<EconomicEventKey, f64>,
) {
    if events.is_empty() {
        return;
    }
    economics.operator_output_usd = 0.0;
    economics.honest_loss_usd = 0.0;
    economics.secondary_sale_loss_usd = 0.0;
    economics.paid_mint_loss_usd = 0.0;
    economics.gross_revenue_usd = 0.0;
    economics.marketplace_fee_usd = 0.0;
    economics.royalty_fee_usd = 0.0;
    economics.operator_royalty_usd = 0.0;
    for ((_, _, _, _, _, _, kind, _), usd) in events {
        match kind {
            EconomicContributionKind::GrossSale => economics.gross_revenue_usd += usd,
            EconomicContributionKind::MarketplaceFee => economics.marketplace_fee_usd += usd,
            EconomicContributionKind::RoyaltyFee => economics.royalty_fee_usd += usd,
            EconomicContributionKind::OperatorSaleProceeds
            | EconomicContributionKind::OperatorRoyalty
            | EconomicContributionKind::OperatorMintPayment => {
                economics.operator_output_usd += usd;
                if kind == EconomicContributionKind::OperatorRoyalty {
                    economics.operator_royalty_usd += usd;
                }
            }
            EconomicContributionKind::HonestSecondaryExposure => {
                economics.secondary_sale_loss_usd += usd;
                economics.honest_loss_usd += usd;
            }
            EconomicContributionKind::HonestMintExposure => {
                economics.paid_mint_loss_usd += usd;
                economics.honest_loss_usd += usd;
            }
        }
    }
}

fn role_is_malicious(role: AddressRole) -> bool {
    matches!(role, AddressRole::SuspectedOperator)
}

fn role_is_honest(role: AddressRole) -> bool {
    matches!(role, AddressRole::LikelyVictim)
}

fn relation_key_matches(key: &str, chain: &str, address: &str) -> bool {
    let Some((key_chain, key_address)) = key.split_once(':') else {
        return false;
    };
    if !key_chain.trim().eq_ignore_ascii_case(chain.trim()) {
        return false;
    }
    if chain.trim().eq_ignore_ascii_case("solana") {
        key_address.trim() == address.trim()
    } else {
        key_address.trim().eq_ignore_ascii_case(address.trim())
    }
}

fn classification_for_seed<'a>(
    analysis: &'a CandidateAnalysis,
    chain: &str,
    address: &str,
) -> Option<&'a crate::analysis::LegitClassification> {
    analysis
        .legit_by_seed
        .iter()
        .find(|(key, _)| relation_key_matches(key, chain, address))
        .map(|(_, classification)| classification)
}

/// Build the analysis rollup for one seed from its related candidate analyses.
pub fn build_seed_analysis_rollup(
    registry: &CandidateRegistry,
    seed_contract: ContractId,
    seed_chain: &str,
    seed_address: &str,
    analyses: &AHashMap<ContractId, CandidateAnalysis>,
    output_dir_label: &str,
) -> (SeedAnalysisRollup, bool) {
    let relations = registry.relations_for_seed(seed_contract);
    let mut analyzed = 0u64;
    let mut suspected = 0u64;
    let mut legit = 0u64;
    let mut infringing_nfts = 0u64;
    let mut malicious = AHashSet::new();
    let mut honest = AHashSet::new();
    let mut economics = EconomicsUsdRollup::default();
    let mut value_flow_events = AHashMap::<
        (String, String, String, String, String),
        (Option<f64>, AHashSet<crate::enrich::ValueFlowKind>),
    >::new();
    let mut gas_events = AHashMap::<(String, String), (GasStage, f64)>::new();
    let mut economic_events = AHashMap::<EconomicEventKey, f64>::new();
    let mut ratio_gas_events = AHashMap::<(String, String), f64>::new();
    let mut ratio_economic_events = AHashMap::<EconomicEventKey, f64>::new();
    let mut complete_ratio_gas_events = AHashMap::<(String, String), f64>::new();
    let mut complete_ratio_economic_events = AHashMap::<EconomicEventKey, f64>::new();
    let mut refs = Vec::new();
    let mut all_ok = true;

    for rel in relations {
        let Some(analysis) = analyses.get(&rel.candidate_contract) else {
            all_ok = false;
            continue;
        };
        if analysis.evidence_quality.excluded_non_nft {
            continue;
        }
        analyzed += 1;
        let is_legit = classification_for_seed(analysis, seed_chain, seed_address)
            .map(|c| c.is_legit_duplicate)
            .unwrap_or(analysis.legit.is_legit_duplicate);
        if is_legit {
            legit += 1;
        } else {
            suspected += 1;
            infringing_nfts += rel.nft_ids.len() as u64;
            for (addr, attr) in &analysis.attribution {
                if role_is_malicious(attr.role) {
                    malicious.insert(format!("{}:{addr}", analysis.chain));
                }
                if role_is_honest(attr.role) {
                    honest.insert(format!("{}:{addr}", analysis.chain));
                }
            }
            merge_usd(&mut economics, &economics_usd_from(analysis));
            collect_economic_events(&mut economic_events, analysis);
            if analysis.economics.output_input_ratio_is_usd {
                collect_economic_events(&mut ratio_economic_events, analysis);
            }
            if analysis.has_complete_evidence() && analysis.economics.output_input_ratio_is_usd {
                collect_economic_events(&mut complete_ratio_economic_events, analysis);
            }
            for contribution in &analysis.economics.value_flow_contributions {
                let event_key = (
                    analysis.chain.trim().to_ascii_lowercase(),
                    normalize_chain_transaction(&analysis.chain, &contribution.tx_hash),
                    contribution.event_id.clone().unwrap_or_else(|| {
                        format!(
                            "legacy:{:016x}",
                            contribution.usd.map(f64::to_bits).unwrap_or_default()
                        )
                    }),
                    normalize_chain_address(&analysis.chain, &contribution.from),
                    normalize_chain_address(&analysis.chain, &contribution.to),
                );
                value_flow_events
                    .entry(event_key)
                    .and_modify(|(usd, kinds)| {
                        if let Some(value) = contribution.usd {
                            *usd = Some(usd.map_or(value, |current| current.max(value)));
                        }
                        kinds.insert(contribution.kind);
                    })
                    .or_insert((contribution.usd, AHashSet::from_iter([contribution.kind])));
            }
            for contribution in &analysis.economics.gas_contributions {
                let event_key = (
                    analysis.chain.trim().to_ascii_lowercase(),
                    normalize_chain_transaction(&analysis.chain, &contribution.tx_hash),
                );
                gas_events
                    .entry(event_key.clone())
                    .and_modify(|(stage, usd)| {
                        *stage = (*stage).max(contribution.stage);
                        *usd = (*usd).max(contribution.usd);
                    })
                    .or_insert((contribution.stage, contribution.usd));
                if analysis.economics.output_input_ratio_is_usd {
                    ratio_gas_events
                        .entry(event_key.clone())
                        .and_modify(|usd| *usd = (*usd).max(contribution.usd))
                        .or_insert(contribution.usd);
                }
                if analysis.has_complete_evidence() && analysis.economics.output_input_ratio_is_usd
                {
                    complete_ratio_gas_events
                        .entry(event_key)
                        .and_modify(|usd| *usd = (*usd).max(contribution.usd))
                        .or_insert(contribution.usd);
                }
            }
        }
        let path = format!(
            "{output_dir_label}/{}",
            candidate_file_name(&analysis.chain, &analysis.address)
        );
        refs.push(CandidateRef {
            chain: analysis.chain.clone(),
            address: analysis.address.clone(),
            is_legit_duplicate: is_legit,
            path,
        });
    }
    apply_deduped_economic_events(&mut economics, economic_events);
    if economics.output_input_ratio_count > 0 {
        let mut ratio_economics = EconomicsUsdRollup::default();
        apply_deduped_economic_events(&mut ratio_economics, ratio_economic_events);
        economics.ratio_operator_output_usd = ratio_economics.operator_output_usd;
        economics.ratio_attacker_input_usd = ratio_gas_events.into_values().sum();
    }
    if economics.complete_output_input_ratio_count > 0 {
        let mut complete_ratio_economics = EconomicsUsdRollup::default();
        apply_deduped_economic_events(
            &mut complete_ratio_economics,
            complete_ratio_economic_events,
        );
        economics.complete_ratio_operator_output_usd = complete_ratio_economics.operator_output_usd;
        economics.complete_ratio_attacker_input_usd = complete_ratio_gas_events.into_values().sum();
    }
    if !value_flow_events.is_empty() {
        economics.funding_usd = 0.0;
        economics.revenue_backflow_usd = 0.0;
        economics.withdrawal_usd = 0.0;
        economics.priced_value_flow_count = 0;
        economics.unpriced_value_flow_count = 0;
        for (_, (usd, kinds)) in value_flow_events {
            let funding = kinds.contains(&crate::enrich::ValueFlowKind::Funding);
            let withdrawal = kinds.contains(&crate::enrich::ValueFlowKind::Withdrawal)
                || kinds.contains(&crate::enrich::ValueFlowKind::Cashout);
            let kind = if kinds.contains(&crate::enrich::ValueFlowKind::RevenueBackflow)
                || (funding && withdrawal)
            {
                crate::enrich::ValueFlowKind::RevenueBackflow
            } else if funding {
                crate::enrich::ValueFlowKind::Funding
            } else {
                crate::enrich::ValueFlowKind::Withdrawal
            };
            if usd.is_some() {
                economics.priced_value_flow_count += 1;
            } else {
                economics.unpriced_value_flow_count += 1;
            }
            let usd = usd.unwrap_or(0.0);
            match kind {
                crate::enrich::ValueFlowKind::Funding => economics.funding_usd += usd,
                crate::enrich::ValueFlowKind::RevenueBackflow => {
                    economics.revenue_backflow_usd += usd;
                }
                crate::enrich::ValueFlowKind::Withdrawal
                | crate::enrich::ValueFlowKind::Cashout => economics.withdrawal_usd += usd,
            }
        }
    }
    if !gas_events.is_empty() {
        economics.setup_gas_usd = 0.0;
        economics.lure_gas_usd = 0.0;
        economics.exit_gas_usd = 0.0;
        economics.total_gas_usd = 0.0;
        economics.attacker_input_usd = 0.0;
        for (_, (stage, usd)) in gas_events {
            match stage {
                GasStage::Setup => economics.setup_gas_usd += usd,
                GasStage::Lure => economics.lure_gas_usd += usd,
                GasStage::Exit => economics.exit_gas_usd += usd,
            }
            economics.total_gas_usd += usd;
            economics.attacker_input_usd += usd;
        }
    }

    let overlapping_role_address_count = malicious.intersection(&honest).count() as u64;
    let honest_only_count = honest.difference(&malicious).count() as u64;

    (
        SeedAnalysisRollup {
            analyzed_candidate_count: analyzed,
            suspected_duplicate_contract_count: suspected,
            legit_duplicate_contract_count: legit,
            infringing_nft_count: infringing_nfts,
            malicious_address_count: malicious.len() as u64,
            honest_address_count: honest_only_count,
            overlapping_role_address_count,
            economics_usd: economics,
            candidate_refs: refs,
        },
        all_ok,
    )
}

fn behavior_kind_key(kind: BehaviorKind) -> &'static str {
    match kind {
        BehaviorKind::WashTrading => "wash_trading",
        BehaviorKind::PumpAndExit => "pump_and_exit",
        BehaviorKind::SybilDistribution => "sybil_distribution",
        BehaviorKind::FraudRevenue => "fraud_revenue",
        BehaviorKind::Poisoning => "poisoning",
        BehaviorKind::LayeredTransfer => "layered_transfer",
        BehaviorKind::InventoryConcentration => "inventory_concentration",
    }
}

#[derive(Default)]
struct CandidateScopeState {
    all_nfts: AHashSet<u32>,
    suspicious_nfts: AHashSet<u32>,
    fallback_nft_count: u64,
    has_legit_relation: bool,
    has_suspicious_relation: bool,
}

fn status_index(status: crate::enrich::EvidenceStatus) -> usize {
    match status {
        crate::enrich::EvidenceStatus::Complete => 0,
        crate::enrich::EvidenceStatus::Empty => 1,
        crate::enrich::EvidenceStatus::Failed => 2,
        crate::enrich::EvidenceStatus::Truncated => 3,
        crate::enrich::EvidenceStatus::NotRequested => 4,
    }
}

fn status_json(counts: [u64; 5]) -> Value {
    json!({
        "complete": counts[0],
        "empty": counts[1],
        "failed": counts[2],
        "truncated": counts[3],
        "not_requested": counts[4],
    })
}

fn normalize_negative_zero(value: &mut Value) {
    match value {
        Value::Number(number)
            if number.is_f64()
                && number
                    .as_f64()
                    .is_some_and(|value| value == 0.0 && value.is_sign_negative()) =>
        {
            *value = json!(0.0);
        }
        Value::Array(values) => values.iter_mut().for_each(normalize_negative_zero),
        Value::Object(values) => values.values_mut().for_each(normalize_negative_zero),
        _ => {}
    }
}

fn apply_dedup_dimensions(summary: &mut Value, dimensions: &Value) {
    if let Some(quality) = summary
        .get_mut("data_quality")
        .and_then(Value::as_object_mut)
    {
        quality.insert("dedup_dimensions".into(), dimensions.clone());
    }
    let Some(rows) = summary
        .get_mut("duplicate_scale")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for row in rows {
        let Some(category) = row.get("category").and_then(Value::as_str) else {
            continue;
        };
        let enabled = dimensions
            .get(format!("{category}_enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let Some(object) = row.as_object_mut() else {
            continue;
        };
        object.insert("enabled".into(), json!(enabled));
        if !enabled {
            for field in [
                "duplicate_nft_count",
                "duplicate_nft_ratio",
                "duplicate_nft_ratio_numerator",
                "duplicate_nft_ratio_denominator",
                "duplicate_contract_count",
                "duplicate_contract_ratio",
                "duplicate_contract_ratio_numerator",
                "duplicate_contract_ratio_denominator",
            ] {
                object.insert(field.into(), Value::Null);
            }
        }
    }
}

/// Aggregate one independently analyzed reporting scope over every available
/// seed report. Additional reports are kept for API compatibility and are
/// included identically.
pub fn build_run_summary_for_scope(
    selected: &[SeedRecord],
    reports: &[&SeedFullReport],
    additional_reports: &[&SeedFullReport],
    _failures: &[FailureRecord],
    analyses: &[&CandidateAnalysis],
    scope: RunSummaryScope<'_>,
) -> Value {
    build_run_summary_for_scope_with_store(
        selected,
        reports,
        additional_reports,
        _failures,
        analyses,
        None,
        scope,
    )
}

fn build_run_summary_for_scope_with_store(
    selected: &[SeedRecord],
    reports: &[&SeedFullReport],
    additional_reports: &[&SeedFullReport],
    _failures: &[FailureRecord],
    analyses: &[&CandidateAnalysis],
    store: Option<&ResidentStore>,
    scope: RunSummaryScope<'_>,
) -> Value {
    let selected_n = selected
        .iter()
        .filter(|seed| scope.seed_matches(&seed.chain))
        .count() as u64;
    let scoped_reports: Vec<_> = reports
        .iter()
        .chain(additional_reports.iter())
        .copied()
        .filter(|report| scope.seed_matches(&report.dedup.seed.chain))
        .collect();
    let included_n = scoped_reports.len() as u64;

    let analysis_by_key: AHashMap<String, &CandidateAnalysis> = analyses
        .iter()
        .map(|analysis| {
            (
                format!("{}:{}", analysis.chain, analysis.address),
                *analysis,
            )
        })
        .collect();
    let excluded_non_nft_contract_count = analyses
        .iter()
        .filter(|analysis| analysis.evidence_quality.excluded_non_nft)
        .map(|analysis| {
            format!(
                "{}:{}",
                analysis.chain.trim().to_ascii_lowercase(),
                normalize_chain_address(&analysis.chain, &analysis.address)
            )
        })
        .collect::<AHashSet<_>>()
        .len() as u64;
    let mut candidates: AHashMap<String, CandidateScopeState> = AHashMap::new();
    let mut representative_nfts = AHashSet::new();
    let mut with_dup = 0u64;
    let mut legit_relation_complete = 0u64;
    let mut legit_relation_incomplete = 0u64;

    for report in &scoped_reports {
        let seed_chain = report.dedup.seed.chain.as_str();
        let mut seed_has_duplicate = false;
        for rel in &report.dedup.relations {
            if !scope.relation_matches(seed_chain, &rel.candidate_chain) {
                continue;
            }
            let key = format!("{}:{}", rel.candidate_chain, rel.candidate_address);
            if analysis_by_key
                .get(&key)
                .is_some_and(|analysis| analysis.evidence_quality.excluded_non_nft)
            {
                continue;
            }
            seed_has_duplicate = true;
            let state = candidates.entry(key.clone()).or_default();
            state.fallback_nft_count = state.fallback_nft_count.max(rel.nft_count);
            state.all_nfts.extend(rel.nft_ids.iter().copied());
            representative_nfts.extend(rel.nft_ids.iter().copied());

            let classification = analysis_by_key.get(&key).and_then(|analysis| {
                classification_for_seed(analysis, seed_chain, &report.dedup.seed.address)
            });
            let is_legit = classification
                .map(|value| value.is_legit_duplicate)
                .or_else(|| {
                    analysis_by_key
                        .get(&key)
                        .map(|analysis| analysis.legit.is_legit_duplicate)
                })
                .unwrap_or(false);
            if classification.is_some_and(|value| value.verification_complete) {
                legit_relation_complete += 1;
            } else {
                legit_relation_incomplete += 1;
            }
            if is_legit {
                state.has_legit_relation = true;
            } else {
                state.has_suspicious_relation = true;
                state.suspicious_nfts.extend(rel.nft_ids.iter().copied());
            }
        }
        with_dup += u64::from(seed_has_duplicate);
    }

    let suspected: AHashSet<String> = candidates
        .iter()
        .filter(|(_, state)| state.has_suspicious_relation)
        .map(|(key, _)| key.clone())
        .collect();
    let legit_contract_count = candidates
        .values()
        .filter(|state| state.has_legit_relation && !state.has_suspicious_relation)
        .count() as u64;
    let infringing_nft_ids: AHashSet<u32> = candidates
        .values()
        .flat_map(|state| state.suspicious_nfts.iter().copied())
        .collect();
    let infringing_fallback: u64 = candidates
        .values()
        .filter(|state| state.has_suspicious_relation && state.suspicious_nfts.is_empty())
        .map(|state| state.fallback_nft_count)
        .sum();
    let infringing_nfts = infringing_nft_ids.len() as u64 + infringing_fallback;
    let mut hit_contract_nft_count = 0u64;
    let mut hit_contract_nft_count_complete = true;
    for key in &suspected {
        let state = &candidates[key];
        let fallback = (state.all_nfts.len() as u64).max(state.fallback_nft_count);
        let analysis = analysis_by_key.get(key);
        let provider_count = analysis
            .filter(|analysis| analysis.collection_nft_count_complete)
            .and_then(|analysis| analysis.collection_nft_count);
        let resident_count = store.and_then(|resident| {
            let analysis = analysis?;
            resident
                .contracts
                .get(analysis.contract_id as usize)
                .map(|contract| contract.nft_count)
        });
        // A provider population explicitly marked complete is authoritative for
        // "all NFTs in the hit contract". The resident store is the input
        // analysis snapshot and can be narrower than the on-chain collection;
        // use it only when no complete provider population is available.
        let count = provider_count.or(resident_count).unwrap_or_else(|| {
            hit_contract_nft_count_complete = false;
            fallback
        });
        if count < fallback {
            hit_contract_nft_count_complete = false;
        }
        hit_contract_nft_count += count.max(fallback);
    }

    let mut behavior_map: AHashMap<&'static str, BehaviorAgg> = [
        "wash_trading",
        "pump_and_exit",
        "sybil_distribution",
        "fraud_revenue",
        "poisoning",
        "layered_transfer",
        "inventory_concentration",
    ]
    .into_iter()
    .map(|key| (key, BehaviorAgg::default()))
    .collect();
    let mut malicious_addrs = AHashSet::new();
    let mut honest_addrs = AHashSet::new();
    let mut addr_to_suspect_contracts: AHashMap<String, AHashSet<String>> = AHashMap::new();
    let mut economics = EconomicsUsdRollup::default();
    let mut total_instances = 0u64;
    let mut wash_cycle_2 = 0u64;
    let mut wash_cycle_3 = 0u64;
    let mut wash_cycle_4 = 0u64;
    let mut wash_cycle_5p = 0u64;
    let mut contracts_with_behavior = AHashSet::new();
    let mut complete_behavior_contracts = AHashSet::new();
    let mut behavior_analyzable_contract_count = 0u64;
    let mut gas_by_contract = Vec::with_capacity(suspected.len());
    let mut value_flow_usd_by_event = AHashMap::<
        (String, String, String, String, String),
        (Option<f64>, AHashSet<crate::enrich::ValueFlowKind>),
    >::new();
    let mut gas_usd_by_tx = AHashMap::<(String, String), (GasStage, f64, String)>::new();
    let mut economic_usd_by_event = AHashMap::<EconomicEventKey, f64>::new();
    let mut ratio_gas_usd_by_tx = AHashMap::<(String, String), f64>::new();
    let mut ratio_economic_usd_by_event = AHashMap::<EconomicEventKey, f64>::new();
    let mut complete_ratio_gas_usd_by_tx = AHashMap::<(String, String), f64>::new();
    let mut complete_ratio_economic_usd_by_event = AHashMap::<EconomicEventKey, f64>::new();
    let mut evidence_statuses: BTreeMap<&'static str, [u64; 5]> = [
        "transfers",
        "sales",
        "holders",
        "prices",
        "assets",
        "histories",
        "gas",
        "value_flows",
    ]
    .into_iter()
    .map(|name| (name, [0; 5]))
    .collect();
    let mut truncation_reason_counts = BTreeMap::<String, u64>::new();
    let mut analyzed_suspected_count = 0u64;

    for analysis in analyses {
        let key = format!("{}:{}", analysis.chain, analysis.address);
        if !suspected.contains(&key) {
            continue;
        }
        analyzed_suspected_count += 1;
        let behavior_evidence_complete = [
            analysis.evidence_quality.transfers,
            analysis.evidence_quality.sales,
            analysis.evidence_quality.holders,
            analysis.evidence_quality.prices,
        ]
        .into_iter()
        .all(|status| {
            matches!(
                status,
                crate::enrich::EvidenceStatus::Complete | crate::enrich::EvidenceStatus::Empty
            )
        });
        if behavior_evidence_complete {
            behavior_analyzable_contract_count += 1;
        }
        let econ = economics_usd_from(analysis);
        merge_usd(&mut economics, &econ);
        collect_economic_events(&mut economic_usd_by_event, analysis);
        if analysis.economics.output_input_ratio_is_usd {
            collect_economic_events(&mut ratio_economic_usd_by_event, analysis);
        }
        if analysis.has_complete_evidence() && analysis.economics.output_input_ratio_is_usd {
            collect_economic_events(&mut complete_ratio_economic_usd_by_event, analysis);
        }
        gas_by_contract.push((
            key.clone(),
            analysis.economics.attacker_input_usd.unwrap_or(0.0),
        ));
        for contribution in &analysis.economics.value_flow_contributions {
            let event_key = (
                analysis.chain.trim().to_ascii_lowercase(),
                normalize_chain_transaction(&analysis.chain, &contribution.tx_hash),
                contribution.event_id.clone().unwrap_or_else(|| {
                    format!(
                        "legacy:{:016x}",
                        contribution.usd.map(f64::to_bits).unwrap_or_default()
                    )
                }),
                normalize_chain_address(&analysis.chain, &contribution.from),
                normalize_chain_address(&analysis.chain, &contribution.to),
            );
            value_flow_usd_by_event
                .entry(event_key)
                .and_modify(|(usd, kinds)| {
                    if let Some(value) = contribution.usd {
                        *usd = Some(usd.map_or(value, |current| current.max(value)));
                    }
                    kinds.insert(contribution.kind);
                })
                .or_insert((contribution.usd, AHashSet::from_iter([contribution.kind])));
        }
        for contribution in &analysis.economics.gas_contributions {
            let tx_key = (
                analysis.chain.trim().to_ascii_lowercase(),
                normalize_chain_transaction(&analysis.chain, &contribution.tx_hash),
            );
            gas_usd_by_tx
                .entry(tx_key.clone())
                .and_modify(|(stage, usd, owner)| {
                    *stage = (*stage).max(contribution.stage);
                    *usd = (*usd).max(contribution.usd);
                    if key < *owner {
                        *owner = key.clone();
                    }
                })
                .or_insert((contribution.stage, contribution.usd, key.clone()));
            if analysis.economics.output_input_ratio_is_usd {
                ratio_gas_usd_by_tx
                    .entry(tx_key.clone())
                    .and_modify(|usd| *usd = (*usd).max(contribution.usd))
                    .or_insert(contribution.usd);
            }
            if analysis.has_complete_evidence() && analysis.economics.output_input_ratio_is_usd {
                complete_ratio_gas_usd_by_tx
                    .entry(tx_key)
                    .and_modify(|usd| *usd = (*usd).max(contribution.usd))
                    .or_insert(contribution.usd);
            }
        }

        for (name, status) in [
            ("transfers", analysis.evidence_quality.transfers),
            ("sales", analysis.evidence_quality.sales),
            ("holders", analysis.evidence_quality.holders),
            ("prices", analysis.evidence_quality.prices),
            ("assets", analysis.evidence_quality.assets),
            ("histories", analysis.evidence_quality.histories),
            ("gas", analysis.evidence_quality.gas),
            ("value_flows", analysis.evidence_quality.value_flows),
        ] {
            evidence_statuses.get_mut(name).unwrap()[status_index(status)] += 1;
        }
        for reason in &analysis.evidence_quality.truncation_reasons {
            *truncation_reason_counts.entry(reason.clone()).or_insert(0) += 1;
        }

        for (addr, attr) in &analysis.attribution {
            let address_key = format!("{}:{}", analysis.chain, addr);
            if role_is_malicious(attr.role) {
                malicious_addrs.insert(address_key.clone());
                addr_to_suspect_contracts
                    .entry(address_key)
                    .or_default()
                    .insert(key.clone());
            }
            if role_is_honest(attr.role) {
                honest_addrs.insert(format!("{}:{}", analysis.chain, addr));
            }
        }
        let mut kinds_seen = AHashSet::new();
        for inst in &analysis.behavior_instances {
            total_instances += 1;
            let behavior_key = behavior_kind_key(inst.kind);
            let aggregate = behavior_map.get_mut(behavior_key).unwrap();
            aggregate.instance_count += 1;
            if behavior_evidence_complete {
                aggregate.complete_instance_count += 1;
                aggregate.complete_contracts.insert(key.clone());
            }
            aggregate.addresses.extend(
                inst.addresses
                    .iter()
                    .map(|address| format!("{}:{address}", analysis.chain)),
            );
            aggregate.nfts.extend(
                inst.nfts
                    .iter()
                    .map(|token| format!("{}:{}:{token}", analysis.chain, analysis.address)),
            );
            aggregate.linked_buyers.extend(
                inst.linked_buyers
                    .iter()
                    .map(|buyer| format!("{}:{buyer}", analysis.chain)),
            );
            for event in &inst.linked_loss_events {
                let event_key = format!(
                    "{}:{}:{}:{}",
                    key,
                    normalize_chain_transaction(&analysis.chain, &event.tx_hash),
                    normalize_chain_address(&analysis.chain, &event.buyer),
                    event.token_id
                );
                aggregate
                    .linked_loss_events
                    .entry(event_key)
                    .or_insert(event.usd_amount);
            }
            kinds_seen.insert(behavior_key);
            if matches!(inst.kind, BehaviorKind::WashTrading) {
                match inst.addresses.len() {
                    0 | 1 => {}
                    2 => wash_cycle_2 += 1,
                    3 => wash_cycle_3 += 1,
                    4 => wash_cycle_4 += 1,
                    _ => wash_cycle_5p += 1,
                }
            }
        }
        if !kinds_seen.is_empty() {
            contracts_with_behavior.insert(key.clone());
            if behavior_evidence_complete {
                complete_behavior_contracts.insert(key.clone());
            }
        }
        for behavior_key in kinds_seen {
            behavior_map
                .get_mut(behavior_key)
                .unwrap()
                .contracts
                .insert(key.clone());
        }
    }

    // Candidate reports intentionally retain their full attributable amounts.
    // Run-level totals deduplicate the same on-chain transaction contribution
    // when it was relevant to more than one candidate.
    apply_deduped_economic_events(&mut economics, economic_usd_by_event);
    if economics.output_input_ratio_count > 0 {
        let mut ratio_economics = EconomicsUsdRollup::default();
        apply_deduped_economic_events(&mut ratio_economics, ratio_economic_usd_by_event);
        economics.ratio_operator_output_usd = ratio_economics.operator_output_usd;
        economics.ratio_attacker_input_usd = ratio_gas_usd_by_tx.into_values().sum();
    }
    if economics.complete_output_input_ratio_count > 0 {
        let mut complete_ratio_economics = EconomicsUsdRollup::default();
        apply_deduped_economic_events(
            &mut complete_ratio_economics,
            complete_ratio_economic_usd_by_event,
        );
        economics.complete_ratio_operator_output_usd = complete_ratio_economics.operator_output_usd;
        economics.complete_ratio_attacker_input_usd =
            complete_ratio_gas_usd_by_tx.into_values().sum();
    }
    if !value_flow_usd_by_event.is_empty() {
        economics.funding_usd = 0.0;
        economics.revenue_backflow_usd = 0.0;
        economics.withdrawal_usd = 0.0;
        economics.priced_value_flow_count = 0;
        economics.unpriced_value_flow_count = 0;
        for (_, (usd, kinds)) in value_flow_usd_by_event {
            let funding = kinds.contains(&crate::enrich::ValueFlowKind::Funding);
            let withdrawal = kinds.contains(&crate::enrich::ValueFlowKind::Withdrawal)
                || kinds.contains(&crate::enrich::ValueFlowKind::Cashout);
            let kind = if kinds.contains(&crate::enrich::ValueFlowKind::RevenueBackflow)
                || (funding && withdrawal)
            {
                crate::enrich::ValueFlowKind::RevenueBackflow
            } else if funding {
                crate::enrich::ValueFlowKind::Funding
            } else {
                crate::enrich::ValueFlowKind::Withdrawal
            };
            if usd.is_some() {
                economics.priced_value_flow_count += 1;
            } else {
                economics.unpriced_value_flow_count += 1;
            }
            let usd = usd.unwrap_or(0.0);
            match kind {
                crate::enrich::ValueFlowKind::Funding => economics.funding_usd += usd,
                crate::enrich::ValueFlowKind::RevenueBackflow => {
                    economics.revenue_backflow_usd += usd;
                }
                crate::enrich::ValueFlowKind::Withdrawal
                | crate::enrich::ValueFlowKind::Cashout => economics.withdrawal_usd += usd,
            }
        }
    }
    if !gas_usd_by_tx.is_empty() {
        economics.setup_gas_usd = 0.0;
        economics.lure_gas_usd = 0.0;
        economics.exit_gas_usd = 0.0;
        economics.total_gas_usd = 0.0;
        economics.attacker_input_usd = 0.0;
        let mut dedup_gas_by_contract = AHashMap::<String, f64>::new();
        for (_, (stage, usd, owner)) in gas_usd_by_tx {
            match stage {
                GasStage::Setup => economics.setup_gas_usd += usd,
                GasStage::Lure => economics.lure_gas_usd += usd,
                GasStage::Exit => economics.exit_gas_usd += usd,
            }
            economics.total_gas_usd += usd;
            economics.attacker_input_usd += usd;
            *dedup_gas_by_contract.entry(owner).or_insert(0.0) += usd;
        }
        gas_by_contract = dedup_gas_by_contract.into_iter().collect();
    }

    let wash_total = wash_cycle_2 + wash_cycle_3 + wash_cycle_4 + wash_cycle_5p;
    let wash_cycle_size_distribution = [
        ("2", wash_cycle_2),
        ("3", wash_cycle_3),
        ("4", wash_cycle_4),
        ("5+", wash_cycle_5p),
    ]
    .into_iter()
    .map(|(bucket, count)| {
        json!({
            "node_count_bucket": bucket,
            "cycle_count": count,
            "cycle_ratio": (wash_total > 0).then_some(count as f64 / wash_total as f64),
            "cycle_ratio_numerator": count,
            "cycle_ratio_denominator": wash_total,
        })
    })
    .collect::<Vec<_>>();

    gas_by_contract.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let gas_total: f64 = gas_by_contract.iter().map(|(_, gas)| *gas).sum();
    let top_k = ((suspected.len() as f64) * 0.10).ceil() as usize;
    let top_k = top_k.min(gas_by_contract.len());
    let top_gas: f64 = gas_by_contract
        .iter()
        .take(top_k)
        .map(|(_, gas)| *gas)
        .sum();
    let gas_concentration = (gas_total > 0.0 && economics.unpriced_gas_cost_contract_count == 0)
        .then_some(top_gas / gas_total);

    let overlapping_role_address_count = malicious_addrs.intersection(&honest_addrs).count() as u64;
    let honest_only_address_count = honest_addrs.difference(&malicious_addrs).count() as u64;
    let repeat_malicious = addr_to_suspect_contracts
        .values()
        .filter(|contracts| contracts.len() >= 2)
        .count() as u64;
    let total_address_count = malicious_addrs.union(&honest_addrs).count() as u64;
    let suspected_n = suspected.len() as u64;

    let mut behaviors = serde_json::Map::new();
    for (key, aggregate) in &behavior_map {
        behaviors.insert(
            (*key).into(),
            json!({
                "contract_count": aggregate.contracts.len() as u64,
                "observed_contract_coverage_ratio": (suspected_n > 0)
                    .then_some(aggregate.contracts.len() as f64 / suspected_n as f64),
                "complete_evidence_contract_count": aggregate.complete_contracts.len() as u64,
                "behavior_analyzable_contract_count": behavior_analyzable_contract_count,
                "contract_coverage_ratio": (behavior_analyzable_contract_count > 0).then_some(
                    aggregate.complete_contracts.len() as f64
                        / behavior_analyzable_contract_count as f64),
                "instance_count": aggregate.instance_count,
                "complete_evidence_instance_count": aggregate.complete_instance_count,
                "instance_ratio": (total_instances > 0)
                    .then_some(aggregate.instance_count as f64 / total_instances as f64),
                "address_count": aggregate.addresses.len() as u64,
                "nft_count": aggregate.nfts.len() as u64,
                "linked_buyer_count": aggregate.linked_buyers.len() as u64,
                "linked_paid_exposure_usd": aggregate.linked_loss_events.values().sum::<f64>(),
            }),
        );
    }
    let mut total_behavior_addresses = AHashSet::new();
    let mut total_behavior_nfts = AHashSet::new();
    let mut total_linked_buyers = AHashSet::new();
    let mut total_linked_loss_events = AHashMap::new();
    for aggregate in behavior_map.values() {
        total_behavior_addresses.extend(aggregate.addresses.iter().cloned());
        total_behavior_nfts.extend(aggregate.nfts.iter().cloned());
        total_linked_buyers.extend(aggregate.linked_buyers.iter().cloned());
        for (event, amount) in &aggregate.linked_loss_events {
            total_linked_loss_events
                .entry(event.clone())
                .or_insert(*amount);
        }
    }
    behaviors.insert(
        "total".into(),
        json!({
            "contract_count": contracts_with_behavior.len() as u64,
            "complete_evidence_contract_count": complete_behavior_contracts.len() as u64,
            "behavior_analyzable_contract_count": behavior_analyzable_contract_count,
            "contract_coverage_ratio": (behavior_analyzable_contract_count > 0).then_some(
                complete_behavior_contracts.len() as f64
                    / behavior_analyzable_contract_count as f64),
            "instance_count": total_instances,
            "instance_ratio": (total_instances > 0).then_some(1.0),
            "address_count": total_behavior_addresses.len() as u64,
            "nft_count": total_behavior_nfts.len() as u64,
            "linked_buyer_count": total_linked_buyers.len() as u64,
            "linked_paid_exposure_usd": total_linked_loss_events.values().sum::<f64>(),
        }),
    );

    let evidence_complete = |name: &str| {
        let counts = evidence_statuses.get(name).copied().unwrap_or_default();
        counts[0] + counts[1] == suspected_n
    };
    let required_evidence_complete = analyzed_suspected_count == suspected_n
        && [
            "transfers",
            "sales",
            "holders",
            "prices",
            "gas",
            "value_flows",
        ]
        .into_iter()
        .all(evidence_complete);
    let evidence_quality: serde_json::Map<String, Value> = evidence_statuses
        .into_iter()
        .map(|(name, counts)| (name.to_owned(), status_json(counts)))
        .collect();
    let representative_candidate_count = representative_nfts.len() as u64
        + candidates
            .values()
            .filter(|state| state.all_nfts.is_empty())
            .map(|state| state.fallback_nft_count)
            .sum::<u64>();
    let observed_usd_pricing_complete = economics.unpriced_sale_count == 0
        && economics.amountless_sale_count == 0
        && economics.assumed_stablecoin_peg_sale_count == 0
        && economics.unpriced_value_flow_count == 0
        && economics.unpriced_operator_sale_proceeds_count == 0
        && economics.unpriced_operator_paid_mint_payment_count == 0
        && economics.unpriced_honest_paid_mint_loss_count == 0
        && economics.unpriced_gas_cost_contract_count == 0;
    let operator_output_attribution_complete = economics.unknown_operator_sale_proceeds_count == 0
        && economics.unknown_royalty_recipient_count == 0
        && economics.unknown_paid_mint_receiver_count == 0;
    let operator_output_pricing_complete = economics.unpriced_operator_sale_proceeds_count == 0
        && economics.unpriced_operator_paid_mint_payment_count == 0;
    let operator_output_complete = required_evidence_complete
        && operator_output_pricing_complete
        && operator_output_attribution_complete;
    let usd_valuation_complete = required_evidence_complete && observed_usd_pricing_complete;
    let ratio_candidate_coverage_complete =
        suspected_n > 0 && economics.output_input_ratio_count == suspected_n;
    let ratio_evidence_complete = ratio_candidate_coverage_complete && operator_output_complete;
    let ratio_sample_complete = ratio_evidence_complete;
    // Completeness is a report-level invariant as well as a per-contract source
    // property. Never publish a complete denominator that cannot contain every
    // distinct stuck NFT counted by the same scope.
    if economics.stuck_nft_count > hit_contract_nft_count {
        hit_contract_nft_count_complete = false;
    }
    let stuck_nft_ratio_valid = hit_contract_nft_count_complete
        && hit_contract_nft_count > 0
        && economics.stuck_nft_count <= hit_contract_nft_count;
    let mut economics_json = json!({
        "operator_output_usd": economics.operator_output_usd,
        "honest_paid_exposure_usd": economics.honest_loss_usd,
        "secondary_sale_paid_exposure_usd": economics.secondary_sale_loss_usd,
        "paid_mint_exposure_usd": economics.paid_mint_loss_usd,
        "gross_sales_volume_usd": economics.gross_revenue_usd,
        "marketplace_fee_usd": economics.marketplace_fee_usd,
        "royalty_fee_usd": economics.royalty_fee_usd,
        "operator_royalty_usd": economics.operator_royalty_usd,
        "setup_gas_usd": economics.setup_gas_usd,
        "lure_gas_usd": economics.lure_gas_usd,
        "exit_gas_usd": economics.exit_gas_usd,
        "total_gas_usd": economics.total_gas_usd,
        "funding_usd": economics.funding_usd,
        "revenue_backflow_usd": economics.revenue_backflow_usd,
        "withdrawal_usd": economics.withdrawal_usd,
        "stuck_nft_count": economics.stuck_nft_count,
        "stuck_nft_ratio": stuck_nft_ratio_valid
            .then_some(economics.stuck_nft_count as f64 / hit_contract_nft_count as f64),
        "output_input_ratio": (economics.ratio_attacker_input_usd > 0.0)
            .then_some(economics.ratio_operator_output_usd / economics.ratio_attacker_input_usd),
        "observed_output_input_ratio": (economics.ratio_attacker_input_usd > 0.0)
            .then_some(economics.ratio_operator_output_usd / economics.ratio_attacker_input_usd),
        "ratio_operator_output_usd": economics.ratio_operator_output_usd,
        "output_input_ratio_count": economics.output_input_ratio_count,
        "output_input_ratio_ge1_count": economics.output_input_ratio_ge1_count,
        "output_input_ratio_lt1_count": economics.output_input_ratio_lt1_count,
        "output_input_ratio_ge1_share": (economics.output_input_ratio_count > 0).then_some(
            economics.output_input_ratio_ge1_count as f64
                / economics.output_input_ratio_count as f64),
        "output_input_ratio_lt1_share": (economics.output_input_ratio_count > 0).then_some(
            economics.output_input_ratio_lt1_count as f64
                / economics.output_input_ratio_count as f64),
        "complete_evidence_output_input_ratio":
            (economics.complete_ratio_attacker_input_usd > 0.0).then_some(
                economics.complete_ratio_operator_output_usd
                    / economics.complete_ratio_attacker_input_usd),
        "complete_evidence_ratio_operator_output_usd": economics.complete_ratio_operator_output_usd,
        "complete_evidence_ratio_attacker_input_usd": economics.complete_ratio_attacker_input_usd,
        "complete_evidence_output_input_ratio_count": economics.complete_output_input_ratio_count,
        "complete_evidence_output_input_ratio_ge1_count": economics.complete_output_input_ratio_ge1_count,
        "complete_evidence_output_input_ratio_lt1_count": economics.complete_output_input_ratio_lt1_count,
        "complete_evidence_output_input_ratio_ge1_share":
            (economics.complete_output_input_ratio_count > 0).then_some(
                economics.complete_output_input_ratio_ge1_count as f64
                    / economics.complete_output_input_ratio_count as f64),
        "complete_evidence_output_input_ratio_lt1_share":
            (economics.complete_output_input_ratio_count > 0).then_some(
                economics.complete_output_input_ratio_lt1_count as f64
                    / economics.complete_output_input_ratio_count as f64),
        "attacker_input_usd": economics.attacker_input_usd,
        "top_contract_gas_contribution_ratio": gas_concentration,
        "top_contract_gas_contribution_numerator_usd": top_gas,
        "top_contract_gas_contribution_denominator_usd": gas_total,
        "top_contract_gas_count": top_k as u64,
        "usd_valuation_complete": usd_valuation_complete,
        "operator_output_complete": operator_output_complete,
    });
    if let Some(object) = economics_json.as_object_mut() {
        object.insert(
            "all_observed_operator_output_usd".into(),
            json!(economics.operator_output_usd),
        );
        object.insert(
            "hit_contract_nft_count".into(),
            json!(hit_contract_nft_count),
        );
        object.insert(
            "hit_contract_nft_count_complete".into(),
            json!(hit_contract_nft_count_complete),
        );
        object.insert("stuck_nft_ratio_valid".into(), json!(stuck_nft_ratio_valid));
        object.insert(
            "ratio_eligible_operator_output_usd".into(),
            json!(economics.ratio_operator_output_usd),
        );
        object.insert(
            "ratio_eligible_attacker_input_usd".into(),
            json!(economics.ratio_attacker_input_usd),
        );
        object.insert(
            "ratio_eligible_contract_count".into(),
            json!(economics.output_input_ratio_count),
        );
        object.insert("ratio_sample_complete".into(), json!(ratio_sample_complete));
        object.insert(
            "ratio_candidate_coverage_complete".into(),
            json!(ratio_candidate_coverage_complete),
        );
        object.insert(
            "ratio_evidence_complete".into(),
            json!(ratio_evidence_complete),
        );
        object.insert(
            "ratio_is_observed_only".into(),
            json!(economics.output_input_ratio_count > 0 && !ratio_evidence_complete),
        );
        object.insert(
            "evidence_coverage_complete".into(),
            json!(required_evidence_complete),
        );
        object.insert(
            "observed_usd_pricing_complete".into(),
            json!(observed_usd_pricing_complete),
        );
        object.insert(
            "operator_output_attribution_complete".into(),
            json!(operator_output_attribution_complete),
        );
    }
    let pricing_quality = json!({
        "sale_count": economics.sale_count,
        "priced_sale_count": economics.priced_sale_count,
        "unpriced_sale_count": economics.unpriced_sale_count,
        "amountless_sale_count": economics.amountless_sale_count,
        "assumed_stablecoin_peg_sale_count": economics.assumed_stablecoin_peg_sale_count,
        "priced_value_flow_count": economics.priced_value_flow_count,
        "unpriced_value_flow_count": economics.unpriced_value_flow_count,
        "operator_sale_count": economics.operator_sale_count,
        "priced_operator_sale_proceeds_count": economics.priced_operator_sale_proceeds_count,
        "unpriced_operator_sale_proceeds_count": economics.unpriced_operator_sale_proceeds_count,
        "unknown_operator_sale_proceeds_count": economics.unknown_operator_sale_proceeds_count,
        "unknown_royalty_recipient_count": economics.unknown_royalty_recipient_count,
        "paid_mint_payment_count": economics.paid_mint_payment_count,
        "operator_paid_mint_payment_count": economics.operator_paid_mint_payment_count,
        "priced_operator_paid_mint_payment_count": economics.priced_operator_paid_mint_payment_count,
        "unpriced_operator_paid_mint_payment_count": economics.unpriced_operator_paid_mint_payment_count,
        "unknown_paid_mint_receiver_count": economics.unknown_paid_mint_receiver_count,
        "honest_paid_mint_exposure_count": economics.honest_paid_mint_loss_count,
        "priced_honest_paid_mint_exposure_count": economics.priced_honest_paid_mint_loss_count,
        "unpriced_honest_paid_mint_exposure_count": economics.unpriced_honest_paid_mint_loss_count,
        "gas_cost_contract_count": economics.gas_cost_contract_count,
        "priced_gas_cost_contract_count": economics.priced_gas_cost_contract_count,
        "unpriced_gas_cost_contract_count": economics.unpriced_gas_cost_contract_count,
        "usd_valuation_complete": usd_valuation_complete,
        "operator_output_complete": operator_output_complete,
        "evidence_coverage_complete": required_evidence_complete,
        "observed_usd_pricing_complete": observed_usd_pricing_complete,
        "operator_output_attribution_complete": operator_output_attribution_complete,
    });
    let data_quality = json!({
        "representative_candidate_count": representative_candidate_count,
        "representative_candidate_nft_count": representative_candidate_count,
        "candidate_contract_count": candidates.len() as u64,
        "candidate_analysis_missing_count": suspected_n.saturating_sub(analyzed_suspected_count),
        "excluded_non_nft_contract_count": excluded_non_nft_contract_count,
        "suspected_duplicate_contract_count": suspected_n,
        "legit_duplicate_contract_count": legit_contract_count,
        "infringing_nft_count": infringing_nfts,
        "hit_contract_nft_count": hit_contract_nft_count,
        "hit_contract_nft_count_complete": hit_contract_nft_count_complete,
        "legit_relation_verification_complete": legit_relation_complete,
        "legit_relation_verification_incomplete": legit_relation_incomplete,
        "pricing": pricing_quality,
        "evidence": evidence_quality,
        "truncation_reason_counts": truncation_reason_counts,
    });
    let seed_rows = scoped_reports
        .iter()
        .map(|report| {
            json!({
                "chain": report.dedup.seed.chain,
                "address": report.dedup.seed.address,
                "candidate_contract_count": report.dedup.relations.iter().filter(|rel|
                    scope.relation_matches(&report.dedup.seed.chain, &rel.candidate_chain)
                ).count() as u64,
                "hit_edge_count_all_scopes": report.dedup.hit_edge_count,
                "scopes_complete": report.scopes_complete,
                "analysis_complete": report.analysis_complete,
            })
        })
        .collect::<Vec<_>>();

    let mut summary = json!({
        "analysis_available": true,
        "selected_seed_count": selected_n,
        "included_seed_report_count": included_n,
        "excluded_seed_count": selected_n.saturating_sub(included_n),
        "seed_with_duplicate_count": with_dup,
        "seed_duplicate_ratio": (included_n > 0).then_some(with_dup as f64 / included_n as f64),
        "seed_duplicate_ratio_numerator": with_dup,
        "seed_duplicate_ratio_denominator": included_n,
        "representative_candidate_count": representative_candidate_count,
        "representative_candidate_nft_count": representative_candidate_count,
        "candidate_contract_count": candidates.len() as u64,
        "excluded_non_nft_contract_count": excluded_non_nft_contract_count,
        "suspected_duplicate_contract_count": suspected_n,
        "legit_duplicate_contract_count": legit_contract_count,
        "infringing_nft_count": infringing_nfts,
        "address_classification": {
            "malicious_address_count": malicious_addrs.len() as u64,
            "repeat_infringing_malicious_address_count": repeat_malicious,
            "honest_address_count": honest_only_address_count,
            "overlapping_role_address_count": overlapping_role_address_count,
            "total_address_count": total_address_count,
        },
        "behaviors": behaviors,
        "behavior_contract_count": contracts_with_behavior.len() as u64,
        "behavior_analyzable_contract_count": behavior_analyzable_contract_count,
        "complete_evidence_behavior_contract_count": complete_behavior_contracts.len() as u64,
        "wash_cycle_size_distribution": wash_cycle_size_distribution,
        "economics": economics_json,
        "data_quality": data_quality,
        "scope_summary": {
            "seed_count": included_n,
            "candidate_contract_count": candidates.len() as u64,
            "economics_usd": economics,
        },
        "seeds": seed_rows,
    });
    normalize_negative_zero(&mut summary);
    summary
}

/// Backward-compatible all-chains summary builder.
pub fn build_run_summary(
    selected: &[SeedRecord],
    reports: &[&SeedFullReport],
    additional_reports: &[&SeedFullReport],
    failures: &[FailureRecord],
    analyses: &[&CandidateAnalysis],
) -> Value {
    build_run_summary_for_scope(
        selected,
        reports,
        additional_reports,
        failures,
        analyses,
        RunSummaryScope::All,
    )
}

#[derive(Default)]
struct BehaviorAgg {
    contracts: AHashSet<String>,
    complete_contracts: AHashSet<String>,
    instance_count: u64,
    complete_instance_count: u64,
    addresses: AHashSet<String>,
    nfts: AHashSet<String>,
    linked_buyers: AHashSet<String>,
    linked_loss_events: AHashMap<String, f64>,
}

fn filter_non_nft_report(
    store: &ResidentStore,
    report: &SeedFullReport,
    excluded_non_nft: &AHashSet<String>,
) -> SeedFullReport {
    let mut report = report.clone();
    report.dedup.relations.retain(|relation| {
        let key = format!(
            "{}:{}",
            relation.candidate_chain.trim().to_ascii_lowercase(),
            normalize_chain_address(&relation.candidate_chain, &relation.candidate_address)
        );
        !excluded_non_nft.contains(&key)
    });
    report.dedup.candidate_contract_count = report.dedup.relations.len() as u64;
    report.dedup.hit_edge_count = report
        .dedup
        .relations
        .iter()
        .map(|relation| relation.hit_edge_count)
        .sum();
    report.dedup.duplicate_scale = rebuild_seed_duplicate_scale(store, &report.dedup);
    report
}

/// Write full `run` artifacts under `output_dir`.
///
/// Layout: `intermediate/` (manifest, failures), `detail/` (seeds + candidates),
/// `summary/` (intra_chain / chain_matrix / cross_chain / all_chains).
#[allow(clippy::too_many_arguments)] // Top-level writer receives each already-built report component.
pub fn write_run_outputs(
    output_dir: &Path,
    params: &DedupRunParams,
    store: &ResidentStore,
    selected_seeds: &[SeedRecord],
    analyzed: &[Result<(SeedRecord, SeedFullReport), FailureRecord>],
    analyses: &[CandidateAnalysis],
    scope_analyses: &ScopeAnalysisSets,
    extra_failures: &[FailureRecord],
) -> Result<(), AnalysisError> {
    ensure_output_layout(output_dir).map_err(AnalysisError::from)?;

    let mut failures = extra_failures.to_vec();
    let excluded_non_nft = analyses
        .iter()
        .filter(|analysis| analysis.evidence_quality.excluded_non_nft)
        .map(|analysis| {
            format!(
                "{}:{}",
                analysis.chain.trim().to_ascii_lowercase(),
                normalize_chain_address(&analysis.chain, &analysis.address)
            )
        })
        .collect::<AHashSet<_>>();
    let mut filtered_reports = Vec::<SeedFullReport>::new();
    for item in analyzed {
        match item {
            Ok((_seed, report)) => {
                filtered_reports.push(filter_non_nft_report(store, report, &excluded_non_nft));
            }
            Err(fail) => failures.push(fail.clone()),
        }
    }
    let ok_reports: Vec<&SeedFullReport> = filtered_reports.iter().collect();

    for report in &ok_reports {
        let dir = seed_report_dir(output_dir, &seed_dir_name(&report.dedup.seed));
        write_json(&dir.join("report.json"), report)?;
        markdown::write_seed_full_report_md(&dir.join("report.md"), report)?;
    }

    // Every available seed report contributes. Evidence problems stay attached
    // to candidate quality/error records instead of deleting the whole seed.
    let dedup_refs: Vec<&SeedDedupReport> = ok_reports.iter().map(|r| &r.dedup).collect();
    let analysis_refs: Vec<&CandidateAnalysis> = analyses.iter().collect();
    let mut all_summary = build_run_summary_for_scope_with_store(
        selected_seeds,
        &ok_reports,
        &[],
        &failures,
        &analysis_refs,
        Some(store),
        RunSummaryScope::All,
    );
    let intra_refs: Vec<&CandidateAnalysis> =
        scope_analyses.intra_chain.iter().map(Arc::as_ref).collect();
    let mut intra_summary = build_run_summary_for_scope_with_store(
        selected_seeds,
        &ok_reports,
        &[],
        &failures,
        &intra_refs,
        Some(store),
        RunSummaryScope::Intra,
    );
    let cross_refs: Vec<&CandidateAnalysis> =
        scope_analyses.cross_chain.iter().map(Arc::as_ref).collect();
    let mut cross_summary = build_run_summary_for_scope_with_store(
        selected_seeds,
        &ok_reports,
        &[],
        &failures,
        &cross_refs,
        Some(store),
        RunSummaryScope::Cross,
    );
    let mut intra_chain_summaries = BTreeMap::new();
    let mut cross_primary_summaries = BTreeMap::new();
    for chain in &store.chains {
        let chain_key = chain.to_ascii_lowercase();
        let intra_refs: Vec<&CandidateAnalysis> = scope_analyses
            .intra_chain_by_chain
            .get(&chain_key)
            .into_iter()
            .flatten()
            .map(Arc::as_ref)
            .collect();
        intra_chain_summaries.insert(
            chain_key.clone(),
            build_run_summary_for_scope_with_store(
                selected_seeds,
                &ok_reports,
                &[],
                &failures,
                &intra_refs,
                Some(store),
                RunSummaryScope::IntraChain { chain },
            ),
        );
        let cross_refs: Vec<&CandidateAnalysis> = scope_analyses
            .cross_chain_by_primary
            .get(&chain_key)
            .into_iter()
            .flatten()
            .map(Arc::as_ref)
            .collect();
        cross_primary_summaries.insert(
            chain_key,
            build_run_summary_for_scope_with_store(
                selected_seeds,
                &ok_reports,
                &[],
                &failures,
                &cross_refs,
                Some(store),
                RunSummaryScope::CrossPrimary {
                    primary_chain: chain,
                },
            ),
        );
    }
    let mut matrix_summaries = BTreeMap::new();
    let mut matrix_primaries: Vec<String> = store
        .chains
        .iter()
        .map(|chain| chain.to_ascii_lowercase())
        .collect();
    matrix_primaries.sort();
    for primary in matrix_primaries {
        for secondary in &store.chains {
            if primary.eq_ignore_ascii_case(secondary) {
                continue;
            }
            let direction = (primary.clone(), secondary.to_ascii_lowercase());
            let refs: Vec<&CandidateAnalysis> = scope_analyses
                .chain_matrix
                .get(&direction)
                .into_iter()
                .flatten()
                .map(Arc::as_ref)
                .collect();
            matrix_summaries.insert(
                direction,
                build_run_summary_for_scope_with_store(
                    selected_seeds,
                    &ok_reports,
                    &[],
                    &failures,
                    &refs,
                    Some(store),
                    RunSummaryScope::Matrix {
                        primary_chain: &primary,
                        secondary_chain: secondary,
                    },
                ),
            );
        }
    }

    let dimensions = json!({
        "token_uri_enabled": true,
        "image_uri_enabled": true,
        "metadata_enabled": true,
        "name_enabled": params.name_threshold.is_some(),
    });
    for summary in [&mut all_summary, &mut intra_summary, &mut cross_summary] {
        apply_dedup_dimensions(summary, &dimensions);
    }
    for summary in matrix_summaries.values_mut() {
        apply_dedup_dimensions(summary, &dimensions);
    }
    for summary in intra_chain_summaries
        .values_mut()
        .chain(cross_primary_summaries.values_mut())
    {
        apply_dedup_dimensions(summary, &dimensions);
    }
    super::json::write_four_scope_paper_summaries_public(
        output_dir,
        store,
        &dedup_refs,
        super::json::ScopePaperSummaries {
            all: &all_summary,
            intra: &intra_summary,
            cross: &cross_summary,
            intra_by_chain: &intra_chain_summaries,
            cross_by_primary: &cross_primary_summaries,
            matrix: &matrix_summaries,
        },
    )?;

    let manifest = RunManifest {
        status: if failures.is_empty() {
            "complete".into()
        } else {
            "complete_with_failures".into()
        },
        command: params.command.clone(),
        params: params.clone(),
        snapshot: json!({
            "inputs": params.inputs,
            "rows_loaded": store.rows_loaded,
            "chains": store.chains,
            "contracts": store.snapshot_contract_count().max(store.contracts.len() as u64),
            "nfts": store.snapshot_nft_count(),
        }),
        seeds: RunManifestSeeds {
            selected: selected_seeds.len() as u64,
            analyzed: ok_reports.len() as u64,
            failed: count_failed_seeds(&failures),
        },
        completeness: json!({
            "seed_result_ratio": if selected_seeds.is_empty() {
                None
            } else {
                Some(ok_reports.len() as f64 / selected_seeds.len() as f64)
            },
            "api_failures_do_not_exclude_results": true,
        }),
        pricing_policy: "alchemy_spot_runtime_usd_only_cross_chain".into(),
        stage_timings: json!([]),
        output_layout: json!({
            "intermediate": super::layout::INTERMEDIATE_DIR,
            "detail": super::layout::DETAIL_DIR,
            "summary": super::layout::SUMMARY_DIR,
            "scopes": [
                "intra_chain/<chain>",
                SCOPE_INTRA_CHAIN,
                "chain_pairs/<primary>_to_<secondary>",
                SCOPE_CHAIN_MATRIX,
                "cross_chain_by_source/<primary>",
                SCOPE_LABEL_CROSS_CHAIN,
                SCOPE_LABEL_ALL_CHAINS,
            ],
        }),
    };
    write_json(
        &intermediate_path(output_dir, "run_manifest.json"),
        &manifest,
    )?;
    write_failures_jsonl(&intermediate_path(output_dir, "failures.jsonl"), &failures)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{
        BehaviorFacts, EconomicContribution, EconomicFacts, EconomicsQuality, GasContribution,
        LegitClassification, LifecycleFacts, ValueFlowContribution, ValueFlowFacts,
    };
    use crate::enrich::{EvidenceQuality, EvidenceStatus, ValueFlowKind};
    use crate::entity::SourceOrder;
    use crate::reporting::aggregate::SeedDuplicateScale;
    use crate::reporting::json::SeedRelationJson;

    fn empty_analysis(chain: &str, address: &str, cid: ContractId) -> CandidateAnalysis {
        CandidateAnalysis {
            contract_id: cid,
            chain: chain.into(),
            address: address.into(),
            legit: LegitClassification {
                is_legit_duplicate: false,
                verification_complete: false,
                evidence_keys: vec![],
                reasons: vec![],
            },
            legit_by_seed: Default::default(),
            attribution: vec![],
            lifecycle: LifecycleFacts::default(),
            value_flow: ValueFlowFacts::default(),
            behaviors: BehaviorFacts::default(),
            behavior_instances: vec![],
            economics: EconomicFacts {
                operator_output_usd: 10.0,
                honest_loss_usd: 3.0,
                gross_revenue_usd: 12.0,
                ..EconomicFacts::default()
            },
            economics_quality: EconomicsQuality {
                gas: EvidenceStatus::NotRequested,
                value_flows: EvidenceStatus::NotRequested,
                notes: vec![],
            },
            evidence_quality: crate::enrich::EvidenceQuality::default(),
            collection_nft_count: None,
            collection_nft_count_complete: false,
            analysis_timestamp: 0,
        }
    }

    #[test]
    fn summary_has_rewrite_design_keys_and_usd_only_economics() {
        let seed = SeedRecord {
            chain: "ethereum".into(),
            address: "0xseed".into(),
            rank: Some(1),
        };
        let report = SeedFullReport {
            dedup: SeedDedupReport {
                seed: seed.clone(),
                hit_edge_count: 1,
                candidate_contract_count: 1,
                relations: vec![SeedRelationJson {
                    candidate_chain: "base".into(),
                    candidate_address: "0xcand".into(),
                    dimensions: vec!["token_uri".into()],
                    nft_count: 1,
                    hit_edge_count: 1,
                    nft_ids: vec![1],
                }],
                duplicate_scale: SeedDuplicateScale::default(),
            },
            scopes_complete: true,
            analysis_complete: true,
            analysis: Some(SeedAnalysisRollup {
                analyzed_candidate_count: 1,
                suspected_duplicate_contract_count: 1,
                legit_duplicate_contract_count: 0,
                infringing_nft_count: 1,
                malicious_address_count: 0,
                honest_address_count: 0,
                overlapping_role_address_count: 0,
                economics_usd: EconomicsUsdRollup {
                    operator_output_usd: 10.0,
                    honest_loss_usd: 3.0,
                    secondary_sale_loss_usd: 3.0,
                    paid_mint_loss_usd: 0.0,
                    gross_revenue_usd: 12.0,
                    ..Default::default()
                },
                candidate_refs: vec![CandidateRef {
                    chain: "base".into(),
                    address: "0xcand".into(),
                    is_legit_duplicate: false,
                    path: "detail/candidates/base__0xcand.json".into(),
                }],
            }),
        };
        let analysis = empty_analysis("base", "0xcand", 1);
        let summary = build_run_summary(&[seed], &[&report], &[], &[], &[&analysis]);
        for key in [
            "selected_seed_count",
            "seed_with_duplicate_count",
            "seed_duplicate_ratio",
            "representative_candidate_count",
            "candidate_contract_count",
            "suspected_duplicate_contract_count",
            "legit_duplicate_contract_count",
            "infringing_nft_count",
            "address_classification",
            "behaviors",
            "behavior_contract_count",
            "wash_cycle_size_distribution",
            "economics",
            "data_quality",
            "scope_summary",
        ] {
            assert!(summary.get(key).is_some(), "missing summary key {key}");
        }
        assert_eq!(summary["behavior_analyzable_contract_count"], 0);
        assert_eq!(
            summary["behaviors"]["pump_and_exit"]["contract_coverage_ratio"],
            Value::Null
        );
        for removed in [
            "analyzed_seed_count",
            "incomplete_seed_count",
            "failed_seed_count",
            "seed_completion_ratio",
        ] {
            assert!(summary.get(removed).is_none(), "obsolete key {removed}");
        }
        let econ = &summary["economics"];
        assert!(econ.get("operator_output_usd").is_some());
        assert!(econ.get("honest_paid_exposure_usd").is_some());
        assert!(econ.get("setup_gas_usd").is_some());
        assert!(econ.get("stuck_nft_count").is_some());
        assert!(econ.get("operator_output_native").is_none());
        assert!(econ.get("honest_loss_native").is_none());
        let encoded = serde_json::to_string(&summary).unwrap();
        assert!(!encoded.contains("_native"));
        assert!(!encoded.contains("_loss"));
        assert!(!encoded.contains("gross_revenue"));
        assert_eq!(econ["operator_output_usd"], 10.0);
        assert_eq!(
            summary["address_classification"]["malicious_address_count"],
            0
        );
        assert!(summary["behaviors"].get("wash_trading").is_some());
        assert!(summary["behaviors"].get("total").is_some());
        assert!(summary["wash_cycle_size_distribution"].as_array().is_some());
    }

    #[allow(clippy::too_many_arguments)] // Compact test fixture builder.
    fn formal_seed_sharing_candidate(
        seed_chain: &str,
        seed_addr: &str,
        cand_chain: &str,
        cand_addr: &str,
        economics: EconomicsUsdRollup,
        infringing_nft_count: u64,
        nft_ids: Vec<u32>,
        dimensions: Vec<String>,
        is_legit: bool,
    ) -> SeedFullReport {
        SeedFullReport {
            dedup: SeedDedupReport {
                seed: SeedRecord {
                    chain: seed_chain.into(),
                    address: seed_addr.into(),
                    rank: Some(1),
                },
                hit_edge_count: 1,
                candidate_contract_count: 1,
                relations: vec![SeedRelationJson {
                    candidate_chain: cand_chain.into(),
                    candidate_address: cand_addr.into(),
                    dimensions,
                    nft_count: infringing_nft_count,
                    hit_edge_count: 1,
                    nft_ids,
                }],
                duplicate_scale: SeedDuplicateScale::default(),
            },
            scopes_complete: true,
            analysis_complete: true,
            analysis: Some(SeedAnalysisRollup {
                analyzed_candidate_count: 1,
                suspected_duplicate_contract_count: if is_legit { 0 } else { 1 },
                legit_duplicate_contract_count: if is_legit { 1 } else { 0 },
                infringing_nft_count: if is_legit { 0 } else { infringing_nft_count },
                malicious_address_count: 0,
                honest_address_count: 0,
                overlapping_role_address_count: 0,
                economics_usd: economics.clone(),
                candidate_refs: vec![CandidateRef {
                    chain: cand_chain.into(),
                    address: cand_addr.into(),
                    is_legit_duplicate: is_legit,
                    path: format!("detail/candidates/{cand_chain}__{cand_addr}.json"),
                }],
            }),
        }
    }

    #[test]
    fn stuck_ratio_uses_resident_complete_contract_population() {
        let mut store = ResidentStore::new();
        for (row, token_id) in ["1", "2", "3", "4"].into_iter().enumerate() {
            store
                .ingest_identity_strs(
                    "base",
                    "0xcand",
                    token_id,
                    "",
                    "",
                    "",
                    SourceOrder {
                        file_ordinal: 0,
                        file_row_number: row as u64,
                    },
                )
                .unwrap();
        }
        store.rebuild_contract_nft_csr();
        store.shrink_identity_for_analysis();

        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![0],
            vec!["token_uri".into()],
            false,
        );
        let mut analysis = empty_analysis("base", "0xcand", 0);
        analysis.economics.stuck_nft_count = 2;
        let summary = build_run_summary_for_scope_with_store(
            std::slice::from_ref(&report.dedup.seed),
            &[&report],
            &[],
            &[],
            &[&analysis],
            Some(&store),
            RunSummaryScope::All,
        );

        assert_eq!(summary["economics"]["hit_contract_nft_count"], 4);
        assert_eq!(summary["economics"]["stuck_nft_count"], 2);
        assert_eq!(summary["economics"]["stuck_nft_ratio"], 0.5);
        assert_eq!(
            summary["economics"]["hit_contract_nft_count_complete"],
            true
        );
        assert_eq!(summary["infringing_nft_count"], 1);
    }

    #[test]
    fn stuck_ratio_prefers_complete_provider_population_over_resident_snapshot() {
        let mut store = ResidentStore::new();
        for (row, token_id) in ["1", "2", "3", "4"].into_iter().enumerate() {
            store
                .ingest_identity_strs(
                    "base",
                    "0xcand",
                    token_id,
                    "",
                    "",
                    "",
                    SourceOrder {
                        file_ordinal: 0,
                        file_row_number: row as u64,
                    },
                )
                .unwrap();
        }
        store.rebuild_contract_nft_csr();
        store.shrink_identity_for_analysis();

        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![0],
            vec!["token_uri".into()],
            false,
        );
        let mut analysis = empty_analysis("base", "0xcand", 0);
        analysis.collection_nft_count = Some(10);
        analysis.collection_nft_count_complete = true;
        analysis.economics.stuck_nft_count = 2;
        let summary = build_run_summary_for_scope_with_store(
            std::slice::from_ref(&report.dedup.seed),
            &[&report],
            &[],
            &[],
            &[&analysis],
            Some(&store),
            RunSummaryScope::All,
        );

        assert_eq!(summary["economics"]["hit_contract_nft_count"], 10);
        assert_eq!(summary["economics"]["stuck_nft_ratio"], 0.2);
        assert_eq!(
            summary["economics"]["hit_contract_nft_count_complete"],
            true
        );
    }

    #[test]
    fn stuck_ratio_rejects_complete_population_smaller_than_stuck_nfts() {
        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![0],
            vec!["token_uri".into()],
            false,
        );
        let mut analysis = empty_analysis("base", "0xcand", 0);
        analysis.collection_nft_count = Some(1);
        analysis.collection_nft_count_complete = true;
        analysis.economics.stuck_nft_count = 2;
        let summary = build_run_summary(
            std::slice::from_ref(&report.dedup.seed),
            &[&report],
            &[],
            &[],
            &[&analysis],
        );

        assert_eq!(summary["economics"]["hit_contract_nft_count"], 1);
        assert_eq!(summary["economics"]["stuck_nft_ratio"], Value::Null);
        assert_eq!(summary["economics"]["stuck_nft_ratio_valid"], false);
        assert_eq!(
            summary["economics"]["hit_contract_nft_count_complete"],
            false
        );
        assert_eq!(
            summary["data_quality"]["hit_contract_nft_count_complete"],
            false
        );
    }

    #[test]
    fn stuck_ratio_is_unavailable_without_provider_complete_population() {
        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![0],
            vec!["token_uri".into()],
            false,
        );
        let mut analysis = empty_analysis("base", "0xcand", 0);
        analysis.economics.stuck_nft_count = 2;
        let summary = build_run_summary(
            std::slice::from_ref(&report.dedup.seed),
            &[&report],
            &[],
            &[],
            &[&analysis],
        );

        assert_eq!(summary["economics"]["stuck_nft_ratio"], Value::Null);
        assert_eq!(summary["economics"]["stuck_nft_ratio_valid"], false);
        assert_eq!(
            summary["economics"]["hit_contract_nft_count_complete"],
            false
        );
    }

    #[test]
    fn disabled_dedup_dimension_is_null_not_zero() {
        let mut summary = json!({
            "data_quality": {},
            "duplicate_scale": [{
                "category": "name",
                "duplicate_nft_count": 0,
                "duplicate_nft_ratio": 0.0,
                "duplicate_nft_ratio_numerator": 0,
                "duplicate_nft_ratio_denominator": 10,
                "duplicate_contract_count": 0,
                "duplicate_contract_ratio": 0.0,
                "duplicate_contract_ratio_numerator": 0,
                "duplicate_contract_ratio_denominator": 5
            }]
        });
        apply_dedup_dimensions(&mut summary, &json!({"name_enabled": false}));
        assert_eq!(summary["duplicate_scale"][0]["enabled"], false);
        assert_eq!(
            summary["duplicate_scale"][0]["duplicate_nft_count"],
            Value::Null
        );
        assert_eq!(
            summary["duplicate_scale"][0]["duplicate_contract_ratio"],
            Value::Null
        );
    }

    #[test]
    fn aggregate_ratio_uses_only_the_same_ratio_eligible_contracts() {
        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_a",
            "base",
            "0xcand_a",
            EconomicsUsdRollup::default(),
            1,
            vec![10],
            vec!["token_uri".into()],
            false,
        );
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_b",
            "base",
            "0xcand_b",
            EconomicsUsdRollup::default(),
            1,
            vec![11],
            vec!["token_uri".into()],
            false,
        );
        let mut eligible = empty_analysis("base", "0xcand_a", 1);
        eligible.economics.operator_output_usd = 0.5;
        eligible.economics.attacker_input_usd = Some(1.0);
        eligible.economics.output_input_ratio = Some(0.5);
        eligible.economics.output_input_ratio_is_usd = true;
        eligible.economics.economic_contributions = vec![EconomicContribution {
            tx_hash: "0xeligible".into(),
            token_id: "1".into(),
            from: "0xbuyer".into(),
            to: "0xoperator".into(),
            kind: EconomicContributionKind::OperatorSaleProceeds,
            usd: 0.5,
        }];
        eligible.economics.gas_contributions = vec![GasContribution {
            tx_hash: "0xeligible-gas".into(),
            stage: GasStage::Setup,
            usd: 1.0,
        }];
        let mut ineligible = empty_analysis("base", "0xcand_b", 2);
        ineligible.economics.operator_output_usd = 100.0;
        ineligible.economics.attacker_input_usd = Some(1.0);
        ineligible.economics.output_input_ratio = None;
        ineligible.economics.output_input_ratio_is_usd = false;
        ineligible.economics.economic_contributions = vec![EconomicContribution {
            tx_hash: "0xineligible".into(),
            token_id: "2".into(),
            from: "0xbuyer".into(),
            to: "0xoperator".into(),
            kind: EconomicContributionKind::OperatorSaleProceeds,
            usd: 100.0,
        }];
        let selected = vec![report_a.dedup.seed.clone(), report_b.dedup.seed.clone()];
        let summary = build_run_summary(
            &selected,
            &[&report_a, &report_b],
            &[],
            &[],
            &[&eligible, &ineligible],
        );
        let economics = &summary["economics"];

        assert_eq!(economics["all_observed_operator_output_usd"], 100.5);
        assert_eq!(economics["ratio_eligible_operator_output_usd"], 0.5);
        assert_eq!(economics["ratio_eligible_attacker_input_usd"], 1.0);
        assert_eq!(economics["output_input_ratio"], 0.5);
        assert_eq!(economics["output_input_ratio_lt1_count"], 1);
        assert_eq!(economics["output_input_ratio_ge1_count"], 0);
        assert_eq!(
            economics["complete_evidence_output_input_ratio"],
            Value::Null
        );
        assert_eq!(economics["complete_evidence_output_input_ratio_count"], 0);
    }

    #[test]
    fn complete_evidence_ratio_excludes_truncated_observed_candidate() {
        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_a",
            "base",
            "0xcand_a",
            EconomicsUsdRollup::default(),
            1,
            vec![10],
            vec!["token_uri".into()],
            false,
        );
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_b",
            "base",
            "0xcand_b",
            EconomicsUsdRollup::default(),
            1,
            vec![11],
            vec!["token_uri".into()],
            false,
        );
        let mut complete = empty_analysis("base", "0xcand_a", 1);
        complete.evidence_quality = EvidenceQuality {
            transfers: EvidenceStatus::Complete,
            sales: EvidenceStatus::Empty,
            holders: EvidenceStatus::Complete,
            prices: EvidenceStatus::Complete,
            gas: EvidenceStatus::Complete,
            value_flows: EvidenceStatus::Complete,
            ..EvidenceQuality::default()
        };
        complete.economics.operator_output_usd = 0.5;
        complete.economics.attacker_input_usd = Some(1.0);
        complete.economics.output_input_ratio = Some(0.5);
        complete.economics.output_input_ratio_is_usd = true;
        complete.economics.economic_contributions = vec![EconomicContribution {
            tx_hash: "0xcomplete".into(),
            token_id: "1".into(),
            from: "0xbuyer".into(),
            to: "0xoperator".into(),
            kind: EconomicContributionKind::OperatorSaleProceeds,
            usd: 0.5,
        }];
        complete.economics.gas_contributions = vec![GasContribution {
            tx_hash: "0xcomplete-gas".into(),
            stage: GasStage::Setup,
            usd: 1.0,
        }];

        let mut partial = empty_analysis("base", "0xcand_b", 2);
        partial.evidence_quality = complete.evidence_quality.clone();
        partial.evidence_quality.gas = EvidenceStatus::Truncated;
        partial
            .evidence_quality
            .truncation_reasons
            .push("gas:receipt_partial".into());
        partial.economics.operator_output_usd = 100.0;
        partial.economics.attacker_input_usd = Some(1.0);
        partial.economics.output_input_ratio = Some(100.0);
        partial.economics.output_input_ratio_is_usd = true;
        partial.economics.economic_contributions = vec![EconomicContribution {
            tx_hash: "0xpartial".into(),
            token_id: "2".into(),
            from: "0xbuyer".into(),
            to: "0xoperator".into(),
            kind: EconomicContributionKind::OperatorSaleProceeds,
            usd: 100.0,
        }];
        partial.economics.gas_contributions = vec![GasContribution {
            tx_hash: "0xpartial-gas".into(),
            stage: GasStage::Setup,
            usd: 1.0,
        }];

        let selected = vec![report_a.dedup.seed.clone(), report_b.dedup.seed.clone()];
        let summary = build_run_summary(
            &selected,
            &[&report_a, &report_b],
            &[],
            &[],
            &[&complete, &partial],
        );
        let economics = &summary["economics"];
        assert_eq!(economics["observed_output_input_ratio"], 50.25);
        assert_eq!(economics["output_input_ratio_count"], 2);
        assert_eq!(economics["complete_evidence_output_input_ratio"], 0.5);
        assert_eq!(economics["complete_evidence_output_input_ratio_count"], 1);
        assert_eq!(
            economics["complete_evidence_output_input_ratio_lt1_count"],
            1
        );
        assert_eq!(
            summary["data_quality"]["truncation_reason_counts"]["gas:receipt_partial"],
            1
        );
    }

    #[test]
    fn summary_economics_count_shared_candidate_once() {
        let econ = EconomicsUsdRollup {
            operator_output_usd: 10.0,
            honest_loss_usd: 3.0,
            secondary_sale_loss_usd: 3.0,
            paid_mint_loss_usd: 0.0,
            gross_revenue_usd: 12.0,
            ..Default::default()
        };
        // Two formal seeds share one candidate; per-seed rollups each carry the full USD.
        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_a",
            "base",
            "0xcand",
            econ.clone(),
            2,
            vec![10, 11],
            vec!["token_uri".into()],
            false,
        );
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_b",
            "base",
            "0xcand",
            econ,
            2,
            vec![10, 11],
            vec!["token_uri".into()],
            false,
        );
        let analysis = empty_analysis("base", "0xcand", 1);
        let selected = vec![report_a.dedup.seed.clone(), report_b.dedup.seed.clone()];
        let summary = build_run_summary(&selected, &[&report_a, &report_b], &[], &[], &[&analysis]);
        let economics = &summary["economics"];
        // Must equal the unique CandidateAnalysis once — not 2× per-seed rollups.
        assert_eq!(economics["operator_output_usd"], 10.0);
        assert_eq!(economics["honest_paid_exposure_usd"], 3.0);
        assert_eq!(economics["gross_sales_volume_usd"], 12.0);
        assert_eq!(
            summary["scope_summary"]["economics_usd"]["operator_output_usd"],
            10.0
        );
        assert_eq!(summary["infringing_nft_count"], 2);
        assert_eq!(summary["candidate_contract_count"], 1);
    }

    #[test]
    fn summary_reports_addresses_with_overlapping_roles() {
        use crate::analysis::{AddressAttribution, AddressRole};

        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_a",
            "base",
            "0xcand_a",
            EconomicsUsdRollup::default(),
            1,
            vec![10],
            vec!["token_uri".into()],
            false,
        );
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_b",
            "base",
            "0xcand_b",
            EconomicsUsdRollup::default(),
            1,
            vec![11],
            vec!["token_uri".into()],
            false,
        );
        let mut analysis_a = empty_analysis("base", "0xcand_a", 1);
        analysis_a.attribution = vec![(
            "0xshared".into(),
            AddressAttribution {
                role: AddressRole::SuspectedOperator,
                evidence: vec![],
            },
        )];
        let mut analysis_b = empty_analysis("base", "0xcand_b", 2);
        analysis_b.attribution = vec![(
            "0xshared".into(),
            AddressAttribution {
                role: AddressRole::LikelyVictim,
                evidence: vec![],
            },
        )];
        let selected = vec![report_a.dedup.seed.clone(), report_b.dedup.seed.clone()];
        let summary = build_run_summary(
            &selected,
            &[&report_a, &report_b],
            &[],
            &[],
            &[&analysis_a, &analysis_b],
        );
        let addresses = &summary["address_classification"];

        assert_eq!(addresses["malicious_address_count"], 1);
        assert_eq!(addresses["honest_address_count"], 0);
        assert_eq!(addresses["overlapping_role_address_count"], 1);
        assert_eq!(addresses["total_address_count"], 1);
    }

    #[test]
    fn summary_deduplicates_shared_value_flow_and_gas_transactions() {
        let empty_rollup = EconomicsUsdRollup::default();
        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_a",
            "base",
            "0xcand_a",
            empty_rollup.clone(),
            1,
            vec![10],
            vec!["token_uri".into()],
            false,
        );
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_b",
            "base",
            "0xcand_b",
            empty_rollup,
            1,
            vec![11],
            vec!["token_uri".into()],
            false,
        );
        let mut analysis_a = empty_analysis("base", "0xcand_a", 1);
        let mut analysis_b = empty_analysis("base", "0xcand_b", 2);
        for (analysis, kind) in [
            (&mut analysis_a, ValueFlowKind::Funding),
            (&mut analysis_b, ValueFlowKind::Withdrawal),
        ] {
            analysis.economics.funding_usd = 5.0;
            analysis.economics.setup_gas_usd = 2.0;
            analysis.economics.total_gas_usd = 2.0;
            analysis.economics.attacker_input_usd = Some(2.0);
            analysis.economics.output_input_ratio = Some(5.0);
            analysis.economics.output_input_ratio_is_usd = true;
            analysis
                .economics
                .value_flow_contributions
                .push(ValueFlowContribution {
                    tx_hash: "0xflow".into(),
                    event_id: Some("event:0".into()),
                    from: "0xfunder".into(),
                    to: "0xoperator".into(),
                    kind,
                    usd: Some(5.0),
                });
            analysis.economics.gas_contributions.push(GasContribution {
                tx_hash: "0xgas".into(),
                stage: GasStage::Setup,
                usd: 2.0,
            });
        }
        analysis_a
            .economics
            .value_flow_contributions
            .push(ValueFlowContribution {
                tx_hash: "0xflow".into(),
                event_id: Some("event:1".into()),
                from: "0xfunder".into(),
                to: "0xoperator".into(),
                kind: ValueFlowKind::Funding,
                usd: Some(3.0),
            });

        let selected = vec![report_a.dedup.seed.clone(), report_b.dedup.seed.clone()];
        let summary = build_run_summary(
            &selected,
            &[&report_a, &report_b],
            &[],
            &[],
            &[&analysis_a, &analysis_b],
        );
        let economics = &summary["economics"];
        // event:0 is shared across candidates and becomes revenue backflow;
        // event:1 is a distinct transfer in the same transaction and must add.
        assert_eq!(economics["funding_usd"], 3.0);
        assert_eq!(economics["withdrawal_usd"], 0.0);
        assert_eq!(economics["revenue_backflow_usd"], 5.0);
        assert_eq!(
            summary["data_quality"]["pricing"]["priced_value_flow_count"],
            2
        );
        assert_eq!(economics["setup_gas_usd"], 2.0);
        assert_eq!(economics["total_gas_usd"], 2.0);
        assert_eq!(economics["attacker_input_usd"], 2.0);
        assert_eq!(economics["ratio_eligible_attacker_input_usd"], 2.0);
        assert_eq!(economics["output_input_ratio_count"], 2);
        assert_eq!(economics["ratio_candidate_coverage_complete"], true);
        assert_eq!(economics["ratio_evidence_complete"], false);
        assert_eq!(economics["ratio_sample_complete"], false);
        assert_eq!(economics["ratio_is_observed_only"], true);
    }

    #[test]
    fn summary_deduplicates_shared_sale_and_mint_amounts() {
        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_a",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![10],
            vec!["token_uri".into()],
            false,
        );
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_b",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![11],
            vec!["token_uri".into()],
            false,
        );
        let mut analysis_a = empty_analysis("base", "0xcand", 1);
        let mut analysis_b = empty_analysis("base", "0xcand", 2);
        let contributions = [
            (
                "0xsale",
                "0xbuyer",
                "",
                EconomicContributionKind::GrossSale,
                100.0,
            ),
            (
                "0xsale",
                "0xbuyer",
                "",
                EconomicContributionKind::MarketplaceFee,
                10.0,
            ),
            (
                "0xsale",
                "0xbuyer",
                "0xcreator",
                EconomicContributionKind::RoyaltyFee,
                10.0,
            ),
            (
                "0xsale",
                "0xbuyer",
                "0xoperator",
                EconomicContributionKind::OperatorSaleProceeds,
                80.0,
            ),
            (
                "0xsale",
                "0xbuyer",
                "0xcreator",
                EconomicContributionKind::OperatorRoyalty,
                10.0,
            ),
            (
                "0xsale",
                "0xbuyer",
                "0xoperator",
                EconomicContributionKind::HonestSecondaryExposure,
                100.0,
            ),
            (
                "0xmint",
                "0xbuyer",
                "0xoperator",
                EconomicContributionKind::OperatorMintPayment,
                20.0,
            ),
            (
                "0xmint",
                "0xbuyer",
                "0xoperator",
                EconomicContributionKind::HonestMintExposure,
                20.0,
            ),
        ];
        for analysis in [&mut analysis_a, &mut analysis_b] {
            analysis.economics.economic_contributions = contributions
                .iter()
                .map(|(tx_hash, from, to, kind, usd)| EconomicContribution {
                    tx_hash: (*tx_hash).into(),
                    token_id: "1".into(),
                    from: (*from).into(),
                    to: (*to).into(),
                    kind: *kind,
                    usd: *usd,
                })
                .collect();
        }

        let selected = vec![report_a.dedup.seed.clone(), report_b.dedup.seed.clone()];
        let summary = build_run_summary(
            &selected,
            &[&report_a, &report_b],
            &[],
            &[],
            &[&analysis_a, &analysis_b],
        );
        let economics = &summary["economics"];
        assert_eq!(economics["gross_sales_volume_usd"], 100.0);
        assert_eq!(economics["marketplace_fee_usd"], 10.0);
        assert_eq!(economics["royalty_fee_usd"], 10.0);
        assert_eq!(economics["operator_royalty_usd"], 10.0);
        assert_eq!(economics["operator_output_usd"], 110.0);
        assert_eq!(economics["secondary_sale_paid_exposure_usd"], 100.0);
        assert_eq!(economics["paid_mint_exposure_usd"], 20.0);
        assert_eq!(economics["honest_paid_exposure_usd"], 120.0);
    }

    #[test]
    fn summary_keeps_equal_sales_for_distinct_tokens() {
        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![10, 11],
            vec!["token_uri".into()],
            false,
        );
        let mut analysis = empty_analysis("base", "0xcand", 1);
        analysis.economics.economic_contributions = ["1", "2"]
            .into_iter()
            .map(|token_id| EconomicContribution {
                tx_hash: "0xbatch".into(),
                token_id: token_id.into(),
                from: "0xseller".into(),
                to: "0xbuyer".into(),
                kind: EconomicContributionKind::GrossSale,
                usd: 100.0,
            })
            .collect();

        let summary = build_run_summary(
            std::slice::from_ref(&report.dedup.seed),
            &[&report],
            &[],
            &[],
            &[&analysis],
        );
        assert_eq!(summary["economics"]["gross_sales_volume_usd"], 200.0);
    }

    #[test]
    fn summary_excludes_legit_duplicate_from_economics_attribution_behavior() {
        use crate::analysis::{
            AddressAttribution, AddressEvidence, AddressEvidenceKind, AddressRole,
            BehaviorInstance, BehaviorKind,
        };
        use crate::enrich::{EvidenceBundle, LegitSignals, finalize_legit_signals};

        // Plumbing: future enrich can set flags; classify → summary must exclude.
        let mut bundle = EvidenceBundle::empty(2, "base", "0xlegit");
        bundle.legit = LegitSignals {
            verified_migration: true,
            evidence_keys: vec!["migration:test".into()],
            verification_complete: true,
            ..LegitSignals::default()
        };
        finalize_legit_signals(&mut bundle);
        assert!(bundle.legit.is_legit_duplicate());

        let econ = EconomicsUsdRollup {
            operator_output_usd: 10.0,
            honest_loss_usd: 3.0,
            secondary_sale_loss_usd: 3.0,
            paid_mint_loss_usd: 0.0,
            gross_revenue_usd: 12.0,
            ..Default::default()
        };
        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed",
            "base",
            "0xlegit",
            econ,
            4,
            vec![1, 2, 3, 4],
            vec!["name".into()],
            true,
        );
        let mut analysis = empty_analysis("base", "0xlegit", 2);
        analysis.legit = LegitClassification {
            is_legit_duplicate: true,
            verification_complete: true,
            evidence_keys: vec!["migration:test".into()],
            reasons: vec!["verified_migration".into()],
        };
        analysis.economics.operator_output_usd = 99.0;
        analysis.economics.honest_loss_usd = 50.0;
        analysis.economics.gross_revenue_usd = 99.0;
        analysis.attribution = vec![(
            "0xop".into(),
            AddressAttribution {
                role: AddressRole::SuspectedOperator,
                evidence: vec![AddressEvidence {
                    evidence_type: AddressEvidenceKind::ControllerOrAuthority,
                    token_id: None,
                    transaction: None,
                    weight: 1.0,
                    confidence: 1.0,
                }],
            },
        )];
        analysis.behavior_instances = vec![BehaviorInstance {
            kind: BehaviorKind::WashTrading,
            addresses: vec!["0xop".into()],
            nfts: vec!["1".into()],
            linked_loss_usd: 7.0,
            ..BehaviorInstance::default()
        }];

        let summary = build_run_summary(
            std::slice::from_ref(&report.dedup.seed),
            &[&report],
            &[],
            &[],
            &[&analysis],
        );
        assert_eq!(summary["legit_duplicate_contract_count"], 1);
        assert_eq!(summary["suspected_duplicate_contract_count"], 0);
        assert_eq!(summary["infringing_nft_count"], 0);
        assert_eq!(summary["economics"]["operator_output_usd"], 0.0);
        assert_eq!(summary["economics"]["honest_paid_exposure_usd"], 0.0);
        assert_eq!(
            summary["address_classification"]["malicious_address_count"],
            0
        );
        assert_eq!(summary["behaviors"]["total"]["instance_count"], 0);
        assert_eq!(summary["behaviors"]["total"]["instance_ratio"], Value::Null);
        assert_eq!(summary["behaviors"]["wash_trading"]["instance_count"], 0);
    }

    #[test]
    fn non_nft_filter_removes_relation_edges_and_duplicate_scale() {
        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![7],
            vec!["token_uri".into()],
            false,
        );
        let evm = ["ethereum", "base"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let store = ResidentStore::with_options(Some(2), &evm);
        let excluded = AHashSet::from(["base:0xcand".to_owned()]);

        let filtered = filter_non_nft_report(&store, &report, &excluded);

        assert!(filtered.dedup.relations.is_empty());
        assert_eq!(filtered.dedup.candidate_contract_count, 0);
        assert_eq!(filtered.dedup.hit_edge_count, 0);
        assert_eq!(
            filtered
                .dedup
                .duplicate_scale
                .cross_chain_summary
                .iter()
                .find(|row| row.category == "total")
                .unwrap()
                .duplicate_contract_count,
            0
        );
    }

    #[test]
    fn summary_infringing_nft_unions_uri_narrow_and_name_wide_hits() {
        let econ = EconomicsUsdRollup::default();
        // Seed A: URI-narrow hit on NFTs {10, 11}
        let report_a = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_uri",
            "base",
            "0xcand",
            econ.clone(),
            2,
            vec![10, 11],
            vec!["token_uri".into()],
            false,
        );
        // Seed B: Name-wide hit expands to {10, 11, 12, 13, 14}
        let report_b = formal_seed_sharing_candidate(
            "ethereum",
            "0xseed_name",
            "base",
            "0xcand",
            econ,
            5,
            vec![10, 11, 12, 13, 14],
            vec!["name".into()],
            false,
        );
        let analysis = empty_analysis("base", "0xcand", 1);
        let selected = vec![report_a.dedup.seed.clone(), report_b.dedup.seed.clone()];
        let summary = build_run_summary(&selected, &[&report_a, &report_b], &[], &[], &[&analysis]);
        // Union of identity keys = 5, not first-wins (2) and not sum (7).
        assert_eq!(summary["infringing_nft_count"], 5);
        assert_eq!(summary["representative_candidate_count"], 5);
        assert_eq!(summary["candidate_contract_count"], 1);
    }

    #[test]
    fn mixed_legit_candidate_counts_only_suspicious_relation_nfts() {
        let report_legit = formal_seed_sharing_candidate(
            "ethereum",
            "0xlegit_seed",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            2,
            vec![1, 2],
            vec!["token_uri".into()],
            true,
        );
        let report_suspicious = formal_seed_sharing_candidate(
            "ethereum",
            "0xsuspicious_seed",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            2,
            vec![2, 3],
            vec!["metadata".into()],
            false,
        );
        let mut analysis = empty_analysis("base", "0xcand", 1);
        analysis.legit_by_seed.insert(
            "ethereum:0xlegit_seed".into(),
            LegitClassification {
                is_legit_duplicate: true,
                verification_complete: true,
                evidence_keys: vec!["official".into()],
                reasons: vec!["verified_migration".into()],
            },
        );
        analysis.legit_by_seed.insert(
            "ethereum:0xsuspicious_seed".into(),
            LegitClassification {
                is_legit_duplicate: false,
                verification_complete: true,
                evidence_keys: vec![],
                reasons: vec![],
            },
        );
        let summary = build_run_summary(
            &[
                report_legit.dedup.seed.clone(),
                report_suspicious.dedup.seed.clone(),
            ],
            &[&report_legit, &report_suspicious],
            &[],
            &[],
            &[&analysis],
        );
        assert_eq!(summary["representative_candidate_count"], 3);
        assert_eq!(summary["suspected_duplicate_contract_count"], 1);
        assert_eq!(summary["legit_duplicate_contract_count"], 0);
        assert_eq!(summary["infringing_nft_count"], 2);
    }

    #[test]
    fn cross_scope_seed_and_candidate_counts_use_only_cross_relations() {
        let intra = formal_seed_sharing_candidate(
            "ethereum",
            "0xintra",
            "ethereum",
            "0xcand_intra",
            EconomicsUsdRollup::default(),
            1,
            vec![1],
            vec!["token_uri".into()],
            false,
        );
        let mut cross = formal_seed_sharing_candidate(
            "ethereum",
            "0xcross",
            "base",
            "0xcand_cross",
            EconomicsUsdRollup::default(),
            1,
            vec![2],
            vec!["token_uri".into()],
            false,
        );
        cross.analysis_complete = false;
        let intra_analysis = empty_analysis("ethereum", "0xcand_intra", 1);
        let cross_analysis = empty_analysis("base", "0xcand_cross", 2);
        let selected = vec![intra.dedup.seed.clone(), cross.dedup.seed.clone()];
        let summary = build_run_summary_for_scope(
            &selected,
            &[&intra],
            &[&cross],
            &[],
            &[&cross_analysis, &intra_analysis],
            RunSummaryScope::Cross,
        );
        assert_eq!(summary["seed_with_duplicate_count"], 1);
        assert_eq!(summary["candidate_contract_count"], 1);
        assert_eq!(summary["representative_candidate_count"], 1);
        assert_eq!(summary["seed_duplicate_ratio"], 0.5);
    }

    #[test]
    fn candidate_report_serialization_omits_native_amount_fields() {
        let mut analysis = empty_analysis("ethereum", "0xcand", 1);
        analysis.economics.total_gas_native = 0.25;
        analysis.economics.total_gas_usd = 500.0;
        analysis.economics.honest_paid_mint_loss_count = 1;
        analysis.value_flow.gross_revenue_native = 2.0;
        analysis.behavior_instances = vec![crate::analysis::BehaviorInstance {
            native_value: 3.0,
            linked_loss_native: 1.0,
            usd_value: 6_000.0,
            linked_loss_usd: 2_000.0,
            ..Default::default()
        }];
        let encoded = serde_json::to_string(&analysis).unwrap();
        assert!(!encoded.contains("_native"));
        assert!(!encoded.contains("honest_paid_mint_loss_count"));
        assert!(encoded.contains("honest_paid_mint_exposure_count"));
        assert!(encoded.contains("total_gas_usd"));
        assert!(encoded.contains("linked_paid_exposure_usd"));
    }

    #[test]
    fn error_document_count_keeps_seed_and_candidate_rows() {
        let failures = vec![
            FailureRecord::seed_stage("ethereum", "0xfail", "resolve_seed", "missing"),
            FailureRecord::candidate_stage("base", "0xc1", "analyze_candidate", "boom"),
            FailureRecord::candidate_stage("base", "0xc2", "analyze_candidate", "boom"),
            FailureRecord::seed_stage("ethereum", "0xfail", "dedup_query", "again"),
        ];
        assert_eq!(count_failed_seeds(&failures), 1);
        assert_eq!(failures.len(), 4);

        let seed = SeedRecord {
            chain: "ethereum".into(),
            address: "0xok".into(),
            rank: Some(1),
        };
        let report = formal_seed_sharing_candidate(
            "ethereum",
            "0xok",
            "base",
            "0xcand",
            EconomicsUsdRollup::default(),
            1,
            vec![1],
            vec!["token_uri".into()],
            false,
        );
        let analysis = empty_analysis("base", "0xcand", 1);
        let summary = build_run_summary(
            &[
                seed,
                SeedRecord {
                    chain: "ethereum".into(),
                    address: "0xfail".into(),
                    rank: Some(2),
                },
            ],
            &[&report],
            &[],
            &failures,
            &[&analysis],
        );
        assert!(summary.get("failed_seed_count").is_none());
        assert!(
            summary["data_quality"]
                .get("failure_record_count")
                .is_none()
        );
    }
}
