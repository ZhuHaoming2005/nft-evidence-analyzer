//! Evidence analysis for enriched candidates.

mod attribution;
mod behavior;
mod economics;
mod graph;
mod legit;
mod lifecycle;

pub use attribution::{AddressAttribution, AddressEvidence, AddressEvidenceKind, AddressRole};
pub use behavior::{BehaviorFacts, BehaviorInstance, BehaviorKind, LinkedLossEvent};
pub use economics::{
    EconomicContribution, EconomicContributionKind, EconomicFacts, EconomicsQuality,
    GasContribution, GasStage, ValueFlowContribution,
};
pub use graph::AddressGraph;
pub use legit::LegitClassification;
pub use lifecycle::{LifecycleFacts, ValueFlowFacts};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::AnalysisError;
use crate::enrich::{EvidenceBundle, EvidenceQuality, EvidenceStatus};
use crate::entity::{ContractId, ResidentStore};

const PARALLEL_CANDIDATE_EVENT_THRESHOLD: usize = 2_048;

/// Paper / CLI knobs for behavior detectors.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperConfig {
    /// Minimum SCC size for a wash cycle (default 2).
    pub min_cycle_size: usize,
    /// Minimum distinct addresses on a layered transfer path (default 3).
    pub layered_path_addresses: usize,
    /// Minimum DAG fan-out for star centers (default 3).
    pub fan_out: usize,
    /// Top-fraction concentration threshold for aggregate reporting (default 0.10).
    pub top_concentration_fraction: f64,
    /// Unix timestamp used for holding-time windows.
    pub analysis_timestamp: i64,
}

impl Default for PaperConfig {
    fn default() -> Self {
        Self {
            min_cycle_size: 2,
            layered_path_addresses: 3,
            fan_out: 3,
            top_concentration_fraction: 0.10,
            analysis_timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

/// Per-candidate deep analysis product.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateAnalysis {
    pub contract_id: ContractId,
    pub chain: String,
    pub address: String,
    pub legit: LegitClassification,
    /// Per-seed relation classification keyed by `"chain:address"`.
    #[serde(default)]
    pub legit_by_seed: std::collections::BTreeMap<String, LegitClassification>,
    pub attribution: Vec<(String, AddressAttribution)>,
    pub lifecycle: LifecycleFacts,
    pub value_flow: ValueFlowFacts,
    pub behaviors: BehaviorFacts,
    pub behavior_instances: Vec<BehaviorInstance>,
    pub economics: EconomicFacts,
    pub economics_quality: EconomicsQuality,
    /// Provider completeness copied before the evidence bundle is released.
    #[serde(default)]
    pub evidence_quality: EvidenceQuality,
    /// Provider-derived whole-contract NFT/token-id population.
    #[serde(default)]
    pub collection_nft_count: Option<u64>,
    #[serde(default)]
    pub collection_nft_count_complete: bool,
    pub analysis_timestamp: i64,
}

impl CandidateAnalysis {
    /// Reuse contract-wide deep analysis for a reporting relation scope.
    ///
    /// Deep graph/behavior/economics facts depend on the candidate contract's
    /// events, not on which seed relation selected it. Only legitimacy changes
    /// by scope. A fully legitimate scope is projected to the same empty facts
    /// that `analyze_candidate` would have returned, without rebuilding graphs.
    pub fn project_relation_signals(
        &self,
        relation_signals: &std::collections::BTreeMap<String, crate::enrich::LegitSignals>,
    ) -> Self {
        let mut projected = self.clone();
        let mut merged = crate::enrich::LegitSignals::default();
        let mut any_complete = false;
        for signals in relation_signals.values() {
            merged.merge_or(signals);
            any_complete |= signals.verification_complete;
        }
        if merged.is_legit_duplicate() || any_complete {
            merged.verification_complete = true;
        }
        projected.legit = legit::classify(&merged);
        projected.legit_by_seed = relation_signals
            .iter()
            .map(|(key, signals)| (key.clone(), legit::classify(signals)))
            .collect();

        let fully_legit = !projected.legit_by_seed.is_empty()
            && projected
                .legit_by_seed
                .values()
                .all(|relation| relation.is_legit_duplicate);
        if fully_legit {
            projected.attribution.clear();
            projected.lifecycle = LifecycleFacts::default();
            projected.value_flow = ValueFlowFacts::default();
            projected.behaviors = BehaviorFacts::default();
            projected.behavior_instances.clear();
            projected.economics = EconomicFacts::default();
            projected.economics_quality = EconomicsQuality::default();
        }
        projected
    }

    /// Whether every required evidence family is complete enough to interpret
    /// the observed USD/behavior values as complete. Partial analyses still
    /// contribute their available results; this flag is quality metadata only.
    pub fn has_complete_evidence(&self) -> bool {
        if self.evidence_quality.excluded_non_nft {
            return true;
        }
        if !self.legit_by_seed.is_empty()
            && self
                .legit_by_seed
                .values()
                .all(|relation| relation.is_legit_duplicate)
        {
            return self
                .legit_by_seed
                .values()
                .all(|relation| relation.verification_complete);
        }
        let complete = |status| matches!(status, EvidenceStatus::Complete | EvidenceStatus::Empty);
        let base_complete = complete(self.evidence_quality.transfers)
            && complete(self.evidence_quality.sales)
            && complete(self.evidence_quality.holders)
            && complete(self.evidence_quality.prices)
            && complete(self.evidence_quality.gas)
            && complete(self.evidence_quality.value_flows);
        let chain_complete = !self.chain.eq_ignore_ascii_case("solana")
            || (complete(self.evidence_quality.assets)
                && complete(self.evidence_quality.histories));
        base_complete
            && chain_complete
            && self.economics.unpriced_sale_count == 0
            && self.economics.amountless_sale_count == 0
            && self.economics.assumed_stablecoin_peg_sale_count == 0
            && self.economics.unpriced_operator_sale_proceeds_count == 0
            && self.economics.unknown_operator_sale_proceeds_count == 0
            && self.economics.unknown_royalty_recipient_count == 0
            && self.economics.unpriced_operator_paid_mint_payment_count == 0
            && self.economics.unknown_paid_mint_receiver_count == 0
            && self.economics.unpriced_honest_paid_mint_loss_count == 0
            && self.economics.unpriced_value_flow_count == 0
    }

    /// Drop detail retained only for per-candidate JSON on disk.
    ///
    /// Keeps fields needed by seed rollups and batch `summary` (roles, behavior
    /// instance identities / linked paid exposure, economics). Safe to call **after**
    /// `write_candidate_json`.
    pub fn shrink_for_summary_memory(&mut self) {
        for (_addr, attr) in &mut self.attribution {
            attr.evidence.clear();
            attr.evidence.shrink_to_fit();
        }
        // Lifecycle / value-flow timelines are fully on disk; summary uses economics.
        self.lifecycle = LifecycleFacts::default();
        self.value_flow = ValueFlowFacts::default();
        for inst in &mut self.behavior_instances {
            // Summary needs kind, addresses, nfts, linked buyers, and linked paid exposure.
            inst.transactions.clear();
            inst.transactions.shrink_to_fit();
            inst.gini_nft_count = None;
            inst.gini_token_transaction_count = None;
            inst.fan_out = None;
            inst.path_length = None;
            inst.low_value_hops = None;
            inst.source_address_count = None;
            inst.nft_share = None;
            inst.value_share = None;
            inst.exit_delay_seconds = None;
            inst.exit_to_internal_price_ratio = None;
            inst.exit_to_cycle_nft_ratio = None;
            inst.start_block = None;
            inst.end_block = None;
            inst.start_timestamp = None;
            inst.end_timestamp = None;
            inst.native_value = 0.0;
            inst.usd_value = 0.0;
            inst.linked_loss_native = 0.0;
        }
    }
}

/// Run deep analysis for one candidate using resident identity + evidence bundle.
pub fn analyze_candidate(
    store: &ResidentStore,
    contract: ContractId,
    evidence: &EvidenceBundle,
    cfg: &PaperConfig,
) -> Result<CandidateAnalysis, AnalysisError> {
    let Some(contract_row) = store.contracts.get(contract as usize) else {
        return Err(AnalysisError::invalid(format!(
            "unknown contract id {contract}"
        )));
    };
    if evidence.contract_id != contract {
        return Err(AnalysisError::invalid(format!(
            "evidence contract_id {} != requested {contract}",
            evidence.contract_id
        )));
    }

    let legit = legit::classify(&evidence.legit);
    let legit_by_seed: std::collections::BTreeMap<String, LegitClassification> = evidence
        .relation_legit
        .iter()
        .map(|(k, v)| (k.clone(), legit::classify(v)))
        .collect();
    let fully_legit = !legit_by_seed.is_empty()
        && legit_by_seed
            .values()
            .all(|relation| relation.is_legit_duplicate);
    if fully_legit {
        return Ok(CandidateAnalysis {
            contract_id: contract,
            chain: store.chain_name(contract_row.chain_id).to_owned(),
            address: contract_row.address.clone(),
            legit,
            legit_by_seed,
            attribution: Vec::new(),
            lifecycle: LifecycleFacts::default(),
            value_flow: ValueFlowFacts::default(),
            behaviors: BehaviorFacts::default(),
            behavior_instances: Vec::new(),
            economics: EconomicFacts::default(),
            economics_quality: EconomicsQuality::default(),
            evidence_quality: evidence.quality.clone(),
            collection_nft_count: evidence.collection_nft_count,
            collection_nft_count_complete: evidence.collection_nft_count_complete,
            analysis_timestamp: cfg.analysis_timestamp,
        });
    }
    let transfer_graph = graph::AddressGraph::from_transfers(&evidence.transfers);
    let event_work = evidence
        .transfers
        .len()
        .saturating_add(evidence.sales.len())
        .saturating_add(evidence.holders.len())
        .saturating_add(evidence.value_flows.len());
    let parallel =
        event_work >= PARALLEL_CANDIDATE_EVENT_THRESHOLD && rayon::current_num_threads() > 1;
    let (transfer_sccs, attribution) = if parallel {
        rayon::join(
            || transfer_graph.strongly_connected_components(),
            || attribution::attribute_addresses(evidence, &transfer_graph),
        )
    } else {
        (
            transfer_graph.strongly_connected_components(),
            attribution::attribute_addresses(evidence, &transfer_graph),
        )
    };
    let lifecycle =
        lifecycle::build_lifecycle(evidence, &attribution.roles, cfg.analysis_timestamp);
    let (value_flow, detected, economics, economics_quality) = if parallel {
        let (value_flow, (detected, (economics, economics_quality))) = rayon::join(
            || lifecycle::build_value_flow(evidence, &attribution.roles),
            || {
                rayon::join(
                    || {
                        behavior::detect_behaviors(
                            evidence,
                            &transfer_graph,
                            &transfer_sccs,
                            &attribution.roles,
                            cfg,
                        )
                    },
                    || {
                        economics::compute_economics(
                            evidence,
                            &attribution.roles,
                            cfg.analysis_timestamp,
                            &lifecycle,
                        )
                    },
                )
            },
        );
        (value_flow, detected, economics, economics_quality)
    } else {
        let value_flow = lifecycle::build_value_flow(evidence, &attribution.roles);
        let detected = behavior::detect_behaviors(
            evidence,
            &transfer_graph,
            &transfer_sccs,
            &attribution.roles,
            cfg,
        );
        let (economics, economics_quality) = economics::compute_economics(
            evidence,
            &attribution.roles,
            cfg.analysis_timestamp,
            &lifecycle,
        );
        (value_flow, detected, economics, economics_quality)
    };

    let mut attribution_rows: Vec<(String, AddressAttribution)> =
        attribution.records.into_iter().collect();
    if attribution_rows.len() >= PARALLEL_CANDIDATE_EVENT_THRESHOLD {
        attribution_rows.par_sort_by(|left, right| left.0.cmp(&right.0));
    } else {
        attribution_rows.sort_by(|left, right| left.0.cmp(&right.0));
    }

    Ok(CandidateAnalysis {
        contract_id: contract,
        chain: store.chain_name(contract_row.chain_id).to_owned(),
        address: contract_row.address.clone(),
        legit,
        legit_by_seed,
        attribution: attribution_rows,
        lifecycle,
        value_flow,
        behaviors: detected.facts,
        behavior_instances: detected.instances,
        economics,
        economics_quality,
        evidence_quality: evidence.quality.clone(),
        collection_nft_count: evidence.collection_nft_count,
        collection_nft_count_complete: evidence.collection_nft_count_complete,
        analysis_timestamp: cfg.analysis_timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::{
        EvidenceQuality, EvidenceStatus, HolderRecord, LegitSignals, SaleEvent, TransferEvent,
        ValueFlowEdge, ValueFlowKind,
    };
    use crate::entity::{IdentityRow, SourceOrder};

    fn store_with_contract(chain: &str, address: &str) -> (ResidentStore, ContractId) {
        let mut store = ResidentStore::new();
        store
            .ingest_identity_row(IdentityRow {
                chain: chain.to_owned(),
                contract_address: address.to_owned(),
                token_id: "1".into(),
                name_norm: String::new(),
                token_uri_norm: String::new(),
                image_uri_norm: String::new(),
                source_order: SourceOrder {
                    file_ordinal: 0,
                    file_row_number: 0,
                },
            })
            .unwrap();
        let contract = store.contract_id(chain, address).unwrap();
        (store, contract)
    }

    fn sale(tx: &str, token: &str, seller: &str, buyer: &str, ts: i64, usd: f64) -> SaleEvent {
        SaleEvent {
            tx_hash: tx.into(),
            token_id: token.into(),
            seller: seller.into(),
            buyer: buyer.into(),
            timestamp: Some(ts),
            block_number: Some(ts as u64),
            marketplace: None,
            native_amount: Some(usd),
            usd_amount: Some(usd),
            currency_symbol: Some("ETH".into()),
            currency_address: None,
            seller_proceeds_native: Some(usd),
            seller_proceeds_usd: Some(usd),
            ..SaleEvent::default()
        }
    }

    fn transfer(
        tx: &str,
        token: &str,
        from: &str,
        to: &str,
        ts: i64,
        is_mint: bool,
    ) -> TransferEvent {
        TransferEvent {
            tx_hash: tx.into(),
            token_id: token.into(),
            from: from.into(),
            to: to.into(),
            timestamp: Some(ts),
            block_number: Some(ts as u64),
            is_mint,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }
    }

    #[test]
    fn wash_cycle_two_node_scc_from_reciprocal_malicious_sales() {
        let (store, contract) = store_with_contract("ethereum", "0xcand");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xcand");
        // Checksum-style controller vs lowercased sale counterparties.
        evidence.controllers = vec!["0xA".into()];
        evidence.sales = vec![
            sale("tx-0", "1", "0xa", "0xb", 10, 1.0),
            sale("tx-1", "1", "0xb", "0xa", 20, 1.0),
        ];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Empty;
        evidence.quality.holders = EvidenceStatus::Empty;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 100,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.behaviors.wash_cycles, 1);
        let wash = analysis
            .behavior_instances
            .iter()
            .find(|instance| instance.kind == BehaviorKind::WashTrading)
            .expect("wash instance");
        assert_eq!(wash.addresses, vec!["0xa".to_owned(), "0xb".to_owned()]);
        assert_eq!(wash.edge_count, 2);
        assert!(matches!(
            analysis
                .attribution
                .iter()
                .find(|(addr, _)| addr == "0xb")
                .map(|(_, row)| row.role),
            Some(AddressRole::SuspectedOperator)
        ));
    }

    #[test]
    fn legit_duplicate_excludes_from_malicious_flag() {
        let (store, contract) = store_with_contract("ethereum", "0xlegit");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xlegit");
        evidence.legit = LegitSignals {
            verified_migration: true,
            evidence_keys: vec!["migration:official".into()],
            verification_complete: true,
            ..LegitSignals::default()
        };
        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 1,
                ..PaperConfig::default()
            },
        )
        .unwrap();
        assert!(analysis.legit.is_legit_duplicate);
        assert_eq!(
            analysis.legit.evidence_keys,
            vec!["migration:official".to_owned()]
        );
    }

    #[test]
    fn economics_marks_gas_not_requested_without_fake_complete() {
        let (store, contract) = store_with_contract("ethereum", "0xecon");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xecon");
        evidence.controllers = vec!["0xop".into()];
        evidence.sales = vec![sale("tx-s", "1", "0xop", "0xv", 50, 5.0)];
        evidence.holders = vec![HolderRecord {
            token_id: "1".into(),
            owner: "0xv".into(),
            balance: Some(1),
        }];
        evidence.quality = EvidenceQuality {
            sales: EvidenceStatus::Complete,
            holders: EvidenceStatus::Complete,
            transfers: EvidenceStatus::Empty,
            gas: EvidenceStatus::NotRequested,
            value_flows: EvidenceStatus::NotRequested,
            ..EvidenceQuality::default()
        };

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 100,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.economics.setup_gas_native, 0.0);
        assert_eq!(analysis.economics.lure_gas_native, 0.0);
        assert_eq!(analysis.economics.exit_gas_native, 0.0);
        assert_eq!(analysis.economics_quality.gas, EvidenceStatus::NotRequested);
        assert_eq!(
            analysis.economics_quality.value_flows,
            EvidenceStatus::NotRequested
        );
        assert!(analysis.economics.honest_loss_usd > 0.0);
    }

    #[test]
    fn layered_and_sybil_detectors_fire_on_synthetic_graphs() {
        let (store, contract) = store_with_contract("ethereum", "0xstar");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xstar");
        evidence.controllers = vec!["0xop".into()];
        // layered path op -> a -> b (3 addresses)
        evidence.transfers = vec![
            transfer("t0", "1", "0xop", "0xa", 1, false),
            transfer("t1", "1", "0xa", "0xb", 2, false),
            // star fan-out from op to three leaves
            transfer("t2", "2", "0xop", "0xc", 3, false),
            transfer("t3", "3", "0xop", "0xd", 4, false),
            transfer("t4", "4", "0xop", "0xe", 5, false),
            // leaves do not propagate further → poisoning / fraud depending on value
        ];
        evidence.sales = vec![
            sale("s0", "2", "0xop", "0xc", 10, 2.0),
            sale("s1", "3", "0xop", "0xd", 11, 2.0),
            sale("s2", "4", "0xop", "0xe", 12, 2.0),
        ];
        evidence.holders = vec![
            HolderRecord {
                token_id: "2".into(),
                owner: "0xc".into(),
                balance: Some(1),
            },
            HolderRecord {
                token_id: "3".into(),
                owner: "0xd".into(),
                balance: Some(1),
            },
            HolderRecord {
                token_id: "4".into(),
                owner: "0xe".into(),
                balance: Some(1),
            },
        ];
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Empty;
        evidence.quality.holders = EvidenceStatus::Complete;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 100,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert!(analysis.behaviors.layered_transfer >= 1);
        assert!(
            analysis.behaviors.fraud_revenue
                + analysis.behaviors.sybil_distribution
                + analysis.behaviors.poisoning
                >= 1
        );
        // Star sales to role-attributed paid victims carry linked paid exposure.
        let star_loss: f64 = analysis
            .behavior_instances
            .iter()
            .filter(|i| {
                matches!(
                    i.kind,
                    BehaviorKind::FraudRevenue
                        | BehaviorKind::SybilDistribution
                        | BehaviorKind::Poisoning
                )
            })
            .map(|i| i.linked_loss_usd)
            .sum();
        assert!(
            star_loss > 0.0,
            "expected star linked_loss_usd from sales to victims, got {star_loss}"
        );
        assert!(
            analysis
                .behavior_instances
                .iter()
                .any(|i| !i.linked_buyers.is_empty()
                    && matches!(
                        i.kind,
                        BehaviorKind::FraudRevenue
                            | BehaviorKind::SybilDistribution
                            | BehaviorKind::Poisoning
                    ))
        );
    }

    #[test]
    fn attribution_marks_paid_holder_as_likely_victim() {
        let (store, contract) = store_with_contract("ethereum", "0xattr");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xattr");
        evidence.controllers = vec!["0xop".into()];
        evidence.sales = vec![sale("tx", "9", "0xop", "0xv", 5, 3.0)];
        evidence.holders = vec![HolderRecord {
            token_id: "9".into(),
            owner: "0xv".into(),
            balance: Some(1),
        }];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Empty;
        evidence.quality.holders = EvidenceStatus::Complete;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 20,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        let victim = analysis
            .attribution
            .iter()
            .find(|(addr, _)| addr == "0xv")
            .unwrap();
        assert_eq!(victim.1.role, AddressRole::LikelyVictim);
        assert_eq!(
            analysis
                .attribution
                .iter()
                .find(|(addr, _)| addr == "0xop")
                .unwrap()
                .1
                .role,
            AddressRole::SuspectedOperator
        );
    }

    #[test]
    fn paid_buyer_is_not_a_victim_when_only_a_different_token_is_still_held() {
        let (store, contract) = store_with_contract("ethereum", "0xattr_same");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xattr_same");
        evidence.sales = vec![sale("tx", "9", "0xop", "0xv", 5, 3.0)];
        evidence.holders = vec![
            HolderRecord {
                token_id: "9".into(),
                owner: "0xv".into(),
                balance: Some(0),
            },
            HolderRecord {
                token_id: "10".into(),
                owner: "0xv".into(),
                balance: Some(1),
            },
        ];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Empty;
        evidence.quality.holders = EvidenceStatus::Complete;

        let analysis =
            analyze_candidate(&store, contract, &evidence, &PaperConfig::default()).unwrap();
        let buyer = analysis
            .attribution
            .iter()
            .find(|(addr, _)| addr == "0xv")
            .unwrap();
        assert_eq!(buyer.1.role, AddressRole::SuspectedOperator);
        assert_eq!(analysis.economics.honest_loss_usd, 0.0);
    }

    #[test]
    fn repeated_seller_and_withdrawal_recipient_are_operator_evidence() {
        let (store, contract) = store_with_contract("ethereum", "0xattr_behavior");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xattr_behavior");
        evidence.sales = (1..=3)
            .map(|index| {
                sale(
                    &format!("sale-{index}"),
                    &index.to_string(),
                    "0xseller",
                    &format!("0xbuyer{index}"),
                    index,
                    1.0,
                )
            })
            .collect();
        evidence.value_flows.push(ValueFlowEdge {
            tx_hash: "withdrawal".into(),
            event_id: None,
            from: "0xattr_behavior".into(),
            to: "0xrecipient".into(),
            kind: ValueFlowKind::Withdrawal,
            native_amount: Some(1.0),
            usd_amount: Some(2_000.0),
            timestamp: Some(4),
            gas_native: None,
            fee_payer: None,
        });

        let analysis =
            analyze_candidate(&store, contract, &evidence, &PaperConfig::default()).unwrap();
        for (address, evidence_kind) in [
            ("0xseller", AddressEvidenceKind::HighVolumeSeller),
            ("0xrecipient", AddressEvidenceKind::WithdrawalRecipient),
        ] {
            let attribution = analysis
                .attribution
                .iter()
                .find(|(candidate, _)| candidate == address)
                .map(|(_, attribution)| attribution)
                .unwrap();
            assert_eq!(attribution.role, AddressRole::SuspectedOperator);
            assert!(
                attribution
                    .evidence
                    .iter()
                    .any(|evidence| evidence.evidence_type == evidence_kind)
            );
        }
    }

    #[test]
    fn paid_mint_current_holder_is_a_victim() {
        let (store, contract) = store_with_contract("ethereum", "0xattr_mint");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xattr_mint");
        let mut paid_mint = transfer(
            "mint-paid",
            "9",
            "0x0000000000000000000000000000000000000000",
            "0xv",
            5,
            true,
        );
        paid_mint.mint_payment_native = Some(0.1);
        paid_mint.mint_payment_usd = Some(300.0);
        paid_mint.mint_payment_receiver = Some("0xoperator".into());
        evidence.transfers = vec![paid_mint];
        evidence.holders = vec![HolderRecord {
            token_id: "9".into(),
            owner: "0xv".into(),
            balance: Some(1),
        }];
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.holders = EvidenceStatus::Complete;

        let analysis =
            analyze_candidate(&store, contract, &evidence, &PaperConfig::default()).unwrap();
        let buyer = analysis
            .attribution
            .iter()
            .find(|(addr, _)| addr == "0xv")
            .unwrap();
        assert_eq!(buyer.1.role, AddressRole::LikelyVictim);
        assert_eq!(analysis.economics.paid_mint_loss_usd, 300.0);
        assert_eq!(analysis.economics.honest_loss_usd, 300.0);
    }

    #[test]
    fn paid_buyer_with_any_later_sale_is_an_operator_not_a_victim() {
        let (store, contract) = store_with_contract("ethereum", "0xattr_resale");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xattr_resale");
        evidence.sales = vec![
            sale("buy", "1", "0xfirst_seller", "0xbuyer", 10, 100.0),
            sale("sell", "1", "0xbuyer", "0xnext_buyer", 20, 120.0),
        ];
        evidence.transfers = vec![
            transfer("buy", "1", "0xfirst_seller", "0xbuyer", 10, false),
            transfer("sell", "1", "0xbuyer", "0xnext_buyer", 20, false),
        ];
        evidence.holders = vec![HolderRecord {
            token_id: "1".into(),
            owner: "0xnext_buyer".into(),
            balance: Some(1),
        }];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.holders = EvidenceStatus::Complete;

        let analysis =
            analyze_candidate(&store, contract, &evidence, &PaperConfig::default()).unwrap();
        let buyer = analysis
            .attribution
            .iter()
            .find(|(address, _)| address == "0xbuyer")
            .unwrap();
        assert_eq!(buyer.1.role, AddressRole::SuspectedOperator);
        // The reseller is excluded, while the next paid buyer is the current
        // holder of token 1 and therefore contributes the 120 USD exposure.
        assert_eq!(analysis.economics.honest_loss_usd, 120.0);
        assert_eq!(analysis.economics.secondary_sale_loss_usd, 120.0);
    }

    #[test]
    fn paid_buyer_with_free_outbound_transfer_is_an_operator_not_a_victim() {
        let (store, contract) = store_with_contract("ethereum", "0xattr_transfer");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xattr_transfer");
        evidence.sales = vec![sale("buy", "1", "0xfirst_seller", "0xbuyer", 10, 100.0)];
        evidence.transfers = vec![transfer(
            "free-transfer",
            "2",
            "0xbuyer",
            "0xrecipient",
            20,
            false,
        )];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Complete;

        let analysis =
            analyze_candidate(&store, contract, &evidence, &PaperConfig::default()).unwrap();
        let buyer = analysis
            .attribution
            .iter()
            .find(|(address, _)| address == "0xbuyer")
            .unwrap();
        assert_eq!(buyer.1.role, AddressRole::SuspectedOperator);
    }

    #[test]
    fn paid_buyer_with_incomplete_transfer_history_is_an_operator() {
        let (store, contract) = store_with_contract("ethereum", "0xattr_incomplete");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xattr_incomplete");
        evidence.sales = vec![sale("buy", "1", "0xfirst_seller", "0xbuyer", 10, 100.0)];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Truncated;

        let analysis =
            analyze_candidate(&store, contract, &evidence, &PaperConfig::default()).unwrap();
        let buyer = analysis
            .attribution
            .iter()
            .find(|(address, _)| address == "0xbuyer")
            .unwrap();
        assert_eq!(buyer.1.role, AddressRole::SuspectedOperator);
    }

    #[test]
    fn output_input_ratio_uses_runtime_usd_for_both_units() {
        let (store, contract) = store_with_contract("ethereum", "0xratio");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xratio");
        evidence.controllers = vec!["0xop".into()];
        evidence.transfers = vec![TransferEvent {
            tx_hash: "mint".into(),
            token_id: "1".into(),
            from: String::new(),
            to: "0xop".into(),
            timestamp: Some(1),
            block_number: Some(1),
            is_mint: true,
            gas_native: Some(0.1),
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }];
        evidence.sales = vec![SaleEvent {
            tx_hash: "sale".into(),
            token_id: "1".into(),
            seller: "0xop".into(),
            buyer: "0xv".into(),
            timestamp: Some(2),
            block_number: Some(2),
            marketplace: None,
            native_amount: Some(2.0),
            usd_amount: Some(400.0),
            currency_symbol: Some("ETH".into()),
            currency_address: None,
            seller_proceeds_native: Some(2.0),
            seller_proceeds_usd: Some(400.0),
            ..SaleEvent::default()
        }];
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.gas = EvidenceStatus::Complete;
        evidence.prices = vec![crate::enrich::PriceBucket {
            chain: "ethereum".into(),
            day_utc: 0,
            symbol: "ETH".into(),
            token_address: None,
            usd_per_native: 2_000.0,
        }];

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 10,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.economics.total_gas_native, 0.1);
        assert_eq!(analysis.economics.operator_output_native, 2.0);
        assert_eq!(analysis.economics.operator_output_usd, 400.0);
        assert_eq!(analysis.economics.total_gas_usd, 200.0);
        // USD/USD only: 400 / 200.
        assert_eq!(analysis.economics.output_input_ratio, Some(2.0));
    }

    #[test]
    fn secondary_sale_honest_loss_counts_native_only_amounts() {
        let (store, contract) = store_with_contract("ethereum", "0xnative");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xnative");
        evidence.controllers = vec!["0xop".into()];
        evidence.sales = vec![SaleEvent {
            tx_hash: "sale".into(),
            token_id: "7".into(),
            seller: "0xop".into(),
            buyer: "0xv".into(),
            timestamp: Some(5),
            block_number: Some(5),
            marketplace: None,
            native_amount: Some(1.5),
            usd_amount: None,
            currency_symbol: Some("ETH".into()),
            currency_address: None,
            seller_proceeds_native: Some(1.5),
            seller_proceeds_usd: None,
            ..SaleEvent::default()
        }];
        evidence.holders = vec![HolderRecord {
            token_id: "7".into(),
            owner: "0xv".into(),
            balance: Some(1),
        }];
        evidence.quality.sales = EvidenceStatus::Complete;
        evidence.quality.transfers = EvidenceStatus::Empty;
        evidence.quality.holders = EvidenceStatus::Complete;

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 20,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.economics.secondary_sale_loss_native, 1.5);
        assert_eq!(analysis.economics.secondary_sale_loss_usd, 0.0);
        assert_eq!(analysis.economics.honest_loss_native, 1.5);
        assert_eq!(analysis.economics.honest_loss_usd, 0.0);
        assert_eq!(analysis.economics.stuck_nft_count, 1);
    }

    /// Regression: enrich-depth fields (gas Complete + Withdrawal/Cashout edges with
    /// `gas_native`) flow through `analyze_candidate` into Setup/Lure/Exit economics.
    #[test]
    fn analyze_candidate_setup_lure_exit_when_gas_and_value_flows_complete() {
        let (store, contract) = store_with_contract("ethereum", "0xe5econ");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xe5econ");
        evidence.controllers = vec!["0xop".into()];
        evidence.transfers = vec![
            TransferEvent {
                tx_hash: "mint-tx".into(),
                token_id: "1".into(),
                from: String::new(),
                to: "0xop".into(),
                timestamp: Some(1),
                block_number: Some(1),
                is_mint: true,
                gas_native: Some(0.01),
                fee_payer: Some("0xop".into()),
                mint_payment_native: None,
                mint_payment_usd: None,
                mint_payment_receiver: None,
            },
            TransferEvent {
                tx_hash: "lure-tx".into(),
                token_id: "1".into(),
                from: "0xop".into(),
                to: "0xv".into(),
                timestamp: Some(2),
                block_number: Some(2),
                is_mint: false,
                gas_native: Some(0.02),
                fee_payer: Some("0xop".into()),
                mint_payment_native: None,
                mint_payment_usd: None,
                mint_payment_receiver: None,
            },
            TransferEvent {
                tx_hash: "cashout-tx".into(),
                token_id: "1".into(),
                from: "0xop".into(),
                to: "0xex".into(),
                timestamp: Some(3),
                block_number: Some(3),
                is_mint: false,
                gas_native: Some(0.05),
                fee_payer: Some("0xop".into()),
                mint_payment_native: None,
                mint_payment_usd: None,
                mint_payment_receiver: None,
            },
        ];
        evidence.value_flows = vec![
            ValueFlowEdge {
                tx_hash: "mint-tx".into(),
                event_id: None,
                from: "0xfunder".into(),
                to: "0xop".into(),
                kind: ValueFlowKind::Funding,
                native_amount: Some(1.0),
                usd_amount: Some(200.0),
                timestamp: Some(1),
                gas_native: None,
                fee_payer: None,
            },
            ValueFlowEdge {
                tx_hash: "cashout-tx".into(),
                event_id: None,
                from: "0xop".into(),
                to: "0xex".into(),
                kind: ValueFlowKind::Cashout,
                native_amount: Some(0.8),
                usd_amount: Some(160.0),
                timestamp: Some(3),
                gas_native: None,
                fee_payer: None,
            },
            ValueFlowEdge {
                tx_hash: "wd-tx".into(),
                event_id: None,
                from: "0xop".into(),
                to: "0xcex".into(),
                kind: ValueFlowKind::Withdrawal,
                native_amount: Some(0.2),
                usd_amount: Some(40.0),
                timestamp: Some(4),
                gas_native: None,
                fee_payer: None,
            },
        ];
        evidence.quality = EvidenceQuality {
            transfers: EvidenceStatus::Complete,
            gas: EvidenceStatus::Complete,
            value_flows: EvidenceStatus::Complete,
            ..EvidenceQuality::default()
        };

        let analysis = analyze_candidate(
            &store,
            contract,
            &evidence,
            &PaperConfig {
                analysis_timestamp: 100,
                ..PaperConfig::default()
            },
        )
        .unwrap();

        assert_eq!(analysis.economics_quality.gas, EvidenceStatus::Complete);
        assert_eq!(
            analysis.economics_quality.value_flows,
            EvidenceStatus::Complete
        );
        assert_eq!(analysis.economics.setup_gas_native, 0.01);
        assert_eq!(analysis.economics.lure_gas_native, 0.02);
        // Funding marks Setup; cashout upgrades the matching tx to Exit.
        assert_eq!(analysis.economics.exit_gas_native, 0.05);
        assert_eq!(analysis.economics.total_gas_native, 0.08);
        assert_eq!(analysis.economics.withdrawal_native, 1.0);
        assert_eq!(analysis.economics.withdrawal_usd, 200.0);
        assert!(analysis.economics_quality.notes.is_empty());
    }

    #[test]
    fn shrink_for_summary_memory_keeps_roles_and_behavior_keys() {
        let mut analysis = CandidateAnalysis {
            contract_id: 1,
            chain: "ethereum".into(),
            address: "0x1".into(),
            legit: legit::classify(&LegitSignals::default()),
            legit_by_seed: Default::default(),
            attribution: vec![(
                "0xop".into(),
                AddressAttribution {
                    role: AddressRole::SuspectedOperator,
                    evidence: vec![AddressEvidence {
                        evidence_type: AddressEvidenceKind::ControllerOrAuthority,
                        token_id: Some("1".into()),
                        transaction: Some("0xtx".into()),
                        weight: 1.0,
                        confidence: 1.0,
                    }],
                },
            )],
            lifecycle: LifecycleFacts {
                first_activity_timestamp: Some(1),
                ..LifecycleFacts::default()
            },
            value_flow: ValueFlowFacts {
                mint_edge_count: 9,
                ..ValueFlowFacts::default()
            },
            behaviors: BehaviorFacts {
                wash_cycles: 1,
                ..BehaviorFacts::default()
            },
            behavior_instances: vec![BehaviorInstance {
                kind: BehaviorKind::WashTrading,
                addresses: vec!["0xa".into()],
                nfts: vec!["1".into()],
                transactions: vec!["0xt".into()],
                linked_buyers: vec!["0xb".into()],
                linked_loss_usd: 3.0,
                ..BehaviorInstance::default()
            }],
            economics: EconomicFacts {
                honest_loss_usd: 1.0,
                ..EconomicFacts::default()
            },
            economics_quality: EconomicsQuality::default(),
            evidence_quality: EvidenceQuality::default(),
            collection_nft_count: None,
            collection_nft_count_complete: false,
            analysis_timestamp: 0,
        };
        analysis.shrink_for_summary_memory();
        assert!(analysis.attribution[0].1.evidence.is_empty());
        assert_eq!(
            analysis.attribution[0].1.role,
            AddressRole::SuspectedOperator
        );
        assert_eq!(analysis.lifecycle.first_activity_timestamp, None);
        assert_eq!(analysis.value_flow.mint_edge_count, 0);
        assert_eq!(analysis.behavior_instances[0].addresses, vec!["0xa"]);
        assert!(analysis.behavior_instances[0].transactions.is_empty());
        assert_eq!(analysis.behavior_instances[0].linked_loss_usd, 3.0);
        assert_eq!(analysis.economics.honest_loss_usd, 1.0);
    }

    #[test]
    fn relation_projection_matches_scoped_reanalysis() {
        let (store, contract) = store_with_contract("ethereum", "0xcandidate");
        let mut evidence = EvidenceBundle::empty(contract, "ethereum", "0xcandidate");
        evidence
            .transfers
            .push(transfer("0xtx", "1", "0xoperator", "0xbuyer", 100, false));
        let legit_key = "ethereum:0xlegit".to_owned();
        let suspicious_key = "ethereum:0xsuspicious".to_owned();
        evidence.relation_legit.insert(
            legit_key.clone(),
            LegitSignals {
                verified_migration: true,
                verification_complete: true,
                ..LegitSignals::default()
            },
        );
        evidence.relation_legit.insert(
            suspicious_key.clone(),
            LegitSignals {
                verification_complete: true,
                ..LegitSignals::default()
            },
        );
        crate::enrich::finalize_legit_signals(&mut evidence);
        let config = PaperConfig {
            analysis_timestamp: 1_000,
            ..PaperConfig::default()
        };
        let base = analyze_candidate(&store, contract, &evidence, &config).unwrap();

        for key in [legit_key, suspicious_key] {
            let keys = ahash::AHashSet::from([key.clone()]);
            let scoped = evidence.filtered_for_analysis(&keys);
            let expected = analyze_candidate(&store, contract, &scoped, &config).unwrap();
            let signals = scoped.relation_legit.clone();
            let projected = base.project_relation_signals(&signals);
            assert_eq!(
                serde_json::to_value(projected).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
    }
}
