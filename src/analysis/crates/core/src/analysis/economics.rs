//! Attacker economics: Setup/Lure/Exit gas, output ratios, and paid exposure.

use std::collections::BTreeMap;

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use super::attribution::AddressRole;
use super::lifecycle::LifecycleFacts;
use crate::enrich::{EvidenceBundle, EvidenceStatus, ValueFlowKind, normalize_chain_address};

/// Transaction-level USD contribution retained only for cross-candidate
/// aggregation. Candidate JSON keeps the human-facing scalar totals.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValueFlowContribution {
    pub tx_hash: String,
    #[serde(default)]
    pub event_id: Option<String>,
    pub from: String,
    pub to: String,
    pub kind: ValueFlowKind,
    pub usd: Option<f64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GasStage {
    Setup = 1,
    Lure = 2,
    Exit = 3,
}

/// Transaction-level priced gas contribution retained for aggregate dedup.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GasContribution {
    pub tx_hash: String,
    pub stage: GasStage,
    pub usd: f64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EconomicContributionKind {
    GrossSale,
    MarketplaceFee,
    RoyaltyFee,
    OperatorSaleProceeds,
    OperatorRoyalty,
    HonestSecondaryExposure,
    OperatorMintPayment,
    HonestMintExposure,
}

/// Transaction-level sale/mint amount retained for cross-candidate aggregate
/// deduplication. Per-candidate JSON keeps the full attributable scalar.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EconomicContribution {
    pub tx_hash: String,
    /// Candidate NFT identity. Together with the candidate contract this
    /// distinguishes independent NFT events inside one transaction while still
    /// allowing the same event to de-duplicate across seed scopes.
    #[serde(default)]
    pub token_id: String,
    pub from: String,
    pub to: String,
    pub kind: EconomicContributionKind,
    pub usd: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EconomicFacts {
    #[serde(skip_serializing)]
    pub gross_revenue_native: f64,
    #[serde(rename = "gross_sales_volume_usd")]
    pub gross_revenue_usd: f64,
    #[serde(skip_serializing)]
    pub setup_gas_native: f64,
    pub setup_gas_usd: f64,
    #[serde(skip_serializing)]
    pub lure_gas_native: f64,
    pub lure_gas_usd: f64,
    #[serde(skip_serializing)]
    pub exit_gas_native: f64,
    pub exit_gas_usd: f64,
    #[serde(skip_serializing)]
    pub total_gas_native: f64,
    pub total_gas_usd: f64,
    #[serde(skip_serializing)]
    pub operator_output_native: f64,
    pub operator_output_usd: f64,
    #[serde(skip_serializing)]
    pub marketplace_fee_native: f64,
    pub marketplace_fee_usd: f64,
    #[serde(skip_serializing)]
    pub royalty_fee_native: f64,
    pub royalty_fee_usd: f64,
    pub operator_royalty_usd: f64,
    /// USD/USD output/input; absent when run-time gas pricing is unavailable.
    pub output_input_ratio: Option<f64>,
    /// Kept for cache compatibility. Any defined ratio is USD/USD.
    #[serde(default)]
    pub output_input_ratio_is_usd: bool,
    /// Attacker gas cost in USD when a spot rate is available for the chain.
    #[serde(default)]
    pub attacker_input_usd: Option<f64>,
    #[serde(skip_serializing)]
    pub secondary_sale_loss_native: f64,
    #[serde(rename = "secondary_sale_paid_exposure_usd")]
    pub secondary_sale_loss_usd: f64,
    #[serde(skip_serializing)]
    pub paid_mint_loss_native: f64,
    #[serde(rename = "paid_mint_exposure_usd")]
    pub paid_mint_loss_usd: f64,
    #[serde(skip_serializing)]
    pub honest_loss_native: f64,
    #[serde(rename = "honest_paid_exposure_usd")]
    pub honest_loss_usd: f64,
    pub stuck_nft_count: u64,
    /// Native funding into operators (from `value_flows` Funding edges).
    #[serde(skip_serializing)]
    pub funding_native: f64,
    pub funding_usd: f64,
    /// Internal native transfers between attributed operators.
    #[serde(skip_serializing)]
    pub revenue_backflow_native: f64,
    pub revenue_backflow_usd: f64,
    /// Native withdrawal / cashout out of operators.
    #[serde(skip_serializing)]
    pub withdrawal_native: f64,
    pub withdrawal_usd: f64,
    pub priced_value_flow_count: u64,
    pub unpriced_value_flow_count: u64,
    pub sale_count: u64,
    pub priced_sale_count: u64,
    pub unpriced_sale_count: u64,
    pub amountless_sale_count: u64,
    pub assumed_stablecoin_peg_sale_count: u64,
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
    pub gas_cost_observed: bool,
    pub gas_cost_priced: bool,
    /// Internal aggregation keys; intentionally absent from report JSON.
    #[serde(skip)]
    pub value_flow_contributions: Vec<ValueFlowContribution>,
    /// Internal aggregation keys; intentionally absent from report JSON.
    #[serde(skip)]
    pub gas_contributions: Vec<GasContribution>,
    /// Internal aggregation keys for sale/mint USD; absent from report JSON.
    #[serde(skip)]
    pub economic_contributions: Vec<EconomicContribution>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EconomicsQuality {
    pub gas: EvidenceStatus,
    pub value_flows: EvidenceStatus,
    pub notes: Vec<String>,
}

fn value_flows_usable(status: EvidenceStatus) -> bool {
    matches!(status, EvidenceStatus::Complete | EvidenceStatus::Truncated)
}

fn gas_usable(status: EvidenceStatus) -> bool {
    matches!(status, EvidenceStatus::Complete | EvidenceStatus::Truncated)
}

pub fn compute_economics(
    evidence: &EvidenceBundle,
    roles: &BTreeMap<String, AddressRole>,
    analysis_timestamp: i64,
    lifecycle: &LifecycleFacts,
) -> (EconomicFacts, EconomicsQuality) {
    let mut quality = EconomicsQuality {
        gas: evidence.quality.gas,
        value_flows: evidence.quality.value_flows,
        notes: Vec::new(),
    };
    if matches!(
        evidence.quality.gas,
        EvidenceStatus::NotRequested | EvidenceStatus::Failed
    ) {
        quality
            .notes
            .push("gas evidence incomplete; Setup/Lure/Exit USD costs are unavailable".into());
    }
    if matches!(
        evidence.quality.value_flows,
        EvidenceStatus::NotRequested | EvidenceStatus::Failed
    ) {
        quality.notes.push(
            "value_flows evidence incomplete; funding/withdrawal aggregates unavailable".into(),
        );
    }

    let operators = roles
        .iter()
        .filter(|(_, role)| matches!(role, AddressRole::SuspectedOperator))
        .map(|(address, _)| address.clone())
        .collect::<AHashSet<_>>();
    let honest_buyers = roles
        .iter()
        .filter(|(_, role)| matches!(role, AddressRole::LikelyVictim))
        .map(|(address, _)| address.clone())
        .collect::<AHashSet<_>>();
    let honest_holders = evidence
        .holders
        .iter()
        .filter(|holder| holder.balance.is_none_or(|balance| balance > 0))
        .filter_map(|holder| {
            let owner = normalize_chain_address(&evidence.chain, &holder.owner);
            honest_buyers
                .contains(&owner)
                .then(|| (holder.token_id.clone(), owner))
        })
        .collect::<AHashSet<_>>();

    let mut facts = EconomicFacts::default();
    let mut stuck_nfts = AHashSet::new();
    let mut gas_by_tx = AHashMap::<String, (GasStage, f64)>::new();
    let mut gas_native_by_tx = AHashMap::<String, f64>::new();

    if gas_usable(evidence.quality.gas)
        && let Some(deployment) = evidence.deployment.as_ref()
        && let (Some(gas), Some(payer)) = (
            deployment.gas_native.filter(|value| *value > 0.0),
            deployment.fee_payer.as_deref(),
        )
        && operators.contains(&normalize_chain_address(&evidence.chain, payer))
    {
        gas_native_by_tx.insert(deployment.tx_hash.clone(), gas);
        gas_by_tx.insert(deployment.tx_hash.clone(), (GasStage::Setup, gas));
    }

    for sale in &evidence.sales {
        let native = sale.native_amount.unwrap_or(0.0).max(0.0);
        let usd = sale.usd_amount.unwrap_or(0.0).max(0.0);
        facts.sale_count += 1;
        if sale.usd_amount.is_some_and(|value| value >= 0.0) {
            facts.priced_sale_count += 1;
            if sale
                .currency_symbol
                .as_deref()
                .is_some_and(is_usd_stablecoin)
                && !evidence.prices.iter().any(|price| {
                    let same_address = sale.currency_address.as_deref().is_some_and(|address| {
                        price
                            .token_address
                            .as_deref()
                            .is_some_and(|priced_address| {
                                normalize_chain_address(&evidence.chain, address)
                                    == normalize_chain_address(&evidence.chain, priced_address)
                            })
                    });
                    let same_symbol = sale
                        .currency_symbol
                        .as_deref()
                        .is_some_and(|symbol| price.symbol.eq_ignore_ascii_case(symbol));
                    (same_address || same_symbol)
                        && price.usd_per_native.is_finite()
                        && price.usd_per_native > 0.0
                })
            {
                facts.assumed_stablecoin_peg_sale_count += 1;
            }
        } else if native > 0.0 {
            facts.unpriced_sale_count += 1;
        } else {
            facts.amountless_sale_count += 1;
        }
        facts.gross_revenue_native += native;
        facts.gross_revenue_usd += usd;
        facts.marketplace_fee_native += sale.marketplace_fee_native.unwrap_or(0.0).max(0.0);
        facts.marketplace_fee_usd += sale.marketplace_fee_usd.unwrap_or(0.0).max(0.0);
        facts.royalty_fee_native += sale.royalty_fee_native.unwrap_or(0.0).max(0.0);
        facts.royalty_fee_usd += sale.royalty_fee_usd.unwrap_or(0.0).max(0.0);
        let seller = normalize_chain_address(&evidence.chain, &sale.seller);
        let buyer = normalize_chain_address(&evidence.chain, &sale.buyer);
        if usd > 0.0 {
            facts.economic_contributions.push(EconomicContribution {
                tx_hash: sale.tx_hash.clone(),
                token_id: sale.token_id.clone(),
                from: buyer.clone(),
                to: seller.clone(),
                kind: EconomicContributionKind::GrossSale,
                usd,
            });
        }
        if let Some(fee_usd) = sale.marketplace_fee_usd.filter(|value| *value > 0.0) {
            facts.economic_contributions.push(EconomicContribution {
                tx_hash: sale.tx_hash.clone(),
                token_id: sale.token_id.clone(),
                from: buyer.clone(),
                to: sale
                    .marketplace
                    .as_deref()
                    .map(|marketplace| format!("marketplace:{marketplace}"))
                    .unwrap_or_else(|| "marketplace:unknown".into()),
                kind: EconomicContributionKind::MarketplaceFee,
                usd: fee_usd,
            });
        }
        if let Some(fee_usd) = sale.royalty_fee_usd.filter(|value| *value > 0.0) {
            facts.economic_contributions.push(EconomicContribution {
                tx_hash: sale.tx_hash.clone(),
                token_id: sale.token_id.clone(),
                from: buyer.clone(),
                to: sale
                    .royalty_recipient
                    .as_deref()
                    .map(|address| normalize_chain_address(&evidence.chain, address))
                    .unwrap_or_default(),
                kind: EconomicContributionKind::RoyaltyFee,
                usd: fee_usd,
            });
        }
        if operators.contains(&seller) {
            facts.operator_sale_count += 1;
            if let Some(seller_native) = sale.seller_proceeds_native.filter(|value| *value >= 0.0) {
                facts.operator_output_native += seller_native;
                if let Some(seller_usd) = sale.seller_proceeds_usd.filter(|value| *value >= 0.0) {
                    facts.operator_output_usd += seller_usd;
                    facts.priced_operator_sale_proceeds_count += 1;
                    if seller_usd > 0.0 {
                        facts.economic_contributions.push(EconomicContribution {
                            tx_hash: sale.tx_hash.clone(),
                            token_id: sale.token_id.clone(),
                            from: buyer.clone(),
                            to: seller.clone(),
                            kind: EconomicContributionKind::OperatorSaleProceeds,
                            usd: seller_usd,
                        });
                    }
                } else {
                    facts.unpriced_operator_sale_proceeds_count += 1;
                }
            } else {
                facts.unknown_operator_sale_proceeds_count += 1;
            }
        }
        if let Some(royalty_native) = sale.royalty_fee_native.filter(|value| *value > 0.0) {
            let recipient = sale
                .royalty_recipient
                .as_deref()
                .map(|address| normalize_chain_address(&evidence.chain, address))
                .filter(|address| !address.is_empty());
            if let Some(recipient) = recipient {
                if operators.contains(&recipient) {
                    facts.operator_output_native += royalty_native;
                    if let Some(royalty_usd) = sale.royalty_fee_usd.filter(|value| *value > 0.0) {
                        facts.operator_output_usd += royalty_usd;
                        facts.operator_royalty_usd += royalty_usd;
                        facts.economic_contributions.push(EconomicContribution {
                            tx_hash: sale.tx_hash.clone(),
                            token_id: sale.token_id.clone(),
                            from: buyer.clone(),
                            to: recipient,
                            kind: EconomicContributionKind::OperatorRoyalty,
                            usd: royalty_usd,
                        });
                    }
                }
            } else {
                facts.unknown_royalty_recipient_count += 1;
            }
        }
        if honest_buyers.contains(&buyer)
            && honest_holders.contains(&(sale.token_id.clone(), buyer.clone()))
            && (native > 0.0 || usd > 0.0)
        {
            facts.secondary_sale_loss_native += native;
            facts.secondary_sale_loss_usd += usd;
            facts.honest_loss_native += native;
            facts.honest_loss_usd += usd;
            if usd > 0.0 {
                facts.economic_contributions.push(EconomicContribution {
                    tx_hash: sale.tx_hash.clone(),
                    token_id: sale.token_id.clone(),
                    from: buyer.clone(),
                    to: seller,
                    kind: EconomicContributionKind::HonestSecondaryExposure,
                    usd,
                });
            }
            stuck_nfts.insert(sale.token_id.clone());
        }
        if gas_usable(evidence.quality.gas)
            && let (Some(gas), Some(payer)) = (
                sale.gas_native.filter(|value| *value > 0.0),
                sale.fee_payer.as_deref(),
            )
            && operators.contains(&normalize_chain_address(&evidence.chain, payer))
        {
            let native_entry = gas_native_by_tx.entry(sale.tx_hash.clone()).or_insert(0.0);
            *native_entry = native_entry.max(gas);
            let entry = gas_by_tx
                .entry(sale.tx_hash.clone())
                .or_insert((GasStage::Lure, 0.0));
            entry.0 = entry.0.max(GasStage::Lure);
            entry.1 = entry.1.max(gas);
        }
        let _ = analysis_timestamp;
        let _ = lifecycle;
    }

    for transfer in &evidence.transfers {
        if transfer.is_mint {
            let native = transfer.mint_payment_native.unwrap_or(0.0).max(0.0);
            let usd = transfer.mint_payment_usd.unwrap_or(0.0).max(0.0);
            if native > 0.0 || usd > 0.0 {
                let recipient = normalize_chain_address(&evidence.chain, &transfer.to);
                facts.paid_mint_payment_count += 1;
                let receiver_is_operator =
                    transfer
                        .mint_payment_receiver
                        .as_deref()
                        .is_some_and(|receiver| {
                            let receiver = normalize_chain_address(&evidence.chain, receiver);
                            operators.contains(&receiver)
                                || receiver
                                    == normalize_chain_address(&evidence.chain, &evidence.address)
                                || evidence.controllers.iter().any(|controller| {
                                    receiver == normalize_chain_address(&evidence.chain, controller)
                                })
                        });
                if transfer.mint_payment_receiver.is_none() {
                    facts.unknown_paid_mint_receiver_count += 1;
                } else if receiver_is_operator {
                    facts.operator_paid_mint_payment_count += 1;
                    facts.operator_output_native += native;
                    if transfer.mint_payment_usd.is_some() {
                        facts.priced_operator_paid_mint_payment_count += 1;
                        facts.operator_output_usd += usd;
                        if usd > 0.0 {
                            facts.economic_contributions.push(EconomicContribution {
                                tx_hash: transfer.tx_hash.clone(),
                                token_id: transfer.token_id.clone(),
                                from: recipient.clone(),
                                to: transfer
                                    .mint_payment_receiver
                                    .as_deref()
                                    .map(|receiver| {
                                        normalize_chain_address(&evidence.chain, receiver)
                                    })
                                    .unwrap_or_default(),
                                kind: EconomicContributionKind::OperatorMintPayment,
                                usd,
                            });
                        }
                    } else {
                        facts.unpriced_operator_paid_mint_payment_count += 1;
                    }
                }
                if honest_buyers.contains(&recipient)
                    && honest_holders.contains(&(transfer.token_id.clone(), recipient.clone()))
                {
                    facts.honest_paid_mint_loss_count += 1;
                    if transfer.mint_payment_usd.is_some() {
                        facts.priced_honest_paid_mint_loss_count += 1;
                    } else {
                        facts.unpriced_honest_paid_mint_loss_count += 1;
                    }
                    facts.paid_mint_loss_native += native;
                    facts.paid_mint_loss_usd += usd;
                    facts.honest_loss_native += native;
                    facts.honest_loss_usd += usd;
                    if usd > 0.0 {
                        facts.economic_contributions.push(EconomicContribution {
                            tx_hash: transfer.tx_hash.clone(),
                            token_id: transfer.token_id.clone(),
                            from: recipient.clone(),
                            to: transfer
                                .mint_payment_receiver
                                .as_deref()
                                .map(|receiver| normalize_chain_address(&evidence.chain, receiver))
                                .unwrap_or_default(),
                            kind: EconomicContributionKind::HonestMintExposure,
                            usd,
                        });
                    }
                    stuck_nfts.insert(transfer.token_id.clone());
                }
            }
        }
        if gas_usable(evidence.quality.gas)
            && let Some(gas) = transfer.gas_native.filter(|value| *value > 0.0)
        {
            let payer = transfer
                .fee_payer
                .as_deref()
                .unwrap_or(if transfer.is_mint {
                    transfer.to.as_str()
                } else {
                    transfer.from.as_str()
                });
            let payer = normalize_chain_address(&evidence.chain, payer);
            if operators.contains(&payer) {
                let native_entry = gas_native_by_tx
                    .entry(transfer.tx_hash.clone())
                    .or_insert(0.0);
                *native_entry = native_entry.max(gas);
                let stage = GasStage::Lure;
                let entry = gas_by_tx
                    .entry(transfer.tx_hash.clone())
                    .or_insert((stage, 0.0));
                if stage > entry.0 {
                    entry.0 = stage;
                }
                entry.1 = entry.1.max(gas);
            }
        }
    }

    if value_flows_usable(evidence.quality.value_flows) {
        for edge in &evidence.value_flows {
            let native = edge
                .native_amount
                .filter(|value| value.is_finite() && *value > 0.0)
                .unwrap_or(0.0);
            let priced_usd = edge
                .usd_amount
                .filter(|value| value.is_finite() && *value >= 0.0);
            let usd = priced_usd.unwrap_or(0.0);
            if priced_usd.is_some() {
                facts.priced_value_flow_count += 1;
            } else if native > 0.0 {
                facts.unpriced_value_flow_count += 1;
            }
            if priced_usd.is_some() || native > 0.0 {
                facts.value_flow_contributions.push(ValueFlowContribution {
                    tx_hash: edge.tx_hash.clone(),
                    event_id: edge.event_id.clone(),
                    from: normalize_chain_address(&evidence.chain, &edge.from),
                    to: normalize_chain_address(&evidence.chain, &edge.to),
                    kind: edge.kind,
                    usd: priced_usd,
                });
            }
            if gas_usable(evidence.quality.gas)
                && let (Some(gas), Some(payer)) = (
                    edge.gas_native.filter(|value| *value > 0.0),
                    edge.fee_payer.as_deref(),
                )
                && operators.contains(&normalize_chain_address(&evidence.chain, payer))
            {
                let stage = match edge.kind {
                    ValueFlowKind::Funding => GasStage::Setup,
                    ValueFlowKind::RevenueBackflow => GasStage::Lure,
                    ValueFlowKind::Withdrawal | ValueFlowKind::Cashout => GasStage::Exit,
                };
                let entry = gas_by_tx
                    .entry(edge.tx_hash.clone())
                    .or_insert((stage, 0.0));
                if stage > entry.0 {
                    entry.0 = stage;
                }
                entry.1 = entry.1.max(gas);
            }
            match edge.kind {
                ValueFlowKind::Funding => {
                    facts.funding_native += native;
                    facts.funding_usd += usd;
                    if let Some(&gas) = gas_native_by_tx.get(&edge.tx_hash) {
                        let entry = gas_by_tx
                            .entry(edge.tx_hash.clone())
                            .or_insert((GasStage::Setup, 0.0));
                        entry.0 = GasStage::Setup;
                        entry.1 = entry.1.max(gas);
                    }
                }
                ValueFlowKind::RevenueBackflow => {
                    facts.revenue_backflow_native += native;
                    facts.revenue_backflow_usd += usd;
                }
                ValueFlowKind::Withdrawal | ValueFlowKind::Cashout => {
                    facts.withdrawal_native += native;
                    facts.withdrawal_usd += usd;
                    if let Some(&gas) = gas_native_by_tx.get(&edge.tx_hash) {
                        let entry = gas_by_tx
                            .entry(edge.tx_hash.clone())
                            .or_insert((GasStage::Exit, 0.0));
                        if GasStage::Exit > entry.0 {
                            entry.0 = GasStage::Exit;
                        }
                        entry.1 = entry.1.max(gas);
                    }
                }
            }
        }
    }

    for (stage, gas) in gas_by_tx.values() {
        match stage {
            GasStage::Setup => facts.setup_gas_native += *gas,
            GasStage::Lure => facts.lure_gas_native += *gas,
            GasStage::Exit => facts.exit_gas_native += *gas,
        }
        facts.total_gas_native += *gas;
    }

    facts.stuck_nft_count = stuck_nfts.len() as u64;

    // Price all stages with one run-time spot rate for the chain.
    let spot_rate = spot_rate(evidence.chain.as_str(), &evidence.prices);
    if let Some(rate) = spot_rate {
        facts.setup_gas_usd = facts.setup_gas_native * rate;
        facts.lure_gas_usd = facts.lure_gas_native * rate;
        facts.exit_gas_usd = facts.exit_gas_native * rate;
        facts.total_gas_usd = facts.total_gas_native * rate;
        facts.gas_contributions = gas_by_tx
            .into_iter()
            .map(|(tx_hash, (stage, native))| GasContribution {
                tx_hash,
                stage,
                usd: native * rate,
            })
            .collect();
    }
    let gas_usd = (facts.total_gas_native > 0.0)
        .then_some(facts.total_gas_usd)
        .filter(|_| spot_rate.is_some());
    facts.gas_cost_observed = facts.total_gas_native > 0.0;
    facts.gas_cost_priced = gas_usd.is_some();
    facts.attacker_input_usd = gas_usd;
    let operator_output_complete = facts.unknown_operator_sale_proceeds_count == 0
        && facts.unpriced_operator_sale_proceeds_count == 0
        && facts.unknown_royalty_recipient_count == 0
        && facts.unknown_paid_mint_receiver_count == 0
        && facts.unpriced_operator_paid_mint_payment_count == 0;
    facts.output_input_ratio = operator_output_complete
        .then_some(gas_usd)
        .flatten()
        .filter(|input| *input > 0.0)
        .map(|input| facts.operator_output_usd / input);
    facts.output_input_ratio_is_usd = facts.output_input_ratio.is_some();

    if facts.unpriced_sale_count > 0 {
        quality.notes.push(format!(
            "{} sale(s) excluded from USD totals because payment-token pricing was unavailable",
            facts.unpriced_sale_count
        ));
    }
    if facts.amountless_sale_count > 0 {
        quality.notes.push(format!(
            "{} sale(s) excluded from USD totals because no usable payment amount was available",
            facts.amountless_sale_count
        ));
    }
    if facts.assumed_stablecoin_peg_sale_count > 0 {
        quality.notes.push(format!(
            "{} sale(s) used a 1 USD stablecoin peg because a run-time quote was unavailable",
            facts.assumed_stablecoin_peg_sale_count
        ));
    }
    if facts.unpriced_honest_paid_mint_loss_count > 0 {
        quality.notes.push(format!(
            "{} honest-buyer paid mint exposure event(s) excluded from USD totals because pricing was unavailable",
            facts.unpriced_honest_paid_mint_loss_count
        ));
    }
    if facts.unpriced_value_flow_count > 0 {
        quality.notes.push(format!(
            "{} value-flow edge(s) excluded from USD totals because run-time native pricing was unavailable",
            facts.unpriced_value_flow_count
        ));
    }
    if facts.unknown_operator_sale_proceeds_count > 0 {
        quality.notes.push(format!(
            "{} operator sale(s) excluded from output because seller net proceeds were unavailable",
            facts.unknown_operator_sale_proceeds_count
        ));
    }
    if facts.unknown_paid_mint_receiver_count > 0 {
        quality.notes.push(format!(
            "{} paid mint payment(s) excluded from output because the receiver was unavailable",
            facts.unknown_paid_mint_receiver_count
        ));
    }

    (facts, quality)
}

fn spot_rate(chain: &str, prices: &[crate::enrich::PriceBucket]) -> Option<f64> {
    prices
        .iter()
        .find(|price| {
            price.chain.eq_ignore_ascii_case(chain)
                && match chain.to_ascii_lowercase().as_str() {
                    "ethereum" | "base" => price.symbol.eq_ignore_ascii_case("ETH"),
                    "polygon" | "matic" => {
                        price.symbol.eq_ignore_ascii_case("MATIC")
                            || price.symbol.eq_ignore_ascii_case("POL")
                    }
                    "solana" => price.symbol.eq_ignore_ascii_case("SOL"),
                    _ => false,
                }
                && price.usd_per_native > 0.0
        })
        .map(|price| price.usd_per_native)
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn is_usd_stablecoin(symbol: &str) -> bool {
    matches!(
        symbol.trim().to_ascii_uppercase().as_str(),
        "USDC"
            | "USDC.E"
            | "USDT"
            | "USDT.E"
            | "DAI"
            | "USDS"
            | "PYUSD"
            | "FDUSD"
            | "TUSD"
            | "USDG"
            | "USDE"
            | "GUSD"
            | "LUSD"
            | "FRAX"
            | "CRVUSD"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::{
        DeploymentEvent, EvidenceQuality, EvidenceStatus, PriceBucket, SaleEvent, TransferEvent,
        ValueFlowEdge, ValueFlowKind,
    };

    fn roles_op(op: &str) -> BTreeMap<String, AddressRole> {
        let mut roles = BTreeMap::new();
        roles.insert(op.to_owned(), AddressRole::SuspectedOperator);
        roles
    }

    fn edge(
        tx: &str,
        from: &str,
        to: &str,
        kind: ValueFlowKind,
        native: f64,
        usd: f64,
    ) -> ValueFlowEdge {
        ValueFlowEdge {
            tx_hash: tx.into(),
            event_id: None,
            from: from.into(),
            to: to.into(),
            kind,
            native_amount: Some(native),
            usd_amount: Some(usd),
            timestamp: Some(1),
            gas_native: None,
            fee_payer: None,
        }
    }

    #[test]
    fn value_flows_complete_aggregates_funding_withdrawal_and_exit_gas() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.controllers = vec!["0xop".into()];
        evidence.transfers = vec![TransferEvent {
            tx_hash: "cashout-tx".into(),
            token_id: "1".into(),
            from: "0xop".into(),
            to: "0xex".into(),
            timestamp: Some(10),
            block_number: Some(10),
            is_mint: false,
            gas_native: Some(0.05),
            fee_payer: Some("0xop".into()),
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }];
        evidence.value_flows = vec![
            edge(
                "fund-tx",
                "0xfunder",
                "0xop",
                ValueFlowKind::Funding,
                1.0,
                200.0,
            ),
            edge(
                "cashout-tx",
                "0xop",
                "0xex",
                ValueFlowKind::Cashout,
                0.8,
                160.0,
            ),
            edge(
                "wd-tx",
                "0xop",
                "0xcex",
                ValueFlowKind::Withdrawal,
                0.2,
                40.0,
            ),
            edge(
                "backflow-tx",
                "0xop",
                "0xpeer",
                ValueFlowKind::RevenueBackflow,
                0.4,
                80.0,
            ),
        ];
        evidence.value_flows[0].gas_native = Some(0.03);
        evidence.value_flows[0].fee_payer = Some("0xfunder".into());
        evidence.value_flows[2].gas_native = Some(0.02);
        evidence.value_flows[2].fee_payer = Some("0xop".into());
        evidence.quality = EvidenceQuality {
            transfers: EvidenceStatus::Complete,
            gas: EvidenceStatus::Complete,
            value_flows: EvidenceStatus::Complete,
            ..EvidenceQuality::default()
        };

        let (facts, quality) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );

        assert_eq!(quality.value_flows, EvidenceStatus::Complete);
        assert!(quality.notes.is_empty());
        assert_eq!(facts.funding_native, 1.0);
        assert_eq!(facts.funding_usd, 200.0);
        assert_eq!(facts.revenue_backflow_native, 0.4);
        assert_eq!(facts.revenue_backflow_usd, 80.0);
        assert_eq!(facts.withdrawal_native, 1.0);
        assert_eq!(facts.withdrawal_usd, 200.0);
        assert_eq!(facts.setup_gas_native, 0.0);
        assert_eq!(facts.exit_gas_native, 0.07);
        assert_eq!(facts.total_gas_native, 0.07);
    }

    #[test]
    fn value_flows_not_requested_keeps_zero_aggregates_and_note() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.value_flows = vec![edge(
            "fund-tx",
            "0xfunder",
            "0xop",
            ValueFlowKind::Funding,
            9.0,
            900.0,
        )];
        evidence.quality.value_flows = EvidenceStatus::NotRequested;

        let (facts, quality) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );

        assert_eq!(facts.funding_native, 0.0);
        assert_eq!(facts.funding_usd, 0.0);
        assert_eq!(facts.withdrawal_native, 0.0);
        assert_eq!(facts.withdrawal_usd, 0.0);
        assert_eq!(facts.exit_gas_native, 0.0);
        assert!(quality.notes.iter().any(|n| n.contains("value_flows")));
    }

    #[test]
    fn deployment_receipt_gas_is_setup_cost() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.deployment = Some(DeploymentEvent {
            tx_hash: "0xdeploy".into(),
            timestamp: Some(1),
            gas_native: Some(0.25),
            fee_payer: Some("0xop".into()),
        });
        evidence.prices = vec![PriceBucket {
            chain: "ethereum".into(),
            day_utc: 0,
            symbol: "ETH".into(),
            token_address: None,
            usd_per_native: 2_000.0,
        }];
        evidence.quality.gas = EvidenceStatus::Complete;

        let (facts, _) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );
        assert_eq!(facts.setup_gas_native, 0.25);
        assert_eq!(facts.setup_gas_usd, 500.0);
        assert_eq!(facts.total_gas_usd, 500.0);
        assert_eq!(facts.gas_contributions.len(), 1);
        assert_eq!(facts.gas_contributions[0].stage, GasStage::Setup);
    }

    #[test]
    fn paid_mint_loss_counts_when_honest_holder_has_payment() {
        use crate::enrich::HolderRecord;

        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.transfers = vec![TransferEvent {
            tx_hash: "mint-tx".into(),
            token_id: "1".into(),
            from: "0x0000000000000000000000000000000000000000".into(),
            to: "0xvictim".into(),
            timestamp: Some(10),
            block_number: Some(10),
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: Some(0.08),
            mint_payment_usd: Some(160.0),
            mint_payment_receiver: Some("0xop".into()),
        }];
        evidence.holders = vec![HolderRecord {
            token_id: "1".into(),
            owner: "0xvictim".into(),
            balance: Some(1),
        }];
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.holders = EvidenceStatus::Complete;

        let mut roles = BTreeMap::new();
        roles.insert("0xvictim".into(), AddressRole::LikelyVictim);

        let (facts, _) = compute_economics(&evidence, &roles, 100, &LifecycleFacts::default());
        assert_eq!(facts.paid_mint_loss_native, 0.08);
        assert_eq!(facts.paid_mint_loss_usd, 160.0);
        assert_eq!(facts.honest_loss_native, 0.08);
        assert_eq!(facts.honest_loss_usd, 160.0);
        assert_eq!(facts.stuck_nft_count, 1);
    }

    #[test]
    fn free_mint_does_not_add_paid_mint_loss() {
        use crate::enrich::HolderRecord;

        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.transfers = vec![TransferEvent {
            tx_hash: "mint-tx".into(),
            token_id: "1".into(),
            from: "0x0000000000000000000000000000000000000000".into(),
            to: "0xvictim".into(),
            timestamp: Some(10),
            block_number: Some(10),
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }];
        evidence.holders = vec![HolderRecord {
            token_id: "1".into(),
            owner: "0xvictim".into(),
            balance: Some(1),
        }];
        let mut roles = BTreeMap::new();
        roles.insert("0xvictim".into(), AddressRole::LikelyVictim);
        let (facts, _) = compute_economics(&evidence, &roles, 100, &LifecycleFacts::default());
        assert_eq!(facts.paid_mint_loss_native, 0.0);
        assert_eq!(facts.paid_mint_loss_usd, 0.0);
    }

    #[test]
    fn unknown_seller_net_proceeds_are_not_reported_as_operator_output() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.transfers = vec![TransferEvent {
            tx_hash: "gas-tx".into(),
            token_id: "1".into(),
            from: "0xop".into(),
            to: "0xbuyer".into(),
            timestamp: Some(10),
            block_number: Some(10),
            is_mint: false,
            gas_native: Some(0.1),
            fee_payer: Some("0xop".into()),
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }];
        evidence.sales = vec![SaleEvent {
            tx_hash: "sale-tx".into(),
            token_id: "1".into(),
            seller: "0xop".into(),
            buyer: "0xbuyer".into(),
            timestamp: Some(11),
            block_number: Some(11),
            marketplace: Some("opensea".into()),
            native_amount: Some(1.0),
            usd_amount: Some(2_000.0),
            currency_symbol: Some("ETH".into()),
            currency_address: None,
            seller_proceeds_native: None,
            seller_proceeds_usd: None,
            ..SaleEvent::default()
        }];
        evidence.prices = vec![crate::enrich::PriceBucket {
            chain: "ethereum".into(),
            day_utc: 0,
            symbol: "ETH".into(),
            token_address: None,
            usd_per_native: 2_000.0,
        }];
        evidence.quality.gas = EvidenceStatus::Complete;

        let (facts, quality) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );
        assert_eq!(facts.operator_output_usd, 0.0);
        assert_eq!(facts.unknown_operator_sale_proceeds_count, 1);
        assert_eq!(facts.output_input_ratio, None);
        assert!(
            quality
                .notes
                .iter()
                .any(|note| note.contains("net proceeds"))
        );
    }

    #[test]
    fn value_flows_truncated_still_computes_available_edges() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.value_flows = vec![edge(
            "fund-tx",
            "0xfunder",
            "0xop",
            ValueFlowKind::Funding,
            2.5,
            500.0,
        )];
        evidence.quality.value_flows = EvidenceStatus::Truncated;

        let (facts, quality) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );

        assert_eq!(quality.value_flows, EvidenceStatus::Truncated);
        assert_eq!(facts.funding_native, 2.5);
        assert_eq!(facts.funding_usd, 500.0);
        assert!(!quality.notes.iter().any(|n| n.contains("value_flows")));
    }

    #[test]
    fn truncated_gas_keeps_observed_receipt_costs() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcand");
        evidence.controllers = vec!["0xop".into()];
        evidence.transfers = vec![TransferEvent {
            tx_hash: "0xknown".into(),
            token_id: "1".into(),
            from: "0xop".into(),
            to: "0xbuyer".into(),
            timestamp: Some(10),
            block_number: Some(10),
            is_mint: false,
            gas_native: Some(0.25),
            fee_payer: Some("0xop".into()),
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }];
        evidence.quality.gas = EvidenceStatus::Truncated;
        evidence.prices = vec![crate::enrich::PriceBucket {
            chain: "ethereum".into(),
            day_utc: 0,
            symbol: "ETH".into(),
            token_address: None,
            usd_per_native: 2_000.0,
        }];

        let (facts, quality) = compute_economics(
            &evidence,
            &roles_op("0xop"),
            100,
            &LifecycleFacts::default(),
        );
        assert_eq!(quality.gas, EvidenceStatus::Truncated);
        assert_eq!(facts.total_gas_native, 0.25);
        assert_eq!(facts.total_gas_usd, 500.0);
        assert_eq!(facts.gas_contributions.len(), 1);
    }
}
