//! Address role attribution with weighted evidence.

use std::collections::{BTreeMap, BTreeSet};

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use super::graph::AddressGraph;
use crate::enrich::roles::{HolderSnapshot, victim_addresses};
use crate::enrich::{EvidenceBundle, ValueFlowKind, normalize_chain_address};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressRole {
    SuspectedOperator,
    LikelyVictim,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressEvidenceKind {
    ControllerOrAuthority,
    DeploymentFeePayer,
    CurrentHolder,
    EventSender,
    EventRecipient,
    MintRecipient,
    PaidAcquisition,
    SubsequentPropagation,
    MaliciousSaleCycle,
    HighVolumeSeller,
    StarDistributor,
    WithdrawalRecipient,
    CashoutParticipant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddressEvidence {
    pub evidence_type: AddressEvidenceKind,
    pub token_id: Option<String>,
    pub transaction: Option<String>,
    pub weight: f64,
    pub confidence: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddressAttribution {
    pub role: AddressRole,
    pub evidence: Vec<AddressEvidence>,
}

pub struct AttributionResult {
    pub roles: BTreeMap<String, AddressRole>,
    pub records: BTreeMap<String, AddressAttribution>,
}

fn norm_addr(chain: &str, address: &str) -> String {
    let normalized = normalize_chain_address(chain, address);
    if !chain.eq_ignore_ascii_case("solana")
        && normalized == "0x0000000000000000000000000000000000000000"
    {
        String::new()
    } else {
        normalized
    }
}

pub fn attribute_addresses(
    evidence: &EvidenceBundle,
    transfer_graph: &AddressGraph,
) -> AttributionResult {
    // Normalize all address keys so controller checksums match sale/transfer casing.
    let controller_set = evidence
        .controllers
        .iter()
        .map(|a| norm_addr(&evidence.chain, a))
        .filter(|a| !a.is_empty())
        .collect::<AHashSet<_>>();
    let mut held_tokens_by_address = ahash::AHashMap::<String, AHashSet<String>>::new();
    for holder in evidence
        .holders
        .iter()
        .filter(|holder| holder.balance.is_none_or(|balance| balance > 0))
    {
        let owner = norm_addr(&evidence.chain, &holder.owner);
        if !owner.is_empty() && !holder.token_id.is_empty() {
            held_tokens_by_address
                .entry(owner)
                .or_default()
                .insert(holder.token_id.clone());
        }
    }
    let holder_set = held_tokens_by_address
        .keys()
        .cloned()
        .collect::<AHashSet<_>>();

    let mut paid_tokens_by_buyer = ahash::AHashMap::<String, AHashSet<String>>::new();
    let mut operator_evidence = controller_set.clone();
    let deployment_fee_payer = evidence
        .deployment
        .as_ref()
        .and_then(|deployment| deployment.fee_payer.as_deref())
        .map(|address| norm_addr(&evidence.chain, address))
        .filter(|address| !address.is_empty());
    operator_evidence.extend(deployment_fee_payer.iter().cloned());
    let mut seller_counts = AHashMap::<String, usize>::new();

    for sale in &evidence.sales {
        let paid = sale.native_amount.unwrap_or(0.0) > 0.0 || sale.usd_amount.unwrap_or(0.0) > 0.0;
        let buyer = norm_addr(&evidence.chain, &sale.buyer);
        if paid && !buyer.is_empty() && !sale.token_id.is_empty() {
            paid_tokens_by_buyer
                .entry(buyer)
                .or_default()
                .insert(sale.token_id.clone());
        }
        let seller = norm_addr(&evidence.chain, &sale.seller);
        if !seller.is_empty() {
            *seller_counts.entry(seller).or_default() += 1;
        }
    }
    for transfer in &evidence.transfers {
        let to = norm_addr(&evidence.chain, &transfer.to);
        if transfer.is_mint {
            let paid = transfer.mint_payment_native.unwrap_or(0.0) > 0.0
                || transfer.mint_payment_usd.unwrap_or(0.0) > 0.0;
            if paid && !to.is_empty() && !transfer.token_id.is_empty() {
                paid_tokens_by_buyer
                    .entry(to.clone())
                    .or_default()
                    .insert(transfer.token_id.clone());
            }
            // Free/paid mint recipient is often an operator seed when also a controller;
            // otherwise mint alone does not imply operator.
            if controller_set.contains(&to) {
                operator_evidence.insert(to);
            }
        }
    }
    operator_evidence.extend(
        seller_counts
            .iter()
            .filter_map(|(seller, count)| (*count >= 3).then_some(seller.clone())),
    );
    for (vertex, address) in transfer_graph.addresses.iter().enumerate() {
        let distinct_targets =
            transfer_graph.offsets[vertex + 1].saturating_sub(transfer_graph.offsets[vertex]);
        if distinct_targets >= 3 {
            let address = norm_addr(&evidence.chain, address);
            if !address.is_empty() {
                operator_evidence.insert(address);
            }
        }
    }
    for edge in &evidence.value_flows {
        let to = norm_addr(&evidence.chain, &edge.to);
        if to.is_empty() {
            continue;
        }
        match edge.kind {
            ValueFlowKind::Withdrawal => {
                operator_evidence.insert(to);
            }
            ValueFlowKind::Cashout | ValueFlowKind::Funding | ValueFlowKind::RevenueBackflow => {}
        }
    }
    let victims = victim_addresses(
        &evidence.chain,
        &evidence.transfers,
        &evidence.sales,
        HolderSnapshot {
            records: &evidence.holders,
            status: evidence.quality.holders,
        },
    );

    let mut all = BTreeSet::new();
    all.extend(controller_set.iter().cloned());
    all.extend(deployment_fee_payer.iter().cloned());
    all.extend(holder_set.iter().cloned());
    all.extend(paid_tokens_by_buyer.keys().cloned());
    for sale in &evidence.sales {
        let s = norm_addr(&evidence.chain, &sale.seller);
        let b = norm_addr(&evidence.chain, &sale.buyer);
        if !s.is_empty() {
            all.insert(s);
        }
        if !b.is_empty() {
            all.insert(b);
        }
    }
    for transfer in &evidence.transfers {
        let f = norm_addr(&evidence.chain, &transfer.from);
        let t = norm_addr(&evidence.chain, &transfer.to);
        if !f.is_empty() {
            all.insert(f);
        }
        if !t.is_empty() {
            all.insert(t);
        }
    }
    for edge in &evidence.value_flows {
        let from = norm_addr(&evidence.chain, &edge.from);
        let to = norm_addr(&evidence.chain, &edge.to);
        if !from.is_empty() {
            all.insert(from);
        }
        if !to.is_empty() {
            all.insert(to);
        }
    }

    let roles = all
        .into_iter()
        .filter(|address| !address.is_empty())
        .map(|address| {
            let role = if victims.contains(&address) {
                AddressRole::LikelyVictim
            } else {
                AddressRole::SuspectedOperator
            };
            (address, role)
        })
        .collect::<BTreeMap<_, _>>();

    let sale_graph = AddressGraph::from_sales(&evidence.sales);
    let sale_components = sale_graph.strongly_connected_components();

    let mut evidence_rows = roles
        .keys()
        .cloned()
        .map(|address| (address, Vec::new()))
        .collect::<BTreeMap<_, Vec<AddressEvidence>>>();

    for controller in &evidence.controllers {
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            controller,
            AddressEvidenceKind::ControllerOrAuthority,
            None,
            None,
            1.0,
            1.0,
        );
    }
    if let Some(deployment) = evidence.deployment.as_ref()
        && let Some(payer) = deployment.fee_payer.as_deref()
    {
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            payer,
            AddressEvidenceKind::DeploymentFeePayer,
            None,
            Some(deployment.tx_hash.clone()),
            1.0,
            1.0,
        );
    }
    for holder in evidence
        .holders
        .iter()
        .filter(|holder| holder.balance.is_none_or(|balance| balance > 0))
    {
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            &holder.owner,
            AddressEvidenceKind::CurrentHolder,
            Some(holder.token_id.clone()),
            None,
            0.5,
            0.75,
        );
    }
    for (seller, count) in &seller_counts {
        if *count >= 3 {
            push_evidence(
                &mut evidence_rows,
                &evidence.chain,
                seller,
                AddressEvidenceKind::HighVolumeSeller,
                None,
                None,
                0.75,
                0.85,
            );
        }
    }
    for (vertex, address) in transfer_graph.addresses.iter().enumerate() {
        let distinct_targets =
            transfer_graph.offsets[vertex + 1].saturating_sub(transfer_graph.offsets[vertex]);
        if distinct_targets >= 3 {
            push_evidence(
                &mut evidence_rows,
                &evidence.chain,
                address,
                AddressEvidenceKind::StarDistributor,
                None,
                None,
                0.8,
                0.9,
            );
        }
    }
    for edge in &evidence.value_flows {
        let (kind, weight, confidence) = match edge.kind {
            ValueFlowKind::Withdrawal => (AddressEvidenceKind::WithdrawalRecipient, 0.85, 0.95),
            ValueFlowKind::Cashout => (AddressEvidenceKind::CashoutParticipant, 0.65, 0.8),
            ValueFlowKind::Funding | ValueFlowKind::RevenueBackflow => continue,
        };
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            &edge.to,
            kind,
            None,
            Some(edge.tx_hash.clone()),
            weight,
            confidence,
        );
    }
    for transfer in &evidence.transfers {
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            &transfer.from,
            AddressEvidenceKind::EventSender,
            Some(transfer.token_id.clone()),
            Some(transfer.tx_hash.clone()),
            0.1,
            0.25,
        );
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            &transfer.to,
            AddressEvidenceKind::EventRecipient,
            Some(transfer.token_id.clone()),
            Some(transfer.tx_hash.clone()),
            0.1,
            0.25,
        );
        if transfer.is_mint {
            push_evidence(
                &mut evidence_rows,
                &evidence.chain,
                &transfer.to,
                AddressEvidenceKind::MintRecipient,
                Some(transfer.token_id.clone()),
                Some(transfer.tx_hash.clone()),
                0.6,
                0.7,
            );
        } else {
            push_evidence(
                &mut evidence_rows,
                &evidence.chain,
                &transfer.from,
                AddressEvidenceKind::SubsequentPropagation,
                Some(transfer.token_id.clone()),
                Some(transfer.tx_hash.clone()),
                0.7,
                0.8,
            );
        }
    }
    for sale in &evidence.sales {
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            &sale.seller,
            AddressEvidenceKind::EventSender,
            Some(sale.token_id.clone()),
            Some(sale.tx_hash.clone()),
            0.1,
            0.25,
        );
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            &sale.buyer,
            AddressEvidenceKind::EventRecipient,
            Some(sale.token_id.clone()),
            Some(sale.tx_hash.clone()),
            0.1,
            0.25,
        );
        let paid = sale.native_amount.unwrap_or(0.0) > 0.0 || sale.usd_amount.unwrap_or(0.0) > 0.0;
        if paid {
            push_evidence(
                &mut evidence_rows,
                &evidence.chain,
                &sale.buyer,
                AddressEvidenceKind::PaidAcquisition,
                Some(sale.token_id.clone()),
                Some(sale.tx_hash.clone()),
                0.8,
                0.9,
            );
        }
        push_evidence(
            &mut evidence_rows,
            &evidence.chain,
            &sale.seller,
            AddressEvidenceKind::SubsequentPropagation,
            Some(sale.token_id.clone()),
            Some(sale.tx_hash.clone()),
            0.7,
            0.8,
        );
    }

    let mut malicious_cycle_by_address = AHashMap::<String, usize>::new();
    for (component_id, component) in sale_components.iter().enumerate() {
        if component.len() >= 2
            && component.iter().any(|&vertex| {
                operator_evidence
                    .contains(&norm_addr(&evidence.chain, &sale_graph.addresses[vertex]))
            })
        {
            for &vertex in component {
                malicious_cycle_by_address.insert(
                    norm_addr(&evidence.chain, &sale_graph.addresses[vertex]),
                    component_id,
                );
            }
        }
    }
    for sale in &evidence.sales {
        let seller = norm_addr(&evidence.chain, &sale.seller);
        let buyer = norm_addr(&evidence.chain, &sale.buyer);
        let same_cycle = malicious_cycle_by_address
            .get(&seller)
            .zip(malicious_cycle_by_address.get(&buyer))
            .is_some_and(|(left, right)| left == right);
        if same_cycle {
            for address in [&seller, &buyer] {
                push_evidence(
                    &mut evidence_rows,
                    &evidence.chain,
                    address,
                    AddressEvidenceKind::MaliciousSaleCycle,
                    Some(sale.token_id.clone()),
                    Some(sale.tx_hash.clone()),
                    0.9,
                    0.95,
                );
            }
        }
    }

    let records = roles
        .iter()
        .map(|(address, role)| {
            let mut rows = evidence_rows.remove(address).unwrap_or_default();
            rows.sort_by(|left, right| {
                (
                    left.evidence_type,
                    left.token_id.as_deref(),
                    left.transaction.as_deref(),
                )
                    .cmp(&(
                        right.evidence_type,
                        right.token_id.as_deref(),
                        right.transaction.as_deref(),
                    ))
            });
            rows.dedup_by(|left, right| {
                left.evidence_type == right.evidence_type
                    && left.token_id == right.token_id
                    && left.transaction == right.transaction
            });
            (
                address.clone(),
                AddressAttribution {
                    role: *role,
                    evidence: rows,
                },
            )
        })
        .collect();

    AttributionResult { roles, records }
}

#[allow(clippy::too_many_arguments)] // Keeps each role-evidence call explicit at the call site.
fn push_evidence(
    evidence: &mut BTreeMap<String, Vec<AddressEvidence>>,
    chain: &str,
    address: &str,
    evidence_type: AddressEvidenceKind,
    token_id: Option<String>,
    transaction: Option<String>,
    weight: f64,
    confidence: f64,
) {
    let address = norm_addr(chain, address);
    if address.is_empty() {
        return;
    }
    evidence.entry(address).or_default().push(AddressEvidence {
        evidence_type,
        token_id,
        transaction,
        weight,
        confidence,
    });
}
