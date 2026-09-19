//! Lifecycle timelines and value-flow aggregates.

use ahash::AHashSet;
use serde::{Deserialize, Serialize};

use super::attribution::AddressRole;
use crate::enrich::{EvidenceBundle, normalize_chain_address};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LifecycleFacts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_timestamp: Option<i64>,
    pub first_activity_timestamp: Option<i64>,
    pub first_mint_timestamp: Option<i64>,
    pub first_transfer_timestamp: Option<i64>,
    pub first_sale_timestamp: Option<i64>,
    pub first_victim_timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_to_first_transfer_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_to_first_sale_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_to_first_victim_seconds: Option<i64>,
    pub first_activity_to_first_victim_seconds: Option<i64>,
    pub first_victim_holding_seconds: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ValueFlowFacts {
    pub mint_edge_count: u64,
    pub transfer_edge_count: u64,
    pub sale_edge_count: u64,
    pub nft_count: u64,
    pub address_count: u64,
    #[serde(skip_serializing)]
    pub gross_revenue_native: f64,
    #[serde(rename = "gross_sales_volume_usd")]
    pub gross_revenue_usd: f64,
    #[serde(skip_serializing)]
    pub operator_revenue_native: f64,
    #[serde(rename = "operator_net_proceeds_usd")]
    pub operator_revenue_usd: f64,
    pub marketplace_fee_usd: f64,
    pub royalty_fee_usd: f64,
    pub operator_royalty_usd: f64,
    pub malicious_address_count: u64,
    pub victim_address_count: u64,
    pub currently_holding_victim_address_count: u64,
    #[serde(rename = "max_net_proceeds_receiver")]
    pub max_value_receiver: Option<String>,
    #[serde(rename = "max_net_proceeds_receiver_usd")]
    pub max_value_receiver_usd: f64,
    #[serde(rename = "max_net_proceeds_receiver_share")]
    pub max_value_receiver_share: Option<f64>,
}

pub fn build_lifecycle(
    evidence: &EvidenceBundle,
    roles: &BTreeMap<String, AddressRole>,
    analysis_timestamp: i64,
) -> LifecycleFacts {
    let deployment_timestamp = evidence.deployment_timestamp;
    let mut first_activity_timestamp = None;
    let mut first_mint_timestamp = None;
    let mut first_transfer_timestamp = None;
    let mut first_sale_timestamp = None;
    let mut first_victim_timestamp = None;
    let current_victim_tokens = evidence
        .holders
        .iter()
        .filter(|holder| holder.balance.is_none_or(|balance| balance > 0))
        .filter_map(|holder| {
            let owner = normalize_chain_address(&evidence.chain, &holder.owner);
            roles
                .get(&owner)
                .is_some_and(|role| matches!(role, AddressRole::LikelyVictim))
                .then(|| (owner, holder.token_id.clone()))
        })
        .collect::<AHashSet<_>>();

    for transfer in &evidence.transfers {
        first_activity_timestamp = minimum_time(first_activity_timestamp, transfer.timestamp);
        if transfer.is_mint {
            first_mint_timestamp = minimum_time(first_mint_timestamp, transfer.timestamp);
            let paid = transfer.mint_payment_native.unwrap_or(0.0) > 0.0
                || transfer.mint_payment_usd.unwrap_or(0.0) > 0.0;
            let recipient = normalize_chain_address(&evidence.chain, &transfer.to);
            let recipient_holds_paid_token =
                current_victim_tokens.contains(&(recipient, transfer.token_id.clone()));
            if paid && recipient_holds_paid_token {
                first_victim_timestamp = minimum_time(first_victim_timestamp, transfer.timestamp);
            }
        } else {
            first_transfer_timestamp = minimum_time(first_transfer_timestamp, transfer.timestamp);
        }
    }
    for sale in &evidence.sales {
        first_activity_timestamp = minimum_time(first_activity_timestamp, sale.timestamp);
        first_sale_timestamp = minimum_time(first_sale_timestamp, sale.timestamp);
        let paid = sale.native_amount.unwrap_or(0.0) > 0.0 || sale.usd_amount.unwrap_or(0.0) > 0.0;
        let buyer = normalize_chain_address(&evidence.chain, &sale.buyer);
        let buyer_holds_paid_token =
            current_victim_tokens.contains(&(buyer, sale.token_id.clone()));
        if paid && buyer_holds_paid_token {
            first_victim_timestamp = minimum_time(first_victim_timestamp, sale.timestamp);
        }
    }

    let first_victim_holding_seconds = first_victim_timestamp.and_then(|acquired_at| {
        let sale_acquisition = evidence
            .sales
            .iter()
            .filter(|sale| sale.timestamp == Some(acquired_at))
            .find(|sale| {
                let buyer = normalize_chain_address(&evidence.chain, &sale.buyer);
                current_victim_tokens.contains(&(buyer, sale.token_id.clone()))
            })
            .map(|sale| {
                (
                    normalize_chain_address(&evidence.chain, &sale.buyer),
                    sale.token_id.as_str(),
                )
            });
        let mint_acquisition = evidence
            .transfers
            .iter()
            .filter(|transfer| transfer.is_mint && transfer.timestamp == Some(acquired_at))
            .find(|transfer| {
                let recipient = normalize_chain_address(&evidence.chain, &transfer.to);
                let paid = transfer.mint_payment_native.unwrap_or(0.0) > 0.0
                    || transfer.mint_payment_usd.unwrap_or(0.0) > 0.0;
                paid && current_victim_tokens.contains(&(recipient, transfer.token_id.clone()))
            })
            .map(|transfer| {
                (
                    normalize_chain_address(&evidence.chain, &transfer.to),
                    transfer.token_id.as_str(),
                )
            });
        let (buyer, token) = sale_acquisition.or(mint_acquisition)?;
        let disposed_at = evidence
            .transfers
            .iter()
            .filter(|event| {
                event.token_id == token
                    && event.timestamp.is_some_and(|time| time > acquired_at)
                    && normalize_chain_address(&evidence.chain, &event.from) == buyer
            })
            .filter_map(|event| event.timestamp)
            .chain(
                evidence
                    .sales
                    .iter()
                    .filter(|sale| {
                        sale.token_id == token
                            && sale.timestamp.is_some_and(|time| time > acquired_at)
                            && normalize_chain_address(&evidence.chain, &sale.seller) == buyer
                    })
                    .filter_map(|sale| sale.timestamp),
            )
            .min();
        let still_holds = evidence.holders.iter().any(|holder| {
            holder.token_id == token
                && normalize_chain_address(&evidence.chain, &holder.owner) == buyer
                && holder.balance.unwrap_or(1) > 0
        });
        disposed_at
            .map(|disposed| disposed - acquired_at)
            .or_else(|| still_holds.then_some(analysis_timestamp - acquired_at))
            .filter(|seconds| *seconds >= 0)
    });

    LifecycleFacts {
        deployment_timestamp,
        first_activity_timestamp,
        first_mint_timestamp,
        first_transfer_timestamp,
        first_sale_timestamp,
        first_victim_timestamp,
        deployment_to_first_transfer_seconds: elapsed(
            deployment_timestamp,
            first_transfer_timestamp,
        ),
        deployment_to_first_sale_seconds: elapsed(deployment_timestamp, first_sale_timestamp),
        deployment_to_first_victim_seconds: elapsed(deployment_timestamp, first_victim_timestamp),
        first_activity_to_first_victim_seconds: elapsed(
            first_activity_timestamp,
            first_victim_timestamp,
        ),
        first_victim_holding_seconds,
    }
}

pub fn build_value_flow(
    evidence: &EvidenceBundle,
    roles: &BTreeMap<String, AddressRole>,
) -> ValueFlowFacts {
    let malicious = roles
        .iter()
        .filter(|(_, role)| matches!(role, AddressRole::SuspectedOperator))
        .map(|(address, _)| address.clone())
        .collect::<AHashSet<_>>();
    let victims = roles
        .iter()
        .filter(|(_, role)| matches!(role, AddressRole::LikelyVictim))
        .map(|(address, _)| address.clone())
        .collect::<AHashSet<_>>();
    let holding_victims = evidence
        .holders
        .iter()
        .filter(|holder| holder.balance.is_none_or(|balance| balance > 0))
        .map(|holder| normalize_chain_address(&evidence.chain, &holder.owner))
        .filter(|holder| victims.contains(holder))
        .collect::<AHashSet<_>>()
        .len() as u64;

    let mut facts = ValueFlowFacts {
        malicious_address_count: malicious.len() as u64,
        victim_address_count: victims.len() as u64,
        currently_holding_victim_address_count: holding_victims,
        ..Default::default()
    };

    let mut nfts = AHashSet::new();
    let mut addresses = AHashSet::new();
    let mut receiver_usd = BTreeMap::<String, f64>::new();
    let mut total_usd = 0.0_f64;

    let usable_address = |address: &str| {
        let address = normalize_chain_address(&evidence.chain, address);
        (!address.is_empty()
            && (evidence.chain.eq_ignore_ascii_case("solana")
                || address != "0x0000000000000000000000000000000000000000"))
            .then_some(address)
    };
    for transfer in &evidence.transfers {
        if !transfer.token_id.is_empty() {
            nfts.insert(transfer.token_id.as_str());
        }
        addresses.extend(usable_address(&transfer.from));
        addresses.extend(usable_address(&transfer.to));
        if transfer.is_mint {
            facts.mint_edge_count += 1;
        } else {
            facts.transfer_edge_count += 1;
        }
    }
    for sale in &evidence.sales {
        if !sale.token_id.is_empty() {
            nfts.insert(sale.token_id.as_str());
        }
        addresses.extend(usable_address(&sale.seller));
        addresses.extend(usable_address(&sale.buyer));
        facts.sale_edge_count += 1;
        let native = sale.native_amount.unwrap_or(0.0).max(0.0);
        let usd = sale.usd_amount.unwrap_or(0.0).max(0.0);
        facts.gross_revenue_native += native;
        facts.gross_revenue_usd += usd;
        facts.marketplace_fee_usd += sale.marketplace_fee_usd.unwrap_or(0.0).max(0.0);
        facts.royalty_fee_usd += sale.royalty_fee_usd.unwrap_or(0.0).max(0.0);
        let seller = normalize_chain_address(&evidence.chain, &sale.seller);
        if malicious.contains(&seller) {
            if let Some(net_native) = sale.seller_proceeds_native.filter(|value| *value >= 0.0) {
                facts.operator_revenue_native += net_native;
            }
            if let Some(net_usd) = sale.seller_proceeds_usd.filter(|value| *value >= 0.0) {
                facts.operator_revenue_usd += net_usd;
                *receiver_usd.entry(seller).or_default() += net_usd;
                total_usd += net_usd;
            }
        }
        let royalty_recipient = sale
            .royalty_recipient
            .as_deref()
            .map(|address| normalize_chain_address(&evidence.chain, address))
            .filter(|address| malicious.contains(address));
        if let Some(recipient) = royalty_recipient {
            facts.operator_revenue_native += sale.royalty_fee_native.unwrap_or(0.0).max(0.0);
            let royalty_usd = sale.royalty_fee_usd.unwrap_or(0.0).max(0.0);
            facts.operator_revenue_usd += royalty_usd;
            facts.operator_royalty_usd += royalty_usd;
            *receiver_usd.entry(recipient).or_default() += royalty_usd;
            total_usd += royalty_usd;
        }
    }
    for holder in evidence
        .holders
        .iter()
        .filter(|holder| holder.balance.is_none_or(|balance| balance > 0))
    {
        if !holder.token_id.is_empty() {
            nfts.insert(holder.token_id.as_str());
        }
        addresses.extend(usable_address(&holder.owner));
    }

    facts.nft_count = nfts.len() as u64;
    facts.address_count = addresses.len() as u64;
    if let Some((receiver, usd)) = receiver_usd.into_iter().max_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.0.cmp(&left.0))
    }) {
        facts.max_value_receiver = Some(receiver);
        facts.max_value_receiver_usd = usd;
        facts.max_value_receiver_share = (total_usd > 0.0).then_some(usd / total_usd);
    }
    facts
}

fn minimum_time(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn elapsed(start: Option<i64>, end: Option<i64>) -> Option<i64> {
    start
        .zip(end)
        .and_then(|(start, end)| end.checked_sub(start))
        .filter(|duration| *duration >= 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::{EvidenceStatus, HolderRecord, SaleEvent, TransferEvent};

    fn transfer(tx: &str, token: &str, from: &str, to: &str, timestamp: i64) -> TransferEvent {
        TransferEvent {
            tx_hash: tx.into(),
            token_id: token.into(),
            from: from.into(),
            to: to.into(),
            timestamp: Some(timestamp),
            block_number: None,
            is_mint: false,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }
    }

    fn sale(timestamp: i64) -> SaleEvent {
        SaleEvent {
            tx_hash: "sale".into(),
            token_id: "victim-token".into(),
            seller: "operator".into(),
            buyer: "victim".into(),
            timestamp: Some(timestamp),
            block_number: None,
            marketplace: None,
            native_amount: Some(1.0),
            usd_amount: Some(100.0),
            currency_symbol: Some("ETH".into()),
            currency_address: None,
            seller_proceeds_native: Some(1.0),
            seller_proceeds_usd: Some(100.0),
            ..SaleEvent::default()
        }
    }

    #[test]
    fn lifecycle_records_activity_and_current_paid_victim() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        evidence.deployment_timestamp = Some(0);
        evidence.sales.push(sale(10));
        evidence.holders.push(HolderRecord {
            token_id: "victim-token".into(),
            owner: "victim".into(),
            balance: Some(1),
        });
        evidence.quality.transfers = EvidenceStatus::Empty;
        evidence.quality.sales = EvidenceStatus::Complete;
        let roles = BTreeMap::from([("victim".into(), AddressRole::LikelyVictim)]);
        let facts = build_lifecycle(&evidence, &roles, 100);
        assert_eq!(facts.first_activity_timestamp, Some(10));
        assert_eq!(facts.first_victim_timestamp, Some(10));
    }

    #[test]
    fn paid_mint_is_the_first_victim_acquisition() {
        let mut evidence = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        let mut mint = transfer(
            "mint",
            "victim-token",
            "0x0000000000000000000000000000000000000000",
            "victim",
            5,
        );
        mint.is_mint = true;
        mint.mint_payment_native = Some(1.0);
        mint.mint_payment_usd = Some(100.0);
        mint.mint_payment_receiver = Some("operator".into());
        evidence.transfers.push(mint);
        evidence.holders.push(HolderRecord {
            token_id: "victim-token".into(),
            owner: "victim".into(),
            balance: Some(1),
        });
        evidence.quality.transfers = EvidenceStatus::Complete;
        evidence.quality.sales = EvidenceStatus::Empty;
        let roles = BTreeMap::from([("victim".into(), AddressRole::LikelyVictim)]);
        let facts = build_lifecycle(&evidence, &roles, 100);
        assert_eq!(facts.first_victim_timestamp, Some(5));
        assert_eq!(facts.first_victim_holding_seconds, Some(95));
    }
}
