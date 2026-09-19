//! Attach same-tx native payments onto mint `TransferEvent`s.

use ahash::AHashMap;

use super::types::{
    PriceBucket, TransferEvent, ValueFlowEdge, normalize_chain_address, normalize_chain_transaction,
};

/// Convert native amount with the run-time spot rate (any matching chain bucket).
fn usd_for_runtime(prices: &[PriceBucket], chain: &str, native: f64) -> Option<f64> {
    let rate = prices.iter().find(|p| {
        p.chain.eq_ignore_ascii_case(chain)
            && chain_matches_symbol(chain, &p.symbol)
            && p.usd_per_native > 0.0
    })?;
    if rate.usd_per_native > 0.0 {
        Some(native * rate.usd_per_native)
    } else {
        None
    }
}

/// Reprice already-attributed mint payments without re-running native payment
/// attribution or changing allocation across batch mints.
pub fn refresh_mint_payment_usd(
    transfers: &mut [TransferEvent],
    prices: &[PriceBucket],
    chain: &str,
) {
    for transfer in transfers.iter_mut().filter(|transfer| transfer.is_mint) {
        transfer.mint_payment_usd = transfer
            .mint_payment_native
            .and_then(|native| usd_for_runtime(prices, chain, native));
    }
}

fn chain_matches_symbol(chain: &str, symbol: &str) -> bool {
    match chain.trim().to_ascii_lowercase().as_str() {
        "ethereum" | "base" => symbol.eq_ignore_ascii_case("ETH"),
        "polygon" | "matic" => {
            symbol.eq_ignore_ascii_case("MATIC") || symbol.eq_ignore_ascii_case("POL")
        }
        "solana" => symbol.eq_ignore_ascii_case("SOL"),
        _ => false,
    }
}

fn payment_from_value_flows(
    mint: &TransferEvent,
    value_flows: &[ValueFlowEdge],
    chain: &str,
) -> Option<(f64, String)> {
    let tx = normalize_chain_transaction(chain, &mint.tx_hash);
    let buyer = normalize_chain_address(chain, &mint.to);
    if tx.is_empty() || buyer.is_empty() {
        return None;
    }
    let mut total = 0.0;
    let mut receivers = ahash::AHashSet::new();
    for edge in value_flows {
        if normalize_chain_transaction(chain, &edge.tx_hash) != tx {
            continue;
        }
        let amt = edge.native_amount.unwrap_or(0.0);
        if amt <= 0.0 {
            continue;
        }
        let from = normalize_chain_address(chain, &edge.from);
        // A paid mint is a buyer outflow. Counting inflows as payments reverses
        // the economic direction and can double-count refunds.
        if from == buyer {
            total += amt;
            let receiver = normalize_chain_address(chain, &edge.to);
            if !receiver.is_empty() && receiver != buyer {
                receivers.insert(receiver);
            }
        }
    }
    if total <= 0.0 || receivers.len() != 1 {
        return None;
    }
    Some((total, receivers.into_iter().next()?))
}

/// Sum same-tx buyer outflows for a paid mint.
pub fn payment_native_from_value_flows(
    mint: &TransferEvent,
    value_flows: &[ValueFlowEdge],
    chain: &str,
) -> Option<f64> {
    payment_from_value_flows(mint, value_flows, chain).map(|(amount, _)| amount)
}

/// Attach `mint_payment_*` from value-flow edges (and optional precomputed native map).
///
/// `extra_native_by_payer_tx`: `(tx_hash, payer)` → `(native amount, receiver)`
/// from EXTERNAL `from=mint.to` probes. Ambiguous multi-receiver transactions
/// are deliberately omitted by the caller.
pub fn attach_mint_payments(
    transfers: &mut [TransferEvent],
    value_flows: &[ValueFlowEdge],
    prices: &[PriceBucket],
    chain: &str,
    extra_native_by_payer_tx: &AHashMap<(String, String), (f64, String)>,
) {
    let mut mint_count_by_payer_tx = AHashMap::<(String, String), usize>::new();
    for transfer in transfers.iter().filter(|transfer| transfer.is_mint) {
        let key = (
            normalize_chain_transaction(chain, &transfer.tx_hash),
            normalize_chain_address(chain, &transfer.to),
        );
        *mint_count_by_payer_tx.entry(key).or_default() += 1;
    }
    for transfer in transfers.iter_mut() {
        if !transfer.is_mint {
            continue;
        }
        let key = (
            normalize_chain_transaction(chain, &transfer.tx_hash),
            normalize_chain_address(chain, &transfer.to),
        );
        let mut observations = Vec::with_capacity(3);
        if let Some((native, receiver)) = payment_from_value_flows(transfer, value_flows, chain) {
            observations.push((native, receiver));
        }
        if let (Some(existing), Some(receiver)) = (
            transfer.mint_payment_native.filter(|value| *value > 0.0),
            transfer.mint_payment_receiver.clone(),
        ) {
            observations.push((existing, receiver));
        }
        if let Some((extra, receiver)) = extra_native_by_payer_tx.get(&key)
            && *extra > 0.0
        {
            observations.push((*extra, receiver.clone()));
        }
        let selected = observations.into_iter().max_by(|left, right| {
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let count = mint_count_by_payer_tx
            .get(&key)
            .copied()
            .unwrap_or(1)
            .max(1);
        let Some((total_native, receiver)) = selected else {
            transfer.mint_payment_native = None;
            transfer.mint_payment_usd = None;
            transfer.mint_payment_receiver = None;
            continue;
        };
        let native = total_native / count as f64;
        transfer.mint_payment_native = Some(native);
        transfer.mint_payment_usd = usd_for_runtime(prices, chain, native);
        transfer.mint_payment_receiver = Some(receiver);
    }
}

/// Keep only payments whose recipient is the candidate contract/collection or
/// one of its verified controllers/authorities.
///
/// A same-transaction buyer outflow to an arbitrary third party is not proof
/// that the NFT mint itself was paid.
pub fn retain_controlled_mint_payments(
    transfers: &mut [TransferEvent],
    chain: &str,
    candidate: &str,
    controllers: &[String],
) {
    let candidate = normalize_chain_address(chain, candidate);
    let controlled = controllers
        .iter()
        .map(|address| normalize_chain_address(chain, address))
        .filter(|address| !address.is_empty())
        .chain((!candidate.is_empty()).then_some(candidate))
        .collect::<ahash::AHashSet<_>>();
    for transfer in transfers.iter_mut().filter(|transfer| transfer.is_mint) {
        let recipient_is_controlled = transfer
            .mint_payment_receiver
            .as_deref()
            .map(|receiver| normalize_chain_address(chain, receiver))
            .is_some_and(|receiver| controlled.contains(&receiver));
        if !recipient_is_controlled {
            transfer.mint_payment_native = None;
            transfer.mint_payment_usd = None;
            transfer.mint_payment_receiver = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::types::ValueFlowKind;

    fn mint(tx: &str, to: &str) -> TransferEvent {
        TransferEvent {
            tx_hash: tx.into(),
            token_id: "1".into(),
            from: "0x0000000000000000000000000000000000000000".into(),
            to: to.into(),
            timestamp: Some(1_700_000_000),
            block_number: Some(1),
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }
    }

    #[test]
    fn attaches_buyer_payout_from_value_flow() {
        let mut transfers = vec![mint("0xabc", "0xbuyer")];
        let flows = vec![ValueFlowEdge {
            tx_hash: "0xABC".into(),
            event_id: None,
            from: "0xBuyer".into(),
            to: "0xcontract".into(),
            kind: ValueFlowKind::Withdrawal,
            native_amount: Some(0.05),
            usd_amount: Some(100.0),
            timestamp: Some(1_700_000_000),
            gas_native: None,
            fee_payer: None,
        }];
        let prices = vec![PriceBucket {
            chain: "ethereum".into(),
            day_utc: 0,
            symbol: "ETH".into(),
            token_address: None,
            usd_per_native: 2000.0,
        }];
        attach_mint_payments(
            &mut transfers,
            &flows,
            &prices,
            "ethereum",
            &AHashMap::new(),
        );
        assert_eq!(transfers[0].mint_payment_native, Some(0.05));
        assert_eq!(transfers[0].mint_payment_usd, Some(100.0)); // 0.05 * 2000
    }

    #[test]
    fn free_mint_stays_none() {
        let mut transfers = vec![mint("0xabc", "0xbuyer")];
        attach_mint_payments(&mut transfers, &[], &[], "ethereum", &AHashMap::new());
        assert!(transfers[0].mint_payment_native.is_none());
    }

    #[test]
    fn batch_mint_payment_is_allocated_once_across_same_buyer_mints() {
        let mut transfers = vec![mint("0xbatch", "0xbuyer"), mint("0xbatch", "0xbuyer")];
        transfers[1].token_id = "2".into();
        let flows = vec![ValueFlowEdge {
            tx_hash: "0xbatch".into(),
            event_id: None,
            from: "0xbuyer".into(),
            to: "0xcontract".into(),
            kind: ValueFlowKind::Withdrawal,
            native_amount: Some(0.1),
            usd_amount: Some(200.0),
            timestamp: Some(1),
            gas_native: None,
            fee_payer: None,
        }];
        let prices = vec![PriceBucket {
            chain: "ethereum".into(),
            day_utc: 0,
            symbol: "ETH".into(),
            token_address: None,
            usd_per_native: 2_000.0,
        }];
        attach_mint_payments(
            &mut transfers,
            &flows,
            &prices,
            "ethereum",
            &AHashMap::new(),
        );
        assert_eq!(transfers[0].mint_payment_native, Some(0.05));
        assert_eq!(transfers[1].mint_payment_native, Some(0.05));
        assert_eq!(
            transfers
                .iter()
                .filter_map(|transfer| transfer.mint_payment_usd)
                .sum::<f64>(),
            200.0
        );
    }

    #[test]
    fn multi_receiver_buyer_outflow_is_not_guessed_as_mint_payment() {
        let mut transfers = vec![mint("0xmulti", "0xbuyer")];
        let flows = ["0xreceiver_a", "0xreceiver_b"]
            .into_iter()
            .map(|receiver| ValueFlowEdge {
                tx_hash: "0xmulti".into(),
                event_id: None,
                from: "0xbuyer".into(),
                to: receiver.into(),
                kind: ValueFlowKind::Withdrawal,
                native_amount: Some(0.05),
                usd_amount: Some(100.0),
                timestamp: Some(1),
                gas_native: None,
                fee_payer: None,
            })
            .collect::<Vec<_>>();
        attach_mint_payments(&mut transfers, &flows, &[], "ethereum", &AHashMap::new());
        assert!(transfers[0].mint_payment_native.is_none());
        assert!(transfers[0].mint_payment_receiver.is_none());
    }

    #[test]
    fn solana_transaction_signatures_remain_case_sensitive() {
        let mut transfers = vec![mint("AbC", "BuyerCase")];
        let flows = vec![ValueFlowEdge {
            tx_hash: "abc".into(),
            event_id: None,
            from: "BuyerCase".into(),
            to: "ReceiverCase".into(),
            kind: ValueFlowKind::Withdrawal,
            native_amount: Some(1.0),
            usd_amount: Some(100.0),
            timestamp: Some(1),
            gas_native: None,
            fee_payer: None,
        }];
        attach_mint_payments(&mut transfers, &flows, &[], "solana", &AHashMap::new());
        assert!(transfers[0].mint_payment_native.is_none());
    }

    #[test]
    fn native_mint_payment_never_uses_an_arbitrary_chain_token_quote() {
        let mut transfers = vec![mint("0xabc", "0xbuyer")];
        let mut extras = AHashMap::new();
        extras.insert(
            ("0xabc".into(), "0xbuyer".into()),
            (1.0, "0xreceiver".into()),
        );
        let prices = vec![PriceBucket {
            chain: "ethereum".into(),
            day_utc: 0,
            symbol: "USDC".into(),
            token_address: Some("0xtoken".into()),
            usd_per_native: 1.0,
        }];
        attach_mint_payments(&mut transfers, &[], &prices, "ethereum", &extras);
        assert_eq!(transfers[0].mint_payment_native, Some(1.0));
        assert!(transfers[0].mint_payment_usd.is_none());
    }

    #[test]
    fn rejects_unique_mint_outflow_to_uncontrolled_receiver() {
        let mut transfers = vec![mint("0xabc", "0xbuyer")];
        transfers[0].mint_payment_native = Some(1.0);
        transfers[0].mint_payment_usd = Some(2_000.0);
        transfers[0].mint_payment_receiver = Some("0xrouter".into());
        retain_controlled_mint_payments(
            &mut transfers,
            "ethereum",
            "0xcontract",
            &["0xowner".into()],
        );
        assert!(transfers[0].mint_payment_native.is_none());
        assert!(transfers[0].mint_payment_receiver.is_none());
    }

    #[test]
    fn keeps_mint_payment_to_candidate_or_verified_controller() {
        for receiver in ["0xcontract", "0xowner"] {
            let mut transfers = vec![mint("0xabc", "0xbuyer")];
            transfers[0].mint_payment_native = Some(1.0);
            transfers[0].mint_payment_receiver = Some(receiver.into());
            retain_controlled_mint_payments(
                &mut transfers,
                "ethereum",
                "0xcontract",
                &["0xowner".into()],
            );
            assert_eq!(transfers[0].mint_payment_native, Some(1.0));
        }
    }
}
