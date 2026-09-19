//! Shared binary victim/operator rule used before and after deep enrichment.

use ahash::AHashSet;

use super::types::{
    EvidenceStatus, HolderRecord, SaleEvent, TransferEvent, normalize_chain_address,
};

fn usable_address(chain: &str, raw: &str) -> Option<String> {
    let address = normalize_chain_address(chain, raw);
    if address.is_empty()
        || (!chain.eq_ignore_ascii_case("solana")
            && address == "0x0000000000000000000000000000000000000000")
    {
        None
    } else {
        Some(address)
    }
}

fn paid(native: Option<f64>, usd: Option<f64>) -> bool {
    native.is_some_and(|amount| amount > 0.0) || usd.is_some_and(|amount| amount > 0.0)
}

#[derive(Clone, Copy)]
pub struct HolderSnapshot<'a> {
    pub records: &'a [HolderRecord],
    pub status: EvidenceStatus,
}

/// Paid mint recipients / secondary-market buyers that still hold the same NFT
/// in the current holder snapshot.
pub(crate) fn victim_addresses(
    chain: &str,
    transfers: &[TransferEvent],
    sales: &[SaleEvent],
    holders: HolderSnapshot<'_>,
) -> AHashSet<String> {
    if !matches!(
        holders.status,
        EvidenceStatus::Complete | EvidenceStatus::Empty
    ) {
        return AHashSet::new();
    }

    let mut paid_acquisitions = AHashSet::new();
    for transfer in transfers {
        if transfer.is_mint
            && paid(transfer.mint_payment_native, transfer.mint_payment_usd)
            && let Some(recipient) = usable_address(chain, &transfer.to)
            && !transfer.token_id.is_empty()
        {
            paid_acquisitions.insert((recipient, transfer.token_id.clone()));
        }
    }
    for sale in sales {
        if paid(sale.native_amount, sale.usd_amount)
            && let Some(buyer) = usable_address(chain, &sale.buyer)
            && !sale.token_id.is_empty()
        {
            paid_acquisitions.insert((buyer, sale.token_id.clone()));
        }
    }

    let current_holdings = holders
        .records
        .iter()
        .filter(|holder| holder.balance.is_none_or(|balance| balance > 0))
        .filter_map(|holder| {
            usable_address(chain, &holder.owner)
                .filter(|_| !holder.token_id.is_empty())
                .map(|owner| (owner, holder.token_id.clone()))
        })
        .collect::<AHashSet<_>>();
    paid_acquisitions
        .intersection(&current_holdings)
        .map(|(address, _)| address.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer(from: &str, to: &str, is_mint: bool) -> TransferEvent {
        TransferEvent {
            tx_hash: "tx".into(),
            token_id: "1".into(),
            from: from.into(),
            to: to.into(),
            timestamp: Some(1),
            block_number: Some(1),
            is_mint,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: is_mint.then_some(1.0),
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }
    }

    fn paid_sale(seller: &str, buyer: &str, token_id: &str) -> SaleEvent {
        SaleEvent {
            tx_hash: format!("sale-{token_id}"),
            token_id: token_id.into(),
            seller: seller.into(),
            buyer: buyer.into(),
            native_amount: Some(1.0),
            ..SaleEvent::default()
        }
    }

    #[test]
    fn paid_buyer_without_current_holding_is_not_a_victim() {
        let transfers = vec![transfer("0xbuyer", "0xother", false)];
        let sales = vec![paid_sale("0xseller", "0xbuyer", "2")];
        assert!(
            victim_addresses(
                "ethereum",
                &transfers,
                &sales,
                HolderSnapshot {
                    records: &[],
                    status: EvidenceStatus::Empty,
                },
            )
            .is_empty()
        );
    }

    #[test]
    fn paid_buyer_still_holding_the_purchased_token_is_a_victim() {
        let victims = victim_addresses(
            "ethereum",
            &[],
            &[paid_sale("0xseller", "0xbuyer", "1")],
            HolderSnapshot {
                records: &[HolderRecord {
                    token_id: "1".into(),
                    owner: "0xbuyer".into(),
                    balance: Some(1),
                }],
                status: EvidenceStatus::Complete,
            },
        );
        assert!(victims.contains("0xbuyer"));
    }

    #[test]
    fn incomplete_holder_snapshot_never_assigns_victim() {
        let sales = vec![paid_sale("0xseller", "0xbuyer", "1")];
        for status in [
            EvidenceStatus::NotRequested,
            EvidenceStatus::Failed,
            EvidenceStatus::Truncated,
        ] {
            assert!(
                victim_addresses(
                    "ethereum",
                    &[],
                    &sales,
                    HolderSnapshot {
                        records: &[],
                        status,
                    },
                )
                .is_empty()
            );
        }
    }
}
