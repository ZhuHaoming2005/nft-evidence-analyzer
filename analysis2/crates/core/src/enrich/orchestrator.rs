//! Enrichment orchestrator: one fetch per unique candidate contract.

use ahash::AHashMap;
use futures_util::{StreamExt, stream};

use crate::dedup::candidates::CandidateRegistry;
use crate::entity::{ContractId, ResidentStore};
use crate::error::Analysis2Error;
use crate::progress::ProgressObserver;

use super::alchemy::{self, FetchOutcome};
use super::controllers;
use super::etherscan;
use super::helius::{self, holders_from_assets};
use super::http::HttpClient;
use super::legit_detect;
use super::mint_payment;
use super::opensea;
use super::roles::HolderSnapshot;
use super::types::{
    ApiKeys, EvidenceBundle, EvidenceStatus, HttpLimits, SaleEvent, TransferEvent,
    finalize_legit_signals,
};
use super::value_flow;

/// Candidate tasks may slightly exceed HTTP slots so waiters pipeline behind
/// [`HttpClient`]. Kept low: each candidate fans out to many nested RPCs, and
/// a large multiplier with high `--http-concurrency` causes mass timeouts.
const CANDIDATE_TASK_MULTIPLIER: usize = 2;

type BundleHook<'a> = dyn FnMut(&EvidenceBundle) -> Result<(), Analysis2Error> + 'a;

/// Enrich each unique candidate once; missing keys → `not_requested`, continue.
pub async fn enrich_candidates(
    registry: &CandidateRegistry,
    store: &ResidentStore,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
) -> Result<AHashMap<ContractId, EvidenceBundle>, Analysis2Error> {
    enrich_candidates_with_hook(registry, store, keys, limits, progress, None).await
}

/// Like [`enrich_candidates`], but invokes `on_bundle` after each candidate is
/// finalized (for incremental disk checkpoints).
pub async fn enrich_candidates_with_hook(
    registry: &CandidateRegistry,
    store: &ResidentStore,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
    mut on_bundle: Option<&mut BundleHook<'_>>,
) -> Result<AHashMap<ContractId, EvidenceBundle>, Analysis2Error> {
    let client = HttpClient::with_retries_and_cache_since(
        limits.concurrency.max(1),
        limits.retries,
        limits.success_response_cache_dir.clone(),
        limits.success_response_cache_min_unix,
    )?;
    progress.set_stage("enrich_legit");
    let preflight =
        legit_detect::prefilter_candidates(registry, store, &client, keys, limits, progress)
            .await?;
    let legit_detect::LegitPreflight {
        evidence,
        candidates_to_enrich: candidates,
        mut candidate_probes,
    } = preflight;
    let mut out = evidence;
    let enrich_set: ahash::AHashSet<ContractId> = candidates.iter().copied().collect();

    // Persist legit-only preflight rows immediately so a cancel mid-HTTP still
    // keeps relation_legit for fully-legit candidates that skip deep enrich.
    if let Some(cb) = on_bundle.as_mut() {
        for (&cid, bundle) in &out {
            if !enrich_set.contains(&cid) {
                cb(bundle)?;
            }
        }
    }

    progress.set_stage("enrich");
    progress.begin_phase("enrich_candidates", Some(candidates.len() as u64));

    // Per-provider HTTP gates live in HttpClient (Alchemy/OpenSea/Helius/…).
    // Candidate tasks use a higher outer slot count so they do not hold a task
    // permit across nested RPCs.
    let task_slots = limits
        .concurrency
        .max(1)
        .saturating_mul(CANDIDATE_TASK_MULTIPLIER);
    let price_cache = alchemy::PriceRequestCache::default();
    let external_transfer_cache = value_flow::ExternalTransferCache::default();
    let receipt_cache = alchemy::ReceiptRequestCache::default();
    let solana_transaction_cache = helius::TransactionRequestCache::default();

    let candidate_work: Vec<_> = candidates
        .into_iter()
        .map(|contract_id| (contract_id, candidate_probes.remove(&contract_id)))
        .collect();
    let mut stream = stream::iter(candidate_work.into_iter().map(|(contract_id, probe)| {
        let client = client.clone();
        let keys = keys.clone();
        let limits = limits.clone();
        let chain = store
            .chain_name(store.contracts[contract_id as usize].chain_id)
            .to_owned();
        let address = store.contracts[contract_id as usize].address.clone();
        let is_evm = store.is_evm_chain(&chain);
        let known_mints = if is_evm {
            Vec::new()
        } else {
            store
                .nfts_for_contract(contract_id)
                .iter()
                .map(|&nft_id| {
                    store
                        .string(store.nfts[nft_id as usize].token_id_id)
                        .to_owned()
                })
                .collect::<Vec<_>>()
        };
        let price_cache = price_cache.clone();
        let external_transfer_cache = external_transfer_cache.clone();
        let receipt_cache = receipt_cache.clone();
        let solana_transaction_cache = solana_transaction_cache.clone();
        async move {
            let bundle = if is_evm {
                enrich_evm(
                    contract_id,
                    &chain,
                    &address,
                    &client,
                    &keys,
                    &limits,
                    probe,
                    &price_cache,
                    &external_transfer_cache,
                    &receipt_cache,
                )
                .await
            } else {
                enrich_solana(
                    contract_id,
                    &chain,
                    &address,
                    &client,
                    &keys,
                    &limits,
                    probe,
                    &known_mints,
                    &price_cache,
                    &solana_transaction_cache,
                )
                .await
            };
            (contract_id, bundle)
        }
    }))
    .buffer_unordered(task_slots);

    while let Some((contract_id, mut bundle)) = stream.next().await {
        progress.check_cancelled()?;
        if let Some(preflight) = out.get_mut(&contract_id) {
            bundle.relation_legit = std::mem::take(&mut preflight.relation_legit);
            bundle.legit = std::mem::take(&mut preflight.legit);
        }
        finalize_legit_signals(&mut bundle);
        if let Some(cb) = on_bundle.as_mut() {
            cb(&bundle)?;
        }
        out.insert(contract_id, bundle);
        progress.add_completed(1);
    }

    Ok(out)
}

/// Refresh only seed-scoped legitimacy evidence for already enriched
/// candidates. This stops after preflight and avoids market, receipt, price,
/// history, and value-flow requests when only the seed list changed.
pub async fn refresh_relation_legit(
    registry: &CandidateRegistry,
    store: &ResidentStore,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
) -> Result<AHashMap<ContractId, EvidenceBundle>, Analysis2Error> {
    let client = HttpClient::with_retries_and_cache_since(
        limits.concurrency.max(1),
        limits.retries,
        limits.success_response_cache_dir.clone(),
        limits.success_response_cache_min_unix,
    )?;
    progress.set_stage("enrich_legit");
    let preflight =
        legit_detect::prefilter_candidates(registry, store, &client, keys, limits, progress)
            .await?;
    Ok(preflight.evidence)
}

/// Refresh run-time USD quotes in cached evidence without re-fetching chain or
/// market history. Price-dependent sale, value-flow, and mint fields are
/// recomputed from their retained native amounts.
pub async fn refresh_cached_prices(
    evidence: &mut AHashMap<ContractId, EvidenceBundle>,
    candidates: &ahash::AHashSet<ContractId>,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    let client = HttpClient::with_retries_and_cache_since(
        limits.concurrency.max(1),
        limits.retries,
        limits.success_response_cache_dir.clone(),
        limits.success_response_cache_min_unix,
    )?;
    let price_cache = alchemy::PriceRequestCache::default();
    progress.set_stage("enrich_prices");
    progress.begin_phase("refresh_cached_prices", Some(candidates.len() as u64));
    let task_slots = limits.concurrency.max(1);
    let mut stream = stream::iter(
        evidence
            .iter_mut()
            .filter(|(candidate_id, _)| candidates.contains(candidate_id))
            .map(|(_, bundle)| {
                let client = client.clone();
                let keys = keys.clone();
                let limits = limits.clone();
                let price_cache = price_cache.clone();
                async move {
                    let symbols = price_symbols_for_sales(&bundle.sales);
                    let addresses = price_addresses_for_sales(&bundle.sales, &bundle.chain);
                    let prices = price_cache
                        .fetch(
                            &client,
                            &limits.endpoints,
                            keys.alchemy(),
                            &bundle.chain,
                            &symbols,
                            &addresses,
                        )
                        .await;
                    bundle
                        .quality
                        .failures
                        .retain(|failure| !failure.to_ascii_lowercase().contains("alchemy_prices"));
                    apply_outcome(
                        &mut bundle.quality.prices,
                        &mut bundle.provenance,
                        &mut bundle.quality.failures,
                        &prices,
                    );
                    bundle.prices = prices.value;
                    apply_prices_to_sales(&mut bundle.sales, &bundle.prices, &bundle.chain);
                    apply_runtime_price_to_value_flows(
                        &mut bundle.value_flows,
                        &bundle.prices,
                        &bundle.chain,
                    );
                    mint_payment::refresh_mint_payment_usd(
                        &mut bundle.transfers,
                        &bundle.prices,
                        &bundle.chain,
                    );
                }
            }),
    )
    .buffer_unordered(task_slots);
    while stream.next().await.is_some() {
        progress.check_cancelled()?;
        progress.add_completed(1);
    }
    Ok(())
}

/// Refresh EVM holder snapshots without repeating transfers, sales, receipts,
/// prices, deployment, controllers, or value-flow calls.
pub async fn refresh_cached_evm_holders(
    evidence: &mut AHashMap<ContractId, EvidenceBundle>,
    candidates: &ahash::AHashSet<ContractId>,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    let client = HttpClient::with_retries_and_cache_since(
        limits.concurrency.max(1),
        limits.retries,
        limits.success_response_cache_dir.clone(),
        limits.success_response_cache_min_unix,
    )?;
    progress.set_stage("enrich_holders");
    progress.begin_phase("refresh_cached_holders", Some(candidates.len() as u64));
    let mut stream = stream::iter(
        evidence
            .iter_mut()
            .filter(|(candidate_id, _)| candidates.contains(candidate_id))
            .map(|(_, bundle)| {
                let client = client.clone();
                let keys = keys.clone();
                let limits = limits.clone();
                async move {
                    let holders = alchemy::fetch_holders(
                        &client,
                        &limits.endpoints,
                        keys.alchemy(),
                        &bundle.chain,
                        &bundle.address,
                        limits.max_holder_pages,
                    )
                    .await;
                    bundle.quality.failures.retain(|failure| {
                        !failure.to_ascii_lowercase().contains("alchemy_holders")
                    });
                    apply_outcome(
                        &mut bundle.quality.holders,
                        &mut bundle.provenance,
                        &mut bundle.quality.failures,
                        &holders,
                    );
                    bundle.holders = holders.value;
                }
            }),
    )
    .buffer_unordered(limits.concurrency.max(1));
    while stream.next().await.is_some() {
        progress.check_cancelled()?;
        progress.add_completed(1);
    }
    Ok(())
}

async fn fetch_evm_sales(
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    chain: &str,
    address: &str,
    prefetched_slug: Option<&str>,
) -> FetchOutcome<Vec<SaleEvent>> {
    if matches!(
        chain.trim().to_ascii_lowercase().as_str(),
        "ethereum" | "polygon" | "matic"
    ) {
        let mut alchemy = alchemy::fetch_sales(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            address,
            limits.max_sale_pages,
        )
        .await;
        if matches!(
            alchemy.status,
            EvidenceStatus::Complete | EvidenceStatus::Empty | EvidenceStatus::Truncated
        ) {
            return alchemy;
        }
        let alchemy_failure = alchemy.failure.take();
        let mut fallback = opensea::fetch_contract_sales_with_slug(
            client,
            &limits.endpoints.opensea,
            keys.opensea(),
            chain,
            address,
            limits.max_sale_pages,
            prefetched_slug,
        )
        .await;
        if let Some(failure) = alchemy_failure {
            fallback.failure = Some(match fallback.failure.take() {
                Some(other) => format!("{failure}; {other}"),
                None => failure,
            });
        }
        return fallback;
    }

    opensea::fetch_contract_sales_with_slug(
        client,
        &limits.endpoints.opensea,
        keys.opensea(),
        chain,
        address,
        limits.max_sale_pages,
        prefetched_slug,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn enrich_evm(
    contract_id: ContractId,
    chain: &str,
    address: &str,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    prefetch: Option<legit_detect::CandidateProbe>,
    price_cache: &alchemy::PriceRequestCache,
    external_transfer_cache: &value_flow::ExternalTransferCache,
    receipt_cache: &alchemy::ReceiptRequestCache,
) -> EvidenceBundle {
    let mut bundle = EvidenceBundle::empty(contract_id, chain, address);
    bundle.quality.gas = EvidenceStatus::NotRequested;
    bundle.quality.value_flows = EvidenceStatus::NotRequested;

    // These provider calls are independent. Starting them together removes
    // three full network round trips from the common EVM critical path.
    let (prefetched_controllers, prefetched_slug) = match prefetch {
        Some(probe) => (probe.evm_controllers, probe.collection_slug),
        None => (None, None),
    };
    let controllers = async {
        match prefetched_controllers {
            Some(outcome) => outcome,
            None => {
                controllers::fetch_evm_controllers(
                    client,
                    &limits.endpoints,
                    keys.alchemy(),
                    chain,
                    address,
                )
                .await
            }
        }
    };
    let (mut transfers, holders, mut sales, controllers_out) = tokio::join!(
        alchemy::fetch_transfers(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            address,
            limits.max_transfer_pages,
        ),
        alchemy::fetch_holders(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            address,
            limits.max_holder_pages,
        ),
        fetch_evm_sales(
            client,
            keys,
            limits,
            chain,
            address,
            prefetched_slug.as_deref(),
        ),
        controllers,
    );

    if matches!(
        transfers.status,
        EvidenceStatus::Failed | EvidenceStatus::NotRequested
    ) {
        let fallback = etherscan::fetch_transfers(
            client,
            &limits.endpoints.etherscan,
            keys.etherscan(),
            chain,
            address,
            limits.max_transfer_pages,
        )
        .await;
        if !matches!(fallback.status, EvidenceStatus::NotRequested) {
            if matches!(transfers.status, EvidenceStatus::Failed)
                && let Some(failure) = transfers.failure.take()
            {
                bundle.quality.failures.push(failure);
            }
            transfers = fallback;
        }
    }

    let deployed_block = controllers_out.value.deployed_block;

    // Spot price, activity receipt gas, and deployment receipt gas are independent.
    let tx_hashes = alchemy::collect_unique_tx_hashes(&transfers.value, &sales.value);
    let payment_symbols = price_symbols_for_sales(&sales.value);
    let payment_addresses = price_addresses_for_sales(&sales.value, chain);
    let (prices, gas, deployment, royalty_recipients) = tokio::join!(
        price_cache.fetch(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            &payment_symbols,
            &payment_addresses,
        ),
        receipt_cache.fetch(client, &limits.endpoints, keys.alchemy(), chain, &tx_hashes,),
        alchemy::fetch_deployment(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            address,
            deployed_block,
        ),
        alchemy::fetch_royalty_recipients(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            address,
            &sales.value,
        ),
    );
    apply_prices_to_sales(&mut sales.value, &prices.value, chain);
    alchemy::attach_royalty_recipients(&mut sales.value, &royalty_recipients.value);
    alchemy::attach_receipt_gas(&mut transfers.value, &gas.value);
    alchemy::attach_sale_receipt_gas(&mut sales.value, &gas.value);

    apply_outcome(
        &mut bundle.quality.transfers,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &transfers,
    );
    bundle.transfers = transfers.value;

    apply_outcome(
        &mut bundle.quality.holders,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &holders,
    );
    let holder_population_complete = matches!(
        holders.status,
        EvidenceStatus::Complete | EvidenceStatus::Empty
    ) && !holders.truncated
        && holders
            .value
            .iter()
            .all(|holder| !holder.token_id.trim().is_empty());
    if holder_population_complete {
        bundle.collection_nft_count = Some(
            holders
                .value
                .iter()
                .filter(|holder| holder.balance.is_none_or(|balance| balance > 0))
                .map(|holder| holder.token_id.as_str())
                .collect::<std::collections::BTreeSet<_>>()
                .len() as u64,
        );
        bundle.collection_nft_count_complete = true;
    }
    bundle.holders = holders.value;

    apply_outcome(
        &mut bundle.quality.sales,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &sales,
    );
    bundle.sales = sales.value;

    apply_outcome(
        &mut bundle.quality.prices,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &prices,
    );
    bundle.prices = prices.value;

    apply_outcome(
        &mut bundle.quality.gas,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &gas,
    );
    bundle.quality.gas = combine_required_status(Some(bundle.quality.gas), Some(deployment.status));
    if let Some(observation) = deployment.observation {
        bundle.provenance.push(observation);
    }
    if let Some(failure) = deployment.failure {
        bundle.quality.failures.push(failure);
    }
    bundle.deployment_timestamp = deployment.value.as_ref().and_then(|event| event.timestamp);
    bundle.deployment = deployment.value;
    if let Some(observation) = royalty_recipients.observation {
        bundle.provenance.push(observation);
    }
    if let Some(failure) = royalty_recipients.failure {
        bundle.quality.failures.push(failure);
    }

    // Controllers before value-flow so operator seeds include on-chain owners.
    if let Some(obs) = controllers_out.observation {
        bundle.provenance.push(obs);
    }
    if let Some(failure) = controllers_out.failure {
        bundle.quality.failures.push(failure);
    }
    bundle.controllers = controllers_out.value.addresses;

    let mut preliminary_operator_seeds = bundle.controllers.clone();
    preliminary_operator_seeds.push(address.to_owned());
    if let Some(payer) = bundle
        .deployment
        .as_ref()
        .and_then(|deployment| deployment.fee_payer.clone())
    {
        preliminary_operator_seeds.push(payer);
    }

    // A controlled-recipient pass supplies mint-payment evidence. After payments are
    // attached, derive the final binary operator set and query that exact set.
    let (mut preliminary_flows, mint_extras) = tokio::join!(
        value_flow::fetch_evm_value_flows_cached(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            &preliminary_operator_seeds,
            &bundle.transfers,
            &bundle.sales,
            external_transfer_cache,
        ),
        collect_evm_mint_payment_extras(
            client,
            keys,
            limits,
            chain,
            &bundle.transfers,
            external_transfer_cache,
        ),
    );
    apply_runtime_price_to_value_flows(&mut preliminary_flows.value, &bundle.prices, chain);
    mint_payment::attach_mint_payments(
        &mut bundle.transfers,
        &preliminary_flows.value,
        &bundle.prices,
        chain,
        &mint_extras.payments,
    );
    mint_payment::retain_controlled_mint_payments(
        &mut bundle.transfers,
        chain,
        address,
        &bundle.controllers,
    );
    if bundle.transfers.iter().any(|transfer| transfer.is_mint)
        && !matches!(
            preliminary_flows.status,
            EvidenceStatus::Complete | EvidenceStatus::Empty
        )
        && matches!(
            bundle.quality.transfers,
            EvidenceStatus::Complete | EvidenceStatus::Empty
        )
    {
        bundle.quality.transfers = EvidenceStatus::Truncated;
        bundle.quality.failures.push(
            "mint payment attribution incomplete because the preliminary controller value-flow pass was incomplete"
                .into(),
        );
    }

    let operator_seeds = value_flow::derive_operator_seeds(
        chain,
        address,
        &bundle.controllers,
        bundle.deployment.as_ref(),
        &bundle.transfers,
        &bundle.sales,
        HolderSnapshot {
            records: &bundle.holders,
            status: bundle.quality.holders,
        },
    );
    let initial_seeds = value_flow::collect_operator_seeds(&preliminary_operator_seeds);
    let final_seeds = value_flow::collect_operator_seeds(&operator_seeds);
    let value_flows = if initial_seeds == final_seeds {
        preliminary_flows
    } else {
        if let Some(observation) = preliminary_flows.observation.take() {
            bundle.provenance.push(observation);
        }
        if let Some(failure) = preliminary_flows.failure.take() {
            bundle.quality.failures.push(failure);
        }
        value_flow::fetch_evm_value_flows_cached(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            &operator_seeds,
            &bundle.transfers,
            &bundle.sales,
            external_transfer_cache,
        )
        .await
    };
    apply_outcome(
        &mut bundle.quality.value_flows,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &value_flows,
    );
    bundle.value_flows = value_flows.value;
    // Receipt gas fetched above only covered NFT transfer/sale transactions.
    // Funding and cashout commonly occur in separate transactions, so attach
    // their own receipt fees before classifying Setup/Exit costs.
    alchemy::attach_value_flow_receipt_gas(&mut bundle.value_flows, &gas.value);
    let mut flow_tx_hashes = alchemy::value_flow_tx_hashes(&bundle.value_flows);
    flow_tx_hashes.retain(|hash| !gas.value.contains_key(hash));
    if !flow_tx_hashes.is_empty() {
        let flow_gas = receipt_cache
            .fetch(
                client,
                &limits.endpoints,
                keys.alchemy(),
                chain,
                &flow_tx_hashes,
            )
            .await;
        let combined = combine_required_status(Some(bundle.quality.gas), Some(flow_gas.status));
        bundle.quality.gas = combined;
        if let Some(obs) = flow_gas.observation.clone() {
            bundle.provenance.push(obs);
        }
        if let Some(failure) = flow_gas.failure.clone() {
            bundle.quality.failures.push(failure);
        }
        alchemy::attach_value_flow_receipt_gas(&mut bundle.value_flows, &flow_gas.value);
    }
    apply_runtime_price_to_value_flows(&mut bundle.value_flows, &bundle.prices, chain);
    if !mint_extras.ambiguous.is_empty()
        && matches!(
            bundle.quality.transfers,
            EvidenceStatus::Complete | EvidenceStatus::Empty
        )
    {
        bundle.quality.transfers = EvidenceStatus::Truncated;
        bundle.quality.failures.push(format!(
            "mint payment attribution ambiguous for {} payer transaction(s); excluded from formal totals",
            mint_extras.ambiguous.len()
        ));
    }

    bundle.quality.assets = EvidenceStatus::NotRequested;
    bundle.quality.histories = EvidenceStatus::NotRequested;
    finalize_legit_signals(&mut bundle);
    bundle
}

#[allow(clippy::too_many_arguments)]
async fn enrich_solana(
    contract_id: ContractId,
    chain: &str,
    address: &str,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    prefetch: Option<legit_detect::CandidateProbe>,
    known_mints: &[String],
    price_cache: &alchemy::PriceRequestCache,
    transaction_cache: &helius::TransactionRequestCache,
) -> EvidenceBundle {
    let mut bundle = EvidenceBundle::empty(contract_id, chain, address);
    bundle.quality.gas = EvidenceStatus::NotRequested;
    bundle.quality.value_flows = EvidenceStatus::NotRequested;
    let mut evidence_collection_address = address.to_owned();

    let mut snapshot = match prefetch.and_then(|probe| probe.solana_snapshot) {
        Some(snapshot) => snapshot,
        None => {
            helius::fetch_collection_assets(
                client,
                &limits.endpoints.helius,
                keys.helius(),
                address,
                limits.max_solana_assets,
            )
            .await
        }
    };

    // A resident candidate with known NFT mints cannot truthfully have an
    // authoritative Empty collection snapshot. Resolve the collection identity
    // from the known mints, then retry verified and finally unverified grouping.
    if snapshot.status == EvidenceStatus::Empty && !known_mints.is_empty() {
        let mut resolved_collection = None;
        let mut resolution_errors = Vec::new();
        if let Some(api_key) = keys.helius() {
            for mint in known_mints {
                match helius::resolve_collection_address(
                    client,
                    &limits.endpoints.helius,
                    api_key,
                    mint,
                )
                .await
                {
                    Ok(Some(collection)) => {
                        resolved_collection = Some(collection);
                        break;
                    }
                    Ok(None) => {}
                    Err(error) => resolution_errors.push(error.to_string()),
                }
            }
        }

        let collection = resolved_collection.as_deref().unwrap_or(address);
        evidence_collection_address = collection.to_owned();
        if !collection.eq_ignore_ascii_case(address) {
            snapshot = helius::fetch_collection_assets(
                client,
                &limits.endpoints.helius,
                keys.helius(),
                collection,
                limits.max_solana_assets,
            )
            .await;
        }
        if snapshot.status == EvidenceStatus::Empty {
            snapshot = helius::fetch_collection_assets_including_unverified(
                client,
                &limits.endpoints.helius,
                keys.helius(),
                collection,
                limits.max_solana_assets,
            )
            .await;
            if matches!(
                snapshot.status,
                EvidenceStatus::Complete | EvidenceStatus::Truncated
            ) && !snapshot.value.assets.is_empty()
            {
                snapshot.status = EvidenceStatus::Truncated;
                snapshot.truncated = true;
                snapshot.failure = Some(
                    "helius_assets_unverified: collection recovered only by including unverified memberships"
                        .into(),
                );
                if let Some(observation) = snapshot.observation.as_mut() {
                    observation.status = EvidenceStatus::Truncated;
                }
            }
        }
        if snapshot.status == EvidenceStatus::Empty {
            // The Solana ingester intentionally stores an NFT without a
            // collection as (contract_address=mint, token_id=mint). Such an
            // asset has no collection grouping, so getAssetsByGroup is
            // expected to be empty even though getAsset can prove it exists.
            let direct = helius::fetch_assets_by_ids(
                client,
                &limits.endpoints.helius,
                keys.helius(),
                known_mints,
                limits.max_solana_assets,
            )
            .await;
            let known_mint_set: std::collections::BTreeSet<_> = known_mints
                .iter()
                .map(|mint| mint.trim())
                .filter(|mint| !mint.is_empty())
                .collect();
            let rejected_mint_set: std::collections::BTreeSet<_> = direct
                .value
                .rejected_non_nft_ids
                .iter()
                .map(String::as_str)
                .collect();
            let all_resident_mints_explicitly_non_nft = !known_mint_set.is_empty()
                && known_mint_set == rejected_mint_set
                && direct.failure.is_none();
            if matches!(
                direct.status,
                EvidenceStatus::Complete | EvidenceStatus::Truncated
            ) && !direct.value.assets.is_empty()
            {
                snapshot = direct;
            } else if all_resident_mints_explicitly_non_nft {
                snapshot = direct;
                bundle.quality.excluded_non_nft = true;
                bundle.quality.identity_exclusion_reason = Some(format!(
                    "Helius getAsset classified all {} resident mint(s) as non-NFT digital assets",
                    known_mint_set.len()
                ));
            } else if let Some(failure) = direct.failure {
                resolution_errors.push(failure);
            }
        }
        if bundle.quality.excluded_non_nft {
            apply_outcome(
                &mut bundle.quality.assets,
                &mut bundle.provenance,
                &mut bundle.quality.failures,
                &snapshot,
            );
            bundle.quality.holders = EvidenceStatus::Empty;
            bundle.quality.histories = EvidenceStatus::Empty;
            bundle.quality.transfers = EvidenceStatus::Empty;
            bundle.quality.sales = EvidenceStatus::Empty;
            bundle.quality.prices = EvidenceStatus::NotRequested;
            finalize_legit_signals(&mut bundle);
            return bundle;
        }
        if snapshot.status == EvidenceStatus::Empty {
            let mut detail = format!(
                "resident collection has {} NFT mint(s), but Helius returned no grouped or direct assets for candidate {address} or resolved collection {collection}",
                known_mints.len()
            );
            if !resolution_errors.is_empty() {
                detail.push_str("; collection resolution errors: ");
                detail.push_str(&resolution_errors.join("; "));
            }
            snapshot = FetchOutcome::failed("helius", "helius_assets_identity", detail);
        }
    }

    let asset_population_complete = matches!(
        snapshot.status,
        EvidenceStatus::Complete | EvidenceStatus::Empty
    ) && !snapshot.truncated
        && snapshot
            .value
            .total
            .is_none_or(|total| total <= snapshot.value.assets.len());
    if asset_population_complete {
        bundle.collection_nft_count =
            Some(snapshot.value.total.unwrap_or(snapshot.value.assets.len()) as u64);
        bundle.collection_nft_count_complete = true;
    }

    let holders = holders_from_assets(&snapshot.value.assets);
    let holder_status = match snapshot.status {
        EvidenceStatus::NotRequested => EvidenceStatus::NotRequested,
        EvidenceStatus::Failed => EvidenceStatus::Failed,
        other => {
            let truncated = snapshot.truncated;
            if truncated {
                EvidenceStatus::Truncated
            } else if holders.is_empty() {
                EvidenceStatus::Empty
            } else {
                other
            }
        }
    };

    // Controllers from collection metadata before decode/value-flow.
    bundle.controllers = snapshot.value.authority.clone();
    if !bundle.controllers.is_empty() {
        bundle.provenance.push(super::types::EvidenceObservation {
            source: "helius".into(),
            request_key: "contract_controllers".into(),
            observed_at: super::types::now_unix(),
            status: EvidenceStatus::Complete,
        });
    }

    let history = match snapshot.status {
        EvidenceStatus::Failed => FetchOutcome {
            value: (Vec::new(), Vec::new()),
            status: EvidenceStatus::Failed,
            observation: snapshot.observation.clone().map(|mut observation| {
                observation.request_key = "helius_histories".into();
                observation
            }),
            failure: None,
            truncated: false,
        },
        EvidenceStatus::NotRequested => FetchOutcome::skipped("helius_histories"),
        _ => {
            helius::fetch_asset_histories(
                client,
                &limits.endpoints.helius,
                keys.helius(),
                &snapshot.value.assets,
                limits.max_history_assets,
                limits.max_signatures_per_asset,
            )
            .await
        }
    };

    let transfer_discovery_complete = matches!(
        history.status,
        EvidenceStatus::Complete | EvidenceStatus::Empty
    ) && !history.truncated
        && !snapshot.truncated
        && snapshot.value.assets.len() <= limits.max_history_assets;
    let history_was_truncated = history.truncated;
    let (mut transfers, mut helius_sales) = history.value;

    // Helius is the authoritative Solana sale source. Its history stubs are
    // decoded into buyer/seller/payment rows while the native SOL quote is
    // fetched independently.
    let (decode_result, prices) = tokio::join!(
        helius::decode_and_attach_transactions_cached(
            client,
            &limits.endpoints.helius,
            keys.helius(),
            helius::DecodeContext {
                candidate: &evidence_collection_address,
                controllers: &bundle.controllers,
                holders: HolderSnapshot {
                    records: &holders,
                    status: holder_status,
                },
                transfer_discovery_complete,
            },
            &mut transfers,
            &mut helius_sales,
            transaction_cache,
        ),
        price_cache.fetch(client, &limits.endpoints, keys.alchemy(), chain, &[], &[],),
    );
    let (mut gas, mut value_flows, decode_stats) = decode_result;
    if history.status == EvidenceStatus::Failed {
        gas.status = EvidenceStatus::Failed;
        value_flows.status = EvidenceStatus::Failed;
        if let Some(observation) = gas.observation.as_mut() {
            observation.status = EvidenceStatus::Failed;
        }
        if let Some(observation) = value_flows.observation.as_mut() {
            observation.status = EvidenceStatus::Failed;
        }
    } else if history.status == EvidenceStatus::NotRequested {
        gas.status = EvidenceStatus::NotRequested;
        value_flows.status = EvidenceStatus::NotRequested;
        if let Some(observation) = gas.observation.as_mut() {
            observation.status = EvidenceStatus::NotRequested;
        }
        if let Some(observation) = value_flows.observation.as_mut() {
            observation.status = EvidenceStatus::NotRequested;
        }
    }
    apply_prices_to_sales(&mut helius_sales, &prices.value, chain);

    apply_outcome(
        &mut bundle.quality.assets,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &snapshot,
    );
    bundle.quality.holders = holder_status;
    if let Some(obs) = snapshot.observation.clone() {
        let mut holder_obs = obs;
        holder_obs.request_key = "helius_holders".into();
        holder_obs.status = holder_status;
        bundle.provenance.push(holder_obs);
    }
    bundle.holders = holders;

    if let Some(failure) = history.failure.clone() {
        bundle.quality.failures.push(failure);
    }
    match history.status {
        EvidenceStatus::NotRequested => {
            bundle.quality.histories = EvidenceStatus::NotRequested;
            bundle.quality.transfers = EvidenceStatus::NotRequested;
            bundle.quality.sales = EvidenceStatus::NotRequested;
        }
        EvidenceStatus::Failed => {
            bundle.quality.histories = EvidenceStatus::Failed;
            bundle.quality.transfers = EvidenceStatus::Failed;
            bundle.quality.sales = EvidenceStatus::Failed;
        }
        _ => {
            // Asset/signature page caps, or decode incomplete → Truncated.
            // Signature-only stubs (no successful getTransaction) never Complete.
            // Asset-list Truncated must also force histories Truncated.
            let page_trunc = history.truncated
                || snapshot.truncated
                || snapshot.value.assets.len() > limits.max_history_assets;
            bundle.quality.transfers = helius::field_status_after_decode(
                transfers.is_empty(),
                page_trunc,
                decode_stats.transfers_all_complete(),
                &decode_stats,
            );
            bundle.quality.histories = helius::histories_status_after_decode(
                transfers.is_empty(),
                helius_sales.is_empty(),
                page_trunc,
                &decode_stats,
            );
            bundle.quality.sales = helius::field_status_after_decode(
                helius_sales.is_empty(),
                page_trunc,
                decode_stats.sales_all_complete(),
                &decode_stats,
            );
        }
    }
    if let Some(mut obs) = history.observation {
        // Discovery observation must match final quality after decode (P3).
        if !matches!(
            history.status,
            EvidenceStatus::NotRequested | EvidenceStatus::Failed
        ) {
            obs.status = bundle.quality.histories;
        }
        bundle.provenance.push(obs);
    }
    bundle.transfers = std::mem::take(&mut transfers);
    if let Some(mut observation) = bundle
        .provenance
        .iter()
        .find(|observation| observation.request_key == "helius_histories")
        .cloned()
    {
        observation.request_key = "helius_sales".into();
        observation.status = bundle.quality.sales;
        bundle.provenance.push(observation);
    }
    bundle.sales = std::mem::take(&mut helius_sales);

    apply_outcome(
        &mut bundle.quality.gas,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &gas,
    );
    apply_outcome(
        &mut bundle.quality.value_flows,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &value_flows,
    );
    bundle.value_flows = value_flows.value;

    apply_outcome(
        &mut bundle.quality.prices,
        &mut bundle.provenance,
        &mut bundle.quality.failures,
        &prices,
    );
    bundle.prices = prices.value;
    apply_runtime_price_to_value_flows(&mut bundle.value_flows, &bundle.prices, chain);

    mint_payment::attach_mint_payments(
        &mut bundle.transfers,
        &bundle.value_flows,
        &bundle.prices,
        chain,
        &ahash::AHashMap::new(),
    );
    // Solana mint programs commonly route payment to a Candy Machine or
    // treasury account distinct from collection authorities. The transaction
    // decoder has already required a unique same-transaction buyer outflow, so
    // do not apply the EVM controller-recipient restriction here.

    record_solana_truncation_reasons(
        &mut bundle,
        &snapshot,
        history_was_truncated,
        limits,
        &decode_stats,
    );

    finalize_legit_signals(&mut bundle);
    bundle
}

fn push_truncation_reason(bundle: &mut EvidenceBundle, family: &str, reason: &str) {
    let value = format!("{family}:{reason}");
    if !bundle.quality.truncation_reasons.contains(&value) {
        bundle.quality.truncation_reasons.push(value);
    }
}

fn record_solana_truncation_reasons(
    bundle: &mut EvidenceBundle,
    snapshot: &FetchOutcome<helius::SolanaAssetSnapshot>,
    history_truncated: bool,
    limits: &HttpLimits,
    decode_stats: &helius::DecodeStats,
) {
    if bundle.quality.assets == EvidenceStatus::Truncated {
        let reason = if snapshot.value.direct_grouped {
            "direct_asset_has_collection_group"
        } else if snapshot
            .value
            .total
            .is_some_and(|total| total > snapshot.value.assets.len())
            || snapshot.value.assets.len() >= limits.max_solana_assets
        {
            "collection_asset_cap"
        } else if snapshot
            .observation
            .as_ref()
            .is_some_and(|obs| obs.request_key == "helius_assets_unverified")
        {
            "unverified_collection_membership"
        } else {
            "provider_partial_population"
        };
        push_truncation_reason(bundle, "assets", reason);
    }
    if bundle.quality.holders == EvidenceStatus::Truncated {
        push_truncation_reason(bundle, "holders", "asset_population_incomplete");
    }

    let mut history_reasons = Vec::new();
    if snapshot.truncated {
        history_reasons.push("asset_population_incomplete");
    }
    if snapshot.value.assets.len() > limits.max_history_assets {
        history_reasons.push("history_asset_cap");
    }
    if history_truncated {
        history_reasons.push("signature_cap_or_provider_partial");
    }
    if !decode_stats.transfers_all_complete() || !decode_stats.sales_all_complete() {
        history_reasons.push("transaction_decode_incomplete");
    }
    for reason in history_reasons {
        if bundle.quality.histories == EvidenceStatus::Truncated {
            push_truncation_reason(bundle, "histories", reason);
        }
        if bundle.quality.transfers == EvidenceStatus::Truncated {
            push_truncation_reason(bundle, "transfers", reason);
        }
        if bundle.quality.sales == EvidenceStatus::Truncated {
            push_truncation_reason(bundle, "sales", reason);
        }
    }
}

fn apply_outcome<T>(
    field: &mut EvidenceStatus,
    provenance: &mut Vec<super::types::EvidenceObservation>,
    failures: &mut Vec<String>,
    outcome: &FetchOutcome<T>,
) {
    *field = outcome.status;
    if let Some(obs) = outcome.observation.clone() {
        provenance.push(obs);
    }
    if let Some(failure) = outcome.failure.clone() {
        failures.push(failure);
    }
}

fn combine_required_status(
    first: Option<EvidenceStatus>,
    second: Option<EvidenceStatus>,
) -> EvidenceStatus {
    let statuses = [first, second].into_iter().flatten().collect::<Vec<_>>();
    if statuses.is_empty() {
        return EvidenceStatus::Empty;
    }
    if statuses.contains(&EvidenceStatus::Failed) {
        EvidenceStatus::Failed
    } else if statuses.contains(&EvidenceStatus::NotRequested) {
        EvidenceStatus::NotRequested
    } else if statuses.contains(&EvidenceStatus::Truncated) {
        EvidenceStatus::Truncated
    } else if statuses
        .iter()
        .all(|status| *status == EvidenceStatus::Empty)
    {
        EvidenceStatus::Empty
    } else {
        EvidenceStatus::Complete
    }
}

#[derive(Default)]
struct MintPaymentExtras {
    payments: ahash::AHashMap<(String, String), (f64, String)>,
    ambiguous: ahash::AHashSet<(String, String)>,
}

async fn collect_evm_mint_payment_extras(
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    chain: &str,
    transfers: &[TransferEvent],
    external_transfer_cache: &value_flow::ExternalTransferCache,
) -> MintPaymentExtras {
    use super::value_flow::activity_block_window;
    use ahash::{AHashMap, AHashSet};

    let mut grouped = AHashMap::<(String, String), (f64, AHashSet<String>)>::new();
    let mint_txs: AHashSet<String> = transfers
        .iter()
        .filter(|t| t.is_mint)
        .map(|t| t.tx_hash.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    if mint_txs.is_empty() || keys.alchemy().is_none() {
        return MintPaymentExtras::default();
    }
    let window = activity_block_window(transfers, &[]);
    let (from_block, to_block) = window.unwrap_or((0, u64::MAX));

    let mut payers = AHashSet::new();
    for t in transfers.iter().filter(|t| t.is_mint) {
        let addr = t.to.trim().to_ascii_lowercase();
        if !addr.is_empty() && addr != "0x0000000000000000000000000000000000000000" {
            payers.insert(addr);
        }
    }
    let mut payers: Vec<String> = payers.into_iter().collect();
    payers.sort();

    let mut handles = Vec::with_capacity(payers.len());
    for (idx, payer) in payers.into_iter().enumerate() {
        let client = client.clone();
        let endpoints = limits.endpoints.clone();
        let api_key = keys.alchemy().map(str::to_owned);
        let chain = chain.to_owned();
        let external_transfer_cache = external_transfer_cache.clone();
        handles.push(tokio::spawn(async move {
            external_transfer_cache
                .fetch(
                    &client,
                    &endpoints,
                    api_key.as_deref(),
                    &chain,
                    &payer,
                    "from",
                    from_block,
                    to_block,
                    idx,
                )
                .await
        }));
    }
    for handle in handles {
        let Ok(outcome) = handle.await else {
            continue;
        };
        for row in outcome.value {
            let tx = row.tx_hash.trim().to_ascii_lowercase();
            if !mint_txs.contains(&tx) {
                continue;
            }
            let amt = row.value_native.unwrap_or(0.0);
            if amt <= 0.0 {
                continue;
            }
            let payer = row.from.trim().to_ascii_lowercase();
            let receiver = row.to.trim().to_ascii_lowercase();
            if payer.is_empty() || receiver.is_empty() || receiver == payer {
                continue;
            }
            let entry = grouped
                .entry((tx, payer))
                .or_insert_with(|| (0.0, AHashSet::new()));
            entry.0 += amt;
            entry.1.insert(receiver);
        }
    }
    let mut extras = MintPaymentExtras::default();
    for (key, (amount, receivers)) in grouped {
        if receivers.len() == 1 {
            if let Some(receiver) = receivers.into_iter().next() {
                extras.payments.insert(key, (amount, receiver));
            }
        } else if receivers.len() > 1 {
            extras.ambiguous.insert(key);
        }
    }
    extras
}

fn apply_prices_to_sales(
    sales: &mut [SaleEvent],
    prices: &[super::types::PriceBucket],
    chain: &str,
) {
    for sale in sales {
        let rate = runtime_currency_rate(
            prices,
            chain,
            sale.currency_symbol.as_deref(),
            sale.currency_address.as_deref(),
        );
        let marketplace_rate = runtime_currency_rate(
            prices,
            chain,
            sale.marketplace_fee_currency_symbol
                .as_deref()
                .or(sale.currency_symbol.as_deref()),
            sale.marketplace_fee_currency_address
                .as_deref()
                .or(sale.currency_address.as_deref()),
        );
        let royalty_rate = runtime_currency_rate(
            prices,
            chain,
            sale.royalty_fee_currency_symbol
                .as_deref()
                .or(sale.currency_symbol.as_deref()),
            sale.royalty_fee_currency_address
                .as_deref()
                .or(sale.currency_address.as_deref()),
        );
        // Never retain a provider's event-day USD estimate. A report amount is
        // defined only when the payment units and a run-time rate are both known.
        sale.seller_proceeds_usd = sale
            .seller_proceeds_native
            .zip(rate)
            .map(|(amount, rate)| amount * rate);
        sale.marketplace_fee_usd = sale
            .marketplace_fee_native
            .zip(marketplace_rate)
            .map(|(amount, rate)| amount * rate);
        sale.royalty_fee_usd = sale
            .royalty_fee_native
            .zip(royalty_rate)
            .map(|(amount, rate)| amount * rate);
        sale.usd_amount = if sale.seller_proceeds_native.is_some()
            && sale.marketplace_fee_native.is_some()
            && sale.royalty_fee_native.is_some()
        {
            sale.seller_proceeds_usd
                .zip(sale.marketplace_fee_usd)
                .zip(sale.royalty_fee_usd)
                .map(|((seller, marketplace), royalty)| seller + marketplace + royalty)
        } else {
            sale.native_amount
                .zip(rate)
                .map(|(amount, rate)| amount * rate)
        };
    }
}

fn runtime_currency_rate(
    prices: &[super::types::PriceBucket],
    chain: &str,
    symbol: Option<&str>,
    address: Option<&str>,
) -> Option<f64> {
    let symbol = symbol.map(str::trim).filter(|symbol| !symbol.is_empty());
    let symbol_quote_is_safe = address.is_none()
        || symbol
            .is_some_and(|symbol| is_native_or_wrapped(chain, symbol) || is_usd_stablecoin(symbol));
    let address_rate = address.and_then(|address| {
        let expected = super::types::normalize_chain_address(chain, address);
        prices
            .iter()
            .find(|price| {
                price.token_address.as_deref().is_some_and(|actual| {
                    super::types::normalize_chain_address(chain, actual) == expected
                }) && price.usd_per_native.is_finite()
                    && price.usd_per_native > 0.0
            })
            .map(|price| price.usd_per_native)
    });
    address_rate.or_else(|| {
        symbol
            .filter(|_| symbol_quote_is_safe)
            .and_then(|symbol| {
                prices
                    .iter()
                    .find(|price| {
                        price.symbol.eq_ignore_ascii_case(symbol)
                            && price.usd_per_native.is_finite()
                            && price.usd_per_native > 0.0
                    })
                    .map(|price| price.usd_per_native)
            })
            .or_else(|| {
                symbol
                    .filter(|symbol| is_usd_stablecoin(symbol))
                    .map(|_| 1.0)
            })
            .or_else(|| {
                if symbol.is_none_or(|symbol| is_native_or_wrapped(chain, symbol)) {
                    prices
                        .iter()
                        .find(|price| {
                            price.chain.eq_ignore_ascii_case(chain)
                                && is_native_or_wrapped(chain, &price.symbol)
                                && price.usd_per_native.is_finite()
                                && price.usd_per_native > 0.0
                        })
                        .map(|price| price.usd_per_native)
                } else {
                    None
                }
            })
    })
}

fn apply_runtime_price_to_value_flows(
    edges: &mut [super::types::ValueFlowEdge],
    prices: &[super::types::PriceBucket],
    chain: &str,
) {
    let rate = prices
        .iter()
        .find(|price| {
            price.chain.eq_ignore_ascii_case(chain)
                && is_native_or_wrapped(chain, &price.symbol)
                && price.usd_per_native.is_finite()
                && price.usd_per_native > 0.0
        })
        .map(|price| price.usd_per_native);
    for edge in edges {
        edge.usd_amount = edge
            .native_amount
            .filter(|amount| amount.is_finite() && *amount >= 0.0)
            .zip(rate)
            .map(|(amount, rate)| amount * rate);
    }
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

fn is_native_or_wrapped(chain: &str, symbol: &str) -> bool {
    let symbol = symbol.trim().to_ascii_uppercase();
    match chain.trim().to_ascii_lowercase().as_str() {
        "ethereum" | "base" => matches!(symbol.as_str(), "ETH" | "WETH"),
        "polygon" | "matic" => {
            matches!(symbol.as_str(), "MATIC" | "POL" | "WMATIC" | "WPOL")
        }
        "solana" => matches!(symbol.as_str(), "SOL" | "WSOL"),
        _ => false,
    }
}

fn price_symbols_for_sales(sales: &[SaleEvent]) -> Vec<String> {
    const MAX_PAYMENT_PRICE_SYMBOLS: usize = 64;
    let mut symbols: Vec<String> = sales
        .iter()
        .flat_map(|sale| {
            [
                sale.currency_symbol.as_deref(),
                sale.marketplace_fee_currency_symbol.as_deref(),
                sale.royalty_fee_currency_symbol.as_deref(),
            ]
        })
        .flatten()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| {
            !symbol.is_empty()
                && symbol.len() <= 20
                && symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        })
        .collect();
    symbols.sort();
    symbols.dedup();
    symbols.truncate(MAX_PAYMENT_PRICE_SYMBOLS);
    symbols
}

fn price_addresses_for_sales(sales: &[SaleEvent], chain: &str) -> Vec<String> {
    const MAX_PAYMENT_PRICE_ADDRESSES: usize = 64;
    let mut addresses = sales
        .iter()
        .flat_map(|sale| {
            [
                (
                    sale.currency_address.as_deref(),
                    sale.currency_symbol.as_deref(),
                ),
                (
                    sale.marketplace_fee_currency_address.as_deref(),
                    sale.marketplace_fee_currency_symbol.as_deref(),
                ),
                (
                    sale.royalty_fee_currency_address.as_deref(),
                    sale.royalty_fee_currency_symbol.as_deref(),
                ),
            ]
        })
        .filter_map(|(address, symbol)| {
            let address = address?;
            let symbol_is_native = symbol.is_some_and(|symbol| is_native_or_wrapped(chain, symbol));
            (!symbol_is_native || !is_evm_native_sentinel(chain, address)).then_some(address)
        })
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    addresses.sort();
    addresses.dedup();
    addresses.truncate(MAX_PAYMENT_PRICE_ADDRESSES);
    addresses
}

fn is_evm_native_sentinel(chain: &str, address: &str) -> bool {
    if chain.eq_ignore_ascii_case("solana") {
        return false;
    }
    matches!(
        address.trim().to_ascii_lowercase().as_str(),
        "0x0000000000000000000000000000000000000000" | "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashSet;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::dedup::hits::{Dimension, HitEdge, HitGraph};
    use crate::enrich::ValueFlowKind;
    use crate::enrich::types::ProviderEndpoints;
    use crate::entity::{IdentityRow, SourceOrder};
    use crate::progress::NoopProgress;

    #[derive(Default)]
    struct RecordingProgress {
        completed: AtomicUsize,
        finishes: AtomicUsize,
        stages: Mutex<Vec<String>>,
        phases: Mutex<Vec<String>>,
    }

    impl ProgressObserver for RecordingProgress {
        fn set_stage(&self, stage: &str) {
            self.stages.lock().unwrap().push(stage.to_owned());
        }
        fn begin_phase(&self, phase: &str, _total: Option<u64>) {
            self.phases.lock().unwrap().push(phase.to_owned());
        }
        fn add_completed(&self, n: u64) {
            self.completed.fetch_add(n as usize, Ordering::Relaxed);
        }
        fn check_cancelled(&self) -> Result<(), Analysis2Error> {
            Ok(())
        }
        fn finish(&self) {
            self.finishes.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn priced_sale(symbol: &str, amount: f64) -> SaleEvent {
        SaleEvent {
            tx_hash: format!("tx-{symbol}"),
            token_id: "1".into(),
            seller: "0xseller".into(),
            buyer: "0xbuyer".into(),
            timestamp: Some(1),
            block_number: Some(1),
            marketplace: None,
            native_amount: Some(amount),
            usd_amount: None,
            currency_symbol: Some(symbol.into()),
            currency_address: None,
            seller_proceeds_native: Some(amount),
            seller_proceeds_usd: None,
            ..SaleEvent::default()
        }
    }

    #[test]
    fn runtime_pricing_handles_stable_native_common_and_unknown_tokens() {
        let prices = vec![
            super::super::types::PriceBucket {
                chain: "ethereum".into(),
                day_utc: 0,
                symbol: "ETH".into(),
                token_address: None,
                usd_per_native: 2_000.0,
            },
            super::super::types::PriceBucket {
                chain: "ethereum".into(),
                day_utc: 0,
                symbol: "WBTC".into(),
                token_address: None,
                usd_per_native: 60_000.0,
            },
        ];
        let mut sales = vec![
            priced_sale("USDC", 5.0),
            priced_sale("WETH", 2.0),
            priced_sale("WBTC", 0.1),
            priced_sale("UNKNOWN", 7.0),
        ];
        sales[3].usd_amount = Some(999.0);
        sales[3].seller_proceeds_usd = Some(999.0);
        apply_prices_to_sales(&mut sales, &prices, "ethereum");
        assert_eq!(sales[0].usd_amount, Some(5.0));
        assert_eq!(sales[1].usd_amount, Some(4_000.0));
        assert_eq!(sales[2].usd_amount, Some(6_000.0));
        assert_eq!(sales[3].usd_amount, None);
        assert_eq!(sales[3].seller_proceeds_usd, None);
        assert_eq!(sales[1].seller_proceeds_usd, Some(4_000.0));
    }

    #[test]
    fn runtime_pricing_values_fee_splits_in_their_own_currencies() {
        let mut sale = priced_sale("ETH", 0.8);
        sale.marketplace_fee_native = Some(10.0);
        sale.marketplace_fee_currency_symbol = Some("USDC".into());
        sale.royalty_fee_native = Some(5.0);
        sale.royalty_fee_currency_symbol = Some("ART".into());
        sale.royalty_fee_currency_address = Some("0xart".into());
        let prices = vec![
            super::super::types::PriceBucket {
                chain: "ethereum".into(),
                day_utc: 0,
                symbol: "ETH".into(),
                token_address: None,
                usd_per_native: 2_000.0,
            },
            super::super::types::PriceBucket {
                chain: "ethereum".into(),
                day_utc: 0,
                symbol: "ART".into(),
                token_address: Some("0xart".into()),
                usd_per_native: 2.0,
            },
        ];
        apply_prices_to_sales(std::slice::from_mut(&mut sale), &prices, "ethereum");
        assert_eq!(sale.seller_proceeds_usd, Some(1_600.0));
        assert_eq!(sale.marketplace_fee_usd, Some(10.0));
        assert_eq!(sale.royalty_fee_usd, Some(10.0));
        assert_eq!(sale.usd_amount, Some(1_620.0));
    }

    #[test]
    fn runtime_price_requests_cover_observed_safe_payment_symbols() {
        let sales = vec![
            priced_sale("BONK", 1.0),
            priced_sale("PEPE", 1.0),
            priced_sale("USDC", 1.0),
            priced_sale("bad&symbol", 1.0),
        ];
        assert_eq!(
            price_symbols_for_sales(&sales),
            vec!["BONK".to_owned(), "PEPE".to_owned(), "USDC".to_owned()]
        );
    }

    #[test]
    fn native_sentinel_is_not_sent_to_token_address_pricing() {
        let mut native = priced_sale("ETH", 1.0);
        native.currency_address = Some("0x0000000000000000000000000000000000000000".into());
        let mut token = priced_sale("ABC", 1.0);
        token.currency_address = Some("0x1111111111111111111111111111111111111111".into());
        assert_eq!(
            price_addresses_for_sales(&[native, token], "ethereum"),
            vec!["0x1111111111111111111111111111111111111111".to_owned()]
        );
    }

    fn identity(chain: &str, address: &str, token: &str, row: u64) -> IdentityRow {
        IdentityRow {
            chain: chain.into(),
            contract_address: address.into(),
            token_id: token.into(),
            name_norm: "n".into(),
            token_uri_norm: format!("uri://{token}"),
            image_uri_norm: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: row,
            },
        }
    }

    fn store_with_candidate(chain: &str, address: &str) -> (ResidentStore, u32, u32) {
        let evm = ["ethereum", "base", "polygon"]
            .into_iter()
            .map(str::to_owned)
            .collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(Some(2), &evm);
        store
            .ingest_identity_row(identity(chain, "0xseed", "1", 1))
            .unwrap();
        store
            .ingest_identity_row(identity(chain, address, "1", 2))
            .unwrap();
        let seed = cid(&store, chain, "0xseed");
        let cand = cid(&store, chain, address);
        (store, seed, cand)
    }

    fn cid(store: &ResidentStore, chain: &str, address: &str) -> u32 {
        store
            .contract_id(chain, address)
            .expect("contract must exist")
    }

    fn registry_one(seed: u32, cand: u32) -> CandidateRegistry {
        let mut g = HitGraph::new();
        g.push(HitEdge {
            seed_contract: seed,
            candidate_contract: cand,
            candidate_nft: Some(1),
            dimension: Dimension::TokenUri,
            score: 1.0,
            primary_chain: 0,
            secondary_chain: 0,
        });
        let mut nfts = AHashMap::new();
        nfts.insert(cand, vec![1]);
        CandidateRegistry::from_hit_graph(&g, &nfts)
    }

    fn mock_endpoints(server: &MockServer) -> ProviderEndpoints {
        let base = server.base_url();
        let mut alchemy_networks = AHashMap::new();
        alchemy_networks.insert("ethereum".into(), "eth-mainnet".into());
        ProviderEndpoints {
            alchemy_rpc_template: format!("{base}/rpc/{{network}}/{{key}}"),
            alchemy_nft_template: format!("{base}/nft/{{network}}/{{key}}/{{method}}"),
            alchemy_prices: format!("{base}/prices/v1"),
            etherscan: format!("{base}/etherscan"),
            helius: format!("{base}/helius"),
            opensea: format!("{base}/opensea"),
            alchemy_networks,
        }
    }

    #[tokio::test]
    async fn missing_keys_mark_not_requested_and_continue() {
        let (store, seed, cand) = store_with_candidate("ethereum", "0xabc");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            ..HttpLimits::default()
        };
        let keys = ApiKeys::default();
        let progress = RecordingProgress::default();
        let map = enrich_candidates(&registry, &store, &keys, &limits, &progress)
            .await
            .unwrap();
        assert_eq!(
            progress.finishes.load(Ordering::Relaxed),
            0,
            "an enrichment subroutine must not stop the caller-owned progress reporter"
        );
        assert_eq!(
            progress.completed.load(Ordering::Relaxed),
            5,
            "seed, identity prefetch, candidate identity, relation, and full enrichment must each advance progress"
        );
        assert_eq!(
            *progress.stages.lock().unwrap(),
            ["enrich_legit", "enrich"],
            "legit filtering must run before full candidate enrichment"
        );
        assert_eq!(
            *progress.phases.lock().unwrap(),
            [
                "seed_caches",
                "candidate_identity_prefetch",
                "candidate_identity",
                "relations",
                "enrich_candidates"
            ]
        );
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.transfers, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.sales, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.holders, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.prices, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.gas, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::NotRequested);
        assert!(bundle.quality.failures.is_empty());
    }

    #[tokio::test]
    async fn sale_provider_policy_uses_alchemy_for_ethereum_and_opensea_for_base() {
        let server = MockServer::start_async().await;
        let alchemy_sales = server
            .mock_async(|when, then| {
                when.method(GET).path("/nft/eth-mainnet/ak/getNFTSales");
                then.status(200).json_body(json!({
                    "nftSales": [{
                        "transactionHash": "0xethsale",
                        "tokenId": "1",
                        "sellerAddress": "0x1111111111111111111111111111111111111111",
                        "buyerAddress": "0x2222222222222222222222222222222222222222",
                        "sellerFee": {
                            "amount": "1000000000000000000",
                            "decimals": 18,
                            "symbol": "ETH"
                        }
                    }]
                }));
            })
            .await;
        let base_sales = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/opensea/api/v2/events/collection/base-slug");
                then.status(200)
                    .json_body(json!({ "asset_events": [], "next": null }));
            })
            .await;
        let limits = HttpLimits {
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("ak".into()),
            opensea: Some("ok".into()),
            ..ApiKeys::default()
        };
        let client = HttpClient::with_retries(2, 0).unwrap();

        let ethereum = fetch_evm_sales(
            &client,
            &keys,
            &limits,
            "ethereum",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None,
        )
        .await;
        assert_eq!(ethereum.status, EvidenceStatus::Complete);
        assert_eq!(ethereum.value.len(), 1);
        assert_eq!(alchemy_sales.hits(), 1);
        assert_eq!(base_sales.hits(), 0);

        let base = fetch_evm_sales(
            &client,
            &keys,
            &limits,
            "base",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            Some("base-slug"),
        )
        .await;
        assert_eq!(base.status, EvidenceStatus::Empty);
        assert_eq!(alchemy_sales.hits(), 1);
        assert_eq!(base_sales.hits(), 1);
    }

    #[tokio::test]
    async fn alchemy_empty_activity_keeps_missing_deployment_gas_truncated() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let (store, _seed, cand) =
            store_with_candidate("ethereum", "0x1111111111111111111111111111111111111111");
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            opensea: None,
            ..ApiKeys::default()
        };
        let client = HttpClient::with_retries(limits.concurrency, limits.retries).unwrap();
        let contract = &store.contracts[cand as usize];
        let bundle = enrich_evm(
            cand,
            "ethereum",
            &contract.address,
            &client,
            &keys,
            &limits,
            None,
            &alchemy::PriceRequestCache::default(),
            &value_flow::ExternalTransferCache::default(),
            &alchemy::ReceiptRequestCache::default(),
        )
        .await;
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Empty);
        assert_eq!(bundle.quality.holders, EvidenceStatus::Empty);
        assert_eq!(bundle.quality.sales, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.gas, EvidenceStatus::Truncated);
        // No operator seeds without mint fee_payers / controllers.
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Empty);
        assert_ne!(bundle.quality.transfers, EvidenceStatus::Failed);
        assert!(bundle.controllers.is_empty());
    }

    #[tokio::test]
    async fn alchemy_controllers_filled_from_metadata_and_onchain() {
        let server = MockServer::start_async().await;
        let _meta = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getContractMetadata");
                then.status(200).json_body(json!({
                    "contractMetadata": {
                        "contractDeployer": "0xDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDdDd",
                        "ownerAddress": "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
                    }
                }));
            })
            .await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(200).json_body(json!([
                    {
                        "jsonrpc": "2.0",
                        "id": "owner",
                        "result": "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": "owner-fallback",
                        "result": "0x0"
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": "admin",
                        "result": "0x0"
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": "eip1967-admin",
                        "result": "0x000000000000000000000000cccccccccccccccccccccccccccccccccccccccc"
                    }
                ]));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let (store, seed, cand) =
            store_with_candidate("ethereum", "0x1111111111111111111111111111111111111111");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let client = HttpClient::with_retries(limits.concurrency, limits.retries).unwrap();
        let contract = &store.contracts[cand as usize];
        let bundle = enrich_evm(
            cand,
            "ethereum",
            &contract.address,
            &client,
            &keys,
            &limits,
            None,
            &alchemy::PriceRequestCache::default(),
            &value_flow::ExternalTransferCache::default(),
            &alchemy::ReceiptRequestCache::default(),
        )
        .await;
        assert!(
            bundle
                .controllers
                .iter()
                .any(|c| c == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "controllers={:?}",
            bundle.controllers
        );
        assert!(
            bundle
                .controllers
                .iter()
                .any(|c| c == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert!(
            bundle
                .controllers
                .iter()
                .any(|c| c == "0xcccccccccccccccccccccccccccccccccccccccc")
        );
        assert!(
            bundle
                .provenance
                .iter()
                .any(|o| o.request_key == "contract_controllers")
        );

        let gated = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let gated_bundle = gated.get(&cand).unwrap();
        assert!(gated_bundle.legit.official_controller_continuity);
        assert_eq!(
            gated_bundle.quality.transfers,
            EvidenceStatus::NotRequested,
            "a fully legitimate candidate must not enter full enrichment"
        );
    }

    #[tokio::test]
    async fn alchemy_transfer_failure_falls_back_to_etherscan() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(500).body("boom");
            })
            .await;
        let _etherscan = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/etherscan")
                    .query_param("action", "tokennfttx");
                then.status(200).json_body(json!({
                    "status": "1",
                    "message": "OK",
                    "result": [{
                        "hash": "0xdead",
                        "from": "0x0000000000000000000000000000000000000000",
                        "to": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "tokenID": "7",
                        "timeStamp": "1700000000",
                        "blockNumber": "100"
                    }]
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({
                    "owners": [{
                        "ownerAddress": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "tokenBalances": [{"tokenId": "7", "balance": "1"}]
                    }]
                }));
            })
            .await;
        let (store, _seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            etherscan: Some("esk".into()),
            ..ApiKeys::default()
        };
        let client = HttpClient::with_retries(limits.concurrency, limits.retries).unwrap();
        let contract = &store.contracts[cand as usize];
        let bundle = enrich_evm(
            cand,
            "ethereum",
            &contract.address,
            &client,
            &keys,
            &limits,
            None,
            &alchemy::PriceRequestCache::default(),
            &value_flow::ExternalTransferCache::default(),
            &alchemy::ReceiptRequestCache::default(),
        )
        .await;
        assert_eq!(
            bundle.quality.transfers,
            EvidenceStatus::Truncated,
            "fallback history is usable, but mint-payment attribution is incomplete when value-flow RPC fails"
        );
        assert_eq!(bundle.transfers.len(), 1);
        assert_eq!(bundle.transfers[0].token_id, "7");
        assert!(bundle.transfers[0].is_mint);
        assert_eq!(bundle.quality.holders, EvidenceStatus::Complete);
    }

    #[tokio::test]
    async fn http_500_marks_failed_quality() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(500).body("nope");
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(500).body("nope");
            })
            .await;
        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xcccccccccccccccccccccccccccccccccccccccc");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            etherscan: None,
            opensea: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.holders, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.sales, EvidenceStatus::NotRequested);
        assert!(!bundle.quality.failures.is_empty());
    }

    #[tokio::test]
    async fn prices_complete_when_spot_rate_returned() {
        let server = MockServer::start_async().await;
        let _rpc = server
            .mock_async(|when, then| {
                when.method(POST).path_contains("/rpc/");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [{
                            "hash": "0xabc",
                            "from": "0x0000000000000000000000000000000000000000",
                            "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                            "erc721TokenId": "0x1",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                            "blockNum": "0x10"
                        }]
                    }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let _contract = server
            .mock_async(|when, then| {
                when.method(GET).path(
                    "/opensea/api/v2/chain/ethereum/contract/0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                );
                then.status(200).json_body(json!({
                    "collection": "priced-collection"
                }));
            })
            .await;
        let _sales = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/opensea/api/v2/events/collection/priced-collection");
                then.status(200).json_body(json!({
                    "asset_events": [{
                        "event_type": "sale",
                        "transaction_hash": "0xsale",
                        "nft": {
                            "contract": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                            "identifier": "1"
                        },
                        "seller": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "buyer": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "event_timestamp": "2024-01-01T12:00:00Z",
                        "payment": {
                            "quantity": "1000000000000000000",
                            "decimals": 18,
                            "symbol": "ETH"
                        }
                    }]
                }));
            })
            .await;
        let _prices = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/tokens/by-symbol");
                then.status(200).json_body(json!({
                    "data": [{
                        "symbol": "ETH",
                        "prices": [{
                            "currency": "usd",
                            "value": "2500.5",
                            "lastUpdatedAt": "2024-01-01T00:00:00Z"
                        }]
                    }]
                }));
            })
            .await;

        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            opensea: Some("osk".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.prices, EvidenceStatus::Complete);
        assert_eq!(bundle.prices.len(), 1);
        assert!(bundle.sales[0].usd_amount.is_some());
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Complete);
        assert_eq!(bundle.quality.sales, EvidenceStatus::Complete);
    }

    #[tokio::test]
    async fn cached_price_refresh_does_not_repeat_chain_or_market_requests() {
        let server = MockServer::start_async().await;
        let prices = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/tokens/by-symbol");
                then.status(200).json_body(json!({
                    "data": [{
                        "symbol": "ETH",
                        "prices": [{
                            "currency": "usd",
                            "value": "2000",
                            "lastUpdatedAt": "2024-01-01T00:00:00Z"
                        }]
                    }]
                }));
            })
            .await;
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let mut bundle = EvidenceBundle::empty(7, "ethereum", "0xcandidate");
        bundle.quality.prices = EvidenceStatus::Failed;
        bundle
            .quality
            .failures
            .push("alchemy_prices: prior failure".into());
        bundle.sales.push(SaleEvent {
            native_amount: Some(2.0),
            seller_proceeds_native: Some(2.0),
            currency_symbol: Some("ETH".into()),
            ..SaleEvent::default()
        });
        let mut evidence = AHashMap::from([(7, bundle)]);
        refresh_cached_prices(
            &mut evidence,
            &AHashSet::from([7]),
            &keys,
            &limits,
            &NoopProgress,
        )
        .await
        .unwrap();

        let refreshed = &evidence[&7];
        assert_eq!(prices.hits_async().await, 1);
        assert_eq!(refreshed.quality.prices, EvidenceStatus::Complete);
        assert_eq!(refreshed.sales[0].usd_amount, Some(4_000.0));
        assert!(
            refreshed
                .quality
                .failures
                .iter()
                .all(|failure| !failure.contains("alchemy_prices"))
        );
    }

    #[tokio::test]
    async fn price_symbols_are_batched_at_the_provider_limit() {
        let server = MockServer::start_async().await;
        let requested = (0..25)
            .map(|index| format!("TOKEN{index}"))
            .collect::<Vec<_>>();
        let response_symbols = std::iter::once("ETH".to_owned())
            .chain(requested.iter().cloned())
            .map(|symbol| {
                json!({
                    "symbol": symbol,
                    "prices": [{"currency": "usd", "value": "1.0"}]
                })
            })
            .collect::<Vec<_>>();
        let price_mock = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/tokens/by-symbol");
                then.status(200)
                    .json_body(json!({ "data": response_symbols }));
            })
            .await;
        let client = HttpClient::with_retries(2, 0).unwrap();
        let outcome = alchemy::fetch_prices(
            &client,
            &mock_endpoints(&server),
            Some("key"),
            "ethereum",
            &requested,
            &[],
        )
        .await;
        assert_eq!(outcome.status, EvidenceStatus::Complete);
        assert_eq!(outcome.value.len(), 26);
        assert_eq!(price_mock.hits_async().await, 2);
    }

    #[tokio::test]
    async fn solana_missing_helius_is_not_requested() {
        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(Some(2), &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed111111111111111111111111111111111",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand111111111111111111111111111111111",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed111111111111111111111111111111111");
        let cand = cid(&store, "solana", "ColCand111111111111111111111111111111111");
        let registry = registry_one(seed, cand);
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            helius: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(
            &registry,
            &store,
            &keys,
            &HttpLimits::default(),
            &NoopProgress,
        )
        .await
        .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.assets, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.histories, EvidenceStatus::NotRequested);
        assert_eq!(bundle.quality.holders, EvidenceStatus::NotRequested);
    }

    #[tokio::test]
    async fn solana_failed_asset_identity_propagates_to_dependent_evidence() {
        let server = MockServer::start_async().await;
        let _helius = server
            .mock_async(|when, then| {
                when.method(POST).path("/helius");
                then.status(500).body("provider unavailable");
            })
            .await;
        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(Some(2), &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed111111111111111111111111111111111",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand111111111111111111111111111111111",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed111111111111111111111111111111111");
        let cand = cid(&store, "solana", "ColCand111111111111111111111111111111111");
        let registry = registry_one(seed, cand);
        let keys = ApiKeys {
            helius: Some("key".into()),
            ..ApiKeys::default()
        };
        let limits = HttpLimits {
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();

        assert_eq!(bundle.quality.assets, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.holders, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.histories, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.sales, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.gas, EvidenceStatus::Failed);
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Failed);
    }

    #[tokio::test]
    async fn solana_signature_stubs_are_not_complete() {
        let server = MockServer::start_async().await;
        let _assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getAssetsByGroup");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "total": 1,
                        "items": [{
                            "id": "MintStub111111111111111111111111111111111",
                            "ownership": {"owner": "OwnerStub1111111111111111111111111111111"},
                            "compression": {"compressed": false}
                        }]
                    }
                }));
            })
            .await;
        let _sigs = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {"items": [
                        ["SigTransfer111111111111111111111111111111", "TRANSFER"],
                        ["SigSale11111111111111111111111111111111111", "SALE"]
                    ]}
                }));
            })
            .await;
        // Null getTransaction results leave signature stubs → Truncated (not Complete).
        let _tx = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getTransaction");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "tx",
                    "result": null
                }));
            })
            .await;

        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(Some(2), &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed222222222222222222222222222222222",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand222222222222222222222222222222222",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed222222222222222222222222222222222");
        let cand = cid(&store, "solana", "ColCand222222222222222222222222222222222");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            helius: Some("hk".into()),
            alchemy: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert!(!bundle.transfers.is_empty() || !bundle.sales.is_empty());
        assert_ne!(bundle.quality.transfers, EvidenceStatus::Complete);
        assert_ne!(bundle.quality.sales, EvidenceStatus::Complete);
        assert_ne!(bundle.quality.histories, EvidenceStatus::Complete);
        if !bundle.transfers.is_empty() {
            assert_eq!(bundle.quality.transfers, EvidenceStatus::Truncated);
        }
        if !bundle.sales.is_empty() {
            assert_eq!(bundle.quality.sales, EvidenceStatus::Truncated);
        }
        assert_eq!(bundle.quality.histories, EvidenceStatus::Truncated);
        // Stubs lack from/fee — gas must not be Complete.
        assert_ne!(bundle.quality.gas, EvidenceStatus::Complete);
        // P3: discovery provenance must not stay Complete when decode leaves Truncated.
        let hist_obs = bundle
            .provenance
            .iter()
            .find(|o| o.request_key == "helius_histories")
            .expect("helius_histories provenance");
        assert_eq!(hist_obs.status, EvidenceStatus::Truncated);
    }

    #[tokio::test]
    async fn solana_get_transaction_decode_can_complete() {
        let server = MockServer::start_async().await;
        let mint = "MintComplete11111111111111111111111111111";
        let seller = "SellerComp1111111111111111111111111111111";
        let buyer = "BuyerComp11111111111111111111111111111111";
        let fee_payer = "FeePayerComp111111111111111111111111111";
        let funder = "FunderComp11111111111111111111111111111";
        let sig = "SigComplete111111111111111111111111111111";

        let _assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getAssetsByGroup");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "total": 1,
                        "items": [{
                            "id": mint,
                            "ownership": {"owner": buyer},
                            "compression": {"compressed": false}
                        }]
                    }
                }));
            })
            .await;
        let _sigs = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {"items": [[sig, "TRANSFER"]]}
                }));
            })
            .await;
        let _tx = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getTransaction");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "tx",
                    "result": {
                        "slot": 99,
                        "blockTime": 1_700_000_100i64,
                        "transaction": {
                            "message": {
                                "accountKeys": [
                                    {"pubkey": fee_payer, "signer": true},
                                    {"pubkey": "TokenAcc11111111111111111111111111111111", "signer": false}
                                ],
                                "instructions": [{
                                    "program": "system",
                                    "parsed": {
                                        "type": "transfer",
                                        "info": {
                                            "source": funder,
                                            "destination": fee_payer,
                                            "lamports": 1_500_000_000u64
                                        }
                                    }
                                }]
                            }
                        },
                        "meta": {
                            "err": null,
                            "fee": 5000,
                            "preTokenBalances": [{
                                "accountIndex": 1,
                                "mint": mint,
                                "owner": seller,
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }],
                            "postTokenBalances": [{
                                "accountIndex": 1,
                                "mint": mint,
                                "owner": buyer,
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }]
                        }
                    }
                }));
            })
            .await;

        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(Some(2), &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed333333333333333333333333333333333",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand333333333333333333333333333333333",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed333333333333333333333333333333333");
        let cand = cid(&store, "solana", "ColCand333333333333333333333333333333333");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            helius: Some("hk".into()),
            alchemy: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.transfers.len(), 1);
        let t = &bundle.transfers[0];
        assert_eq!(t.from, seller);
        assert_eq!(t.to, buyer);
        assert_eq!(t.timestamp, Some(1_700_000_100));
        assert!(t.gas_native.is_some());
        assert_eq!(t.fee_payer.as_deref(), Some(fee_payer));
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Complete);
        assert_eq!(bundle.quality.histories, EvidenceStatus::Complete);
        assert_eq!(bundle.quality.gas, EvidenceStatus::Complete);
        assert!(
            !bundle
                .value_flows
                .iter()
                .any(|e| e.from == funder && e.to == fee_payer),
            "transaction fee payer is not an operator and must not create a funding edge: {:?}",
            bundle.value_flows
        );
        assert_ne!(bundle.quality.value_flows, EvidenceStatus::Failed);
        assert_ne!(bundle.quality.value_flows, EvidenceStatus::NotRequested);
        let hist_obs = bundle
            .provenance
            .iter()
            .find(|o| o.request_key == "helius_histories")
            .expect("helius_histories provenance");
        assert_eq!(hist_obs.status, EvidenceStatus::Complete);
    }

    #[tokio::test]
    async fn solana_mint_without_token_balances_stays_truncated() {
        // Mint stub + getTransaction fee/timestamp but no pre/postTokenBalances
        // (Bubblegum/compressed) → never Complete.
        let server = MockServer::start_async().await;
        let mint = "MintNoBalInt111111111111111111111111111";
        let owner = "OwnerNoBalInt1111111111111111111111111";
        let fee_payer = "FeePayerNoBal111111111111111111111111";
        let sig = "SigMintNoBalInt111111111111111111111111";

        let _assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getAssetsByGroup");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "total": 1,
                        "items": [{
                            "id": mint,
                            "ownership": {"owner": owner},
                            "compression": {"compressed": true}
                        }]
                    }
                }));
            })
            .await;
        let _sigs = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "items": [[sig, "mint"]]
                    }
                }));
            })
            .await;
        let _tx = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getTransaction");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "tx",
                    "result": {
                        "slot": 3,
                        "blockTime": 1_700_000_300i64,
                        "transaction": {
                            "message": {
                                "accountKeys": [
                                    {"pubkey": fee_payer, "signer": true}
                                ],
                                "instructions": []
                            }
                        },
                        "meta": {
                            "err": null,
                            "fee": 5000
                        }
                    }
                }));
            })
            .await;

        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(Some(2), &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed555555555555555555555555555555555",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand555555555555555555555555555555555",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed555555555555555555555555555555555");
        let cand = cid(&store, "solana", "ColCand555555555555555555555555555555555");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            helius: Some("hk".into()),
            alchemy: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.transfers.len(), 1);
        assert!(bundle.transfers[0].is_mint);
        assert!(bundle.transfers[0].gas_native.is_some());
        assert!(bundle.transfers[0].timestamp.is_some());
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Truncated);
        assert_eq!(bundle.quality.histories, EvidenceStatus::Truncated);
        let hist_obs = bundle
            .provenance
            .iter()
            .find(|o| o.request_key == "helius_histories")
            .expect("helius_histories provenance");
        assert_eq!(hist_obs.status, EvidenceStatus::Truncated);
    }

    #[tokio::test]
    async fn solana_asset_page_truncation_keeps_histories_truncated() {
        // Even when the single returned asset fully decodes, reaching the configured
        // asset cap makes the snapshot and dependent histories Truncated.
        let server = MockServer::start_async().await;
        let mint = "MintTruncPage111111111111111111111111111";
        let seller = "SellerTrunc11111111111111111111111111111";
        let buyer = "BuyerTrunc111111111111111111111111111111";
        let fee_payer = "FeePayerTrunc1111111111111111111111111";
        let sig = "SigTruncPage11111111111111111111111111111";

        let _assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getAssetsByGroup");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "total": 50,
                        "items": [{
                            "id": mint,
                            "ownership": {"owner": buyer},
                            "compression": {"compressed": false}
                        }]
                    }
                }));
            })
            .await;
        let _sigs = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {"items": [[sig, "TRANSFER"]]}
                }));
            })
            .await;
        let _tx = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/helius")
                    .body_contains("getTransaction");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "tx",
                    "result": {
                        "slot": 11,
                        "blockTime": 1_700_000_200i64,
                        "transaction": {
                            "message": {
                                "accountKeys": [
                                    {"pubkey": fee_payer, "signer": true},
                                    {"pubkey": "TokenAccTrunc1111111111111111111111111", "signer": false}
                                ],
                                "instructions": []
                            }
                        },
                        "meta": {
                            "err": null,
                            "fee": 5000,
                            "preTokenBalances": [{
                                "accountIndex": 1,
                                "mint": mint,
                                "owner": seller,
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }],
                            "postTokenBalances": [{
                                "accountIndex": 1,
                                "mint": mint,
                                "owner": buyer,
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }]
                        }
                    }
                }));
            })
            .await;

        let evm = ["ethereum"].into_iter().map(str::to_owned).collect();
        let mut store = ResidentStore::with_options(Some(2), &evm);
        store
            .ingest_identity_row(identity(
                "solana",
                "ColSeed444444444444444444444444444444444",
                "m1",
                1,
            ))
            .unwrap();
        store
            .ingest_identity_row(identity(
                "solana",
                "ColCand444444444444444444444444444444444",
                "m2",
                2,
            ))
            .unwrap();
        let seed = cid(&store, "solana", "ColSeed444444444444444444444444444444444");
        let cand = cid(&store, "solana", "ColCand444444444444444444444444444444444");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            max_solana_assets: 1,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            helius: Some("hk".into()),
            alchemy: None,
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.assets, EvidenceStatus::Truncated);
        assert_eq!(bundle.transfers.len(), 1);
        assert_eq!(bundle.transfers[0].from, seller);
        assert_eq!(bundle.transfers[0].to, buyer);
        // Decoded transfer fields are complete, but asset-list truncation forbids
        // Complete histories.
        assert_eq!(bundle.quality.histories, EvidenceStatus::Truncated);
        assert_eq!(bundle.quality.transfers, EvidenceStatus::Truncated);
        assert!(
            bundle
                .quality
                .truncation_reasons
                .contains(&"assets:collection_asset_cap".to_owned())
        );
        assert!(
            bundle
                .quality
                .truncation_reasons
                .contains(&"histories:asset_population_incomplete".to_owned())
        );
        let hist_obs = bundle
            .provenance
            .iter()
            .find(|o| o.request_key == "helius_histories")
            .expect("helius_histories provenance");
        assert_eq!(hist_obs.status, EvidenceStatus::Truncated);
    }

    #[tokio::test]
    async fn alchemy_receipt_gas_fills_transfer_but_deployment_remains_required() {
        let server = MockServer::start_async().await;
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [{
                            "hash": "0xabc123",
                            "from": "0x0000000000000000000000000000000000000000",
                            "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                            "erc721TokenId": "0x1",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                            "blockNum": "0x10"
                        }]
                    }
                }));
            })
            .await;
        let _external = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _receipt = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-0",
                    "result": {
                        "transactionHash": "0xabc123",
                        "from": "0xFeePayer1111111111111111111111111111111111",
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.gas, EvidenceStatus::Truncated);
        assert_eq!(bundle.transfers.len(), 1);
        // 21000 * 1e9 wei = 2.1e13 → 0.000021 ETH
        let gas = bundle.transfers[0].gas_native.unwrap();
        assert!((gas - 0.000021).abs() < 1e-12);
        assert_eq!(
            bundle.transfers[0].fee_payer.as_deref(),
            Some("0xfeepayer1111111111111111111111111111111111")
        );
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Empty);
    }

    #[tokio::test]
    async fn alchemy_deployment_fetches_creation_receipt_gas_and_block_time() {
        let server = MockServer::start_async().await;
        let contract = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let payer = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let _receipts = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getTransactionReceipts")
                    .body_contains("0x10");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "deployment-receipts",
                    "result": {
                        "receipts": [{
                            "transactionHash": "0xdeploy",
                            "contractAddress": contract,
                            "from": payer,
                            "gasUsed": "0x5208",
                            "effectiveGasPrice": "0x3b9aca00"
                        }]
                    }
                }));
            })
            .await;
        let _block = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getBlockByNumber")
                    .body_contains("0x10");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "deployment-block",
                    "result": {"timestamp": "0x65"}
                }));
            })
            .await;
        let endpoints = mock_endpoints(&server);
        let client = HttpClient::with_retries(2, 0).unwrap();
        let outcome = alchemy::fetch_deployment(
            &client,
            &endpoints,
            Some("key"),
            "ethereum",
            contract,
            Some(16),
        )
        .await;

        assert_eq!(outcome.status, EvidenceStatus::Complete);
        let deployment = outcome.value.unwrap();
        assert_eq!(deployment.tx_hash, "0xdeploy");
        assert_eq!(deployment.timestamp, Some(101));
        assert_eq!(deployment.fee_payer.as_deref(), Some(payer));
        assert!((deployment.gas_native.unwrap() - 0.000021).abs() < 1e-12);
    }

    #[tokio::test]
    async fn alchemy_receipt_gas_truncated_when_partial() {
        let server = MockServer::start_async().await;
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [
                            {
                                "hash": "0xgood",
                                "from": "0x0000000000000000000000000000000000000000",
                                "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                                "erc721TokenId": "0x1",
                                "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                                "blockNum": "0x10"
                            },
                            {
                                "hash": "0xbad",
                                "from": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "to": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "erc721TokenId": "0x2",
                                "metadata": { "blockTimestamp": "2024-01-01T01:00:00Z" },
                                "blockNum": "0x11"
                            }
                        ]
                    }
                }));
            })
            .await;
        let _external = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _receipt_ok = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt")
                    .body_contains("0xgood");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-ok",
                    "result": {
                        "from": "0xcccccccccccccccccccccccccccccccccccccccc",
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _receipt_bad = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt")
                    .body_contains("0xbad");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-bad",
                    "result": null
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let (store, _seed, cand) =
            store_with_candidate("ethereum", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let client = HttpClient::with_retries(limits.concurrency, limits.retries).unwrap();
        let contract = &store.contracts[cand as usize];
        let bundle = enrich_evm(
            cand,
            "ethereum",
            &contract.address,
            &client,
            &keys,
            &limits,
            None,
            &alchemy::PriceRequestCache::default(),
            &value_flow::ExternalTransferCache::default(),
            &alchemy::ReceiptRequestCache::default(),
        )
        .await;
        assert_eq!(bundle.quality.gas, EvidenceStatus::Truncated);
        let good = bundle
            .transfers
            .iter()
            .find(|t| t.tx_hash.eq_ignore_ascii_case("0xgood"))
            .unwrap();
        assert!(good.gas_native.is_some());
        let bad = bundle
            .transfers
            .iter()
            .find(|t| t.tx_hash.eq_ignore_ascii_case("0xbad"))
            .unwrap();
        assert!(bad.gas_native.is_none());
    }

    #[tokio::test]
    async fn alchemy_value_flows_complete_funding_and_withdrawal() {
        let server = MockServer::start_async().await;
        let operator = "0xcccccccccccccccccccccccccccccccccccccccc";
        let _controllers = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path_contains("/getContractMetadata")
                    .query_param(
                        "contractAddress",
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    );
                then.status(200).json_body(json!({
                    "contractMetadata": {"contractDeployer": operator}
                }));
            })
            .await;
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [{
                            "hash": "0xmint",
                            "from": "0x0000000000000000000000000000000000000000",
                            "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                            "erc721TokenId": "0x1",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                            "blockNum": "0x10"
                        }]
                    }
                }));
            })
            .await;
        let _receipt = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-0",
                    "result": {
                        "from": operator,
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _external_to = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains("toAddress");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-to",
                    "result": {
                        "transfers": [{
                            "hash": "0xfund",
                            "from": "0x1111111111111111111111111111111111111111",
                            "to": operator,
                            "category": "external",
                            "value": 2.0,
                            "blockNum": "0x10",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" }
                        }]
                    }
                }));
            })
            .await;
        let _external_from = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains("fromAddress");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-from",
                    "result": {
                        "transfers": [{
                            "hash": "0xwithdraw",
                            "from": operator,
                            "to": "0x2222222222222222222222222222222222222222",
                            "category": "external",
                            "value": 0.5,
                            "blockNum": "0x10",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:01Z" }
                        }]
                    }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Complete);
        assert_eq!(bundle.value_flows.len(), 2);
        let funding = bundle
            .value_flows
            .iter()
            .find(|e| e.kind == ValueFlowKind::Funding)
            .unwrap();
        assert!((funding.native_amount.unwrap() - 2.0).abs() < 1e-12);
        let withdrawal = bundle
            .value_flows
            .iter()
            .find(|e| e.kind == ValueFlowKind::Withdrawal)
            .unwrap();
        assert!((withdrawal.native_amount.unwrap() - 0.5).abs() < 1e-12);
    }

    #[tokio::test]
    async fn alchemy_value_flows_full_history_is_complete_without_page_key() {
        let server = MockServer::start_async().await;
        let operator = "0xcccccccccccccccccccccccccccccccccccccccc";
        let _controllers = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path_contains("/getContractMetadata")
                    .query_param(
                        "contractAddress",
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    );
                then.status(200).json_body(json!({
                    "contractMetadata": {"contractDeployer": operator}
                }));
            })
            .await;
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [{
                            "hash": "0xmint",
                            "from": "0x0000000000000000000000000000000000000000",
                            "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                            "erc721TokenId": "0x1",
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                            "blockNum": "0x10"
                        }]
                    }
                }));
            })
            .await;
        let _receipt = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-0",
                    "result": {
                        "from": operator,
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _external = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external",
                    "result": {
                        "transfers": [{
                            "hash": "0xfund",
                            "from": "0x1111111111111111111111111111111111111111",
                            "to": operator,
                            "category": "external",
                            "value": 1.0,
                            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                            "blockNum": "0xf"
                        }]
                    }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Complete);
        assert!(!bundle.value_flows.is_empty());
        assert!(
            !bundle
                .quality
                .failures
                .iter()
                .any(|f| f.contains("activity block window")),
            "block-associated full-history query must remain complete: {:?}",
            bundle.quality.failures
        );
        assert!(
            !bundle
                .provenance
                .iter()
                .any(|o| o.request_key.contains("activity block window unknown")),
            "known activity blocks must not create an association truncation: {:?}",
            bundle.provenance
        );
    }

    #[tokio::test]
    async fn alchemy_value_flows_ignore_all_transaction_fee_payers_as_operator_seeds() {
        let server = MockServer::start_async().await;
        let mint_op = "0xcccccccccccccccccccccccccccccccccccccccc";
        let secondary_fee = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let _transfers = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("erc721");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "1",
                    "result": {
                        "transfers": [
                            {
                                "hash": "0xmint",
                                "from": "0x0000000000000000000000000000000000000000",
                                "to": "0xdddddddddddddddddddddddddddddddddddddddd",
                                "erc721TokenId": "0x1",
                                "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" },
                                "blockNum": "0x10"
                            },
                            {
                                "hash": "0xsec",
                                "from": "0xdddddddddddddddddddddddddddddddddddddddd",
                                "to": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                                "erc721TokenId": "0x1",
                                "metadata": { "blockTimestamp": "2024-01-01T00:00:01Z" },
                                "blockNum": "0x11"
                            }
                        ]
                    }
                }));
            })
            .await;
        let _receipt_mint = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt")
                    .body_contains("0xmint");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-0",
                    "result": {
                        "from": mint_op,
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        let _receipt_sec = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("eth_getTransactionReceipt")
                    .body_contains("0xsec");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "receipt-1",
                    "result": {
                        "from": secondary_fee,
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }));
            })
            .await;
        // Neither receipt sender is a verified controller, so neither may be queried
        // as an operator merely because it paid transaction gas.
        let external_mint = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains(mint_op);
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-mint",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let external_secondary = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains(secondary_fee);
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-secondary",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _external_candidate = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-candidate",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _external_first_holder = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains("0xdddddddddddddddddddddddddddddddddddddddd");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-first-holder",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _external_final_holder = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path_contains("/rpc/")
                    .body_contains("alchemy_getAssetTransfers")
                    .body_contains("external")
                    .body_contains("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-final-holder",
                    "result": { "transfers": [] }
                }));
            })
            .await;
        let _holders = server
            .mock_async(|when, then| {
                when.method(GET).path_contains("/getOwnersForContract");
                then.status(200).json_body(json!({ "owners": [] }));
            })
            .await;
        let (store, seed, cand) =
            store_with_candidate("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let registry = registry_one(seed, cand);
        let limits = HttpLimits {
            concurrency: 2,
            retries: 0,
            endpoints: mock_endpoints(&server),
            ..HttpLimits::default()
        };
        let keys = ApiKeys {
            alchemy: Some("key".into()),
            ..ApiKeys::default()
        };
        let map = enrich_candidates(&registry, &store, &keys, &limits, &NoopProgress)
            .await
            .unwrap();
        let bundle = map.get(&cand).unwrap();
        assert_eq!(bundle.quality.value_flows, EvidenceStatus::Empty);
        assert_eq!(
            external_mint.hits(),
            0,
            "mint fee_payer must not be queried as operator seed"
        );
        assert_eq!(
            external_secondary.hits(),
            0,
            "secondary fee_payer must not be queried as operator seed"
        );
    }
}
