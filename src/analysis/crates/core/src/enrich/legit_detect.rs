//! Relation-level legit detection: controller continuity, OpenSea slug, seed NFT interaction.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use ahash::{AHashMap, AHashSet};
use futures_util::{StreamExt, stream};

use crate::dedup::candidates::CandidateRegistry;
use crate::entity::{ContractId, ResidentStore};
use crate::error::AnalysisError;
use crate::progress::{NoopProgress, ProgressObserver};

use super::alchemy::{self, FetchOutcome};
use super::controllers::{self, EvmControllerEvidence, normalize_evm_address};
use super::helius;
use super::http::HttpClient;
use super::opensea;
use super::types::{
    ApiKeys, EvidenceBundle, EvidenceStatus, HttpLimits, LegitSignals, finalize_legit_signals,
};

/// Seed-side cache reused across all candidates for that seed.
#[derive(Clone, Debug, Default)]
struct SeedCache {
    /// Seed-level CC0 / public-domain license.
    open_license: bool,
    controllers: Vec<String>,
    collection_slug: Option<String>,
    /// Normalized addresses that appear as from/to on seed NFT transfers.
    transfer_counterparties: BTreeSet<String>,
    /// Current owners of seed NFTs used for in-memory holds checks.
    current_owners: BTreeSet<String>,
    /// A missing owner is conclusive only when every EVM holder page was read.
    current_owners_complete: bool,
    controllers_probed: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CandidateProbe {
    pub chain: String,
    pub address: String,
    pub controllers: Vec<String>,
    pub collection_slug: Option<String>,
    pub evm_controllers: Option<FetchOutcome<EvmControllerEvidence>>,
    pub solana_snapshot: Option<FetchOutcome<helius::SolanaAssetSnapshot>>,
}

#[derive(Clone, Default)]
struct CandidatePrefetch {
    evm_controllers: Option<FetchOutcome<EvmControllerEvidence>>,
    solana_identity: Option<helius::CollectionIdentityProbe>,
}

const CANDIDATE_IDENTITY_CACHE_VERSION: u32 = 1;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct CandidateIdentityCacheRow {
    chain: String,
    address: String,
    evm_controllers: Option<FetchOutcome<EvmControllerEvidence>>,
    solana_identity: Option<helius::CollectionIdentityProbe>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CandidateIdentityCacheFile {
    version: u32,
    rows: Vec<CandidateIdentityCacheRow>,
}

fn candidate_identity_key(chain: &str, address: &str) -> String {
    format!(
        "{}:{}",
        chain.trim().to_ascii_lowercase(),
        normalize_addr(chain, address)
    )
}

fn reusable_identity_row(row: &CandidateIdentityCacheRow) -> bool {
    row.evm_controllers.as_ref().is_some_and(|outcome| {
        outcome.failure.is_none()
            && !outcome.truncated
            && matches!(
                outcome.status,
                EvidenceStatus::Complete | EvidenceStatus::Empty
            )
    }) || row
        .solana_identity
        .as_ref()
        .is_some_and(|probe| probe.identity.is_some() || !probe.authorities.is_empty())
}

fn load_candidate_identity_cache(
    path: Option<&Path>,
) -> AHashMap<String, CandidateIdentityCacheRow> {
    let Some(path) = path.filter(|path| path.is_file()) else {
        return AHashMap::new();
    };
    let Ok(bytes) = fs::read(path) else {
        return AHashMap::new();
    };
    let Ok(cache) = serde_json::from_slice::<CandidateIdentityCacheFile>(&bytes) else {
        eprintln!(
            "evidence: ignoring malformed candidate identity cache at {}",
            path.display()
        );
        return AHashMap::new();
    };
    if cache.version != CANDIDATE_IDENTITY_CACHE_VERSION {
        return AHashMap::new();
    }
    cache
        .rows
        .into_iter()
        .filter(reusable_identity_row)
        .map(|row| (candidate_identity_key(&row.chain, &row.address), row))
        .collect()
}

fn write_candidate_identity_cache(
    path: Option<&Path>,
    rows: &AHashMap<String, CandidateIdentityCacheRow>,
) {
    let Some(path) = path else {
        return;
    };
    if let Some(parent) = path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        eprintln!(
            "evidence: candidate identity cache mkdir failed at {}: {error}",
            parent.display()
        );
        return;
    }
    let mut values = rows.values().cloned().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        candidate_identity_key(&left.chain, &left.address)
            .cmp(&candidate_identity_key(&right.chain, &right.address))
    });
    let cache = CandidateIdentityCacheFile {
        version: CANDIDATE_IDENTITY_CACHE_VERSION,
        rows: values,
    };
    let Ok(bytes) = serde_json::to_vec(&cache) else {
        return;
    };
    let tmp = path.with_extension("json.tmp");
    if let Err(error) = fs::write(&tmp, bytes).and_then(|_| {
        fs::rename(&tmp, path).or_else(|_| {
            fs::copy(&tmp, path)?;
            fs::remove_file(&tmp)
        })
    }) {
        eprintln!(
            "evidence: candidate identity cache write failed at {}: {error}",
            path.display()
        );
    }
}

pub(super) struct LegitPreflight {
    pub evidence: AHashMap<ContractId, EvidenceBundle>,
    pub candidates_to_enrich: Vec<ContractId>,
    pub candidate_probes: AHashMap<ContractId, CandidateProbe>,
}

fn seed_key(chain: &str, address: &str) -> String {
    format!("{chain}:{address}")
}

fn normalize_addr(chain: &str, address: &str) -> String {
    if chain.eq_ignore_ascii_case("solana") {
        address.trim().to_owned()
    } else {
        normalize_evm_address(address).unwrap_or_else(|| address.trim().to_ascii_lowercase())
    }
}

fn controller_set(addrs: &[String], chain: &str) -> BTreeSet<String> {
    addrs
        .iter()
        .map(|a| normalize_addr(chain, a))
        .filter(|a| !a.is_empty())
        .collect()
}

/// Resolve a collection identity string for legit "same collection" matching.
///
/// Preference order (OpenSea only as last resort, and never for Solana):
/// - Solana: Helius DAS metadata symbol/name for the collection address
/// - EVM: Alchemy NFT collection slug → OpenSea contract slug fallback
async fn resolve_collection_slug(
    client: &HttpClient,
    limits: &HttpLimits,
    keys: &ApiKeys,
    chain: &str,
    address: &str,
) -> Option<String> {
    if chain.eq_ignore_ascii_case("solana") {
        // Prefer Helius; do not spend OpenSea quota for Solana legit slug.
        return helius::fetch_collection_identity(
            client,
            &limits.endpoints.helius,
            keys.helius(),
            address,
        )
        .await
        .map(|s| s.to_ascii_lowercase());
    }
    if let Some(slug) =
        alchemy::fetch_collection_slug(client, &limits.endpoints, keys.alchemy(), chain, address)
            .await
    {
        return Some(slug.to_ascii_lowercase());
    }
    // Last resort only when Alchemy could not supply a slug.
    opensea::fetch_contract_collection_slug(
        client,
        &limits.endpoints.opensea,
        keys.opensea(),
        chain,
        address,
    )
    .await
    .map(|s| s.to_ascii_lowercase())
}

async fn build_seed_cache(
    client: &HttpClient,
    store: &ResidentStore,
    evidence: &AHashMap<ContractId, EvidenceBundle>,
    keys: &ApiKeys,
    limits: &HttpLimits,
    seed_id: ContractId,
) -> SeedCache {
    let contract = &store.contracts[seed_id as usize];
    let chain = store.chain_name(contract.chain_id).to_owned();
    let address = contract.address.clone();
    let is_evm = store.is_evm_chain(&chain);

    let mut cache = SeedCache::default();

    if is_evm {
        // These seed-side probes are independent. Holder snapshots replace
        // thousands of relation-local `isHolderOfContract` requests whenever
        // the bounded snapshot is complete.
        let controller_probe = async {
            if let Some(bundle) = evidence.get(&seed_id) {
                return (bundle.controllers.clone(), true);
            }
            let outcome = controllers::fetch_evm_controllers(
                client,
                &limits.endpoints,
                keys.alchemy(),
                &chain,
                &address,
            )
            .await;
            let probed = !matches!(outcome.status, EvidenceStatus::NotRequested);
            (outcome.value.addresses, probed)
        };
        let profile_probe = alchemy::fetch_collection_profile(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &chain,
            &address,
        );
        let transfer_probe = alchemy::fetch_transfers(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &chain,
            &address,
            limits.max_transfer_pages.clamp(1, 3),
        );
        let holder_probe = alchemy::fetch_holders(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &chain,
            &address,
            limits.max_holder_pages,
        );
        let ((controllers, controllers_probed), profile, transfers, holders) = tokio::join!(
            controller_probe,
            profile_probe,
            transfer_probe,
            holder_probe
        );

        cache.controllers = controllers;
        cache.controllers_probed = controllers_probed;
        cache.collection_slug = profile.slug.map(|slug| slug.to_ascii_lowercase());
        cache.open_license = profile.open_license;
        if cache.collection_slug.is_none() {
            cache.collection_slug = opensea::fetch_contract_collection_slug(
                client,
                &limits.endpoints.opensea,
                keys.opensea(),
                &chain,
                &address,
            )
            .await
            .map(|slug| slug.to_ascii_lowercase());
        }
        cache.current_owners_complete = matches!(
            holders.status,
            EvidenceStatus::Complete | EvidenceStatus::Empty
        );
        for holder in holders.value {
            cache
                .current_owners
                .insert(normalize_addr(&chain, &holder.owner));
        }
        for transfer in transfers.value {
            if !transfer.from.is_empty() {
                cache
                    .transfer_counterparties
                    .insert(normalize_addr(&chain, &transfer.from));
            }
            if !transfer.to.is_empty() {
                cache
                    .transfer_counterparties
                    .insert(normalize_addr(&chain, &transfer.to));
            }
        }
    } else {
        let slug_probe = resolve_collection_slug(client, limits, keys, &chain, &address);
        let asset_probe = helius::fetch_collection_assets(
            client,
            &limits.endpoints.helius,
            keys.helius(),
            &address,
            limits.max_solana_assets.clamp(1, 50),
        );
        let (slug, snapshot) = tokio::join!(slug_probe, asset_probe);

        cache.collection_slug = slug;
        cache.open_license = snapshot.value.open_license;
        // Controllers: reuse candidate enrich if seed was also a candidate.
        if let Some(bundle) = evidence.get(&seed_id) {
            cache.controllers = bundle.controllers.clone();
            cache.controllers_probed = true;
        } else {
            cache.controllers = snapshot.value.authority.clone();
            cache.controllers_probed = !matches!(snapshot.status, EvidenceStatus::NotRequested);
        }
        for asset in &snapshot.value.assets {
            if let Some(owner) = &asset.owner {
                cache.current_owners.insert(normalize_addr(&chain, owner));
            }
        }
    }

    cache
}

fn relation_needs_holder_request(seed: &SeedCache, chain: &str, candidate_address: &str) -> bool {
    !seed.current_owners_complete
        && !seed
            .current_owners
            .contains(&normalize_addr(chain, candidate_address))
}

fn apply_cached_holder_signal(
    signals: &mut LegitSignals,
    seed: &SeedCache,
    chain: &str,
    candidate_address: &str,
) {
    if seed
        .current_owners
        .contains(&normalize_addr(chain, candidate_address))
    {
        signals.seed_nft_interaction = true;
        signals.evidence_keys.push("holds_seed_nft".into());
    }
}

fn offline_relation_signals(
    seed: &SeedCache,
    chain: &str,
    candidate_address: &str,
) -> LegitSignals {
    let mut signals = LegitSignals::default();
    if seed.open_license {
        signals.seed_open_license = true;
        signals.evidence_keys.push("seed_open_license".into());
    }
    apply_cached_holder_signal(&mut signals, seed, chain, candidate_address);
    let candidate = normalize_addr(chain, candidate_address);
    if seed.transfer_counterparties.contains(&candidate) {
        signals.seed_nft_interaction = true;
        signals
            .evidence_keys
            .push("seed_transfer_counterparty".into());
    }
    signals
}

async fn build_candidate_probe(
    client: &HttpClient,
    store: &ResidentStore,
    keys: &ApiKeys,
    limits: &HttpLimits,
    candidate_id: ContractId,
    resolve_slug: bool,
    prefetch: CandidatePrefetch,
) -> CandidateProbe {
    let contract = &store.contracts[candidate_id as usize];
    let chain = store.chain_name(contract.chain_id).to_owned();
    let address = contract.address.clone();

    if store.is_evm_chain(&chain) {
        let controller_probe = async {
            match prefetch.evm_controllers {
                Some(outcome) => outcome,
                None => {
                    controllers::fetch_evm_controllers(
                        client,
                        &limits.endpoints,
                        keys.alchemy(),
                        &chain,
                        &address,
                    )
                    .await
                }
            }
        };
        let slug_probe = async {
            if resolve_slug {
                resolve_collection_slug(client, limits, keys, &chain, &address).await
            } else {
                None
            }
        };
        let (controllers, collection_slug) = tokio::join!(controller_probe, slug_probe);
        CandidateProbe {
            chain,
            address,
            controllers: controllers.value.addresses.clone(),
            collection_slug,
            evm_controllers: Some(controllers),
            solana_snapshot: None,
        }
    } else {
        let identity = prefetch.solana_identity.unwrap_or_default();
        let collection_slug = resolve_slug.then_some(identity.identity).flatten();
        CandidateProbe {
            chain,
            address,
            controllers: identity.authorities,
            collection_slug,
            evm_controllers: None,
            // Full collection assets are intentionally deferred until deep
            // enrichment, after the legitimacy gate has removed candidates.
            solana_snapshot: None,
        }
    }
}

fn all_relations_legit(bundle: &EvidenceBundle, expected_relations: usize) -> bool {
    expected_relations > 0
        && bundle.relation_legit.len() == expected_relations
        && bundle
            .relation_legit
            .values()
            .all(LegitSignals::is_legit_duplicate)
}

fn continuity_signals(
    seed_controllers: &[String],
    cand_controllers: &[String],
    chain: &str,
    probed_both: bool,
) -> LegitSignals {
    let mut signals = LegitSignals {
        verification_complete: probed_both,
        ..LegitSignals::default()
    };
    let seed_set = controller_set(seed_controllers, chain);
    let cand_set = controller_set(cand_controllers, chain);
    for addr in seed_set.intersection(&cand_set) {
        signals.official_controller_continuity = true;
        signals
            .evidence_keys
            .push(format!("controller_continuity:{addr}"));
    }
    signals
}

#[allow(clippy::too_many_arguments)] // Relation probing combines two identities and shared provider context.
async fn probe_relation(
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    chain: &str,
    is_evm: bool,
    seed_address: &str,
    candidate_address: &str,
    cand_controllers: &[String],
    cand_slug: Option<&str>,
    seed: &SeedCache,
) -> LegitSignals {
    let mut signals = continuity_signals(
        &seed.controllers,
        cand_controllers,
        chain,
        seed.controllers_probed && !cand_controllers.is_empty(),
    );
    signals.merge_or(&offline_relation_signals(seed, chain, candidate_address));
    // Controllers probed on candidate side even if empty when enrich ran with key.
    if seed.controllers_probed {
        signals.verification_complete = true;
    }

    // Candidate slugs are fetched once per candidate by the orchestrator.
    if let (Some(seed_slug), Some(cand_slug)) = (&seed.collection_slug, cand_slug)
        && !seed_slug.is_empty()
        && seed_slug == cand_slug
    {
        signals.official_collection_relation = true;
        signals
            .evidence_keys
            .push(format!("collection_relation:{seed_slug}"));
    }

    // Current holds seed NFT. Prefer one seed-wide owner snapshot; fall back to
    // relation-local lookup only when that snapshot was truncated or failed.
    if is_evm
        && normalize_evm_address(candidate_address).is_some()
        && relation_needs_holder_request(seed, chain, candidate_address)
    {
        match alchemy::is_holder_of_contract(
            client,
            &limits.endpoints,
            keys.alchemy(),
            chain,
            candidate_address,
            seed_address,
        )
        .await
        {
            Ok(Some(true)) => {
                signals.seed_nft_interaction = true;
                signals.evidence_keys.push("holds_seed_nft".into());
            }
            Ok(Some(false)) | Ok(None) => {}
            Err(_) => {}
        }
    }

    // Historical transfer counterparty.
    signals
}

/// Lightweight relation gate run before full candidate enrichment.
///
/// A candidate is excluded from full enrichment only when every seed relation
/// has a positive official/interaction signal.
pub(super) async fn prefilter_candidates(
    registry: &CandidateRegistry,
    store: &ResidentStore,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
    progress: &dyn ProgressObserver,
) -> Result<LegitPreflight, AnalysisError> {
    let seed_ids: Vec<ContractId> = {
        let mut set = BTreeSet::new();
        for rel in registry.relations() {
            set.insert(rel.seed_contract);
        }
        set.into_iter().collect()
    };
    if seed_ids.is_empty() {
        let mut evidence = AHashMap::with_capacity(registry.candidate_contracts().len());
        for &candidate_id in registry.candidate_contracts() {
            let contract = &store.contracts[candidate_id as usize];
            let chain = store.chain_name(contract.chain_id).to_owned();
            evidence.insert(
                candidate_id,
                EvidenceBundle::empty(candidate_id, chain, contract.address.clone()),
            );
        }
        return Ok(LegitPreflight {
            evidence,
            candidates_to_enrich: registry.candidate_contracts().to_vec(),
            candidate_probes: AHashMap::new(),
        });
    }

    let concurrency = limits.concurrency.max(1);
    let empty_evidence = AHashMap::new();

    progress.begin_phase("seed_caches", Some(seed_ids.len() as u64));
    let empty_evidence_ref = &empty_evidence;
    let mut seed_results = stream::iter(seed_ids.iter().copied().map(|seed_id| async move {
        (
            seed_id,
            build_seed_cache(client, store, empty_evidence_ref, keys, limits, seed_id).await,
        )
    }))
    .buffer_unordered(concurrency);
    let mut seed_caches = AHashMap::with_capacity(seed_ids.len());
    while let Some((seed_id, cache)) = seed_results.next().await {
        progress.check_cancelled()?;
        seed_caches.insert(seed_id, cache);
        progress.add_completed(1);
    }

    // Candidate slugs are useful only for relations whose seed has a slug.
    let slug_candidates: AHashSet<ContractId> = registry
        .relations()
        .iter()
        .filter(|rel| {
            seed_caches
                .get(&rel.seed_contract)
                .and_then(|seed| seed.collection_slug.as_ref())
                .is_some()
        })
        .map(|rel| rel.candidate_contract)
        .collect();

    let mut expected_relations: AHashMap<ContractId, usize> = AHashMap::new();
    let mut offline_relations: AHashMap<ContractId, Vec<(String, LegitSignals)>> = AHashMap::new();
    for relation in registry.relations() {
        *expected_relations
            .entry(relation.candidate_contract)
            .or_default() += 1;
        let Some(seed) = seed_caches.get(&relation.seed_contract) else {
            continue;
        };
        let seed_row = &store.contracts[relation.seed_contract as usize];
        let seed_chain = store.chain_name(seed_row.chain_id);
        let candidate = &store.contracts[relation.candidate_contract as usize];
        let candidate_address = candidate.address.as_str();
        offline_relations
            .entry(relation.candidate_contract)
            .or_default()
            .push((
                seed_key(seed_chain, &seed_row.address),
                offline_relation_signals(seed, seed_chain, candidate_address),
            ));
    }
    let locally_fully_legit: AHashSet<ContractId> = offline_relations
        .iter()
        .filter(|(candidate_id, rows)| {
            rows.len() == expected_relations.get(candidate_id).copied().unwrap_or(0)
                && rows.iter().all(|(_, signals)| signals.is_legit_duplicate())
        })
        .map(|(&candidate_id, _)| candidate_id)
        .collect();
    let identity_candidates = registry
        .candidate_contracts()
        .iter()
        .copied()
        .filter(|candidate_id| !locally_fully_legit.contains(candidate_id))
        .collect::<Vec<_>>();
    eprintln!(
        "evidence: identity prefilter candidates={} offline_short_circuit={} http_required={}",
        registry.candidate_contracts().len(),
        locally_fully_legit.len(),
        identity_candidates.len()
    );
    let slug_candidates_ref = &slug_candidates;
    let solana_identity_rows: Vec<(ContractId, String, String)> = identity_candidates
        .iter()
        .filter_map(|&candidate_id| {
            let contract = &store.contracts[candidate_id as usize];
            let chain = store.chain_name(contract.chain_id);
            chain
                .eq_ignore_ascii_case("solana")
                .then(|| (candidate_id, chain.to_owned(), contract.address.clone()))
        })
        .collect();
    let all_evm_controller_requests: Vec<(ContractId, String, String)> = identity_candidates
        .iter()
        .filter_map(|&candidate_id| {
            let contract = &store.contracts[candidate_id as usize];
            let chain = store.chain_name(contract.chain_id);
            store
                .is_evm_chain(chain)
                .then(|| (candidate_id, chain.to_owned(), contract.address.clone()))
        })
        .collect();
    let mut identity_cache =
        load_candidate_identity_cache(limits.candidate_identity_cache_path.as_deref());
    let mut prefetched_solana_identities = AHashMap::new();
    let mut solana_identity_requests = Vec::new();
    for (candidate_id, chain, address) in &solana_identity_rows {
        let key = candidate_identity_key(chain, address);
        if let Some(probe) = identity_cache
            .get(&key)
            .and_then(|row| row.solana_identity.clone())
        {
            prefetched_solana_identities.insert(*candidate_id, probe);
        } else {
            solana_identity_requests.push(address.clone());
        }
    }
    let mut prefetched_evm_controllers = AHashMap::new();
    let mut evm_controller_requests = Vec::new();
    for (candidate_id, chain, address) in &all_evm_controller_requests {
        let key = candidate_identity_key(chain, address);
        if let Some(outcome) = identity_cache
            .get(&key)
            .and_then(|row| row.evm_controllers.clone())
        {
            prefetched_evm_controllers.insert(*candidate_id, outcome);
        } else {
            evm_controller_requests.push((*candidate_id, chain.clone(), address.clone()));
        }
    }
    let identity_cache_hits = prefetched_solana_identities.len() + prefetched_evm_controllers.len();
    eprintln!(
        "evidence: identity cache_hits={} evm_http_missing={} solana_http_missing={}",
        identity_cache_hits,
        evm_controller_requests.len(),
        solana_identity_requests.len()
    );
    progress.begin_phase(
        "candidate_identity_prefetch",
        Some((solana_identity_rows.len() + all_evm_controller_requests.len()) as u64),
    );
    progress.add_completed(identity_cache_hits as u64);
    let (solana_identities, fetched_evm_controllers) = tokio::join!(
        helius::fetch_collection_identities_batch(
            client,
            &limits.endpoints.helius,
            keys.helius(),
            &solana_identity_requests,
            concurrency,
            progress,
        ),
        controllers::fetch_evm_controllers_batch(
            client,
            &limits.endpoints,
            keys.alchemy(),
            &evm_controller_requests,
            concurrency,
            progress,
        ),
    );
    prefetched_evm_controllers.extend(fetched_evm_controllers);
    for (candidate_id, chain, address) in &solana_identity_rows {
        if let Some(mut probe) = solana_identities.get(address).cloned() {
            probe.identity = probe.identity.map(|slug| slug.to_ascii_lowercase());
            prefetched_solana_identities.insert(*candidate_id, probe.clone());
            let key = candidate_identity_key(chain, address);
            let row = CandidateIdentityCacheRow {
                chain: chain.clone(),
                address: address.clone(),
                evm_controllers: None,
                solana_identity: Some(probe),
            };
            if reusable_identity_row(&row) {
                identity_cache.insert(key, row);
            }
        }
    }
    for (candidate_id, chain, address) in &all_evm_controller_requests {
        let Some(outcome) = prefetched_evm_controllers.get(candidate_id).cloned() else {
            continue;
        };
        let row = CandidateIdentityCacheRow {
            chain: chain.clone(),
            address: address.clone(),
            evm_controllers: Some(outcome),
            solana_identity: None,
        };
        if reusable_identity_row(&row) {
            identity_cache.insert(candidate_identity_key(chain, address), row);
        }
    }
    write_candidate_identity_cache(
        limits.candidate_identity_cache_path.as_deref(),
        &identity_cache,
    );
    let prefetched_solana_identities_ref = &prefetched_solana_identities;
    let prefetched_evm_controllers_ref = &prefetched_evm_controllers;
    progress.begin_phase("candidate_identity", Some(identity_candidates.len() as u64));
    let mut candidate_results = stream::iter(identity_candidates.iter().copied().map(
        |candidate_id| async move {
            (
                candidate_id,
                build_candidate_probe(
                    client,
                    store,
                    keys,
                    limits,
                    candidate_id,
                    slug_candidates_ref.contains(&candidate_id),
                    CandidatePrefetch {
                        evm_controllers: prefetched_evm_controllers_ref.get(&candidate_id).cloned(),
                        solana_identity: prefetched_solana_identities_ref
                            .get(&candidate_id)
                            .cloned(),
                    },
                )
                .await,
            )
        },
    ))
    .buffer_unordered(concurrency);
    let mut candidate_probes = AHashMap::with_capacity(registry.candidate_contracts().len());
    while let Some((candidate_id, probe)) = candidate_results.next().await {
        progress.check_cancelled()?;
        candidate_probes.insert(candidate_id, probe);
        progress.add_completed(1);
    }

    let relation_total = registry
        .relations()
        .iter()
        .filter(|rel| !locally_fully_legit.contains(&rel.candidate_contract))
        .count();
    progress.begin_phase("relations", Some(relation_total as u64));
    let mut relation_results = stream::iter(registry.relations().iter().filter_map(|rel| {
        if locally_fully_legit.contains(&rel.candidate_contract) {
            return None;
        }
        let candidate = candidate_probes.get(&rel.candidate_contract)?;
        let seed_cache = seed_caches.get(&rel.seed_contract)?;
        let seed_row = &store.contracts[rel.seed_contract as usize];
        let seed_chain = store.chain_name(seed_row.chain_id);
        let seed_address = seed_row.address.as_str();
        let is_evm = store.is_evm_chain(seed_chain);
        let candidate_id = rel.candidate_contract;
        Some(async move {
            let signals = probe_relation(
                client,
                keys,
                limits,
                seed_chain,
                is_evm,
                seed_address,
                &candidate.address,
                &candidate.controllers,
                candidate.collection_slug.as_deref(),
                seed_cache,
            )
            .await;
            (candidate_id, seed_key(seed_chain, seed_address), signals)
        })
    }))
    .buffer_unordered(concurrency);

    let mut relation_legit: AHashMap<ContractId, Vec<(String, LegitSignals)>> = AHashMap::new();
    while let Some((candidate_id, key, signals)) = relation_results.next().await {
        progress.check_cancelled()?;
        relation_legit
            .entry(candidate_id)
            .or_default()
            .push((key, signals));
        progress.add_completed(1);
    }
    drop(relation_results);

    let mut evidence = AHashMap::with_capacity(registry.candidate_contracts().len());
    for candidate_id in locally_fully_legit {
        let contract = &store.contracts[candidate_id as usize];
        let chain = store.chain_name(contract.chain_id).to_owned();
        let mut bundle = EvidenceBundle::empty(candidate_id, chain, contract.address.clone());
        if let Some(rows) = offline_relations.remove(&candidate_id) {
            for (key, signals) in rows {
                bundle.relation_legit.insert(key, signals);
            }
        }
        finalize_legit_signals(&mut bundle);
        evidence.insert(candidate_id, bundle);
    }
    let mut candidates_to_enrich = Vec::new();
    for (&candidate_id, probe) in &candidate_probes {
        let mut bundle =
            EvidenceBundle::empty(candidate_id, probe.chain.clone(), probe.address.clone());
        bundle.controllers = probe.controllers.clone();
        if let Some(rows) = relation_legit.remove(&candidate_id) {
            for (key, signals) in rows {
                bundle.relation_legit.insert(key, signals);
            }
        }
        finalize_legit_signals(&mut bundle);
        let expected = expected_relations.get(&candidate_id).copied().unwrap_or(0);
        let fully_legit = all_relations_legit(&bundle, expected);
        if !fully_legit {
            candidates_to_enrich.push(candidate_id);
        }
        evidence.insert(candidate_id, bundle);
    }
    candidates_to_enrich.sort_unstable();

    Ok(LegitPreflight {
        evidence,
        candidates_to_enrich,
        candidate_probes,
    })
}

/// Compatibility entry point for callers that already hold enriched bundles.
///
/// New pipeline code should use the pre-enrichment gate in the orchestrator.
pub async fn attach_relation_legit(
    evidence: &mut AHashMap<ContractId, EvidenceBundle>,
    registry: &CandidateRegistry,
    store: &ResidentStore,
    client: &HttpClient,
    keys: &ApiKeys,
    limits: &HttpLimits,
) {
    let Ok(mut preflight) =
        prefilter_candidates(registry, store, client, keys, limits, &NoopProgress).await
    else {
        return;
    };
    for (&candidate_id, bundle) in evidence.iter_mut() {
        let Some(mut relation_bundle) = preflight.evidence.remove(&candidate_id) else {
            continue;
        };
        bundle.relation_legit = std::mem::take(&mut relation_bundle.relation_legit);
        bundle.legit = std::mem::take(&mut relation_bundle.legit);
        finalize_legit_signals(bundle);
    }
}

/// Pure helper for unit tests: continuity + slug + interaction without HTTP.
pub fn classify_relation_offline(
    seed_controllers: &[String],
    cand_controllers: &[String],
    chain: &str,
    seed_slug: Option<&str>,
    cand_slug: Option<&str>,
    holds_seed: bool,
    transfer_counterparty: bool,
) -> LegitSignals {
    let mut signals = continuity_signals(seed_controllers, cand_controllers, chain, true);
    if let (Some(a), Some(b)) = (seed_slug, cand_slug)
        && !a.is_empty()
        && a.eq_ignore_ascii_case(b)
    {
        signals.official_collection_relation = true;
        signals
            .evidence_keys
            .push(format!("collection_relation:{}", a.to_ascii_lowercase()));
    }
    if holds_seed {
        signals.seed_nft_interaction = true;
        signals.evidence_keys.push("holds_seed_nft".into());
    }
    if transfer_counterparty {
        signals.seed_nft_interaction = true;
        signals
            .evidence_keys
            .push("seed_transfer_counterparty".into());
    }
    signals
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn shared_controller_marks_continuity() {
        let s = classify_relation_offline(
            &["0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa".into()],
            &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()],
            "ethereum",
            None,
            None,
            false,
            false,
        );
        assert!(s.official_controller_continuity);
        assert!(s.is_legit_duplicate());
    }

    #[test]
    fn same_slug_marks_collection_relation() {
        let s = classify_relation_offline(
            &[],
            &[],
            "ethereum",
            Some("boredapeyachtclub"),
            Some("BoredApeYachtClub"),
            false,
            false,
        );
        assert!(s.official_collection_relation);
        assert!(s.is_legit_duplicate());
    }

    #[test]
    fn holds_or_transfer_marks_interaction() {
        let hold = classify_relation_offline(&[], &[], "ethereum", None, None, true, false);
        assert!(hold.seed_nft_interaction);
        let xfer = classify_relation_offline(&[], &[], "ethereum", None, None, false, true);
        assert!(xfer.seed_nft_interaction);
    }

    #[test]
    fn full_enrich_is_skipped_only_when_every_relation_is_legit() {
        let mut bundle = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        bundle.relation_legit.insert(
            "ethereum:0xseed-a".into(),
            LegitSignals {
                official_controller_continuity: true,
                ..LegitSignals::default()
            },
        );
        assert!(all_relations_legit(&bundle, 1));

        bundle
            .relation_legit
            .insert("ethereum:0xseed-b".into(), LegitSignals::default());
        assert!(
            !all_relations_legit(&bundle, 2),
            "one unresolved/suspicious seed relation must retain the candidate"
        );
    }

    #[test]
    fn open_license_excludes_only_the_licensed_seed_relation() {
        let mut bundle = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        bundle.relation_legit.insert(
            "ethereum:0xopen-seed".into(),
            LegitSignals {
                seed_open_license: true,
                evidence_keys: vec!["seed_open_license".into()],
                verification_complete: true,
                ..LegitSignals::default()
            },
        );
        assert!(all_relations_legit(&bundle, 1));

        bundle
            .relation_legit
            .insert("ethereum:0xclosed-seed".into(), LegitSignals::default());
        assert!(
            !all_relations_legit(&bundle, 2),
            "a suspicious relation must still send a mixed candidate to deep enrichment"
        );
    }

    #[test]
    fn missing_relation_result_never_excludes_candidate() {
        let mut bundle = EvidenceBundle::empty(1, "ethereum", "0xcandidate");
        bundle.relation_legit.insert(
            "ethereum:0xseed-a".into(),
            LegitSignals {
                official_collection_relation: true,
                ..LegitSignals::default()
            },
        );
        assert!(!all_relations_legit(&bundle, 2));
    }

    #[test]
    fn offline_signals_short_circuit_known_interactions() {
        let mut seed = SeedCache {
            open_license: true,
            ..SeedCache::default()
        };
        seed.current_owners
            .insert("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into());
        let signals = offline_relation_signals(
            &seed,
            "ethereum",
            "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa",
        );
        assert!(signals.seed_open_license);
        assert!(signals.seed_nft_interaction);
        assert!(signals.is_legit_duplicate());
    }

    #[test]
    fn candidate_identity_cache_round_trips_only_reusable_rows() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "analysis-candidate-identity-{}-{unique}.json",
            std::process::id()
        ));
        let valid = CandidateIdentityCacheRow {
            chain: "ethereum".into(),
            address: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            evm_controllers: Some(FetchOutcome {
                value: EvmControllerEvidence {
                    addresses: vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()],
                    deployed_block: Some(7),
                },
                status: EvidenceStatus::Complete,
                observation: None,
                failure: None,
                truncated: false,
            }),
            solana_identity: None,
        };
        let failed = CandidateIdentityCacheRow {
            chain: "ethereum".into(),
            address: "0xcccccccccccccccccccccccccccccccccccccccc".into(),
            evm_controllers: Some(FetchOutcome {
                value: EvmControllerEvidence::default(),
                status: EvidenceStatus::Failed,
                observation: None,
                failure: Some("transient".into()),
                truncated: false,
            }),
            solana_identity: None,
        };
        let rows = AHashMap::from_iter([
            (candidate_identity_key(&valid.chain, &valid.address), valid),
            (
                candidate_identity_key(&failed.chain, &failed.address),
                failed,
            ),
        ]);
        write_candidate_identity_cache(Some(&path), &rows);
        let loaded = load_candidate_identity_cache(Some(&path));
        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains_key("ethereum:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        let _ = fs::remove_file(path);
    }
}
