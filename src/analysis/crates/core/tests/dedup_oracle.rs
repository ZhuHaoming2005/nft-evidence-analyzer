//! Oracle fixtures for seed-scoped URI dedup (known duplicates).

use ahash::AHashMap;
use analysis_core::dedup::uri::query_uri_for_seed;
use analysis_core::{
    Dimension, HitGraph, IdentityRow, NoopProgress, ResidentStore, ScopeKind, SourceOrder,
    count_scope_nfts,
};

fn row(
    chain: &str,
    contract: &str,
    token: &str,
    token_uri: &str,
    image_uri: &str,
    row_number: u64,
) -> IdentityRow {
    IdentityRow {
        chain: chain.to_owned(),
        contract_address: contract.to_owned(),
        token_id: token.to_owned(),
        name_norm: String::new(),
        token_uri_norm: token_uri.to_owned(),
        image_uri_norm: image_uri.to_owned(),
        source_order: SourceOrder {
            file_ordinal: 0,
            file_row_number: row_number,
        },
    }
}

fn prepared(rows: impl IntoIterator<Item = IdentityRow>) -> ResidentStore {
    let mut store = ResidentStore::new();
    for r in rows {
        store.ingest_identity_row(r).expect("ingest");
    }
    store.rebuild_uri_csr();
    store
}

fn contract_id(store: &ResidentStore, chain: &str, address: &str) -> u32 {
    store
        .contract_id(chain, address)
        .expect("contract must exist")
}

fn contract_nft_map(store: &ResidentStore) -> AHashMap<u32, Vec<u32>> {
    let mut map: AHashMap<u32, Vec<u32>> = AHashMap::new();
    for (nft_id, nft) in store.nfts.iter().enumerate() {
        map.entry(nft.contract_id).or_default().push(nft_id as u32);
    }
    map
}

#[test]
fn oracle_intra_and_cross_token_uri_duplicates() {
    // ethereum seed 0xa shares token URI with ethereum 0xb and base 0xc.
    let store = prepared([
        row("ethereum", "0xa", "1", "ipfs://shared", "", 1),
        row("ethereum", "0xb", "1", "ipfs://shared", "", 2),
        row("base", "0xc", "1", "ipfs://shared", "", 3),
        row("ethereum", "0xd", "1", "ipfs://unique", "", 4),
    ]);
    let seed = contract_id(&store, "ethereum", "0xa");
    let mut graph = HitGraph::new();
    query_uri_for_seed(&store, seed, &mut graph, &NoopProgress).unwrap();

    // Seed self excluded.
    assert!(
        graph
            .edges()
            .iter()
            .all(|e| e.seed_contract != e.candidate_contract)
    );

    let eth = store.chain_ids["ethereum"];
    let base = store.chain_ids["base"];
    let map = contract_nft_map(&store);

    let intra = count_scope_nfts(&graph, seed, ScopeKind::IntraChain, eth, None, &map);
    assert_eq!(intra.token_uri, 1, "one candidate NFT on ethereum 0xb");
    assert_eq!(intra.image_uri, 0);

    let matrix = count_scope_nfts(&graph, seed, ScopeKind::ChainMatrix, eth, Some(base), &map);
    assert_eq!(matrix.token_uri, 1, "one candidate NFT on base 0xc");

    let cross = count_scope_nfts(&graph, seed, ScopeKind::CrossChainSummary, eth, None, &map);
    assert_eq!(cross.token_uri, 1);

    // Unique-only contract is not a candidate of the seed.
    let unique = contract_id(&store, "ethereum", "0xd");
    assert!(graph.edges().iter().all(|e| e.candidate_contract != unique));
}

#[test]
fn oracle_image_uri_fallback_when_token_misses_scope() {
    // Same NFT has token hit intra, but only image hit cross-chain.
    let store = prepared([
        row("ethereum", "0xa", "1", "token://same", "image://same", 1),
        row("ethereum", "0xb", "1", "token://same", "image://other", 2),
        row("base", "0xc", "1", "token://base-only", "image://same", 3),
    ]);
    let seed = contract_id(&store, "ethereum", "0xa");
    let mut graph = HitGraph::new();
    query_uri_for_seed(&store, seed, &mut graph, &NoopProgress).unwrap();

    let eth = store.chain_ids["ethereum"];
    let base = store.chain_ids["base"];
    let map = contract_nft_map(&store);

    let intra = count_scope_nfts(&graph, seed, ScopeKind::IntraChain, eth, None, &map);
    assert_eq!(intra.token_uri, 1);
    assert_eq!(
        intra.image_uri, 0,
        "image supplemental skipped when token already hit intra"
    );

    let matrix = count_scope_nfts(&graph, seed, ScopeKind::ChainMatrix, eth, Some(base), &map);
    assert_eq!(matrix.token_uri, 0);
    assert_eq!(matrix.image_uri, 1, "image fills token miss on matrix");

    assert!(graph.edges().iter().any(|e| {
        e.dimension == Dimension::ImageUri && e.primary_chain == eth && e.secondary_chain == base
    }));
}

#[test]
fn oracle_single_contract_uri_is_not_intra_duplicate() {
    let store = prepared([
        row("ethereum", "0xa", "1", "ipfs://only-here", "", 1),
        row("ethereum", "0xa", "2", "ipfs://only-here", "", 2),
    ]);
    let seed = contract_id(&store, "ethereum", "0xa");
    let mut graph = HitGraph::new();
    query_uri_for_seed(&store, seed, &mut graph, &NoopProgress).unwrap();
    assert!(graph.is_empty());
}
