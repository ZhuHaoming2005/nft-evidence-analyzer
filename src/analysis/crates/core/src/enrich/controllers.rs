//! Fill `EvidenceBundle.controllers` from Alchemy / on-chain (EVM) and Helius (Solana).

use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::collections::BTreeMap;

use crate::entity::ContractId;
use crate::progress::ProgressObserver;

use super::alchemy::FetchOutcome;
use super::http::HttpClient;
use super::types::ProviderEndpoints;

const EIP1967_ADMIN_SLOT: &str =
    "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103";

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct EvmControllerEvidence {
    pub addresses: Vec<String>,
    pub deployed_block: Option<u64>,
}

/// EVM: Alchemy `getContractMetadata` fields + on-chain owner/admin/EIP-1967.
pub async fn fetch_evm_controllers(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
) -> FetchOutcome<EvmControllerEvidence> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("contract_controllers");
    };
    let Some(nft_url) = endpoints.alchemy_nft(chain, api_key, "getContractMetadata") else {
        return FetchOutcome::failed(
            "alchemy",
            "contract_controllers",
            format!("unsupported alchemy network for {chain}"),
        );
    };
    let Some(rpc_url) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "contract_controllers",
            format!("unsupported alchemy rpc for {chain}"),
        );
    };

    let meta_url = format!("{nft_url}?contractAddress={contract}");
    let mut controllers = Vec::new();
    let mut supplemental_failed = false;
    let mut deployer: Option<String> = None;
    let mut deployed_block = None;

    match client.get_json_alchemy(&meta_url, &[]).await {
        Ok(payload) => {
            if payload.get("error").is_some() {
                supplemental_failed = true;
            } else {
                let metadata = payload.get("contractMetadata").unwrap_or(&payload);
                for field in [
                    "contractDeployer",
                    "ownerAddress",
                    "owner",
                    "adminAddress",
                    "proxyAdminAddress",
                ] {
                    push_evm_address(
                        &mut controllers,
                        metadata
                            .get(field)
                            .or_else(|| payload.get(field))
                            .and_then(Value::as_str),
                    );
                }
                deployer = [
                    "contractDeployer",
                    "deployerAddress",
                    "deployer",
                    "creatorAddress",
                ]
                .into_iter()
                .find_map(|field| {
                    metadata
                        .get(field)
                        .or_else(|| payload.get(field))
                        .and_then(Value::as_str)
                        .and_then(normalize_evm_address)
                });
                deployed_block = metadata
                    .get("deployedBlockNumber")
                    .or_else(|| payload.get("deployedBlockNumber"))
                    .and_then(parse_block_number);
            }
        }
        Err(_) => {
            supplemental_failed = true;
        }
    }

    match onchain_controllers(client, &rpc_url, contract).await {
        Ok(onchain) => {
            for addr in onchain {
                push_evm_address(&mut controllers, Some(&addr));
            }
        }
        Err(_) => {
            supplemental_failed = true;
        }
    }

    if let Some(deployer) = deployer {
        push_evm_address(&mut controllers, Some(&deployer));
    }

    controllers.sort();
    controllers.dedup();
    let count = controllers.len();
    let mut outcome = FetchOutcome::ok(
        EvmControllerEvidence {
            addresses: controllers,
            deployed_block,
        },
        count,
        supplemental_failed,
        "alchemy",
        "contract_controllers",
    );
    // Truncated when supplemental probes failed but we still have some addresses.
    if supplemental_failed && count > 0 {
        outcome.status = super::types::EvidenceStatus::Truncated;
        if let Some(obs) = outcome.observation.as_mut() {
            obs.status = super::types::EvidenceStatus::Truncated;
        }
    }
    outcome
}

/// Fetch controller evidence for many contracts while preserving the exact
/// per-contract output shape of [`fetch_evm_controllers`]. Contracts are
/// grouped by chain and sent in bounded metadata/RPC batches; an unusable
/// provider batch falls back to the original individual path.
pub async fn fetch_evm_controllers_batch(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    requests: &[(ContractId, String, String)],
    concurrency: usize,
    progress: &dyn ProgressObserver,
) -> ahash::AHashMap<ContractId, FetchOutcome<EvmControllerEvidence>> {
    // Each contract contributes four JSON-RPC calls. Keep the resulting RPC
    // batch below 50 calls, then run several small batches concurrently.
    const CONTRACTS_PER_BATCH: usize = 12;

    let mut out = ahash::AHashMap::with_capacity(requests.len());
    let Some(api_key) = api_key else {
        for (id, _, _) in requests {
            out.insert(*id, FetchOutcome::skipped("contract_controllers"));
        }
        progress.add_completed(requests.len() as u64);
        return out;
    };

    let mut by_chain: BTreeMap<String, Vec<(ContractId, String)>> = BTreeMap::new();
    for (id, chain, contract) in requests {
        by_chain
            .entry(chain.clone())
            .or_default()
            .push((*id, contract.clone()));
    }

    let jobs = by_chain
        .into_iter()
        .flat_map(|(chain, rows)| {
            rows.chunks(CONTRACTS_PER_BATCH)
                .map(|chunk| (chain.clone(), chunk.to_vec()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    // Each batch starts metadata and RPC requests together. Reserving two
    // provider slots per job prevents the outer scheduler from flooding the
    // shared Alchemy lane.
    let batch_concurrency = (concurrency.max(1) / 2).max(1);
    let mut results = stream::iter(jobs.into_iter().map(|(chain, chunk)| async move {
        if chunk.len() == 1 {
            let (id, contract) = &chunk[0];
            return ahash::AHashMap::from_iter([(
                *id,
                fetch_evm_controllers(client, endpoints, Some(api_key), &chain, contract).await,
            )]);
        }
        if let Some(rows) = fetch_controller_chunk(client, endpoints, api_key, &chain, &chunk).await
        {
            return rows;
        }

        // A malformed/unusable provider batch must not serialize the whole
        // chunk. Retry its candidates independently under the same HTTP lane.
        let mut fallback = stream::iter(chunk.into_iter().map(|(id, contract)| {
            let chain = chain.clone();
            async move {
                (
                    id,
                    fetch_evm_controllers(client, endpoints, Some(api_key), &chain, &contract)
                        .await,
                )
            }
        }))
        .buffer_unordered(concurrency.max(1));
        let mut rows = ahash::AHashMap::new();
        while let Some((id, outcome)) = fallback.next().await {
            rows.insert(id, outcome);
        }
        rows
    }))
    .buffer_unordered(batch_concurrency);
    while let Some(rows) = results.next().await {
        progress.add_completed(rows.len() as u64);
        out.extend(rows);
    }
    out
}

async fn fetch_controller_chunk(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: &str,
    chain: &str,
    rows: &[(ContractId, String)],
) -> Option<ahash::AHashMap<ContractId, FetchOutcome<EvmControllerEvidence>>> {
    let metadata_url = endpoints.alchemy_nft(chain, api_key, "getContractMetadataBatch")?;
    let rpc_url = endpoints.alchemy_rpc(chain, api_key)?;
    let metadata_body = json!({
        "contractAddresses": rows
            .iter()
            .map(|(_, address)| address)
            .collect::<Vec<_>>()
    });
    let rpc_body = Value::Array(
        rows.iter()
            .enumerate()
            .flat_map(|(idx, (_, contract))| {
                [
                    json!({
                        "jsonrpc": "2.0",
                        "id": format!("{idx}:owner"),
                        "method": "eth_call",
                        "params": [{"to": contract, "data": "0x8da5cb5b"}, "latest"]
                    }),
                    json!({
                        "jsonrpc": "2.0",
                        "id": format!("{idx}:owner-fallback"),
                        "method": "eth_call",
                        "params": [{"to": contract, "data": "0x893d20e8"}, "latest"]
                    }),
                    json!({
                        "jsonrpc": "2.0",
                        "id": format!("{idx}:admin"),
                        "method": "eth_call",
                        "params": [{"to": contract, "data": "0xf851a440"}, "latest"]
                    }),
                    json!({
                        "jsonrpc": "2.0",
                        "id": format!("{idx}:eip1967-admin"),
                        "method": "eth_getStorageAt",
                        "params": [contract, EIP1967_ADMIN_SLOT, "latest"]
                    }),
                ]
            })
            .collect(),
    );
    let (metadata_payload, rpc_payload) = tokio::join!(
        client.post_json_alchemy(&metadata_url, &[], &metadata_body),
        client.post_json_alchemy(&rpc_url, &[], &rpc_body),
    );
    let metadata_payload = metadata_payload.ok()?;
    let rpc_payload = rpc_payload.ok()?;
    let metadata_rows = metadata_payload.as_array()?;
    let rpc_rows = rpc_payload.as_array()?;

    let mut metadata_by_address = ahash::AHashMap::with_capacity(metadata_rows.len());
    for row in metadata_rows {
        let metadata = row.get("contractMetadata").unwrap_or(row);
        let Some(address) = metadata
            .get("address")
            .or_else(|| row.get("address"))
            .and_then(Value::as_str)
            .and_then(normalize_evm_address)
        else {
            continue;
        };
        metadata_by_address.insert(address, metadata);
    }

    let mut rpc_by_id = ahash::AHashMap::with_capacity(rpc_rows.len());
    for row in rpc_rows {
        if let Some(id) = row.get("id").and_then(Value::as_str) {
            rpc_by_id.insert(id.to_owned(), row);
        }
    }

    let mut out = ahash::AHashMap::with_capacity(rows.len());
    for (idx, (contract_id, contract)) in rows.iter().enumerate() {
        let normalized = normalize_evm_address(contract)?;
        let metadata = metadata_by_address.get(&normalized).copied();
        let mut controllers = Vec::new();
        let mut deployer = None;
        let mut deployed_block = None;
        let mut supplemental_failed = metadata.is_none();
        if let Some(metadata) = metadata {
            for field in [
                "contractDeployer",
                "ownerAddress",
                "owner",
                "adminAddress",
                "proxyAdminAddress",
            ] {
                push_evm_address(
                    &mut controllers,
                    metadata.get(field).and_then(Value::as_str),
                );
            }
            deployer = [
                "contractDeployer",
                "deployerAddress",
                "deployer",
                "creatorAddress",
            ]
            .into_iter()
            .find_map(|field| {
                metadata
                    .get(field)
                    .and_then(Value::as_str)
                    .and_then(normalize_evm_address)
            });
            deployed_block = metadata
                .get("deployedBlockNumber")
                .and_then(parse_block_number);
        }

        let storage_id = format!("{idx}:eip1967-admin");
        if rpc_by_id
            .get(&storage_id)
            .is_none_or(|row| row.get("result").and_then(Value::as_str).is_none())
        {
            supplemental_failed = true;
        }
        let mut owner = None;
        let mut owner_fallback = None;
        for suffix in ["owner", "owner-fallback", "admin", "eip1967-admin"] {
            let id = format!("{idx}:{suffix}");
            let Some(address) = rpc_by_id
                .get(&id)
                .and_then(|row| abi_address(row.get("result").and_then(Value::as_str)))
            else {
                continue;
            };
            match suffix {
                "owner" => owner = Some(address),
                "owner-fallback" => owner_fallback = Some(address),
                _ => controllers.push(address),
            }
        }
        if let Some(address) = owner.or(owner_fallback) {
            controllers.push(address);
        }
        if let Some(deployer) = deployer {
            controllers.push(deployer);
        }
        controllers.sort();
        controllers.dedup();
        let count = controllers.len();
        let outcome = FetchOutcome::ok(
            EvmControllerEvidence {
                addresses: controllers,
                deployed_block,
            },
            count,
            supplemental_failed,
            "alchemy",
            "contract_controllers",
        );
        out.insert(*contract_id, outcome);
    }
    Some(out)
}

fn parse_block_number(value: &Value) -> Option<u64> {
    if let Some(number) = value.as_u64() {
        return Some(number);
    }
    let text = value.as_str()?.trim();
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16).ok()
    } else {
        text.parse().ok()
    }
}

async fn onchain_controllers(
    client: &HttpClient,
    rpc_url: &str,
    contract: &str,
) -> Result<Vec<String>, String> {
    let batch = json!([
        {
            "jsonrpc": "2.0",
            "id": "owner",
            "method": "eth_call",
            "params": [{"to": contract, "data": "0x8da5cb5b"}, "latest"]
        },
        {
            "jsonrpc": "2.0",
            "id": "owner-fallback",
            "method": "eth_call",
            "params": [{"to": contract, "data": "0x893d20e8"}, "latest"]
        },
        {
            "jsonrpc": "2.0",
            "id": "admin",
            "method": "eth_call",
            "params": [{"to": contract, "data": "0xf851a440"}, "latest"]
        },
        {
            "jsonrpc": "2.0",
            "id": "eip1967-admin",
            "method": "eth_getStorageAt",
            "params": [contract, EIP1967_ADMIN_SLOT, "latest"]
        }
    ]);
    let payload = client
        .post_json_alchemy(rpc_url, &[], &batch)
        .await
        .map_err(|e| e.to_string())?;
    let rows = payload
        .as_array()
        .ok_or_else(|| "Alchemy controller batch response was not an array".to_owned())?;
    let storage_complete = rows.iter().any(|row| {
        row.get("id").and_then(Value::as_str) == Some("eip1967-admin")
            && row.get("result").and_then(Value::as_str).is_some()
    });
    if !storage_complete {
        return Err("Alchemy controller batch omitted the EIP-1967 storage result".into());
    }

    let mut controllers = Vec::new();
    let mut owner = None;
    let mut owner_fallback = None;
    for row in rows {
        let id = row.get("id").and_then(Value::as_str).unwrap_or_default();
        let Some(address) = abi_address(row.get("result").and_then(Value::as_str)) else {
            continue;
        };
        match id {
            "owner" => owner = Some(address),
            "owner-fallback" => owner_fallback = Some(address),
            _ => controllers.push(address),
        }
    }
    if let Some(address) = owner.or(owner_fallback) {
        controllers.push(address);
    }
    controllers.sort();
    controllers.dedup();
    Ok(controllers)
}

fn abi_address(value: Option<&str>) -> Option<String> {
    let raw = value?.trim();
    let hex = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X"))?;
    if hex.len() < 40 {
        return None;
    }
    let address = &hex[hex.len() - 40..];
    if address.bytes().all(|byte| byte == b'0')
        || !address.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(format!("0x{}", address.to_ascii_lowercase()))
}

fn push_evm_address(values: &mut Vec<String>, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return;
    };
    let Some(value) = normalize_evm_address(value) else {
        return;
    };
    values.push(value);
}

pub fn normalize_evm_address(value: &str) -> Option<String> {
    let hex = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    if hex.len() != 40
        || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        || hex.bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    Some(format!("0x{}", hex.to_ascii_lowercase()))
}

/// Extract collection `updateAuthority` (+ verified creators) from a DAS asset item / result.
pub fn solana_authorities_from_asset(
    item: &Value,
    result: &Value,
    collection: &str,
) -> Vec<String> {
    let mut out = Vec::new();
    let metadata = item
        .get("grouping")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|group| {
            let key = group
                .get("group_key")
                .or_else(|| group.get("groupKey"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            key == "collection"
                && group
                    .get("group_value")
                    .or_else(|| group.get("groupValue"))
                    .and_then(Value::as_str)
                    .is_none_or(|value| value == collection)
        })
        .and_then(|group| {
            group
                .get("collection_metadata")
                .or_else(|| group.get("collectionMetadata"))
        })
        .or_else(|| result.get("collection_metadata"))
        .or_else(|| result.get("collectionMetadata"));

    if let Some(metadata) = metadata {
        for field in ["update_authority", "updateAuthority"] {
            if let Some(addr) = metadata.get(field).and_then(Value::as_str) {
                let trimmed = addr.trim();
                if !trimmed.is_empty() {
                    out.push(trimmed.to_owned());
                }
            }
        }
    }

    // Verified creators on the asset itself.
    let creators = item
        .get("creators")
        .or_else(|| item.pointer("/content/metadata/creators"))
        .and_then(Value::as_array);
    if let Some(creators) = creators {
        for creator in creators {
            let verified = creator
                .get("verified")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !verified {
                continue;
            }
            if let Some(addr) = creator
                .get("address")
                .or_else(|| creator.get("creator"))
                .and_then(Value::as_str)
            {
                let trimmed = addr.trim();
                if !trimmed.is_empty() {
                    out.push(trimmed.to_owned());
                }
            }
        }
    }

    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use serde_json::json;

    #[test]
    fn normalize_rejects_zero_and_short() {
        assert!(normalize_evm_address("0x0000000000000000000000000000000000000000").is_none());
        assert!(normalize_evm_address("0xabc").is_none());
        assert_eq!(
            normalize_evm_address("0xAbcDef0123456789AbcDef0123456789AbcDef01").as_deref(),
            Some("0xabcdef0123456789abcdef0123456789abcdef01")
        );
    }

    #[test]
    fn abi_address_takes_trailing_40() {
        let padded = "0x000000000000000000000000abcdef0123456789abcdef0123456789abcdef01";
        assert_eq!(
            abi_address(Some(padded)).as_deref(),
            Some("0xabcdef0123456789abcdef0123456789abcdef01")
        );
    }

    #[test]
    fn solana_authorities_prefer_update_authority_and_verified_creators() {
        let item = json!({
            "grouping": [{
                "group_key": "collection",
                "group_value": "Coll1111111111111111111111111111111111111",
                "collection_metadata": {
                    "updateAuthority": "Auth1111111111111111111111111111111111111"
                }
            }],
            "creators": [
                {"address": "Cre11111111111111111111111111111111111111", "verified": true},
                {"address": "Fake1111111111111111111111111111111111111", "verified": false}
            ]
        });
        let authorities = solana_authorities_from_asset(
            &item,
            &Value::Null,
            "Coll1111111111111111111111111111111111111",
        );
        assert!(
            authorities
                .iter()
                .any(|a| a == "Auth1111111111111111111111111111111111111")
        );
        assert!(
            authorities
                .iter()
                .any(|a| a == "Cre11111111111111111111111111111111111111")
        );
        assert!(
            !authorities
                .iter()
                .any(|a| a == "Fake1111111111111111111111111111111111111")
        );
    }

    #[tokio::test]
    async fn controller_batch_uses_one_metadata_and_one_rpc_request() {
        let server = MockServer::start_async().await;
        let first = "0x1111111111111111111111111111111111111111";
        let second = "0x2222222222222222222222222222222222222222";
        let metadata = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/nft/getContractMetadataBatch")
                    .body_contains("contractAddresses");
                then.status(200).json_body(json!([
                    {
                        "address": first,
                        "contractDeployer": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "deployedBlockNumber": 10
                    },
                    {
                        "address": second,
                        "contractDeployer": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "deployedBlockNumber": 20
                    }
                ]));
            })
            .await;
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST).path("/rpc").body_contains("eip1967-admin");
                then.status(200).json_body(json!([
                    {"jsonrpc":"2.0","id":"0:owner","result":"0x000000000000000000000000cccccccccccccccccccccccccccccccccccccccc"},
                    {"jsonrpc":"2.0","id":"0:owner-fallback","result":"0x"},
                    {"jsonrpc":"2.0","id":"0:admin","result":"0x"},
                    {"jsonrpc":"2.0","id":"0:eip1967-admin","result":"0x"},
                    {"jsonrpc":"2.0","id":"1:owner","result":"0x000000000000000000000000dddddddddddddddddddddddddddddddddddddddd"},
                    {"jsonrpc":"2.0","id":"1:owner-fallback","result":"0x"},
                    {"jsonrpc":"2.0","id":"1:admin","result":"0x"},
                    {"jsonrpc":"2.0","id":"1:eip1967-admin","result":"0x"}
                ]));
            })
            .await;
        let endpoints = ProviderEndpoints {
            alchemy_nft_template: format!("{}/nft/{{method}}", server.base_url()),
            alchemy_rpc_template: format!("{}/rpc", server.base_url()),
            ..ProviderEndpoints::default()
        };
        let client = HttpClient::with_retries(4, 0).unwrap();
        let rows = fetch_evm_controllers_batch(
            &client,
            &endpoints,
            Some("key"),
            &[
                (1, "ethereum".into(), first.into()),
                (2, "ethereum".into(), second.into()),
            ],
            4,
            &crate::progress::NoopProgress,
        )
        .await;

        assert_eq!(metadata.hits(), 1);
        assert_eq!(rpc.hits(), 1);
        assert_eq!(rows[&1].value.deployed_block, Some(10));
        assert!(
            rows[&1]
                .value
                .addresses
                .contains(&"0xcccccccccccccccccccccccccccccccccccccccc".into())
        );
        assert_eq!(rows[&2].value.deployed_block, Some(20));
    }
}
