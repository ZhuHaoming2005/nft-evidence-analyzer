//! Tiny multi-chain Parquet fixtures for load tests.

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::AnalysisError;

fn write_rows(path: &Path, rows: &[FixtureRow]) -> Result<(), AnalysisError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("chain", DataType::Utf8, false),
        Field::new("contract_address", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("name_norm", DataType::Utf8, false),
        Field::new("token_uri_norm", DataType::Utf8, false),
        Field::new("image_uri_norm", DataType::Utf8, false),
        Field::new("metadata_json", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.chain).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.contract_address).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.token_id).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.name_norm).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.token_uri_norm).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.image_uri_norm).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.metadata_json).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| AnalysisError::parquet(e.to_string()))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .map_err(|e| AnalysisError::parquet(e.to_string()))?;
    writer
        .write(&batch)
        .map_err(|e| AnalysisError::parquet(e.to_string()))?;
    writer
        .close()
        .map_err(|e| AnalysisError::parquet(e.to_string()))?;
    Ok(())
}

struct FixtureRow {
    chain: &'static str,
    contract_address: &'static str,
    token_id: &'static str,
    name_norm: &'static str,
    token_uri_norm: &'static str,
    image_uri_norm: &'static str,
    metadata_json: &'static str,
}

/// 2 EVM contracts + 1 Solana collection; shared token URI across chains; k=2 anchors.
pub fn write_tiny_multichain_fixture(path: &Path) -> Result<(), AnalysisError> {
    write_rows(
        path,
        &[
            // ethereum 0xaaa — tokens 1, 10, 2 (descending anchors → 10, 2)
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xaaa",
                token_id: "1",
                name_norm: "Alpha",
                token_uri_norm: "ipfs://shared",
                image_uri_norm: "",
                metadata_json: r#"{"name":"t1"}"#,
            },
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xaaa",
                token_id: "10",
                name_norm: "Alpha",
                token_uri_norm: "ipfs://a10",
                image_uri_norm: "",
                metadata_json: r#"{"name":"t10"}"#,
            },
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xaaa",
                token_id: "2",
                name_norm: "Alpha",
                token_uri_norm: "ipfs://a2",
                image_uri_norm: "",
                metadata_json: r#"{"name":"t2"}"#,
            },
            // base 0xbbb — shares token URI with ethereum#1
            FixtureRow {
                chain: "base",
                contract_address: "0xbbb",
                token_id: "5",
                name_norm: "Beta",
                token_uri_norm: "ipfs://shared",
                image_uri_norm: "ipfs://img5",
                metadata_json: r#"{"name":"b5"}"#,
            },
            // solana collxyz — lex descending anchors → mint_z, mint_a
            FixtureRow {
                chain: "solana",
                contract_address: "collxyz",
                token_id: "mint_a",
                name_norm: "SolA",
                token_uri_norm: "ipfs://sola",
                image_uri_norm: "",
                metadata_json: r#"{"name":"sa"}"#,
            },
            FixtureRow {
                chain: "solana",
                contract_address: "collxyz",
                token_id: "mint_z",
                name_norm: "SolZ",
                token_uri_norm: "ipfs://solz",
                image_uri_norm: "",
                metadata_json: r#"{"name":"sz"}"#,
            },
        ],
    )
}

/// Same logical key with conflicting non-empty token_uri_norm.
pub fn write_uri_conflict_fixture(path: &Path) -> Result<(), AnalysisError> {
    write_rows(
        path,
        &[
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xconflict",
                token_id: "1",
                name_norm: "X",
                token_uri_norm: "ipfs://one",
                image_uri_norm: "",
                metadata_json: r#"{"name":"a"}"#,
            },
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xconflict",
                token_id: "1",
                name_norm: "X",
                token_uri_norm: "ipfs://two",
                image_uri_norm: "",
                metadata_json: r#"{"name":"b"}"#,
            },
        ],
    )
}

/// Controlled duplicate URIs for offline report golden tests.
///
/// ethereum `0xseed` (3 NFTs) shares `ipfs://intra-shared` with ethereum `0xdup`
/// and `ipfs://cross-shared` with base `0xbase`. Solana is a non-matching peer.
pub fn write_report_golden_fixture(path: &Path) -> Result<(), AnalysisError> {
    write_rows(
        path,
        &[
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xseed",
                token_id: "1",
                name_norm: "GoldenSeed",
                token_uri_norm: "ipfs://intra-shared",
                image_uri_norm: "",
                metadata_json: r#"{"name":"seed1"}"#,
            },
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xseed",
                token_id: "2",
                name_norm: "GoldenSeed",
                token_uri_norm: "ipfs://seed-only",
                image_uri_norm: "",
                metadata_json: r#"{"name":"seed2"}"#,
            },
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xseed",
                token_id: "3",
                name_norm: "GoldenSeed",
                token_uri_norm: "ipfs://cross-shared",
                image_uri_norm: "",
                metadata_json: r#"{"name":"seed3"}"#,
            },
            FixtureRow {
                chain: "ethereum",
                contract_address: "0xdup",
                token_id: "1",
                name_norm: "DupContract",
                token_uri_norm: "ipfs://intra-shared",
                image_uri_norm: "",
                metadata_json: r#"{"name":"dup1"}"#,
            },
            FixtureRow {
                chain: "base",
                contract_address: "0xbase",
                token_id: "1",
                name_norm: "BaseOnly",
                token_uri_norm: "ipfs://cross-shared",
                image_uri_norm: "",
                metadata_json: r#"{"name":"base1"}"#,
            },
            FixtureRow {
                chain: "solana",
                contract_address: "collsol",
                token_id: "mint_a",
                name_norm: "SolOnly",
                token_uri_norm: "ipfs://sol-only",
                image_uri_norm: "",
                metadata_json: r#"{"name":"sol1"}"#,
            },
        ],
    )
}
