use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

fn write_parquet(path: &Path, rows: &[[&str; 7]]) {
    let schema = Arc::new(Schema::new(
        [
            "chain",
            "contract_address",
            "token_id",
            "name_norm",
            "token_uri_norm",
            "image_uri_norm",
            "metadata_json",
        ]
        .into_iter()
        .map(|name| Field::new(name, DataType::Utf8, false))
        .collect::<Vec<_>>(),
    ));
    let mut columns = vec![Vec::new(); 7];
    for row in rows {
        for (column, value) in row.iter().enumerate() {
            columns[column].push((*value).to_owned());
        }
    }
    let arrays = columns
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn complete_rows() -> Vec<[&'static str; 7]> {
    let image = "data:image/png;base64,iVBORw0KGgo=";
    let metadata = r#"{"collection":"shared","name":"one"}"#;
    vec![
        ["ethereum", "0xa", "1", "same", "ipfs:a", image, metadata],
        ["ethereum", "0xb", "1", "same", "ipfs:b", image, metadata],
        ["base", "0xc", "1", "same", "ipfs:c", image, metadata],
        ["base", "0xd", "1", "same", "ipfs:d", image, metadata],
    ]
}

#[test]
fn all_writes_complete_dedup_reports_without_sampling_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(&input, &complete_rows());
    let out = temp.path().join("out");
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "all",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--chains",
            "ethereum,base",
            "--evm-chains",
            "ethereum,base",
            "--name-threshold",
            "98",
            "--progress",
            "off",
            "--threads",
            "2",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    for name in [
        "summary.csv",
        "chain_matrix.csv",
        "name_summary.csv",
        "uri_summary.csv",
        "run_manifest.json",
    ] {
        assert!(out.join(name).is_file(), "missing report {name}");
    }
    assert!(!out.join("metadata_duplicate_pairs.csv").exists());
    assert!(!out.join("metadata_image_samples.csv").exists());
    assert!(!out.join(".metadata-image-cache").exists());
    assert!(!out.join(".metadata-candidate-cache").exists());
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("run_manifest.json")).unwrap()).unwrap();
    assert!(manifest.get("sample_pairs").is_none());
    assert!(manifest.get("sample_candidate_limit").is_none());
}

#[test]
fn omitted_name_threshold_disables_name_and_omitted_anchors_are_unbounded() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(
        &input,
        &[[
            "ethereum",
            "0xa",
            "1",
            "name-is-not-loaded",
            "",
            "",
            r#"{"name":"metadata"}"#,
        ]],
    );
    let out = temp.path().join("out");
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "all",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--evm-chains",
            "ethereum",
            "--progress",
            "off",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(!out.join("name_summary.csv").exists());
    assert!(!out.join("name_chain_matrix.csv").exists());
    assert!(!out.join("metadata_duplicate_pairs.csv").exists());
    assert!(!out.join("metadata_image_samples.csv").exists());
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("run_manifest.json")).unwrap()).unwrap();
    assert!(manifest["name_threshold"].is_null());
    assert!(manifest["metadata_anchors"].is_null());
    assert_eq!(manifest["interned_strings"], 0);
}

#[test]
fn dedup_commands_reject_legacy_sampling_flags() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(&input, &complete_rows());
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "run-metadata",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            temp.path().join("out").to_str().unwrap(),
            "--evm-chains",
            "ethereum,base",
            "--sample-pairs",
            "2",
            "--progress",
            "off",
        ])
        .status()
        .unwrap();
    assert!(!status.success());
}

#[test]
fn sample_metadata_fills_independent_intra_and_cross_chain_pools() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(&input, &complete_rows());
    let out = temp.path().join("out");
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "sample-metadata",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--chains",
            "ethereum,base",
            "--evm-chains",
            "ethereum,base",
            "--sample-pairs",
            "2",
            "--seed",
            "4242",
            "--progress",
            "off",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    let all = csv::Reader::from_path(out.join("metadata_duplicate_pairs.csv"))
        .unwrap()
        .records()
        .count();
    let intra = csv::Reader::from_path(out.join("metadata_duplicate_pairs_intra_chain.csv"))
        .unwrap()
        .records()
        .count();
    let cross = csv::Reader::from_path(out.join("metadata_duplicate_pairs_chain_matrix.csv"))
        .unwrap()
        .records()
        .count();
    assert_eq!((all, intra, cross), (4, 2, 2));
    let mut manifest = csv::Reader::from_path(out.join("metadata_image_samples.csv")).unwrap();
    let headers = manifest.headers().unwrap().clone();
    let pool_column = headers.iter().position(|field| field == "pool").unwrap();
    let pool_row_column = headers
        .iter()
        .position(|field| field == "pool_row")
        .unwrap();
    let seed_column = headers
        .iter()
        .position(|field| field == "sampling_seed")
        .unwrap();
    let records = manifest.records().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 4);
    assert!(records.iter().all(|record| &record[seed_column] == "4242"));
    for pool in ["intra_chain", "cross_chain"] {
        let mut pool_rows = records
            .iter()
            .filter(|record| &record[pool_column] == pool)
            .map(|record| record[pool_row_column].parse::<usize>().unwrap())
            .collect::<Vec<_>>();
        pool_rows.sort_unstable();
        assert_eq!(pool_rows, vec![1, 2]);
        for index in 1..=2 {
            let row = out.join(format!("metadata_sample_images/{pool}/{index}"));
            assert!(row.join(format!("{index}a.png")).is_file());
            assert!(row.join(format!("{index}b.png")).is_file());
            assert!(row.join(format!("{index}a.json")).is_file());
            assert!(row.join(format!("{index}b.json")).is_file());
        }
    }
    assert!(!out.join("metadata_sample_images/1").exists());
    assert!(out.join(".metadata-image-cache").is_dir());
    assert!(!out.join(".metadata-candidate-cache").exists());
    assert!(!out.join("summary.csv").exists());
    assert!(!out.join("run_manifest.json").exists());
}

#[test]
fn failed_downloads_do_not_consume_pool_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    let image = "data:image/png;base64,iVBORw0KGgo=";
    let metadata = r#"{"collection":"shared"}"#;
    write_parquet(
        &input,
        &[
            [
                "ethereum",
                "0xbad",
                "1",
                "",
                "",
                "unsupported://bad",
                metadata,
            ],
            ["ethereum", "0xa", "1", "", "", image, metadata],
            ["ethereum", "0xb", "1", "", "", image, metadata],
            ["base", "0xc", "1", "", "", image, metadata],
            ["base", "0xd", "1", "", "", image, metadata],
        ],
    );
    let out = temp.path().join("out");
    let status = Command::new(env!("CARGO_BIN_EXE_dedup"))
        .args([
            "sample-metadata",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--evm-chains",
            "ethereum,base",
            "--sample-pairs",
            "1",
            "--progress",
            "off",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(
        csv::Reader::from_path(out.join("metadata_image_samples.csv"))
            .unwrap()
            .records()
            .count(),
        2
    );
}
