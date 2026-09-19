use crate::entity::{EntityStore, InputRow, SourceOrder};
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use ahash::AHashSet;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_cast::{can_cast_types, cast};
use arrow_schema::DataType;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use rayon::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};

pub const REQUIRED_COLUMNS: [&str; 7] = [
    "chain",
    "contract_address",
    "token_id",
    "name_norm",
    "token_uri_norm",
    "image_uri_norm",
    "metadata_json",
];

#[derive(Clone, Debug)]
pub struct LoadOptions {
    pub allowed_chains: AHashSet<String>,
    pub evm_chains: AHashSet<String>,
    pub metadata_anchors: Option<usize>,
    pub load_names: bool,
    pub load_token_uris: bool,
    pub load_image_uris: bool,
    pub load_metadata: bool,
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self {
            allowed_chains: AHashSet::new(),
            evm_chains: AHashSet::new(),
            metadata_anchors: None,
            load_names: true,
            load_token_uris: true,
            load_image_uris: true,
            load_metadata: true,
        }
    }
}

impl LoadOptions {
    pub fn new(
        allowed_chains: impl IntoIterator<Item = String>,
        evm_chains: impl IntoIterator<Item = String>,
        metadata_anchors: impl Into<Option<usize>>,
    ) -> Self {
        Self {
            allowed_chains: allowed_chains.into_iter().collect(),
            evm_chains: evm_chains.into_iter().collect(),
            metadata_anchors: metadata_anchors.into(),
            load_names: true,
            load_token_uris: true,
            load_image_uris: true,
            load_metadata: true,
        }
    }

    fn loads_column(&self, column: usize) -> bool {
        match column {
            0..=2 => true,
            3 => self.load_names,
            4 => self.load_token_uris,
            5 => self.load_image_uris,
            6 => self.load_metadata,
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
struct ValidatedInput {
    path: PathBuf,
    file_ordinal: u32,
    row_group_count: usize,
    root_projection: Vec<usize>,
    metadata: ArrowReaderMetadata,
    row_count: u64,
}

/// Load with default fixture options. Production callers should use
/// [`load_entities_with_options`] so filtering and anchor bounding happen during scan.
pub fn load_entities(
    input_files: &[PathBuf],
    progress: &dyn ProgressObserver,
) -> Result<EntityStore, DedupError> {
    load_entities_with_options(input_files, &LoadOptions::default(), progress)
}

/// Validate all schemas, scan files in parallel into local shards, then merge shards in
/// explicit input order. The merge preserves stable source order without global hot locks.
pub fn load_entities_with_options(
    input_files: &[PathBuf],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<EntityStore, DedupError> {
    if input_files.is_empty() {
        return Err(DedupError::invalid(
            "load",
            "at least one --input is required",
        ));
    }
    progress.set_stage("load");
    progress.begin_phase("validate", Some(input_files.len() as u64));
    let inputs = validate_inputs(input_files, options, progress)?;
    let total_rows: u64 = inputs.iter().map(|input| input.row_count).sum();
    progress.begin_phase("scan_files", Some(total_rows));

    let shard_results: Vec<Result<EntityStore, DedupError>> = inputs
        .par_iter()
        .map(|input| scan_file_to_shard(input, options, progress))
        .collect();

    progress.begin_phase("merge_shards", Some(shard_results.len() as u64));
    progress.check_cancelled()?;
    let shard_count = shard_results.len() as u64;
    let mut store = merge_shards_ordered(shard_results, options)?;
    progress.add_completed(shard_count);
    store.finalize_metadata_anchors();
    if !options.allowed_chains.is_empty() && store.contracts.is_empty() {
        return Err(DedupError::invalid(
            "load",
            "none of the requested --chains were present in the inputs",
        ));
    }
    if options.load_token_uris || options.load_image_uris {
        progress.begin_phase("build_uri_postings", Some(store.nfts.len() as u64));
        store.rebuild_uri_postings();
        progress.add_completed(store.nfts.len() as u64);
    }
    Ok(store)
}

fn validate_inputs(
    input_files: &[PathBuf],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<Vec<ValidatedInput>, DedupError> {
    let results: Vec<Result<ValidatedInput, DedupError>> =
        input_files
            .par_iter()
            .enumerate()
            .map(|(ordinal, path)| {
                progress.check_cancelled()?;
                let file_ordinal = u32::try_from(ordinal).map_err(|_| {
                    DedupError::invalid("load", "too many input files for file_ordinal")
                })?;
                let file = File::open(path).map_err(|error| DedupError::ParquetRead {
                    path: path.clone(),
                    message: error.to_string(),
                })?;
                let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
                    .map_err(|error| DedupError::ParquetSchema {
                        path: path.clone(),
                        message: error.to_string(),
                    })?;
                let schema = metadata.schema();
                let mut root_projection = Vec::with_capacity(REQUIRED_COLUMNS.len());
                for (column, required) in REQUIRED_COLUMNS.into_iter().enumerate() {
                    let Some((index, field)) = schema
                        .fields()
                        .iter()
                        .enumerate()
                        .find(|(_, field)| field.name() == required)
                    else {
                        return Err(DedupError::ParquetSchema {
                            path: path.clone(),
                            message: format!("missing required column `{required}`"),
                        });
                    };
                    if !can_cast_types(field.data_type(), &DataType::Utf8) {
                        return Err(DedupError::ParquetSchema {
                            path: path.clone(),
                            message: format!(
                                "column `{required}` cannot be cast from {:?} to Utf8",
                                field.data_type()
                            ),
                        });
                    }
                    if options.loads_column(column) {
                        root_projection.push(index);
                    }
                }
                let row_count = metadata.metadata().file_metadata().num_rows().max(0) as u64;
                progress.add_completed(1);
                Ok(ValidatedInput {
                    path: path.clone(),
                    file_ordinal,
                    row_group_count: metadata.metadata().num_row_groups(),
                    root_projection,
                    metadata,
                    row_count,
                })
            })
            .collect();
    results.into_iter().collect()
}

fn scan_file_to_shard(
    input: &ValidatedInput,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<EntityStore, DedupError> {
    let mut row_starts = Vec::with_capacity(input.row_group_count + 1);
    row_starts.push(0_u64);
    for row_group in 0..input.row_group_count {
        let rows = input
            .metadata
            .metadata()
            .row_group(row_group)
            .num_rows()
            .max(0) as u64;
        row_starts.push(row_starts.last().copied().unwrap_or(0).saturating_add(rows));
    }

    // Keep enough independent work for load balancing without paying one file
    // open, reader, EntityStore, and merge path per row group on large files.
    let target_chunks = rayon::current_num_threads().saturating_mul(2).max(1);
    let row_groups_per_chunk = input.row_group_count.div_ceil(target_chunks).max(1);
    let chunks = (0..input.row_group_count)
        .step_by(row_groups_per_chunk)
        .map(|start| {
            let end = (start + row_groups_per_chunk).min(input.row_group_count);
            (start, end, row_starts[start])
        })
        .collect::<Vec<_>>();
    let row_group_results: Vec<Result<EntityStore, DedupError>> = chunks
        .par_iter()
        .map(|&(start, end, row_start)| {
            scan_row_groups_to_shard(input, start..end, row_start, options, progress)
        })
        .collect();
    merge_shards_ordered(row_group_results, options)
}

fn merge_shards_ordered(
    mut shards: Vec<Result<EntityStore, DedupError>>,
    options: &LoadOptions,
) -> Result<EntityStore, DedupError> {
    match shards.len() {
        0 => Ok(EntityStore::with_options(
            options.metadata_anchors,
            &options.evm_chains,
        )),
        1 => shards.pop().expect("one shard is present"),
        _ => {
            let right = shards.split_off(shards.len() / 2);
            let (left, right) = rayon::join(
                || merge_shards_ordered(shards, options),
                || merge_shards_ordered(right, options),
            );
            let mut left = left?;
            left.merge_shard(right?)?;
            Ok(left)
        }
    }
}

fn scan_row_groups_to_shard(
    input: &ValidatedInput,
    row_groups: std::ops::Range<usize>,
    row_start: u64,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<EntityStore, DedupError> {
    progress.check_cancelled()?;
    let file = File::open(&input.path).map_err(|error| DedupError::ParquetRead {
        path: input.path.clone(),
        message: error.to_string(),
    })?;
    let mask = ProjectionMask::roots(
        input.metadata.metadata().file_metadata().schema_descr(),
        input.root_projection.iter().copied(),
    );
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, input.metadata.clone())
        .with_projection(mask)
        .with_row_groups(row_groups.collect())
        .with_batch_size(64 * 1024)
        .build()
        .map_err(|error| DedupError::ParquetRead {
            path: input.path.clone(),
            message: error.to_string(),
        })?;
    let mut shard = EntityStore::with_options(options.metadata_anchors, &options.evm_chains);
    shard.defer_unbounded_metadata_sort();
    let mut row_offset = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|error| DedupError::ParquetRead {
            path: input.path.clone(),
            message: error.to_string(),
        })?;
        let columns = ProjectedColumns::new(&batch, &input.path, options)?;
        for row_index in 0..batch.num_rows() {
            let source_order = SourceOrder {
                file_ordinal: input.file_ordinal,
                file_row_number: row_start + row_offset,
            };
            row_offset += 1;
            let chain = columns.decode_chain(row_index);
            if !options.allowed_chains.is_empty() && !options.allowed_chains.contains(&chain) {
                continue;
            }
            let row = columns.decode_with_chain(row_index, source_order, chain);
            shard.try_ingest_row(row)?;
        }
        progress.add_completed(batch.num_rows() as u64);
    }
    Ok(shard)
}

struct ProjectedColumns {
    columns: [Option<StringArray>; REQUIRED_COLUMNS.len()],
}

impl ProjectedColumns {
    fn new(batch: &RecordBatch, path: &Path, options: &LoadOptions) -> Result<Self, DedupError> {
        let mut columns = std::array::from_fn(|_| None);
        for (column, required) in REQUIRED_COLUMNS.into_iter().enumerate() {
            if !options.loads_column(column) {
                continue;
            }
            let index =
                batch
                    .schema()
                    .index_of(required)
                    .map_err(|error| DedupError::ParquetSchema {
                        path: path.to_path_buf(),
                        message: error.to_string(),
                    })?;
            let source = batch.column(index);
            let converted = if source.data_type() == &DataType::Utf8 {
                source.clone()
            } else {
                cast(source, &DataType::Utf8).map_err(|error| DedupError::ParquetSchema {
                    path: path.to_path_buf(),
                    message: format!(
                        "column `{required}` cannot be cast from {:?} to Utf8: {error}",
                        source.data_type()
                    ),
                })?
            };
            let converted = converted
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("arrow cast to Utf8 must return StringArray")
                .clone();
            columns[column] = Some(converted);
        }
        Ok(Self { columns })
    }

    fn value(&self, column: usize, row: usize) -> &str {
        let Some(column) = &self.columns[column] else {
            return "";
        };
        if column.is_null(row) {
            ""
        } else {
            column.value(row)
        }
    }

    fn decode_chain(&self, row_index: usize) -> String {
        normalize_chain(self.value(0, row_index))
    }

    fn decode_with_chain(
        &self,
        row_index: usize,
        source_order: SourceOrder,
        chain: String,
    ) -> InputRow {
        InputRow {
            chain,
            contract_address: self.value(1, row_index).trim().to_owned(),
            token_id: self.value(2, row_index).trim().to_owned(),
            name_norm: coalesce(self.value(3, row_index)),
            token_uri_norm: coalesce(self.value(4, row_index)),
            image_uri_norm: coalesce(self.value(5, row_index)),
            metadata_json: coalesce_metadata(self.value(6, row_index)),
            source_order,
        }
    }
}

fn normalize_chain(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn coalesce(value: &str) -> String {
    value.to_owned()
}

fn coalesce_metadata(value: &str) -> String {
    value.trim().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::NoopProgress;
    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::{Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    fn write_mixed_schema(path: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chain", DataType::Utf8, false),
            Field::new("contract_address", DataType::Utf8, false),
            Field::new("token_id", DataType::Int64, false),
            Field::new("name_norm", DataType::Utf8, false),
            Field::new("token_uri_norm", DataType::Utf8, false),
            Field::new("image_uri_norm", DataType::Utf8, false),
            Field::new("metadata_json", DataType::Utf8, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["ethereum"])),
            Arc::new(StringArray::from(vec!["0xabc"])),
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(StringArray::from(vec!["collection"])),
            Arc::new(StringArray::from(vec!["uri://42"])),
            Arc::new(StringArray::from(vec!["image://42"])),
            Arc::new(StringArray::from(vec![r#"{"name":"token"}"#])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_multi_row_group_snapshot(path: &Path) {
        let schema = Arc::new(Schema::new(
            REQUIRED_COLUMNS
                .into_iter()
                .map(|name| Field::new(name, DataType::Utf8, false))
                .collect::<Vec<_>>(),
        ));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                "ethereum", "base", "ethereum", "base",
            ])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(StringArray::from(vec!["1", "2", "3", "4"])),
            Arc::new(StringArray::from(vec!["n1", "n2", "n3", "n4"])),
            Arc::new(StringArray::from(vec!["u1", "u2", "u3", "u4"])),
            Arc::new(StringArray::from(vec!["i1", "i2", "i3", "i4"])),
            Arc::new(StringArray::from(vec!["{}", "{}", "{}", "{}"])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_size(1)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn casts_compatible_columns_and_filters_during_scan() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mixed.parquet");
        write_mixed_schema(&path);
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], 1);
        let store = load_entities_with_options(&[path], &options, &NoopProgress).unwrap();
        assert_eq!(store.nfts.len(), 1);
        assert_eq!(store.nfts[0].token_id, "42");
        assert_eq!(store.contracts[0].metadata_by_token.len(), 1);
    }

    #[test]
    fn disabled_dimensions_are_not_materialized() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("identity-only.parquet");
        write_mixed_schema(&path);
        let mut options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], None);
        options.load_names = false;
        options.load_token_uris = false;
        options.load_image_uris = false;
        options.load_metadata = false;

        let store = load_entities_with_options(&[path], &options, &NoopProgress).unwrap();

        assert_eq!(store.nfts.len(), 1);
        assert!(store.strings.is_empty());
        assert!(store.nfts[0].name_id.is_none());
        assert!(store.nfts[0].token_uri_id.is_none());
        assert!(store.nfts[0].image_uri_id.is_none());
        assert!(store.contracts[0].metadata_by_token.is_empty());
        assert!(store.token_uri_postings.is_empty());
        assert!(store.image_uri_postings.is_empty());
    }

    #[test]
    fn chunked_row_groups_preserve_source_rows_after_early_chain_filter() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("row-groups.parquet");
        write_multi_row_group_snapshot(&path);
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], None);

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let store = pool
            .install(|| load_entities_with_options(&[path], &options, &NoopProgress))
            .unwrap();

        assert_eq!(
            store
                .nfts
                .iter()
                .map(|nft| (nft.token_id.as_str(), nft.source_order.file_row_number))
                .collect::<Vec<_>>(),
            [("1", 0), ("3", 2)]
        );
    }

    #[test]
    fn unknown_requested_chain_is_an_error() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mixed.parquet");
        write_mixed_schema(&path);
        let options = LoadOptions::new(["missing".to_owned()], Vec::new(), 1);
        let error = load_entities_with_options(&[path], &options, &NoopProgress).unwrap_err();
        assert!(error.to_string().contains("none of the requested"));
    }

    #[test]
    fn ordered_tree_merge_preserves_source_order_and_stable_contract_state() {
        let options = LoadOptions::default();
        let shards = (0..5)
            .map(|index| {
                let mut shard = EntityStore::default();
                shard
                    .try_ingest_row(InputRow {
                        chain: "solana".to_owned(),
                        contract_address: "collection".to_owned(),
                        token_id: index.to_string(),
                        name_norm: format!("name-{}", 5 - index),
                        token_uri_norm: format!("uri://{index}"),
                        image_uri_norm: String::new(),
                        metadata_json: String::new(),
                        source_order: SourceOrder {
                            file_ordinal: index,
                            file_row_number: 0,
                        },
                    })
                    .unwrap();
                Ok(shard)
            })
            .collect();

        let merged = merge_shards_ordered(shards, &options).unwrap();
        assert_eq!(merged.rows_loaded, 5);
        assert_eq!(
            merged
                .nfts
                .iter()
                .map(|nft| merged.string(nft.name_id.unwrap()))
                .collect::<Vec<_>>(),
            ["name-5", "name-4", "name-3", "name-2", "name-1"]
        );
        assert_eq!(
            merged
                .nfts
                .iter()
                .map(|nft| nft.token_id.as_str())
                .collect::<Vec<_>>(),
            ["0", "1", "2", "3", "4"]
        );
    }
}
