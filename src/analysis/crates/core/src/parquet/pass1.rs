//! Pass 1: identity + name + URI column projection.

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::DataType;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::borrow::Cow;
use std::fs::File;
use std::ops::Range;
use std::path::Path;

use crate::AnalysisError;
use crate::entity::{ResidentStore, SourceOrder};
use crate::parquet::LoadOptions;
use crate::parquet::merge::merge_shards_ordered;
use crate::parquet::validate::{IDENTITY_COLUMNS, PASS1_COLUMNS, ValidatedInput};
use crate::progress::ProgressObserver;

pub fn scan_pass1(
    inputs: &[ValidatedInput],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, AnalysisError> {
    let shard_results: Vec<Result<ResidentStore, AnalysisError>> = inputs
        .par_iter()
        .map(|input| scan_file_pass1(input, options, progress))
        .collect();
    merge_shards_ordered(shard_results, options)
}

#[derive(Clone, Debug)]
pub(crate) struct RowGroupChunk {
    pub(crate) row_groups: Range<usize>,
    pub(crate) row_start: u64,
}

/// Split a file into a bounded number of contiguous row-group tasks. Keeping
/// several row groups on one reader amortizes file open/reader construction
/// without reducing parallelism on the default large-file workload.
pub(crate) fn row_group_chunks(input: &ValidatedInput) -> Vec<RowGroupChunk> {
    if input.row_group_count == 0 {
        return Vec::new();
    }
    let target_tasks = rayon::current_num_threads().saturating_mul(2).max(1);
    let groups_per_task = input.row_group_count.div_ceil(target_tasks).max(1);
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
    (0..input.row_group_count)
        .step_by(groups_per_task)
        .map(|start| RowGroupChunk {
            row_groups: start..(start + groups_per_task).min(input.row_group_count),
            row_start: row_starts[start],
        })
        .collect()
}

fn scan_file_pass1(
    input: &ValidatedInput,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, AnalysisError> {
    let chunks = row_group_chunks(input);
    let chunk_results: Vec<Result<ResidentStore, AnalysisError>> = chunks
        .par_iter()
        .map(|chunk| scan_row_group_chunk_pass1(input, chunk, options, progress))
        .collect();
    merge_shards_ordered(chunk_results, options)
}

fn scan_row_group_chunk_pass1(
    input: &ValidatedInput,
    chunk: &RowGroupChunk,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, AnalysisError> {
    progress.check_cancelled()?;
    let file = File::open(&input.path)
        .map_err(|error| AnalysisError::parquet(format!("{}: {error}", input.path.display())))?;
    let (projection, column_names): (&[usize], &[&str]) = if options.build_dedup_indexes {
        (&input.pass1_projection, &PASS1_COLUMNS)
    } else {
        (&input.identity_projection, &IDENTITY_COLUMNS)
    };
    let mask = ProjectionMask::roots(
        input.metadata.metadata().file_metadata().schema_descr(),
        projection.iter().copied(),
    );
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, input.metadata.clone())
        .with_projection(mask)
        .with_row_groups(chunk.row_groups.clone().collect())
        .with_batch_size(8 * 1024)
        .build()
        .map_err(|error| AnalysisError::parquet(format!("{}: {error}", input.path.display())))?;
    let mut shard = ResidentStore::with_options(options.metadata_anchors, &options.evm_chains);
    let mut row_offset = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|error| {
            AnalysisError::parquet(format!("{}: {error}", input.path.display()))
        })?;
        let columns = ProjectedUtf8Columns::new(&batch, &input.path, column_names)?;
        for row_index in 0..batch.num_rows() {
            let source_order = SourceOrder {
                file_ordinal: input.file_ordinal,
                file_row_number: chunk.row_start + row_offset,
            };
            row_offset += 1;
            let chain = normalized_chain(columns.value_at(0, row_index));
            if !options.allowed_chains.is_empty()
                && !options.allowed_chains.contains(chain.as_ref())
            {
                continue;
            }
            let contract_address = columns.value_at(1, row_index).trim();
            if let Some(filter) = &options.identity_contract_filter {
                let address = if options.evm_chains.contains(chain.as_ref()) {
                    if contract_address
                        .bytes()
                        .any(|byte| byte.is_ascii_uppercase())
                    {
                        Cow::Owned(contract_address.to_ascii_lowercase())
                    } else {
                        Cow::Borrowed(contract_address)
                    }
                } else {
                    Cow::Borrowed(contract_address)
                };
                if !filter
                    .get(chain.as_ref())
                    .is_some_and(|contracts| contracts.contains(address.as_ref()))
                {
                    continue;
                }
            }
            // Cache replay only needs identity. Avoid decoding and interning the
            // large Name/URI columns that dedup queries would consume.
            let dedup_values = if options.build_dedup_indexes {
                (
                    columns.value_at(3, row_index),
                    columns.value_at(4, row_index),
                    columns.value_at(5, row_index),
                )
            } else {
                ("", "", "")
            };
            shard.ingest_identity_strs(
                chain.as_ref(),
                contract_address,
                columns.value_at(2, row_index).trim(),
                dedup_values.0,
                dedup_values.1,
                dedup_values.2,
                source_order,
            )?;
        }
        progress.add_completed(batch.num_rows() as u64);
    }
    Ok(shard)
}

pub(crate) struct ProjectedUtf8Columns {
    columns: Vec<ArrayRef>,
}

impl ProjectedUtf8Columns {
    pub(crate) fn new(
        batch: &RecordBatch,
        path: &Path,
        names: &[&str],
    ) -> Result<Self, AnalysisError> {
        let mut columns = Vec::with_capacity(names.len());
        for required in names {
            let index = batch
                .schema()
                .index_of(required)
                .map_err(|error| AnalysisError::parquet(format!("{}: {error}", path.display())))?;
            let source = batch.column(index);
            let converted = cast(source, &DataType::Utf8).map_err(|error| {
                AnalysisError::parquet(format!(
                    "{}: column `{required}` cannot be cast from {:?} to Utf8: {error}",
                    path.display(),
                    source.data_type()
                ))
            })?;
            columns.push(converted);
        }
        Ok(Self { columns })
    }

    pub(crate) fn value_at(&self, column: usize, row: usize) -> &str {
        let array = self.columns[column]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arrow cast to Utf8 must return StringArray");
        if array.is_null(row) {
            ""
        } else {
            array.value(row)
        }
    }
}

pub(crate) fn normalize_chain(value: &str) -> String {
    normalized_chain(value).into_owned()
}

pub(crate) fn normalized_chain(value: &str) -> Cow<'_, str> {
    let trimmed = value.trim();
    if trimmed.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Owned(trimmed.to_ascii_lowercase())
    } else {
        Cow::Borrowed(trimmed)
    }
}
