//! Schema validation and Arrow reader metadata for Parquet inputs.

use arrow_cast::can_cast_types;
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use rayon::prelude::*;
use std::fs::File;
use std::path::PathBuf;

use crate::AnalysisError;
use crate::progress::ProgressObserver;

pub const PASS1_COLUMNS: [&str; 6] = [
    "chain",
    "contract_address",
    "token_id",
    "name_norm",
    "token_uri_norm",
    "image_uri_norm",
];

pub const IDENTITY_COLUMNS: [&str; 3] = ["chain", "contract_address", "token_id"];

pub const PASS2_COLUMNS: [&str; 4] = ["chain", "contract_address", "token_id", "metadata_json"];

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
pub struct ValidatedInput {
    pub path: PathBuf,
    pub file_ordinal: u32,
    pub row_group_count: usize,
    pub identity_projection: Vec<usize>,
    pub pass1_projection: Vec<usize>,
    pub pass2_projection: Vec<usize>,
    pub metadata: ArrowReaderMetadata,
    pub row_count: u64,
}

pub fn validate_inputs(
    input_files: &[PathBuf],
    progress: &dyn ProgressObserver,
) -> Result<Vec<ValidatedInput>, AnalysisError> {
    let results: Vec<Result<ValidatedInput, AnalysisError>> = input_files
        .par_iter()
        .enumerate()
        .map(|(ordinal, path)| {
            progress.check_cancelled()?;
            let file_ordinal = u32::try_from(ordinal)
                .map_err(|_| AnalysisError::invalid("too many input files for file_ordinal"))?;
            let file = File::open(path)
                .map_err(|error| AnalysisError::parquet(format!("{}: {error}", path.display())))?;
            let metadata =
                ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).map_err(|error| {
                    AnalysisError::parquet(format!("{}: schema load: {error}", path.display()))
                })?;
            let schema = metadata.schema();
            let mut required_indices = Vec::with_capacity(REQUIRED_COLUMNS.len());
            for required in REQUIRED_COLUMNS {
                let Some((index, field)) = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, field)| field.name() == required)
                else {
                    return Err(AnalysisError::parquet(format!(
                        "{}: missing required column `{required}`",
                        path.display()
                    )));
                };
                if !can_cast_types(field.data_type(), &DataType::Utf8) {
                    return Err(AnalysisError::parquet(format!(
                        "{}: column `{required}` cannot be cast from {:?} to Utf8",
                        path.display(),
                        field.data_type()
                    )));
                }
                required_indices.push((required, index));
            }
            let index_of = |name: &str| -> usize {
                required_indices
                    .iter()
                    .find(|(n, _)| *n == name)
                    .map(|(_, i)| *i)
                    .expect("required column present")
            };
            let identity_projection = IDENTITY_COLUMNS.iter().map(|c| index_of(c)).collect();
            let pass1_projection = PASS1_COLUMNS.iter().map(|c| index_of(c)).collect();
            let pass2_projection = PASS2_COLUMNS.iter().map(|c| index_of(c)).collect();
            let row_count = metadata.metadata().file_metadata().num_rows().max(0) as u64;
            progress.add_completed(1);
            Ok(ValidatedInput {
                path: path.clone(),
                file_ordinal,
                row_group_count: metadata.metadata().num_row_groups(),
                identity_projection,
                pass1_projection,
                pass2_projection,
                metadata,
                row_count,
            })
        })
        .collect();
    results.into_iter().collect()
}
