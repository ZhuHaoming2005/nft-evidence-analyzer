//! Ordered tree-merge of ResidentStore shards.

use crate::AnalysisError;
use crate::entity::ResidentStore;
use crate::parquet::LoadOptions;

pub fn merge_shards_ordered(
    mut shards: Vec<Result<ResidentStore, AnalysisError>>,
    options: &LoadOptions,
) -> Result<ResidentStore, AnalysisError> {
    match shards.len() {
        0 => Ok(ResidentStore::with_options(
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
