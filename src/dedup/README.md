# NFT deduplication

Match Parquet snapshots in memory by Name, URI, and Metadata. Run from the repository root:

## Recommended commands

After exporting snapshots to `output/`, run full matching or sample matching media:

```powershell
$inputArgs = @(
  '--input', 'output/ethereum.parquet', '--input', 'output/base.parquet',
  '--input', 'output/polygon.parquet', '--input', 'output/solana.parquet',
  '--chains', 'ethereum,base,polygon,solana',
  '--evm-chains', 'ethereum,base,polygon'
)
cargo run -p dedup_cli --locked --profile dedup-release -- all @inputArgs `
  --output-dir out/dedup --name-threshold 98 --metadata-threshold 0.6
cargo run -p dedup_cli --locked --profile dedup-release -- sample-metadata @inputArgs `
  --output-dir out/samples --sample-pairs 100 --seed 42
```

## Behavior and outputs

- `all`, `run-name`, `run-uri`, and `run-metadata` perform matching without media downloads.
- Name matching requires `--name-threshold` as a percentage, such as `98`.
- Metadata defaults to threshold `0.6` and all valid records; `--metadata-anchors N` caps retention.
- `--threads N` controls Rayon parallelism. Outputs include `summary.csv`, `chain_matrix.csv`, and `run_manifest.json`.

Use `sample-metadata` with the same input/chain options and `--sample-pairs N`
for separate intra-chain and cross-chain media pools. `--seed` controls random
ordering, but downloads affect inclusion; samples are not uniform population
estimates. Insufficient complete pairs return an error and preserve existing outputs.
Successful downloads are cached in `.metadata-image-cache`.

Sampling writes `metadata_duplicate_pairs.csv`, `metadata_image_samples.csv`,
and media under `metadata_sample_images/`.

For Linux SMT comparisons, run `python src/dedup/scripts/compare_smt.py --help`.
The script requires `taskset` and visible SMT topology; it manages thread counts
and output directories for each run.
