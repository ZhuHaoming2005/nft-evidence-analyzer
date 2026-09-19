# In-memory NFT deduplication

`dedup_core` implements matching and `dedup_cli` builds the `dedup` binary.
Parquet columns are scanned into resident indexes; there is no disk spill or
automatic approximate fallback. See the [methods](../../docs/dedup/EXPERIMENT.md).
All commands run from the repository root.

```bash
cargo build -p dedup_cli --locked --profile dedup-release
./target/dedup-release/dedup all \
  --input ./output/base.parquet \
  --input ./output/ethereum.parquet \
  --input ./output/polygon.parquet \
  --input ./output/solana.parquet \
  --output-dir ./out/dedup \
  --chains base,ethereum,polygon,solana \
  --evm-chains base,ethereum,polygon
```

On Windows, use `target/dedup-release/dedup.exe`. Name matching is disabled unless
`--name-threshold` is supplied as a percentage, for example `98`. Metadata uses
all valid records by default; `--metadata-anchors N` explicitly limits retention.
The Metadata similarity threshold defaults to `0.6`. `--threads N` sets Rayon
parallelism; omission uses the system default. `--progress auto` selects terminal
output for a TTY and JSON Lines otherwise.

`all`, `run-name`, `run-uri`, and `run-metadata` perform matching without media
downloads. Outputs include `summary.csv`, `chain_matrix.csv`, and
`run_manifest.json`. Completed Name and URI stages also publish their own
`name_*` and `uri_*` summary/matrix CSVs.

## Metadata media samples

```bash
./target/dedup-release/dedup sample-metadata \
  --input ./output/base.parquet \
  --input ./output/ethereum.parquet \
  --input ./output/polygon.parquet \
  --input ./output/solana.parquet \
  --output-dir ./out/samples \
  --chains base,ethereum,polygon,solana \
  --evm-chains base,ethereum,polygon \
  --sample-pairs 100 --seed 42
```

Sampling is a separate command and does not produce full-population duplicate
statistics. It searches matching candidates until it obtains `N` complete media
pairs in each of the intra-chain and cross-chain pools, balancing chain or
unordered chain-pair buckets where candidates are available. It is not uniform
sampling over every valid duplicate pair. The same contract may occur in multiple
pairs. Download failures affect eligibility, so a fixed `--seed` controls random
ordering but does not freeze live network outcomes. Omit it for fresh randomness.

HTTP requests and redirects accept only public destinations, use no proxy, and
share a per-image timeout. IPFS, Arweave, and inline image URIs are supported.
Successful media bytes are cached under `.metadata-image-cache`; failed responses
are not cached. Matching indexes remain in memory.

The command publishes complete outputs transactionally and recovers interrupted
publication on the next run. Insufficient complete pairs return an error while
preserving any previously published sample set. Outputs are:

- `metadata_duplicate_pairs.csv` and its intra-chain, chain-matrix, and cross-chain summaries;
- `metadata_image_samples.csv`, including selected witnesses and the sampling seed;
- `metadata_sample_images/{intra_chain,cross_chain}/<pool_row>/`, containing original media and per-side JSON metadata.

## Linux SMT comparison

The optional script compares one worker per physical core against all available
SMT siblings using `taskset` and the current cpuset. It alternates modes, isolates
outputs, and compares median `direct_bm25` duration from run manifests.

```bash
python3 src/dedup/scripts/compare_smt.py \
  --binary ./target/dedup-release/dedup --output-root ./out/smt --repetitions 3 \
  -- run-metadata --input ./output/base.parquet --input ./output/ethereum.parquet \
  --chains base,ethereum --evm-chains base,ethereum
```

The script supplies `--threads` and `--output-dir`; do not repeat them after `--`.
It requires visible SMT topology and writes `smt_comparison.json`.

## Validation

```bash
cargo test -p dedup_core -p dedup_cli --locked
cargo clippy -p dedup_core -p dedup_cli --all-targets --locked -- -D warnings
```
