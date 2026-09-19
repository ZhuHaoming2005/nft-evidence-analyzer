# NFT Evidence Analyzer

Research artifact for NFT data collection, content deduplication, and evidence
analysis across Ethereum, Base, Polygon, and Solana.

## Components

| Directory | Function |
|---|---|
| `src/fetch` | Python collectors that store NFT records and metadata in PostgreSQL |
| `src/export` | Rust exporter that produces per-chain Parquet snapshots |
| `src/dedup` | In-memory Name, URI, and Metadata matching and media sampling |
| `src/analysis` | Seed selection, candidate matching, evidence collection, and behavior/economic reports |
| `tests` | Offline Python collector tests using mocked APIs and databases |

```text
Blockchain/provider APIs -> Collectors -> PostgreSQL -> Parquet snapshots
                                                        |-> Deduplication
                                                        |-> Seed evidence analysis
```

All commands run from the repository root. Component READMEs describe their
specific inputs, outputs, and options.

## Environment and build

Use Python 3.10+ and a Rust toolchain supporting edition 2024. Live collection
requires PostgreSQL and provider credentials. Rust dependencies are recorded in
`Cargo.lock`; Python dependencies are listed in `requirements.txt` without a lockfile.

```powershell
python -m venv .venv
.venv/Scripts/Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
cargo build --workspace --locked
```

Configure PostgreSQL and provider settings in `.env` for the Python collectors.
For Solana, merge `src/fetch/solana/.env.example` into that file. Rust commands do
not load `.env`: export database settings as process environment variables and
pass analysis provider keys through CLI options.

The root Cargo workspace manages all Rust packages and shares `target/`:

| Package | Executable |
|---|---|
| `nft-snapshot-export` | `nft-snapshot-export` |
| `dedup_cli` | `dedup` |
| `analysis_cli` | `analysis` |
| `dedup_core`, `analysis_core` | Libraries |

Use `--release` for the shared thin-LTO profile or `--profile dedup-release` for
the deduplication fat-LTO profile. Matching indexes remain in memory; there is no
automatic disk spill or approximate fallback. The large-scale design target is
128 vCPU / 512 GiB RAM on Linux, not a measured minimum requirement.

## Workflow

1. Run the chain scanner and metadata fetcher in separate terminals. Collection
   persists scan progress and stores accepted records in `nft_assets_<chain>`.
2. Export each chain to Parquet. Each export uses a consistent database transaction;
   separate chain exports are not synchronized snapshots.
3. Run `dedup all` for snapshot-wide matching, or `sample-metadata` for media samples.
4. Use `analysis select-seeds` to rank collections, then `run-dedup` for seed matching
   or `run` for evidence collection and full reports. Manually supplied seeds are supported.

Inspect command options without contacting providers:

```powershell
cargo run -p nft-snapshot-export --locked -- --help
cargo run -p dedup_cli --locked -- --help
cargo run -p analysis_cli --locked -- --help
```

Name matching is disabled unless a threshold is supplied. `dedup` uses a percentage
such as `98`; `analysis` uses a fraction such as `0.98`. Metadata similarity defaults
to `0.6`. Analysis downloads uncached seed populations through Alchemy or Helius,
so `run-dedup` is not necessarily offline.

Deduplication writes CSV summaries and a run manifest. Analysis writes JSON and
Markdown reports under `detail/` and `summary/`, with caches and manifests under
`intermediate/`. Compatible caches are reused automatically.

## Validation

```powershell
python -m unittest discover -s tests -v
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Rust tests cover matching oracles, generated Parquet fixtures, and report behavior.
The exporter database test is ignored by default; set `NFT_EXPORT_TEST_DATABASE_URL`
to a disposable PostgreSQL database and run:

```powershell
cargo test -p nft-snapshot-export --locked metadata_column_follows_search_path -- --ignored
```

## Reproducibility and interpretation

Record the revision, toolchain and dependency versions, ordered input hashes,
export bounds, seeds, CLI parameters, hardware, and run manifests. Archive the
seed and evidence caches used for each experiment. Live rankings, API responses,
holder snapshots, and spot prices can change between runs. Keep local inputs and
outputs under `data/`, `seeds/`, `out/`, or `output/`; these are ignored by Git.

No production dataset or provider credentials are distributed. Offline tests do
not establish full-dataset performance or reproduce paper results. Collection
filters and provider coverage constrain the observed population. Media samples
are not uniform population estimates, and content similarity alone does not
establish infringement. Reports retain evidence quality and attribution limits;
buyer paid exposure is not realized loss, and USD amounts use execution-time prices.

Licensed under MIT.
