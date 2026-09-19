# NFT Evidence Analyzer

Research artifact for NFT collection, snapshot export, content deduplication, and
evidence analysis across Ethereum, Base, Polygon, and Solana.

| Component | Purpose |
|---|---|
| [fetch](src/fetch/README.md) | Collect NFT records and metadata into PostgreSQL |
| [export](src/export/README.md) | Export consistent per-chain Parquet snapshots |
| [dedup](src/dedup/README.md) | Measure snapshot-wide Name, URI, and Metadata duplication; sample matching media |
| [analysis](src/analysis/README.md) | Select seeds, identify matches, collect evidence, and generate reports |
| [docs](docs/README.md) | Matching methods, reporting semantics, and reproducibility boundaries |
| [tests](tests) | Offline collector regression tests |

```text
Chain/provider APIs -> Python collectors -> PostgreSQL -> Parquet exporter
                                                           |-> dedup
                                                           |-> seed analysis
```

## Setup

Run commands from the repository root. Install Python 3.10+ and a Rust toolchain
supporting edition 2024. Live collection also needs PostgreSQL and provider access.
One root Cargo workspace manages all five Rust crates with a shared `Cargo.lock`
and `target/` directory. Use `--locked` for reproducible dependency resolution.
JSON arbitrary-precision support is enabled for every member so numeric parsing
does not depend on whether a package is built alone or with the whole workspace.

| Package | Location | Executable |
|---|---|---|
| `analysis_core` | `src/analysis/crates/core` | Library |
| `analysis_cli` | `src/analysis/crates/cli` | `analysis` |
| `dedup_core` | `src/dedup/crates/core` | Library |
| `dedup_cli` | `src/dedup/crates/cli` | `dedup` |
| `nft-snapshot-export` | `src/export` | `nft-snapshot-export` |

Use `cargo build --workspace --locked` to build everything, or select a package
with `-p`, for example `cargo run -p analysis_cli --locked -- --help`.
The shared release profile uses thin LTO. `--profile dedup-release` preserves
the deduplication executable's fat-LTO build; outputs go to `target/dedup-release/`.

```powershell
python -m venv .venv
.venv/Scripts/Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Fill in `.env` for Python collection. The exporter reads process environment
variables; the analysis CLI accepts provider keys as command-line flags. Neither
Rust program automatically loads `.env`. Component READMEs contain runnable commands.

## Offline validation

```powershell
python -m unittest discover -s tests -v
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Rust tests cover matching oracles, report fields, and generated Parquet fixtures.
Collector tests use mocked providers and databases. No production dataset or
provider credentials are distributed. Local inputs and results belong in `data/`,
`seeds/`, `out/`, or `output/` and are excluded from version control.

## Reproducing an experiment

Record the Git revision, toolchain versions, dependency versions, ordered input
paths and hashes, export time and block bounds, selected seeds, CLI flags, hardware,
and generated run manifests alongside each result. Archive seed and evidence caches
when comparing reruns: live rankings, holder snapshots, API responses, and spot
prices change over time. Python dependencies currently have no lockfile, so record
`python -m pip freeze` with the run.

The target large-scale host is 128 vCPU / 512 GiB RAM on Linux; this is a design
target, not a measured minimum. Matching indexes are resident in memory. Passing
offline tests does not establish full-dataset performance, live evidence coverage,
or reproduction of paper results. Content similarity alone does not establish
infringement; reports retain evidence-quality and attribution boundaries.

Code is distributed under the [MIT license](LICENSE).
