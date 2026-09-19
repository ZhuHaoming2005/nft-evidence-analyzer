# Multi-chain NFT evidence analysis

In-memory matching, evidence collection, and reporting for Ethereum, Base, Polygon, and Solana seeds.

Methods and reporting semantics: [`docs/analysis/EXPERIMENT.md`](../../docs/analysis/EXPERIMENT.md)

## Hardware

Target research host: **128 vCPU / 512 GiB RAM** (Linux preferred). Dedup indexes,
evidence, and decoded seed NFT download batches remain in process memory while each seed
population is also persisted to a compressed durable cache. Seed download memory is released
after the identity and metadata overlays consume it. Memory exhaustion still fails the process
(no approximate fallback). Prefer `--rayon-threads` near core count
and tune `--http-concurrency` to provider limits (the default is 12 per provider).

## Build

Run commands from the repository root. Provider keys below are process environment
variables passed explicitly to the CLI; the program does not load `.env`.
Name thresholds here use a fraction (for example `0.98`), while the standalone
`dedup` CLI uses a percentage (for example `98`).

```powershell
cargo build -p analysis_cli --locked --release
```

Binary: `target/release/analysis.exe` (Windows) / `target/release/analysis` (Unix).

## Seeds JSON

`select-seeds` writes a JSON array under `--output-dir/seeds.json` (plus `seeds.audit.json`).
Hand-written seeds for `run-dedup` / `run` still work:

```json
[
  { "chain": "ethereum", "address": "0xseed", "rank": 1 }
]
```

`chain` + `address` are required; `rank` is optional. Extra fields from `select-seeds`
(`name`, `metric`, `window`, `source`, `collected_at`) are ignored by the run readers.

### `select-seeds`

```powershell
cargo run -p analysis_cli --locked --release -- select-seeds `
  --output-dir ./out/seeds `
  --chains ethereum,base,polygon,solana `
  --seeds-per-chain 25 `
  --opensea-api-key $env:OPENSEA_API_KEY `
  --nftscan-api-key $env:NFTSCAN_API_KEY `
  --progress auto
```

EVM ranking uses OpenSea `thirty_days_volume` (API key required). Solana uses NFTScan's
30-day trade ranking (API key required) and accepts only valid Solana collection addresses.
Incomplete chains are recorded in `seeds.audit.json` and are not backfilled from other chains.

## Seed snapshots and `run-dedup`

Materialize the golden Parquet once (writes `src/analysis/crates/core/testdata/report_golden.parquet`):

```powershell
cargo test -p analysis_core --locked --test report_golden
```

The golden fixture is exercised by tests and deliberately does not contact live providers.
For real seeds, `run-dedup` first downloads the complete seed-contract NFT population
(up to 50,000 NFTs per contract), then deduplicates it against the input snapshot:

```powershell
cargo run -p analysis_cli --locked --release -- run-dedup `
  --input ./data/base.parquet `
  --input ./data/ethereum.parquet `
  --input ./data/polygon.parquet `
  --input ./data/solana.parquet `
  --seeds ./out/seeds/seeds.json `
  --output-dir ./out/dedup `
  --chains ethereum,base,polygon,solana `
  --evm-chains ethereum,base,polygon `
  --alchemy-api-key $env:ALCHEMY_API_KEY `
  --helius-api-key $env:HELIUS_API_KEY `
  --progress off
```

Name deduplication is disabled unless `--name-threshold VALUE` is supplied. URI and
Metadata deduplication still run normally.

Each completed seed snapshot is written to
`intermediate/seed_nfts/<chain>__<digest>.jsonl.zst` and atomically published. A valid
completed cache is reused automatically; use `--refresh-seed-nfts` to fetch it again or
`--seed-nft-cache-dir PATH` to relocate it. Alchemy pages EVM contracts and Helius DAS
pages Solana collections. Parquet scanning/index construction overlaps these downloads;
decoded rows stay resident for faster identity and metadata overlays, while the compressed
cache remains the durable reusable copy. The decoded batches are cleared immediately after
their final overlay consumer, before Name and Metadata indexes are queried.

Metadata has no anchor-count limit: every valid NFT metadata document is retained. Token
alignment, exact-match fast paths, candidate selection, and BM25 decision rules apply to
all retained documents. A provider population above 50,000 is explicitly marked capped and
the first 50,000 records are used.

Writes under `--output-dir` in three roots:

```text
intermediate/          # run_manifest.json, failures.jsonl, derived + raw API success caches
detail/seeds/…         # per-seed report.json|.md
summary/
  intra_chain/<chain>.*                 # Per-chain results
  intra_chain.*                         # Intra-chain aggregate
  chain_pairs/<primary>_to_<secondary>.* # Directed chain-pair results
  chain_matrix.*                        # Directed chain matrix
  cross_chain_by_source/<primary>.*     # Cross-chain aggregate by source
  cross_chain.*                         # Cross-chain aggregate
  all_chains.*                          # Overall aggregate + batch metrics
```

## Full `run`

End-to-end: load → dedup all seeds → enrich unique candidates → deep analysis → reports.
The example explicitly enables Name deduplication with `--name-threshold 0.98`; omit
that flag to skip both the Name index build and Name duplicate queries.

Every fully successful non-price provider JSON response is durably cached under
`intermediate/api_success_cache/v2/<provider>/<sha-prefix>/` as zstd-compressed,
SHA-256-addressed entries. Cache identities exclude API secrets and are independent
of the derived evidence-cache version. Exact method + URI + POST-body matches reuse
the successful response, including OpenSea REST,
collection, holder, asset, and market requests. Current-price requests are deliberately
excluded from the durable cache and are fetched again on each run (while identical
price requests are still coalesced within one run). Failed responses are retried.
Pass `--refresh-api-cache` to ignore responses predating the current run; each exact
request is then refreshed once and reused for the rest of that run.
Candidate controller and Solana collection-identity probes are additionally
checkpointed by stable chain/address under
`intermediate/candidate_identity_cache.json`, so changing HTTP batch boundaries
does not invalidate successful identity work. Transient failures are not reused.

```powershell
cargo run -p analysis_cli --locked --release -- run `
  --input ./data/base.parquet `
  --input ./data/ethereum.parquet `
  --input ./data/polygon.parquet `
  --input ./data/solana.parquet `
  --seeds ./out/seeds/seeds.json `
  --output-dir ./out/run `
  --chains base,ethereum,polygon,solana `
  --evm-chains base,ethereum,polygon `
  --name-threshold 0.98 `
  --metadata-threshold 0.6 `
  --alchemy-api-key $env:ALCHEMY_API_KEY `
  --etherscan-api-key $env:ETHERSCAN_API_KEY `
  --helius-api-key $env:HELIUS_API_KEY `
  --opensea-api-key $env:OPENSEA_API_KEY `
  --rayon-threads 128 `
  --http-concurrency 32 `
  --progress auto
```

Alchemy is required for an uncached EVM seed and Helius is required for an uncached
Solana seed. Once the compressed seed cache exists those keys are not required for the
seed-download stage. Other missing provider keys mark dependent evidence `not_requested`
and the run continues. Ethereum/Polygon sales use Alchemy `getNFTSales` with OpenSea
fallback, Base sales use OpenSea, and Solana sales are decoded from Helius histories.
Cancel / OOM paths do **not** write
`status: complete` into
`intermediate/run_manifest.json`. Incomplete four-scope seeds are excluded from formal
summary denominators. Cross-chain economics in `summary/all_chains.json` sum **USD only**.

### Dedup cache (skip re-query)

After URI/Name/Metadata queries finish, `run` always writes a portable checkpoint:

```text
<output-dir>/intermediate/dedup_cache.json
```

(override with `--dedup-cache PATH`). Edges are stored with stable chain/address/token
identities (not process-local ids). The ordered compressed seed-cache fingerprint is part
of compatibility validation, so replacing or refreshing a seed snapshot invalidates
derived dedup results.

### Evidence cache (skip re-enrich / resume after interrupt)

While enrich runs, network results are checkpointed **in batches** (default every 16
candidates) as independently replaceable compressed shards:

```text
<output-dir>/intermediate/evidence_cache.meta.json  # version + params
<output-dir>/intermediate/evidence_cache.entries/   # SHA-256-sharded candidate *.json.zst
```

(override base path with `--evidence-cache PATH`). Bundles use stable chain/address;
`contract_id` is remapped on load.

On the next `run` with the same output dir / params, the cache is **auto-resumed**:
already-cached candidates skip HTTP and only missing ones are fetched. A missing,
damaged, or incompatible cache automatically falls through to API requests.
Pagination limits must match the cache. Matching candidate contracts are reused;
new seed relations, stale prices, and retryable failed fields are selectively
refreshed when their dependencies permit an equivalent partial update.

### Rerunning with caches

Repeat the full command with the same input order, seeds, parameters, and output
directory. Compatible dedup caches still load Parquet identity for candidate
expansion, but skip matching indexes and queries. Changes to inputs, chains,
thresholds, retention settings, seed snapshots, or seeds invalidate the checkpoint.

### Evidence depth (enrich → economics)

- **Sales:** Ethereum/Polygon use paginated Alchemy `getNFTSales` as the primary
  source and fall back to OpenSea only when Alchemy is unavailable or fails.
  Base uses OpenSea collection Sale events filtered back to the candidate
  contract. Solana uses Helius asset histories plus decoded `getTransaction`
  buyer/seller/payment evidence. The cache version is producer metadata rather
  than an expiry switch: successfully collected historical evidence remains
  reusable across upgrades, while failed or missing evidence is retried.
- **EVM gas:** Alchemy/ETH `eth_getTransactionReceipt` → `TransferEvent.gas_native` /
  `fee_payer`; `quality.gas` Complete/Truncated/Empty/Failed/NotRequested.
- **EVM value flows:** native EXTERNAL transfers for positively identified
  operator seeds (candidate/controller/deployer, repeated seller, or star
  distributor), restricted to the candidate NFT activity interval plus a
  50,000-block setup/cashout margin → `ValueFlowEdge` (Funding / Withdrawal /
  RevenueBackflow). Ordinary non-victim participants are not operator seeds.
  Page-capped histories are `Truncated` and excluded from formal summaries.
- **USD policy:** report amounts use execution-day Alchemy spot prices. Payment
  token addresses are preferred over symbols; unpriced or peg-assumed amounts
  remain in detail quality metadata but do not enter formal summaries.
- **Exposure semantics:** buyer payments are reported as `paid_exposure`, not
  realized loss. A paid mint/secondary buyer is a victim only when the current,
  complete holder snapshot proves that the same address still holds the same
  purchased NFT; incomplete holder evidence never establishes victim status.
- **Shared candidates:** candidate-wide funding/withdrawal amounts appear in
  every related per-seed report; all run summaries re-union formal relations and
  count each candidate once.
- **Solana decode:** compressed NFTs use Helius `getSignaturesForAsset`, ordinary NFTs use
  `getSignaturesForAddress`; deduped `getTransaction` jsonParsed then fills
  from/to/timestamp/fee and SOL value-flow edges. A fully resolved asset with no
  collection group is a complete single-NFT analysis unit; grouped/partial direct
  recovery and signature-only stubs stay Truncated.
- **Request reuse:** preflight controller/slug/asset results flow into deep enrich. Prices,
  EVM receipts, EVM external transfers, and Solana transactions use run-scoped
  singleflight caches. Helius asset histories use 10-call history batches and
  `getTransaction` uses batches of up to 100; failed/incomplete cache entries remain
  retryable.
- **Known limitations:** Bubblegum/compressed NFT mint completeness (no token balances → Truncated);
  Cashout classification is coarse (often Withdrawal).
- **Economics:** when `quality.gas` is Complete or Truncated, every observed
  operator-paid receipt cost remains usable; missing receipts keep formal
  completeness truncated. Withdrawal/Cashout edges with known `gas_native`
  contribute Exit. Reports expose both the observed output/input ratio and a
  separate ratio restricted to candidates with complete required evidence.
- **Summary denominators:** stuck-NFT prevalence uses all NFTs in each hit
  contract from the resident input snapshot. Behavior prevalence uses only
  candidates with complete required behavior evidence; incomplete non-detections
  are not rendered as zero. Disabled dedup dimensions are rendered as `n/a`.

Additional outputs vs `run-dedup`:

- `detail/candidates/<chain>__<address>.json` (streamed as each candidate finishes analysis)
- Seed reports under `detail/seeds/` include `scopes_complete`, `analysis_complete`, and `analysis` rollups
- `summary/all_chains.*` adds candidate / address / behavior / economics / data_quality / `duplicate_scale`

## CLI

```text
analysis select-seeds ...   # seed ranking
analysis run-dedup ...      # seed snapshot download/cache + dedup hit reports
analysis run ...            # full enrich + analysis + reports
```

## Tests

```powershell
cargo test -p analysis_core -p analysis_cli --locked
cargo clippy -p analysis_core -p analysis_cli --all-targets --locked -- -D warnings
```
