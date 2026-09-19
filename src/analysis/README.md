# NFT evidence analysis

Select seed collections, match candidates, collect provider evidence, and generate
behavior and economic reports. Run the recommended commands below from the
repository root, after exporting snapshots to `output/` and setting provider keys
in the process environment. The CLI does not load `.env`.

## Recommended commands

Select seeds:

```powershell
cargo run -p analysis_cli --locked --release -- select-seeds `
  --output-dir out/seeds --chains ethereum,base,polygon,solana `
  --seeds-per-chain 25 `
  --opensea-api-key $env:OPENSEA_API_KEY --nftscan-api-key $env:NFTSCAN_API_KEY
```

Prepare shared matching arguments, then run matching only or full analysis:

```powershell
$matchingArgs = @(
  '--input', 'output/ethereum.parquet', '--input', 'output/base.parquet',
  '--input', 'output/polygon.parquet', '--input', 'output/solana.parquet',
  '--seeds', 'out/seeds/seeds.json',
  '--chains', 'ethereum,base,polygon,solana',
  '--evm-chains', 'ethereum,base,polygon',
  '--name-threshold', '0.98', '--metadata-threshold', '0.6',
  '--alchemy-api-key', $env:ALCHEMY_API_KEY,
  '--helius-api-key', $env:HELIUS_API_KEY
)
cargo run -p analysis_cli --locked --release -- run-dedup @matchingArgs --output-dir out/matches
cargo run -p analysis_cli --locked --release -- run @matchingArgs `
  --output-dir out/analysis `
  --etherscan-api-key $env:ETHERSCAN_API_KEY --opensea-api-key $env:OPENSEA_API_KEY
```

The two commands are alternatives; `run` includes matching. Omit any optional
provider flag whose key is unset. Omit `--name-threshold` to disable Name matching.

## Behavior and outputs

| Command | Purpose |
|---|---|
| `select-seeds` | Rank EVM collections through OpenSea and Solana collections through NFTScan |
| `run-dedup` | Load snapshots, download seed populations, and report candidate matches |
| `run` | Match, enrich candidates, analyze evidence, and write full reports |

`run` and `run-dedup` require `--input`, `--seeds`, `--output-dir`, `--chains`, and
`--evm-chains`. Seeds are JSON arrays with `chain` and `address`; `rank` is optional.
Name matching requires a fractional `--name-threshold`, such as `0.98`.
Metadata retains every valid document; seed downloads are capped at 50,000 NFTs
per contract and capped populations are marked explicitly.

Pass provider keys through CLI flags. Uncached EVM seeds require Alchemy and
uncached Solana seeds require Helius. Other missing keys leave dependent evidence
`not_requested`. Compatible caches resume automatically; `--refresh-seed-nfts`
and `--refresh-api-cache` refresh the respective inputs. Prices are fetched again
on each run. Tune `--rayon-threads` and `--http-concurrency` for host/provider limits.

Outputs use `intermediate/` for caches and manifests, `detail/` for seed/candidate
reports, and `summary/` for intra-chain, directed chain-pair, cross-chain, and overall
results. Incomplete seeds are excluded from formal matching denominators. Evidence
quality controls behavior/economic summaries; incomplete non-detections are not zero.
Buyer paid exposure is not realized loss; cross-chain economic totals sum priced USD
only. Candidate amounts may repeat in seed details but are counted once in run summaries.
