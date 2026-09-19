# PostgreSQL snapshot export

`nft-snapshot-export` streams one `nft_assets_<chain>` table to a ZSTD-compressed
Parquet file for `dedup` and `analysis`. Supported chains are Ethereum, Base,
Polygon, and Solana. Commands below run from the repository root.

## Schema

All columns are non-null UTF-8 strings; unavailable values can be empty:

```text
chain, contract_address, token_id,
token_uri, image_uri, name, symbol, metadata_json,
token_uri_norm, image_uri_norm, name_norm
```

EVM addresses are lowercased; Solana addresses retain case. URI normalization
unifies IPFS and Arweave gateway forms. Name normalization applies Unicode NFKC,
removes trailing token numbers, collapses whitespace, and lowercases text.

## Export

Set `DATABASE_URL`, or the `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`,
and `DB_CONNECT_TIMEOUT` environment variables. The exporter does not load `.env`.

```powershell
$env:DATABASE_URL = "postgresql://user:password@localhost:5432/nft_data"
foreach ($chain in @("ethereum", "base", "polygon", "solana")) {
  cargo run -p nft-snapshot-export --locked --release -- `
    --chain $chain --output "output/$chain.parquet" --fetch-size 100000
}
```

Each file uses a `REPEATABLE READ READ ONLY` transaction. Exports from separate
chains are individually consistent; they are not a synchronized cross-chain snapshot.
EVM exports can restrict the inclusive first-seen block interval:

```powershell
cargo run -p nft-snapshot-export --locked --release -- `
  --chain base --output output/base-subset.parquet `
  --start-block 1000000 --end-block 2000000
```

Solana rejects block bounds. Existing outputs are refused unless `--force` is
specified; replacement occurs only after the new snapshot is fully written.

## Validation

```powershell
cargo test -p nft-snapshot-export --locked
cargo clippy -p nft-snapshot-export --all-targets --locked -- -D warnings
```

Offline tests cover normalization, query bounds, Parquet schema, and output
replacement. The PostgreSQL regression test is ignored by default. Run it against
a disposable test database to verify metadata-column lookup through `search_path`,
column preference, and dropped-column handling:

```powershell
$env:NFT_EXPORT_TEST_DATABASE_URL = "postgresql://user:password@localhost:5432/test_db"
cargo test -p nft-snapshot-export --locked metadata_column_follows_search_path -- --ignored
```

The test creates a temporary table and rolls back its transaction.
