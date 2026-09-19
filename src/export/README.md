# Snapshot export

Stream `nft_assets_<chain>` into ZSTD-compressed Parquet for matching and analysis.
Run from the repository root; the exporter reads process environment variables,
not `.env`.

## Recommended command

Export the four chains after collection:

```powershell
$env:DATABASE_URL = "postgresql://user:password@localhost:5432/nft_data"
foreach ($chain in @('ethereum', 'base', 'polygon', 'solana')) {
  cargo run -p nft-snapshot-export --locked --release -- `
    --chain $chain --output "output/$chain.parquet" --fetch-size 100000
  if ($LASTEXITCODE -ne 0) { throw "Export failed for $chain" }
}
```

Alternatively configure `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`, and
`DB_CONNECT_TIMEOUT`. Supported chains are `ethereum`, `base`, `polygon`, and `solana`.
EVM exports accept inclusive `--start-block` and `--end-block`; Solana rejects them.
Use a file path for `--output`; `--force` permits replacing an existing output file.

Each export uses a `REPEATABLE READ READ ONLY` transaction. Output columns are
non-null UTF-8 strings; unavailable values may be empty:

```text
chain, contract_address, token_id, token_uri, image_uri, name, symbol,
metadata_json, token_uri_norm, image_uri_norm, name_norm
```

EVM addresses are lowercased; Solana addresses retain case. Normalization unifies
IPFS/Arweave gateway forms and applies Unicode normalization, trailing token-number
removal, whitespace collapse, and lowercase conversion to names.
