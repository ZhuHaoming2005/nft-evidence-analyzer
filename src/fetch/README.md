# NFT collection

Install the root `requirements.txt`, copy `.env.example` to `.env`, and fill in
provider credentials and PostgreSQL settings. Run from the repository root.
Collection writes to `temp_<chain>` and `nft_assets_<chain>` and persists scan progress.

## EVM

Ethereum, Base, and Polygon share `src/fetch/evm/`. Set `CHAIN_NAME`, `RPC_URL`, and
`ALCHEMY_NETWORK` to the same chain. Run the scanner and metadata consumer in
separate terminals:

```powershell
python src/fetch/evm/log_scanner.py
python src/fetch/evm/metadata_fetcher.py
```

The scanner walks backward from `END_BLOCK` (zero selects the current tip) to
`START_BLOCK` and resumes saved progress. Metadata workers claim staging rows,
fetch Alchemy metadata, and write accepted records before removing processed rows.
Failed batches release claims for retry. Worker count and claim timeout are
configured in `.env.example`.

Retry missing images from stored token URIs with:

```powershell
python src/fetch/evm/retry.py --chain base
```

## Solana

Merge settings from [solana/.env.example](solana/.env.example) into the root `.env`.
Set `CHAIN_NAME=solana` and a Helius key, then run in separate terminals:

```powershell
python src/fetch/solana/tx_scanner.py
python src/fetch/solana/metadata_fetcher.py
```

Discovery enumerates Metaplex MetadataV1 accounts through `getProgramAccountsV2`;
it is not a historical transaction scan or a census of every Solana asset format.
Pagination cursors and incremental slots are checkpointed. Metadata comes from
Helius DAS `getAssetBatch`. Ungrouped assets use their mint as a singleton collection.

## Collection boundaries

Missing or invalid token URIs are excluded. Inline `data:image` media trigger
collection filters: EVM contracts are blacklisted and removed from both database
tables, while Solana mints are skipped. These are dataset selection rules, not
proof that an NFT is malicious. Account for them when reporting coverage.

```powershell
python -m unittest discover -s tests -v
```

Tests use mocked APIs and databases; they do not verify live provider availability.
