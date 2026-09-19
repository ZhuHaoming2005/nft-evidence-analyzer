# NFT collection

Collect NFT records and metadata into PostgreSQL staging and asset tables.
Configure the root `.env` before running commands from the repository root.

## Recommended commands

For Ethereum, Base, or Polygon, set `CHAIN_NAME`, `RPC_URL`, and `ALCHEMY_NETWORK`
to the same chain. Run these in separate terminals:

```powershell
python src/fetch/evm/log_scanner.py
python src/fetch/evm/metadata_fetcher.py
```

For Solana, merge `src/fetch/solana/.env.example` into `.env`, set the Helius key,
and run these in separate terminals:

```powershell
python src/fetch/solana/tx_scanner.py
python src/fetch/solana/metadata_fetcher.py
```

Scans resume saved progress. EVM discovery walks backward through configured block
bounds; Solana enumerates Metaplex MetadataV1 accounts, not every asset format.
Retry missing EVM images for the configured chain (Base in this example):

```powershell
python src/fetch/evm/retry.py --chain base
```

Invalid or missing token URIs are excluded. Inline `data:image` media cause EVM
contracts to be blacklisted and removed from both tables; Solana mints are skipped.
These are collection filters, not evidence of malicious behavior.
