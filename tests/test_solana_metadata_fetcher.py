import asyncio
import importlib.util
import sys
import time
import types
import unittest
from pathlib import Path


def _load_fetcher(fake_common):
    aiohttp = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            pass

    aiohttp.ClientSession = _ClientSession
    aiohttp.TCPConnector = _TCPConnector

    injected = {
        "aiohttp": aiohttp,
        "common": fake_common,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "src" / "fetch" / "solana" / "metadata_fetcher.py"
        spec = importlib.util.spec_from_file_location("solana_metadata_fetcher_under_test", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        for name, value in original.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value


class SolanaMetadataFetcherFormatTests(unittest.IsolatedAsyncioTestCase):
    def _fake_common(self, fetch_metadata_batch=None):
        fake_common = types.ModuleType("common")
        fake_common.CHAIN_NAME = "solana"
        fake_common.HELIUS_BATCH_SIZE = 1000
        fake_common.CONCURRENT_HELIUS = 1
        fake_common.FETCH_IDLE_WAIT = 30
        fake_common.FETCH_CLAIM_BATCH_SIZE = 5000
        fake_common.CLAIM_RETRY_AFTER_SECONDS = 1800
        fake_common.REQUEST_STARTUP_STAGGER_SECONDS = 0
        fake_common.logger = types.SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
            exception=lambda *args, **kwargs: None,
        )
        fake_common.get_conn = lambda: None
        fake_common.init_db = lambda *args, **kwargs: None
        fake_common.load_pending_nfts = lambda *args, **kwargs: []
        fake_common.claim_pending_nfts = lambda *args, **kwargs: []
        fake_common.release_temp_claims = lambda *args, **kwargs: 0
        fake_common.batch_insert_main = lambda *args, **kwargs: 0
        fake_common.delete_temp_nfts = lambda *args, **kwargs: 0
        fake_common.fetch_metadata_batch = fetch_metadata_batch or (
            lambda *args, **kwargs: []
        )
        return fake_common

    async def test_process_batch_builds_evm_compatible_insert_tuple_with_metadata(self):
        metadata = {"name": "Asset #1", "image": "https://image.example/1.png"}

        async def _fetch_metadata_batch(_session, _helius_sem, mints):
            self.assertEqual(mints, ["Mint111"])
            return [
                (
                    "Collection1111111111111111111111111111111111",
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "Asset #1",
                    "AST",
                    metadata,
                )
            ]

        fake_common = self._fake_common(_fetch_metadata_batch)

        fetcher = _load_fetcher(fake_common)

        inserts, ids = await fetcher._process_batch(
            object(),
            asyncio.Semaphore(1),
            [(7, "Mint111", "Metaplex", 123456)],
        )

        self.assertEqual(ids, [7])
        self.assertEqual(
            inserts,
            [
                (
                    "Collection1111111111111111111111111111111111",
                    "Mint111",
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "Asset #1",
                    "AST",
                    metadata,
                    "Metaplex",
                    123456,
                )
            ],
        )

    async def test_process_batch_uses_mint_as_singleton_collection_when_collection_missing(self):
        metadata = {"name": "Ungrouped #1"}

        async def _fetch_metadata_batch(_session, _helius_sem, mints):
            self.assertEqual(mints, ["Mint111"])
            return [
                (
                    None,
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "Ungrouped #1",
                    "UNG",
                    metadata,
                )
            ]

        fake_common = self._fake_common(_fetch_metadata_batch)

        fetcher = _load_fetcher(fake_common)

        inserts, ids = await fetcher._process_batch(
            object(),
            asyncio.Semaphore(1),
            [(7, "Mint111", "Metaplex", 123456)],
        )

        self.assertEqual(ids, [7])
        self.assertEqual(
            inserts,
            [
                (
                    "Mint111",
                    "Mint111",
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "Ungrouped #1",
                    "UNG",
                    metadata,
                    "Metaplex",
                    123456,
                )
            ],
        )

    async def test_do_write_does_not_delete_temp_rows_when_main_insert_fails(self):
        fake_common = self._fake_common()
        fetcher = _load_fetcher(fake_common)
        events = []

        def _insert(*_args, **_kwargs):
            events.append("insert")
            time.sleep(0.05)
            raise RuntimeError("insert failed")

        def _delete(*_args, **_kwargs):
            events.append("delete")
            return 1

        fetcher.batch_insert_main = _insert
        fetcher.delete_temp_nfts = _delete

        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            await fetcher._do_write(object(), object(), [("row",)], [7])

        self.assertEqual(events, ["insert"])


if __name__ == "__main__":
    unittest.main()
