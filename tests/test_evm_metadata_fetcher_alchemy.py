import asyncio
import importlib.util
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import patch


async def _unexpected_rpc_fallback(*args, **kwargs):
    raise AssertionError("metadata_fetcher should not call RPC tokenURI fallback")


def _load_fetcher():
    aiohttp = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            pass

    aiohttp.ClientSession = _ClientSession
    aiohttp.TCPConnector = _TCPConnector

    web3 = types.ModuleType("web3")

    class _AsyncWeb3:
        @staticmethod
        def to_checksum_address(value):
            return value

    web3.AsyncWeb3 = _AsyncWeb3

    providers = types.ModuleType("web3.providers")

    class _AsyncHTTPProvider:
        def __init__(self, *args, **kwargs):
            pass

    providers.AsyncHTTPProvider = _AsyncHTTPProvider

    async def _fetch_alchemy_batch(_session, _sem, _tokens, startup_delay_seconds=0.0):
        return [(None, "https://cdn.example/1.png", "Collection", "COL", {"name": "NFT 1"})]

    common = types.ModuleType("common")
    common.CHAIN_NAME = "polygon"
    common.RPC_URL = "https://rpc.example"
    common.ALCHEMY_BATCH_SIZE = 2
    common.CONCURRENT_ALCHEMY = 2
    common.CONCURRENT_RPC = 2
    common.RPC_BATCH_SIZE = 2
    common.FETCH_IDLE_WAIT = 30
    common.FETCH_CLAIM_BATCH_SIZE = 100
    common.CLAIM_RETRY_AFTER_SECONDS = 600
    common.REQUEST_STARTUP_STAGGER_SECONDS = 0.0
    common.logger = types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
    )
    common.get_conn = lambda: None
    common.init_db = lambda conn, chain_name: None
    common.claim_pending_nfts = lambda *args, **kwargs: []
    common.batch_insert_main = lambda *args, **kwargs: 0
    common.delete_temp_nfts = lambda *args, **kwargs: 0
    common.release_temp_claims = lambda *args, **kwargs: 0
    common.delete_contract_nfts = lambda *args, **kwargs: (0, 0)
    common.append_blacklist_env = lambda *args, **kwargs: None
    common.load_blacklist = lambda *args, **kwargs: set()
    common.fetch_alchemy_batch = _fetch_alchemy_batch
    common.fetch_token_uri_batch = _unexpected_rpc_fallback
    common.fetch_token_uri = _unexpected_rpc_fallback
    common._decode_inline_image = lambda *args, **kwargs: None
    common.replace_token_id_placeholder = lambda uri, tid: uri
    common.fix_token_id_placeholders = lambda conn, chain_name: 0

    injected = {
        "aiohttp": aiohttp,
        "web3": web3,
        "web3.providers": providers,
        "common": common,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "src" / "fetch" / "evm" / "metadata_fetcher.py"
        spec = importlib.util.spec_from_file_location("evm_metadata_fetcher_alchemy_under_test", path)
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


fetcher = _load_fetcher()


class _FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _BoomAsyncWeb3:
    def __init__(self, *args, **kwargs):
        raise AssertionError("metadata_fetcher should not require RPC connectivity")


class EvmMetadataFetcherAlchemyOnlyTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_batch_skips_empty_alchemy_uri_without_rpc_fallback(self):
        pending = [(1, "0xabc", 1, "ERC-721", 123)]

        inserts, all_ids, onchain_image_contracts = await fetcher._process_batch(
            object(),
            asyncio.Semaphore(1),
            pending,
        )

        self.assertEqual(inserts, [])
        self.assertEqual(all_ids, [1])
        self.assertEqual(onchain_image_contracts, set())

    async def test_worker_does_not_require_rpc_before_processing_alchemy_batches(self):
        stop_event = threading.Event()
        conn = _FakeConn()
        pending = [(1, "0xabc", 1, "ERC-721", 123)]
        processed = []

        async def _process_once(*args, **kwargs):
            processed.append(args)
            stop_event.set()
            return [], [1], set()

        with patch.object(fetcher, "_maybe_wait_worker_startup"), \
             patch.object(fetcher, "AsyncWeb3", _BoomAsyncWeb3, create=True), \
             patch.object(fetcher, "AsyncHTTPProvider", lambda *args, **kwargs: object(), create=True), \
             patch.object(fetcher, "get_conn", return_value=conn), \
             patch.object(fetcher, "fix_token_id_placeholders", return_value=0), \
             patch.object(fetcher, "claim_pending_nfts", return_value=pending), \
             patch.object(fetcher.aiohttp, "ClientSession", return_value=_FakeSession()), \
             patch.object(fetcher, "_process_batch", side_effect=_process_once), \
             patch.object(fetcher, "batch_insert_main", return_value=0), \
             patch.object(fetcher, "delete_temp_nfts", return_value=1):
            await fetcher._worker_main(1, stop_event=stop_event)

        self.assertEqual(len(processed), 1)
        self.assertTrue(conn.closed)


if __name__ == "__main__":
    unittest.main()
