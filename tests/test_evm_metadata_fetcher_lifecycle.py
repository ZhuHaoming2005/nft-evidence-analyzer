import asyncio
import importlib.util
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import patch


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

    common = types.ModuleType("common")
    common.CHAIN_NAME = "ethereum"
    common.RPC_URL = "https://rpc.example"
    common.ALCHEMY_BATCH_SIZE = 2
    common.CONCURRENT_ALCHEMY = 2
    common.CONCURRENT_RPC = 2
    common.RPC_BATCH_SIZE = 2
    common.FETCH_IDLE_WAIT = 30
    common.FETCH_CLAIM_BATCH_SIZE = 100
    common.CLAIM_RETRY_AFTER_SECONDS = 600
    common.REQUEST_STARTUP_STAGGER_SECONDS = 0.0
    common.logger = types.SimpleNamespace(info=lambda *args, **kwargs: None, exception=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)
    common.get_conn = lambda: None
    common.init_db = lambda conn, chain_name: None
    common.claim_pending_nfts = lambda *args, **kwargs: []
    common.batch_insert_main = lambda *args, **kwargs: 0
    common.delete_temp_nfts = lambda *args, **kwargs: 0
    common.release_temp_claims = lambda *args, **kwargs: 0
    common.delete_contract_nfts = lambda *args, **kwargs: (0, 0)
    common.append_blacklist_env = lambda *args, **kwargs: None
    common.load_blacklist = lambda *args, **kwargs: set()
    common.fetch_alchemy_batch = None
    common.fetch_token_uri_batch = None
    common.fetch_token_uri = None
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
        spec = importlib.util.spec_from_file_location("evm_metadata_fetcher_lifecycle_under_test", path)
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


class MetadataFetcherBlacklistCleanupTests(unittest.TestCase):
    def test_cleanup_blacklisted_pending_deletes_claimed_ids_before_contract_sweep(self):
        pending = [
            (5, "0xblack", 1, "ERC-721", 100),
            (2, "0xkeep", 2, "ERC-721", 101),
            (4, "0xblack", 3, "ERC-721", 102),
        ]

        calls = []

        def _record_delete_temp(_conn, _chain, ids):
            calls.append(("delete_temp_nfts", list(ids)))
            return len(ids)

        def _record_delete_contract(_conn, _chain, contracts, skip_temp_ids=None):
            calls.append(("delete_contract_nfts", set(contracts), list(skip_temp_ids or [])))
            return 7, 3

        with patch.object(fetcher, "delete_temp_nfts", side_effect=_record_delete_temp), \
             patch.object(fetcher, "delete_contract_nfts", side_effect=_record_delete_contract):
            remaining_ids, blacklisted_ids, main_deleted, temp_deleted = fetcher._cleanup_blacklisted_pending(
                object(),
                pending,
                [row[0] for row in pending],
                {"0xblack"},
                {"0xblack"},
            )

        self.assertEqual(remaining_ids, [2])
        self.assertEqual(blacklisted_ids, [4, 5])
        self.assertEqual(main_deleted, 7)
        self.assertEqual(temp_deleted, 3)
        self.assertEqual(
            calls,
            [
                ("delete_temp_nfts", [4, 5]),
                ("delete_contract_nfts", {"0xblack"}, [4, 5]),
            ],
        )

    def test_cleanup_blacklisted_pending_skips_contract_sweep_when_nothing_new_was_blacklisted(self):
        pending = [
            (1, "0xknown", 1, "ERC-721", 100),
            (2, "0xkeep", 2, "ERC-721", 101),
        ]

        with patch.object(fetcher, "delete_temp_nfts", return_value=1) as delete_temp_mock, \
             patch.object(fetcher, "delete_contract_nfts") as delete_contract_mock:
            remaining_ids, blacklisted_ids, main_deleted, temp_deleted = fetcher._cleanup_blacklisted_pending(
                object(),
                pending,
                [1, 2],
                {"0xknown"},
                set(),
            )

        self.assertEqual(remaining_ids, [2])
        self.assertEqual(blacklisted_ids, [1])
        self.assertEqual((main_deleted, temp_deleted), (0, 0))
        delete_temp_mock.assert_called_once_with(unittest.mock.ANY, "ethereum", [1])
        delete_contract_mock.assert_not_called()


class MetadataFetcherStartupJitterTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_startup_jitter_scales_with_worker_index(self):
        original_delay = getattr(fetcher, "WORKER_STARTUP_DELAY_SECONDS", 0.0)
        try:
            fetcher.WORKER_STARTUP_DELAY_SECONDS = 1.5
            with patch.object(fetcher.asyncio, "sleep") as sleep_mock:
                await fetcher._maybe_wait_worker_startup(3)
        finally:
            fetcher.WORKER_STARTUP_DELAY_SECONDS = original_delay

        sleep_mock.assert_awaited_once_with(3.0)

    async def test_worker_startup_jitter_skips_first_worker(self):
        original_delay = getattr(fetcher, "WORKER_STARTUP_DELAY_SECONDS", 0.0)
        try:
            fetcher.WORKER_STARTUP_DELAY_SECONDS = 1.5
            with patch.object(fetcher.asyncio, "sleep") as sleep_mock:
                await fetcher._maybe_wait_worker_startup(1)
        finally:
            fetcher.WORKER_STARTUP_DELAY_SECONDS = original_delay

        sleep_mock.assert_not_awaited()


class MetadataFetcherShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_releases_claimed_rows_when_interrupted_mid_batch(self):
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

        conn = _FakeConn()
        released = []
        pending = [(1, "0xabc", 1, "ERC-721", 100)]

        async def _boom(*args, **kwargs):
            raise KeyboardInterrupt()

        with patch.object(fetcher, "_maybe_wait_worker_startup"), \
             patch.object(fetcher, "get_conn", return_value=conn), \
             patch.object(fetcher, "fix_token_id_placeholders", return_value=0), \
             patch.object(fetcher, "claim_pending_nfts", side_effect=[pending]), \
             patch.object(fetcher.aiohttp, "ClientSession", return_value=_FakeSession()), \
             patch.object(fetcher, "_process_batch", side_effect=_boom), \
             patch.object(fetcher, "release_temp_claims", side_effect=lambda _conn, _chain, ids, worker_id: released.append((list(ids), worker_id)) or len(ids)):
            with self.assertRaises(KeyboardInterrupt):
                await fetcher._worker_main(1, stop_event=threading.Event())

        self.assertEqual(len(released), 1)
        self.assertEqual(released[0][0], [1])
        self.assertIn("worker-1", released[0][1])
        self.assertTrue(conn.closed)


if __name__ == "__main__":
    unittest.main()
