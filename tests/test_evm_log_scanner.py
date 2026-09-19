import asyncio
import importlib.util
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import patch


class _AwaitableValue:
    def __init__(self, value):
        self._value = value

    def __await__(self):
        async def _coro():
            return self._value

        return _coro().__await__()


def _load_scanner():
    aiohttp = types.ModuleType("aiohttp")

    class _ClientSession:
        def __init__(self, *args, **kwargs):
            pass

        async def close(self):
            return None

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            self.closed = False

        def close(self):
            self.closed = True

    aiohttp.ClientSession = _ClientSession
    aiohttp.TCPConnector = _TCPConnector

    web3 = types.ModuleType("web3")

    class _AsyncWeb3:
        def __init__(self, *args, **kwargs):
            self.eth = types.SimpleNamespace(block_number=_AwaitableValue(12))

        async def is_connected(self):
            return True

    web3.AsyncWeb3 = _AsyncWeb3

    providers = types.ModuleType("web3.providers")

    class _AsyncHTTPProvider:
        def __init__(self, *args, **kwargs):
            pass

    providers.AsyncHTTPProvider = _AsyncHTTPProvider

    common = types.ModuleType("common")
    common.CHAIN_NAME = "ethereum"
    common.RPC_URL = "https://rpc.example"
    common.START_BLOCK = 0
    common.END_BLOCK = 0
    common.BLOCK_BATCH_SIZE = 2
    common.SCAN_WINDOW = 2
    common.REQUEST_STARTUP_STAGGER_SECONDS = 0.0
    common.logger = types.SimpleNamespace(info=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)
    common.get_conn = lambda: None
    common.init_db = lambda conn, chain_name: None
    common.load_seen_nfts = lambda conn, chain_name: set()
    common.get_last_block = lambda conn, chain_name: None
    common.save_progress = lambda conn, chain_name, from_block: None
    common.batch_insert_temp = lambda conn, chain_name, records: len(records)
    common.load_blacklist = lambda: set()
    common.fetch_logs_http = None
    common.extract_nfts = lambda log: []

    injected = {
        "aiohttp": aiohttp,
        "web3": web3,
        "web3.providers": providers,
        "common": common,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "src" / "fetch" / "evm" / "log_scanner.py"
        spec = importlib.util.spec_from_file_location("evm_log_scanner_under_test", path)
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


scanner = _load_scanner()


class _RecordingConn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class LogScannerShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_main_does_not_enqueue_new_ranges_when_stop_already_requested(self):
        stop_event = threading.Event()
        stop_event.set()
        main_conn = _RecordingConn()
        write_conn = _RecordingConn()
        fetch_calls = []
        insert_calls = []

        async def _fake_fetch_logs_http(*args, **kwargs):
            fetch_calls.append((args, kwargs))
            return []

        with patch.object(scanner, "get_conn", side_effect=[main_conn, write_conn]), \
             patch.object(scanner, "fetch_logs_http", side_effect=_fake_fetch_logs_http), \
             patch.object(scanner, "batch_insert_temp", side_effect=lambda conn, chain, records: insert_calls.append(list(records)) or len(records)), \
             patch.object(scanner, "save_progress"):
            await scanner.main(stop_event=stop_event)

        self.assertEqual(fetch_calls, [])
        self.assertEqual(insert_calls, [])
        self.assertTrue(main_conn.closed)
        self.assertTrue(write_conn.closed)
