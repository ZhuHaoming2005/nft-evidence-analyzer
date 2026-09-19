import asyncio
import importlib.util
import sys
import threading
import types
import unittest
from pathlib import Path


def _load_scanner(fake_common):
    aiohttp = types.ModuleType("aiohttp")

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            pass

        async def close(self):
            pass

    class _ClientSession:
        def __init__(self, *args, **kwargs):
            pass

        async def close(self):
            pass

    aiohttp.TCPConnector = _TCPConnector
    aiohttp.ClientSession = _ClientSession

    injected = {
        "aiohttp": aiohttp,
        "common": fake_common,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "src" / "fetch" / "solana" / "tx_scanner.py"
        spec = importlib.util.spec_from_file_location("solana_tx_scanner_under_test", path)
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


class _FakeConn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class SolanaTxScannerProgressTests(unittest.IsolatedAsyncioTestCase):
    def _fake_common(self):
        fake_common = types.ModuleType("common")
        fake_common.CHAIN_NAME = "solana"
        fake_common.HELIUS_RPC_URL = "https://rpc.example"
        fake_common.GPA_PAGE_SIZE = 1000
        fake_common.GPA_SINCE_SLOT = 0
        fake_common.logger = types.SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
            exception=lambda *args, **kwargs: None,
        )
        fake_common.get_conn = lambda: _FakeConn()
        fake_common.init_db = lambda *args, **kwargs: None
        fake_common.batch_insert_temp = lambda *_args, **_kwargs: 0
        fake_common.get_gpa_progress = lambda *_args, **_kwargs: (None, 0, 0)
        fake_common.save_gpa_progress = lambda *args, **kwargs: None

        async def _fetch_gpa_page(*_args, **_kwargs):
            return [], None

        async def _get_latest_slot(*_args, **_kwargs):
            return 0

        fake_common.fetch_gpa_page = _fetch_gpa_page
        fake_common.get_latest_slot = _get_latest_slot
        return fake_common

    def test_env_slot_resume_uses_matching_saved_pagination_key(self):
        scanner = _load_scanner(self._fake_common())

        state = scanner._resolve_scan_state(
            env_since=123,
            saved_key="page-2",
            saved_since_slot=123,
            saved_pages=7,
        )

        self.assertEqual(state.since_slot, 123)
        self.assertEqual(state.resume_key, "page-2")
        self.assertEqual(state.total_pages_base, 7)

    def test_env_slot_starts_fresh_when_saved_key_belongs_to_other_slot(self):
        scanner = _load_scanner(self._fake_common())

        state = scanner._resolve_scan_state(
            env_since=456,
            saved_key="page-2",
            saved_since_slot=123,
            saved_pages=7,
        )

        self.assertEqual(state.since_slot, 456)
        self.assertIsNone(state.resume_key)
        self.assertEqual(state.total_pages_base, 0)

    def test_env_slot_does_not_override_newer_completed_db_progress(self):
        scanner = _load_scanner(self._fake_common())

        state = scanner._resolve_scan_state(
            env_since=123,
            saved_key=None,
            saved_since_slot=999,
            saved_pages=42,
        )

        self.assertEqual(state.since_slot, 999)
        self.assertIsNone(state.resume_key)
        self.assertEqual(state.total_pages_base, 0)

    def test_env_slot_does_not_override_newer_db_resume_key(self):
        scanner = _load_scanner(self._fake_common())

        state = scanner._resolve_scan_state(
            env_since=123,
            saved_key="page-9",
            saved_since_slot=999,
            saved_pages=42,
        )

        self.assertEqual(state.since_slot, 999)
        self.assertEqual(state.resume_key, "page-9")
        self.assertEqual(state.total_pages_base, 42)

    def test_progress_save_slot_preserves_incremental_slot_until_scan_completes(self):
        scanner = _load_scanner(self._fake_common())

        self.assertEqual(
            scanner._progress_since_slot(
                current_since_slot=123,
                latest_slot=999,
                next_key="page-3",
            ),
            123,
        )
        self.assertEqual(
            scanner._progress_since_slot(
                current_since_slot=123,
                latest_slot=999,
                next_key=None,
            ),
            999,
        )
        self.assertEqual(
            scanner._progress_since_slot(
                current_since_slot=123,
                latest_slot=0,
                next_key=None,
            ),
            123,
        )

    async def test_stop_event_finishes_current_page_and_saves_resume_key(self):
        fake_common = self._fake_common()
        fake_common.GPA_SINCE_SLOT = 123
        stop_event = threading.Event()
        fetch_calls = []
        saved = []

        async def _get_latest_slot(*_args, **_kwargs):
            return 999

        async def _fetch_gpa_page(_session, _url, _page_size, *, pagination_key=None, changed_since_slot=None):
            fetch_calls.append((pagination_key, changed_since_slot))
            return ["Mint111"], "page-2"

        def _batch_insert_temp(_conn, _chain, records):
            self.assertEqual(records, [("Mint111", "Metaplex", 0)])
            stop_event.set()
            return len(records)

        def _save_gpa_progress(_conn, chain_name, pagination_key, since_slot, total_pages):
            saved.append((chain_name, pagination_key, since_slot, total_pages))

        fake_common.get_latest_slot = _get_latest_slot
        fake_common.fetch_gpa_page = _fetch_gpa_page
        fake_common.batch_insert_temp = _batch_insert_temp
        fake_common.save_gpa_progress = _save_gpa_progress

        scanner = _load_scanner(fake_common)

        await scanner.main(stop_event=stop_event)

        self.assertEqual(fetch_calls[0], (None, 123))
        self.assertEqual(saved[-1], ("solana", "page-2", 123, 1))


if __name__ == "__main__":
    unittest.main()
