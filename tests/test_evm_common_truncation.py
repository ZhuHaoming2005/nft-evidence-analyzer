import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path


def _load_evm_common():
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extensions = types.SimpleNamespace(connection=object)
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.execute_values = lambda *args, **kwargs: None

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *args, **kwargs: None

    aiohttp = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            pass

    class _ClientTimeout:
        def __init__(self, *args, **kwargs):
            pass

    aiohttp.ClientSession = _ClientSession
    aiohttp.TCPConnector = _TCPConnector
    aiohttp.ClientTimeout = _ClientTimeout

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

    injected = {
        "psycopg2": psycopg2,
        "psycopg2.extras": psycopg2.extras,
        "dotenv": dotenv,
        "aiohttp": aiohttp,
        "web3": web3,
        "web3.providers": providers,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "src" / "fetch" / "evm" / "common.py"
        spec = importlib.util.spec_from_file_location("evm_common_truncation_under_test", path)
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


class _RecordingCursor:
    def __init__(self, fetchall_results=None):
        self.fetchall_results = list(fetchall_results or [])
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        if self.fetchall_results:
            return list(self.fetchall_results.pop(0))
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingConn:
    def __init__(self, fetchall_results=None):
        self.cursor_obj = _RecordingCursor(fetchall_results)
        self.commit_count = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1


class EvmCommonTruncationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_evm_common()

    def test_batch_insert_main_truncates_strings_to_db_varchar_limits(self):
        conn = _RecordingConn(
            fetchall_results=[
                [("name", 200), ("symbol", 20), ("token_standard", 10)],
            ]
        )
        calls = []

        def _fake_execute_values(cur, sql, page, template=None, page_size=None):
            calls.append(list(page))
            cur.rowcount = len(page)

        original = self.common.execute_values
        self.common.execute_values = _fake_execute_values
        try:
            inserted = self.common.batch_insert_main(
                conn,
                "ethereum",
                [
                    (
                        "0xabc",
                        "1",
                        "ipfs://token/1",
                        "ipfs://image/1",
                        "N" * 250,
                        "S" * 25,
                        {"name": "NFT #1"},
                        "ERC-721-TOO-LONG",
                        123,
                    )
                ],
            )
        finally:
            self.common.execute_values = original

        self.assertEqual(inserted, 1)
        page = calls[0]
        self.assertEqual(page[0][4], "N" * 200)
        self.assertEqual(page[0][5], "S" * 20)
        self.assertEqual(page[0][7], "ERC-721-TO")

    def test_batch_insert_main_removes_null_bytes_from_nested_metadata_strings(self):
        conn = _RecordingConn(
            fetchall_results=[
                [("name", 200), ("symbol", 20), ("token_standard", 10)],
            ]
        )
        calls = []

        def _fake_execute_values(cur, sql, page, template=None, page_size=None):
            calls.append(list(page))
            cur.rowcount = len(page)

        original = self.common.execute_values
        self.common.execute_values = _fake_execute_values
        try:
            inserted = self.common.batch_insert_main(
                conn,
                "ethereum",
                [
                    (
                        "0xabc",
                        "1",
                        "ipfs://token/1",
                        "ipfs://image/1",
                        "NFT\x00 Name",
                        "SYM\x00",
                        {
                            "name": "ABU\x00NDANCE",
                            "description": "t{zg%pzeuq\x00tail",
                            "attributes": [
                                {"trait_type": "rarity\x00", "value": "legendary\x00"},
                            ],
                        },
                        "ERC-721",
                        123,
                    )
                ],
            )
        finally:
            self.common.execute_values = original

        self.assertEqual(inserted, 1)
        page = calls[0]
        self.assertEqual(page[0][4], "NFT Name")
        self.assertEqual(page[0][5], "SYM")
        metadata = json.loads(page[0][6])
        self.assertEqual(metadata["name"], "ABUNDANCE")
        self.assertEqual(metadata["description"], "t{zg%pzeuqtail")
        self.assertEqual(metadata["attributes"][0]["trait_type"], "rarity")
        self.assertEqual(metadata["attributes"][0]["value"], "legendary")
        self.assertNotIn("\\u0000", page[0][6])


if __name__ == "__main__":
    unittest.main()
