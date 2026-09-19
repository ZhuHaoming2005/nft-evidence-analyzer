import asyncio
import importlib.util
import json
import sys
import threading
import time
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import request


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
        spec = importlib.util.spec_from_file_location("evm_common_batch_schema_under_test", path)
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


def _encode_abi_string(value: str) -> str:
    payload = value.encode("utf-8")
    padded = payload + (b"\x00" * ((32 - len(payload) % 32) % 32))
    encoded = (32).to_bytes(32, "big") + len(payload).to_bytes(32, "big") + padded
    return "0x" + encoded.hex()


class _BatchRpcHandler(BaseHTTPRequestHandler):
    request_delay = 0.2
    request_count = 0

    def do_POST(self):
        type(self).request_count += 1
        time.sleep(self.request_delay)

        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        payload = json.loads(body)

        responses = []
        for item in payload:
            token_id = int(item["params"][0]["data"][-64:], 16)
            if item["params"][0]["data"].startswith("0xc87b56dd"):
                uri = f"https://erc721.example/{token_id}"
            else:
                uri = f"https://erc1155.example/{token_id}"
            responses.append(
                {"jsonrpc": "2.0", "id": item["id"], "result": _encode_abi_string(uri)}
            )

        encoded = json.dumps(responses).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):
        return


class _FakeHttpResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self, content_type=None):
        return self._payload


class _FakePostContext:
    def __init__(self, url: str, payload):
        self.url = url
        self.payload = payload
        self.response = None

    async def __aenter__(self):
        def _send():
            body = json.dumps(self.payload).encode("utf-8")
            req = request.Request(
                self.url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=5) as resp:
                return _FakeHttpResponse(resp.status, json.loads(resp.read()))

        self.response = await asyncio.to_thread(_send)
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeClientSession:
    def post(self, url, json=None, timeout=None):
        return _FakePostContext(url, json)


class _FakeAlchemyPostContext:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return _FakeHttpResponse(200, self.payload)

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeAlchemySession:
    def __init__(self, payload):
        self.payload = payload

    def post(self, url, json=None, timeout=None):
        return _FakeAlchemyPostContext(self.payload)


class _RecordingCursor:
    def __init__(self, fetchall_result=None):
        self.fetchall_result = fetchall_result or []
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, params_seq):
        self.executed.append((sql, list(params_seq)))

    def fetchall(self):
        return list(self.fetchall_result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingConn:
    def __init__(self, fetchall_result=None):
        self.cursor_obj = _RecordingCursor(fetchall_result)
        self.commit_count = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        pass


class _UndefinedColumnCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 0
        self._failed = False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "WITH candidates AS" in sql and not self._failed:
            self._failed = True
            exc = RuntimeError('column "claimed_at" does not exist')
            exc.pgcode = "42703"
            raise exc

    def fetchall(self):
        return [(11, "0xdef", 2, "ERC-1155", 456)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _UndefinedColumnConn:
    def __init__(self):
        self.cursor_obj = _UndefinedColumnCursor()
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


class EvmCommonBatchRpcTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_evm_common()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _BatchRpcHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.rpc_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=2)

    async def test_fetch_token_uri_batch_uses_single_batch_request(self):
        _BatchRpcHandler.request_count = 0
        tokens = [
            ("0xabc", 1, "ERC-721"),
            ("0xdef", 2, "ERC-1155"),
            ("0xghi", 3, "ERC-721"),
        ]

        session = _FakeClientSession()
        start = time.perf_counter()
        result = await self.common.fetch_token_uri_batch(
            session,
            self.rpc_url,
            asyncio.Semaphore(4),
            tokens,
        )
        elapsed = time.perf_counter() - start

        self.assertEqual(
            result,
            [
                "https://erc721.example/1",
                "https://erc1155.example/2",
                "https://erc721.example/3",
            ],
        )
        self.assertEqual(_BatchRpcHandler.request_count, 1)
        self.assertLess(elapsed, 0.5)

    async def test_fetch_alchemy_batch_returns_contract_fields_and_raw_metadata(self):
        payload = [
            {
                "contract": {"name": "Collection A", "symbol": "COLA"},
                "raw": {
                    "tokenUri": "ipfs://token/1",
                    "metadata": {"name": "NFT #1", "attributes": [{"trait_type": "bg", "value": "red"}]},
                },
            }
        ]

        result = await self.common.fetch_alchemy_batch(
            _FakeAlchemySession(payload),
            asyncio.Semaphore(1),
            [("0xabc", 1, "ERC-721")],
        )

        self.assertEqual(
            result,
            [
                (
                    "ipfs://token/1",
                    None,
                    "Collection A",
                    "COLA",
                    {"name": "NFT #1", "attributes": [{"trait_type": "bg", "value": "red"}]},
                )
            ],
        )


class EvmCommonDbSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_evm_common()

    def test_init_db_ensures_name_symbol_metadata_columns_exist(self):
        conn = _RecordingConn()

        self.common.init_db(conn, "polygon")

        executed_sql = "\n".join(sql for sql, _ in conn.cursor_obj.executed)
        self.assertIn("name             TEXT", executed_sql)
        self.assertIn("symbol           TEXT", executed_sql)
        self.assertIn("metadata         JSONB", executed_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS name", executed_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS symbol", executed_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS metadata", executed_sql)
        self.assertIn("claimed_at       TIMESTAMPTZ", executed_sql)
        self.assertIn("claimed_by       TEXT", executed_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS claimed_at", executed_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS claimed_by", executed_sql)

    def test_batch_insert_main_includes_name_symbol_and_metadata(self):
        conn = _RecordingConn()
        calls = []

        def _fake_execute_values(cur, sql, page, template=None, page_size=None):
            calls.append((sql, list(page), template, page_size))
            cur.rowcount = len(page)

        original = self.common.execute_values
        self.common.execute_values = _fake_execute_values
        try:
            inserted = self.common.batch_insert_main(
                conn,
                "polygon",
                [
                    (
                        "0xabc",
                        "1",
                        "ipfs://token/1",
                        "ipfs://image/1",
                        "Collection A",
                        "COLA",
                        {"name": "NFT #1"},
                        "ERC-721",
                        123,
                    )
                ],
            )
        finally:
            self.common.execute_values = original

        self.assertEqual(inserted, 1)
        self.assertEqual(len(calls), 1)
        sql, page, template, page_size = calls[0]
        self.assertIn("contract_address, token_id, token_uri, image_uri,", sql)
        self.assertIn("name, symbol, metadata, token_standard, first_seen_block", sql)
        self.assertEqual(page[0][4], "Collection A")
        self.assertEqual(page[0][5], "COLA")
        self.assertEqual(page[0][6], "{\"name\": \"NFT #1\"}")
        self.assertIn("jsonb", template.lower())
        self.assertEqual(page_size, 1)

    def test_claim_pending_nfts_uses_skip_locked_and_marks_worker(self):
        conn = _RecordingConn(
            [
                (
                    10,
                    "0xabc",
                    1,
                    "ERC-721",
                    123,
                )
            ]
        )

        claimed = self.common.claim_pending_nfts(
            conn,
            "polygon",
            worker_id="worker-1",
            batch_size=250,
            reclaim_after_seconds=900,
        )

        executed_sql, params = conn.cursor_obj.executed[0]
        self.assertEqual(
            claimed,
            [
                (
                    10,
                    "0xabc",
                    1,
                    "ERC-721",
                    123,
                )
            ],
        )
        self.assertIn("FOR UPDATE SKIP LOCKED", executed_sql)
        self.assertIn("SET claimed_at = NOW(), claimed_by = %s", executed_sql)
        self.assertIn("claimed_at IS NULL", executed_sql)
        self.assertEqual(params, ("900 seconds", 250, "worker-1"))
        self.assertEqual(conn.commit_count, 1)

    def test_claim_pending_nfts_recovers_after_missing_claim_columns(self):
        conn = _UndefinedColumnConn()

        claimed = self.common.claim_pending_nfts(
            conn,
            "polygon",
            worker_id="worker-2",
            batch_size=100,
            reclaim_after_seconds=600,
        )

        executed_sql = "\n".join(sql for sql, _ in conn.cursor_obj.executed)
        self.assertEqual(
            claimed,
            [
                (
                    11,
                    "0xdef",
                    2,
                    "ERC-1155",
                    456,
                )
            ],
        )
        self.assertIn("ALTER TABLE temp_polygon ADD COLUMN IF NOT EXISTS claimed_at", executed_sql)
        self.assertIn("ALTER TABLE temp_polygon ADD COLUMN IF NOT EXISTS claimed_by", executed_sql)
        self.assertGreaterEqual(conn.rollback_count, 1)


if __name__ == "__main__":
    unittest.main()
