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


def _load_retry():
    aiohttp = types.ModuleType("aiohttp")

    class _ClientTimeout:
        def __init__(self, *args, **kwargs):
            pass

    class _TCPConnector:
        def __init__(self, *args, **kwargs):
            pass

    class _FakeResponse:
        def __init__(self, status, body):
            self.status = status
            self._body = body

        def raise_for_status(self):
            if self.status >= 400:
                raise RuntimeError(f"HTTP {self.status}")

        async def json(self, content_type=None):
            return json.loads(self._body)

    class _GetContext:
        def __init__(self, url, headers=None):
            self.url = url
            self.headers = headers or {}
            self.response = None

        async def __aenter__(self):
            def _send():
                req = request.Request(self.url, headers=self.headers, method="GET")
                with request.urlopen(req, timeout=5) as resp:
                    return _FakeResponse(resp.status, resp.read())

            self.response = await asyncio.to_thread(_send)
            return self.response

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _ClientSession:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None, allow_redirects=True):
            return _GetContext(url, headers=headers)

    aiohttp.ClientTimeout = _ClientTimeout
    aiohttp.TCPConnector = _TCPConnector
    aiohttp.ClientSession = _ClientSession

    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extensions = types.SimpleNamespace(connection=object, cursor=object)
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.DictCursor = object

    injected = {
        "aiohttp": aiohttp,
        "psycopg2": psycopg2,
        "psycopg2.extras": psycopg2.extras,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "src" / "fetch" / "evm" / "retry.py"
        spec = importlib.util.spec_from_file_location("retry_under_test", path)
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


retry = _load_retry()


class _MetadataHandler(BaseHTTPRequestHandler):
    response_delay = 0.25

    def do_GET(self):
        time.sleep(self.response_delay)
        token = self.path.rsplit("/", 1)[-1]
        body = json.dumps(
            {
                "image": f"https://cdn.example/{token}.png",
                "name": f"NFT {token}",
                "attributes": [{"trait_type": "token", "value": token}],
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


class RetryAsyncFetchTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _MetadataHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=2)

    async def test_fetch_image_uris_concurrently(self):
        token_uris = [f"{self.base_url}/metadata/{idx}" for idx in range(3)]

        start = time.perf_counter()
        result = await retry.fetch_image_uris_for_token_uris(token_uris, concurrency=3)
        elapsed = time.perf_counter() - start

        self.assertEqual(
            result,
            {
                token_uri: f"https://cdn.example/{idx}.png"
                for idx, token_uri in enumerate(token_uris)
            },
        )
        self.assertLess(elapsed, 0.55)

    async def test_fetch_metadata_records_returns_full_metadata(self):
        token_uri = f"{self.base_url}/metadata/7"

        result = await retry.fetch_metadata_records_for_token_uris([token_uri], concurrency=1)

        self.assertEqual(
            result,
            {
                token_uri: {
                    "image": "https://cdn.example/7.png",
                    "name": "NFT 7",
                    "attributes": [{"trait_type": "token", "value": "7"}],
                }
            },
        )


class _FakeCursor:
    def __init__(self):
        self.calls = []
        self.rowcount = 2

    def execute(self, sql, params):
        self.calls.append((sql, params))


class RetryUpdateTests(unittest.TestCase):
    def test_update_metadata_by_token_uri_updates_image_and_metadata(self):
        cur = _FakeCursor()
        metadata = {"image": "https://cdn.example/1.png", "name": "NFT 1"}

        updated = retry.update_metadata_by_token_uri(
            cur,
            "ipfs://token/1",
            metadata,
            "polygon",
        )

        self.assertEqual(updated, 2)
        sql, params = cur.calls[0]
        self.assertIn("SET image_uri = %s,", sql)
        self.assertIn("metadata = %s::jsonb", sql)
        self.assertEqual(params[0], "https://cdn.example/1.png")
        self.assertEqual(params[1], json.dumps(metadata, ensure_ascii=False))
        self.assertEqual(params[2], "ipfs://token/1")

    def test_get_conn_enables_autocommit(self):
        class _FakeConn:
            def __init__(self):
                self.autocommit = False

        fake_conn = _FakeConn()
        original = getattr(retry.psycopg2, "connect", None)
        retry.psycopg2.connect = lambda **kwargs: fake_conn
        try:
            conn = retry.get_conn()
        finally:
            if original is None:
                delattr(retry.psycopg2, "connect")
            else:
                retry.psycopg2.connect = original

        self.assertIs(conn, fake_conn)
        self.assertTrue(conn.autocommit)


if __name__ == "__main__":
    unittest.main()
