import asyncio
import importlib.util
import json
import struct
import sys
import types
import unittest
from pathlib import Path


def _load_solana_common():
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extensions = types.SimpleNamespace(connection=object)
    psycopg2.extras = types.ModuleType("psycopg2.extras")

    def _execute_values(cur, sql, page, template=None, page_size=None):
        flat = [item for rec in page for item in rec]
        recorded_sql = f"{sql}\n{template or ''}"
        cur.execute(recorded_sql, flat)
        cur.rowcount = len(page)

    psycopg2.extras.execute_values = _execute_values

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *args, **kwargs: None

    aiohttp = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _ClientTimeout:
        def __init__(self, *args, **kwargs):
            pass

    class _ClientResponseError(Exception):
        def __init__(self, *args, status=None, **kwargs):
            super().__init__(status)
            self.status = status

    aiohttp.ClientSession = _ClientSession
    aiohttp.ClientTimeout = _ClientTimeout
    aiohttp.ClientResponseError = _ClientResponseError

    injected = {
        "psycopg2": psycopg2,
        "psycopg2.extras": psycopg2.extras,
        "dotenv": dotenv,
        "aiohttp": aiohttp,
    }
    original = {name: sys.modules.get(name) for name in injected}
    sys.modules.update(injected)
    try:
        path = Path(__file__).resolve().parents[1] / "src" / "fetch" / "solana" / "common.py"
        spec = importlib.util.spec_from_file_location("solana_common_under_test", path)
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
    def __init__(self):
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "INSERT INTO" in sql:
            self.rowcount = 1

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingConn:
    def __init__(self):
        self.cursor_obj = _RecordingCursor()
        self.commit_count = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1


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
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return _FakeHttpResponse(200, self.payload)

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, payload):
        self.payload = payload

    def post(self, url, json=None, timeout=None):
        return _FakePostContext(self.payload)


class _FailingPostResponse:
    status = 500

    def raise_for_status(self):
        raise RuntimeError("HTTP 500")

    async def json(self, content_type=None):
        return {}


class _FailingPostContext:
    async def __aenter__(self):
        return _FailingPostResponse()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FlakyThenSuccessPostContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        if self.session.post_count < self.session.succeed_on:
            return _FailingPostResponse()
        return _FakeHttpResponse(200, self.session.payload)

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FlakyThenSuccessPostSession:
    def __init__(self, succeed_on, payload):
        self.succeed_on = succeed_on
        self.payload = payload
        self.post_count = 0

    def post(self, url, json=None, timeout=None):
        self.post_count += 1
        return _FlakyThenSuccessPostContext(self)


class SolanaCommonDbFormatTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_solana_common()

    def test_init_db_creates_collection_asset_shape_for_snapshot_export(self):
        conn = _RecordingConn()

        self.common.init_db(conn, "solana")

        all_sql = "\n".join(sql for sql, _params in conn.cursor_obj.executed)
        self.assertIn("contract_address VARCHAR(44)  NOT NULL", all_sql)
        self.assertIn("token_id         VARCHAR(44)  NOT NULL", all_sql)
        self.assertIn("mint_address     VARCHAR(44) NOT NULL", all_sql)
        self.assertIn("metadata         JSONB", all_sql)
        self.assertIn("UNIQUE (contract_address, token_id)", all_sql)
        self.assertIn("UNIQUE (mint_address)", all_sql)
        self.assertIn("ADD CONSTRAINT nft_assets_solana_contract_token_key", all_sql)
        self.assertIn("ADD CONSTRAINT temp_solana_mint_key", all_sql)
        self.assertIn("claimed_at       TIMESTAMPTZ", all_sql)
        self.assertIn("claimed_by       TEXT", all_sql)
        self.assertIn("idx_sol_temp_claimed_at", all_sql)
        self.assertNotIn("ALTER COLUMN token_id TYPE NUMERIC", all_sql)
        self.assertNotIn("ALTER COLUMN token_id SET DEFAULT 1", all_sql)
        self.assertNotIn("CREATE UNIQUE INDEX IF NOT EXISTS idx_sol_nft_contract_token", all_sql)
        self.assertNotIn("CREATE UNIQUE INDEX IF NOT EXISTS idx_sol_temp_contract_token", all_sql)

    def test_batch_insert_temp_records_pending_mints_without_token_ids(self):
        conn = _RecordingConn()

        inserted = self.common.batch_insert_temp(
            conn,
            "solana",
            [
                (
                    "Mint111111111111111111111111111111111111111",
                    "Metaplex",
                    123456,
                )
            ],
        )

        self.assertEqual(inserted, 1)
        insert_sql, params = conn.cursor_obj.executed[-1]
        self.assertIn("INSERT INTO temp_solana (mint_address, token_standard, first_seen_block)", insert_sql)
        self.assertIn("ON CONFLICT (mint_address)", insert_sql)
        self.assertEqual(
            params,
            [
                "Mint111111111111111111111111111111111111111",
                "Metaplex",
                123456,
            ],
        )

    def test_batch_insert_main_serializes_metadata_and_uses_collection_plus_mint(self):
        conn = _RecordingConn()
        metadata = {"name": "NFT\x00 #1", "attributes": [{"trait_type": "rank\x00"}]}

        inserted = self.common.batch_insert_main(
            conn,
            "solana",
            [
                (
                    "Collection1111111111111111111111111111111111",
                    "Mint111111111111111111111111111111111111111",
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "NFT\x00 #1",
                    "SYM",
                    metadata,
                    "Metaplex",
                    123456,
                )
            ],
        )

        self.assertEqual(inserted, 1)
        insert_sql, params = conn.cursor_obj.executed[-1]
        self.assertIn("metadata", insert_sql)
        self.assertIn("%s::jsonb", insert_sql)
        self.assertIn("ON CONFLICT (contract_address, token_id)", insert_sql)
        self.assertEqual(params[0], "Collection1111111111111111111111111111111111")
        self.assertEqual(params[1], "Mint111111111111111111111111111111111111111")
        self.assertEqual(params[4], "NFT #1")
        self.assertEqual(json.loads(params[6]), {"name": "NFT #1", "attributes": [{"trait_type": "rank"}]})

    def test_batch_insert_main_uses_paged_execute_values(self):
        conn = _RecordingConn()
        calls = []
        original_execute_values = self.common.execute_values
        original_page_size = self.common.DB_INSERT_PAGE_SIZE
        self.common.DB_INSERT_PAGE_SIZE = 2

        def _fake_execute_values(cur, sql, page, template=None, page_size=None):
            calls.append((sql, list(page), template, page_size))
            cur.rowcount = len(page)

        self.common.execute_values = _fake_execute_values
        try:
            inserted = self.common.batch_insert_main(
                conn,
                "solana",
                [
                    (
                        "Collection1111111111111111111111111111111111",
                        f"Mint{i:041d}",
                        f"ipfs://token/{i}",
                        f"https://image.example/{i}.png",
                        f"NFT #{i}",
                        "SYM",
                        {"rank": i},
                        "Metaplex",
                        123456,
                    )
                    for i in range(5)
                ],
            )
        finally:
            self.common.execute_values = original_execute_values
            self.common.DB_INSERT_PAGE_SIZE = original_page_size

        self.assertEqual(inserted, 5)
        self.assertEqual([len(call[1]) for call in calls], [2, 2, 1])
        self.assertTrue(all(call[3] == len(call[1]) for call in calls))
        self.assertTrue(all("%s::jsonb" in call[2] for call in calls))

    def test_claim_pending_nfts_uses_skip_locked_and_marks_worker(self):
        class _ClaimCursor(_RecordingCursor):
            def __init__(self):
                super().__init__()
                self.rowcount = 1

            def fetchall(self):
                return [(7, "Mint111", "Metaplex", 123456)]

        class _ClaimConn(_RecordingConn):
            def __init__(self):
                self.cursor_obj = _ClaimCursor()
                self.commit_count = 0

        conn = _ClaimConn()

        claimed = self.common.claim_pending_nfts(
            conn,
            "solana",
            worker_id="host:123:worker-1",
            batch_size=500,
            reclaim_after_seconds=1800,
        )

        sql, params = conn.cursor_obj.executed[-1]
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("SET claimed_at = NOW(), claimed_by = %s", sql)
        self.assertEqual(params, ("1800 seconds", 500, "host:123:worker-1"))
        self.assertEqual(claimed, [(7, "Mint111", "Metaplex", 123456)])

    def test_release_temp_claims_clears_only_current_worker_claims(self):
        conn = _RecordingConn()
        conn.cursor_obj.rowcount = 2

        released = self.common.release_temp_claims(
            conn,
            "solana",
            [7, 8],
            "host:123:worker-1",
        )

        sql, params = conn.cursor_obj.executed[-1]
        self.assertIn("SET claimed_at = NULL, claimed_by = NULL", sql)
        self.assertIn("WHERE id = ANY(%s) AND claimed_by = %s", sql)
        self.assertEqual(params, ([7, 8], "host:123:worker-1"))
        self.assertEqual(released, 2)


class SolanaHeliusMetadataTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_solana_common()

    async def test_fetch_helius_metadata_batch_returns_raw_metadata_payload(self):
        self.common.HELIUS_API_KEY = "test-key"
        payload = {
            "result": [
                {
                    "id": "Mint111",
                    "content": {
                        "json_uri": "ipfs://token/1",
                        "links": {"image": "https://image.example/1.png"},
                        "metadata": {
                            "name": "Collection #1",
                            "symbol": "COL",
                            "attributes": [{"trait_type": "rank", "value": "1"}],
                        },
                    },
                    "grouping": [
                        {
                            "group_key": "collection",
                            "group_value": "Collection1111111111111111111111111111111111",
                        }
                    ],
                }
            ]
        }

        result = await self.common.fetch_helius_metadata_batch(
            _FakeSession(payload),
            asyncio.Semaphore(1),
            ["Mint111"],
        )

        self.assertEqual(
            result,
            [
                (
                    "Collection1111111111111111111111111111111111",
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "Collection #1",
                    "COL",
                    {
                        "name": "Collection #1",
                        "symbol": "COL",
                        "attributes": [{"trait_type": "rank", "value": "1"}],
                    },
                )
            ],
        )

    async def test_fetch_gpa_page_raises_on_rpc_error_instead_of_marking_complete(self):
        payload = {"error": {"code": -32005, "message": "upstream unavailable"}}

        with self.assertRaisesRegex(RuntimeError, "GPA RPC"):
            await self.common.fetch_gpa_page(
                _FakeSession(payload),
                "https://rpc.example",
                page_size=1000,
                pagination_key=None,
            )

    async def test_fetch_metadata_batch_does_not_fallback_when_only_collection_is_missing(self):
        self.common.HELIUS_API_KEY = "test-key"
        original_helius = self.common.fetch_helius_metadata_batch

        async def _helius(*_args, **_kwargs):
            return [
                (
                    None,
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "Ungrouped #1",
                    "UNG",
                    {"name": "Ungrouped #1"},
                )
            ]

        self.common.fetch_helius_metadata_batch = _helius
        try:
            result = await self.common.fetch_metadata_batch(
                object(),
                asyncio.Semaphore(1),
                ["Mint111"],
            )
        finally:
            self.common.fetch_helius_metadata_batch = original_helius

        self.assertEqual(result[0][0], None)
        self.assertEqual(result[0][1], "ipfs://token/1")
        self.assertEqual(result[0][2], "https://image.example/1.png")

    async def test_fetch_metadata_batch_uses_helius_only_when_uri_or_image_missing(self):
        self.common.HELIUS_API_KEY = "test-key"
        original_helius = self.common.fetch_helius_metadata_batch

        async def _helius(*_args, **_kwargs):
            return [
                (
                    "Collection1111111111111111111111111111111111",
                    None,
                    None,
                    "Partial #1",
                    "PAR",
                    {"name": "Partial #1"},
                )
            ]

        self.common.fetch_helius_metadata_batch = _helius
        try:
            result = await self.common.fetch_metadata_batch(
                object(),
                asyncio.Semaphore(1),
                ["Mint111"],
            )
        finally:
            self.common.fetch_helius_metadata_batch = original_helius

        self.assertEqual(
            result,
            [
                (
                    "Collection1111111111111111111111111111111111",
                    None,
                    None,
                    "Partial #1",
                    "PAR",
                    {"name": "Partial #1"},
                )
            ],
        )

    async def test_fetch_metadata_batch_without_helius_key_returns_empty_without_onchain_fallback(self):
        self.common.HELIUS_API_KEY = ""
        result = await self.common.fetch_metadata_batch(
            object(),
            asyncio.Semaphore(1),
            ["Mint111"],
        )

        self.assertEqual(result, [(None, None, None, None, None, None)])

    async def test_fetch_helius_metadata_batch_retries_failed_request_before_success(self):
        self.common.HELIUS_API_KEY = "test-key"
        session = _FlakyThenSuccessPostSession(
            3,
            {
                "result": [
                    {
                        "id": "Mint111",
                        "content": {
                            "json_uri": "ipfs://token/1",
                            "links": {"image": "https://image.example/1.png"},
                            "metadata": {"name": "Retry #1", "symbol": "TRY"},
                        },
                    }
                ]
            },
        )

        result = await self.common.fetch_helius_metadata_batch(
            session,
            asyncio.Semaphore(1),
            ["Mint111"],
        )

        self.assertEqual(session.post_count, 3)
        self.assertEqual(
            result,
            [
                (
                    None,
                    "ipfs://token/1",
                    "https://image.example/1.png",
                    "Retry #1",
                    "TRY",
                    {"name": "Retry #1", "symbol": "TRY"},
                )
            ],
        )


class SolanaOnchainMetadataParsingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_solana_common()

    def test_parse_metadata_account_extracts_collection_after_seller_fee(self):
        mint_bytes = bytes([1]) * 32
        collection_bytes = bytes([2]) * 32
        mint_address = self.common.b58encode(mint_bytes)
        collection_address = self.common.b58encode(collection_bytes)

        def _borsh_string(value):
            raw = value.encode("utf-8")
            return struct.pack("<I", len(raw)) + raw

        data = b"".join(
            [
                b"\x04",
                bytes([9]) * 32,
                mint_bytes,
                _borsh_string("Onchain Name"),
                _borsh_string("OCN"),
                _borsh_string("https://metadata.example/1.json"),
                struct.pack("<H", 500),
                b"\x00",
                b"\x00",
                b"\x01",
                b"\x00",
                b"\x01\x00",
                b"\x01\x01",
                collection_bytes,
            ]
        )

        parsed = self.common.parse_metadata_account(data)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["mint"], mint_address)
        self.assertEqual(parsed["collection"], collection_address)


if __name__ == "__main__":
    unittest.main()
