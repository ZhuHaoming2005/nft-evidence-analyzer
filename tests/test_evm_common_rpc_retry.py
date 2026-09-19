import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path


def _encode_abi_string(value: str) -> str:
    payload = value.encode("utf-8")
    padded = payload + (b"\x00" * ((32 - len(payload) % 32) % 32))
    encoded = (32).to_bytes(32, "big") + len(payload).to_bytes(32, "big") + padded
    return "0x" + encoded.hex()


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
        spec = importlib.util.spec_from_file_location("evm_common_rpc_retry_under_test", path)
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


class _FakeHttpResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self, content_type=None):
        return self._payload


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


class _RetryAwareAlchemyPostContext:
    def __init__(self, session, contract_address: str):
        self.session = session
        self.contract_address = contract_address

    async def __aenter__(self):
        attempts = self.session.attempts.get(self.contract_address, 0) + 1
        self.session.attempts[self.contract_address] = attempts
        self.session.entered.append((self.contract_address, attempts))
        if self.contract_address == "0x1" and attempts == 1:
            raise RuntimeError("temporary alchemy failure")
        return _FakeHttpResponse(
            200,
            [
                {
                    "contract": {"name": f"Collection {self.contract_address}", "symbol": "COL"},
                    "raw": {
                        "tokenUri": f"ipfs://token/{self.contract_address}",
                        "metadata": {"name": f"NFT {self.contract_address}"},
                    },
                }
            ],
        )

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _RetryAwareAlchemySession:
    def __init__(self):
        self.attempts = {}
        self.entered = []

    def post(self, url, json=None, timeout=None):
        contract_address = json["tokens"][0]["contractAddress"]
        return _RetryAwareAlchemyPostContext(self, contract_address)


class _RetryAwareRpcPostContext:
    def __init__(self, session, contract_address: str, token_id: int):
        self.session = session
        self.contract_address = contract_address
        self.token_id = token_id

    async def __aenter__(self):
        attempts = self.session.attempts.get(self.contract_address, 0) + 1
        self.session.attempts[self.contract_address] = attempts
        self.session.entered.append((self.contract_address, attempts))
        if self.contract_address == "0x1" and attempts == 1:
            raise RuntimeError("temporary rpc failure")
        return _FakeHttpResponse(
            200,
            [
                {
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": _encode_abi_string(f"https://example/{self.token_id}"),
                }
            ],
        )

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _RetryAwareRpcSession:
    def __init__(self):
        self.attempts = {}
        self.entered = []

    def post(self, url, json=None, timeout=None):
        contract_address, token_id = json[0]["params"][0]["to"], int(json[0]["params"][0]["data"][-64:], 16)
        return _RetryAwareRpcPostContext(self, contract_address, token_id)


class EvmCommonRpcRetryTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.common = _load_evm_common()

    async def test_fetch_alchemy_batch_normalizes_nested_image_object(self):
        payload = [
            {
                "contract": {"name": "Collection A", "symbol": "COLA"},
                "image": {"originalUrl": "https://cdn.example/top-level.png"},
                "raw": {
                    "tokenUri": "ipfs://token/1",
                    "metadata": {
                        "name": "NFT #1",
                        "image": {"originalUrl": "https://cdn.example/raw.png"},
                    },
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
                    "https://cdn.example/raw.png",
                    "Collection A",
                    "COLA",
                    {
                        "name": "NFT #1",
                        "image": {"originalUrl": "https://cdn.example/raw.png"},
                    },
                )
            ],
        )

    async def test_fetch_alchemy_batch_releases_semaphore_during_retry_backoff(self):
        session = _RetryAwareAlchemySession()
        sem = asyncio.Semaphore(1)
        sleep_started = asyncio.Event()
        release_sleep = asyncio.Event()

        async def _fake_sleep(_delay):
            sleep_started.set()
            await release_sleep.wait()

        original_sleep = self.common.asyncio.sleep
        self.common.asyncio.sleep = _fake_sleep
        try:
            first = asyncio.create_task(
                self.common.fetch_alchemy_batch(session, sem, [("0x1", 1, "ERC-721")])
            )
            await asyncio.wait_for(sleep_started.wait(), timeout=0.2)

            second = asyncio.create_task(
                self.common.fetch_alchemy_batch(session, sem, [("0x2", 2, "ERC-721")])
            )
            second_result = await asyncio.wait_for(second, timeout=0.2)
            self.assertEqual(second_result[0][0], "ipfs://token/0x2")

            release_sleep.set()
            first_result = await asyncio.wait_for(first, timeout=0.2)
            self.assertEqual(first_result[0][0], "ipfs://token/0x1")
        finally:
            self.common.asyncio.sleep = original_sleep

        self.assertEqual(session.attempts["0x1"], 2)
        self.assertEqual(session.attempts["0x2"], 1)
        self.assertEqual(session.entered[:2], [("0x1", 1), ("0x2", 1)])

    async def test_fetch_token_uri_batch_releases_semaphore_during_retry_backoff(self):
        session = _RetryAwareRpcSession()
        sem = asyncio.Semaphore(1)
        sleep_started = asyncio.Event()
        release_sleep = asyncio.Event()

        async def _fake_sleep(_delay):
            sleep_started.set()
            await release_sleep.wait()

        original_sleep = self.common.asyncio.sleep
        self.common.asyncio.sleep = _fake_sleep
        try:
            first = asyncio.create_task(
                self.common.fetch_token_uri_batch(
                    session,
                    "https://rpc.example",
                    sem,
                    [("0x1", 1, "ERC-721")],
                )
            )
            await asyncio.wait_for(sleep_started.wait(), timeout=0.2)

            second = asyncio.create_task(
                self.common.fetch_token_uri_batch(
                    session,
                    "https://rpc.example",
                    sem,
                    [("0x2", 2, "ERC-721")],
                )
            )
            second_result = await asyncio.wait_for(second, timeout=0.2)
            self.assertEqual(second_result, ["https://example/2"])

            release_sleep.set()
            first_result = await asyncio.wait_for(first, timeout=0.2)
            self.assertEqual(first_result, ["https://example/1"])
        finally:
            self.common.asyncio.sleep = original_sleep

        self.assertEqual(session.attempts["0x1"], 2)
        self.assertEqual(session.attempts["0x2"], 1)
        self.assertEqual(session.entered[:2], [("0x1", 1), ("0x2", 1)])

    async def test_fetch_alchemy_batch_applies_startup_jitter_only_once(self):
        session = _RetryAwareAlchemySession()
        sem = asyncio.Semaphore(1)
        original_delay = getattr(self.common, "REQUEST_STARTUP_STAGGER_SECONDS", 0.0)
        try:
            self.common.REQUEST_STARTUP_STAGGER_SECONDS = 0.5
            with unittest.mock.patch.object(self.common.asyncio, "sleep") as sleep_mock:
                result = await self.common.fetch_alchemy_batch(
                    session,
                    sem,
                    [("0x2", 2, "ERC-721")],
                    startup_delay_seconds=1.25,
                )
        finally:
            self.common.REQUEST_STARTUP_STAGGER_SECONDS = original_delay

        self.assertEqual(result[0][0], "ipfs://token/0x2")
        sleep_mock.assert_awaited_once_with(1.25)

    async def test_fetch_token_uri_batch_applies_startup_jitter_only_once(self):
        session = _RetryAwareRpcSession()
        sem = asyncio.Semaphore(1)
        original_delay = getattr(self.common, "REQUEST_STARTUP_STAGGER_SECONDS", 0.0)
        try:
            self.common.REQUEST_STARTUP_STAGGER_SECONDS = 0.5
            with unittest.mock.patch.object(self.common.asyncio, "sleep") as sleep_mock:
                result = await self.common.fetch_token_uri_batch(
                    session,
                    "https://rpc.example",
                    sem,
                    [("0x2", 2, "ERC-721")],
                    startup_delay_seconds=0.75,
                )
        finally:
            self.common.REQUEST_STARTUP_STAGGER_SECONDS = original_delay

        self.assertEqual(result, ["https://example/2"])
        sleep_mock.assert_awaited_once_with(0.75)


if __name__ == "__main__":
    unittest.main()
