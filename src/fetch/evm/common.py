#!/usr/bin/env python3
"""Shared configuration, PostgreSQL helpers, Alchemy/RPC requests, and EVM log decoding."""

import asyncio
import base64
import json as _json
import logging
import os
import re
import sys
from typing import Any, List, Optional, Set, Tuple
from urllib.parse import unquote

import aiohttp
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

load_dotenv()


_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


ERC721_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
ERC1155_SINGLE_TOPIC = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)
ERC1155_BATCH_TOPIC = (
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)
ALL_TOPICS = [ERC721_TRANSFER_TOPIC, ERC1155_SINGLE_TOPIC, ERC1155_BATCH_TOPIC]

_ERC721_TRANSFER_B = bytes.fromhex(ERC721_TRANSFER_TOPIC[2:])
_ERC1155_SINGLE_B  = bytes.fromhex(ERC1155_SINGLE_TOPIC[2:])
_ERC1155_BATCH_B   = bytes.fromhex(ERC1155_BATCH_TOPIC[2:])


ERC721_ABI = [
    {
        "inputs": [{"name": "tokenId", "type": "uint256"}],
        "name": "tokenURI",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    }
]
ERC1155_ABI = [
    {
        "inputs": [{"name": "id", "type": "uint256"}],
        "name": "uri",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    }
]


CHAIN_NAME       = os.getenv("CHAIN_NAME", "polygon")
RPC_URL          = os.getenv("RPC_URL", "https://polygon-rpc.com")
START_BLOCK      = int(os.getenv("START_BLOCK", "0"))
END_BLOCK        = int(os.getenv("END_BLOCK", "0"))
BLOCK_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "2000"))
REQUEST_DELAY    = float(os.getenv("REQUEST_DELAY", "0.1"))
REQUEST_STARTUP_STAGGER_SECONDS = max(
    0.0,
    float(os.getenv("REQUEST_STARTUP_STAGGER_SECONDS", "0")),
)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

METADATA_TIMEOUT         = int(os.getenv("METADATA_TIMEOUT", "10"))
METADATA_CONNECT_TIMEOUT = int(os.getenv("METADATA_CONNECT_TIMEOUT", "15"))

ALCHEMY_API_KEY    = os.getenv("ALCHEMY_API_KEY", "")
ALCHEMY_NETWORK    = os.getenv("ALCHEMY_NETWORK", "polygon-mainnet")
ALCHEMY_BATCH_SIZE = int(os.getenv("ALCHEMY_BATCH_SIZE", "100"))
RPC_BATCH_SIZE     = int(os.getenv("RPC_BATCH_SIZE", "100"))

DEFI_BLACKLIST_ENV = os.getenv("DEFI_BLACKLIST", "")

CONCURRENT_ALCHEMY = int(os.getenv("CONCURRENT_ALCHEMY", "5"))
CONCURRENT_RPC     = int(os.getenv("CONCURRENT_RPC", "10"))
FETCH_CLAIM_BATCH_SIZE = int(os.getenv("FETCH_CLAIM_BATCH_SIZE", "5000"))
CLAIM_RETRY_AFTER_SECONDS = int(os.getenv("CLAIM_RETRY_AFTER_SECONDS", "1800"))


SCAN_WINDOW = int(os.getenv("SCAN_WINDOW", "3"))


FETCH_IDLE_WAIT  = int(os.getenv("FETCH_IDLE_WAIT", "30"))
_ERC721_TOKEN_URI_SELECTOR = "c87b56dd"
_ERC1155_URI_SELECTOR = "0e89341c"
DB_INSERT_PAGE_SIZE = int(os.getenv("DB_INSERT_PAGE_SIZE", "1000"))


def _nft_table_name(chain_name: str) -> str:
    """Return the validated name of the persistent NFT table."""
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower()) or "default"
    return f"nft_assets_{safe}"


def _temp_table_name(chain_name: str) -> str:
    """Return the validated name of the staging table."""
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower()) or "default"
    return f"temp_{safe}"


def _load_varchar_limits(conn, table_name: str, columns: List[str]) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
              AND column_name = ANY(%s)
              AND data_type IN ('character varying', 'character')
            """,
            (table_name, columns),
        )
        return {
            row[0]: int(row[1])
            for row in cur.fetchall()
            if row[1] is not None
        }


def load_blacklist() -> Set[str]:
    out: Set[str] = set()
    for part in DEFI_BLACKLIST_ENV.split(","):
        a = part.strip()
        if a.startswith("0x"):
            out.add(a.lower())
    try:
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped.startswith("DEFI_BLACKLIST="):
                    continue
                for part in stripped[len("DEFI_BLACKLIST="):].split(","):
                    a = part.strip()
                    if a.startswith("0x"):
                        out.add(a.lower())
                break
    except FileNotFoundError:
        pass
    return out


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=10,
    )


def init_db(conn, chain_name: str) -> None:
    """Create NFT, staging, and scan-progress tables if absent."""
    tbl = _nft_table_name(chain_name)
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scan_progress (
                chain_name         VARCHAR(50) PRIMARY KEY,
                last_scanned_block BIGINT      NOT NULL,
                updated_at         TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                id               BIGSERIAL    PRIMARY KEY,
                contract_address VARCHAR(42)  NOT NULL,
                token_id         NUMERIC      NOT NULL,
                token_uri        TEXT,
                image_uri        TEXT,
                name             TEXT,
                symbol           TEXT,
                metadata         JSONB,
                token_standard   VARCHAR(10),
                first_seen_block BIGINT,
                created_at       TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (contract_address, token_id)
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_nft_contract ON {tbl} (contract_address)"
        )

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tmp} (
                id               BIGSERIAL    PRIMARY KEY,
                contract_address VARCHAR(42)  NOT NULL,
                token_id         NUMERIC      NOT NULL,
                token_standard   VARCHAR(10),
                first_seen_block BIGINT,
                claimed_at       TIMESTAMPTZ,
                claimed_by       TEXT,
                created_at       TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (contract_address, token_id)
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_temp_contract ON {tmp} (contract_address)"
        )
    ensure_main_table_columns(conn, chain_name)
    ensure_temp_table_claim_columns(conn, chain_name)
    conn.commit()
    logger.info("Database initialized: main=%s staging=%s", tbl, tmp)


def ensure_main_table_columns(conn, chain_name: str) -> None:
    """Add metadata columns required by the collector if absent."""
    tbl = _nft_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS name TEXT")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS symbol TEXT")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS metadata JSONB")
    conn.commit()
    logger.info("Main table columns ready: %s (name, symbol, metadata)", tbl)


def ensure_temp_table_claim_columns(conn, chain_name: str) -> None:
    """Add claim columns for concurrent metadata workers if absent."""
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {tmp} ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ")
        cur.execute(f"ALTER TABLE {tmp} ADD COLUMN IF NOT EXISTS claimed_by TEXT")
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_temp_claimed_at ON {tmp} (claimed_at, id)"
        )
    conn.commit()
    logger.info("Staging claim columns ready: %s (claimed_at, claimed_by)", tmp)


def _is_missing_claim_column_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if getattr(exc, "pgcode", None) != "42703" and "does not exist" not in message:
        return False
    return "claimed_at" in message or "claimed_by" in message


def load_seen_nfts(conn, chain_name: str) -> Set[Tuple[str, int]]:
    """Load existing (contract_address, token_id) keys from persistent and staging tables."""
    tbl = _nft_table_name(chain_name)
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT contract_address, token_id FROM {tbl}
            UNION
            SELECT contract_address, token_id FROM {tmp}
        """)
        result = {(row[0], int(row[1])) for row in cur.fetchall()}
    conn.commit()  # Close the implicit transaction before network waits.
    return result


def get_last_block(conn, chain_name: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_scanned_block FROM scan_progress WHERE chain_name = %s",
            (chain_name,),
        )
        row = cur.fetchone()
    conn.commit()  # Close the implicit transaction before network waits.
    return row[0] if row else None


def save_progress(conn, chain_name: str, block: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scan_progress (chain_name, last_scanned_block)
            VALUES (%s, %s)
            ON CONFLICT (chain_name) DO UPDATE
                SET last_scanned_block = EXCLUDED.last_scanned_block,
                    updated_at = NOW()
            """,
            (chain_name, block),
        )
    conn.commit()


def batch_insert_temp(conn, chain_name: str, records: List[Tuple]) -> int:
    """Insert discovered NFT records into the staging table."""
    if not records:
        return 0
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(records))
        flat = [item for rec in records for item in rec]
        cur.execute(
            f"""
            INSERT INTO {tmp} (contract_address, token_id, token_standard, first_seen_block)
            VALUES {placeholders}
            ON CONFLICT (contract_address, token_id) DO NOTHING
            """,
            flat,
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def load_pending_nfts(conn, chain_name: str) -> List[Tuple]:
    """Read pending staging rows, including their database IDs."""
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, contract_address, token_id, token_standard, first_seen_block
            FROM {tmp}
            ORDER BY id
            LIMIT 5000
            """,
        )
        result = [(row[0], row[1], int(row[2]), row[3], row[4]) for row in cur.fetchall()]
    # Close the transaction before network I/O to avoid idle timeouts and blocking vacuum.

    conn.commit()
    return result


def claim_pending_nfts(
    conn,
    chain_name: str,
    worker_id: str,
    batch_size: int = FETCH_CLAIM_BATCH_SIZE,
    reclaim_after_seconds: int = CLAIM_RETRY_AFTER_SECONDS,
) -> List[Tuple]:
    """Atomically claim staging rows for concurrent workers and return their IDs and NFT fields."""
    tmp = _temp_table_name(chain_name)
    params = (f"{reclaim_after_seconds} seconds", batch_size, worker_id)

    def _claim_once() -> List[Tuple]:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH candidates AS (
                    SELECT id
                    FROM {tmp}
                    WHERE claimed_at IS NULL
                       OR claimed_at < NOW() - %s::interval
                    ORDER BY id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE {tmp} AS t
                SET claimed_at = NOW(), claimed_by = %s
                FROM candidates
                WHERE t.id = candidates.id
                RETURNING t.id, t.contract_address, t.token_id, t.token_standard, t.first_seen_block
                """,
                params,
            )
            return [(row[0], row[1], int(row[2]), row[3], row[4]) for row in cur.fetchall()]

    try:
        result = _claim_once()
    except Exception as exc:
        if not _is_missing_claim_column_error(exc):
            raise
        if hasattr(conn, "rollback"):
            conn.rollback()
        logger.warning("Missing claim columns; add columns and retry: %s", tmp)
        ensure_temp_table_claim_columns(conn, chain_name)
        result = _claim_once()

    conn.commit()
    return result


def batch_insert_main(conn, chain_name: str, records: List[Tuple]) -> int:
    """Insert valid NFT records: address, token ID, token URI, image URI, name, symbol, metadata, standard, and first-seen height."""
    if not records:
        return 0
    tbl = _nft_table_name(chain_name)
    varchar_limits = _load_varchar_limits(
        conn,
        tbl,
        ["contract_address", "token_id", "token_uri", "image_uri", "name", "symbol", "token_standard"],
    )
    conn.commit()

    def _clean(v):
        if isinstance(v, str):
            return v.replace("\x00", "")
        if isinstance(v, dict):
            return {_clean(k): _clean(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_clean(item) for item in v]
        if isinstance(v, tuple):
            return tuple(_clean(item) for item in v)
        return v

    def _normalize(rec: Tuple[Any, ...]) -> Tuple[Any, ...]:
        cleaned = tuple(_clean(v) for v in rec)
        metadata = cleaned[6]
        if metadata is not None and not isinstance(metadata, str):
            metadata = _json.dumps(metadata, ensure_ascii=False)
        values = list(cleaned[:6] + (metadata,) + cleaned[7:])
        column_positions = {
            "contract_address": 0,
            "token_id": 1,
            "token_uri": 2,
            "image_uri": 3,
            "name": 4,
            "symbol": 5,
            "token_standard": 7,
        }
        for column, pos in column_positions.items():
            limit = varchar_limits.get(column)
            value = values[pos]
            if limit and isinstance(value, str) and len(value) > limit:
                values[pos] = value[:limit]
        return tuple(values)

    records = [_normalize(rec) for rec in records]
    sql = f"""
        INSERT INTO {tbl}
            (
                contract_address, token_id, token_uri, image_uri,
                name, symbol, metadata, token_standard, first_seen_block
            )
        VALUES %s
        ON CONFLICT (contract_address, token_id) DO NOTHING
    """

    inserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(records), DB_INSERT_PAGE_SIZE):
            page = records[start: start + DB_INSERT_PAGE_SIZE]
            execute_values(
                cur,
                sql,
                page,
                template="(%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)",
                page_size=len(page),
            )
            inserted += max(cur.rowcount, 0)
    conn.commit()
    return inserted


def delete_temp_nfts(conn, chain_name: str, ids: List[int]) -> int:
    """Delete processed staging rows, including records rejected by collection filters."""
    if not ids:
        return 0
    tmp = _temp_table_name(chain_name)
    ordered_ids = sorted(set(ids))
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {tmp} WHERE id = ANY(%s)", (ordered_ids,))
        deleted = cur.rowcount
    conn.commit()
    return deleted


def release_temp_claims(conn, chain_name: str, ids: List[int], worker_id: str) -> int:
    """Release this worker's unfinished claims for retry."""
    if not ids:
        return 0
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {tmp}
            SET claimed_at = NULL, claimed_by = NULL
            WHERE id = ANY(%s) AND claimed_by = %s
            """,
            (ids, worker_id),
        )
        released = cur.rowcount
    conn.commit()
    return released


def delete_contract_nfts(
    conn,
    chain_name: str,
    contract_addrs: Set[str],
    *,
    skip_temp_ids: Optional[List[int]] = None,
) -> Tuple[int, int]:
    """Delete blacklisted contracts from both tables; return (main_deleted, staging_deleted)."""
    if not contract_addrs:
        return 0, 0
    tbl  = _nft_table_name(chain_name)
    tmp  = _temp_table_name(chain_name)
    addrs = [a.lower() for a in contract_addrs]
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {tbl} WHERE contract_address = ANY(%s)", (addrs,))
        main_del = cur.rowcount
        if skip_temp_ids:
            cur.execute(
                f"""
                WITH locked_rows AS (
                    SELECT id
                    FROM {tmp}
                    WHERE contract_address = ANY(%s)
                      AND id <> ALL(%s)
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM {tmp} AS t
                USING locked_rows
                WHERE t.id = locked_rows.id
                """,
                (addrs, sorted(set(skip_temp_ids))),
            )
        else:
            cur.execute(
                f"""
                WITH locked_rows AS (
                    SELECT id
                    FROM {tmp}
                    WHERE contract_address = ANY(%s)
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM {tmp} AS t
                USING locked_rows
                WHERE t.id = locked_rows.id
                """,
                (addrs,),
            )
        temp_del = cur.rowcount
    conn.commit()
    return main_del, temp_del


def append_blacklist_env(new_addrs: Set[str], env_path: str = ".env") -> None:
    """Append unique contract addresses to DEFI_BLACKLIST in .env, preserving existing entries."""
    if not new_addrs:
        return

    try:
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        lines = []

    existing: Set[str] = set()
    bl_idx = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("DEFI_BLACKLIST="):
            bl_idx = i
            val = stripped[len("DEFI_BLACKLIST="):]
            for part in val.split(","):
                a = part.strip()
                if a.startswith("0x"):
                    existing.add(a.lower())
            break

    all_addrs = existing | {a.lower() for a in new_addrs}
    new_line  = "DEFI_BLACKLIST=" + ",".join(sorted(all_addrs)) + "\n"

    if bl_idx >= 0:
        lines[bl_idx] = new_line
    else:
        lines.append(new_line)

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    logger.info(
        "Updated .env blacklist with %d contracts: %s",
        len(new_addrs), sorted(new_addrs),
    )


def replace_token_id_placeholder(uri: str, token_id: int) -> str:
    """Replace ERC-1155 {id} with the token ID as 64 lowercase, zero-padded hexadecimal digits."""
    if "{id}" not in uri:
        return uri
    return uri.replace("{id}", format(token_id, "064x"))


def fix_token_id_placeholders(conn, chain_name: str) -> int:
    """Expand unresolved ERC-1155 placeholders in stored token URIs; return the updated row count."""
    tbl = _nft_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, token_id, token_uri FROM {tbl} WHERE token_uri LIKE %s",
            ("%{id}%",),
        )
        rows = cur.fetchall()
    conn.commit()

    if not rows:
        return 0

    updates = []
    for row_id, token_id, token_uri in rows:
        fixed = replace_token_id_placeholder(token_uri, int(token_id))
        if fixed != token_uri:
            updates.append((fixed, row_id))

    if not updates:
        return 0

    with conn.cursor() as cur:
        cur.executemany(
            f"UPDATE {tbl} SET token_uri = %s WHERE id = %s",
            updates,
        )
        updated = cur.rowcount
    conn.commit()
    return updated


def _decode_inline_image(uri: str) -> Optional[str]:
    """Extract image/image_url from an inline data:application/ token URI."""
    comma = uri.find(",")
    if comma == -1:
        return None
    header  = uri[:comma].lower()
    payload = uri[comma + 1:]
    try:
        obj = (
            _json.loads(base64.b64decode(payload + "=="))
            if ";base64" in header
            else _json.loads(unquote(payload))
        )
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    image = obj.get("image") or obj.get("image_url")
    return image.strip() if image and isinstance(image, str) else None


def _normalize_image_url(value: Any) -> Optional[str]:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        for key in (
            "originalUrl",
            "original_url",
            "cachedUrl",
            "cached_url",
            "gateway",
            "url",
            "uri",
            "href",
            "src",
            "pngUrl",
            "png_url",
            "thumbnailUrl",
            "thumbnail_url",
        ):
            candidate = value.get(key)
            if isinstance(candidate, str):
                text = candidate.strip()
                if text:
                    return text
        for key in ("image", "image_url", "imageUrl", "image_uri", "imageUri"):
            candidate = _normalize_image_url(value.get(key))
            if candidate:
                return candidate
    return None


async def fetch_alchemy_batch(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    tokens: List[Tuple[str, int, str]],
    *,
    startup_delay_seconds: float = 0.0,
) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]]:
    """Fetch (token_uri, image_url, contract_name, contract_symbol, metadata) in input order. Failed entries contain five None values."""
    def _to_alchemy_type(std: str) -> str:
        return std.replace("-", "")

    url = (
        f"https://{ALCHEMY_NETWORK}.g.alchemy.com"
        f"/nft/v3/{ALCHEMY_API_KEY}/getNFTMetadataBatch"
    )
    payload = {
        "tokens": [
            {
                "contractAddress": addr,
                "tokenId": str(tid),
                "tokenType": _to_alchemy_type(std),
            }
            for addr, tid, std in tokens
        ],
        "tokenUriTimeoutInMs": 5000,
        "refreshCache": False,
    }
    timeout = aiohttp.ClientTimeout(
        total=METADATA_TIMEOUT, connect=METADATA_CONNECT_TIMEOUT
    )

    max_retries = 3
    data = None
    if startup_delay_seconds > 0:
        await asyncio.sleep(startup_delay_seconds)
    for attempt in range(1, max_retries + 1):
        try:
            async with sem:
                async with session.post(url, json=payload, timeout=timeout) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            break
        except Exception as exc:
            if attempt < max_retries:
                wait = 2 ** (attempt - 1)  # 1s, 2s
                logger.warning(
                    "Alchemy batch attempt %d/%d failed for %d tokens: [%s] %s; retry in %.0fs",
                    attempt, max_retries, len(tokens), type(exc).__name__, exc, wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.info(
                    "Alchemy batch exhausted %d attempts for %d tokens: [%s] %s",
                    max_retries, len(tokens), type(exc).__name__, exc,
                )
                return [(None, None, None, None, None)] * len(tokens)

    nft_list = data if isinstance(data, list) else data.get("nfts", [])
    results: List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]] = []
    for nft in nft_list:
        raw = nft.get("raw") or {}
        if raw.get("error"):
            results.append((None, None, None, None, None))
            continue
        token_uri: Optional[str] = raw.get("tokenUri") or None
        image_url: Optional[str] = None
        contract_name: Optional[str] = None
        contract_symbol: Optional[str] = None
        metadata: Optional[Any] = None
        raw_meta = raw.get("metadata")
        if isinstance(raw_meta, dict):
            image_url = (
                _normalize_image_url(raw_meta.get("image"))
                or _normalize_image_url(raw_meta.get("image_url"))
                or _normalize_image_url(raw_meta.get("imageUrl"))
                or _normalize_image_url(raw_meta.get("image_uri"))
                or _normalize_image_url(raw_meta.get("imageUri"))
                or _normalize_image_url(nft.get("image"))
            )
            metadata = raw_meta
        contract = nft.get("contract")
        if isinstance(contract, dict):
            contract_name = contract.get("name") or None
            contract_symbol = contract.get("symbol") or None
        results.append((token_uri, image_url, contract_name, contract_symbol, metadata))

    while len(results) < len(tokens):
        results.append((None, None, None, None, None))
    return results


async def fetch_token_uri(
    w3: AsyncWeb3,
    sem: asyncio.Semaphore,
    contract_address: str,
    token_id: int,
    standard: str,
) -> Optional[str]:
    """Read tokenURI (ERC-721) or uri (ERC-1155) using eth_call."""
    async with sem:
        try:
            addr = AsyncWeb3.to_checksum_address(contract_address)
            if standard == "ERC-721":
                contract = w3.eth.contract(address=addr, abi=ERC721_ABI)
                uri = await contract.functions.tokenURI(token_id).call()
            else:
                contract = w3.eth.contract(address=addr, abi=ERC1155_ABI)
                uri = await contract.functions.uri(token_id).call()
            if uri and "{id}" in uri:
                uri = uri.replace("{id}", str(token_id))
            return uri
        except Exception:
            return None


def _build_token_uri_call_data(token_id: int, standard: str) -> str:
    selector = (
        _ERC721_TOKEN_URI_SELECTOR if standard == "ERC-721" else _ERC1155_URI_SELECTOR
    )
    return "0x" + selector + format(token_id, "064x")


def _decode_abi_string_result(result: object) -> Optional[str]:
    if not isinstance(result, str) or not result.startswith("0x"):
        return None
    try:
        raw = bytes.fromhex(result[2:])
    except ValueError:
        return None
    if len(raw) < 64:
        return None

    offset = int.from_bytes(raw[:32], "big")
    if offset + 32 > len(raw):
        return None

    strlen = int.from_bytes(raw[offset: offset + 32], "big")
    start = offset + 32
    end = start + strlen
    if end > len(raw):
        return None

    try:
        return raw[start:end].decode("utf-8")
    except UnicodeDecodeError:
        return None


async def fetch_token_uri_batch(
    session: aiohttp.ClientSession,
    rpc_url: str,
    sem: asyncio.Semaphore,
    tokens: List[Tuple[str, int, str]],
    *,
    startup_delay_seconds: float = 0.0,
) -> List[Optional[str]]:
    """Read token URIs using a JSON-RPC eth_call batch."""
    if not tokens:
        return []

    payload = [
        {
            "jsonrpc": "2.0",
            "id": idx,
            "method": "eth_call",
            "params": [
                {
                    "to": contract_address,
                    "data": _build_token_uri_call_data(token_id, standard),
                },
                "latest",
            ],
        }
        for idx, (contract_address, token_id, standard) in enumerate(tokens)
    ]
    timeout = aiohttp.ClientTimeout(
        total=METADATA_TIMEOUT, connect=METADATA_CONNECT_TIMEOUT
    )

    max_retries = 3
    data = None
    if startup_delay_seconds > 0:
        await asyncio.sleep(startup_delay_seconds)
    for attempt in range(1, max_retries + 1):
        try:
            async with sem:
                async with session.post(rpc_url, json=payload, timeout=timeout) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                if not isinstance(data, list):
                    raise ValueError("RPC batch response is not a list")
            break
        except Exception as exc:
            if attempt < max_retries:
                wait = 2 ** (attempt - 1)
                logger.warning(
                    "RPC batch attempt %d/%d failed for %d tokens: [%s] %s; retry in %.0fs",
                    attempt, max_retries, len(tokens), type(exc).__name__, exc, wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.info(
                    "RPC batch exhausted %d attempts for %d tokens: [%s] %s",
                    max_retries, len(tokens), type(exc).__name__, exc,
                )
                return [None] * len(tokens)

    results: List[Optional[str]] = [None] * len(tokens)
    for item in data:
        if not isinstance(item, dict):
            continue
        idx = item.get("id")
        if not isinstance(idx, int) or idx < 0 or idx >= len(tokens):
            continue
        if item.get("error"):
            continue

        uri = _decode_abi_string_result(item.get("result"))
        if uri and "{id}" in uri:
            uri = uri.replace("{id}", str(tokens[idx][1]))
        results[idx] = uri
    return results


def _to_bytes(data) -> bytes:
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    s = str(data)
    return bytes.fromhex(s[2:] if s.startswith("0x") else s)


def _decode_single(raw: bytes) -> int:
    return int.from_bytes(raw[0:32], "big")


def _decode_batch(raw: bytes) -> List[int]:
    if len(raw) < 64:
        return []
    ids_offset = int.from_bytes(raw[0:32], "big")
    if ids_offset + 32 > len(raw):
        return []
    ids_len = int.from_bytes(raw[ids_offset: ids_offset + 32], "big")
    result: List[int] = []
    for i in range(ids_len):
        start = ids_offset + 32 + i * 32
        if start + 32 > len(raw):
            break
        result.append(int.from_bytes(raw[start: start + 32], "big"))
    return result


def extract_nfts(log) -> List[Tuple[str, int, str]]:
    """Decode an event into (lowercase contract address, token ID, standard) records."""
    topics = log["topics"]
    if not topics or len(topics) < 3:
        return []
    topic0  = bytes(topics[0])
    address = log["address"].lower()
    results: List[Tuple[str, int, str]] = []

    if topic0 == _ERC721_TRANSFER_B and len(topics) == 4:
        results.append((address, int.from_bytes(topics[3], "big"), "ERC-721"))
    elif topic0 == _ERC1155_SINGLE_B and len(topics) == 4:
        raw = _to_bytes(log["data"])
        if len(raw) >= 32:
            results.append((address, _decode_single(raw), "ERC-1155"))
    elif topic0 == _ERC1155_BATCH_B and len(topics) == 4:
        raw = _to_bytes(log["data"])
        for tid in _decode_batch(raw):
            results.append((address, tid, "ERC-1155"))
    return results


_TOPIC_LABEL = {
    ERC721_TRANSFER_TOPIC: "ERC-721 Transfer    ",
    ERC1155_SINGLE_TOPIC:  "ERC-1155 Single     ",
    ERC1155_BATCH_TOPIC:   "ERC-1155 Batch      ",
}


def _parse_raw_log(raw: dict) -> dict:
    """Decode JSON-RPC hex topics, data, and block number for extract_nfts."""
    def h2b(h: str) -> bytes:
        h = h or "0x"
        return bytes.fromhex(h[2:] if h.startswith("0x") else h)

    bn = raw.get("blockNumber", "0x0")
    return {
        "address":     raw.get("address", "").lower(),
        "topics":      [h2b(t) for t in raw.get("topics", [])],
        "data":        h2b(raw.get("data", "0x")),
        "blockNumber": int(bn, 16) if isinstance(bn, str) else int(bn),
    }


async def _fetch_logs_one_topic_http(
    session: aiohttp.ClientSession,
    rpc_url: str,
    from_block: int,
    to_block: int,
    topic: str,
    label: str,
) -> list:
    """Fetch one eth_getLogs topic through the shared aiohttp connection pool."""
    payload = {
        "jsonrpc": "2.0",
        "method":  "eth_getLogs",
        "params":  [{"fromBlock": hex(from_block), "toBlock": hex(to_block), "topics": [topic]}],
        "id":      1,
    }
    timeout = aiohttp.ClientTimeout(total=120)
    try:
        async with session.post(rpc_url, json=payload, timeout=timeout) as resp:
            resp.raise_for_status()
            body = await resp.json(content_type=None)
    except Exception as exc:
        logger.warning("    %s → eth_getLogs failed: %s", label, exc)
        return []

    if "error" in body:
        logger.warning("    %s → RPC error: %s", label, body["error"])
        return []

    raw_logs = body.get("result") or []
    logs = [_parse_raw_log(r) for r in raw_logs]

    if topic == ERC721_TRANSFER_TOPIC:
        nft_count = sum(1 for lg in logs if len(lg["topics"]) == 4)
        logger.info(
            "    %s → %d logs (NFT: %d, ERC-20: %d)",
            label, len(logs), nft_count, len(logs) - nft_count,
        )
    else:
        logger.info("    %s → %d logs", label, len(logs))
    return logs


async def fetch_logs_http(
    session: aiohttp.ClientSession,
    rpc_url: str,
    from_block: int,
    to_block: int,
    *,
    startup_delay_seconds: float = 0.0,
) -> list:
    """Fetch the three NFT event topics concurrently through aiohttp."""
    if startup_delay_seconds > 0:
        await asyncio.sleep(startup_delay_seconds)
    results = await asyncio.gather(*[
        _fetch_logs_one_topic_http(
            session, rpc_url, from_block, to_block, topic, _TOPIC_LABEL[topic]
        )
        for topic in ALL_TOPICS
    ])
    if REQUEST_DELAY > 0:
        await asyncio.sleep(REQUEST_DELAY)
    return [log for logs in results for log in logs]
