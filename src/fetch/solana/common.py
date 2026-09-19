#!/usr/bin/env python3
"""Shared PostgreSQL and Helius helpers for Metaplex account discovery and DAS metadata collection."""

import asyncio
import base64
import json as _json
import logging
import os
import re
import struct
import sys
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


TOKEN_METADATA_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
SPL_TOKEN_PROGRAM_ID      = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SYSTEM_PROGRAM_ID         = "11111111111111111111111111111111"


CHAIN_NAME = os.getenv("CHAIN_NAME", "solana")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")


HELIUS_API_KEY    = os.getenv("HELIUS_API_KEY", "")

HELIUS_BATCH_SIZE = int(os.getenv("HELIUS_BATCH_SIZE", "1000"))

METADATA_TIMEOUT         = int(os.getenv("METADATA_TIMEOUT", "30"))
METADATA_CONNECT_TIMEOUT = int(os.getenv("METADATA_CONNECT_TIMEOUT", "30"))


CONCURRENT_HELIUS = int(os.getenv("CONCURRENT_HELIUS", "5"))
FETCH_IDLE_WAIT   = int(os.getenv("FETCH_IDLE_WAIT", "30"))
FETCH_CLAIM_BATCH_SIZE = int(os.getenv("FETCH_CLAIM_BATCH_SIZE", "5000"))
CLAIM_RETRY_AFTER_SECONDS = int(os.getenv("CLAIM_RETRY_AFTER_SECONDS", "1800"))
REQUEST_STARTUP_STAGGER_SECONDS = max(
    0.0,
    float(os.getenv("REQUEST_STARTUP_STAGGER_SECONDS", "0")),
)
DB_INSERT_PAGE_SIZE = int(os.getenv("DB_INSERT_PAGE_SIZE", "1000"))


HELIUS_RPC_URL = (
    f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY', '')}"
    if os.getenv("HELIUS_API_KEY") else ""
)

GPA_PAGE_SIZE  = int(os.getenv("GPA_PAGE_SIZE", "1000"))


GPA_SINCE_SLOT = int(os.getenv("GPA_SINCE_SLOT", "0"))


_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def b58encode(data: bytes) -> str:
    """Encode bytes as Base58, preserving leading zero bytes."""
    n = int.from_bytes(data, "big")
    result = []
    while n > 0:
        n, r = divmod(n, 58)
        result.append(_B58_ALPHABET[r])
    pad = len(data) - len(data.lstrip(b"\x00"))
    return "1" * pad + bytes(reversed(result)).decode()


def _read_string(data: bytes, pos: int) -> Tuple[str, int]:
    """Read a Borsh UTF-8 string with a little-endian u32 length prefix."""
    if pos + 4 > len(data):
        return "", pos + 4
    length = struct.unpack_from("<I", data, pos)[0]
    pos += 4
    raw = data[pos: pos + length]
    pos += length
    return raw.decode("utf-8", errors="replace").rstrip("\x00").strip(), pos


def parse_metadata_account(data: bytes) -> Optional[Dict]:
    """Decode MetadataV1 Borsh fields into mint, name, symbol, URI, and optional collection; return None on failure."""
    try:
        if not data or len(data) < 70:
            return None
        pos = 0

        key = data[pos]; pos += 1
        if key != 4:  # MetadataV1 only.
            return None

        pos += 32  # skip update_authority
        mint_bytes = data[pos: pos + 32]; pos += 32
        mint_b58 = b58encode(bytes(mint_bytes))

        name,   pos = _read_string(data, pos)
        symbol, pos = _read_string(data, pos)
        uri,    pos = _read_string(data, pos)

        if pos + 2 > len(data):
            return {"mint": mint_b58, "name": name, "symbol": symbol, "uri": uri, "collection": None}
        pos += 2  # seller_fee_basis_points

        # Data.creators layout: option u8 + length u32 + creator[34].
        if pos < len(data):
            creators_present = data[pos]
            pos += 1
            if creators_present and pos + 4 <= len(data):
                creator_count = struct.unpack_from("<I", data, pos)[0]
                pos += 4 + creator_count * 34

        # primary_sale_happened + is_mutable
        if pos < len(data):
            pos += 1
        if pos < len(data):
            pos += 1

        # edition_nonce: option u8 + value u8
        if pos < len(data):
            edition_nonce_present = data[pos]
            pos += 1
            if edition_nonce_present and pos < len(data):
                pos += 1

        # token_standard: option u8 + enum u8
        if pos < len(data):
            token_standard_present = data[pos]
            pos += 1
            if token_standard_present and pos < len(data):
                pos += 1

        collection = None
        if pos < len(data):
            collection_present = data[pos]
            pos += 1
            if collection_present and pos + 33 <= len(data):
                pos += 1  # verified bool
                collection = b58encode(bytes(data[pos: pos + 32]))

        return {
            "mint": mint_b58,
            "name": name,
            "symbol": symbol,
            "uri": uri,
            "collection": collection,
        }
    except Exception:
        return None


def _normalize_solana_address(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if 32 <= len(value) <= 44:
        return value
    return None


def _extract_collection_address(asset: Dict) -> Optional[str]:
    grouping = asset.get("grouping")
    if isinstance(grouping, list):
        for group in grouping:
            if not isinstance(group, dict):
                continue
            if group.get("group_key") != "collection":
                continue
            collection = _normalize_solana_address(group.get("group_value"))
            if collection:
                return collection

    content = asset.get("content") if isinstance(asset.get("content"), dict) else {}
    metadata = content.get("metadata") if isinstance(content.get("metadata"), dict) else {}
    collection_value = metadata.get("collection")
    if isinstance(collection_value, dict):
        for key in ("key", "address", "id", "mint"):
            collection = _normalize_solana_address(collection_value.get(key))
            if collection:
                return collection
    return _normalize_solana_address(collection_value)


def _nft_table_name(chain_name: str) -> str:
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower()) or "default"
    return f"nft_assets_{safe}"


def _temp_table_name(chain_name: str) -> str:
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower()) or "default"
    return f"temp_{safe}"


def _ensure_unique_constraint_sql(
    table_name: str,
    constraint_name: str,
    columns: Tuple[str, ...],
) -> str:
    column_list = ", ".join(columns)
    column_array = "ARRAY[" + ", ".join(f"'{column}'" for column in columns) + "]"
    return f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint c
                    JOIN pg_class t ON t.oid = c.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = current_schema()
                      AND t.relname = '{table_name}'
                      AND c.contype = 'u'
                      AND (
                          SELECT array_agg(a.attname::text ORDER BY k.ord)
                          FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
                          JOIN pg_attribute a
                            ON a.attrelid = c.conrelid
                           AND a.attnum = k.attnum
                      ) = {column_array}
                ) THEN
                    ALTER TABLE {table_name}
                        ADD CONSTRAINT {constraint_name}
                        UNIQUE ({column_list});
                END IF;
            END $$;
        """


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
        # Store the collection as contract_address and the mint as token_id.
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                id               BIGSERIAL    PRIMARY KEY,
                contract_address VARCHAR(44)  NOT NULL,
                token_id         VARCHAR(44)  NOT NULL,
                token_uri        TEXT,
                image_uri        TEXT,
                name             TEXT,
                symbol           TEXT,
                metadata         JSONB,
                token_standard   VARCHAR(20),
                first_seen_block BIGINT,
                created_at       TIMESTAMPTZ  DEFAULT NOW(),
                CONSTRAINT {tbl}_contract_token_key UNIQUE (contract_address, token_id)
            )
        """)
        # Migrate existing tables to the shared EVM/Solana column semantics.
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS name TEXT")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS symbol TEXT")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS metadata JSONB")
        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN name TYPE TEXT")
        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN symbol TYPE TEXT")
        cur.execute(
            f"ALTER TABLE {tbl} ALTER COLUMN token_id TYPE VARCHAR(44) USING token_id::text"
        )
        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN token_id DROP DEFAULT")
        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN token_standard TYPE VARCHAR(20)")
        cur.execute(f"ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS {tbl}_contract_address_key")
        cur.execute("DROP INDEX IF EXISTS idx_sol_nft_contract_token")
        cur.execute(
            _ensure_unique_constraint_sql(
                tbl,
                f"{tbl}_contract_token_key",
                ("contract_address", "token_id"),
            )
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sol_nft_contract"
            f" ON {tbl} (contract_address)"
        )

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tmp} (
                id               BIGSERIAL   PRIMARY KEY,
                mint_address     VARCHAR(44) NOT NULL,
                token_standard   VARCHAR(20),
                first_seen_block BIGINT,
                claimed_at       TIMESTAMPTZ,
                claimed_by       TEXT,
                created_at       TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT {tmp}_mint_key UNIQUE (mint_address)
            )
        """)
        cur.execute(f"ALTER TABLE {tmp} ADD COLUMN IF NOT EXISTS mint_address VARCHAR(44)")
        cur.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = '{tmp}'
                      AND column_name = 'contract_address'
                ) THEN
                    UPDATE {tmp}
                    SET mint_address = contract_address
                    WHERE mint_address IS NULL
                      AND contract_address IS NOT NULL;
                END IF;
            END $$;
            """
        )
        cur.execute(f"DELETE FROM {tmp} WHERE mint_address IS NULL")
        cur.execute(f"ALTER TABLE {tmp} ALTER COLUMN mint_address SET NOT NULL")
        cur.execute(f"ALTER TABLE {tmp} ALTER COLUMN token_standard TYPE VARCHAR(20)")
        cur.execute(f"ALTER TABLE {tmp} DROP CONSTRAINT IF EXISTS {tmp}_contract_address_key")
        cur.execute(f"ALTER TABLE {tmp} DROP CONSTRAINT IF EXISTS {tmp}_contract_token_key")
        cur.execute("DROP INDEX IF EXISTS idx_sol_temp_contract_token")
        cur.execute(
            _ensure_unique_constraint_sql(
                tmp,
                f"{tmp}_mint_key",
                ("mint_address",),
            )
        )
        cur.execute(f"ALTER TABLE {tmp} DROP COLUMN IF EXISTS contract_address")
        cur.execute(f"ALTER TABLE {tmp} DROP COLUMN IF EXISTS token_id")
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sol_temp_mint"
            f" ON {tmp} (mint_address)"
        )
        cur.execute(f"ALTER TABLE {tmp} ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ")
        cur.execute(f"ALTER TABLE {tmp} ADD COLUMN IF NOT EXISTS claimed_by TEXT")
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sol_temp_claimed_at"
            f" ON {tmp} (claimed_at, id)"
        )

        cur.execute("""
            CREATE TABLE IF NOT EXISTS scan_progress_gpa (
                chain_name      VARCHAR(50)  PRIMARY KEY,
                pagination_key  TEXT,
                since_slot      BIGINT       NOT NULL DEFAULT 0,
                total_pages     BIGINT       NOT NULL DEFAULT 0,
                updated_at      TIMESTAMPTZ  DEFAULT NOW()
            )
        """)
    conn.commit()
    logger.info("Database initialized: main=%s staging=%s", tbl, tmp)


def ensure_temp_table_claim_columns(conn, chain_name: str) -> None:
    """Add claim columns for concurrent metadata workers if absent."""
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {tmp} ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ")
        cur.execute(f"ALTER TABLE {tmp} ADD COLUMN IF NOT EXISTS claimed_by TEXT")
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sol_temp_claimed_at"
            f" ON {tmp} (claimed_at, id)"
        )
    conn.commit()
    logger.info("Staging claim columns ready: %s (claimed_at, claimed_by)", tmp)


def _is_missing_claim_column_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if getattr(exc, "pgcode", None) != "42703" and "does not exist" not in message:
        return False
    return "claimed_at" in message or "claimed_by" in message


def get_gpa_progress(conn, chain_name: str) -> Tuple[Optional[str], int, int]:
    """Return (pagination_key, since_slot, total_pages). A None cursor starts a new pass; slot zero denotes a full scan."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pagination_key, since_slot, total_pages"
            " FROM scan_progress_gpa WHERE chain_name = %s",
            (chain_name,),
        )
        row = cur.fetchone()
    conn.commit()
    if row:
        return row[0], row[1] or 0, row[2] or 0
    return None, 0, 0


def save_gpa_progress(
    conn,
    chain_name: str,
    pagination_key: Optional[str],
    since_slot: int,
    total_pages: int,
) -> None:
    """Persist the next cursor and page count. Keep changedSinceSlot while paging; save the observed latest slot after completion."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scan_progress_gpa
                (chain_name, pagination_key, since_slot, total_pages)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chain_name) DO UPDATE
                SET pagination_key = EXCLUDED.pagination_key,
                    since_slot     = EXCLUDED.since_slot,
                    total_pages    = EXCLUDED.total_pages,
                    updated_at     = NOW()
            """,
            (chain_name, pagination_key, since_slot, total_pages),
        )
    conn.commit()


def batch_insert_temp(conn, chain_name: str, records: List[Tuple[str, str, int]]) -> int:
    """Insert (mint_address, token_standard, first_seen_block) staging records."""
    if not records:
        return 0
    tmp = _temp_table_name(chain_name)
    for rec in records:
        if len(rec) != 3:
            raise ValueError(
                "Solana temp records must be (mint_address, token_standard, first_seen_block)"
            )

    with conn.cursor() as cur:
        placeholders = ", ".join(["(%s, %s, %s)"] * len(records))
        flat = [item for rec in records for item in rec]
        cur.execute(
            f"""
            INSERT INTO {tmp} (mint_address, token_standard, first_seen_block)
            VALUES {placeholders}
            ON CONFLICT (mint_address) DO NOTHING
            """,
            flat,
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def load_pending_nfts(conn, chain_name: str, limit: int = 5000) -> List[Tuple]:
    """Read pending staging rows, including their database IDs."""
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, mint_address, token_standard, first_seen_block
            FROM {tmp}
            ORDER BY id
            LIMIT %s
            """,
            (limit,),
        )
        result = [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]
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
                RETURNING t.id, t.mint_address, t.token_standard, t.first_seen_block
                """,
                params,
            )
            return [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]

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

    def _clean(v: Any) -> Any:
        if isinstance(v, str):
            v = v.replace("\x00", "")

            return v.encode("utf-8", errors="ignore").decode("utf-8")
        if isinstance(v, dict):
            return {_clean(k): _clean(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_clean(item) for item in v]
        if isinstance(v, tuple):
            return tuple(_clean(item) for item in v)
        return v

    normalized_records = []
    for rec in records:
        cleaned = tuple(_clean(v) for v in rec)
        if len(cleaned) == 8:
            cleaned = cleaned[:6] + (None,) + cleaned[6:]
        metadata = cleaned[6]
        if metadata is not None and not isinstance(metadata, str):
            metadata = _json.dumps(metadata, ensure_ascii=False)
        normalized_records.append(cleaned[:6] + (metadata,) + cleaned[7:])

    sql = f"""
            INSERT INTO {tbl}
                (contract_address, token_id, token_uri, image_uri,
                 name, symbol, metadata, token_standard, first_seen_block)
            VALUES %s
            ON CONFLICT (contract_address, token_id) DO UPDATE SET
                token_uri        = COALESCE(EXCLUDED.token_uri,  {tbl}.token_uri),
                image_uri        = COALESCE(EXCLUDED.image_uri,  {tbl}.image_uri),
                name             = COALESCE(EXCLUDED.name,       {tbl}.name),
                symbol           = COALESCE(EXCLUDED.symbol,     {tbl}.symbol),
                metadata         = COALESCE(EXCLUDED.metadata,   {tbl}.metadata),
                token_standard   = COALESCE(EXCLUDED.token_standard, {tbl}.token_standard)
            """

    inserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(normalized_records), DB_INSERT_PAGE_SIZE):
            page = normalized_records[start: start + DB_INSERT_PAGE_SIZE]
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
    if not ids:
        return 0
    tmp = _temp_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {tmp} WHERE id = ANY(%s)", (ids,))
        deleted = cur.rowcount
    conn.commit()
    return deleted


def release_temp_claims(conn, chain_name: str, ids: List[int], worker_id: str) -> int:
    """Release this worker's unfinished claims for retry."""
    if not ids:
        return 0
    tmp = _temp_table_name(chain_name)
    ordered_ids = sorted(set(ids))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {tmp}
            SET claimed_at = NULL, claimed_by = NULL
            WHERE id = ANY(%s) AND claimed_by = %s
            """,
            (ordered_ids, worker_id),
        )
        released = cur.rowcount
    conn.commit()
    return released


async def get_latest_slot(
    session: aiohttp.ClientSession,
    rpc_url: str,
) -> int:
    """Fetch the latest confirmed slot."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": "finalized"}],
    }
    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with session.post(rpc_url, json=payload, timeout=timeout) as resp:
            resp.raise_for_status()
            body = await resp.json(content_type=None)
            return int(body.get("result", 0))
    except Exception as exc:
        logger.error("Failed to fetch latest slot: %s", exc)
        return 0


async def fetch_helius_metadata_batch(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    mints: List[str],
) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]]:
    """Fetch DAS getAssetBatch metadata in bounded batches and align responses by mint ID."""
    if not HELIUS_API_KEY:
        return [(None, None, None, None, None, None)] * len(mints)

    helius_url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    timeout = aiohttp.ClientTimeout(
        total=METADATA_TIMEOUT, connect=METADATA_CONNECT_TIMEOUT
    )

    async def _fetch_chunk(
        ci: int, chunk: List[str]
    ) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]]:
        payload = {
            "jsonrpc": "2.0",
            "id": f"helius-batch-{ci}",
            "method": "getAssetBatch",
            "params": {
                "ids": chunk,
                "options": {
                    "showUnverifiedCollections": True,
                    "showCollectionMetadata": True
                },
            },
        }
        max_retries = 4
        body: Optional[Dict] = None

        for attempt in range(1, max_retries + 1):
            try:
                # Release the semaphore during retry backoff.
                async with sem:
                    async with session.post(
                        helius_url, json=payload, timeout=timeout
                    ) as resp:
                        if resp.status == 429:
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=429
                            )
                        resp.raise_for_status()
                        body = await resp.json(content_type=None)
                break
            except aiohttp.ClientResponseError as exc:
                wait = min(2 ** attempt, 32) if exc.status == 429 else 2 ** (attempt - 1)
                if attempt < max_retries:
                    logger.debug(
                        "Helius getAssetBatch attempt %d/%d failed (HTTP %d); retry in %.0fs",
                        attempt, max_retries, exc.status, wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.warning("Helius getAssetBatch exhausted retries (%d mints)", len(chunk))
                    return [(None, None, None, None, None, None)] * len(chunk)
            except Exception as exc:
                if attempt < max_retries:
                    await asyncio.sleep(2 ** (attempt - 1))
                else:
                    logger.warning("Helius getAssetBatch request failed: %s", exc)
                    return [(None, None, None, None, None, None)] * len(chunk)

        if body is None:
            return [(None, None, None, None, None, None)] * len(chunk)

        assets = body.get("result") or []

        # DAS responses may be reordered; align by asset ID.
        asset_map: Dict[
            str,
            Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]],
        ] = {}
        for asset in (assets if isinstance(assets, list) else []):
            mint = asset.get("id")
            if not mint:
                continue

            content   = asset.get("content") or {}
            token_uri = content.get("json_uri") or None
            meta_raw  = content.get("metadata")
            meta      = meta_raw if isinstance(meta_raw, dict) else {}
            collection_address = _extract_collection_address(asset)

            name:   Optional[str] = meta.get("name")   or None
            symbol: Optional[str] = meta.get("symbol") or None

            image_url: Optional[str] = None
            links = content.get("links") or {}
            image_url = links.get("image") or None

            if not image_url:
                image_url = meta.get("image") or None

            if not image_url:
                for f in (content.get("files") or []):
                    if isinstance(f, dict) and (f.get("mime") or "").startswith("image/"):
                        image_url = f.get("uri") or f.get("cdn_uri") or None
                        break

            asset_map[mint] = (
                collection_address,
                token_uri or None,
                image_url or None,
                name,
                symbol,
                meta_raw if isinstance(meta_raw, dict) else None,
            )

        return [asset_map.get(m, (None, None, None, None, None, None)) for m in chunk]

    chunk_size = HELIUS_BATCH_SIZE
    chunks = [mints[i: i + chunk_size] for i in range(0, len(mints), chunk_size)]
    chunk_results = await asyncio.gather(*[
        _fetch_chunk(ci, chunk) for ci, chunk in enumerate(chunks)
    ])
    return [item for chunk in chunk_results for item in chunk]


async def fetch_metadata_batch(
    session: aiohttp.ClientSession,
    helius_sem: asyncio.Semaphore,
    mints: List[str],
) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]]:
    """Return (collection, token_uri, image_url, name, symbol, metadata) in mint order using Helius only. Missing fields remain unresolved."""
    _empty: Tuple[
        Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]
    ] = (
        None, None, None, None, None, None
    )
    results: List[
        Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]
    ] = [_empty] * len(mints)

    if HELIUS_API_KEY:
        results = list(await fetch_helius_metadata_batch(session, helius_sem, mints))

    return results


class GpaPageFetchError(RuntimeError):
    """Raised when a GPA page cannot be fetched without corrupting scan progress."""


async def fetch_gpa_page(
    session: aiohttp.ClientSession,
    helius_rpc_url: str,
    page_size: int,
    pagination_key: Optional[str] = None,
    changed_since_slot: Optional[int] = None,
) -> Tuple[List[str], Optional[str]]:
    """Fetch MetadataV1 mint addresses and the next GPA cursor (None on the last page). Read the 32-byte mint at offset 33; optionally filter by changedSinceSlot."""
    # Base58 "5" encodes byte 0x04 (MetadataV1).
    params: Dict = {
        "encoding":  "base64",
        "filters":   [{"memcmp": {"offset": 0, "bytes": "5"}}],
        "dataSlice": {"offset": 33, "length": 32},
        "limit":     page_size,
    }
    if pagination_key:
        params["paginationKey"] = pagination_key
    if changed_since_slot:
        params["changedSinceSlot"] = changed_since_slot

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccountsV2",
        "params": [TOKEN_METADATA_PROGRAM_ID, params],
    }

    timeout = aiohttp.ClientTimeout(total=90)
    body: Optional[Dict] = None

    for attempt in range(1, 6):
        try:
            async with session.post(helius_rpc_url, json=payload, timeout=timeout) as resp:
                if resp.status == 429:
                    wait = min(2 ** attempt, 60)
                    logger.warning("GPA rate limited (attempt %d); retry in %ds", attempt, wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                body = await resp.json(content_type=None)
            break
        except asyncio.TimeoutError:
            wait = min(2 ** attempt, 30)
            logger.warning("GPA timeout (attempt %d); retry in %ds", attempt, wait)
            await asyncio.sleep(wait)
        except Exception as exc:
            wait = min(2 ** attempt, 30)
            logger.warning("GPA request failed (attempt %d): %s; retry in %ds", attempt, exc, wait)
            await asyncio.sleep(wait)

    if body is None:
        message = f"GPA exhausted retries; paginationKey={pagination_key!r}"
        logger.error(message)
        raise GpaPageFetchError(message)

    if body.get("error"):
        message = f"GPA RPC error: {body['error']}"
        logger.error(message)
        raise GpaPageFetchError(message)

    result   = body.get("result") or {}
    accounts = result.get("accounts") or []
    next_key = result.get("paginationKey")  # None marks the last page.

    mints: List[str] = []
    for acc in accounts:
        data_field = (acc.get("account") or {}).get("data")
        if not data_field:
            continue

        raw_b64 = data_field[0] if isinstance(data_field, list) else data_field
        try:
            raw = base64.b64decode(raw_b64)
        except Exception:
            continue
        if len(raw) != 32:
            continue
        # Exclude the all-zero System Program address.
        if raw == b"\x00" * 32:
            continue
        mint = b58encode(raw)
        # A Base58 public key must contain 32 to 44 characters.
        if 32 <= len(mint) <= 44:
            mints.append(mint)

    return mints, next_key
