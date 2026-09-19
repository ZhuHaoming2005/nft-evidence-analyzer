#!/usr/bin/env python3
"""Refetch metadata for missing image URIs in nft_assets_{chain}. Select the chain with --chain."""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

import aiohttp
import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    def load_dotenv() -> bool:
        return False


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nft_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

CHAIN_NAME = os.getenv("CHAIN_NAME", "ethereum")

METADATA_TIMEOUT = int(os.getenv("METADATA_TIMEOUT", "15"))
METADATA_CONNECT_TIMEOUT = int(os.getenv("METADATA_CONNECT_TIMEOUT", "15"))
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", "20"))

IPFS_GATEWAYS: List[str] = [
    gateway.strip()
    for gateway in os.getenv(
        "IPFS_GATEWAYS",
        "https://gateway.pinata.cloud/ipfs,https://dweb.link/ipfs,https://ipfs.io/ipfs",
    ).split(",")
    if gateway.strip()
]

_PINATA_GATEWAY_TOKEN = os.getenv("PINATA_GATEWAY_TOKEN", "").strip()
IPFS_GATEWAY_HEADERS: Dict[str, Dict[str, str]] = (
    {IPFS_GATEWAYS[0]: {"x-pinata-gateway-token": _PINATA_GATEWAY_TOKEN}}
    if IPFS_GATEWAYS and _PINATA_GATEWAY_TOKEN
    else {}
)

ARWEAVE_GATEWAY = "https://arweave.net"
BATCH_SIZE = int(os.getenv("RETRY_BATCH_SIZE", "1000"))


def _nft_table_name(chain_name: str) -> str:
    safe = re.sub(r"[^a-z0-9_]", "", chain_name.lower())
    return f"nft_assets_{safe}" if safe else "nft_assets_default"


def _metadata_url(token_uri: str, ipfs_gateway: Optional[str] = None) -> str:
    s = token_uri.strip()
    if s.startswith("ipfs://ipfs/"):
        gw = (ipfs_gateway or IPFS_GATEWAYS[0]).rstrip("/")
        return gw + "/" + s[12:].lstrip("/")
    if s.startswith("ipfs://"):
        gw = (ipfs_gateway or IPFS_GATEWAYS[0]).rstrip("/")
        return gw + "/" + s[7:].lstrip("/")
    if s.startswith("ar://"):
        gw = ARWEAVE_GATEWAY.rstrip("/")
        return gw + "/" + s[5:].lstrip("/")
    return s


def _candidate_gateways(token_uri: str) -> List[Optional[str]]:
    if token_uri.strip().startswith("ipfs://"):
        return IPFS_GATEWAYS
    return [None]


def _request_headers(gateway: Optional[str], url: str) -> Dict[str, str]:
    if not gateway:
        return {}
    gateway_base = gateway.rstrip("/")
    if gateway_base in IPFS_GATEWAY_HEADERS and url.startswith(gateway_base):
        return dict(IPFS_GATEWAY_HEADERS[gateway_base])
    return {}


async def _fetch_json(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]):
    async with session.get(url, headers=headers or None, allow_redirects=True) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


async def fetch_metadata_for_token_uri_async(
    token_uri: str,
    session: aiohttp.ClientSession,
) -> Optional[Dict[str, Any]]:
    if not token_uri or token_uri.startswith("data:application/"):
        return None

    last_exc: Optional[Exception] = None
    gateways = _candidate_gateways(token_uri)

    for idx, gateway in enumerate(gateways):
        url = _metadata_url(token_uri, gateway)
        if not url.startswith("http"):
            continue

        headers = _request_headers(gateway, url)
        try:
            data = await _fetch_json(session, url, headers)
        except Exception as exc:
            last_exc = exc
            logger.info(
                "[retry image_uri=NULL] token_uri=%s uri=%s gateway[%d]=%s reason: HTTP request failed [%s] %s",
                token_uri[:80],
                url,
                idx,
                gateway or "<direct>",
                type(exc).__name__,
                str(exc) or repr(exc),
            )
            continue

        if not isinstance(data, dict):
            logger.info(
                "[retry image_uri=NULL] token_uri=%s gateway[%d]=%s reason: metadata is not a JSON object",
                token_uri[:80],
                idx,
                gateway or "<direct>",
            )
            continue

        return data

    if last_exc is not None:
        logger.debug(
            "Metadata retries exhausted token_uri=%s; last error [%s] %s",
            token_uri[:80],
            type(last_exc).__name__,
            str(last_exc) or repr(last_exc),
        )
    return None


async def fetch_image_uri_for_token_uri_async(
    token_uri: str,
    session: aiohttp.ClientSession,
) -> Optional[str]:
    metadata = await fetch_metadata_for_token_uri_async(token_uri, session)
    if not metadata:
        return None

    image = metadata.get("image") or metadata.get("image_url")
    if not image or not isinstance(image, str):
        logger.info(
            "[retry image_uri=NULL] token_uri=%s reason: JSON lacks image/image_url",
            token_uri[:100],
        )
        return None
    return image.strip()


async def fetch_metadata_records_for_token_uris(
    token_uris: List[str],
    concurrency: int = CONCURRENT_REQUESTS,
) -> Dict[str, Optional[Dict[str, Any]]]:
    if not token_uris:
        return {}

    timeout = aiohttp.ClientTimeout(
        total=METADATA_CONNECT_TIMEOUT + METADATA_TIMEOUT,
        connect=METADATA_CONNECT_TIMEOUT,
        sock_connect=METADATA_CONNECT_TIMEOUT,
        sock_read=METADATA_TIMEOUT,
    )
    connector = aiohttp.TCPConnector(limit=max(1, concurrency))
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async def fetch_one(token_uri: str):
            async with semaphore:
                metadata = await fetch_metadata_for_token_uri_async(token_uri, session)
                return token_uri, metadata

        pairs = await asyncio.gather(*(fetch_one(token_uri) for token_uri in token_uris))

    return {token_uri: metadata for token_uri, metadata in pairs}


async def fetch_image_uris_for_token_uris(
    token_uris: List[str],
    concurrency: int = CONCURRENT_REQUESTS,
) -> Dict[str, Optional[str]]:
    metadata_records = await fetch_metadata_records_for_token_uris(
        token_uris,
        concurrency=concurrency,
    )
    image_uris: Dict[str, Optional[str]] = {}
    for token_uri, metadata in metadata_records.items():
        image = None
        if isinstance(metadata, dict):
            candidate = metadata.get("image") or metadata.get("image_url")
            if isinstance(candidate, str):
                image = candidate.strip()
        image_uris[token_uri] = image
    return image_uris


def fetch_image_uri_for_token_uri(token_uri: str) -> Optional[str]:
    return asyncio.run(fetch_image_uris_for_token_uris([token_uri], concurrency=1)).get(token_uri)


def ensure_retry_checked_column(conn, chain_name: str) -> None:
    tbl = _nft_table_name(chain_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            ALTER TABLE {tbl}
            ADD COLUMN IF NOT EXISTS retry_checked_at TIMESTAMPTZ
            """
        )
        cur.execute(
            f"""
            ALTER TABLE {tbl}
            ADD COLUMN IF NOT EXISTS metadata JSONB
            """
        )
    conn.commit()
    logger.info("Columns ready: %s.retry_checked_at / metadata", tbl)


def get_conn() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=10,
    )
    conn.autocommit = True
    return conn


def fetch_missing_image_rows(
    cur: psycopg2.extensions.cursor, chain_name: str, limit: int
) -> list:
    tbl = _nft_table_name(chain_name)
    cur.execute(
        f"""
        SELECT token_uri
        FROM (
            SELECT token_uri, MIN(retry_checked_at) AS oldest
            FROM {tbl}
            WHERE image_uri IS NULL
              AND token_uri IS NOT NULL
              AND (
                  token_uri LIKE 'ipfs://%%'
                  OR token_uri LIKE 'ar://%%'
                  OR token_uri LIKE 'http%%'
              )
            GROUP BY token_uri
            ORDER BY oldest ASC NULLS FIRST
            LIMIT %s
        ) sub
        """,
        (limit,),
    )
    return cur.fetchall()


def mark_retry_checked(
    cur: psycopg2.extensions.cursor,
    conn: psycopg2.extensions.connection,
    chain_name: str,
    token_uris: list,
) -> None:
    if not token_uris:
        return
    tbl = _nft_table_name(chain_name)
    cur.execute(
        f"""
        UPDATE {tbl}
        SET retry_checked_at = NOW()
        WHERE image_uri IS NULL
          AND token_uri = ANY(%s)
        """,
        ([r["token_uri"] for r in token_uris],),
    )
    conn.commit()


def update_metadata_by_token_uri(
    cur: psycopg2.extensions.cursor,
    token_uri: str,
    metadata: Dict[str, Any],
    chain_name: str,
) -> int:
    tbl = _nft_table_name(chain_name)
    image_uri = metadata.get("image") or metadata.get("image_url")
    image_uri = image_uri.strip() if isinstance(image_uri, str) else None
    cur.execute(
        f"""
        UPDATE {tbl}
        SET image_uri = %s,
            metadata = %s::jsonb
        WHERE token_uri = %s
          AND (image_uri IS NULL OR metadata IS NULL)
        """,
        (image_uri, json.dumps(metadata, ensure_ascii=False), token_uri),
    )
    return cur.rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry missing NFT image URIs")
    parser.add_argument(
        "--chain",
        default=CHAIN_NAME,
        help="Chain for nft_assets_{chain} (default: %(default)s)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=CONCURRENT_REQUESTS,
        help="Concurrent metadata requests (default: %(default)s)",
    )
    args = parser.parse_args()
    chain_name = args.chain
    concurrency = max(1, args.concurrency)

    logger.info(
        "Starting image URI retry | chain: %s | table: %s | concurrency: %s",
        chain_name,
        _nft_table_name(chain_name),
        concurrency,
    )

    conn = get_conn()
    ensure_retry_checked_column(conn, chain_name)
    conn.close()

    total_processed = 0
    total_updated = 0

    while True:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            rows = fetch_missing_image_rows(cur, chain_name, BATCH_SIZE)
            if rows:
                mark_retry_checked(cur, conn, chain_name, rows)
        finally:
            cur.close()
            conn.close()

        if not rows:
            logger.info("No rows to retry; close database connection and sleep for 600 seconds")
            time.sleep(600)
            continue

        token_uris = [row["token_uri"] for row in rows]
        logger.info(
            "Pending token URIs: %d, total processed: %d, total updated: %d",
            len(token_uris),
            total_processed,
            total_updated,
        )

        metadata_records = asyncio.run(
            fetch_metadata_records_for_token_uris(
                token_uris,
                concurrency=concurrency,
            )
        )

        batch_updated = 0
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            for token_uri in token_uris:
                total_processed += 1
                metadata = metadata_records.get(token_uri)
                if not metadata:
                    continue

                updated_count = update_metadata_by_token_uri(
                    cur,
                    token_uri,
                    metadata,
                    chain_name,
                )
                batch_updated += updated_count
                total_updated += updated_count
                image_uri = metadata.get("image") or metadata.get("image_url") or ""

                logger.info(
                    "Updated token_uri=%s rows=%d image_uri=%s metadata_keys=%s",
                    token_uri[:80],
                    updated_count,
                    image_uri[:80],
                    sorted(metadata.keys())[:10],
                )
        finally:
            cur.close()
            conn.close()
            logger.info("Database connection closed")

        logger.info(
            "Batch complete: %d updated, %d total updated",
            batch_updated,
            total_updated,
        )


if __name__ == "__main__":
    main()
