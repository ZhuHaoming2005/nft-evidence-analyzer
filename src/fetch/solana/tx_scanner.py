#!/usr/bin/env python3
"""Discover Metaplex MetadataV1 mints using Helius getProgramAccountsV2. Checkpoint pagination and changedSinceSlot for resumable incremental scans."""

import asyncio
import signal
import sys
import threading
from contextlib import contextmanager
from typing import List, NamedTuple, Optional, Tuple

import aiohttp

from common import (
    CHAIN_NAME,
    HELIUS_RPC_URL,
    GPA_PAGE_SIZE,
    GPA_SINCE_SLOT,
    logger,
    get_conn,
    init_db,
    batch_insert_temp,
    get_gpa_progress,
    save_gpa_progress,
    fetch_gpa_page,
    get_latest_slot,
)


class _ScanState(NamedTuple):
    since_slot: int
    resume_key: Optional[str]
    total_pages_base: int
    mode: str


def _is_stop_requested(stop_event) -> bool:
    return bool(stop_event is not None and stop_event.is_set())


@contextmanager
def _install_stop_signal_handlers(stop_event):
    handlers = {}

    def _handle_stop(signum, frame):
        if not _is_stop_requested(stop_event):
            logger.info("Stop requested; persist the current GPA page and checkpoint before exiting.")
        stop_event.set()

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        try:
            handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _handle_stop)
        except (OSError, RuntimeError, ValueError):
            continue
    try:
        yield
    finally:
        for sig, previous in handlers.items():
            try:
                signal.signal(sig, previous)
            except (OSError, RuntimeError, ValueError):
                continue


def _resolve_scan_state(
    *,
    env_since: int,
    saved_key: Optional[str],
    saved_since_slot: int,
    saved_pages: int,
) -> _ScanState:
    if env_since > 0:
        if saved_since_slot > env_since:
            if saved_key is not None:
                return _ScanState(
                    saved_since_slot,
                    saved_key,
                    saved_pages,
                    f"Resume incremental scan (DB since={saved_since_slot}, ignoring older .env since={env_since})",
                )
            return _ScanState(
                saved_since_slot,
                None,
                0,
                f"Incremental scan (DB since={saved_since_slot}, ignoring older .env since={env_since})",
            )
        if saved_key is not None and saved_since_slot == env_since:
            return _ScanState(
                env_since,
                saved_key,
                saved_pages,
                f"Resume incremental scan (since={env_since})",
            )
        return _ScanState(
            env_since,
            None,
            0,
            f"Incremental scan (.env since={env_since})",
        )

    if saved_key is not None:
        since_slot = saved_since_slot or 0
        mode = (
            f"Resume incremental scan (since={since_slot})"
            if since_slot > 0
            else "Resume full scan"
        )
        return _ScanState(since_slot, saved_key, saved_pages, mode)

    if saved_since_slot > 0:
        return _ScanState(
            saved_since_slot,
            None,
            0,
            f"Incremental scan (DB since={saved_since_slot})",
        )

    return _ScanState(0, None, 0, "Full scan")


def _progress_since_slot(
    *,
    current_since_slot: int,
    latest_slot: int,
    next_key: Optional[str],
) -> int:
    if next_key is not None:
        return current_since_slot
    if latest_slot > 0:
        return latest_slot
    return current_since_slot


async def main(stop_event=None) -> None:
    stop_event = stop_event or threading.Event()
    logger.info("Solana GPA scanner starting: chain=%s", CHAIN_NAME)

    if not HELIUS_RPC_URL:
        logger.error(
            "HELIUS_API_KEY is required for getProgramAccountsV2. "
            "Set HELIUS_API_KEY in .env."
        )
        return

    logger.info("Helius RPC: %s", HELIUS_RPC_URL.split("?")[0])
    logger.info("Page size: %d", GPA_PAGE_SIZE)

    conn       = get_conn()
    write_conn = get_conn()
    init_db(conn, CHAIN_NAME)

    saved_key, saved_since_slot, saved_pages = get_gpa_progress(conn, CHAIN_NAME)

    scan_state = _resolve_scan_state(
        env_since=GPA_SINCE_SLOT,
        saved_key=saved_key,
        saved_since_slot=saved_since_slot,
        saved_pages=saved_pages,
    )
    since_slot       = scan_state.since_slot
    resume_key       = scan_state.resume_key
    total_pages_base = scan_state.total_pages_base
    logger.info("Scan start: %s", scan_state.mode)

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, enable_cleanup_closed=True)
    session   = aiohttp.ClientSession(connector=connector, trust_env=True)
    prefetch_task: Optional[asyncio.Task] = None

    try:
        latest_slot = await get_latest_slot(session, HELIUS_RPC_URL)
        if latest_slot == 0:
            logger.warning("Latest slot unavailable; a completed full scan will save since_slot=0")

        mode_str = (
            f"Incremental scan (since={since_slot})" if since_slot > 0
            else "Full scan"
        )
        logger.info("Scan mode: %s | Latest slot: %d", mode_str, latest_slot)

        # GPA cursors are sequential; overlap the next fetch with the current database write.

        total_new    = 0
        total_pages  = total_pages_base
        current_key  = resume_key

        prefetch_task = asyncio.create_task(
            fetch_gpa_page(
                session, HELIUS_RPC_URL, GPA_PAGE_SIZE,
                pagination_key=current_key,
                changed_since_slot=since_slot or None,
            )
        )

        while True:
            # A failed fetch must not checkpoint this page as complete.
            mints, next_key = await prefetch_task

            if next_key is not None:
                prefetch_task = asyncio.create_task(
                    fetch_gpa_page(
                        session, HELIUS_RPC_URL, GPA_PAGE_SIZE,
                        pagination_key=next_key,
                        changed_since_slot=since_slot or None,
                    )
                )

            total_pages += 1

            records: List[Tuple] = [(mint, "Metaplex", 0) for mint in mints]

            inserted = await asyncio.to_thread(
                batch_insert_temp, write_conn, CHAIN_NAME, records
            )

            # Checkpoint the next cursor only after this page has been persisted.
            await asyncio.to_thread(
                save_gpa_progress,
                write_conn, CHAIN_NAME,
                next_key,       # None marks the end of this pass.
                _progress_since_slot(
                    current_since_slot=since_slot,
                    latest_slot=latest_slot,
                    next_key=next_key,
                ),
                total_pages,
            )

            del records

            total_new += inserted
            logger.info(
                "Page %d: %d mints, %d inserted | Total inserted: %d",
                total_pages, len(mints), inserted, total_new,
            )

            if next_key is None:
                break
            if _is_stop_requested(stop_event):
                logger.info(
                    "Scan interrupted; current page persisted with paginationKey=%s",
                    next_key,
                )
                break

            current_key = next_key

        if since_slot == 0 and latest_slot > 0:
            logger.info(
                "Full scan complete: %d pages, %d new mints",
                total_pages, total_new,
            )
            logger.info(
                "Saved since_slot=%d. The next run will use incremental mode, "
                "fetching accounts created or changed after that slot.",
                latest_slot,
            )
        else:
            logger.info(
                "Scan complete: %d pages, %d new mints",
                total_pages, total_new,
            )
    finally:
        if prefetch_task is not None:
            if not prefetch_task.done():
                prefetch_task.cancel()
            await asyncio.gather(prefetch_task, return_exceptions=True)
        await session.close()
        await connector.close()
        write_conn.close()
        conn.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    _stop_event = threading.Event()
    with _install_stop_signal_handlers(_stop_event):
        asyncio.run(main(stop_event=_stop_event))
