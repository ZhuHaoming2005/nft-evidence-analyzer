#!/usr/bin/env python3
"""Fetch Helius metadata for claimed mints. Use singleton collections for ungrouped assets and exclude inline images."""

import asyncio
import multiprocessing
import os
import signal
import socket
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, List, Optional, Tuple

import aiohttp

from common import (
    CHAIN_NAME,
    HELIUS_BATCH_SIZE,
    CONCURRENT_HELIUS,
    FETCH_IDLE_WAIT,
    FETCH_CLAIM_BATCH_SIZE,
    CLAIM_RETRY_AFTER_SECONDS,
    REQUEST_STARTUP_STAGGER_SECONDS,
    logger,
    get_conn,
    init_db,
    claim_pending_nfts,
    batch_insert_main,
    delete_temp_nfts,
    release_temp_claims,
    fetch_metadata_batch,
)


_INVALID_URI_PREFIXES = (
    "data:text/",
    "data:application/xml",
    "data:application/json",
)

FETCHER_WORKERS = max(int(os.getenv("FETCHER_WORKERS", "1")), 1)
WORKER_STARTUP_DELAY_SECONDS = max(
    0.0,
    float(os.getenv("WORKER_STARTUP_DELAY_SECONDS", str(REQUEST_STARTUP_STAGGER_SECONDS))),
)
WORKER_SHUTDOWN_GRACE_SECONDS = max(
    1.0,
    float(os.getenv("WORKER_SHUTDOWN_GRACE_SECONDS", "5")),
)


def _is_stop_requested(stop_event) -> bool:
    return bool(stop_event is not None and stop_event.is_set())


@contextmanager
def _install_stop_signal_handlers(stop_event):
    handlers = {}

    def _handle_stop(signum, frame):
        if not _is_stop_requested(stop_event):
            logger.info("Stop requested; stop claiming rows and finish the current batch and cleanup.")
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


async def _sleep_interruptibly(delay: float, stop_event=None, *, interval: float = 0.5) -> bool:
    remaining = max(0.0, delay)
    while remaining > 0:
        if _is_stop_requested(stop_event):
            return True
        step = min(interval, remaining)
        await asyncio.sleep(step)
        remaining -= step
    return _is_stop_requested(stop_event)


async def _maybe_wait_worker_startup(worker_index: int, stop_event=None) -> None:
    delay = WORKER_STARTUP_DELAY_SECONDS * max(worker_index - 1, 0)
    if delay <= 0:
        return
    logger.info("worker-%d startup delay: %.2f seconds", worker_index, delay)
    await _sleep_interruptibly(delay, stop_event)


async def _process_batch(
    session: aiohttp.ClientSession,
    helius_sem: asyncio.Semaphore,
    pending: List[Tuple],
) -> Tuple[List[Tuple], List[int]]:
    """Fetch metadata for pending rows and return valid inserts and processed IDs."""
    mints   = [row[1] for row in pending]
    all_ids = [row[0] for row in pending]

    chunk_size = HELIUS_BATCH_SIZE
    chunks = [mints[i: i + chunk_size] for i in range(0, len(mints), chunk_size)]

    chunk_results = list(await asyncio.gather(*[
        fetch_metadata_batch(session, helius_sem, chunk)
        for chunk in chunks
    ]))

    results: List[
        Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[Any]]
    ] = [
        item for chunk in chunk_results for item in chunk
    ]

    inserts: List[Tuple] = []
    for (
        (_, mint, std, first_seen_slot),
        (collection_address, token_uri, image_url, name, symbol, metadata),
    ) in zip(pending, results):
        if not isinstance(image_url, str):
            continue
        if image_url and image_url.startswith("data:image"):
            continue
        if not token_uri or any(token_uri.startswith(p) for p in _INVALID_URI_PREFIXES):
            continue
        contract_address = collection_address or mint
        inserts.append((
            contract_address, mint, token_uri, image_url,
            name, symbol, metadata,
            std or "Metaplex", first_seen_slot,
        ))

    return inserts, all_ids


async def _fetch_all(
    session: aiohttp.ClientSession,
    helius_sem: asyncio.Semaphore,
    batches: List[List[Tuple]],
    forced: bool = False,
) -> Tuple[List[Tuple], List[int], int, int]:
    """Fetch batches without database writes; return (inserts, IDs, raw_count, insert_count). The Helius semaphore bounds HTTP concurrency."""
    tag = "[flush remaining rows]" if forced else f"[{len(batches)} batches / {sum(len(b) for b in batches)} rows]"
    logger.info("Fetching concurrently: %s", tag)

    all_results: List[Tuple[List[Tuple], List[int]]] = list(
        await asyncio.gather(*[
            _process_batch(session, helius_sem, batch)
            for batch in batches
        ])
    )

    all_inserts: List[Tuple] = [row for inserts, _ in all_results for row in inserts]
    all_ids:     List[int]   = [rid for _, ids    in all_results for rid in ids]
    total_raw  = sum(len(b) for b in batches)
    n_inserts  = len(all_inserts)
    return all_inserts, all_ids, total_raw, n_inserts


async def _do_write(
    conn_insert,
    conn_delete,
    all_inserts: List[Tuple],
    all_ids: List[int],
) -> Tuple[int, int]:
    """Insert persistent rows before deleting staging rows; retain staging rows if insertion fails."""
    inserted = await asyncio.to_thread(
        batch_insert_main, conn_insert, CHAIN_NAME, all_inserts
    )
    deleted = await asyncio.to_thread(
        delete_temp_nfts, conn_delete, CHAIN_NAME, all_ids
    )
    return inserted, deleted


def _build_worker_id(worker_index: int) -> str:
    return f"{socket.gethostname()}:{os.getpid()}:worker-{worker_index}"


async def _worker_main(worker_index: int, stop_event=None) -> None:
    stop_event = stop_event or threading.Event()
    await _maybe_wait_worker_startup(worker_index, stop_event)
    worker_id = _build_worker_id(worker_index)
    logger.info(
        "Solana metadata collector starting: chain=%s | worker=%s | "
        "claim_batch=%d | reclaim_after=%ds",
        CHAIN_NAME,
        worker_id,
        FETCH_CLAIM_BATCH_SIZE,
        CLAIM_RETRY_AFTER_SECONDS,
    )

    conn_claim = conn_insert = conn_delete = None
    pending_ids: List[int] = []
    try:
        conn_claim = get_conn()
        conn_insert = get_conn()
        conn_delete = get_conn()

        total_inserted = total_deleted = 0
        helius_sem = asyncio.Semaphore(CONCURRENT_HELIUS)
        connector = aiohttp.TCPConnector(
            limit=max(CONCURRENT_HELIUS * 2, 20),
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )

        async with aiohttp.ClientSession(
            connector=connector,
            trust_env=True,
            connector_owner=True,
        ) as session:
            while not _is_stop_requested(stop_event):
                pending = claim_pending_nfts(
                    conn_claim,
                    CHAIN_NAME,
                    worker_id=worker_id,
                    batch_size=FETCH_CLAIM_BATCH_SIZE,
                    reclaim_after_seconds=CLAIM_RETRY_AFTER_SECONDS,
                )
                pending_ids = [row[0] for row in pending]

                if _is_stop_requested(stop_event):
                    break

                if not pending:
                    logger.info(
                        "worker=%s no rows claimed; retry in %d seconds",
                        worker_id,
                        FETCH_IDLE_WAIT,
                    )
                    if await _sleep_interruptibly(FETCH_IDLE_WAIT, stop_event):
                        break
                    continue

                logger.info("worker=%s processing %d claimed staging rows", worker_id, len(pending))
                try:
                    all_inserts, all_ids, total_raw, n_inserts = await _fetch_all(
                        session,
                        helius_sem,
                        [pending],
                    )
                    ins, del_ = await _do_write(
                        conn_insert,
                        conn_delete,
                        all_inserts,
                        all_ids,
                    )
                    pending_ids = []
                except Exception:
                    released = release_temp_claims(
                        conn_claim,
                        CHAIN_NAME,
                        pending_ids,
                        worker_id,
                    )
                    pending_ids = []
                    logger.exception(
                        "worker=%s batch failed; released %d claims for retry",
                        worker_id,
                        released,
                    )
                    if await _sleep_interruptibly(1, stop_event):
                        break
                    continue

                total_inserted += ins
                total_deleted  += del_
                logger.info(
                    "worker=%s batch: inserted %d, staging deleted %d (invalid %d)"
                    " | total inserted %d, total deleted %d",
                    worker_id,
                    ins,
                    del_,
                    total_raw - n_inserts,
                    total_inserted,
                    total_deleted,
                )

        if _is_stop_requested(stop_event):
            logger.info("worker=%s stopped; current batch and cleanup complete", worker_id)
    finally:
        if pending_ids and conn_claim is not None:
            try:
                released = release_temp_claims(conn_claim, CHAIN_NAME, pending_ids, worker_id)
                logger.info("worker=%s released %d unfinished claims before exit", worker_id, released)
            except Exception:
                logger.exception("worker=%s failed to release claims during exit", worker_id)
        for conn in (conn_delete, conn_insert, conn_claim):
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def _run_worker_process(worker_index: int, stop_event=None) -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    local_stop_event = stop_event or threading.Event()
    with _install_stop_signal_handlers(local_stop_event):
        asyncio.run(_worker_main(worker_index, stop_event=local_stop_event))


def _wait_for_workers(workers: List[multiprocessing.Process], stop_event) -> None:
    deadline: Optional[float] = None
    while any(proc.is_alive() for proc in workers):
        for proc in workers:
            proc.join(timeout=0.2)
        if _is_stop_requested(stop_event):
            if deadline is None:
                deadline = time.monotonic() + WORKER_SHUTDOWN_GRACE_SECONDS
            elif time.monotonic() >= deadline:
                break

    lingering = [proc for proc in workers if proc.is_alive()]
    if lingering:
        logger.info("Force-stopping %d metadata workers after shutdown grace period", len(lingering))
        for proc in lingering:
            proc.terminate()
        for proc in lingering:
            proc.join(timeout=1)


def main() -> None:
    conn = get_conn()
    try:
        init_db(conn, CHAIN_NAME)
    finally:
        conn.close()

    stop_event = multiprocessing.Event()
    with _install_stop_signal_handlers(stop_event):
        if FETCHER_WORKERS <= 1:
            _run_worker_process(1, stop_event)
            return

        logger.info(
            "Starting %d Solana metadata workers; per-process Helius concurrency=%d",
            FETCHER_WORKERS,
            CONCURRENT_HELIUS,
        )
        workers = [
            multiprocessing.Process(
                target=_run_worker_process,
                args=(index, stop_event),
                name=f"{CHAIN_NAME}-metadata-{index}",
            )
            for index in range(1, FETCHER_WORKERS + 1)
        ]
        for proc in workers:
            proc.start()

        try:
            _wait_for_workers(workers, stop_event)
        except KeyboardInterrupt:
            stop_event.set()
            _wait_for_workers(workers, stop_event)


if __name__ == "__main__":
    main()
