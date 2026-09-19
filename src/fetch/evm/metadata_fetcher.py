#!/usr/bin/env python3
"""Fetch Alchemy metadata for claimed staging rows. Persist valid records before deleting processed rows; release failed claims for retry."""

import asyncio
import multiprocessing
import os
import signal
import socket
import sys
import threading
import time
from contextlib import contextmanager
from typing import List, Optional, Set, Tuple

import aiohttp

from common import (
    CHAIN_NAME,
    ALCHEMY_BATCH_SIZE,
    CONCURRENT_ALCHEMY,
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
    delete_contract_nfts,
    append_blacklist_env,
    load_blacklist,
    fetch_alchemy_batch,
    _decode_inline_image,
    replace_token_id_placeholder,
    fix_token_id_placeholders,
)


_INVALID_URI_PREFIXES = ("api.tierlock.com/uri/",)
FETCHER_WORKERS = max(int(os.getenv("FETCHER_WORKERS", "1")), 1)
WORKER_STARTUP_DELAY_SECONDS = max(
    0.0,
    float(os.getenv("WORKER_STARTUP_DELAY_SECONDS", "0")),
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


def _cleanup_blacklisted_pending(
    conn,
    pending: List[Tuple],
    all_ids: List[int],
    blacklisted_contracts: Set[str],
    newly_blacklisted_contracts: Set[str],
) -> Tuple[List[int], List[int], int, int]:
    blacklisted_contracts = {addr.lower() for addr in blacklisted_contracts}
    newly_blacklisted_contracts = {addr.lower() for addr in newly_blacklisted_contracts}
    blacklisted_ids = sorted(
        row[0]
        for row in pending
        if row[1] in blacklisted_contracts
    )
    if blacklisted_ids:
        delete_temp_nfts(conn, CHAIN_NAME, blacklisted_ids)

    main_deleted = temp_deleted = 0
    if newly_blacklisted_contracts:
        main_deleted, temp_deleted = delete_contract_nfts(
            conn,
            CHAIN_NAME,
            newly_blacklisted_contracts,
            skip_temp_ids=blacklisted_ids,
        )

    remaining_ids = [
        rid for rid, row in zip(all_ids, pending)
        if row[1] not in blacklisted_contracts
    ]
    return remaining_ids, blacklisted_ids, main_deleted, temp_deleted


async def _maybe_wait_worker_startup(worker_index: int, stop_event=None) -> None:
    delay = WORKER_STARTUP_DELAY_SECONDS * max(worker_index - 1, 0)
    if delay > 0:
        logger.info("worker-%d startup delay: %.2f seconds", worker_index, delay)
        if stop_event is None:
            await asyncio.sleep(delay)
        else:
            await _sleep_interruptibly(delay, stop_event)


def _batch_startup_delay(batch_index: int) -> float:
    if REQUEST_STARTUP_STAGGER_SECONDS <= 0:
        return 0.0
    return REQUEST_STARTUP_STAGGER_SECONDS * max(batch_index - 1, 0)


async def _process_batch(
    session: aiohttp.ClientSession,
    alchemy_sem: asyncio.Semaphore,
    pending: List[Tuple],
) -> Tuple[List[Tuple], List[int], Set[str]]:
    """Return (inserts, processed IDs, inline-image contracts). Inline-image contracts are blacklisted and removed from both tables."""

    tokens  = [(addr, tid, std) for _, addr, tid, std, _ in pending]
    all_ids = [row[0] for row in pending]

    chunks = [
        tokens[i: i + ALCHEMY_BATCH_SIZE]
        for i in range(0, len(tokens), ALCHEMY_BATCH_SIZE)
    ]
    chunk_results = await asyncio.gather(*[
        fetch_alchemy_batch(
            session,
            alchemy_sem,
            chunk,
            startup_delay_seconds=_batch_startup_delay(batch_index),
        )
        for batch_index, chunk in enumerate(chunks, start=1)
    ]) if chunks else []

    alchemy_results: List[
        Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[object]]
    ] = [
        item for chunk in chunk_results for item in chunk
    ]

    for i, (token_uri, image_url, name, symbol, metadata) in enumerate(alchemy_results):
        if image_url is not None and not isinstance(image_url, str):
            image_url = None
            alchemy_results[i] = (token_uri, image_url, name, symbol, metadata)
        if token_uri and token_uri.startswith("data:application/") and image_url is None:
            decoded_img = _decode_inline_image(token_uri)
            if decoded_img:
                alchemy_results[i] = (token_uri, decoded_img, name, symbol, metadata)

    onchain_image_contracts: Set[str] = set()
    for (_, addr, _, _, _), (_, image_url, _, _, _) in zip(pending, alchemy_results):
        if isinstance(image_url, str) and image_url.startswith("data:image"):
            onchain_image_contracts.add(addr)

    inserts: List[Tuple] = []

    for (_, addr, tid, std, first_seen_block), (
        token_uri,
        image_url,
        contract_name,
        contract_symbol,
        raw_metadata,
    ) in zip(
        pending, alchemy_results
    ):

        if addr in onchain_image_contracts:
            continue

        if not token_uri or any(token_uri.startswith(p) for p in _INVALID_URI_PREFIXES):
            continue

        token_uri = replace_token_id_placeholder(token_uri, tid)

        inserts.append(
            (
                addr,
                str(tid),
                token_uri,
                image_url,
                contract_name,
                contract_symbol,
                raw_metadata,
                std,
                first_seen_block,
            )
        )

    return inserts, all_ids, onchain_image_contracts


def _build_worker_id(worker_index: int) -> str:
    return f"{socket.gethostname()}:{os.getpid()}:worker-{worker_index}"


async def _worker_main(worker_index: int, stop_event=None) -> None:
    stop_event = stop_event or threading.Event()
    await _maybe_wait_worker_startup(worker_index, stop_event)
    worker_id = _build_worker_id(worker_index)
    logger.info(
        "Metadata collector starting: chain=%s | worker=%s | claim_batch=%d | reclaim_after=%ds",
        CHAIN_NAME,
        worker_id,
        FETCH_CLAIM_BATCH_SIZE,
        CLAIM_RETRY_AFTER_SECONDS,
    )

    conn = None
    pending_ids: List[int] = []
    try:
        conn = get_conn()

        fixed = fix_token_id_placeholders(conn, CHAIN_NAME)
        if fixed:
            logger.info("Expanded stored {id} placeholders: %d rows updated", fixed)

        total_inserted = total_deleted = 0
        alchemy_sem = asyncio.Semaphore(CONCURRENT_ALCHEMY)
        connector = aiohttp.TCPConnector(limit=max(CONCURRENT_ALCHEMY * 2, 20), ttl_dns_cache=300)

        async with aiohttp.ClientSession(
            trust_env=True,
            connector=connector,
            connector_owner=True,
        ) as session:
            while not _is_stop_requested(stop_event):
                pending = claim_pending_nfts(
                    conn,
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
                    inserts, all_ids, onchain_image_contracts = await _process_batch(
                        session,
                        alchemy_sem,
                        pending,
                    )
                except Exception:
                    released = release_temp_claims(
                        conn,
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
                newly_blacklisted_contracts = {addr.lower() for addr in onchain_image_contracts}

                if newly_blacklisted_contracts:
                    logger.info(
                        "Detected %d inline-image contracts (data:image URI); "
                        "blacklisting: %s",
                        len(newly_blacklisted_contracts), sorted(newly_blacklisted_contracts),
                    )
                    append_blacklist_env(newly_blacklisted_contracts)

                effective_blacklist = {
                    addr.lower() for addr in load_blacklist()
                } | newly_blacklisted_contracts
                inserts = [
                    record for record in inserts
                    if record[0] not in effective_blacklist
                ]

                blacklisted_main_del = blacklisted_temp_sweep_del = 0
                blacklisted_ids: List[int] = []
                if effective_blacklist:
                    all_ids, blacklisted_ids, blacklisted_main_del, blacklisted_temp_sweep_del = (
                        _cleanup_blacklisted_pending(
                            conn,
                            pending,
                            all_ids,
                            effective_blacklist,
                            newly_blacklisted_contracts,
                        )
                    )
                    logger.info(
                        "Blacklist cleanup: %d batch rows deleted, %d main rows deleted, "
                        "%d additional staging rows deleted",
                        len(blacklisted_ids),
                        blacklisted_main_del,
                        blacklisted_temp_sweep_del,
                    )
                blacklisted_rows = len(blacklisted_ids)

                inserted = batch_insert_main(conn, CHAIN_NAME, inserts)
                deleted = delete_temp_nfts(conn, CHAIN_NAME, all_ids)
                pending_ids = []
                discarded = len(pending) - len(inserts) - blacklisted_rows

                total_inserted += inserted
                total_deleted += deleted + blacklisted_rows + blacklisted_temp_sweep_del
                logger.info(
                    "worker=%s batch: inserted %d, staging deleted %d (invalid %d, blacklisted contracts %d)"
                    " | total inserted %d, total deleted %d",
                    worker_id,
                    inserted, deleted + blacklisted_rows + blacklisted_temp_sweep_del,
                    discarded,
                    len(newly_blacklisted_contracts),
                    total_inserted, total_deleted,
                )

        if _is_stop_requested(stop_event):
            logger.info("worker=%s stopped; current batch and cleanup complete", worker_id)
    finally:
        if pending_ids and conn is not None:
            try:
                released = release_temp_claims(conn, CHAIN_NAME, pending_ids, worker_id)
                logger.info("worker=%s released %d unfinished claims before exit", worker_id, released)
            except Exception:
                logger.exception("worker=%s failed to release claims during exit", worker_id)
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
    init_db(conn, CHAIN_NAME)
    conn.close()
    stop_event = multiprocessing.Event()

    with _install_stop_signal_handlers(stop_event):
        if FETCHER_WORKERS <= 1:
            _run_worker_process(1, stop_event)
            return

        logger.info(
            "Starting %d metadata workers; per-process Alchemy concurrency=%d",
            FETCHER_WORKERS,
            CONCURRENT_ALCHEMY,
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
