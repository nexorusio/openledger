"""Dedicated browser-independent investigation worker."""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import uuid

from maigret.web.app import case_store, record_internal_error, run_persistent_job

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("openledger.worker")
stopping = threading.Event()


def request_shutdown(signum, _frame) -> None:
    logger.info(
        "Worker received signal %s; stopping the active investigation safely", signum
    )
    stopping.set()


def run() -> int:
    if case_store is None:
        raise RuntimeError("DATABASE_URL is required by the OpenLedger worker")

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
    poll_seconds = max(0.25, float(os.getenv("WORKER_POLL_SECONDS", "2")))

    case_store.ping()
    worker_lock = case_store.try_acquire_worker_lock()
    if worker_lock is None:
        logger.error("Another OpenLedger investigation worker already owns the lock")
        return 1
    try:
        interrupted = case_store.mark_stale_running(0)
        if interrupted:
            logger.warning(
                "Marked %s abandoned investigation(s) as interrupted", interrupted
            )
        logger.info("OpenLedger worker %s is ready", worker_id)

        while not stopping.is_set():
            job = case_store.claim_next(worker_id)
            if not job:
                stopping.wait(poll_seconds)
                continue
            logger.info("Starting investigation %s", job["job_id"])
            try:
                run_persistent_job(
                    case_store,
                    job,
                    shutdown_check=stopping.is_set,
                )
            except Exception as error:
                public_error = record_internal_error(
                    "Investigation worker crashed",
                    error,
                    session=job["job_id"],
                )
                case_store.finish(
                    job["job_id"],
                    {
                        "status": "failed",
                        "error": public_error,
                        "usernames": job["usernames"],
                    },
                )
    finally:
        worker_lock.close()

    logger.info("OpenLedger worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
