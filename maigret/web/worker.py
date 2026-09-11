"""Dedicated browser-independent investigation worker."""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import uuid

from maigret.web.app import app, case_store, record_internal_error, run_persistent_job
from maigret.web.case_store import CaseStore
from maigret.web.worker_execution import execute_profile_process
from maigret.web.case_store import (
    WORKER_HEARTBEAT_SECONDS,
    WORKER_STALE_AFTER_SECONDS,
)
from maigret.web.profile_discovery_policy import PROFILE_DISCOVERY_JOB_KINDS

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("openledger.worker")
stopping = threading.Event()
AI_JOB_KINDS = frozenset({"case_fusion_ai"})


def request_shutdown(signum, _frame) -> None:
    logger.info(
        "Worker received signal %s; stopping the active investigation safely", signum
    )
    stopping.set()


def maintain_job_heartbeat(store, job, stop_event, lease_lost) -> None:
    """Persist a heartbeat every five seconds until the job or lease ends."""
    job_id = str(job["job_id"])
    worker_id = str(job.get("worker_id") or "")
    if not worker_id:
        lease_lost.set()
        return
    while not stop_event.wait(WORKER_HEARTBEAT_SECONDS):
        try:
            if not store.heartbeat(job_id, worker_id):
                logger.warning("Investigation %s lost its worker lease", job_id)
                lease_lost.set()
                return
        except Exception as error:
            record_internal_error(
                "Investigation heartbeat failed",
                error,
                session=job_id,
            )
            # A worker unable to verify its lease stops collection. It never
            # assumes that a failed renewal still authorizes later writes.
            lease_lost.set()
            return


def monitor_stale_jobs(store, stop_event) -> None:
    """Interrupt expired jobs while the worker process remains alive."""
    while not stop_event.wait(WORKER_HEARTBEAT_SECONDS):
        try:
            interrupted = store.mark_stale_running(WORKER_STALE_AFTER_SECONDS)
            if interrupted:
                logger.warning(
                    "Marked %s stale investigation(s) as interrupted", interrupted
                )
        except Exception as error:
            record_internal_error("Stale investigation watchdog failed", error)


def execute_job(store, job, *, shutdown_check, fixture_adapter_factory=None) -> None:
    """Run one claimed job while maintaining its durable worker lease."""
    logger.info("Starting investigation %s (%s)", job["job_id"], job.get("kind"))
    heartbeat_stop = threading.Event()
    lease_lost = threading.Event()
    heartbeat_thread = threading.Thread(
        target=maintain_job_heartbeat,
        args=(store, job, heartbeat_stop, lease_lost),
        name=f"openledger-heartbeat-{job['job_id']}",
        daemon=True,
    )
    heartbeat_thread.start()

    def execution_should_stop():
        if lease_lost.is_set():
            return 'lease_lost'
        if shutdown_check():
            return 'worker_shutdown'
        return False

    try:
        if isinstance(store, CaseStore) and job.get('kind') in PROFILE_DISCOVERY_JOB_KINDS:
            if app.config.get('TESTING') and fixture_adapter_factory is None:
                raise RuntimeError('Test collectors require an explicit spawn-safe fixture adapter')
            execute_profile_process(
                store, job, shutdown_check=execution_should_stop,
                config={key: app.config[key] for key in ('TESTING', 'REPORTS_FOLDER', 'MAIGRET_DB_FILE')},
                fixture_adapter_factory=fixture_adapter_factory,
            )
        else:
            run_persistent_job(store, job, shutdown_check=execution_should_stop)
    except Exception as error:
        public_error = record_internal_error(
            "Investigation worker crashed",
            error,
            session=job["job_id"],
        )
        if job.get('kind') in PROFILE_DISCOVERY_JOB_KINDS:
            # The runner may have failed while persisting its own error or
            # terminal event. Keep the profile job eligible for the existing
            # stale reconciler, which commits retained/unknown accounting and
            # exactly one done event without replaying collection.
            return
        store.finish(
            job["job_id"],
            {
                "status": "failed",
                "error": public_error,
                "usernames": job["usernames"],
            },
            worker_id=job.get("worker_id"),
        )
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=WORKER_HEARTBEAT_SECONDS)


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
    watchdog_stop = threading.Event()
    watchdog_thread = None
    try:
        interrupted = case_store.mark_stale_running(0)
        if interrupted:
            logger.warning(
                "Marked %s abandoned investigation(s) as interrupted", interrupted
            )
        watchdog_thread = threading.Thread(
            target=monitor_stale_jobs,
            args=(case_store, watchdog_stop),
            name="openledger-stale-watchdog",
            daemon=True,
        )
        watchdog_thread.start()
        logger.info("OpenLedger worker %s is ready", worker_id)

        ai_thread = None
        while not stopping.is_set():
            if ai_thread is not None and not ai_thread.is_alive():
                ai_thread.join()
                ai_thread = None

            if ai_thread is None:
                ai_job = case_store.claim_next_matching(
                    f"{worker_id}:ai", include_kinds=AI_JOB_KINDS
                )
                if ai_job:
                    ai_thread = threading.Thread(
                        target=execute_job,
                        args=(case_store, ai_job),
                        kwargs={"shutdown_check": stopping.is_set},
                        name="openledger-combined-ai",
                        daemon=True,
                    )
                    ai_thread.start()

            job = case_store.claim_next_matching(worker_id, exclude_kinds=AI_JOB_KINDS)
            if not job:
                stopping.wait(poll_seconds)
                continue
            execute_job(case_store, job, shutdown_check=stopping.is_set)

        if ai_thread is not None:
            ai_thread.join(timeout=30)
            if ai_thread.is_alive():
                logger.error(
                    "AI synthesis did not stop within the shutdown grace period"
                )
    finally:
        watchdog_stop.set()
        if watchdog_thread is not None:
            watchdog_thread.join(timeout=WORKER_HEARTBEAT_SECONDS)
        worker_lock.close()

    logger.info("OpenLedger worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
