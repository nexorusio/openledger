"""Supervised process boundary for external P2 collectors.

The parent owns task finalization. A fresh interpreter owns its own database
connections and may only append under the original attempt/worker lease. Parent
cancellation kills the entire collector session before a terminal outcome can
be recorded. An independent stdlib-only watchdog cleans up that session after
parent death, including subprocess grandchildren, even if provider code blocks
the Python interpreter or ignores coroutine cancellation.
"""

from __future__ import annotations

import asyncio
import ctypes
import json
import os
from pathlib import Path
import signal
import select
import subprocess
import sys
import time
from typing import Sequence

POLL_SECONDS = 0.1
TERMINATE_GRACE_SECONDS = 0.5
MAX_OUTPUT_BYTES = 256 * 1024
MAX_DIAGNOSTIC_BYTES = 64 * 1024


class CollectorProcessError(RuntimeError):
    """A process/contract error, with no provider payload or credentials exposed."""


def process_is_alive(pid):
    """Probe a kernel process handle; zombies and PID reuse cannot fake liveness."""
    try:
        descriptor = os.pidfd_open(int(pid))
    except ProcessLookupError:
        return False
    try:
        poller = select.poll()
        poller.register(descriptor, select.POLLIN)
        return not poller.poll(0)
    finally:
        os.close(descriptor)


def _signal_group(pid, signum):
    if pid <= 1 or pid == os.getpgrp():
        raise CollectorProcessError("Refusing an unsafe collector process group")
    try:
        os.killpg(pid, signum)
    except ProcessLookupError:
        pass


async def _drain_group(process):
    # Always signal the group, even if the direct child exited while leaving a
    # grandchild alive. Never rely on Process.terminate() for tree cleanup.
    _signal_group(process.pid, signal.SIGTERM)
    if process.returncode is None:
        try:
            await asyncio.wait_for(process.wait(), TERMINATE_GRACE_SECONDS)
        except asyncio.TimeoutError:
            pass
    _signal_group(process.pid, signal.SIGKILL)
    if process.returncode is None:
        try:
            await asyncio.wait_for(process.wait(), 2)
        except asyncio.TimeoutError as error:
            raise CollectorProcessError(
                "Collector could not be reaped after SIGKILL"
            ) from error


async def run_bounded_process(
    command: Sequence[str],
    payload: bytes,
    *,
    timeout_seconds: float,
    cancelled,
    environment=None,
    max_output_bytes=MAX_OUTPUT_BYTES,
):
    """Execute without a shell; bound time, memory and descendant lifetime."""
    if sys.platform != "linux" or not hasattr(os, "pidfd_open"):
        raise CollectorProcessError(
            "Released collectors require Linux process supervision"
        )
    if timeout_seconds <= 0 or cancelled():
        if cancelled():
            raise asyncio.CancelledError()
        raise asyncio.TimeoutError()
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        env=environment,
    )
    started = time.monotonic()

    async def read_output():
        chunks, size = [], 0
        while chunk := await process.stdout.read(8192):
            size += len(chunk)
            if size > max_output_bytes:
                raise CollectorProcessError(
                    "Collector exceeded its bounded output contract"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    async def drain_diagnostic():
        tail = b""
        while chunk := await process.stderr.read(8192):
            tail = (tail + chunk)[-MAX_DIAGNOSTIC_BYTES:]
        return tail

    async def write_input():
        try:
            process.stdin.write(payload)
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            process.stdin.close()

    readers = [
        asyncio.create_task(read_output()),
        asyncio.create_task(drain_diagnostic()),
        asyncio.create_task(write_input()),
    ]
    communication = asyncio.gather(*readers)
    try:
        while not communication.done() or process.returncode is None:
            if cancelled():
                raise asyncio.CancelledError()
            if time.monotonic() - started >= timeout_seconds:
                raise asyncio.TimeoutError()
            # Propagate oversized output immediately, even when stderr is held
            # open by an otherwise stuck child or grandchild.
            if communication.done():
                communication.result()
                await asyncio.sleep(POLL_SECONDS)
            else:
                await asyncio.wait({communication}, timeout=POLL_SECONDS)
        stdout, stderr, _ = communication.result()
        return stdout, stderr, process.returncode
    finally:
        # KILL is sent synchronously even if the enclosing coroutine receives
        # another cancellation while awaiting graceful cleanup.
        try:
            await _drain_group(process)
        finally:
            _signal_group(process.pid, signal.SIGKILL)
            for reader in readers:
                if not reader.done():
                    reader.cancel()
            await asyncio.gather(*readers, return_exceptions=True)
            if not communication.done():
                communication.cancel()
            await asyncio.gather(communication, return_exceptions=True)


def _watchdog(parent_descriptor, collector_descriptor, collector_pid, ready_descriptor):
    """Independent interpreter watches kernel handles unaffected by PID reuse."""
    poller = select.poll()
    for descriptor in (parent_descriptor, collector_descriptor):
        poller.register(descriptor, select.POLLIN)
    os.write(ready_descriptor, b"1")
    os.close(ready_descriptor)
    # An exit event from either process stops the entire collector session.
    poller.poll()
    _signal_group(collector_pid, signal.SIGKILL)
    return 0


def _start_parent_watchdog(parent_pid):
    if os.getppid() != parent_pid:
        raise CollectorProcessError("Collector supervisor is no longer alive")
    # The kernel kills the direct collector during any gap before the independent
    # watchdog can observe parent death. The watchdog then kills grandchildren.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
        raise CollectorProcessError(
            "Cannot establish collector parent-death protection"
        )
    parent_descriptor = os.pidfd_open(parent_pid)
    collector_descriptor = os.pidfd_open(os.getpid())
    ready_reader, ready_writer = os.pipe()
    try:
        if os.getppid() != parent_pid:
            raise CollectorProcessError("Collector supervisor exited during startup")
        watchdog = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--watchdog",
                str(parent_descriptor),
                str(collector_descriptor),
                str(os.getpid()),
                str(ready_writer),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            pass_fds=(parent_descriptor, collector_descriptor, ready_writer),
            env={"PATH": os.defpath, "LANG": "C.UTF-8"},
        )
        os.close(ready_writer)
        ready_writer = None
        readiness = select.poll()
        readiness.register(ready_reader, select.POLLIN)
        if not readiness.poll(3000) or os.read(ready_reader, 1) != b"1":
            # A failed watchdog is a collection startup failure. No provider may
            # run until its independent parent-death protection is established.
            watchdog.kill()
            watchdog.wait(timeout=2)
            raise CollectorProcessError(
                "Collector parent-death watchdog did not become ready"
            )
    finally:
        os.close(parent_descriptor)
        os.close(collector_descriptor)
        os.close(ready_reader)
        if ready_writer is not None:
            os.close(ready_writer)
    return watchdog


async def supervise_collector(task, context, *, timeout_seconds, cancelled):
    """Run one real registered adapter; injection hooks never enter this path."""
    payload = {
        "supervisor_pid": os.getpid(),
        "database_url": context.store.engine.url.render_as_string(hide_password=False),
        "job": context.job,
        "request": context.request,
        "task": task,
        "attempt": context.attempt,
        "options": context.options,
        "research_context": context.context,
    }
    # Sensitive connection values travel on an anonymous local pipe, never in
    # argv, stdout, a retained JSON file, or an HTTP/configuration execution hook.
    encoded = json.dumps(payload, default=_json_default).encode()
    if len(encoded) > 4 * 1024 * 1024:
        raise CollectorProcessError(
            "Collector process request exceeds its input budget"
        )
    try:
        stdout, _diagnostic, code = await run_bounded_process(
            [sys.executable, str(Path(__file__).resolve()), "--collector"],
            encoded,
            timeout_seconds=timeout_seconds,
            cancelled=cancelled,
        )
        if code != 0:
            raise CollectorProcessError(f"Collector process exited with status {code}")
        try:
            response = json.loads(stdout)
        except (ValueError, UnicodeDecodeError) as error:
            raise CollectorProcessError(
                "Collector returned an invalid process envelope"
            ) from error
        if response.get("contract") != "p2-e2e-v1" or not isinstance(
            response.get("result"), dict
        ):
            raise CollectorProcessError(
                "Collector returned an incompatible process envelope"
            )
        return response["result"]
    finally:
        # The process group has been drained before reading durable counts. This
        # also counts partial writes when cancellation prevented a final reply.
        from sqlalchemy import func, select

        observations = context.pipeline._table("observations")
        with context.store.engine.connect() as connection:
            context.observation_count = connection.scalar(
                select(func.count())
                .select_from(observations)
                .where(observations.c.attempt_id == context.attempt["id"])
            )


def _json_default(value):
    from datetime import datetime, date

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError("Collector process input must contain JSON values")


async def _child_dispatch(payload):
    # No imported connection objects cross this boundary. Initialization occurs
    # only after the kernel protection and independent watchdog are active.
    from maigret.web.case_store import CaseStore
    from maigret.web.pipeline_store import PipelineStore
    from maigret.web.pipeline_execution import (
        CollectorContext,
        dispatch_collector,
        _app,
    )
    from maigret.web.pipeline_release import assert_runtime_ready

    store = CaseStore(payload["database_url"])
    try:
        assert_runtime_ready(store, role="worker")
        pipeline = PipelineStore(store)
        job, task, attempt = payload["job"], payload["task"], payload["attempt"]
        # Re-read the linked job and active attempt immediately before collection.
        # append_observations repeats this check transactionally on every write.
        with store.engine.begin() as connection:
            pipeline._attempt_context(connection, attempt["id"], job.get("worker_id"))
        cancelled = lambda: store.is_cancel_requested(job["job_id"])
        sink = _app().PersistentEventSink(
            store, job["job_id"], worker_id=job.get("worker_id")
        )
        context = CollectorContext(
            store,
            pipeline,
            job,
            payload["request"],
            task,
            attempt,
            sink,
            cancelled,
            payload["options"],
            payload["research_context"],
        )
        from maigret.web.pipeline_http import TransportGuard
        from maigret.web.pipeline_runtime import ProviderCooldown, RequestBudgetExceeded

        with TransportGuard(store, payload["request"]["id"], attempt["id"], job.get("worker_id")).install() as guard:
            try:
                result = await dispatch_collector(task, context)
            except RequestBudgetExceeded:
                # Preserve the typed runtime decision across the JSON process
                # boundary.  Collapsing this to a bare inconclusive outcome
                # made the parent forget the durable budget error code.
                result = {
                    "outcome": "inconclusive",
                    "completeness": "partial",
                    "error_code": "request_budget_exhausted",
                    "retryable": False,
                }
            except ProviderCooldown as error:
                # The worker owns retry scheduling, so return the bounded
                # cooldown interval instead of hiding it in the child.
                result = {
                    "outcome": "error",
                    "completeness": "partial",
                    "error_code": "provider_cooldown",
                    "retryable": True,
                    "retry_after_seconds": error.retry_after_seconds,
                }
            result = guard.finish(result)
        return {"contract": "p2-e2e-v1", "result": result}
    finally:
        store.dispose()


def _main(argv):
    if len(argv) == 5 and argv[0] == "--watchdog":
        return _watchdog(int(argv[1]), int(argv[2]), int(argv[3]), int(argv[4]))
    if argv != ["--collector"]:
        raise CollectorProcessError("Unknown collector process command")
    payload = json.loads(sys.stdin.buffer.read(4 * 1024 * 1024 + 1))
    _watcher = _start_parent_watchdog(int(payload["supervisor_pid"]))
    # Running a file by its path normally adds only maigret/web to sys.path.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import contextlib

    with contextlib.redirect_stdout(sys.stderr):
        response = asyncio.run(_child_dispatch(payload))
    print(json.dumps(response))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(_main(sys.argv[1:]))
    except Exception as error:
        # The privileged parent receives a bounded error type, never a provider
        # payload or database URL. Source-specific details remain in the ledger.
        print(f"Collector process failed: {type(error).__name__}", file=sys.stderr)
        raise SystemExit(1)
