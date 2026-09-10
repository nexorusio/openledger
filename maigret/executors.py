"""Bounded asynchronous executor used by Maigret checks.

The public result stream remains the legacy stream of values returned by a
check (normally ``(site_name, SiteResult)``).  Accounting is an additive side
channel, so older consumers continue to receive the native/default tuple.
"""

import asyncio
import hashlib
import inspect
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, Iterable, Mapping, Optional, Tuple


TaskSpec = Tuple[Callable[..., Any], list, dict]
TaskNotifier = Callable[[Mapping[str, object]], None]
DEFAULT_CLEANUP_SECONDS = 5.0


class ExecutorAccountingError(RuntimeError):
    """A durable terminal-accounting sink rejected an outcome.

    This is intentionally distinct from a source failure.  Callers must not
    mark a collection complete when they could not record its task outcome.
    """


class ExecutorCleanupIncompleteError(ExecutorAccountingError):
    """Cancellation cleanup outlived the collection finalization budget.

    The collection has durable per-task terminal records, but at least one
    source coroutine remains active.  A caller must surface this as an
    interrupted/partial collection instead of publishing normal completion.
    """


class ExecutorTaskMetadataError(ValueError):
    """Executor task metadata cannot produce unique durable identities."""


class AsyncioQueueGeneratorExecutor:
    """Run coroutine factories with bounded active and cleaning-up work.

    ``max_outstanding`` includes a timed-out query until its cancellation
    cleanup completes.  The default is the worker count.  A coroutine that
    suppresses cancellation can therefore stop new admissions, but cannot
    cause an unbounded population of detached tasks.

    ``task_notify`` receives exactly one terminal event per planned task.  It
    is the durable accounting seam: a callback failure raises
    :class:`ExecutorAccountingError` rather than being silently counted as a
    successful collection.  The event contains only opaque identifiers and
    classification fields; native results and diagnostics remain in the
    legacy result stream and normal logging paths.
    """

    def __init__(self, *args, **kwargs):
        self.workers_count = max(1, kwargs.get("in_parallel", 10))
        self.timeout = kwargs.get("timeout")
        self.logger = kwargs["logger"]
        self.task_notify: Optional[TaskNotifier] = kwargs.get("task_notify")
        self.max_outstanding = max(
            1, kwargs.get("max_outstanding", self.workers_count)
        )
        self.execution_id = str(kwargs.get("execution_id", "executor"))
        self.execution_time = 0.0
        self.cleanup_incomplete = False
        # Retained until their callbacks run.  They are not awaited during
        # finalization because cancellation cleanup may intentionally or
        # accidentally suppress cancellation forever.
        self._cleaning_tasks = set()
        self._cleanup_metadata: Dict[asyncio.Task, Dict[str, object]] = {}
        self._completed_cleanup = []
        self._reported_cleanup_states = set()

    @property
    def pending_cleanup_count(self) -> int:
        """Number of cancellation-cleanup tasks still running."""
        return sum(1 for task in self._cleaning_tasks if not task.done())

    @staticmethod
    def _opaque_id(*parts: object) -> str:
        value = "\x1f".join(str(part) for part in parts)
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _metadata(self, task: TaskSpec, ordinal: int) -> Dict[str, object]:
        raw = task[2].get("_executor_task", {})
        raw = raw if isinstance(raw, dict) else {}
        attempt = raw.get("attempt", 1)
        return {
            "schema_version": 1,
            "task_id": str(
                raw.get("task_id", self._opaque_id(self.execution_id, ordinal, attempt))
            ),
            "source_id": raw.get("source_id"),
            "target_id": raw.get("target_id"),
            "check_id": raw.get("check_id"),
            "attempt": attempt,
        }

    @staticmethod
    def _fallback(task: TaskSpec, disposition: str) -> Any:
        fallbacks = task[2].get("_executor_defaults", {})
        if isinstance(fallbacks, dict) and disposition in fallbacks:
            return fallbacks[disposition]
        return task[2].get("default")

    def _notify(
        self,
        metadata: Dict[str, object],
        disposition: str,
        *,
        attempted: bool,
        cleanup_state: str = "not_required",
        reason: Optional[str] = None,
    ) -> None:
        if self.task_notify is None:
            return
        event: Dict[str, object] = dict(metadata)
        event.update(
            disposition=disposition,
            attempted=attempted,
            cleanup_state=cleanup_state,
        )
        if reason is not None:
            event["reason"] = reason
        try:
            value = self.task_notify(event)
            if inspect.isawaitable(value):
                # An async durable sink would outlive this executor without a
                # supervised delivery contract.  Reject it loudly instead.
                close = getattr(value, "close", None)
                if callable(close):
                    close()
                raise TypeError("task_notify must be synchronous")
        except ExecutorAccountingError:
            raise
        except Exception as exc:
            raise ExecutorAccountingError(
                "executor terminal accounting callback failed"
            ) from exc

    def _notify_cleanup(self, metadata: Dict[str, object], state: str) -> None:
        """Record the bounded cleanup follow-up after a terminal timeout."""
        if self.task_notify is None:
            return
        event: Dict[str, object] = dict(metadata)
        event.update(event_type="cleanup", cleanup_state=state)
        try:
            value = self.task_notify(event)
            if inspect.isawaitable(value):
                close = getattr(value, "close", None)
                if callable(close):
                    close()
                raise TypeError("task_notify must be synchronous")
        except ExecutorAccountingError:
            raise
        except Exception as exc:
            raise ExecutorAccountingError(
                "executor cleanup accounting callback failed"
            ) from exc

    def _track_cleanup(
        self, query_task: asyncio.Task, metadata: Dict[str, object]
    ) -> None:
        self._cleaning_tasks.add(query_task)
        self._cleanup_metadata[query_task] = metadata

        def done(task: asyncio.Task) -> None:
            self._cleaning_tasks.discard(task)
            completed_metadata = self._cleanup_metadata.pop(task, None)
            if completed_metadata is not None:
                self._completed_cleanup.append(completed_metadata)
            if task.cancelled():
                return
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                return
            if exc is not None:
                self.logger.debug("Timed-out/cancelled check task raised: %s", exc)

        query_task.add_done_callback(done)

    @staticmethod
    def _finalize_task_callback(task: TaskSpec) -> None:
        """Close a task-local notifier gate before an outcome is published.

        The callback is local process hygiene, not durable accounting: its
        failure is logged because it must not hide the already-classified
        source outcome.  ``checking`` uses it to prevent a cancellation-
        resistant check coroutine from emitting a late progress update.
        """
        callback = task[2].get("_executor_finalize")
        if callable(callback):
            try:
                callback()
            except Exception:
                # Kept deliberately diagnostic-only.  Durable terminal sink
                # failures are handled by _notify and are never swallowed.
                return

    async def drain_cleanup(self, timeout: float = DEFAULT_CLEANUP_SECONDS) -> bool:
        """Wait at most ``timeout`` for tracked cancellation cleanup.

        The query tasks were cancelled at their timeout/cancellation boundary;
        this method never waits without a cap and never creates replacement
        tasks.  ``False`` means callers must finalise as interrupted rather
        than start a later stage that could race the retained source task.
        """
        timeout = min(DEFAULT_CLEANUP_SECONDS, max(0.0, float(timeout)))
        pending = {task for task in self._cleaning_tasks if not task.done()}
        if pending:
            await asyncio.wait(pending, timeout=timeout)
            # Run done callbacks before exposing the final state to callers.
            await asyncio.sleep(0)
        for metadata in self._completed_cleanup:
            task_id = str(metadata["task_id"])
            state_key = (task_id, "complete")
            if state_key not in self._reported_cleanup_states:
                self._notify_cleanup(metadata, "complete")
                self._reported_cleanup_states.add(state_key)
        self._completed_cleanup.clear()
        self._cleaning_tasks.intersection_update(
            task for task in self._cleaning_tasks if not task.done()
        )
        self.cleanup_incomplete = bool(self._cleaning_tasks)
        if self.cleanup_incomplete:
            for task in self._cleaning_tasks:
                metadata = self._cleanup_metadata[task]
                task_id = str(metadata["task_id"])
                state_key = (task_id, "incomplete")
                if state_key not in self._reported_cleanup_states:
                    self._notify_cleanup(metadata, "incomplete")
                    self._reported_cleanup_states.add(state_key)
        return not self.cleanup_incomplete

    async def _run_one(self, task: TaskSpec, metadata: Dict[str, object]) -> Any:
        f, args, task_kwargs = task
        call_kwargs = dict(task_kwargs)
        call_kwargs.pop("_executor_task", None)
        call_kwargs.pop("_executor_defaults", None)
        call_kwargs.pop("_executor_finalize", None)
        query_task: Optional[asyncio.Task] = None
        try:
            query_task = asyncio.create_task(f(*args, **call_kwargs))
            if self.timeout is None:
                # Do not await the child directly: cancelling this worker
                # would otherwise wait for a child which suppresses
                # cancellation during transport cleanup.  ``asyncio.wait``
                # leaves the child available for bounded tracking below.
                done, _ = await asyncio.wait({query_task})
            else:
                done, _ = await asyncio.wait({query_task}, timeout=self.timeout)
                if query_task not in done:
                    self._finalize_task_callback(task)
                    query_task.cancel()
                    self._track_cleanup(query_task, metadata)
                    self._notify(
                        metadata, "timeout", attempted=True, cleanup_state="pending"
                    )
                    return self._fallback(task, "timeout")
            result = query_task.result()
            self._finalize_task_callback(task)
            self._notify(metadata, "completed", attempted=True)
            return result
        except asyncio.CancelledError:
            # A source coroutine may cancel itself.  That is an attempted
            # source outcome, not a cancellation of the executor: retain its
            # native-unknown fallback and keep admitting healthy siblings.
            # Parent cancellation is delivered to this wrapper task, so its
            # cancellation counter is non-zero even when the child is done.
            wrapper_cancelled = asyncio.current_task().cancelling() > 0
            if (
                query_task is not None
                and query_task.cancelled()
                and not wrapper_cancelled
            ):
                self._finalize_task_callback(task)
                self._notify(
                    metadata, "cancelled", attempted=True,
                    cleanup_state="not_required",
                )
                return self._fallback(task, "cancelled")
            if query_task is not None and not query_task.done():
                self._finalize_task_callback(task)
                query_task.cancel()
                self._track_cleanup(query_task, metadata)
                cleanup_state = "pending"
            else:
                cleanup_state = "not_required"
            self._notify(
                metadata, "cancelled", attempted=query_task is not None,
                cleanup_state=cleanup_state,
            )
            raise
        except ExecutorAccountingError:
            raise
        except Exception as exc:
            self._finalize_task_callback(task)
            self.logger.error("Error in worker: %s", exc, exc_info=True)
            self._notify(metadata, "error", attempted=True)
            return self._fallback(task, "error")

    def _emit_unattempted(
        self, pending: Deque[Tuple[int, TaskSpec]], reason: str
    ) -> Iterable[Any]:
        while pending:
            ordinal, task = pending.popleft()
            self._notify(
                self._metadata(task, ordinal), "unattempted", attempted=False,
                reason=reason,
            )
            yield self._fallback(task, "unattempted")

    @staticmethod
    def _raise_gathered_accounting_error(outcomes: Iterable[object]) -> None:
        """Do not hide a durable-sink failure from cancelled workers."""
        for outcome in outcomes:
            if isinstance(outcome, ExecutorAccountingError):
                raise outcome

    async def run(self, queries: Iterable[TaskSpec]):
        """Yield one native/default result for every planned task.

        Errors and timeouts yield their task-specific fallback.  If occupied
        cleanup reaches ``max_outstanding``, remaining tasks are never started
        and yield their ``unattempted`` fallback.  This preserves complete
        accounting without waiting forever for a cancellation-resistant task.
        """
        start_time = time.monotonic()
        pending: Deque[Tuple[int, TaskSpec]] = deque(enumerate(queries))
        task_ids = set()
        for ordinal, task in pending:
            task_id = self._metadata(task, ordinal)["task_id"]
            if task_id in task_ids:
                raise ExecutorTaskMetadataError(
                    "duplicate executor task_id before admission"
                )
            task_ids.add(task_id)
        workers: Dict[asyncio.Task, Tuple[int, TaskSpec, Dict[str, object]]] = {}
        cancelled = False
        terminal_error: Optional[BaseException] = None

        try:
            while pending or workers:
                while (
                    pending
                    and len(workers) < self.workers_count
                    and len(workers) + len(self._cleaning_tasks) < self.max_outstanding
                ):
                    ordinal, task = pending.popleft()
                    metadata = self._metadata(task, ordinal)
                    worker = asyncio.create_task(self._run_one(task, metadata))
                    workers[worker] = (ordinal, task, metadata)

                if not workers:
                    for result in self._emit_unattempted(
                        pending, "outstanding_limit_reached"
                    ):
                        yield result
                    break

                done, _ = await asyncio.wait(
                    workers, return_when=asyncio.FIRST_COMPLETED
                )
                # asyncio.wait returns a set.  Preserve the legacy executor's
                # stable admission order when several checks finish in the
                # same loop turn (notably zero-delay synthetic checks).
                for worker in sorted(done, key=lambda item: workers[item][0]):
                    workers.pop(worker, None)
                    # _run_one converts source failures to fallbacks.  An
                    # accounting failure must remain visible to the caller.
                    try:
                        result = worker.result()
                    except BaseException as exc:
                        terminal_error = exc
                        raise
                    yield result

        except asyncio.CancelledError:
            cancelled = True
            for worker in workers:
                worker.cancel()
            outcomes = await asyncio.gather(*workers, return_exceptions=True)
            self._raise_gathered_accounting_error(outcomes)
            workers.clear()
            # Values cannot be yielded after an async-generator cancellation,
            # but the durable sink still receives a terminal record for every
            # task that was never admitted.
            for ordinal, task in pending:
                self._notify(
                    self._metadata(task, ordinal), "unattempted", attempted=False,
                    reason="executor_cancelled",
                )
            pending.clear()
            raise
        finally:
            if not cancelled:
                # A consumer may close the async iterator early, or an
                # unexpected executor exception may leave items unadmitted.
                # They cannot be yielded any longer, but must not disappear
                # from durable accounting.  A durable-sink failure here must
                # still cancel active checks; otherwise an accounting outage
                # leaks source work past this executor's lifetime.
                try:
                    for _ in self._emit_unattempted(pending, "consumer_closed"):
                        pass
                finally:
                    for worker in workers:
                        if not worker.done():
                            worker.cancel()
                    if workers:
                        outcomes = await asyncio.gather(*workers, return_exceptions=True)
                        if not isinstance(terminal_error, ExecutorAccountingError):
                            self._raise_gathered_accounting_error(outcomes)
            self.cleanup_incomplete = bool(self._cleaning_tasks)
            self.execution_time = time.monotonic() - start_time
            self.logger.debug("Spent time: %s", self.execution_time)
