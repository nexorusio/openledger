# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Bounded, injectable collection-stage orchestration for profile discovery.

The worker supplies existing source callbacks and persists their native values.
This module has no source I/O or storage knowledge: it admits one stage at a
time, retains time for selected later stages, and publishes safe accounting.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    Union,
)

COLLECTION_ACCOUNTING_SCHEMA_VERSION = 1
DEFAULT_CLEANUP_SECONDS = 5.0
PROFILE_STAGE_WEIGHTS: Mapping[str, float] = {
    "native": 15,
    "maigret": 55,
    "github": 5,
    "unfurl": 3,
    "wayback": 5,
    "user_scanner_username": 10,
    "user_scanner_email": 7,
}
PROFILE_STAGE_ENGINE_IDS: Mapping[str, str] = {
    "native": "native-profile-search",
    "maigret": "maigret",
    "github": "github-public-profile",
    "unfurl": "unfurl-url-analysis",
    "wayback": "wayback-cdx",
    "user_scanner_username": "user-scanner-username",
    "user_scanner_email": "user-scanner",
}
_STAGE_STATUSES = frozenset(
    {
        "not_selected",
        "skipped_no_targets",
        "blocked_dependency",
        "not_started_budget",
        "running",
        "completed",
        "failed",
        "timed_out",
        "cancelled",
        "interrupted",
        "unknown",
    }
)
_OPERATION_DISPOSITIONS = frozenset(
    {
        "completed",
        "error",
        "timeout",
        "cancelled",
    }
)


class CollectionOrchestrationError(ValueError):
    """Raised for an invalid server-owned source plan."""


class StopCause(str, Enum):
    """A durable reason that the scheduler stopped admitting source work."""

    OPERATOR_CANCEL = "operator_cancel"
    JOB_DEADLINE = "job_deadline"
    STAGE_DEADLINE = "stage_deadline"
    CLEANUP_INCOMPLETE = "cleanup_incomplete"
    PERSISTENCE_FAILURE = "persistence_failure"
    LEASE_LOST = "lease_lost"
    WORKER_SHUTDOWN = "worker_shutdown"


StopSignal = Union[bool, StopCause, str, None]
StopCallback = Callable[[], StopSignal]


def _normalize_stop_signal(value: StopSignal) -> Tuple[Optional[StopCause], bool]:
    """Return ``(cause, legacy_boolean)`` for a stop callback result.

    ``True`` remains the historical operator cancellation signal.  Typed
    callers must use the stable values in :class:`StopCause`; rejecting other
    truthy values prevents a persistence or lease boundary from being silently
    reported as a user cancellation.
    """
    if value is None or value is False:
        return None, False
    if value is True:
        return StopCause.OPERATOR_CANCEL, True
    if isinstance(value, StopCause):
        return value, False
    if isinstance(value, str):
        try:
            return StopCause(value), False
        except ValueError as error:
            raise CollectionOrchestrationError(
                "Unknown collection stop cause"
            ) from error
    raise CollectionOrchestrationError("Stop callback must return bool or StopCause")


def _stop_status(cause: StopCause) -> str:
    if cause is StopCause.OPERATOR_CANCEL:
        return "cancelled"
    if cause in {StopCause.JOB_DEADLINE, StopCause.STAGE_DEADLINE}:
        return "timed_out"
    return "interrupted"


def _stop_reason(cause: StopCause, *, legacy_boolean: bool = False) -> str:
    if legacy_boolean and cause is StopCause.OPERATOR_CANCEL:
        return "cancellation_requested"
    if cause is StopCause.STAGE_DEADLINE:
        return "stage_budget_exhausted"
    if cause is StopCause.JOB_DEADLINE:
        return "overall_budget_exhausted"
    return cause.value


class _ParentCancellation(asyncio.CancelledError):
    def __init__(
        self, cleanup_complete: bool, result: Optional["StageResult"] = None
    ) -> None:
        super().__init__()
        self.cleanup_complete = cleanup_complete
        self.result = result


@dataclass(frozen=True)
class StageCounts:
    """Counts for one consistent source unit; ``None`` stays unknown."""

    planned: Optional[int] = None
    started: Optional[int] = None
    terminal: Optional[int] = None
    completed: Optional[int] = None
    errors: Optional[int] = None
    timeouts: Optional[int] = None
    cancelled: Optional[int] = None
    interrupted: Optional[int] = None
    unattempted: Optional[int] = None
    unknown: Optional[int] = None
    observations: Optional[int] = None

    def __post_init__(self) -> None:
        values = self.__dict__
        for value in values.values():
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise CollectionOrchestrationError(
                    "Stage counts must be non-negative integers"
                )
        planned = self.planned
        started = self.started
        terminal = self.terminal
        if planned is not None and started is not None and started > planned:
            raise CollectionOrchestrationError("Started count exceeds planned count")
        if planned is not None and terminal is not None and terminal > planned:
            raise CollectionOrchestrationError("Terminal count exceeds planned count")
        if started is not None and terminal is not None and terminal > started:
            raise CollectionOrchestrationError("Terminal count exceeds started count")

        terminal_components = (
            self.completed,
            self.errors,
            self.timeouts,
            self.cancelled,
        )
        if terminal is not None and all(value is not None for value in terminal_components):
            if sum(terminal_components) != terminal:
                raise CollectionOrchestrationError(
                    "Terminal count does not match terminal dispositions"
                )
        elif terminal is not None:
            known_terminal_components = sum(
                value for value in terminal_components if value is not None
            )
            if known_terminal_components > terminal:
                raise CollectionOrchestrationError(
                    "Terminal dispositions exceed terminal count"
                )

        unfinished_components = (self.interrupted, self.unknown)
        if started is not None and terminal is not None:
            remaining_started = started - terminal
            if all(value is not None for value in unfinished_components):
                if sum(unfinished_components) != remaining_started:
                    raise CollectionOrchestrationError(
                        "Started count does not match terminal and unfinished work"
                    )
            elif sum(value for value in unfinished_components if value is not None) > remaining_started:
                raise CollectionOrchestrationError(
                    "Unfinished work exceeds started work"
                )
        if planned is not None and started is not None and self.unattempted is not None:
            if self.unattempted != planned - started:
                raise CollectionOrchestrationError(
                    "Planned count does not match started and unattempted work"
                )

    def with_planned(self, planned: Optional[int]) -> "StageCounts":
        return StageCounts(
            # The server-owned plan wins whenever it is known.  A dynamic
            # plan intentionally remains callback/ledger supplied.
            planned=planned if planned is not None else self.planned,
            started=self.started,
            terminal=self.terminal,
            completed=self.completed,
            errors=self.errors,
            timeouts=self.timeouts,
            cancelled=self.cancelled,
            interrupted=self.interrupted,
            unattempted=self.unattempted,
            unknown=self.unknown,
            observations=self.observations,
        )

    @property
    def known(self) -> bool:
        return all(value is not None for value in self.__dict__.values())

    def as_dict(self) -> Dict[str, Optional[int]]:
        return dict(self.__dict__)


def _known_unattempted_counts(planned: int) -> StageCounts:
    return StageCounts(
        planned=planned,
        started=0,
        terminal=0,
        completed=0,
        errors=0,
        timeouts=0,
        cancelled=0,
        interrupted=0,
        unattempted=planned,
        unknown=0,
        observations=0,
    )


class OperationLedger:
    """Idempotent private task accounting for one source callback.

    The worker can persist opaque task IDs through its existing guarded event
    path.  This ledger only aggregates them and never puts an identifier in a
    public accounting snapshot.
    """

    def __init__(self) -> None:
        self._planned: set[str] = set()
        self._started: set[str] = set()
        self._terminal: Dict[str, Tuple[str, Optional[int]]] = {}
        self._finalized = False

    def record_planned(self, operation_id: str) -> bool:
        if self._finalized or not operation_id or operation_id in self._planned:
            return False
        self._planned.add(operation_id)
        return True

    def record_started(self, operation_id: str) -> bool:
        if (
            self._finalized
            or operation_id not in self._planned
            or operation_id in self._started
            or operation_id in self._terminal
        ):
            return False
        self._started.add(operation_id)
        return True

    def record_terminal(
        self,
        operation_id: str,
        disposition: str,
        *,
        attempted: bool,
        observations: Optional[int] = None,
    ) -> bool:
        if (
            self._finalized
            or operation_id not in self._planned
            or operation_id in self._terminal
            or disposition not in _OPERATION_DISPOSITIONS
            or (not attempted and disposition != "unattempted")
            or (attempted and disposition == "unattempted")
        ):
            return False
        if attempted:
            self._started.add(operation_id)
        self._terminal[operation_id] = (disposition, _optional_count(observations))
        return True

    def finalize(self, *, unfinished_disposition: str = "unknown") -> StageCounts:
        """Seal progress and classify planned work lacking a terminal event."""
        if unfinished_disposition not in {"interrupted", "unknown"}:
            raise CollectionOrchestrationError("Invalid unfinished task disposition")
        self._finalized = True
        counts = {key: 0 for key in StageCounts().__dict__}
        counts["planned"] = len(self._planned)
        counts["started"] = len(self._started)
        counts["terminal"] = len(self._terminal)
        observation_values = []
        observation_unknown = False
        for disposition, observations in self._terminal.values():
            key = (
                "errors"
                if disposition == "error"
                else ("timeouts" if disposition == "timeout" else disposition)
            )
            counts[key] += 1
            if observations is None:
                observation_unknown = True
            else:
                observation_values.append(observations)
        for operation_id in self._planned.difference(self._terminal):
            if operation_id in self._started:
                counts[unfinished_disposition] += 1
                observation_unknown = True
            else:
                counts["unattempted"] += 1
        counts["observations"] = (
            None if observation_unknown else sum(observation_values)
        )
        return StageCounts(**counts)

    def snapshot(self) -> StageCounts:
        """Return current aggregate progress without sealing the ledger.

        Work that has not received a terminal disposition remains unknown in a
        progress snapshot; it is classified as interrupted or unattempted only
        by :meth:`finalize`.
        """
        counts = {key: None for key in StageCounts().__dict__}
        counts["planned"] = len(self._planned)
        counts["started"] = len(self._started)
        counts["terminal"] = len(self._terminal)
        counts["completed"] = 0
        counts["errors"] = 0
        counts["timeouts"] = 0
        counts["cancelled"] = 0
        observation_values = []
        observation_unknown = False
        for disposition, observations in self._terminal.values():
            key = (
                "errors"
                if disposition == "error"
                else ("timeouts" if disposition == "timeout" else disposition)
            )
            if key in {"completed", "errors", "timeouts", "cancelled"}:
                counts[key] += 1
            if observations is None:
                observation_unknown = True
            else:
                observation_values.append(observations)
        counts["observations"] = (
            None if observation_unknown else sum(observation_values)
        )
        return StageCounts(**counts)

    @property
    def has_records(self) -> bool:
        return bool(self._planned)

    @property
    def finalized(self) -> bool:
        return self._finalized


@dataclass(frozen=True)
class StageContext:
    """The bounded context supplied to a source callback."""

    stage_id: str
    engine_id: str
    deadline: float
    cancellation_check: StopCallback
    operation_ledger: OperationLedger
    clock: Callable[[], float] = time.monotonic
    _publish_progress: Callable[[Optional[StageCounts]], bool] = field(
        default=lambda _counts: False, repr=False, compare=False
    )

    def remaining_seconds(self) -> float:
        return max(0.0, self.deadline - self.clock())

    def stop_cause(self) -> Optional[StopCause]:
        """Return the active scheduler stop cause, if one is known."""
        cause, _legacy_boolean = _normalize_stop_signal(self.cancellation_check())
        return cause or (
            StopCause.STAGE_DEADLINE if self.remaining_seconds() <= 0 else None
        )

    def is_cancelled(self) -> bool:
        return self.stop_cause() is not None

    def counts_snapshot(self) -> StageCounts:
        """Return the non-final operation-ledger snapshot for this stage."""
        return self.operation_ledger.snapshot()

    def publish_progress(self, counts: Optional[StageCounts] = None) -> bool:
        """Publish a durable all-stage snapshot while this callback is active."""
        if counts is not None and not isinstance(counts, StageCounts):
            raise CollectionOrchestrationError("Progress counts must be StageCounts")
        return self._publish_progress(counts)


@dataclass(frozen=True)
class StageResult:
    """A native callback value and explicit source counters."""

    value: Any = None
    counts: StageCounts = field(default_factory=StageCounts)


Resolver = Union[bool, int, Callable[..., Any]]
StageCallback = Callable[[StageContext], Awaitable[StageResult]]


@dataclass(frozen=True)
class StageSpec:
    """One source stage and its server-owned admission policy."""

    stage_id: str
    engine_id: str
    label: str
    unit: str
    callback: StageCallback
    planned_units: Resolver = 1
    selected: Resolver = True
    readiness: Optional[Resolver] = None
    weight: float = 1.0
    dependencies: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not all((self.stage_id, self.engine_id, self.label, self.unit)):
            raise CollectionOrchestrationError(
                "Stage identifiers and unit are required"
            )
        if (
            not callable(self.callback)
            or not math.isfinite(self.weight)
            or self.weight <= 0
        ):
            raise CollectionOrchestrationError(
                "Stage callbacks require a positive weight"
            )
        if not callable(self.planned_units) and self.planned_units is not None:
            if (
                isinstance(self.planned_units, bool)
                or not isinstance(self.planned_units, int)
                or self.planned_units < 0
            ):
                raise CollectionOrchestrationError(
                    "Static planned counts must be non-negative integers or unknown"
                )


@dataclass(frozen=True)
class StageOutcome:
    """A safe, source-level row. Native callback values are private only."""

    stage_id: str
    engine_id: str
    label: str
    unit: str
    status: str
    reason: Optional[str]
    counts: StageCounts
    cleanup_complete: bool = True
    value: Any = field(default=None, repr=False, compare=False)
    stop_cause: Optional[StopCause] = None

    def __post_init__(self) -> None:
        if self.status not in _STAGE_STATUSES:
            raise CollectionOrchestrationError("Unknown stage accounting status")

    def as_dict(self) -> Dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "engine_id": self.engine_id,
            "label": self.label,
            "unit": self.unit,
            "status": self.status,
            "reason": self.reason,
            "planned": self.counts.planned,
            "started": self.counts.started,
            "terminal": self.counts.terminal,
            "completed": self.counts.completed,
            "errors": self.counts.errors,
            "timeouts": self.counts.timeouts,
            "cancelled": self.counts.cancelled,
            "interrupted": self.counts.interrupted,
            "unattempted": self.counts.unattempted,
            "unknown": self.counts.unknown,
            "observations": self.counts.observations,
            "cleanup_complete": self.cleanup_complete,
            "stop_cause": (
                self.stop_cause.value if self.stop_cause is not None else None
            ),
        }


@dataclass(frozen=True)
class OrchestrationSummary:
    deadline: float
    started_at: float
    finished_at: float
    outcomes: Tuple[StageOutcome, ...]
    revision: int

    @property
    def state(self) -> str:
        statuses = {item.status for item in self.outcomes}
        if "interrupted" in statuses:
            return "interrupted"
        if "cancelled" in statuses:
            return "cancelled"
        if "unknown" in statuses:
            return "unknown"
        if statuses.issubset({"completed", "not_selected", "skipped_no_targets"}) and all(
            _outcome_is_complete(item) for item in self.outcomes
        ):
            return "completed"
        if "completed" in statuses:
            return "partial"
        return "failed"

    @property
    def known(self) -> bool:
        return all(item.counts.known for item in self.outcomes)

    @property
    def cancelled(self) -> bool:
        return self.state == "cancelled"

    @property
    def budget_exhausted(self) -> bool:
        """Whether the overall collection deadline blocked admission."""
        return self.overall_budget_exhausted

    @property
    def overall_budget_exhausted(self) -> bool:
        return any(
            item.status == "not_started_budget"
            and item.reason == "overall_budget_exhausted"
            for item in self.outcomes
        )

    @property
    def stage_budget_exhausted(self) -> bool:
        """Whether a reserved per-stage share expired before its callback ended."""
        return any(
            item.status == "timed_out" and item.reason == "stage_budget_exhausted"
            for item in self.outcomes
        )

    @property
    def stop_cause(self) -> Optional[StopCause]:
        return next(
            (item.stop_cause for item in self.outcomes if item.stop_cause is not None),
            None,
        )

    def as_dict(self) -> Dict[str, Any]:
        envelope = _accounting_envelope(
            self.outcomes,
            revision=self.revision,
            state=self.state,
        )
        envelope["overall_budget_exhausted"] = self.overall_budget_exhausted
        envelope["stage_budget_exhausted"] = self.stage_budget_exhausted
        envelope["stop_cause"] = (
            self.stop_cause.value if self.stop_cause is not None else None
        )
        return envelope


def profile_stage_weights() -> Mapping[str, float]:
    """Return fixed source-policy weights, never client-provided values."""
    return dict(PROFILE_STAGE_WEIGHTS)


def profile_stage_engine_ids() -> Mapping[str, str]:
    """Return the stable existing engine IDs for the seven profile stages."""
    return dict(PROFILE_STAGE_ENGINE_IDS)


def _outcome_is_complete(outcome: StageOutcome) -> bool:
    if outcome.status in {"not_selected", "skipped_no_targets"}:
        return outcome.counts.known
    counts = outcome.counts
    return (
        outcome.status == "completed"
        and counts.known
        and counts.errors == 0
        and counts.timeouts == 0
        and counts.cancelled == 0
        and counts.interrupted == 0
        and counts.unattempted == 0
        and counts.unknown == 0
    )


def _safe_error_code(error: BaseException) -> str:
    name = type(error).__name__
    cleaned = "".join(
        character for character in name if character.isalnum() or character == "_"
    )
    return cleaned[:80] or "source_error"


def _optional_count(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _planned_count(value: Any) -> Optional[int]:
    if value is None:
        return None
    normalized = _optional_count(value)
    if normalized is None:
        raise CollectionOrchestrationError(
            "Planned count must be a non-negative integer or unknown"
        )
    return normalized


def _resolve(value: Resolver, outcomes: Mapping[str, StageOutcome]) -> Any:
    if not callable(value):
        return value
    try:
        signature = inspect.signature(value)
    except (TypeError, ValueError):
        return value(outcomes)
    return value() if not signature.parameters else value(outcomes)


def _accounting_envelope(
    outcomes: Iterable[StageOutcome], *, revision: int, state: str
) -> Dict[str, Any]:
    rows = [outcome.as_dict() for outcome in outcomes]
    overall_budget_exhausted = any(
        row["status"] == "not_started_budget"
        and row["reason"] == "overall_budget_exhausted"
        for row in rows
    )
    stage_budget_exhausted = any(
        row["status"] == "timed_out"
        and row["reason"] == "stage_budget_exhausted"
        for row in rows
    )
    return {
        "schema_version": COLLECTION_ACCOUNTING_SCHEMA_VERSION,
        "revision": revision,
        "state": state,
        "known": all(
            all(row[field] is not None for field in StageCounts().__dict__)
            for row in rows
        ),
        "overall_budget_exhausted": overall_budget_exhausted,
        "stage_budget_exhausted": stage_budget_exhausted,
        "stop_cause": next(
            (row["stop_cause"] for row in rows if row["stop_cause"] is not None),
            None,
        ),
        "stages": rows,
    }


def _snapshot(
    outcomes: Iterable[StageOutcome], *, revision: int, state: str
) -> Dict[str, Any]:
    return {
        "type": "collection_accounting",
        "collection_accounting": _accounting_envelope(
            outcomes,
            revision=revision,
            state=state,
        ),
    }


def _outcome(
    spec: StageSpec,
    status: str,
    *,
    reason: Optional[str] = None,
    counts: Optional[StageCounts] = None,
    cleanup_complete: bool = True,
    stop_cause: Optional[StopCause] = None,
    value: Any = None,
) -> StageOutcome:
    return StageOutcome(
        spec.stage_id,
        spec.engine_id,
        spec.label,
        spec.unit,
        status,
        reason,
        counts or StageCounts(),
        cleanup_complete,
        value,
        stop_cause,
    )


def _validate(stages: Tuple[StageSpec, ...]) -> None:
    stage_ids = [stage.stage_id for stage in stages]
    if not stages or len(set(stage_ids)) != len(stage_ids):
        raise CollectionOrchestrationError("Collection stage IDs must be unique")
    positions = {stage.stage_id: index for index, stage in enumerate(stages)}
    for index, stage in enumerate(stages):
        unknown = set(stage.dependencies).difference(stage_ids)
        if unknown or stage.stage_id in stage.dependencies:
            raise CollectionOrchestrationError(
                "Collection dependencies must name other stages"
            )
        if any(positions[dependency] >= index for dependency in stage.dependencies):
            raise CollectionOrchestrationError(
                "Collection dependencies must precede their stage"
            )


async def _stop_callback(running, *, cleanup_seconds: float) -> bool:
    running.cancel()
    done, _ = await asyncio.wait({running}, timeout=cleanup_seconds)
    if not done:
        running.add_done_callback(
            lambda task: task.exception() if not task.cancelled() else None
        )
        return False
    await asyncio.gather(running, return_exceptions=True)
    return True


def _stopped_result(running, *, cleanup_complete: bool) -> Optional[StageResult]:
    """Return a cooperative callback result after bounded cancellation."""
    if not cleanup_complete or running.cancelled():
        return None
    try:
        result = running.result()
    except BaseException:
        return None
    return result if isinstance(result, StageResult) else None


async def _run_callback(
    spec: StageSpec,
    context: StageContext,
    *,
    cancellation_check: StopCallback,
    clock: Callable[[], float],
    cleanup_seconds: float,
    progress_failures: list[BaseException],
) -> Tuple[str, Optional[StageResult], Optional[str], bool, Optional[StopCause]]:
    running = asyncio.create_task(spec.callback(context))
    try:
        while True:
            if progress_failures:
                cleaned = await _stop_callback(running, cleanup_seconds=cleanup_seconds)
                raise _ProgressSinkFailure(progress_failures[0], cleaned)
            stop_cause, legacy_boolean = _normalize_stop_signal(cancellation_check())
            if stop_cause is not None:
                cleaned = await _stop_callback(running, cleanup_seconds=cleanup_seconds)
                if progress_failures:
                    raise _ProgressSinkFailure(progress_failures[0], cleaned)
                return (
                    _stop_status(stop_cause),
                    _stopped_result(running, cleanup_complete=cleaned),
                    _stop_reason(stop_cause, legacy_boolean=legacy_boolean),
                    cleaned,
                    stop_cause,
                )
            remaining = context.deadline - clock()
            if remaining <= 0:
                cleaned = await _stop_callback(running, cleanup_seconds=cleanup_seconds)
                if progress_failures:
                    raise _ProgressSinkFailure(progress_failures[0], cleaned)
                return (
                    "timed_out",
                    _stopped_result(running, cleanup_complete=cleaned),
                    _stop_reason(StopCause.STAGE_DEADLINE),
                    cleaned,
                    StopCause.STAGE_DEADLINE,
                )
            done, _ = await asyncio.wait({running}, timeout=min(remaining, 0.1))
            if not done:
                continue
            if progress_failures:
                cleaned = await _stop_callback(running, cleanup_seconds=cleanup_seconds)
                raise _ProgressSinkFailure(progress_failures[0], cleaned)
            try:
                result = running.result()
            except asyncio.CancelledError:
                if progress_failures:
                    raise _ProgressSinkFailure(progress_failures[0], True)
                # This synchronous result read can only expose cancellation
                # of the completed source. Parent cancellation is delivered
                # at the await above and handled by the outer cleanup branch.
                # Do not require Task.cancelling(), which Python 3.10 lacks.
                if clock() >= context.deadline:
                    return (
                        "timed_out",
                        None,
                        _stop_reason(StopCause.STAGE_DEADLINE),
                        True,
                        StopCause.STAGE_DEADLINE,
                    )
                stop_cause, legacy_boolean = _normalize_stop_signal(
                    cancellation_check()
                )
                if stop_cause is not None:
                    return (
                        _stop_status(stop_cause),
                        None,
                        _stop_reason(stop_cause, legacy_boolean=legacy_boolean),
                        True,
                        stop_cause,
                    )
                return "interrupted", None, "callback_cancelled", True, None
            except Exception as error:
                if progress_failures:
                    raise _ProgressSinkFailure(progress_failures[0], True)
                return "failed", None, _safe_error_code(error), True, None
            if not isinstance(result, StageResult):
                return "failed", None, "invalid_stage_result", True, None
            return "completed", result, None, True, None
    except asyncio.CancelledError:
        cleaned = await _stop_callback(running, cleanup_seconds=cleanup_seconds)
        if progress_failures:
            raise _ProgressSinkFailure(progress_failures[0], cleaned)
        raise _ParentCancellation(
            cleaned, _stopped_result(running, cleanup_complete=cleaned)
        )


class _ProgressSinkFailure(Exception):
    """Preserves an event-sink failure through callback exception containment."""

    def __init__(self, error: BaseException, cleanup_complete: bool) -> None:
        super().__init__(str(error))
        self.error = error
        self.cleanup_complete = cleanup_complete


def _merge_counts(
    result: Optional[StageResult],
    ledger: OperationLedger,
    planned: Optional[int],
    *,
    interrupted: bool,
) -> StageCounts:
    ledger_counts = ledger.finalize(
        unfinished_disposition="interrupted" if interrupted else "unknown"
    )
    if ledger.has_records:
        if planned is not None and ledger_counts.planned != planned:
            raise CollectionOrchestrationError(
                "Operation ledger count differs from server-owned plan"
            )
        observations = (
            result.counts.observations
            if result is not None and result.counts.observations is not None
            else ledger_counts.observations
        )
        return StageCounts(
            planned=ledger_counts.planned,
            started=ledger_counts.started,
            terminal=ledger_counts.terminal,
            completed=ledger_counts.completed,
            errors=ledger_counts.errors,
            timeouts=ledger_counts.timeouts,
            cancelled=ledger_counts.cancelled,
            interrupted=ledger_counts.interrupted,
            unattempted=ledger_counts.unattempted,
            unknown=ledger_counts.unknown,
            observations=observations,
        )
    if result is not None:
        return result.counts.with_planned(planned)
    return StageCounts(planned=planned)


def _progress_counts(
    reported: Optional[StageCounts], ledger: OperationLedger, planned: Optional[int]
) -> StageCounts:
    """Merge a non-final ledger snapshot without replacing measured findings."""
    if ledger.has_records:
        ledger_counts = ledger.snapshot()
        return StageCounts(
            planned=ledger_counts.planned,
            started=ledger_counts.started,
            terminal=ledger_counts.terminal,
            completed=ledger_counts.completed,
            errors=ledger_counts.errors,
            timeouts=ledger_counts.timeouts,
            cancelled=ledger_counts.cancelled,
            interrupted=ledger_counts.interrupted,
            unattempted=ledger_counts.unattempted,
            unknown=ledger_counts.unknown,
            observations=(
                reported.observations
                if reported is not None and reported.observations is not None
                else ledger_counts.observations
            ),
        )
    if reported is not None:
        return reported.with_planned(planned)
    return StageCounts(planned=planned)


def _future_weight(
    stages: Tuple[StageSpec, ...], index: int, outcomes: Mapping[str, StageOutcome]
) -> float:
    total = 0.0
    for stage in stages[index + 1 :]:
        try:
            if not bool(_resolve(stage.selected, outcomes)):
                continue
            if any(dependency not in outcomes for dependency in stage.dependencies):
                total += stage.weight
                continue
            ready = (
                _default_ready(stage, outcomes)
                if stage.readiness is None
                else bool(_resolve(stage.readiness, outcomes))
            )
            if not ready:
                continue
            planned = _planned_count(_resolve(stage.planned_units, outcomes))
        except Exception:
            # An unresolved dynamic plan may still require its reserved share.
            # This includes planning callbacks that depend on an upstream stage
            # not admitted yet.
            total += stage.weight
            continue
        if planned != 0:
            total += stage.weight
    return total


def _default_ready(spec: StageSpec, outcomes: Mapping[str, StageOutcome]) -> bool:
    return all(
        outcomes[dependency].status == "completed" for dependency in spec.dependencies
    )


def _replace(
    outcomes: list[StageOutcome],
    index: int,
    outcome: StageOutcome,
    by_stage: Dict[str, StageOutcome],
) -> None:
    outcomes[index] = outcome
    by_stage[outcome.stage_id] = outcome


def _materialize_interrupted(
    stages: Tuple[StageSpec, ...],
    start: int,
    outcomes: list[StageOutcome],
    by_stage: Dict[str, StageOutcome],
    *,
    status: str = "interrupted",
    reason: str = "parent_cancelled",
    stop_cause: Optional[StopCause] = None,
) -> None:
    """Finalize every not-yet-admitted full-plan row without dispatching it."""
    for index in range(start, len(stages)):
        spec = stages[index]
        try:
            selected = bool(_resolve(spec.selected, by_stage))
            planned = (
                _planned_count(_resolve(spec.planned_units, by_stage))
                if selected
                else 0
            )
        except Exception:
            selected, planned = True, None
        if not selected:
            outcome = _outcome(
                spec,
                "not_selected",
                reason="source_not_selected",
                counts=_known_unattempted_counts(0),
            )
        elif planned == 0:
            outcome = _outcome(
                spec,
                "skipped_no_targets",
                reason="no_eligible_targets",
                counts=_known_unattempted_counts(0),
            )
        else:
            counts = (
                _known_unattempted_counts(planned)
                if planned is not None
                else StageCounts()
            )
            outcome = _outcome(
                spec,
                status,
                reason=reason,
                counts=counts,
                stop_cause=stop_cause,
            )
        _replace(outcomes, index, outcome, by_stage)


async def run_collection_stages(
    stages: Iterable[StageSpec],
    *,
    deadline: Optional[float] = None,
    remaining_seconds: Optional[float] = None,
    cancellation_check: StopCallback = lambda: False,
    event_sink: Callable[[Mapping[str, Any]], None] = lambda _event: None,
    on_progress: Optional[Callable[[Mapping[str, Any]], None]] = None,
    clock: Callable[[], float] = time.monotonic,
    cleanup_seconds: float = DEFAULT_CLEANUP_SECONDS,
) -> OrchestrationSummary:
    """Run stages sequentially while reserving time for selected later sources.

    Pass the claimed job's absolute monotonic ``deadline`` on resume. A source
    with a dynamic operation count may return ``None`` from ``planned_units``;
    its private :class:`OperationLedger` then supplies authoritative task
    counts once the adapter has built its actual site/check plan.
    """
    if (deadline is None) == (remaining_seconds is None):
        raise CollectionOrchestrationError(
            "Pass exactly one of deadline or remaining_seconds"
        )
    if (
        isinstance(cleanup_seconds, bool)
        or not isinstance(cleanup_seconds, (int, float))
        or not math.isfinite(cleanup_seconds)
        or cleanup_seconds < 0
        or cleanup_seconds > DEFAULT_CLEANUP_SECONDS
    ):
        raise CollectionOrchestrationError(
            "Cleanup time must be finite and between zero and five seconds"
        )
    started_at = clock()
    if not math.isfinite(started_at):
        raise CollectionOrchestrationError("Clock must return a finite monotonic value")
    if deadline is None:
        if not math.isfinite(float(remaining_seconds)):
            raise CollectionOrchestrationError("Remaining duration must be finite")
        absolute_deadline = started_at + max(0.0, float(remaining_seconds))
    else:
        absolute_deadline = float(deadline)
    if not math.isfinite(absolute_deadline):
        raise CollectionOrchestrationError("Deadline must be finite")
    stage_list = tuple(stages)
    _validate(stage_list)
    outcomes = [_outcome(spec, "unknown", reason="not_admitted") for spec in stage_list]
    by_stage: Dict[str, StageOutcome] = {}
    revision = 0

    def publish(state: str) -> None:
        nonlocal revision
        revision += 1
        event = _snapshot(outcomes, revision=revision, state=state)
        # Persist before notifying a lossy live stream.  Either failure is a
        # boundary failure and must stop admission rather than become a source
        # result.
        event_sink(event)
        if on_progress is not None:
            on_progress(event)

    publish("running")
    for index, spec in enumerate(stage_list):
        try:
            selected = bool(_resolve(spec.selected, by_stage))
        except Exception as error:
            _replace(
                outcomes,
                index,
                _outcome(spec, "failed", reason="selection_" + _safe_error_code(error)),
                by_stage,
            )
            publish("running")
            continue
        if not selected:
            _replace(
                outcomes,
                index,
                _outcome(
                    spec,
                    "not_selected",
                    reason="source_not_selected",
                    counts=_known_unattempted_counts(0),
                ),
                by_stage,
            )
            publish("running")
            continue
        try:
            ready = (
                _default_ready(spec, by_stage)
                if spec.readiness is None
                else bool(_resolve(spec.readiness, by_stage))
            )
        except Exception as error:
            _replace(
                outcomes,
                index,
                _outcome(
                    spec,
                    "failed",
                    reason="readiness_" + _safe_error_code(error),
                    counts=StageCounts(),
                ),
                by_stage,
            )
            publish("running")
            continue
        if not ready:
            try:
                planned = _planned_count(_resolve(spec.planned_units, by_stage))
            except Exception:
                planned = None
            pre_admission_counts = (
                _known_unattempted_counts(planned)
                if planned is not None
                else StageCounts()
            )
            _replace(
                outcomes,
                index,
                _outcome(
                    spec,
                    "blocked_dependency",
                    reason="dependency_not_ready",
                    counts=pre_admission_counts,
                ),
                by_stage,
            )
            publish("running")
            continue
        try:
            planned = _planned_count(_resolve(spec.planned_units, by_stage))
        except Exception as error:
            _replace(
                outcomes,
                index,
                _outcome(spec, "failed", reason="planning_" + _safe_error_code(error)),
                by_stage,
            )
            publish("running")
            continue
        if planned == 0:
            _replace(
                outcomes,
                index,
                _outcome(
                    spec,
                    "skipped_no_targets",
                    reason="no_eligible_targets",
                    counts=_known_unattempted_counts(0),
                ),
                by_stage,
            )
            publish("running")
            continue
        pre_admission_counts = (
            _known_unattempted_counts(planned) if planned is not None else StageCounts()
        )
        stop_cause, legacy_boolean = _normalize_stop_signal(cancellation_check())
        if stop_cause is not None:
            status = _stop_status(stop_cause)
            reason = _stop_reason(stop_cause, legacy_boolean=legacy_boolean)
            _replace(
                outcomes,
                index,
                _outcome(
                    spec,
                    status,
                    reason=reason,
                    counts=pre_admission_counts,
                    stop_cause=stop_cause,
                ),
                by_stage,
            )
            _materialize_interrupted(
                stage_list,
                index + 1,
                outcomes,
                by_stage,
                status=status,
                reason=reason,
                stop_cause=stop_cause,
            )
            publish(
                "cancelled"
                if stop_cause is StopCause.OPERATOR_CANCEL
                else "interrupted"
            )
            break
        available = absolute_deadline - clock()
        future_weight = _future_weight(stage_list, index, by_stage)
        allocation = (
            available * spec.weight / (spec.weight + future_weight)
            if available > 0
            else 0.0
        )
        if allocation <= 0:
            _replace(
                outcomes,
                index,
                _outcome(
                    spec,
                    "not_started_budget",
                    reason=_stop_reason(StopCause.JOB_DEADLINE),
                    counts=pre_admission_counts,
                    stop_cause=StopCause.JOB_DEADLINE,
                ),
                by_stage,
            )
            publish("running")
            continue

        ledger = OperationLedger()
        progress_active = True
        progress_failures: list[BaseException] = []
        reported_progress: Optional[StageCounts] = None
        last_progress_counts: Optional[StageCounts] = None

        def publish_stage_progress(counts: Optional[StageCounts]) -> bool:
            nonlocal last_progress_counts, reported_progress
            if not progress_active:
                return False
            if counts is not None:
                reported_progress = counts
            last_progress_counts = _progress_counts(reported_progress, ledger, planned)
            _replace(
                outcomes,
                index,
                _outcome(
                    spec,
                    "running",
                    counts=last_progress_counts,
                ),
                by_stage,
            )
            try:
                publish("running")
            except BaseException as error:
                progress_failures.append(error)
                raise
            return True

        context = StageContext(
            spec.stage_id,
            spec.engine_id,
            clock() + allocation,
            cancellation_check,
            ledger,
            clock,
            publish_stage_progress,
        )
        _replace(
            outcomes,
            index,
            _outcome(spec, "running", counts=StageCounts(planned=planned)),
            by_stage,
        )
        publish("running")
        try:
            status, result, reason, cleanup_complete, stop_cause = await _run_callback(
                spec,
                context,
                cancellation_check=cancellation_check,
                clock=clock,
                cleanup_seconds=cleanup_seconds,
                progress_failures=progress_failures,
            )
        except asyncio.CancelledError as error:
            progress_active = False
            cleanup_complete = getattr(error, "cleanup_complete", True)
            if cleanup_complete:
                parent_stop_cause, legacy_boolean = _normalize_stop_signal(
                    cancellation_check()
                )
                parent_reason = (
                    _stop_reason(parent_stop_cause, legacy_boolean=legacy_boolean)
                    if parent_stop_cause is not None
                    else "parent_cancelled"
                )
            else:
                parent_stop_cause = StopCause.CLEANUP_INCOMPLETE
                parent_reason = StopCause.CLEANUP_INCOMPLETE.value
            stopped_result = getattr(error, "result", None)
            if (
                stopped_result is None
                and cleanup_complete
                and last_progress_counts is not None
            ):
                stopped_result = StageResult(counts=last_progress_counts)
            counts = _merge_counts(stopped_result, ledger, planned, interrupted=True)
            _replace(
                outcomes,
                index,
                _outcome(
                    spec,
                    "interrupted",
                    reason=parent_reason,
                    counts=counts,
                    cleanup_complete=cleanup_complete,
                    stop_cause=parent_stop_cause,
                ),
                by_stage,
            )
            _materialize_interrupted(
                stage_list,
                index + 1,
                outcomes,
                by_stage,
                reason=parent_reason,
                stop_cause=parent_stop_cause,
            )
            try:
                publish("interrupted")
            except BaseException as publish_error:
                # Parent cancellation remains observable even if final durable
                # accounting cannot be written.
                raise error from publish_error
            raise
        except _ProgressSinkFailure as error:
            progress_active = False
            # Sealing blocks a callback that ignored cancellation after the
            # bounded cleanup window.  Do not attempt another durable event.
            _merge_counts(None, ledger, planned, interrupted=True)
            raise error.error
        except BaseException:
            progress_active = False
            _merge_counts(None, ledger, planned, interrupted=True)
            raise
        progress_active = False
        counts = _merge_counts(
            result, ledger, planned, interrupted=status == "interrupted"
        )
        if not cleanup_complete:
            status, reason, stop_cause = (
                "interrupted",
                StopCause.CLEANUP_INCOMPLETE.value,
                StopCause.CLEANUP_INCOMPLETE,
            )
        _replace(
            outcomes,
            index,
            _outcome(
                spec,
                status,
                reason=reason,
                counts=counts,
                cleanup_complete=cleanup_complete,
                stop_cause=stop_cause,
                value=result.value if result is not None else None,
            ),
            by_stage,
        )
        if not cleanup_complete:
            _materialize_interrupted(
                stage_list,
                index + 1,
                outcomes,
                by_stage,
                reason=StopCause.CLEANUP_INCOMPLETE.value,
                stop_cause=StopCause.CLEANUP_INCOMPLETE,
            )
            publish("interrupted")
            break
        publish("running")
    summary = OrchestrationSummary(
        absolute_deadline, started_at, clock(), tuple(outcomes), revision + 1
    )
    publish(summary.state)
    return OrchestrationSummary(
        absolute_deadline, started_at, clock(), tuple(outcomes), revision
    )
