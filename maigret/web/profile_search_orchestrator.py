"""Bounded orchestration for native search-first profile discovery."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Optional, Sequence, Tuple

from maigret.web.profile_search_backend import (
    ProfileSearchClient,
    ProfileSearchRun,
)
from maigret.web.profile_search_candidates import merge_profile_search_runs
from maigret.web.profile_search_contract import (
    MAX_PROFILE_SEARCH_RESULTS,
    ProfileSearchQuery,
)
from maigret.web.profile_search_planner import (
    MAX_PROFILE_SEARCH_QUERIES,
    plan_profile_search_queries,
)
from maigret.web.profile_search_ranking import (
    RankedProfileSearchCandidate,
    rank_profile_search_candidates,
)

PROFILE_SEARCH_ORCHESTRATION_VERSION = 1
PROFILE_SEARCH_TERMINAL_ERROR_CODES = frozenset(
    {
        "circuit_open",
        "credential_rejected",
        "invalid_response",
        "oversized_response",
        "provider_error",
    }
)


class ProfileSearchOrchestrationError(RuntimeError):
    """Raised when an execution violates the profile-search run contract."""


@dataclass(frozen=True)
class ProfileSearchDiscoveryResult:
    """Bounded in-memory result for one native profile-search operation."""

    status: str
    queries: Tuple[ProfileSearchQuery, ...]
    runs: Tuple[ProfileSearchRun, ...]
    candidates: Tuple[RankedProfileSearchCandidate, ...]
    stopped: bool = False
    final: bool = True
    active_query_count: int = 0
    interrupted_query_count: int = 0
    orchestration_version: int = field(
        default=PROFILE_SEARCH_ORCHESTRATION_VERSION,
        init=False,
    )

    @property
    def planned_query_count(self) -> int:
        return len(self.queries)

    @property
    def executed_query_count(self) -> int:
        return len(self.runs)

    @property
    def skipped_query_count(self) -> int:
        """Legacy count of planned queries without a completed run.

        This includes active and interrupted queries. New accounting consumers
        should use ``unattempted_query_count`` and the explicit active or
        interrupted counts instead.
        """
        return self.planned_query_count - self.executed_query_count

    @property
    def attempted_query_count(self) -> int:
        """Queries dispatched to the provider, including unfinished work."""
        return (
            self.executed_query_count
            + self.active_query_count
            + self.interrupted_query_count
        )

    @property
    def unattempted_query_count(self) -> int:
        """Planned queries that were never dispatched to the provider."""
        return self.planned_query_count - self.attempted_query_count

    @property
    def error_count(self) -> int:
        return sum(run.error is not None for run in self.runs)

    def __post_init__(self) -> None:
        for value in (
            self.active_query_count,
            self.interrupted_query_count,
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProfileSearchOrchestrationError(
                    "Profile-search unfinished query counts must be non-negative integers"
                )
        if self.active_query_count > 1:
            raise ProfileSearchOrchestrationError(
                "Profile-search execution has more than one active query"
            )
        if self.attempted_query_count > self.planned_query_count:
            raise ProfileSearchOrchestrationError(
                "Profile-search attempted query count exceeds the plan"
            )
        if self.final and self.active_query_count:
            raise ProfileSearchOrchestrationError(
                "Final profile-search result has an active query"
            )
        if self.status == "running":
            if self.final or self.stopped:
                raise ProfileSearchOrchestrationError(
                    "Running profile-search result must be non-final and unstopped"
                )
        elif not self.final:
            raise ProfileSearchOrchestrationError(
                "Non-final profile-search result must be running"
            )
        if self.stopped != (self.status == "stopped"):
            raise ProfileSearchOrchestrationError(
                "Profile-search stop status is inconsistent"
            )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "orchestration_version": self.orchestration_version,
            "status": self.status,
            "stopped": self.stopped,
            "final": self.final,
            "planned_query_count": self.planned_query_count,
            "executed_query_count": self.executed_query_count,
            "attempted_query_count": self.attempted_query_count,
            "skipped_query_count": self.skipped_query_count,
            "unattempted_query_count": self.unattempted_query_count,
            "active_query_count": self.active_query_count,
            "interrupted_query_count": self.interrupted_query_count,
            "error_count": self.error_count,
            "candidate_count": len(self.candidates),
            "queries": [query.as_dict() for query in self.queries],
            "runs": [run.as_dict() for run in self.runs],
            "candidates": [candidate.as_dict() for candidate in self.candidates],
        }


def _validate_run(query: ProfileSearchQuery, run: Any) -> ProfileSearchRun:
    if not isinstance(run, ProfileSearchRun):
        raise ProfileSearchOrchestrationError(
            "Profile-search client returned an invalid run"
        )
    if run.query != query:
        raise ProfileSearchOrchestrationError(
            "Profile-search run does not match its planned query"
        )
    if not isinstance(run.evidence, tuple):
        raise ProfileSearchOrchestrationError(
            "Profile-search run evidence must be immutable"
        )
    if len(run.evidence) > query.max_results:
        raise ProfileSearchOrchestrationError(
            "Profile-search run exceeded its result limit"
        )
    if run.error is not None:
        if (
            run.error.query_id != query.query_id
            or run.provenance is not None
            or run.evidence
        ):
            raise ProfileSearchOrchestrationError(
                "Failed profile-search run has conflicting lineage"
            )
        return run
    if run.provenance is None:
        raise ProfileSearchOrchestrationError(
            "Successful profile-search run lacks provenance"
        )
    if (
        run.provenance.query_id != query.query_id
        or run.provenance.query_fingerprint != query.fingerprint
    ):
        raise ProfileSearchOrchestrationError(
            "Successful profile-search run has conflicting lineage"
        )
    return run


def _result_status(runs: Tuple[ProfileSearchRun, ...], *, stopped: bool) -> str:
    if stopped:
        return "stopped"
    error_count = sum(run.error is not None for run in runs)
    if runs and error_count == len(runs):
        return "failed"
    if error_count:
        return "partial"
    return "completed"


def _discovery_result(
    queries: Tuple[ProfileSearchQuery, ...],
    runs: Sequence[ProfileSearchRun],
    *,
    stopped: bool = False,
    final: bool = True,
    active_query_count: int = 0,
    interrupted_query_count: int = 0,
) -> ProfileSearchDiscoveryResult:
    """Build one typed final result or durable in-progress checkpoint."""
    immutable_runs = tuple(runs)
    groups = merge_profile_search_runs(immutable_runs)
    ranked = rank_profile_search_candidates(groups, queries=queries)
    return ProfileSearchDiscoveryResult(
        status=(
            _result_status(immutable_runs, stopped=stopped) if final else "running"
        ),
        stopped=stopped,
        final=final,
        active_query_count=active_query_count,
        interrupted_query_count=interrupted_query_count,
        queries=queries,
        runs=immutable_runs,
        candidates=ranked,
    )


class ProfileSearchOrchestrator:
    """Plan, execute, merge, and rank without persistence or retries."""

    def __init__(self, client: ProfileSearchClient) -> None:
        self.client = client

    async def discover(
        self,
        investigation_plan: Any,
        *,
        existing_evidence: Iterable[Any] = (),
        platforms: Optional[Sequence[Any]] = None,
        max_queries: int = MAX_PROFILE_SEARCH_QUERIES,
        max_results: int = MAX_PROFILE_SEARCH_RESULTS,
        cancellation_check: Optional[Callable[[], bool]] = None,
        progress_sink: Optional[Callable[[ProfileSearchDiscoveryResult], Any]] = None,
        result_sink: Optional[Callable[[ProfileSearchDiscoveryResult], Any]] = None,
    ) -> ProfileSearchDiscoveryResult:
        """Execute a deterministic plan and retain partial provider results.

        ``progress_sink`` receives bounded, typed, non-final checkpoints both
        before dispatching a query and after a completed provider run. The
        pre-dispatch checkpoint marks its one active query, so a later worker
        interruption cannot make attempted work appear unattempted.
        ``result_sink`` receives the single final result. Sink failures
        propagate to the caller and are never converted into search outcomes.
        """
        queries = tuple(
            plan_profile_search_queries(
                investigation_plan,
                existing_evidence=existing_evidence,
                platforms=platforms,
                max_queries=max_queries,
                max_results=max_results,
            )
        )
        runs = []
        stopped = False
        interrupted_query_count = 0

        def publish_progress(*, active_query_count: int) -> None:
            if progress_sink is not None:
                progress_sink(
                    _discovery_result(
                        queries,
                        runs,
                        final=False,
                        active_query_count=active_query_count,
                    )
                )

        for query in queries:
            if cancellation_check is not None and cancellation_check():
                stopped = True
                break
            publish_progress(active_query_count=1)
            try:
                run = await self.client.search(query)
            except asyncio.CancelledError:
                # A worker stop can interrupt an in-flight request. Preserve
                # completed runs so the caller can persist a stopped audit.
                stopped = True
                interrupted_query_count = 1
                break
            validated_run = _validate_run(query, run)
            runs.append(validated_run)
            publish_progress(active_query_count=0)
            if (
                validated_run.error is not None
                and validated_run.error.code in PROFILE_SEARCH_TERMINAL_ERROR_CODES
            ):
                # Provider-wide failures cannot improve on later queries.
                # Stop without retries while preserving the failed run lineage.
                break

        result = _discovery_result(
            queries,
            runs,
            stopped=stopped,
            interrupted_query_count=interrupted_query_count,
        )
        if result_sink is not None:
            result_sink(result)
        return result
