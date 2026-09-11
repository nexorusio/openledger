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
        return self.planned_query_count - self.executed_query_count

    @property
    def error_count(self) -> int:
        return sum(run.error is not None for run in self.runs)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "orchestration_version": self.orchestration_version,
            "status": self.status,
            "stopped": self.stopped,
            "planned_query_count": self.planned_query_count,
            "executed_query_count": self.executed_query_count,
            "skipped_query_count": self.skipped_query_count,
            "error_count": self.error_count,
            "candidate_count": len(self.candidates),
            "queries": [query.as_dict() for query in self.queries],
            "runs": [run.as_dict() for run in self.runs],
            "candidates": [
                candidate.as_dict() for candidate in self.candidates
            ],
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


def _result_status(
    runs: Tuple[ProfileSearchRun, ...], *, stopped: bool
) -> str:
    if stopped:
        return "stopped"
    error_count = sum(run.error is not None for run in runs)
    if runs and error_count == len(runs):
        return "failed"
    if error_count:
        return "partial"
    return "completed"


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
    ) -> ProfileSearchDiscoveryResult:
        """Execute a deterministic plan and retain partial provider results."""
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
        for query in queries:
            if cancellation_check is not None and cancellation_check():
                stopped = True
                break
            try:
                run = await self.client.search(query)
            except asyncio.CancelledError:
                # A worker stop can interrupt an in-flight request. Preserve
                # completed runs so the caller can persist a stopped audit.
                stopped = True
                break
            validated_run = _validate_run(query, run)
            runs.append(validated_run)
            if (
                validated_run.error is not None
                and validated_run.error.code
                in PROFILE_SEARCH_TERMINAL_ERROR_CODES
            ):
                # Provider-wide failures cannot improve on later queries.
                # Stop without retries while preserving the failed run lineage.
                break

        immutable_runs = tuple(runs)
        groups = merge_profile_search_runs(immutable_runs)
        ranked = rank_profile_search_candidates(groups, queries=queries)
        return ProfileSearchDiscoveryResult(
            status=_result_status(immutable_runs, stopped=stopped),
            stopped=stopped,
            queries=queries,
            runs=immutable_runs,
            candidates=ranked,
        )
