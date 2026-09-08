# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import asyncio

import pytest

from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_contract import (
    ProfileSearchError,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
)
from maigret.web.profile_search_orchestrator import (
    ProfileSearchOrchestrationError,
    ProfileSearchOrchestrator,
)


PLATFORM_URLS = {
    "facebook": "https://www.facebook.com/alice_example",
    "instagram": "https://www.instagram.com/alice_example/",
    "threads": "https://www.threads.com/@alice_example",
    "tiktok": "https://www.tiktok.com/@alice_example",
    "x": "https://x.com/alice_example",
}


def _plan():
    return {
        "identifiers": [],
        "search_targets": [
            {
                "value": "alice_example",
                "source_type": "username",
            }
        ],
    }


def _success(query, *, rank=1):
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T12:00:00Z",
        provider_request_id=f"request-{query.platform}",
    )
    return ProfileSearchRun(
        query=query,
        provenance=provenance,
        evidence=(
            ProfileSearchEvidence(
                result_rank=rank,
                source_url=PLATFORM_URLS[query.platform],
                title="Alice Example",
            ),
        ),
    )


def _failure(query, *, code="rate_limited"):
    return ProfileSearchRun(
        query=query,
        provenance=None,
        evidence=(),
        error=ProfileSearchError(
            query_id=query.query_id,
            provider="brave",
            code=code,
            message="Search provider request failed.",
            retryable=True,
            occurred_at="2026-09-08T12:00:00Z",
            http_status=429,
        ),
    )


class _Client:
    def __init__(self, response):
        self.response = response
        self.queries = []

    async def search(self, query):
        self.queries.append(query)
        return self.response(query, len(self.queries))


@pytest.mark.asyncio
async def test_orchestrator_runs_fair_plan_then_merges_and_ranks():
    client = _Client(lambda query, _index: _success(query))

    result = await ProfileSearchOrchestrator(client).discover(
        _plan(), platforms=("instagram", "x"), max_results=3
    )

    assert result.status == "completed"
    assert result.stopped is False
    assert result.planned_query_count == 2
    assert result.executed_query_count == 2
    assert result.skipped_query_count == 0
    assert result.error_count == 0
    assert [query.platform for query in client.queries] == [
        "instagram",
        "x",
    ]
    assert [item.candidate.platform for item in result.candidates] == [
        "instagram",
        "x",
    ]
    assert all(
        item.candidate.identity_status == "unverified"
        for item in result.candidates
    )


@pytest.mark.asyncio
async def test_orchestrator_retains_partial_errors_and_continues_plan():
    def response(query, index):
        return _failure(query) if index == 1 else _success(query)

    client = _Client(response)
    result = await ProfileSearchOrchestrator(client).discover(
        _plan(), platforms=("instagram", "x")
    )

    assert result.status == "partial"
    assert result.executed_query_count == 2
    assert result.error_count == 1
    assert len(result.candidates) == 1
    assert result.candidates[0].candidate.platform == "x"


@pytest.mark.asyncio
async def test_orchestrator_reports_failed_when_every_run_has_an_error():
    client = _Client(lambda query, _index: _failure(query))

    result = await ProfileSearchOrchestrator(client).discover(
        _plan(), platforms=("instagram", "x")
    )

    assert result.status == "failed"
    assert result.error_count == 2
    assert result.candidates == ()


@pytest.mark.asyncio
async def test_cancellation_stops_before_next_call_and_keeps_partial_work():
    client = _Client(lambda query, _index: _success(query))
    checks = iter((False, True))

    result = await ProfileSearchOrchestrator(client).discover(
        _plan(),
        platforms=("facebook", "instagram", "threads"),
        cancellation_check=lambda: next(checks),
    )

    assert result.status == "stopped"
    assert result.stopped is True
    assert result.planned_query_count == 3
    assert result.executed_query_count == 1
    assert result.skipped_query_count == 2
    assert len(result.candidates) == 1
    assert [query.platform for query in client.queries] == ["facebook"]


@pytest.mark.asyncio
async def test_inflight_task_cancellation_returns_a_stopped_partial_result():
    completed = []

    class _CancellableClient:
        async def search(self, query):
            if not completed:
                completed.append(query)
                return _success(query)
            raise asyncio.CancelledError

    result = await ProfileSearchOrchestrator(_CancellableClient()).discover(
        _plan(), platforms=("facebook", "instagram", "threads")
    )

    assert result.status == "stopped"
    assert result.stopped is True
    assert result.executed_query_count == 1
    assert result.skipped_query_count == 2
    assert len(result.candidates) == 1


@pytest.mark.asyncio
async def test_provider_wide_error_stops_remaining_queries_without_retry():
    client = _Client(
        lambda query, _index: _failure(query, code="credential_rejected")
    )

    result = await ProfileSearchOrchestrator(client).discover(
        _plan(), platforms=("facebook", "instagram", "threads")
    )

    assert result.status == "failed"
    assert result.executed_query_count == 1
    assert result.skipped_query_count == 2
    assert len(client.queries) == 1


@pytest.mark.asyncio
async def test_empty_plan_completes_without_calling_provider():
    client = _Client(lambda query, _index: _success(query))

    result = await ProfileSearchOrchestrator(client).discover(
        {"identifiers": [], "search_targets": []},
        platforms=("instagram",),
    )

    assert result.status == "completed"
    assert result.planned_query_count == 0
    assert result.executed_query_count == 0
    assert result.candidates == ()
    assert client.queries == []


@pytest.mark.asyncio
async def test_result_serialization_is_bounded_and_review_safe():
    client = _Client(lambda query, _index: _success(query))

    result = await ProfileSearchOrchestrator(client).discover(
        _plan(), platforms=("instagram",), max_queries=1
    )
    serialized = result.as_dict()

    assert serialized["orchestration_version"] == 1
    assert serialized["planned_query_count"] == 1
    assert serialized["executed_query_count"] == 1
    assert serialized["candidate_count"] == 1
    assert serialized["candidates"][0]["score_scope"] == (
        "discovery_review_priority"
    )
    assert serialized["candidates"][0]["identity_status"] == "unverified"
    assert serialized["candidates"][0]["review_status"] == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "message"),
    [
        (lambda query: object(), "invalid run"),
        (
            lambda query: ProfileSearchRun(query, None, ()),
            "lacks provenance",
        ),
        (
            lambda query: ProfileSearchRun(
                query,
                ProfileSearchProvenance.for_query(
                    query,
                    provider="brave",
                    retrieved_at="2026-09-08T12:00:00Z",
                ),
                [
                    ProfileSearchEvidence(
                        result_rank=1,
                        source_url=PLATFORM_URLS[query.platform],
                    )
                ],
            ),
            "must be immutable",
        ),
    ],
)
async def test_orchestrator_rejects_malformed_client_runs(response, message):
    client = _Client(lambda query, _index: response(query))

    with pytest.raises(ProfileSearchOrchestrationError, match=message):
        await ProfileSearchOrchestrator(client).discover(
            _plan(), platforms=("instagram",), max_queries=1
        )


@pytest.mark.asyncio
async def test_orchestrator_rejects_a_run_for_a_different_query():
    first_query = None

    def response(query, index):
        nonlocal first_query
        if index == 1:
            first_query = query
            return _success(query)
        return _success(first_query)

    client = _Client(response)

    with pytest.raises(ProfileSearchOrchestrationError, match="planned query"):
        await ProfileSearchOrchestrator(client).discover(
            _plan(), platforms=("instagram", "x")
        )
