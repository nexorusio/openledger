import asyncio
import json
from datetime import datetime, timezone

import pytest

from maigret.web.investigation_input import build_investigation_plan
from maigret.web.pipeline_query import build_query_plan
from maigret.web.pipeline_public_search import collect_public_exact_matches
from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_contract import (
    ProfileSearchEvidence,
    ProfileSearchError,
    ProfileSearchProvenance,
)


def task_for(kind="email", value="test@example.test"):
    plan = build_investigation_plan(
        {"identifier_type": [kind], "identifier_value": [value]}
    )
    query = build_query_plan(
        plan,
        case_id="case-a",
        subject_id="subject-a",
        request_id="request-a",
        source_status={"public_search": {"enabled": True}},
    )
    return next(
        task for task in query["tasks"] if task["engine_id"] == "public_exact_match"
    )


class FakeClient:
    def __init__(self, evidence=(), error=None):
        self.evidence = evidence
        self.error = error
        self.queries = []

    async def search(self, query):
        self.queries.append(query)
        if isinstance(self.error, BaseException):
            raise self.error
        return ProfileSearchRun(
            query=query,
            provenance=ProfileSearchProvenance.for_query(
                query,
                provider="searxng",
                retrieved_at=datetime.now(timezone.utc),
                provider_request_id="fixture-only",
            ),
            evidence=tuple(self.evidence),
            error=(
                ProfileSearchError(
                    query_id=query.query_id,
                    provider="searxng",
                    code=self.error,
                    message="Provider blocked",
                    retryable=True,
                    occurred_at=datetime.now(timezone.utc),
                )
                if self.error
                else None
            ),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,value,snippet",
    [
        (
            "email",
            "test+research@example.test",
            "Contact test+research@example.test for the project",
        ),
        ("phone", "+628123456789", "Public office telephone: +62 (812) 3456-789"),
        ("full_name", "Test Person", "Test Person is named on this page"),
    ],
)
async def test_exact_mentions_preserve_provider_lineage_as_unverified_candidates(
    kind, value, snippet
):
    client = FakeClient(
        [ProfileSearchEvidence(1, "https://example.test/public", snippet=snippet)]
    )
    task = task_for(kind, value)
    result = await collect_public_exact_matches(task, client=client)
    assert result["outcome"] == "candidate"
    assert result["request_count"] == 1
    assert client.queries[0].query_text == f'"{value}"'
    observation = result["observations"][0]
    assert observation["identity_status"] == "unverified"
    assert observation["probability"] is None
    assert observation["eligible_for_assessment"] is True
    assert observation["source_dependence"] == "search_excerpt_of_source"
    assert observation["provenance"]["provider_request_id"] == "fixture-only"
    assert observation["case_id"] == task["case_id"]
    assert observation["input_id"] == task["input_id"]


@pytest.mark.asyncio
async def test_unrelated_provider_result_is_retained_as_inconclusive_not_claimed_match():
    client = FakeClient(
        [
            ProfileSearchEvidence(
                1, "https://example.test/public", snippet="othertest@example.test"
            )
        ]
    )
    result = await collect_public_exact_matches(task_for(), client=client)
    assert result["outcome"] == "inconclusive"
    assert len(result["observations"]) == 1
    assert result["observations"][0]["eligible_for_assessment"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,outcome",
    [
        ("rate_limited", "blocked"),
        ("credential_rejected", "blocked"),
        ("circuit_open", "blocked"),
        ("provider_unavailable", "error"),
        (TimeoutError(), "timeout"),
        (RuntimeError("secret=do-not-leak"), "error"),
    ],
)
async def test_provider_failures_never_become_negative_account_findings(error, outcome):
    result = await collect_public_exact_matches(
        task_for(), client=FakeClient(error=error)
    )
    assert result["outcome"] == outcome
    assert result["observations"] == []
    assert "do-not-leak" not in repr(result)


@pytest.mark.asyncio
async def test_negative_is_only_a_successful_empty_provider_query():
    result = await collect_public_exact_matches(task_for(), client=FakeClient())
    assert result["outcome"] == "not_found"
    assert "bounded public-index query" in result["diagnostic"]
    assert result["query"]["seed_kind"] == "email"


@pytest.mark.asyncio
async def test_nonactive_routes_and_cancelled_queries_never_call_provider():
    client = FakeClient()
    task = task_for()
    for changes in ({"route_state": "unavailable"}, {"request_budget": 0}):
        result = await collect_public_exact_matches({**task, **changes}, client=client)
        assert result["outcome"] == "not_executed"
    assert (
        await collect_public_exact_matches(task, client=client, cancelled=lambda: True)
    )["outcome"] == "cancelled"
    assert client.queries == []


@pytest.mark.asyncio
async def test_ambiguous_national_number_does_not_escape_country_prerequisite():
    client = FakeClient()
    task = task_for("phone", "08123456789")
    task["route_state"] = "active"  # even a corrupt persisted plan cannot bypass it
    result = await collect_public_exact_matches(task, client=client)
    assert result["outcome"] == "not_executed"
    assert "country context" in result["diagnostic"]
    assert client.queries == []


@pytest.mark.asyncio
async def test_cancel_during_provider_request_has_explicit_outcome():
    result = await collect_public_exact_matches(
        task_for(), client=FakeClient(error=asyncio.CancelledError())
    )
    assert result["outcome"] == "cancelled"


@pytest.mark.asyncio
async def test_repeated_origin_stays_the_same_evidence_family_but_keeps_observations():
    client = FakeClient(
        [
            ProfileSearchEvidence(
                1, "https://example.test/contact#first", snippet="test@example.test"
            ),
            ProfileSearchEvidence(
                2, "https://example.test/contact#second", snippet="test@example.test"
            ),
        ]
    )
    result = await collect_public_exact_matches(task_for(), client=client)
    first, second = result["observations"]
    assert first["source_origin_family"] == second["source_origin_family"]
    assert first["source_record_id"] != second["source_record_id"]


@pytest.mark.asyncio
async def test_public_query_runs_through_existing_real_searxng_backend():
    from maigret.web.profile_search_backend import (
        ProfileSearchClient,
        load_profile_search_config,
    )
    from tests.test_profile_search_backend import _Response, _Session

    capture = {}
    response = _Response(
        200,
        json.dumps(
            {
                "results": [
                    {
                        "url": "https://example.test/contact",
                        "title": "Public contact",
                        "content": "test@example.test",
                    }
                ]
            }
        ).encode(),
    )
    config = load_profile_search_config(
        {"OPENLEDGER_PROFILE_SEARCH_PROVIDER": "searxng"}
    )
    client = ProfileSearchClient(
        config, session_factory=lambda **kwargs: _Session(response, capture)
    )
    result = await collect_public_exact_matches(task_for(), client=client)
    assert result["outcome"] == "candidate"
    assert capture["request"]["params"]["q"] == '"test@example.test"'
    assert result["observations"][0]["provenance"]["provider"] == "searxng"
