# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import json
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import func, select, update

from maigret.web.case_store import (
    CaseStore,
    MAX_PROFILE_SEARCH_AUDITS_PER_JOB,
    investigation_jobs,
    profile_search_audits,
    utcnow,
)
from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_contract import (
    ProfileSearchError,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
)
from maigret.web.profile_search_orchestrator import ProfileSearchOrchestrator


@pytest.fixture
def store(tmp_path):
    instance = CaseStore(
        f"sqlite:///{tmp_path / 'profile-search.db'}",
        create_schema=True,
    )
    yield instance
    instance.dispose()


def _plan():
    return {
        "identifiers": [],
        "search_targets": [
            {"value": "alice_example", "source_type": "username"}
        ],
    }


class _Client:
    def __init__(self, *, fail_platform=None, rank=1):
        self.fail_platform = fail_platform
        self.rank = rank

    async def search(self, query):
        if query.platform == self.fail_platform:
            return ProfileSearchRun(
                query=query,
                provenance=None,
                evidence=(),
                error=ProfileSearchError(
                    query_id=query.query_id,
                    provider="brave",
                    code="rate_limited",
                    message="Search provider rate limit was reached.",
                    retryable=True,
                    occurred_at="2026-09-08T13:00:00Z",
                    http_status=429,
                ),
            )
        provenance = ProfileSearchProvenance.for_query(
            query,
            provider="brave",
            retrieved_at="2026-09-08T13:00:00Z",
            provider_request_id=f"request-{query.platform}",
        )
        source_url = {
            "instagram": "https://instagram.com/alice_example/?ref=search",
            "x": "https://twitter.com/alice_example/status/12345?s=20",
        }[query.platform]
        return ProfileSearchRun(
            query=query,
            provenance=provenance,
            evidence=(
                ProfileSearchEvidence(
                    result_rank=self.rank,
                    source_url=source_url,
                    title="Alice Example",
                    snippet="Public profile search result.",
                ),
            ),
        )


async def _result(*, fail_platform=None, rank=1):
    return await ProfileSearchOrchestrator(
        _Client(fail_platform=fail_platform, rank=rank)
    ).discover(_plan(), platforms=("instagram", "x"))


def _claimed_job(store, worker_id="worker:profile-search"):
    job_id = store.create_investigation(["alice"], {})
    claimed = store.claim_next(worker_id)
    assert claimed["job_id"] == job_id
    return job_id, worker_id


@pytest.mark.asyncio
async def test_complete_search_audit_is_durable_complete_and_idempotent(store):
    job_id, worker_id = _claimed_job(store)
    result = await _result()

    first_id = store.record_profile_search_result(
        job_id, result, worker_id=worker_id
    )
    second_id = store.record_profile_search_result(
        job_id, result, worker_id=worker_id
    )
    audits = store.list_profile_search_audits(job_id)

    assert first_id == second_id
    assert len(audits) == 1
    assert audits[0]["id"] == first_id
    assert audits[0]["status"] == "completed"
    assert audits[0]["planned_query_count"] == 2
    assert audits[0]["executed_query_count"] == 2
    assert audits[0]["error_count"] == 0
    assert audits[0]["candidate_count"] == 2
    assert len(audits[0]["document_sha256"]) == 64
    assert store.get_profile_search_audit(job_id, first_id) == audits[0]
    events = [
        item["event"]
        for item in store.get_events(job_id)
        if item["event"]["type"] == "profile_search_audit"
    ]
    assert len(events) == 1
    assert "alice_example" not in json.dumps(events[0])


@pytest.mark.asyncio
async def test_audit_retains_queries_errors_raw_evidence_and_ranked_candidates(
    store,
):
    job_id, worker_id = _claimed_job(store)
    result = await _result(fail_platform="x")

    audit_id = store.record_profile_search_result(
        job_id, result, worker_id=worker_id
    )
    document = store.get_profile_search_audit(job_id, audit_id)["document"]

    assert document["status"] == "partial"
    assert document["error_count"] == 1
    assert len(document["queries"]) == 2
    assert document["runs"][1]["error"]["code"] == "rate_limited"
    assert document["runs"][0]["evidence"][0]["source_url"] == (
        "https://instagram.com/alice_example/?ref=search"
    )
    candidate = document["candidates"][0]
    assert candidate["profile_url"] == (
        "https://www.instagram.com/alice_example/"
    )
    assert candidate["identity_status"] == "unverified"
    assert candidate["review_status"] == "pending"
    assert candidate["score_scope"] == "discovery_review_priority"
    assert candidate["observations"][0]["provenance"]["provider"] == (
        "brave"
    )


@pytest.mark.asyncio
async def test_fail_fast_provider_error_persists_skipped_plan(store):
    class _CredentialFailureClient:
        async def search(self, query):
            return ProfileSearchRun(
                query=query,
                provenance=None,
                evidence=(),
                error=ProfileSearchError(
                    query_id=query.query_id,
                    provider="brave",
                    code="credential_rejected",
                    message="Search provider rejected its server credential.",
                    retryable=False,
                    occurred_at="2026-09-08T13:00:00Z",
                    http_status=401,
                ),
            )

    job_id, worker_id = _claimed_job(store)
    result = await ProfileSearchOrchestrator(
        _CredentialFailureClient()
    ).discover(_plan(), platforms=("instagram", "x"))

    assert result.status == "failed"
    assert result.executed_query_count == 1
    assert result.skipped_query_count == 1
    audit_id = store.record_profile_search_result(
        job_id, result, worker_id=worker_id
    )
    assert store.get_profile_search_audit(job_id, audit_id)[
        "executed_query_count"
    ] == 1


@pytest.mark.asyncio
async def test_changed_result_appends_without_overwriting_history(store):
    job_id, worker_id = _claimed_job(store)
    first = await _result(rank=1)
    second = await _result(rank=2)

    first_id = store.record_profile_search_result(
        job_id, first, worker_id=worker_id
    )
    second_id = store.record_profile_search_result(
        job_id, second, worker_id=worker_id
    )
    audits = store.list_profile_search_audits(job_id)

    assert first_id != second_id
    assert len(audits) == 2
    assert {item["id"] for item in audits} == {first_id, second_id}
    stored_ranks = {
        item["document"]["runs"][0]["evidence"][0]["result_rank"]
        for item in audits
    }
    assert stored_ranks == {1, 2}


@pytest.mark.asyncio
async def test_append_only_audit_history_has_a_hard_storage_bound(store):
    job_id, worker_id = _claimed_job(store)
    for rank in range(1, MAX_PROFILE_SEARCH_AUDITS_PER_JOB + 1):
        result = await _result(rank=rank)
        assert store.record_profile_search_result(
            job_id, result, worker_id=worker_id
        )

    with pytest.raises(ValueError, match="storage limit"):
        store.record_profile_search_result(
            job_id,
            await _result(rank=MAX_PROFILE_SEARCH_AUDITS_PER_JOB + 1),
            worker_id=worker_id,
        )
    assert len(store.list_profile_search_audits(job_id, limit=100)) == (
        MAX_PROFILE_SEARCH_AUDITS_PER_JOB
    )


@pytest.mark.asyncio
async def test_wrong_or_expired_worker_cannot_publish_search_audit(store):
    job_id, worker_id = _claimed_job(store)
    result = await _result()

    assert store.record_profile_search_result(job_id, result) is None
    assert (
        store.record_profile_search_result(
            job_id, result, worker_id="worker:wrong"
        )
        is None
    )
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(heartbeat_at=utcnow() - timedelta(seconds=31))
        )
    assert (
        store.record_profile_search_result(
            job_id, result, worker_id=worker_id
        )
        is None
    )
    assert store.list_profile_search_audits(job_id) == []


@pytest.mark.asyncio
async def test_non_discovery_job_cannot_receive_a_search_audit(store):
    job_id = store.create_investigation(
        ["alice"], {}, kind="identity_enrichment"
    )
    claimed = store.claim_next("worker:other-kind")
    result = await _result()

    with pytest.raises(ValueError, match="profile-discovery job"):
        store.record_profile_search_result(
            job_id, result, worker_id=claimed["worker_id"]
        )


@pytest.mark.asyncio
async def test_cancelled_worker_can_retain_only_a_stopped_snapshot(store):
    job_id, worker_id = _claimed_job(store)
    checks = iter((False, True))
    stopped = await ProfileSearchOrchestrator(_Client()).discover(
        _plan(),
        platforms=("instagram", "x"),
        cancellation_check=lambda: next(checks),
    )
    completed = await _result()
    assert store.request_cancel(job_id) is True

    assert (
        store.record_profile_search_result(
            job_id, completed, worker_id=worker_id
        )
        is None
    )
    audit_id = store.record_profile_search_result(
        job_id, stopped, worker_id=worker_id
    )

    assert audit_id
    audit = store.get_profile_search_audit(job_id, audit_id)
    assert audit["status"] == "stopped"
    assert audit["executed_query_count"] == 1
    assert audit["document"]["candidates"][0]["review_status"] == "pending"


@pytest.mark.asyncio
async def test_terminal_job_rejects_late_audit_and_delete_cascades(store):
    job_id, worker_id = _claimed_job(store)
    result = await _result()
    audit_id = store.record_profile_search_result(
        job_id, result, worker_id=worker_id
    )
    assert audit_id
    store.finish(
        job_id,
        {"status": "completed", "usernames": ["alice"]},
        worker_id=worker_id,
    )

    assert (
        store.record_profile_search_result(
            job_id, result, worker_id=worker_id
        )
        is None
    )
    assert store.delete_job(job_id) is True
    with store.engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(profile_search_audits)
            )
            == 0
        )


def test_store_requires_a_typed_bounded_discovery_result(store):
    job_id, worker_id = _claimed_job(store)

    with pytest.raises(ValueError, match="discovery result"):
        store.record_profile_search_result(
            job_id,
            {"status": "completed", "api_key": "must-not-persist"},
            worker_id=worker_id,
        )


@pytest.mark.asyncio
async def test_store_rejects_mutable_discovery_collections(store):
    job_id, worker_id = _claimed_job(store)
    result = await _result()
    malformed = replace(result, runs=list(result.runs))

    with pytest.raises(ValueError, match="must be immutable"):
        store.record_profile_search_result(
            job_id, malformed, worker_id=worker_id
        )
