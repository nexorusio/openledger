"""P3R durable native-search checkpoint contract tests."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import update

from maigret.web.case_store import CaseStore, investigation_jobs, utcnow
from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_contract import (
    ProfileSearchError,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
)
from maigret.web.profile_search_orchestrator import ProfileSearchOrchestrator

PLATFORM_URLS = {
    "facebook": "https://www.facebook.com/alice_example",
    "instagram": "https://www.instagram.com/alice_example/",
    "threads": "https://www.threads.com/@alice_example",
    "tiktok": "https://www.tiktok.com/@alice_example",
    "x": "https://x.com/alice_example",
}


def _plan(seed_count=1):
    return {
        "identifiers": [],
        "search_targets": [
            {"value": f"alice_{index}", "source_type": "username"}
            for index in range(seed_count)
        ],
    }


def _success(query):
    return ProfileSearchRun(
        query=query,
        provenance=ProfileSearchProvenance.for_query(
            query,
            provider="brave",
            retrieved_at="2026-09-10T12:00:00Z",
        ),
        evidence=(
            ProfileSearchEvidence(
                result_rank=1,
                source_url=PLATFORM_URLS[query.platform],
                title="Alice Example",
            ),
        ),
    )


def _error(query):
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
            occurred_at="2026-09-10T12:00:00Z",
            http_status=429,
        ),
    )


@pytest.fixture
def store(tmp_path):
    instance = CaseStore(
        f"sqlite:///{tmp_path / 'native-checkpoints.db'}",
        create_schema=True,
    )
    yield instance
    instance.dispose()


def _claimed_job(store, *, worker_id="worker:checkpoint"):
    job_id = store.create_investigation(["alice"], {})
    claimed = store.claim_next(worker_id)
    assert claimed["job_id"] == job_id
    return job_id, worker_id


@pytest.mark.asyncio
async def test_running_checkpoints_upsert_beyond_audit_cap_and_redact_job(store):
    class Client:
        async def search(self, query):
            return _success(query)

    checkpoints = []
    terminal = []
    result = await ProfileSearchOrchestrator(Client()).discover(
        _plan(seed_count=5),
        progress_sink=checkpoints.append,
        result_sink=terminal.append,
    )
    assert result.planned_query_count == 25
    assert len(checkpoints) == 50
    assert terminal == [result]

    job_id, worker_id = _claimed_job(store)
    for checkpoint in checkpoints:
        assert checkpoint.status == "running"
        assert checkpoint.final is False
        assert store.save_native_profile_checkpoint(
            job_id, checkpoint, worker_id=worker_id
        )

    assert store.list_profile_search_audits(job_id, limit=100) == []
    assert (
        store.save_native_profile_checkpoint(
            job_id, checkpoints[-1], worker_id="worker:wrong-owner"
        )
        is False
    )
    with pytest.raises(ValueError, match="Invalid profile-search audit status"):
        store.record_profile_search_result(job_id, checkpoints[-1], worker_id=worker_id)
    assert "native_profile_checkpoint" not in store.get_job(job_id)["progress"]

    audit_id = store.record_profile_search_result(job_id, result, worker_id=worker_id)
    assert audit_id
    audits = store.list_profile_search_audits(job_id, limit=100)
    assert len(audits) == 1
    assert audits[0]["document"]["candidates"][0]["review_status"] == "pending"


@pytest.mark.asyncio
async def test_expired_inflight_checkpoint_promotes_once_without_replay(store):
    third_started = asyncio.Event()

    class Client:
        def __init__(self):
            self.calls = 0

        async def search(self, query):
            self.calls += 1
            if self.calls == 1:
                return _success(query)
            if self.calls == 2:
                return _error(query)
            third_started.set()
            await asyncio.Event().wait()

    client = Client()
    checkpoints = []
    discovery = asyncio.create_task(
        ProfileSearchOrchestrator(client).discover(
            _plan(),
            platforms=("facebook", "instagram", "x"),
            progress_sink=checkpoints.append,
        )
    )
    await third_started.wait()
    checkpoint = checkpoints[-1]
    assert checkpoint.executed_query_count == 2
    assert checkpoint.error_count == 1
    assert checkpoint.active_query_count == 1
    assert checkpoint.interrupted_query_count == 0
    assert checkpoint.unattempted_query_count == 0

    job_id, worker_id = _claimed_job(store)
    assert store.save_native_profile_checkpoint(job_id, checkpoint, worker_id=worker_id)
    store.append_event(
        job_id,
        {
            'type': 'collection_accounting',
            'collection_accounting': {
                'schema_version': 1,
                'revision': 1,
                'state': 'running',
                'known': True,
                'stages': [
                    {
                        'stage_id': 'native',
                        'engine_id': 'native-profile-search',
                        'unit': 'queries',
                        'status': 'running',
                        'reason': None,
                        'planned': 3,
                        'started': 3,
                        'terminal': 2,
                        'completed': 1,
                        'errors': 1,
                        'timeouts': 0,
                        'cancelled': 0,
                        'interrupted': 0,
                        'unattempted': 0,
                        'unknown': 1,
                        'observations': len(checkpoint.candidates),
                    }
                ],
            },
        },
        runtime_guard=True,
        worker_id=worker_id,
    )
    assert (
        store.save_native_profile_checkpoint(
            job_id, checkpoint, worker_id="worker:wrong-owner"
        )
        is False
    )

    discovery.cancel()
    await discovery
    assert client.calls == 3
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(heartbeat_at=utcnow() - timedelta(seconds=31))
        )

    assert store.mark_stale_running(30) == 1
    interrupted = store.get_job(job_id)
    assert interrupted["status"] == "interrupted"
    assert (
        interrupted['collection_accounting']['stages'][0]['cleanup_complete'] is False
    )
    assert "native_profile_checkpoint" not in interrupted["progress"]
    audits = store.list_profile_search_audits(job_id, limit=100)
    assert len(audits) == 1
    audit = audits[0]
    assert audit["status"] == "stopped"
    assert audit["document"]["active_query_count"] == 0
    assert audit["document"]["interrupted_query_count"] == 1
    assert audit["document"]["unattempted_query_count"] == 0
    assert audit["document"]["runs"][0]["provenance"] is not None
    assert audit["document"]["runs"][1]["error"]["code"] == "rate_limited"
    assert audit["document"]["candidates"][0]["review_status"] == "pending"
    assert client.calls == 3

    assert store.mark_stale_running(30) == 0
    assert len(store.list_profile_search_audits(job_id, limit=100)) == 1
