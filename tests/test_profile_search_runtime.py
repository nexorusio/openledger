# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import asyncio
import json
import queue
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from maigret.web import app as web_app
from maigret.web.case_store import CaseStore, persona_claims, utcnow
from maigret.web.investigation_input import build_unified_investigation_plan
from maigret.web.profile_discovery_policy import (
    govern_profile_discovery_options,
)
from maigret.web.profile_search_backend import (
    ProfileSearchConfig,
    ProfileSearchRun,
)
from maigret.web.profile_search_contract import (
    ProfileSearchError,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
)
from maigret.web.profile_search_planner import plan_profile_search_queries
from maigret.web.profile_search_runtime import GovernedProfileSearchClient
from maigret.web.provider_circuit_breaker import ProviderCircuitBreaker


def _plan():
    return {
        "identifiers": [],
        "search_targets": [{"value": "alice_example", "source_type": "username"}],
    }


def _config():
    return ProfileSearchConfig(
        provider="brave",
        api_key_file="/not-read-by-test",
        timeout_seconds=10,
        max_results=3,
    )


def _success(query):
    return ProfileSearchRun(
        query=query,
        provenance=ProfileSearchProvenance.for_query(
            query,
            provider="brave",
            retrieved_at="2026-09-08T12:00:00Z",
        ),
        evidence=(
            ProfileSearchEvidence(
                result_rank=1,
                source_url="https://www.instagram.com/alice_example/",
                title="Alice Example",
            ),
        ),
    )


def _retryable_failure(query):
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
            occurred_at=datetime.now(timezone.utc),
            http_status=429,
        ),
    )


class _RawClient:
    def __init__(self, response):
        self.config = _config()
        self.response = response
        self.calls = 0

    async def search(self, query):
        self.calls += 1
        return self.response(query)


@pytest.mark.asyncio
async def test_governed_client_opens_shared_circuit_without_retrying():
    raw = _RawClient(_retryable_failure)
    circuits = ProviderCircuitBreaker(failure_threshold=1, cooldown_seconds=60)
    client = GovernedProfileSearchClient(
        raw, circuit_breaker_enabled=True, circuits=circuits
    )
    query = plan_profile_search_queries(
        _plan(), platforms=("instagram",), max_queries=1
    )[0]

    first = await client.search(query)
    second = await client.search(query)

    assert first.error.code == "rate_limited"
    assert second.error.code == "circuit_open"
    assert raw.calls == 1
    assert client.last_circuit_open is not None
    assert circuits.snapshot("profile-search:brave")["status"] == "open"


@pytest.mark.asyncio
async def test_search_phase_precedes_maigret_and_emits_only_bounded_counts(
    monkeypatch,
):
    call_order = []
    stored_results = []

    class _SuccessfulClient(_RawClient):
        def __init__(self, _config_value):
            super().__init__(_success)

        async def search(self, query):
            call_order.append("native-search")
            return await super().search(query)

    async def maigret_search(*_args, **_kwargs):
        call_order.append("maigret")
        return {}

    monkeypatch.setattr(web_app, "load_profile_search_config", _config)
    monkeypatch.setattr(web_app, "ProfileSearchClient", _SuccessfulClient)
    monkeypatch.setattr(web_app, "maigret_search", maigret_search)
    events = queue.Queue()
    runtime_job = {
        "queue": events,
        "cancelled": False,
        "profile_search_result_sink": lambda result: (
            stored_results.append(result) or "audit-1"
        ),
    }
    options = govern_profile_discovery_options(
        {"investigation_spec": _plan()},
        environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "true"},
    )

    await web_app._stream_search(runtime_job, ["alice"], options)

    assert call_order[0] == "native-search"
    assert call_order[-1] == "maigret"
    assert runtime_job["profile_search_audit_id"] == "audit-1"
    assert stored_results == [runtime_job["profile_search_result"]]
    public_events = []
    while not events.empty():
        public_events.append(events.get_nowait())
    native_events = [
        event
        for event in public_events
        if event.get("collector") == "native-profile-search"
    ]
    assert [event["type"] for event in native_events] == [
        "collector_started",
        "collector_completed",
    ]
    assert native_events[-1]["candidates"] == 1
    serialized_events = json.dumps(public_events)
    assert "alice_example" not in serialized_events
    assert "instagram.com/alice_example" not in serialized_events


@pytest.mark.asyncio
async def test_search_phase_prioritizes_approved_persona_evidence(
    monkeypatch,
):
    observed_queries = []

    class _CapturingClient(_RawClient):
        def __init__(self, _config_value):
            super().__init__(_success)

        async def search(self, query):
            observed_queries.append(query)
            return await super().search(query)

    monkeypatch.setattr(web_app, "load_profile_search_config", _config)
    monkeypatch.setattr(web_app, "ProfileSearchClient", _CapturingClient)
    runtime_job = {
        "queue": queue.Queue(),
        "cancelled": False,
        "profile_search_existing_evidence": (
            {
                "field_name": "social_account",
                "review_status": "approved",
                "value": {"username": "confirmed_persona_handle"},
            },
        ),
    }
    options = govern_profile_discovery_options(
        {"investigation_spec": _plan()},
        environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "true"},
    )

    await web_app.run_native_profile_search_phase(runtime_job, options)

    assert observed_queries[0].seed_kind == "confirmed_username"
    assert observed_queries[0].seed_value == "confirmed_persona_handle"


def test_persona_refresh_loads_only_approved_social_claims():
    claims = [
        {
            "field_name": "social_account",
            "review_status": "approved",
            "value": {"username": "confirmed_handle"},
        },
        {
            "field_name": "social_account",
            "review_status": "pending",
            "value": {"username": "pending_handle"},
        },
        {
            "field_name": "location",
            "review_status": "approved",
            "value": "Jakarta",
        },
    ]

    class _Store:
        def list_approved_persona_social_accounts(self, persona_id, *, limit):
            assert persona_id == "persona-1"
            assert limit == 8
            return claims

    result = web_app._profile_search_existing_evidence(
        _Store(),
        {"investigation_spec": {"target_persona_id": "persona-1"}},
    )

    assert result == (claims[0],)


def test_governed_profile_pivot_uses_only_its_verified_source_claim():
    claims = [
        {
            "id": "claim-source",
            "field_name": "social_account",
            "review_status": "approved",
            "value": {"username": "source_handle"},
        },
        {
            "id": "claim-other",
            "field_name": "social_account",
            "review_status": "approved",
            "value": {"username": "other_handle"},
        },
    ]

    class _Store:
        def list_approved_persona_social_accounts(self, persona_id, *, limit):
            assert persona_id == "persona-1"
            assert limit == 8
            return claims

    result = web_app._profile_search_existing_evidence(
        _Store(),
        {
            "investigation_spec": {"target_persona_id": "persona-1"},
            "governed_pivot_plan": {
                "pivot_kind": "verified_profile_discovery",
                "source_claim": {"id": "claim-source"},
            },
        },
    )

    assert result == (claims[0],)


def test_persona_social_seed_query_filters_before_applying_bound(tmp_path):
    store = CaseStore(
        f"sqlite:///{tmp_path / 'profile-search-seeds.db'}",
        create_schema=True,
    )
    try:
        job_id = store.create_investigation(["alice"], {})
        persona_id = store.get_case(store.get_job(job_id)["case_id"])["personas"][0][
            "id"
        ]
        now = utcnow()
        rows = []
        for index in range(500):
            rows.append(
                {
                    "id": f"non-social-{index}",
                    "persona_id": persona_id,
                    "field_name": "platform_identifier",
                    "value": {"identifier": f"identifier-{index}"},
                    "display_value": f"identifier-{index}",
                    "normalized_value": f"identifier-{index}",
                    "confidence": 100,
                    "review_status": "approved",
                    "source_engine": "test",
                    "source_job_id": None,
                    "fingerprint": f"{index:064x}",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "created_at": now,
                    "updated_at": now,
                    "reviewed_at": now,
                    "reviewed_by": "analyst",
                }
            )
        for index in range(10):
            rows.append(
                {
                    "id": f"social-{index}",
                    "persona_id": persona_id,
                    "field_name": "social_account",
                    "value": {"username": f"approved-{index}"},
                    "display_value": f"approved-{index}",
                    "normalized_value": f"approved-{index}",
                    "confidence": 90 - index,
                    "review_status": "approved",
                    "source_engine": "test",
                    "source_job_id": None,
                    "fingerprint": f"{500 + index:064x}",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "created_at": now,
                    "updated_at": now,
                    "reviewed_at": now,
                    "reviewed_by": "analyst",
                }
            )
        with store.engine.begin() as connection:
            connection.execute(persona_claims.insert(), rows)

        result = web_app._profile_search_existing_evidence(
            store,
            {"investigation_spec": {"target_persona_id": persona_id}},
        )

        assert len(result) == 8
        assert [claim["value"]["username"] for claim in result] == [
            f"approved-{index}" for index in range(8)
        ]
    finally:
        store.dispose()


@pytest.mark.asyncio
async def test_disabled_or_misconfigured_search_degrades_to_existing_collector(
    monkeypatch,
):
    maigret_calls = []

    async def maigret_search(*_args, **_kwargs):
        maigret_calls.append(True)
        return {}

    monkeypatch.setattr(web_app, "maigret_search", maigret_search)
    disabled_events = queue.Queue()
    await web_app._stream_search(
        {"queue": disabled_events, "cancelled": False},
        ["alice"],
        govern_profile_discovery_options({"investigation_spec": _plan()}, environ={}),
    )
    # Declared accounting is emitted even when native search is not selected.
    assert all(
        event['type'] == 'collection_accounting'
        for event in list(disabled_events.queue)
    )

    enabled_events = queue.Queue()
    await web_app._stream_search(
        {"queue": enabled_events, "cancelled": False},
        ["alice"],
        govern_profile_discovery_options(
            {"investigation_spec": _plan()},
            environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "true"},
        ),
    )
    assert maigret_calls == [True, True]
    assert [
        event['type']
        for event in list(enabled_events.queue)
        if event['type'] != 'collection_accounting'
    ] == [
        "collector_started",
        "collector_error",
    ]


@pytest.mark.asyncio
async def test_quick_native_fallback_does_not_call_server_disabled_maigret(
    monkeypatch,
):
    maigret_calls = []

    async def native_search(*_args, **_kwargs):
        return None

    async def maigret_search(*_args, **_kwargs):
        maigret_calls.append(True)
        return {}

    monkeypatch.setattr(web_app, "run_native_profile_search_phase", native_search)
    monkeypatch.setattr(web_app, "maigret_search", maigret_search)
    options = govern_profile_discovery_options(
        {
            "investigation_spec": build_unified_investigation_plan(
                {
                    "investigation_token": ["Alice Example"],
                    "mode": "quick",
                    "search_likely_username_aliases": "on",
                }
            )
        },
        environ={
            "OPENLEDGER_MAIGRET_DISCOVERY_ENABLED": "false",
            "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "true",
        },
    )
    usernames = [
        target["value"] for target in options["investigation_spec"]["search_targets"]
    ]
    events = queue.Queue()

    await web_app._stream_search(
        {"queue": events, "cancelled": False},
        usernames,
        options,
    )

    assert usernames
    assert maigret_calls == []
    assert not any(event.get("type") == "error" for event in list(events.queue))


@pytest.mark.asyncio
async def test_inflight_cancel_saves_stopped_audit_and_skips_maigret(
    monkeypatch,
):
    search_started = asyncio.Event()
    maigret_calls = []
    stopped_results = []

    class _BlockingClient(_RawClient):
        def __init__(self, _config_value):
            super().__init__(_success)

        async def search(self, query):
            search_started.set()
            await asyncio.Event().wait()
            return _success(query)

    async def maigret_search(*_args, **_kwargs):
        maigret_calls.append(True)
        return {}

    monkeypatch.setattr(web_app, "load_profile_search_config", _config)
    monkeypatch.setattr(web_app, "ProfileSearchClient", _BlockingClient)
    monkeypatch.setattr(web_app, "maigret_search", maigret_search)
    events = queue.Queue()
    runtime_job = {
        "queue": events,
        "cancelled": False,
        "profile_search_result_sink": lambda result: (
            stopped_results.append(result) or "stopped-audit"
        ),
    }
    options = govern_profile_discovery_options(
        {"investigation_spec": _plan()},
        environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "true"},
    )

    stream_task = asyncio.create_task(
        web_app._stream_search(runtime_job, ["alice"], options)
    )
    await search_started.wait()
    runtime_job["cancelled"] = True
    runtime_job["task"].cancel()
    result = await stream_task

    assert result == []
    assert maigret_calls == []
    assert len(stopped_results) == 1
    assert stopped_results[0].status == "stopped"
    assert runtime_job["profile_search_audit_id"] == "stopped-audit"
    assert any(
        event.get("type") == "stopped"
        and event.get("collector") == "native-profile-search"
        for event in list(events.queue)
    )


def test_worker_refreshes_search_flag_and_persists_native_audit(tmp_path, monkeypatch):
    store = CaseStore(
        f"sqlite:///{tmp_path / 'profile-search-runtime.db'}",
        create_schema=True,
    )
    monkeypatch.setenv("OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED", "true")
    monkeypatch.setattr(web_app, "case_store", store)
    options = govern_profile_discovery_options({"investigation_spec": _plan()})
    job_id = store.create_investigation(["alice"], options, kind="live")
    claimed = store.claim_next("worker:profile-search")

    class _SuccessfulClient(_RawClient):
        def __init__(self, _config_value):
            super().__init__(_success)

    async def maigret_search(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(web_app, "load_profile_search_config", _config)
    monkeypatch.setattr(web_app, "ProfileSearchClient", _SuccessfulClient)
    monkeypatch.setattr(web_app, "maigret_search", maigret_search)

    def fake_reports(_results, usernames, session_key, **kwargs):
        if kwargs.get('write_files', True) and kwargs.get('reports_root'):
            Path(kwargs['reports_root'], f'search_{session_key}').mkdir()
        return {
            "status": "completed",
            "session_folder": f"search_{session_key}",
            "usernames": usernames,
            "individual_reports": [],
            "graph_file": f"search_{session_key}/graph.html",
            "found_count": 0,
            "profile_reliability_version": 1,
        }

    monkeypatch.setattr(web_app, 'build_reports', fake_reports)
    monkeypatch.setattr(web_app, "persist_job_result", lambda *_args: None)

    try:
        assert (
            claimed["options"]["profile_discovery_policy"]["flags"][
                "search_first_enabled"
            ]
            is True
        )
        web_app.run_persistent_job(store, claimed)
        audits = store.list_profile_search_audits(job_id)
        assert len(audits) == 1
        assert audits[0]["status"] == "completed"
        assert audits[0]["candidate_count"] == 1
        assert store.get_job(job_id)["status"] == "completed"
    finally:
        store.dispose()


def test_worker_claim_replaces_stale_app_search_flag(tmp_path, monkeypatch):
    store = CaseStore(
        f"sqlite:///{tmp_path / 'profile-search-parity.db'}",
        create_schema=True,
    )
    monkeypatch.setenv("OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED", "true")
    options = govern_profile_discovery_options({"investigation_spec": _plan()})
    job_id = store.create_investigation(["alice"], options, kind="live")
    assert (
        store.get_job(job_id)["options"]["profile_discovery_policy"]["flags"][
            "search_first_enabled"
        ]
        is True
    )
    monkeypatch.delenv("OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED", raising=False)

    try:
        claimed = store.claim_next("worker:profile-search-parity")
        assert claimed["job_id"] == job_id
        assert (
            claimed["options"]["profile_discovery_policy"]["flags"][
                "search_first_enabled"
            ]
            is False
        )
    finally:
        store.dispose()


def test_worker_deadline_stops_native_search_and_retains_audit(tmp_path, monkeypatch):
    store = CaseStore(
        f"sqlite:///{tmp_path / 'profile-search-deadline.db'}",
        create_schema=True,
    )
    monkeypatch.setenv("OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED", "true")
    monkeypatch.setattr(web_app, "case_store", store)
    job_id = store.create_investigation(
        ["alice"],
        govern_profile_discovery_options({"investigation_spec": _plan()}),
        kind="live",
    )
    claimed = store.claim_next("worker:profile-search-deadline")
    claimed["deadline_at"] = (
        datetime.now(timezone.utc) + timedelta(milliseconds=30)
    ).isoformat()

    class _BlockingClient(_RawClient):
        def __init__(self, _config_value):
            super().__init__(_success)

        async def search(self, query):
            await asyncio.Event().wait()
            return _success(query)

    monkeypatch.setattr(web_app, "load_profile_search_config", _config)
    monkeypatch.setattr(web_app, "ProfileSearchClient", _BlockingClient)
    monkeypatch.setattr(web_app, "persist_job_result", lambda *_args: None)

    try:
        web_app.run_persistent_job(store, claimed)
        audits = store.list_profile_search_audits(job_id)
        assert len(audits) == 1
        assert audits[0]["status"] == "stopped"
        assert audits[0]["executed_query_count"] == 0
        completed = store.get_job(job_id)
        assert completed["status"] == "budget_exhausted"
        assert completed["collection_status"] == "budget_exhausted"
    finally:
        store.dispose()
