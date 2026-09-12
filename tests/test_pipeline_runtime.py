"""Real durable budgets and transport boundaries, with offline HTTP fixtures."""

import json
import io
import os
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import requests
import urllib3
import pytest
from sqlalchemy import update

from maigret.web.case_store import CaseStore, investigation_jobs, utcnow
from maigret.web.pipeline_store import PipelineStore
from maigret.web.pipeline_runtime import (
    PipelineRuntimeStore,
    ProviderCooldown,
    RequestBudgetExceeded,
)
from maigret.web.pipeline_http import TransportGuard


@pytest.fixture
def runtime(tmp_path):
    store = CaseStore(
        os.getenv("OPENLEDGER_TEST_POSTGRES_URL") or f"sqlite:///{tmp_path}/runtime.db",
        create_schema=True,
    )
    pipeline = PipelineStore(store)
    job_id = store.create_investigation(["fixture-" + uuid.uuid4().hex[:8]], {})
    worker = "worker:" + uuid.uuid4().hex
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(status="running", worker_id=worker, heartbeat_at=utcnow())
        )
    case_id = store.get_job(job_id)["case_id"]
    persona_id = store.get_case(case_id)["personas"][0]["id"]
    request = pipeline.create_request(
        case_id,
        persona_id,
        [{"type": "username", "value": "fixture"}],
        {
            "pipeline_id": "p2-e2e-v1",
            "budgets": {"max_requests": 2},
            "tasks": [
                {
                    "task_id": "runtime-fixture",
                    "engine_id": "fixture",
                    "route_state": "active",
                    "retry_ceiling": 2,
                }
            ],
        },
        actor="operator",
        job_id=job_id,
    )
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], worker)
    yield store, pipeline, PipelineRuntimeStore(store), request, attempt, worker
    store.dispose()


def test_budget_survives_retry_and_rejects_stale_attempt(runtime):
    store, pipeline, meter, request, attempt, worker = runtime
    meter.reserve_request(request["id"], attempt["id"], worker)
    pipeline.finish_attempt(attempt["id"], "timeout", worker_id=worker)
    successor = pipeline.start_attempt(request["tasks"][0]["id"], worker)
    reopened = PipelineRuntimeStore(store)
    assert reopened.budget_snapshot(request["id"])["consumed"] == 1
    with pytest.raises(ValueError, match="Stale|completed"):
        reopened.reserve_request(request["id"], attempt["id"], worker)
    reopened.reserve_request(request["id"], successor["id"], worker)
    with pytest.raises(RequestBudgetExceeded):
        reopened.reserve_request(request["id"], successor["id"], worker)
    assert reopened.budget_snapshot(request["id"])["consumed"] == 2


def test_request_budget_is_atomic_across_workers(runtime):
    store, _, _, request, attempt, worker = runtime

    def reserve(_):
        try:
            PipelineRuntimeStore(store).reserve_request(
                request["id"], attempt["id"], worker
            )
            return True
        except RequestBudgetExceeded:
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(12)))
    assert sum(results) == 2


def test_shared_provider_cooldown_and_single_probe(runtime):
    store, _, meter, request, attempt, worker = runtime
    provider = "fixture-" + uuid.uuid4().hex + ".test"
    for _ in range(3):
        PipelineRuntimeStore(store).record_provider_result(provider, "timeout")
    with pytest.raises(ProviderCooldown):
        meter.reserve_request(request["id"], attempt["id"], worker, provider=provider)
    assert meter.budget_snapshot(request["id"])["consumed"] == 0
    with store.engine.begin() as connection:
        connection.execute(
            update(meter.providers)
            .where(meter.providers.c.provider == provider)
            .values(cooldown_until=utcnow() - timedelta(seconds=1))
        )
    permit = meter.reserve_request(
        request["id"], attempt["id"], worker, provider=provider
    )["provider_permit"]
    assert permit
    with pytest.raises(ProviderCooldown):
        PipelineRuntimeStore(store).reserve_request(
            request["id"], attempt["id"], worker, provider=provider
        )
    meter.record_provider_result(provider, "success", permit=permit)
    assert meter.provider_status(provider)["consecutive_failures"] == 0
    meter.reserve_request(request["id"], attempt["id"], worker, provider=provider)


def test_transport_redirects_consume_separate_permits(runtime, monkeypatch):
    store, _, meter, request, attempt, worker = runtime
    calls = []

    def provider(pool, connection, method, url, **kwargs):
        calls.append(url)
        return urllib3.response.HTTPResponse(
            body=io.BytesIO(b""),
            status=302,
            headers={"Location": "/next"},
            preload_content=False,
        )

    monkeypatch.setattr(
        urllib3.connectionpool.HTTPConnectionPool, "_make_request", provider
    )
    with TransportGuard(store, request["id"], attempt["id"], worker).install() as guard:
        with requests.Session() as client:
            client.trust_env = False
            with pytest.raises(RequestBudgetExceeded):
                client.get("https://fixture-redirect.test/start")
        result = guard.finish({"outcome": "found"})
    assert len(calls) == 2
    assert meter.budget_snapshot(request["id"])["consumed"] == 2
    assert result["completeness"] == "partial"
    assert result["retryable"] is False


def test_retry_after_is_shared_before_second_transport_call(runtime, monkeypatch):
    store, _, meter, request, attempt, worker = runtime
    host = "fixture-" + uuid.uuid4().hex + ".test"
    calls = []

    def provider(pool, connection, method, url, **kwargs):
        calls.append(url)
        return urllib3.response.HTTPResponse(
            body=io.BytesIO(b""),
            status=429,
            headers={"Retry-After": "120"},
            preload_content=False,
        )

    monkeypatch.setattr(
        urllib3.connectionpool.HTTPConnectionPool, "_make_request", provider
    )
    with TransportGuard(store, request["id"], attempt["id"], worker).install():
        with requests.Session() as client:
            client.trust_env = False
            assert client.get("https://" + host).status_code == 429
            with pytest.raises(ProviderCooldown):
                client.get("https://" + host)
    assert len(calls) == 1
    assert meter.provider_status(host)["retry_after_seconds"] > 100


def test_scanner_subprocess_inherits_meter(runtime):
    store, _, meter, request, attempt, worker = runtime
    payload = dict(
        mode="email",
        email="fixture@example.test",
        _pipeline_runtime=TransportGuard(
            store, request["id"], attempt["id"], worker
        ).payload,
    )
    script = """
import io
import requests
import urllib3
from maigret.web import user_scanner_runner as runner
from maigret.web.pipeline_runtime import RequestBudgetExceeded
calls = []
def respond(pool, connection, method, url, **kwargs):
    calls.append(url)
    return urllib3.response.HTTPResponse(body=io.BytesIO(b''), status=200, preload_content=False)
urllib3.connectionpool.HTTPConnectionPool._make_request = respond
def fake_scan(request):
    with requests.Session() as client:
        client.trust_env = False
        try:
            for _ in range(3): client.get('https://scanner-fixture.test')
        except RequestBudgetExceeded: pass
    return [{'fixture_calls': len(calls)}]
runner._scan_email = fake_scan
raise SystemExit(runner.main())
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    )
    result = json.loads(completed.stdout)
    assert result["results"][0]["fixture_calls"] == 2
    assert result["execution_control"]["error_code"] == "request_budget_exhausted"
    assert meter.budget_snapshot(request["id"])["consumed"] == 2


def test_curl_redirects_use_physical_attempt_boundary(runtime, monkeypatch):
    from curl_cffi.requests import Session
    from types import SimpleNamespace

    store, _, meter, request, attempt, worker = runtime
    calls = []

    def send(client, method, url, **options):
        calls.append(url)
        assert options["allow_redirects"] is False
        if len(calls) > 1:
            assert method == "GET"
            assert options.get("params") is None
            assert options.get("content") is None
            assert options.get("auth") is None
        return SimpleNamespace(
            status_code=302 if len(calls) == 1 else 200,
            headers=(
                {"Location": "https://second-fixture.test/final"}
                if len(calls) == 1
                else {}
            ),
        )

    boundary = "_request_once" if hasattr(Session, "_request_once") else "request"
    monkeypatch.setattr(Session, boundary, send)
    with TransportGuard(store, request["id"], attempt["id"], worker).install():
        with Session() as client:
            assert (
                client.post(
                    "https://first-fixture.test/start",
                    params={"api_key": "synthetic-only"},
                    content=b"fixture-body",
                    auth=("fixture", "synthetic-only"),
                ).status_code
                == 200
            )
            with pytest.raises(RequestBudgetExceeded):
                client.get("https://first-fixture.test/third")
    assert len(calls) == 2
    assert meter.budget_snapshot(request["id"])["consumed"] == 2


def test_curl_refuses_https_downgrade_before_forwarding_credentials(
    runtime, monkeypatch
):
    from curl_cffi.requests import Session
    from types import SimpleNamespace

    store, _, _, request, attempt, worker = runtime
    calls = []

    def send(client, method, url, **options):
        calls.append(url)
        return SimpleNamespace(
            status_code=302, headers={"Location": "http://secure-fixture.test/final"}
        )

    boundary = "_request_once" if hasattr(Session, "_request_once") else "request"
    monkeypatch.setattr(Session, boundary, send)
    with TransportGuard(store, request["id"], attempt["id"], worker).install():
        with Session() as client:
            with pytest.raises(ValueError, match="downgrade"):
                client.get(
                    "https://secure-fixture.test/start",
                    auth=("fixture", "synthetic-only"),
                )
    assert calls == ["https://secure-fixture.test/start"]


def test_native_retry_honors_shared_cooldown_before_next_send(runtime, monkeypatch):
    from urllib3.util import Retry

    store, _, _, request, attempt, worker = runtime
    calls = []

    def send(pool, connection, method, url, **options):
        calls.append(url)
        return urllib3.response.HTTPResponse(
            body=io.BytesIO(b""),
            status=429,
            headers={"Retry-After": "120"},
            preload_content=False,
        )

    monkeypatch.setattr(
        urllib3.connectionpool.HTTPConnectionPool, "_make_request", send
    )
    # The real urllib3 retry branch should consult our durable cooldown, not sleep
    # in the test. Only the provider/library delay is replaced; send is still real.
    monkeypatch.setattr(Retry, "sleep", lambda *args, **kwargs: None)
    with TransportGuard(store, request["id"], attempt["id"], worker).install():
        pool = urllib3.HTTPSConnectionPool("rate-" + uuid.uuid4().hex + ".test")
        with pytest.raises(ProviderCooldown):
            pool.urlopen(
                "GET", "/fixture", retries=Retry(total=5, status_forcelist=[429])
            )
    assert len(calls) == 1


def test_aiohttp_redirects_consume_permits_at_each_send(runtime, httpserver):
    import asyncio
    import aiohttp

    store, _, meter, request, attempt, worker = runtime
    httpserver.expect_request("/start").respond_with_data(
        "", status=302, headers={"Location": "/next"}
    )
    httpserver.expect_request("/next").respond_with_data(
        "", status=302, headers={"Location": "/third"}
    )
    httpserver.expect_request("/third").respond_with_data("should not be requested")

    async def collect():
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RequestBudgetExceeded):
                await session.get(httpserver.url_for("/start"))

    with TransportGuard(store, request["id"], attempt["id"], worker).install():
        asyncio.run(collect())
    assert len(httpserver.log) == 2
    assert meter.budget_snapshot(request["id"])["consumed"] == 2
