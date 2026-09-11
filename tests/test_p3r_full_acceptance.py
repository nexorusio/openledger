"""Full P3R acceptance contract.

This is deliberately separate from the ordinary fast suite.  When enabled it
requires a disposable PostgreSQL database, a private loopback-only network
namespace, and the real persistent-worker execution boundary.  It never falls
back to SQLite, injected terminal events, or public source transport.

The fixture adapter factory is intentionally a top-level callable so the
spawned worker can import it.  It is handed to ``worker.execute_job`` directly;
no HTTP request, environment import string, or production configuration can
select it.
"""

from __future__ import annotations

import asyncio
from contextlib import ExitStack, contextmanager
from pathlib import Path
import hashlib
import os
import re
import shutil
from threading import Thread
import time
from typing import Iterator
from unittest.mock import patch
from urllib.parse import urlsplit

import pytest
from sqlalchemy import text

from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web import app as web_app
from maigret.web import worker as worker_module
from maigret.web.case_store import CaseStore

FULL_ACCEPTANCE_ENV = "OPENLEDGER_P3R_FULL_ACCEPTANCE"
OFFLINE_NAMESPACE_ENV = "OPENLEDGER_P3R_OFFLINE_NAMESPACE"
POSTGRES_URL = os.getenv("OPENLEDGER_TEST_POSTGRES_URL", "").strip()

pytestmark = pytest.mark.skipif(
    os.getenv(FULL_ACCEPTANCE_ENV) != "1",
    reason=(
        "P3R full acceptance is a separately provisioned PostgreSQL/Chromium "
        "gate; set OPENLEDGER_P3R_FULL_ACCEPTANCE=1 in its private namespace."
    ),
)


class FixtureAdapterViolation(AssertionError):
    """Raised if the fixture worker reaches an adapter that was not selected."""


def _stable_id(*parts: object) -> str:
    return hashlib.sha256("\x1f".join(map(str, parts)).encode("utf-8")).hexdigest()


def _fixture_task(username: str, index: int) -> dict:
    check_id = _stable_id("fixture-check", username, index)
    return {
        "schema_version": 1,
        "task_id": _stable_id("fixture-task", check_id),
        "check_id": check_id,
        "source_id": _stable_id("fixture-source", "maigret"),
        "target_id": _stable_id("fixture-target", username),
        "attempt": 0,
    }


def _fixture_result(username: str, index: int) -> MaigretCheckResult:
    # These are existing Maigret database names.  The fixture only supplies
    # deterministic terminal responses; it does not open a source transport.
    site = (
        "GitHub",
        "Facebook",
        "Flickr",
        "Twitter",
        "YouTube",
        "GitHubGist",
        "Wikipedia",
        "Pinterest",
        "Vimeo",
        "TikTok",
        "Tumblr",
        "Telegram",
    )[index]
    return MaigretCheckResult(
        username=username,
        site_name=site,
        site_url_user=f"https://fixture.invalid/{site.casefold()}/{username}/{index}",
        status=MaigretCheckStatus.CLAIMED,
        ids_data={
            "id": f"{username}-{index}",
            "fullname": "Fixture Shared Name",
            "email": "fixture-shared@example.test",
        },
        http_status=200,
    )


def _forbidden_adapter(*_args, **_kwargs):
    raise FixtureAdapterViolation("an unselected fixture worker adapter was called")


async def _fixture_maigret(username: str, _options: dict, *, query_notify=None):
    """Emit a complete, accounting-aware local Maigret result set."""
    tasks = [_fixture_task(username, index) for index in range(12)]
    if query_notify is not None:
        query_notify.task_plan(tasks)
        query_notify.set_total(len(tasks))
    results = {}
    for index, task in enumerate(tasks):
        result = _fixture_result(username, index)
        results[result.site_name] = result
        if query_notify is not None:
            query_notify.update(result)
            query_notify.task_terminal(
                {
                    **task,
                    "disposition": "completed",
                    "attempted": True,
                    "cleanup_state": "not_required",
                }
            )
    if query_notify is not None:
        query_notify.flush_accounting()
    return dict(query_notify.results) if query_notify is not None else results


async def _blocking_fixture_maigret(
    username: str, _options: dict, *, query_notify=None
):
    """Retain one committed finding before the normal stop watcher cancels us."""
    task = _fixture_task(username, 0)
    if query_notify is not None:
        query_notify.task_plan([task])
        query_notify.set_total(1)
        query_notify.update(_fixture_result(username, 0))
        query_notify.checkpoint()
    try:
        while True:
            await asyncio.sleep(0.05)
    finally:
        if query_notify is not None:
            query_notify.task_terminal(
                {
                    **task,
                    "disposition": "interrupted",
                    "attempted": True,
                    "cleanup_state": "complete",
                }
            )
            query_notify.flush_accounting()


async def _resistant_fixture_maigret(
    username: str, _options: dict, *, query_notify=None
):
    """Model a source that ignores task cancellation until its process is reaped."""
    task = _fixture_task(username, 0)
    if query_notify is not None:
        query_notify.task_plan([task])
        query_notify.set_total(1)
        query_notify.update(_fixture_result(username, 0))
        query_notify.checkpoint()
    while True:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            # The parent must end this spawned process group after its declared
            # grace; this deliberately never returns to the event loop owner.
            continue


async def _crashing_fixture_maigret(
    username: str, _options: dict, *, query_notify=None
):
    """Crash only the spawned child after its first durable checkpoint."""
    task = _fixture_task(username, 0)
    if query_notify is not None:
        query_notify.task_plan([task])
        query_notify.set_total(1)
        query_notify.update(_fixture_result(username, 0))
        query_notify.checkpoint()
    os._exit(86)


def _fixture_adapter_context(app_module, maigret_adapter):
    if not app_module.app.config.get("TESTING"):
        raise RuntimeError("P3R fixture adapters require Flask TESTING mode")
    return (
        patch.object(app_module, "maigret_search", maigret_adapter),
        patch.object(app_module, "run_native_profile_search_phase", _forbidden_adapter),
        patch.object(app_module, "run_github_public_profile", _forbidden_adapter),
        patch.object(app_module, "run_unfurl_url_analysis", _forbidden_adapter),
        patch.object(app_module, "run_wayback_capture_index", _forbidden_adapter),
        patch.object(app_module, "run_user_scanner_usernames", _forbidden_adapter),
        patch.object(app_module, "run_user_scanner_email", _forbidden_adapter),
    )


@contextmanager
def fixture_adapter_factory(app_module):
    """Patch only collection I/O inside a TESTING-only spawned worker.

    ``worker.execute_job`` must reject this factory outside ``app.config['TESTING']``.
    The namespace gate is tested before the worker is started so native and child
    transports cannot escape the fixture boundary.
    """
    with _patches(_fixture_adapter_context(app_module, _fixture_maigret)):
        yield


@contextmanager
def fixture_adapter_factory_blocking(app_module):
    """Fixture adapter used only to exercise the durable stop path."""
    with _patches(_fixture_adapter_context(app_module, _blocking_fixture_maigret)):
        yield


@contextmanager
def fixture_adapter_factory_resistant(app_module):
    """Fixture adapter that requires the parent to kill and reap the child."""
    with _patches(_fixture_adapter_context(app_module, _resistant_fixture_maigret)):
        yield


@contextmanager
def fixture_adapter_factory_crashing(app_module):
    """Fixture adapter that exits only the child process after a checkpoint."""
    with _patches(_fixture_adapter_context(app_module, _crashing_fixture_maigret)):
        yield


@contextmanager
def _patches(patches):
    with ExitStack() as stack:
        for adapter_patch in patches:
            stack.enter_context(adapter_patch)
        yield


def _require_full_runtime() -> None:
    assert os.getenv(OFFLINE_NAMESPACE_ENV) == "1", (
        "P3R full acceptance must run under the private loopback-only namespace; "
        "the runner must set OPENLEDGER_P3R_OFFLINE_NAMESPACE=1 after verifying it."
    )
    assert (
        POSTGRES_URL
    ), "OPENLEDGER_TEST_POSTGRES_URL must name a disposable PostgreSQL service"


@pytest.fixture
def postgres_store() -> Iterator[CaseStore]:
    _require_full_runtime()
    store = CaseStore(POSTGRES_URL)
    assert (
        store.engine.dialect.name == "postgresql"
    ), "full acceptance never falls back to SQLite"
    assert store.ping() is True
    # Migrations are a prerequisite of this gate; create_all would hide a
    # migration omission and is intentionally not used here.
    with store.engine.begin() as connection:
        assert connection.scalar(
            text("SELECT to_regclass('public.investigation_jobs')")
        )
        connection.execute(text("TRUNCATE TABLE cases RESTART IDENTITY CASCADE"))
    try:
        yield store
    finally:
        with store.engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE cases RESTART IDENTITY CASCADE"))
        store.dispose()


@pytest.fixture
def full_app(
    tmp_path, monkeypatch, postgres_store
) -> Iterator[tuple[object, CaseStore, Path]]:
    reports = tmp_path / "reports"
    previous_store = web_app.case_store
    web_app.case_store = postgres_store
    web_app.job_results.clear()
    web_app.app.config.update(
        TESTING=True,
        AUTH_REQUIRED=False,
        REPORTS_FOLDER=str(reports),
        SETTINGS_FILE=str(tmp_path / "settings.json"),
    )
    monkeypatch.setenv("OPENLEDGER_UNIFIED_INVESTIGATION_INPUT_ENABLED", "1")
    try:
        yield web_app.app, postgres_store, reports
    finally:
        web_app.case_store = previous_store
        web_app.job_results.clear()
        shutil.rmtree(reports, ignore_errors=True)


def _csrf_token(client) -> str:
    response = client.get("/")
    assert response.status_code == 200
    with client.session_transaction() as session:
        return str(session["csrf_token"])


def _submit_live_scan(client, username: str) -> str:
    token = _csrf_token(client)
    response = client.post(
        "/live",
        data={
            "csrf_token": token,
            "investigation_token": username,
            "investigation_token_type": "username",
            "mode": "quick",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    match = re.fullmatch(
        r"/live/([0-9a-f-]{36})", urlsplit(response.headers["Location"]).path
    )
    assert match, response.headers["Location"]
    return match.group(1)


def _run_claimed_job(store: CaseStore, job_id: str) -> None:
    job = store.claim_next("worker:p3r-full-parent")
    assert job and job["job_id"] == job_id
    # The implementation under acceptance owns the spawned child, its
    # heartbeat/collector lock, cancellation grace, and reaping.  Calling this
    # public worker boundary is the distinction from the old injected journey.
    worker_module.execute_job(
        store,
        job,
        shutdown_check=lambda: False,
        fixture_adapter_factory=fixture_adapter_factory,
    )


def _event_payloads(store: CaseStore, job_id: str) -> list[dict]:
    return [row["event"] for row in store.get_events(job_id)]


def test_real_worker_submission_sse_runtime_and_reports(full_app):
    app, store, reports = full_app
    with app.test_client() as client:
        job_id = _submit_live_scan(client, "alicefixture")
        queued = store.get_job(job_id)
        assert queued and queued["status"] == "queued"
        _run_claimed_job(store, job_id)

        terminal = store.get_job(job_id)
        assert terminal and terminal["status"] == "completed"
        # ``CaseStore.get_job`` serializes the persisted result at the top
        # level; it deliberately has no nested ``result`` key.
        result = terminal
        assert result and result["status"] == "completed"
        assert result["collection_accounting"]["state"] == "completed"
        maigret = next(
            row
            for row in result["collection_accounting"]["stages"]
            if row["stage_id"] == "maigret"
        )
        assert (maigret["planned"], maigret["terminal"], maigret["completed"]) == (
            12,
            12,
            12,
        )

        runtime = client.get(f"/api/scan/{job_id}/runtime")
        assert runtime.status_code == 200
        assert runtime.get_json()["status"] == "completed"

        stream = client.get(f"/api/scan/{job_id}/stream?after=0", buffered=True)
        assert stream.status_code == 200
        payload = stream.get_data(as_text=True)
        assert '"type": "done"' in payload
        assert '"status": "completed"' in payload
        assert payload.count('"type": "done"') == 1
        assert "collection_task_terminal" not in payload

        results_page = client.get(f"/results/search_{job_id}")
        assert results_page.status_code == 200
        assert b"Declared collection accounting" in results_page.data
        declared = {
            Path(result["individual_reports"][0][key]).name
            for key in ("csv_file", "json_file", "pdf_file", "html_file")
        }
        # The report producer, rather than a test double, must publish each
        # declared artifact before the terminal result is visible.
        assert {
            "report_alicefixture.csv",
            "report_alicefixture.json",
            "report_alicefixture.html",
            "report_alicefixture.pdf",
        } <= declared
        for filename in declared:
            path = reports / f"search_{job_id}" / filename
            assert path.is_file() and path.stat().st_size > 0
            assert client.get(f"/reports/search_{job_id}/{filename}").status_code == 200


def _wait_for_event(store: CaseStore, job_id: str, event_type: str) -> list[dict]:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        events = _event_payloads(store, job_id)
        if any(event.get("type") == event_type for event in events):
            return events
        time.sleep(0.05)
    pytest.fail(f"real worker did not persist {event_type!r} before its deadline")


def _assert_one_terminal_reconciliation(store: CaseStore, job_id: str, *, cause: str) -> dict:
    terminal = store.get_job(job_id)
    assert terminal and terminal["status"] in {"completed", "cancelled", "interrupted"}
    result = terminal
    lifecycle = result.get("lifecycle") or (terminal.get("progress") or {}).get("lifecycle")
    assert lifecycle and lifecycle["phase"] == "terminal"
    assert lifecycle["stop_cause"] == cause
    events = _event_payloads(store, job_id)
    done = [event for event in events if event.get("type") == "done"]
    assert len(done) == 1
    assert done[0].get("reason") in {"cancelled", cause}
    assert all(event.get("type") != "running" for event in events[events.index(done[0]) + 1 :])
    return terminal


def _run_in_thread(store: CaseStore, job: dict, factory) -> Thread:
    runner = Thread(
        target=worker_module.execute_job,
        kwargs={
            "store": store,
            "job": job,
            "shutdown_check": lambda: False,
            "fixture_adapter_factory": factory,
        },
        daemon=True,
    )
    runner.start()
    return runner


def test_stop_retains_partial_findings_and_records_operator_source_cause(full_app):
    app, store, _reports = full_app
    with app.test_client() as client:
        job_id = _submit_live_scan(client, "stopfixture")
        job = store.claim_next("worker:p3r-full-stop-parent")
        assert job and job["job_id"] == job_id
        runner = _run_in_thread(store, job, fixture_adapter_factory_blocking)

        # ``running`` is emitted when the parent claims the lease.  The stop
        # boundary is useful only after a child has persisted its task plan.
        _wait_for_event(store, job_id, "collection_task_plan")
        assert web_app.app.config["TESTING"] is True
        assert web_app.case_store is store

        token = _csrf_token(client)
        started = time.monotonic()
        stopped = client.post(
            f"/api/scan/{job_id}/stop",
            headers={"X-OpenLedger-CSRF": token},
        )
        assert stopped.status_code == 200
        assert stopped.get_json()["cancel_requested"] is True
        runner.join(timeout=35)  # Must cover the worker's 20-second child grace.
        elapsed = time.monotonic() - started
        assert not runner.is_alive(), "worker did not honour the bounded cancellation grace"
        assert elapsed < 35

        terminal = _assert_one_terminal_reconciliation(store, job_id, cause="operator_cancel")
        assert any(
            event.get("type") == "stopped" and event.get("reason") == "source_stopped"
            for event in _event_payloads(store, job_id)
        )
        result = terminal
        assert result["collection_status"] == "cancelled"
        assert "fixture-shared@example.test" in repr(result)
        maigret = next(row for row in result["collection_accounting"]["stages"] if row["stage_id"] == "maigret")
        assert maigret["planned"] == 1
        assert maigret["observations"] >= 1


def test_parent_reaps_cancellation_resistant_fixture_within_stop_bound(full_app):
    app, store, _reports = full_app
    with app.test_client() as client:
        job_id = _submit_live_scan(client, "resistantfixture")
        job = store.claim_next("worker:p3r-full-resistant-parent")
        assert job and job["job_id"] == job_id
        runner = _run_in_thread(store, job, fixture_adapter_factory_resistant)
        _wait_for_event(store, job_id, "collection_task_plan")

        token = _csrf_token(client)
        started = time.monotonic()
        assert client.post(
            f"/api/scan/{job_id}/stop", headers={"X-OpenLedger-CSRF": token}
        ).status_code == 200
        runner.join(timeout=35)
        elapsed = time.monotonic() - started
        assert not runner.is_alive(), "parent left a cancellation-resistant child alive"
        assert elapsed < 35

        terminal = _assert_one_terminal_reconciliation(store, job_id, cause="operator_cancel")
        progress = terminal["progress"] or {}
        accounting = terminal.get("collection_accounting") or progress.get("collection_accounting")
        assert accounting and accounting["state"] == "interrupted"
        maigret = next(row for row in accounting["stages"] if row["stage_id"] == "maigret")
        assert maigret["cleanup_complete"] is False
        assert maigret["unknown"] >= 1


def test_parent_recovers_spawn_child_crash_from_durable_checkpoint(full_app):
    app, store, _reports = full_app
    with app.test_client() as client:
        job_id = _submit_live_scan(client, "crashfixture")
    job = store.claim_next("worker:p3r-full-crash-parent")
    assert job and job["job_id"] == job_id
    runner = _run_in_thread(store, job, fixture_adapter_factory_crashing)
    runner.join(timeout=15)
    assert not runner.is_alive(), "parent did not reap its crashed collector child"

    terminal = _assert_one_terminal_reconciliation(store, job_id, cause="cleanup_incomplete")
    result = terminal
    assert result["status"] == "interrupted"
    assert "fixture-shared@example.test" in repr(result)
    assert result["collection_accounting"]["known"] is False


def _persona_email_claim(store: CaseStore, job_id: str) -> tuple[str, dict]:
    case = store.get_case(store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    email = next(
        claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "email"
    )
    return persona_id, email


def test_real_worker_keeps_cross_case_exact_provenance_and_all_citations(full_app):
    app, store, _reports = full_app
    with app.test_client() as client:
        first_job = _submit_live_scan(client, "citationone")
        _run_claimed_job(store, first_job)
        second_job = _submit_live_scan(client, "citationtwo")
        _run_claimed_job(store, second_job)

        first_persona, first_email = _persona_email_claim(store, first_job)
        second_persona, second_email = _persona_email_claim(store, second_job)
        assert len(first_email["evidence"]) == len(second_email["evidence"]) == 12
        assert store.build_relationship_graph()["edges"] == []

        csrf = _csrf_token(client)
        for persona_id, claim in (
            (first_persona, first_email),
            (second_persona, second_email),
        ):
            reviewed = client.post(
                f"/claims/{claim['id']}/review",
                data={
                    "csrf_token": csrf,
                    "persona_id": persona_id,
                    "decision": "approved",
                    "note": "Fixture evidence reviewed.",
                },
                follow_redirects=False,
            )
            assert reviewed.status_code == 302

        graph = store.build_relationship_graph()
        assert graph["stats"]["connection_count"] == 2
        assert {edge["claim_id"] for edge in graph["edges"]} == {
            first_email["id"],
            second_email["id"],
        }
        provenance_by_claim = {
            first_email["id"]: first_persona,
            second_email["id"]: second_persona,
        }
        for edge in graph["edges"]:
            assert edge["source_count"] == 12
            assert len(edge["sources"]) == 10
            assert edge["provenance_url"] == (
                f"/personas/{provenance_by_claim[edge['claim_id']]}#claim-{edge['claim_id']}"
            )
