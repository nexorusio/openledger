# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Failure-boundary regression tests for durable P3R collection."""

import asyncio
from datetime import timedelta
import hashlib
import socket

import pytest
from sqlalchemy import update

from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web import app as web_app
from maigret.web.case_store import (
    WORKER_STALE_AFTER_SECONDS,
    CaseStore,
    investigation_jobs,
    utcnow,
)
from maigret.web.investigation_input import build_unified_investigation_plan
from maigret.web.profile_discovery_policy import govern_profile_discovery_options


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    def rejected(*_args, **_kwargs):
        raise AssertionError("failure-boundary test attempted network I/O")

    monkeypatch.setattr(socket, "create_connection", rejected)
    monkeypatch.setattr(socket.socket, "connect", rejected)
    monkeypatch.setattr(socket.socket, "connect_ex", rejected)
    monkeypatch.setenv("OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED", "false")
    monkeypatch.setenv("OPENLEDGER_MAIGRET_DISCOVERY_ENABLED", "true")
    monkeypatch.setenv("OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED", "true")
    monkeypatch.setenv("OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED", "true")


@pytest.fixture
def store(tmp_path):
    value = CaseStore(
        f"sqlite:///{tmp_path / 'failure-boundaries.db'}",
        create_schema=True,
    )
    yield value
    value.dispose()


class _Budget:
    def remaining_seconds(self):
        return 2.0


def _digest(*parts):
    return hashlib.sha256("\x1f".join(map(str, parts)).encode()).hexdigest()


def _task(username, index):
    check_id = _digest("check", username, index)
    return {
        "schema_version": 1,
        "task_id": _digest("task", check_id, 0),
        "check_id": check_id,
        "source_id": _digest("source", "maigret"),
        "target_id": _digest("target", username),
        "attempt": 0,
    }


def _result(username, index):
    site = ("GitHub", "GitLab")[index]
    return MaigretCheckResult(
        username=username,
        site_name=site,
        site_url_user=f"https://{site.casefold()}.example/{username}",
        status=MaigretCheckStatus.CLAIMED,
        ids_data={"id": f"{username}-{index}"},
        http_status=200,
    )


def _options(usernames, *, followons=False):
    form = {
        "investigation_token": usernames,
        "investigation_token_type": ["username"] * len(usernames),
        "mode": "quick",
    }
    if followons:
        form.update(
            {
                "enable_github_profile_enrichment": "on",
                "enable_archived_url_evidence": "on",
                "enable_user_scanner_username": "on",
                "user_scanner_platforms_present": "1",
                "user_scanner_platform": ["instagram", "x"],
                "allow_user_scanner_vxtwitter": "on",
            }
        )
    plan = build_unified_investigation_plan(form)
    return govern_profile_discovery_options(
        {"execution_mode": "focused", "investigation_spec": plan}
    )


def _profile_count(report):
    return sum(
        len(row.get("claimed_profiles") or [])
        for row in report.get("individual_reports") or []
    )


def _maigret_accounting(job):
    return next(
        row
        for row in job["collection_accounting"]["stages"]
        if row["stage_id"] == "maigret"
    )


def _expire_worker(store, job_id):
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(
                heartbeat_at=utcnow()
                - timedelta(seconds=WORKER_STALE_AFTER_SECONDS + 1)
            )
        )


def _terminal(task):
    return {
        **task,
        "disposition": "completed",
        "attempted": True,
        "cleanup_state": "not_required",
    }


def test_second_username_flush_preserves_aggregate_observations_for_recovery(
    store,
    monkeypatch,
):
    usernames = ["alicefixture", "bobfixture"]
    options = _options(usernames)
    job_id = store.create_investigation(usernames, options, kind="live")
    claimed = store.claim_next("worker:p3r:aggregate-flush")
    persistent_sink = web_app.PersistentEventSink(
        store,
        job_id,
        worker_id=claimed["worker_id"],
    )

    class CrashAfterCheckpoint:
        reject = False

        def put(self, event):
            if self.reject:
                raise RuntimeError("simulated process loss after the second flush")
            return persistent_sink.put(event)

    crash_sink = CrashAfterCheckpoint()

    def checkpoint(snapshot):
        assert store.save_collection_checkpoint(
            job_id,
            snapshot,
            worker_id=claimed["worker_id"],
        )
        if _profile_count(snapshot) == 3:
            crash_sink.reject = True
        return True

    async def maigret(username, _options, query_notify=None):
        count = 2 if username == usernames[0] else 1
        tasks = [_task(username, index) for index in range(count)]
        query_notify.task_plan(tasks)
        query_notify.set_total(count)
        for index, task in enumerate(tasks):
            query_notify.update(_result(username, index))
            query_notify.task_terminal(_terminal(task))
        query_notify.flush_accounting()
        if username == usernames[1]:
            raise RuntimeError("collector crashed after its committed flush")
        return dict(query_notify.results)

    monkeypatch.setattr(web_app, "maigret_search", maigret)
    runtime_job = {
        "job_id": job_id,
        "queue": crash_sink,
        "cancelled": False,
        "execution_budget_object": _Budget(),
        "collection_checkpoint_sink": checkpoint,
    }

    with pytest.raises(RuntimeError, match="process loss"):
        asyncio.run(web_app._stream_search(runtime_job, usernames, options))

    assert runtime_job["persistence_failed"] is True
    committed = store.get_collection_checkpoint(job_id)
    assert _profile_count(committed) == 3
    assert _maigret_accounting(store.get_job(job_id))["observations"] == 3
    assert _maigret_accounting(committed)["observations"] == 3

    _expire_worker(store, job_id)
    assert store.mark_stale_running(WORKER_STALE_AFTER_SECONDS) == 1
    recovered = store.get_job(job_id)
    assert recovered["status"] == "interrupted"
    assert recovered["collection_status"] == "interrupted"
    assert _profile_count(recovered) == 3
    assert _maigret_accounting(recovered)["observations"] == 3


def test_terminal_persistence_failure_freezes_admission_and_finalization(
    store,
    monkeypatch,
):
    usernames = ["alicefixture"]
    options = _options(usernames, followons=True)
    job_id = store.create_investigation(usernames, options, kind="live")
    claimed = store.claim_next("worker:p3r:terminal-write")
    monkeypatch.setattr(web_app, "case_store", store)
    appended = []
    rejected = []
    original_append = store.append_event

    def append_event(job_id, event, **kwargs):
        appended.append(event["type"])
        if event["type"] == "collection_task_terminal":
            rejected.append(event)
            return 0
        return original_append(job_id, event, **kwargs)

    monkeypatch.setattr(store, "append_event", append_event)
    report_calls = []
    original_reports = web_app.build_reports

    def build_reports(*args, **kwargs):
        write_files = kwargs.get("write_files", True)
        report_calls.append(write_files)
        if write_files:
            raise AssertionError("terminal report publication was attempted")
        return original_reports(*args, **kwargs)

    monkeypatch.setattr(web_app, "build_reports", build_reports)
    finish_calls = []
    original_finish = store.finish

    def finish(*args, **kwargs):
        finish_calls.append((args, kwargs))
        return original_finish(*args, **kwargs)

    monkeypatch.setattr(store, "finish", finish)
    publication_calls = []

    def publish(*_args, **_kwargs):
        publication_calls.append(True)
        raise AssertionError("result publication was attempted")

    monkeypatch.setattr(web_app, "persist_job_result", publish)
    followon_calls = []

    async def forbidden_followon(*_args, **_kwargs):
        followon_calls.append(True)
        raise AssertionError("follow-on source was admitted after persistence loss")

    monkeypatch.setattr(web_app, "run_github_public_profile", forbidden_followon)
    monkeypatch.setattr(web_app, "run_unfurl_url_analysis", forbidden_followon)
    monkeypatch.setattr(web_app, "run_wayback_capture_index", forbidden_followon)
    monkeypatch.setattr(web_app, "run_user_scanner_usernames", forbidden_followon)

    async def maigret(username, _options, query_notify=None):
        tasks = [_task(username, index) for index in range(2)]
        query_notify.task_plan(tasks)
        query_notify.set_total(2)
        query_notify.update(_result(username, 0))
        query_notify.checkpoint()
        query_notify.task_terminal(_terminal(tasks[0]))
        query_notify.flush_accounting()
        raise AssertionError("terminal rejection should abort the flush")

    monkeypatch.setattr(web_app, "maigret_search", maigret)

    assert web_app.run_persistent_job(store, claimed) is False

    running = store.get_job(job_id)
    checkpoint = store.get_collection_checkpoint(job_id)
    events = [row["event"] for row in store.get_events(job_id)]
    assert running["status"] == "running"
    assert running.get("result") is None
    assert _profile_count(checkpoint) == 1
    assert "collection_task_plan" in appended
    assert "collection_accounting" in appended
    assert rejected
    assert all(event["tasks"] == rejected[0]["tasks"] for event in rejected)
    assert not any(event["type"] == "collection_task_terminal" for event in events)
    assert not any(event["type"] == "done" for event in events)
    assert report_calls and set(report_calls) == {False}
    assert finish_calls == []
    assert publication_calls == []
    assert followon_calls == []

    _expire_worker(store, job_id)
    assert store.mark_stale_running(WORKER_STALE_AFTER_SECONDS) == 1
    recovered = store.get_job(job_id)
    maigret = _maigret_accounting(recovered)
    assert recovered["status"] == "interrupted"
    assert _profile_count(recovered) == 1
    assert maigret["planned"] == 2
    assert maigret["started"] is None
    assert maigret["unknown"] == 2
    assert maigret["cleanup_complete"] is False
    assert maigret["unattempted"] == 0
    assert [row["event"]["type"] for row in store.get_events(job_id)[-1:]] == ["done"]


def test_cancelled_sink_only_quietly_discards_display_events(store, monkeypatch):
    usernames = ["alicefixture"]
    job_id = store.create_investigation(
        usernames,
        _options(usernames),
        kind="live",
    )
    claimed = store.claim_next("worker:p3r:cancelled-projection")
    sink = web_app.PersistentEventSink(
        store,
        job_id,
        worker_id=claimed["worker_id"],
    )
    task = _task(usernames[0], 0)
    assert sink.put({"type": "collection_task_plan", "tasks": [task]}) > 0
    assert store.request_cancel(job_id) is True

    assert (
        sink.put(
            {
                "type": "collector_completed",
                "collector": "maigret",
                "observations": 1,
                "found": 1,
            }
        )
        == 0
    )
    assert not any(
        row["event"]["type"] == "collector_completed"
        for row in store.get_events(job_id)
    )

    original_append = store.append_event

    def reject_terminal(job_id, event, **kwargs):
        if event["type"] == "collection_task_terminal":
            return 0
        return original_append(job_id, event, **kwargs)

    monkeypatch.setattr(store, "append_event", reject_terminal)
    with pytest.raises(RuntimeError, match="ownership lost or finalized"):
        sink.put(
            {
                "type": "collection_task_terminal",
                "tasks": [_terminal(task)],
            }
        )


def test_outer_worker_exception_remains_reconcilable_before_first_checkpoint(
    store, monkeypatch
):
    from maigret.web import worker

    job_id = store.create_investigation(
        ['alicefixture'], _options(['alicefixture']), kind='live'
    )
    claimed = store.claim_next('worker:p3r:outer-failure')

    def crash(*_args, **_kwargs):
        raise RuntimeError('outer worker failure before accounting')

    monkeypatch.setattr(worker, 'execute_profile_process', crash)
    worker.execute_job(store, claimed, shutdown_check=lambda: False)
    assert store.get_job(job_id)['status'] == 'running'
    assert not any(row['event']['type'] == 'done' for row in store.get_events(job_id))
    _expire_worker(store, job_id)
    assert store.mark_stale_running() == 1
    result = store.get_job(job_id)
    assert result['status'] == 'interrupted'
    assert result['collection_accounting']['known'] is False
    assert result['collection_accounting']['stages'] == []
    assert sum(row['event']['type'] == 'done' for row in store.get_events(job_id)) == 1
    assert store.mark_stale_running() == 0
