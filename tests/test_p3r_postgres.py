# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Production-shaped P3R checks for the disposable PostgreSQL CI service.

The fixtures exercise only the queue and persistence boundary.  All collection
results are local, deterministic values; these tests never invoke a source.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import hashlib
import os

import pytest
from sqlalchemy import select, text, update

from maigret.web.case_store import (
    WORKER_STALE_AFTER_SECONDS,
    CaseStore,
    investigation_jobs,
    utcnow,
)
from maigret.web.investigation_input import (
    build_unified_investigation_plan,
    search_usernames,
)

POSTGRES_URL = os.getenv("OPENLEDGER_TEST_POSTGRES_URL", "")
pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="OPENLEDGER_TEST_POSTGRES_URL is not configured",
)


@pytest.fixture
def postgres_store():
    store = CaseStore(POSTGRES_URL)
    with store.engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE cases RESTART IDENTITY CASCADE"))
    yield store
    with store.engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE cases RESTART IDENTITY CASCADE"))
    store.dispose()


def _plan(target_count):
    usernames = [f"p3rtarget{index:02d}" for index in range(target_count)]
    plan = build_unified_investigation_plan(
        {
            "investigation_token": usernames,
            "investigation_token_type": ["username"] * target_count,
            "mode": "quick",
        }
    )
    return usernames, plan


def _enqueue(store, target_count):
    usernames, plan = _plan(target_count)
    job_id = store.create_investigation(
        usernames,
        {
            "execution_mode": "focused",
            "investigation_spec": plan,
            "proxy_configured": False,
            "tor_proxy_configured": False,
            "i2p_proxy_configured": False,
        },
    )
    return job_id, usernames


def _claim_result(job, *, full_name="Alice Example"):
    """Return permitted, review-pending evidence without source I/O."""
    username = job["usernames"][0]
    return {
        "status": "completed",
        "session_folder": f"search_{job['job_id']}",
        "usernames": list(job["usernames"]),
        "individual_reports": [
            {
                "username": username,
                "claimed_profiles": [
                    {
                        "site_name": "Local Fixture",
                        "url": f"https://social.example/{username}",
                        "confidence": "strong",
                        "evidence": {"fullname": full_name},
                    }
                ],
            }
        ],
        "found_count": 1,
    }


def _raw_progress(store, job_id):
    with store.engine.connect() as connection:
        return dict(
            connection.scalar(
                select(investigation_jobs.c.progress).where(
                    investigation_jobs.c.id == job_id
                )
            )
            or {}
        )


def _opaque_id(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _task_fixture(label="fixture", *, attempt=0):
    return {
        "schema_version": 1,
        "task_id": _opaque_id(f"task:{label}"),
        "check_id": _opaque_id(f"check:{label}"),
        "source_id": _opaque_id("source:local-fixture"),
        "target_id": _opaque_id(f"target:{label}"),
        "attempt": attempt,
    }


def _accounting_event(revision, *, completed):
    return {
        "type": "collection_accounting",
        "collection_accounting": {
            "schema_version": 1,
            "revision": revision,
            "state": "completed" if completed else "running",
            "known": True,
            "stages": [
                {
                    "stage_id": "maigret",
                    "engine_id": "maigret",
                    "unit": "site_checks",
                    "status": "completed" if completed else "running",
                    "reason": None,
                    "planned": 1,
                    "started": 1,
                    "terminal": int(completed),
                    "completed": int(completed),
                    "errors": 0,
                    "timeouts": 0,
                    "cancelled": 0,
                    "interrupted": 0,
                    "unattempted": 0,
                    "unknown": 0 if completed else 1,
                    "observations": int(completed),
                }
            ],
        },
    }


@pytest.mark.parametrize("queue_count", (1, 10, 50))
def test_postgres_claims_every_queued_job_once(postgres_store, queue_count):
    expected = dict(_enqueue(postgres_store, 1) for _ in range(queue_count))
    with ThreadPoolExecutor(max_workers=min(queue_count, 8)) as pool:
        claimed = list(
            pool.map(
                postgres_store.claim_next,
                (f"worker:p3r:queue:{index}" for index in range(queue_count)),
            )
        )

    for job in claimed:
        assert job is not None
        assert job["usernames"] == expected[job["job_id"]]
        assert postgres_store.finish(
            job["job_id"],
            {
                "status": "completed",
                "session_folder": f"search_{job['job_id']}",
                "usernames": job["usernames"],
                "individual_reports": [],
                "found_count": 0,
            },
            worker_id=job["worker_id"],
        )

    assert postgres_store.claim_next("worker:p3r:empty") is None
    assert {job["job_id"] for job in claimed} == set(expected)
    assert len(claimed) == len({job["job_id"] for job in claimed})


@pytest.mark.parametrize("target_count", (1, 4, 16))
def test_postgres_preserves_bounded_target_plan_at_worker_claim(
    postgres_store, target_count
):
    job_id, usernames = _enqueue(postgres_store, target_count)

    queued = postgres_store.get_job(job_id)
    claimed = postgres_store.claim_next(f"worker:p3r:targets:{target_count}")

    assert queued["usernames"] == usernames
    assert claimed["usernames"] == usernames
    assert search_usernames(claimed["options"]["investigation_spec"]) == usernames
    assert [
        target["value"]
        for target in claimed["options"]["investigation_spec"]["search_targets"]
    ] == usernames


def test_postgres_rejects_wrong_and_expired_worker_event_writes(postgres_store):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:owner")
    initial = postgres_store.get_job(job_id)
    initial_events = postgres_store.get_events(job_id)

    assert (
        postgres_store.append_event(
            job_id,
            {"type": "progress", "checked": 1, "total": 1},
            runtime_guard=True,
            worker_id="worker:p3r:not-owner",
        )
        == 0
    )
    assert postgres_store.get_events(job_id) == initial_events
    assert postgres_store.get_job(job_id)["progress"] == initial["progress"]

    expired_at = utcnow() - timedelta(seconds=WORKER_STALE_AFTER_SECONDS + 1)
    with postgres_store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(heartbeat_at=expired_at)
        )
    before_expired_write = postgres_store.get_job(job_id)
    events_before_expired_write = postgres_store.get_events(job_id)

    assert (
        postgres_store.append_event(
            job_id,
            {"type": "progress", "checked": 1, "total": 1},
            runtime_guard=True,
            worker_id=claimed["worker_id"],
        )
        == 0
    )
    after_expired_write = postgres_store.get_job(job_id)
    assert postgres_store.get_events(job_id) == events_before_expired_write
    assert after_expired_write["progress"] == before_expired_write["progress"]
    assert after_expired_write["status"] == "running"
    assert after_expired_write.get("result") == before_expired_write.get("result")
    assert after_expired_write["completed_at"] is None

    assert postgres_store.mark_stale_running(WORKER_STALE_AFTER_SECONDS) == 1
    interrupted = postgres_store.get_job(job_id)
    assert interrupted["status"] == "interrupted"
    events = postgres_store.get_events(job_id)
    assert events[:-1] == events_before_expired_write
    assert events[-1]['event']['type'] == 'done'
    assert events[-1]['event']['status'] == 'interrupted'
    assert interrupted['collection_accounting']['known'] is False
    assert interrupted['collection_accounting']['stages'] == []
    assert postgres_store.mark_stale_running(WORKER_STALE_AFTER_SECONDS) == 0
    assert postgres_store.get_events(job_id) == events


@pytest.mark.parametrize("claimed", (False, True), ids=("queued", "running"))
def test_postgres_repeated_stop_is_idempotent(postgres_store, claimed):
    job_id, _usernames = _enqueue(postgres_store, 1)
    if claimed:
        postgres_store.claim_next("worker:p3r:stop")

    assert postgres_store.request_cancel(job_id) is True
    first = postgres_store.get_job(job_id)
    assert postgres_store.request_cancel(job_id) is True
    second = postgres_store.get_job(job_id)

    expected_status = "cancel_requested" if claimed else "cancelled"
    expected_event = "cancel_requested" if claimed else "cancelled"
    assert first["status"] == second["status"] == expected_status
    assert first["cancel_requested_at"] == second["cancel_requested_at"]
    assert [row["event"]["type"] for row in postgres_store.get_events(job_id)].count(
        expected_event
    ) == 1


def test_postgres_replayed_task_events_are_idempotent_and_attempts_remain_distinct(
    postgres_store,
):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:replay")
    task = _task_fixture()
    planned = {"type": "collection_task_plan", "tasks": [task]}

    first_id = postgres_store.append_event(
        job_id,
        planned,
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    replay_id = postgres_store.append_event(
        job_id,
        {"tasks": [dict(reversed(tuple(task.items())))], "type": planned["type"]},
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    retry_id = postgres_store.append_event(
        job_id,
        {
            "type": "collection_task_plan",
            "tasks": [
                {
                    **task,
                    "attempt": 1,
                    "task_id": hashlib.sha256(b'fixture-retry-attempt').hexdigest(),
                }
            ],
        },
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    terminal = {
        "type": "collection_task_terminal",
        "tasks": [
            {
                **task,
                "disposition": "error",
                "attempted": True,
                "cleanup_state": "complete",
                "reason": "fixture_error",
            }
        ],
    }
    terminal_id = postgres_store.append_event(
        job_id,
        terminal,
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    terminal_replay_id = postgres_store.append_event(
        job_id,
        terminal,
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )

    assert replay_id == first_id
    assert retry_id != first_id
    assert terminal_replay_id == terminal_id
    task_events = [
        row["event"]
        for row in postgres_store.get_events(job_id)
        if row["event"]["type"].startswith("collection_task_")
    ]
    assert len(task_events) == 3
    assert len({event["event_key"] for event in task_events}) == 3
    assert all(len(event["event_key"]) == 64 for event in task_events)
    assert [event["tasks"][0]["attempt"] for event in task_events[:2]] == [0, 1]


def test_postgres_accounting_revision_never_regresses(postgres_store):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:revision")

    current_id = postgres_store.append_event(
        job_id,
        _accounting_event(2, completed=True),
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    stale_id = postgres_store.append_event(
        job_id,
        _accounting_event(1, completed=False),
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )

    assert current_id > 0
    assert stale_id == 0
    assert (
        _raw_progress(postgres_store, job_id)["collection_accounting"]["revision"] == 2
    )
    accounting_events = [
        row
        for row in postgres_store.get_events(job_id)
        if row["event"]["type"] == "collection_accounting"
    ]
    assert len(accounting_events) == 1


def test_postgres_checkpoint_and_pending_claims_share_one_transaction(
    postgres_store, monkeypatch
):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:checkpoint")
    checkpoint = _claim_result(claimed)
    original_sync = postgres_store.sync_persona_claims

    def sync_then_fail(sync_job_id, result, *, connection=None):
        original_sync(sync_job_id, result, connection=connection)
        raise RuntimeError("injected checkpoint failure")

    monkeypatch.setattr(postgres_store, "sync_persona_claims", sync_then_fail)
    with pytest.raises(RuntimeError, match="injected checkpoint failure"):
        postgres_store.save_collection_checkpoint(
            job_id,
            checkpoint,
            worker_id=claimed["worker_id"],
        )

    persona_id = postgres_store.get_case(claimed["case_id"])["personas"][0]["id"]
    assert "collection_checkpoint" not in _raw_progress(postgres_store, job_id)
    assert postgres_store.get_persona(persona_id)["claims"] == []

    monkeypatch.setattr(postgres_store, "sync_persona_claims", original_sync)
    assert postgres_store.save_collection_checkpoint(
        job_id,
        checkpoint,
        worker_id=claimed["worker_id"],
    )
    stored_checkpoint = _raw_progress(postgres_store, job_id)["collection_checkpoint"]
    assert stored_checkpoint["found_count"] == 1
    assert stored_checkpoint["individual_reports"] == checkpoint["individual_reports"]
    assert {
        claim["field_name"]
        for claim in postgres_store.get_persona(persona_id)["claims"]
    } == {"social_account", "full_name"}
    assert "collection_checkpoint" not in postgres_store.get_job(job_id)["progress"]


def test_postgres_committed_checkpoint_survives_disconnect_and_lease_expiry(
    postgres_store,
):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:disconnect")
    checkpoint = _claim_result(claimed)
    last_seen_id = postgres_store.get_events(job_id)[-1]["id"]

    assert postgres_store.save_collection_checkpoint(
        job_id,
        checkpoint,
        worker_id=claimed["worker_id"],
    )
    accounting_id = postgres_store.append_event(
        job_id,
        _accounting_event(1, completed=False),
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )

    expired_at = utcnow() - timedelta(seconds=WORKER_STALE_AFTER_SECONDS + 1)
    with postgres_store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(heartbeat_at=expired_at)
        )
    assert postgres_store.mark_stale_running(WORKER_STALE_AFTER_SECONDS) == 1

    resumed_events = postgres_store.get_events(job_id, after_id=last_seen_id)
    assert resumed_events[0]['id'] == accounting_id
    assert [row['event']['type'] for row in resumed_events] == [
        'collection_accounting',
        'done',
    ]
    assert resumed_events[-1]['event']['status'] == 'interrupted'
    assert (
        resumed_events[-1]['event']['collection_accounting']['state'] == 'interrupted'
    )
    assert resumed_events[0]["event"]["collection_accounting"]["revision"] == 1
    assert (
        _raw_progress(postgres_store, job_id)["collection_checkpoint"]["found_count"]
        == 1
    )
    persona_id = postgres_store.get_case(claimed["case_id"])["personas"][0]["id"]
    assert {
        claim["field_name"]
        for claim in postgres_store.get_persona(persona_id)["claims"]
    } == {"social_account", "full_name"}
    assert (
        postgres_store.save_collection_checkpoint(
            job_id,
            {**checkpoint, "found_count": 2},
            worker_id=claimed["worker_id"],
        )
        is False
    )
    assert (
        _raw_progress(postgres_store, job_id)["collection_checkpoint"]["found_count"]
        == 1
    )


def test_postgres_atomic_finish_rolls_back_and_blocks_late_writes(postgres_store):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:finish")
    result = _claim_result(claimed)
    persona_id = postgres_store.get_case(claimed["case_id"])["personas"][0]["id"]
    calls = []
    terminal_event = {
        "type": "done",
        "status": "completed",
        "found_count": 1,
    }

    assert (
        postgres_store.finish(
            job_id,
            result,
            worker_id="worker:p3r:not-owner",
            synchronize_claims=True,
            publication=lambda: calls.append("wrong-owner"),
            terminal_event=terminal_event,
        )
        is False
    )
    assert calls == []

    def fail_publication():
        calls.append("failed")
        raise RuntimeError("injected publication failure")

    with pytest.raises(RuntimeError, match="injected publication failure"):
        postgres_store.finish(
            job_id,
            result,
            worker_id=claimed["worker_id"],
            synchronize_claims=True,
            publication=fail_publication,
            terminal_event=terminal_event,
        )

    after_rollback = postgres_store.get_job(job_id)
    assert after_rollback["status"] == "running"
    assert after_rollback.get("result") is None
    assert after_rollback["completed_at"] is None
    assert postgres_store.get_persona(persona_id)["claims"] == []
    assert not any(
        row["event"]["type"] == "done" for row in postgres_store.get_events(job_id)
    )

    assert postgres_store.finish(
        job_id,
        result,
        worker_id=claimed["worker_id"],
        synchronize_claims=True,
        publication=lambda: calls.append("published"),
        terminal_event=terminal_event,
    )
    assert calls == ["failed", "published"]
    completed = postgres_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["found_count"] == 1
    assert {
        claim["field_name"]
        for claim in postgres_store.get_persona(persona_id)["claims"]
    } == {"social_account", "full_name"}
    assert (
        sum(row["event"]["type"] == "done" for row in postgres_store.get_events(job_id))
        == 1
    )

    assert (
        postgres_store.append_event(
            job_id,
            {
                "type": "collection_task_terminal",
                "tasks": [
                    {
                        **_task_fixture(),
                        "disposition": "completed",
                        "attempted": True,
                        "cleanup_state": "not_required",
                    }
                ],
            },
            runtime_guard=True,
            worker_id=claimed["worker_id"],
        )
        == 0
    )
    assert (
        postgres_store.save_collection_checkpoint(
            job_id,
            {**result, "found_count": 2},
            worker_id=claimed["worker_id"],
        )
        is False
    )
    assert (
        postgres_store.finish(
            job_id,
            {**result, "found_count": 2},
            worker_id=claimed["worker_id"],
            synchronize_claims=True,
            terminal_event=terminal_event,
        )
        is False
    )
    assert postgres_store.get_job(job_id)["found_count"] == 1
    assert (
        sum(row["event"]["type"] == "done" for row in postgres_store.get_events(job_id))
        == 1
    )


def test_postgres_rejects_a_second_task_id_for_the_same_logical_attempt(
    postgres_store,
):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:logical-attempt")
    task = _task_fixture("logical-attempt")

    plan_id = postgres_store.append_event(
        job_id,
        {"type": "collection_task_plan", "tasks": [task]},
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    conflicting = {
        **task,
        "task_id": _opaque_id("task:logical-attempt:conflicting-id"),
    }
    with pytest.raises(ValueError, match="logical collection attempt"):
        postgres_store.append_event(
            job_id,
            {"type": "collection_task_plan", "tasks": [conflicting]},
            runtime_guard=True,
            worker_id=claimed["worker_id"],
        )

    task_events = [
        row
        for row in postgres_store.get_events(job_id)
        if row["event"]["type"].startswith("collection_task_")
    ]
    assert [row["id"] for row in task_events] == [plan_id]
    assert task_events[0]["event"]["tasks"] == [task]


def test_postgres_rebatching_and_cleanup_progression_are_idempotent(
    postgres_store,
):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:cleanup")
    first = _task_fixture("cleanup-first")
    second = _task_fixture("cleanup-second")

    plan_id = postgres_store.append_event(
        job_id,
        {"type": "collection_task_plan", "tasks": [first, second]},
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    assert (
        postgres_store.append_event(
            job_id,
            {"type": "collection_task_plan", "tasks": [first]},
            runtime_guard=True,
            worker_id=claimed["worker_id"],
        )
        == plan_id
    )

    def cleanup(state):
        return {
            "type": "collection_task_cleanup",
            "tasks": [
                {
                    **first,
                    "event_type": "cleanup",
                    "cleanup_state": state,
                }
            ],
        }

    with pytest.raises(ValueError, match="no pending terminal task"):
        postgres_store.append_event(
            job_id,
            cleanup("complete"),
            runtime_guard=True,
            worker_id=claimed["worker_id"],
        )

    terminal_id = postgres_store.append_event(
        job_id,
        {
            "type": "collection_task_terminal",
            "tasks": [
                {
                    **first,
                    "disposition": "timeout",
                    "attempted": True,
                    "cleanup_state": "pending",
                    "reason": "stage_budget_exhausted",
                }
            ],
        },
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    incomplete_id = postgres_store.append_event(
        job_id,
        cleanup("incomplete"),
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    complete_id = postgres_store.append_event(
        job_id,
        cleanup("complete"),
        runtime_guard=True,
        worker_id=claimed["worker_id"],
    )
    assert (
        postgres_store.append_event(
            job_id,
            cleanup("complete"),
            runtime_guard=True,
            worker_id=claimed["worker_id"],
        )
        == complete_id
    )
    with pytest.raises(ValueError, match="terminal disposition"):
        postgres_store.append_event(
            job_id,
            cleanup("incomplete"),
            runtime_guard=True,
            worker_id=claimed["worker_id"],
        )

    task_events = [
        row["event"]
        for row in postgres_store.get_events(job_id)
        if row["event"]["type"].startswith("collection_task_")
    ]
    assert plan_id < terminal_id < incomplete_id < complete_id
    assert [event["type"] for event in task_events] == [
        "collection_task_plan",
        "collection_task_terminal",
        "collection_task_cleanup",
        "collection_task_cleanup",
    ]
    assert [event["tasks"][0].get("cleanup_state") for event in task_events[1:]] == [
        "pending",
        "incomplete",
        "complete",
    ]


def test_postgres_committed_stop_blocks_normal_finish_and_publication(
    postgres_store,
):
    job_id, _usernames = _enqueue(postgres_store, 1)
    claimed = postgres_store.claim_next("worker:p3r:stop-before-finish")
    result = _claim_result(claimed)
    persona_id = postgres_store.get_case(claimed["case_id"])["personas"][0]["id"]
    published = []

    assert postgres_store.request_cancel(job_id) is True
    assert (
        postgres_store.finish(
            job_id,
            result,
            worker_id=claimed["worker_id"],
            synchronize_claims=True,
            publication=lambda: published.append(True),
            terminal_event={"type": "done", "status": "completed"},
        )
        is False
    )

    stopped = postgres_store.get_job(job_id)
    assert published == []
    assert stopped["status"] == "cancel_requested"
    assert stopped.get("result") is None
    assert stopped["completed_at"] is None
    assert postgres_store.get_persona(persona_id)["claims"] == []
    assert not any(
        row["event"]["type"] == "done" for row in postgres_store.get_events(job_id)
    )
