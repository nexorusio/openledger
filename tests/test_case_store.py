from datetime import timedelta

import pytest
from sqlalchemy import func, select, update

from maigret.web.case_store import (
    CaseStore,
    cases,
    claim_evidence,
    claim_reviews,
    database_url_from_environment,
    investigation_events,
    investigation_jobs,
    personas,
    persona_claims,
    utcnow,
)


@pytest.fixture
def store(tmp_path):
    instance = CaseStore(
        f"sqlite:///{tmp_path / 'openledger.db'}",
        create_schema=True,
    )
    yield instance
    instance.dispose()


def test_job_lifecycle_is_transactional_and_auditable(store):
    job_id = store.create_investigation(
        ["alice", "bob"],
        {"top_sites": 500, "all_sites": False},
    )
    queued = store.get_job(job_id)
    assert queued["status"] == "queued"
    assert queued["usernames"] == ["alice", "bob"]
    assert queued["progress"] == {"checked": 0, "total": None, "found": 0}

    claimed = store.claim_next("worker:test")
    assert claimed["job_id"] == job_id
    assert claimed["status"] == "running"
    assert claimed["attempts"] == 1

    store.append_event(job_id, {"type": "start", "username": "alice", "total": 10})
    store.append_event(job_id, {"type": "progress", "checked": 4, "total": 10})
    store.append_event(job_id, {"type": "found", "site": "Example"})
    progress = store.get_job(job_id)["progress"]
    assert progress["checked"] == 4
    assert progress["total"] == 10
    assert progress["found"] == 1

    events = store.get_events(job_id)
    assert [item["event"]["type"] for item in events] == [
        "queued",
        "running",
        "start",
        "progress",
        "found",
    ]
    assert (
        store.get_events(job_id, after_id=events[2]["id"])[0]["event"]["type"]
        == "progress"
    )

    store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": ["alice", "bob"],
            "individual_reports": [],
            "graph_file": f"search_{job_id}/graph.html",
            "found_count": 1,
        },
    )
    completed = store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["found_count"] == 1


def test_case_personas_and_events_are_removed_with_terminal_job(store):
    job_id = store.create_investigation(["alice"], {})
    job = store.claim_next("worker:test")
    store.finish(job_id, {"status": "cancelled", "usernames": job["usernames"]})
    assert store.delete_job(job_id) is True
    assert store.get_job(job_id) is None

    with store.engine.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(cases)) == 0
        assert connection.scalar(select(func.count()).select_from(personas)) == 0
        assert (
            connection.scalar(select(func.count()).select_from(investigation_events))
            == 0
        )


def test_active_job_cannot_be_deleted(store):
    job_id = store.create_investigation(["alice"], {})
    with pytest.raises(ValueError, match="Active investigations"):
        store.delete_job(job_id)


def test_legacy_terminal_result_is_imported_once(store):
    result = {
        "status": "completed",
        "session_folder": "search_legacy1",
        "usernames": ["alice"],
        "individual_reports": [],
        "graph_file": "search_legacy1/graph.html",
        "found_count": 3,
        "started_at": "2026-08-27 12:00:00",
    }
    assert store.import_legacy_result("legacy1", result) is True
    assert store.import_legacy_result("legacy1", result) is False
    imported = store.get_job("legacy1")
    assert imported["kind"] == "legacy"
    assert imported["status"] == "completed"
    assert imported["found_count"] == 3


def test_cancel_request_is_persistent_and_idempotent(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    assert store.request_cancel(job_id) is True
    assert store.is_cancel_requested(job_id) is True
    assert store.get_job(job_id)["status"] == "cancel_requested"
    assert store.request_cancel(job_id) is False


def test_queued_job_is_cancelled_immediately(store):
    job_id = store.create_investigation(["alice"], {})
    assert store.request_cancel(job_id) is True
    cancelled = store.get_job(job_id)
    assert cancelled["status"] == "cancelled"
    assert cancelled["completed_at"] is not None
    assert store.claim_next("worker:test") is None


def test_store_provides_worker_lock_handle(store):
    lock = store.try_acquire_worker_lock()
    assert lock is not None
    lock.close()


def test_stale_worker_job_is_marked_interrupted(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(heartbeat_at=utcnow() - timedelta(hours=1))
        )
    assert store.mark_stale_running(300) == 1
    interrupted = store.get_job(job_id)
    assert interrupted["status"] == "interrupted"
    assert "worker stopped" in interrupted["error"]


def test_database_url_uses_protected_password_file(tmp_path, monkeypatch):
    password_file = tmp_path / "postgres_password"
    password_file.write_text("complex:/ password\n", encoding="utf-8")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_PASSWORD_FILE", str(password_file))
    monkeypatch.setenv("DATABASE_USER", "case analyst")
    monkeypatch.setenv("DATABASE_HOST", "private-db")
    monkeypatch.setenv("DATABASE_NAME", "case records")
    url = database_url_from_environment()
    assert url == (
        "postgresql+psycopg://case%20analyst:complex%3A%2F%20password"
        "@private-db:5432/case%20records"
    )
    assert "complex:/ password" not in url


def test_completed_findings_create_traceable_persona_claims(store):
    job_id = store.create_investigation(["mastercorbuzier"], {})
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["mastercorbuzier"],
        "graph_file": f"search_{job_id}/graph.html",
        "found_count": 1,
        "individual_reports": [
            {
                "username": "mastercorbuzier",
                "claimed_profiles": [
                    {
                        "site_name": "Example Social",
                        "url": "https://social.example/mastercorbuzier",
                        "confidence": "strong",
                        "evidence": {
                            "fullname": "Deddy Corbuzier",
                            "location": "Indonesia",
                            "description": "Indonesian media figure",
                        },
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    assert store.sync_persona_claims(job_id, result) == 4

    case = store.get_case(store.get_job(job_id)["case_id"])
    persona = store.get_persona(case["personas"][0]["id"])
    fields = {claim["field_name"] for claim in persona["claims"]}
    assert fields == {"social_account", "full_name", "current_location", "summary"}
    full_name = next(
        claim for claim in persona["claims"] if claim["field_name"] == "full_name"
    )
    assert full_name["display_value"] == "Deddy Corbuzier"
    assert full_name["confidence"] == 90
    assert full_name["review_status"] == "pending"
    assert full_name["evidence"][0]["source_url"].startswith("https://")


def test_refresh_preserves_human_review_and_graph_excludes_rejected_claim(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice"],
        "graph_file": f"search_{job_id}/graph.html",
        "found_count": 1,
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": "https://example.test/alice",
                        "confidence": "moderate",
                        "evidence": {"email": "alice@example.test"},
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    case = store.get_case(store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    persona = store.get_persona(persona_id)
    email = next(claim for claim in persona["claims"] if claim["field_name"] == "email")
    assert store.review_claim(email["id"], "rejected", "analyst", "Collision")

    refresh_job_id = store.repeat_persona_investigation(persona_id)
    queued_refresh = store.get_job(refresh_job_id)
    assert queued_refresh["case_id"] == case["id"]
    assert queued_refresh["kind"] == "refresh"
    assert queued_refresh["usernames"] == ["alice"]
    store.claim_next("worker:refresh")
    refreshed_result = dict(result)
    refreshed_result["session_folder"] = f"search_{refresh_job_id}"
    refreshed_result["graph_file"] = f"search_{refresh_job_id}/graph.html"
    store.finish(refresh_job_id, refreshed_result)
    assert store.sync_persona_claims(refresh_job_id, refreshed_result) == 2
    refreshed = store.get_persona(persona_id)
    email = next(
        claim for claim in refreshed["claims"] if claim["field_name"] == "email"
    )
    assert email["review_status"] == "rejected"
    assert email["reviews"][0]["note"] == "Collision"
    assert email["source_job_id"] == refresh_job_id
    graph = store.build_persona_graph(persona_id)
    assert all(node.get("label") != "alice@example.test" for node in graph["nodes"])

    assert store.delete_job(job_id) is True
    assert store.get_job(job_id) is None
    assert store.get_case(case["id"]) is not None
    assert store.get_job(refresh_job_id) is not None

    with store.engine.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(persona_claims)) == 2
        assert connection.scalar(select(func.count()).select_from(claim_evidence)) == 2
        assert connection.scalar(select(func.count()).select_from(claim_reviews)) == 1
