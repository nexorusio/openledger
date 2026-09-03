import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from sqlalchemy import func, select, update

from maigret.web.case_store import (
    CaseStore,
    ReferencedCaseError,
    _bounded_round_robin_case_records,
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
from maigret.web.persona_intelligence import extract_case_chat_persona_claims


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


def test_terminal_case_and_all_of_its_jobs_can_be_deleted(store):
    first_job_id = store.create_investigation(["alice"], {})
    first_job = store.claim_next("worker:test")
    store.finish(
        first_job_id,
        {"status": "cancelled", "usernames": first_job["usernames"]},
    )
    case = store.get_case(first_job["case_id"])
    second_job_id = store.repeat_persona_investigation(case["personas"][0]["id"])
    second_job = store.claim_next("worker:test")
    store.finish(
        second_job_id,
        {"status": "cancelled", "usernames": second_job["usernames"]},
    )

    assert store.delete_case(case["id"]) is True
    assert store.get_case(case["id"]) is None
    assert store.get_job(first_job_id) is None
    assert store.get_job(second_job_id) is None


def test_case_delete_rechecks_exact_confirmation_inside_transaction(store):
    job_id = store.create_investigation(["alice"], {})
    job = store.claim_next("worker:test")
    store.finish(job_id, {"status": "cancelled", "usernames": job["usernames"]})

    with pytest.raises(ValueError, match="confirmation does not match"):
        store.delete_case(job["case_id"], confirmation_name="Alice")

    assert store.get_case(job["case_id"]) is not None


def test_case_with_active_investigation_cannot_be_deleted(store):
    job_id = store.create_investigation(["alice"], {})
    case_id = store.get_job(job_id)["case_id"]

    with pytest.raises(ValueError, match="active investigations"):
        store.delete_case(case_id)

    assert store.get_case(case_id) is not None


def test_case_chat_is_durable_and_persona_proposals_retain_message_provenance(store):
    job_id = store.create_investigation(["alice"], {})
    case = store.get_case(store.get_job(job_id)["case_id"])
    case_id = case["id"]
    persona_id = case["personas"][0]["id"]
    user_message = store.append_case_chat_message(
        case_id,
        role="user",
        author="field.analyst",
        content="Alice works at Acme Labs.",
        persona_id=persona_id,
    )
    assistant_message = store.append_case_chat_message(
        case_id,
        role="assistant",
        author="OpenLedger AI",
        content="That statement is unverified and can be proposed for review.",
        persona_id=persona_id,
        proposals={"status": "processing", "count": 0},
        model="test-model",
    )
    diagnostics = {}
    candidates = extract_case_chat_persona_claims(
        [
            {
                "field_name": "company",
                "value": "Acme Labs",
                "confidence": 70,
                "evidence_basis": "user_statement",
                "source_url": None,
                "source_title": None,
                "reason": "The analyst explicitly supplied this employer.",
                "latitude": None,
                "longitude": None,
                "coordinate_precision": None,
            }
        ],
        sources=[],
        target_persona="alice",
        model="test-model",
        user_message=user_message["content"],
        user_message_id=user_message["id"],
        assistant_message_id=assistant_message["id"],
        provided_by="field.analyst",
        diagnostics=diagnostics,
    )
    synchronized = store.sync_case_chat_persona_claims(
        case_id, persona_id, candidates
    )
    store.update_case_chat_message_proposals(
        assistant_message["id"],
        {
            "status": "pending_review",
            "count": synchronized["count"],
            "persona_id": persona_id,
        },
    )

    retained = store.list_case_chat_messages(case_id)
    assert [message["role"] for message in retained] == ["user", "assistant"]
    assert retained[0]["content"] == "Alice works at Acme Labs."
    assert retained[1]["proposals"]["status"] == "pending_review"
    claim = next(
        item
        for item in store.get_persona(persona_id)["claims"]
        if item["field_name"] == "company"
    )
    assert claim["review_status"] == "pending"
    assert claim["confidence"] == 50
    assert claim["source_engine"] == "case_chat_user_statement"
    assert claim["evidence"][0]["details"]["unverified_user_statement"] is True
    lineage = store.get_claim_lineage(claim["id"])
    assert lineage[0]["provenance_type"] == "case_chat_message"
    assert lineage[0]["chat_message_id"] == user_message["id"]
    assert diagnostics["accepted"] == 1


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


def test_persona_export_snapshot_blocks_concurrent_review_until_capture(
    store, monkeypatch
):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "usernames": ["alice"],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Example Social",
                        "url": "https://example.test/alice",
                        "confidence": "strong",
                        "evidence": {"fullname": "Alice Example"},
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    case = store.get_case(store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    claim = next(
        item
        for item in store.get_persona(persona_id)["claims"]
        if item["field_name"] == "full_name"
    )
    store.review_claim(claim["id"], "approved", "first-analyst")
    tied_observed_at = utcnow()
    with store.engine.begin() as connection:
        connection.execute(
            claim_evidence.insert(),
            [
                {
                    "id": "00000000-0000-0000-0000-00000000000b",
                    "claim_id": claim["id"],
                    "evidence_type": "cited_public_web",
                    "source_name": "Source B",
                    "source_url": "https://example.test/b",
                    "details": {},
                    "fingerprint": "b" * 64,
                    "observed_at": tied_observed_at,
                },
                {
                    "id": "00000000-0000-0000-0000-00000000000a",
                    "claim_id": claim["id"],
                    "evidence_type": "cited_public_web",
                    "source_name": "Source A",
                    "source_url": "https://example.test/a",
                    "details": {},
                    "fingerprint": "a" * 64,
                    "observed_at": tied_observed_at,
                },
            ],
        )

    snapshot_loaded = threading.Event()
    release_snapshot = threading.Event()
    original_loader = store._get_persona_with_connection

    def paused_loader(connection, selected_persona_id):
        persona = original_loader(connection, selected_persona_id)
        snapshot_loaded.set()
        assert release_snapshot.wait(timeout=5)
        return persona

    monkeypatch.setattr(store, "_get_persona_with_connection", paused_loader)
    with ThreadPoolExecutor(max_workers=2) as executor:
        export_future = executor.submit(store.get_persona_export_snapshot, persona_id)
        assert snapshot_loaded.wait(timeout=5)
        review_future = executor.submit(
            store.review_claim,
            claim["id"],
            "rejected",
            "second-analyst",
        )
        time.sleep(0.1)
        assert not review_future.done()
        release_snapshot.set()
        snapshot, generated_at = export_future.result(timeout=5)
        review_future.result(timeout=5)

    exported_claim = next(
        item for item in snapshot["claims"] if item["id"] == claim["id"]
    )
    assert exported_claim["review_status"] == "approved"
    assert [
        evidence["id"]
        for evidence in exported_claim["evidence"]
        if evidence["id"]
        in {
            "00000000-0000-0000-0000-00000000000a",
            "00000000-0000-0000-0000-00000000000b",
        }
    ] == [
        "00000000-0000-0000-0000-00000000000a",
        "00000000-0000-0000-0000-00000000000b",
    ]
    assert generated_at.tzinfo is not None
    current_claim = next(
        item
        for item in store.get_persona(persona_id)["claims"]
        if item["id"] == claim["id"]
    )
    assert current_claim["review_status"] == "rejected"


def test_same_subject_email_hint_is_a_pending_claim_not_an_approved_fact(store):
    job_id = store.create_investigation(
        ["alice"],
        {
            "investigation_spec": {
                "processing_mode": "same_subject",
                "identifiers": [
                    {"type": "username", "value": "alice"},
                    {"type": "email", "value": "alice@example.test"},
                ],
            }
        },
    )
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice"],
        "individual_reports": [],
    }
    store.finish(job_id, result)

    assert store.sync_persona_claims(job_id, result) == 1
    case = store.get_case(store.get_job(job_id)["case_id"])
    persona = store.get_persona(case["personas"][0]["id"])
    assert len(persona["claims"]) == 1
    email = persona["claims"][0]
    assert email["field_name"] == "email"
    assert email["display_value"] == "alice@example.test"
    assert email["review_status"] == "pending"
    assert email["source_engine"] == "investigation_input"
    assert email["evidence"][0]["evidence_type"] == "operator_provided_identifier"

    assert store.sync_persona_claims(job_id, result) == 1
    persona = store.get_persona(persona["id"])
    assert len(persona["claims"]) == 1
    assert len(persona["claims"][0]["evidence"]) == 1


def test_cited_ai_proposals_are_pending_idempotent_and_preserve_rejection(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice"],
        "individual_reports": [],
    }
    store.finish(job_id, result)
    sources = [{"title": "Official biography", "url": "https://example.test/alice"}]
    proposals = [
        {
            "username": "alice",
            "field_name": "occupation",
            "value": "Research engineer",
            "confidence": 78,
            "source_url": "https://example.test/alice",
            "source_title": "Ignored model title",
            "reason": "The official biography states this occupation.",
        }
    ]

    first = store.sync_ai_persona_claims(
        job_id,
        proposals,
        sources=sources,
        usernames=["alice"],
        model="gpt-5.6-terra",
    )
    assert first["count"] == 1
    case = store.get_case(store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    claim = store.get_persona(persona_id)["claims"][0]
    assert claim["source_engine"] == "openai_web_research"
    assert claim["review_status"] == "pending"
    assert claim["evidence"][0]["evidence_type"] == "cited_public_web"
    assert claim["evidence"][0]["details"]["proposal_reason"].startswith(
        "The official biography"
    )

    store.review_claim(claim["id"], "rejected", "analyst", "Wrong Alice")
    second = store.sync_ai_persona_claims(
        job_id,
        proposals,
        sources=sources,
        usernames=["alice"],
        model="gpt-5.6-terra",
    )
    assert second["count"] == 1
    refreshed = store.get_persona(persona_id)["claims"][0]
    assert refreshed["review_status"] == "rejected"
    assert len(refreshed["evidence"]) == 1
    assert refreshed["reviews"][0]["note"] == "Wrong Alice"


def test_ai_proposals_reject_sensitive_fields_and_invented_citations(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": ["alice"],
            "individual_reports": [],
        },
    )
    result = store.sync_ai_persona_claims(
        job_id,
        [
            {
                "username": "alice",
                "field_name": "financial_profile",
                "value": "High net worth",
                "confidence": 80,
                "source_url": "https://example.test/alice",
                "source_title": "Source",
                "reason": "Disallowed sensitive field.",
            },
            {
                "username": "alice",
                "field_name": "company",
                "value": "Example Ltd",
                "confidence": 80,
                "source_url": "https://invented.test/alice",
                "source_title": "Invented",
                "reason": "URL is absent from citations.",
            },
        ],
        sources=[{"title": "Source", "url": "https://example.test/alice"}],
        usernames=["alice"],
        model="gpt-5.6-terra",
    )
    assert result["count"] == 0
    persona_id = store.get_case(store.get_job(job_id)["case_id"])["personas"][0]["id"]
    assert store.get_persona(persona_id)["claims"] == []


def test_ai_corroboration_merges_existing_claim_without_altering_reviewed_score(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    source_url = "https://social.example/alice"
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice"],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Example Social",
                        "url": source_url,
                        "confidence": "moderate",
                        "evidence": {"fullname": "Alice Example"},
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    persona_id = store.get_case(store.get_job(job_id)["case_id"])["personas"][0]["id"]
    claims = store.get_persona(persona_id)["claims"]
    name_claim = next(claim for claim in claims if claim["field_name"] == "full_name")
    assert name_claim["confidence"] == 75
    store.review_claim(name_claim["id"], "approved", "analyst")

    proposals = [
        {
            "username": "alice",
            "field_name": "full_name",
            "value": "Alice Example",
            "confidence": 85,
            "source_url": source_url,
            "source_title": "Official profile",
            "reason": "The official profile confirms the full name.",
        },
        {
            "username": "alice",
            "field_name": "social_account",
            "value": source_url,
            "confidence": 85,
            "source_url": source_url,
            "source_title": "Official profile",
            "reason": "The URL is the subject's official account.",
        },
    ]
    store.sync_ai_persona_claims(
        job_id,
        proposals,
        sources=[{"title": "Official profile", "url": source_url}],
        usernames=["alice"],
        model="gpt-5.6-terra",
    )

    refreshed = store.get_persona(persona_id)["claims"]
    assert len(refreshed) == 2
    name_claim = next(
        claim for claim in refreshed if claim["field_name"] == "full_name"
    )
    account_claim = next(
        claim for claim in refreshed if claim["field_name"] == "social_account"
    )
    assert name_claim["review_status"] == "approved"
    assert name_claim["confidence"] == 75
    assert len(name_claim["evidence"]) == 2
    assert len(account_claim["evidence"]) == 2


def test_user_scanner_observations_share_the_canonical_claim_and_evidence_store(store):
    job_id = store.create_investigation(
        ["alice"],
        {
            "investigation_spec": {
                "processing_mode": "same_subject",
                "subject_label": "Alice",
                "enable_user_scanner_email": True,
            }
        },
    )
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice"],
        "graph_file": f"search_{job_id}/graph.html",
        "found_count": 0,
        "individual_reports": [],
        "collector_observations": [
            {
                "source_engine": "user_scanner_email",
                "subject_type": "email",
                "subject_value": "alice@example.test",
                "status": "Registered",
                "site_name": "Gravatar",
                "category": "social",
                "source_url": "https://gravatar.com",
                "extra": {"username": "alice"},
                "media": {},
            }
        ],
    }
    store.finish(job_id, result)

    assert store.sync_persona_claims(job_id, result) == 1
    case = store.get_case(store.get_job(job_id)["case_id"])
    persona = store.get_persona(case["personas"][0]["id"])
    claim = persona["claims"][0]

    assert claim["field_name"] == "account_registration"
    assert claim["review_status"] == "pending"
    assert claim["source_engine"] == "user_scanner_email"
    assert claim["evidence"][0]["evidence_type"] == "email_registration_probe"
    lineage = store.get_claim_lineage(claim["id"])
    assert len(lineage) == 1
    assert lineage[0]["provenance_type"] == "investigation_job"
    assert lineage[0]["job_id"] == job_id
    assert lineage[0]["source_engine"] == "user_scanner_email"
    assert lineage[0]["native_status"] == "registered"
    assert lineage[0]["source_record_id"].startswith("user_scanner_email:")


def test_github_enrichment_binds_only_to_its_persona_and_stays_pending(store):
    job_id = store.create_investigation(
        ["alice", "bob"],
        {
            "investigation_spec": {
                "processing_mode": "independent",
                "enable_github_profile_enrichment": True,
            }
        },
    )
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice", "bob"],
        "graph_file": f"search_{job_id}/graph.html",
        "found_count": 1,
        "individual_reports": [],
        "collector_observations": [
            {
                "source_engine": "github_public_profile",
                "subject_type": "username",
                "subject_value": "alice",
                "status": "observed",
                "site_name": "GitHub",
                "category": "developer",
                "source_url": "https://github.com/alice",
                "source_record_id": "github-user:12345",
                "extra": {
                    "api_version": "2026-03-10",
                    "github_id": 12345,
                    "login": "alice",
                    "account_type": "User",
                    "name": "Alice Example",
                    "bio": "Public-interest technologist",
                    "created_at": "2012-01-02T03:04:05Z",
                    "followers": 12,
                },
                "media": {},
            }
        ],
    }
    store.finish(job_id, result)

    assert store.sync_persona_claims(job_id, result) == 4
    case = store.get_case(store.get_job(job_id)["case_id"])
    personas = {
        persona["display_name"]: store.get_persona(persona["id"])
        for persona in case["personas"]
    }
    assert personas["bob"]["claims"] == []
    assert {claim["field_name"] for claim in personas["alice"]["claims"]} == {
        "social_account",
        "platform_identifier",
        "full_name",
        "summary",
    }
    assert all(
        claim["review_status"] == "pending" for claim in personas["alice"]["claims"]
    )
    assert "followers" not in {
        claim["field_name"] for claim in personas["alice"]["claims"]
    }
    identifier = next(
        claim
        for claim in personas["alice"]["claims"]
        if claim["field_name"] == "platform_identifier"
    )
    lineage = store.get_claim_lineage(identifier["id"])
    assert lineage[0]["source_engine"] == "github_public_profile"
    assert lineage[0]["source_record_id"] == "github-user:12345"


def test_url_analysis_and_wayback_attach_evidence_without_new_identity_facts(store):
    job_id = store.create_investigation(
        ["alice"],
        {
            "investigation_spec": {
                "processing_mode": "independent",
                "enable_archived_url_evidence": True,
            }
        },
    )
    store.claim_next("worker:test")
    profile_url = "https://social.example/alice"
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
                        "site_name": "Example Social",
                        "url": profile_url,
                        "confidence": "moderate",
                        "evidence": {},
                    }
                ],
            }
        ],
        "collector_observations": [
            {
                "source_engine": "unfurl_url_analysis",
                "subject_type": "username",
                "subject_value": "alice",
                "status": "analyzed",
                "site_name": "Example Social",
                "category": "url_analysis",
                "source_url": profile_url,
                "source_record_id": "unfurl:record",
                "extra": {
                    "unfurl_version": "20260405",
                    "remote_lookups": False,
                    "nodes": [
                        {
                            "id": 1,
                            "data_type": "url.hostname",
                            "key": None,
                            "value": "social.example",
                            "parent_id": None,
                        }
                    ],
                },
            },
            {
                "source_engine": "wayback_cdx",
                "subject_type": "username",
                "subject_value": "alice",
                "status": "archived",
                "site_name": "Example Social",
                "category": "archive",
                "source_url": (
                    "https://web.archive.org/web/20260304050607id_/"
                    "https://social.example/alice"
                ),
                "source_record_id": "wayback:20260304050607:DIGEST",
                "extra": {
                    "queried_profile_url": profile_url,
                    "sampled_capture_count": 1,
                    "oldest_sampled_capture_at": "2026-03-04T05:06:07Z",
                    "latest_sampled_capture_at": "2026-03-04T05:06:07Z",
                    "captures": [
                        {
                            "original_url": profile_url,
                            "replay_url": (
                                "https://web.archive.org/web/20260304050607id_/"
                                "https://social.example/alice"
                            ),
                        }
                    ],
                },
            },
        ],
    }
    store.finish(job_id, result)

    assert store.sync_persona_claims(job_id, result) == 3
    case = store.get_case(store.get_job(job_id)["case_id"])
    persona = store.get_persona(case["personas"][0]["id"])

    assert len(persona["claims"]) == 1
    account = persona["claims"][0]
    assert account["field_name"] == "social_account"
    assert account["review_status"] == "pending"
    assert account["confidence"] == 65
    evidence_types = {item["evidence_type"] for item in account["evidence"]}
    assert evidence_types == {
        "observed_profile",
        "deterministic_url_analysis",
        "wayback_capture_index",
    }
    assert {
        item["source_engine"] for item in store.get_claim_lineage(account["id"])
    } == {
        "openledger_profile_discovery",
        "unfurl_url_analysis",
        "wayback_cdx",
    }


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


def test_configured_refresh_routes_variants_to_selected_persona_in_multi_person_case(
    store,
):
    source_job_id = store.create_investigation(["alice", "bob"], {})
    source_job = store.claim_next("worker:multi-person-source")
    store.finish(
        source_job_id,
        {
            "status": "completed",
            "usernames": ["alice", "bob"],
            "individual_reports": [],
            "found_count": 0,
        },
    )
    case = store.get_case(source_job["case_id"])
    persona_ids = {
        persona["display_name"]: persona["id"] for persona in case["personas"]
    }
    alice_id = persona_ids["alice"]
    refresh_id = store.repeat_persona_investigation(
        alice_id,
        ["alice.one"],
        {
            "all_sites": True,
            "investigation_spec": {
                "processing_mode": "same_subject",
                "subject_label": "Alice Example",
                "target_persona_id": alice_id,
                "identifiers": [
                    {"type": "full_name", "value": "Alice Example"}
                ],
            },
        },
    )
    queued = store.get_job(refresh_id)
    assert queued["options"]["investigation_spec"]["target_persona_id"] == alice_id
    assert queued["options"]["investigation_spec"]["subject_label"] == "alice"
    store.claim_next("worker:targeted-refresh")
    result = {
        "status": "completed",
        "usernames": ["alice.one"],
        "individual_reports": [
            {
                "username": "alice.one",
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": "https://example.test/alice.one",
                        "confidence": "strong",
                        "evidence": {"email": "alice@example.test"},
                    }
                ],
            }
        ],
    }
    store.finish(refresh_id, result)
    assert store.sync_persona_claims(refresh_id, result) >= 1
    ai_sync = store.sync_ai_persona_claims(
        refresh_id,
        [
            {
                "username": "alice.one",
                "field_name": "company",
                "value": "Example Organization",
                "confidence": 80,
                "source_url": "https://example.test/alice.one",
                "source_title": "Example profile",
                "reason": "The cited profile explicitly names the organization.",
                "latitude": None,
                "longitude": None,
                "coordinate_precision": None,
            }
        ],
        sources=[
            {
                "title": "Example profile",
                "url": "https://example.test/alice.one",
            }
        ],
        usernames=["alice.one"],
        model="gpt-5.6-terra",
    )
    assert ai_sync["count"] == 1

    alice = store.get_persona(alice_id)
    bob = store.get_persona(persona_ids["bob"])
    assert any(
        claim["field_name"] == "email"
        and claim["display_value"] == "alice@example.test"
        for claim in alice["claims"]
    )
    assert any(
        claim["field_name"] == "company"
        and claim["display_value"] == "Example Organization"
        for claim in alice["claims"]
    )
    assert all(
        claim["field_name"] not in {"email", "company"}
        for claim in bob["claims"]
    )


def test_socid_metadata_refresh_preserves_account_claim_and_observation_history(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:first")

    def result_for(job, follower_count):
        return {
            "status": "completed",
            "session_folder": f"search_{job}",
            "usernames": ["alice"],
            "found_count": 1,
            "individual_reports": [
                {
                    "username": "alice",
                    "claimed_profiles": [
                        {
                            "site_name": "Example Social",
                            "url": "https://social.example.test/alice",
                            "confidence": "strong",
                            "evidence": {
                                "uid": "stable-123",
                                "follower_count": str(follower_count),
                                "is_verified": "True",
                                "links": ["https://linked.example.test/alice"],
                            },
                        }
                    ],
                }
            ],
        }

    first_result = result_for(job_id, 10)
    store.finish(job_id, first_result)
    assert store.sync_persona_claims(job_id, first_result) == 3
    persona_id = store.get_case(store.get_job(job_id)["case_id"])["personas"][0][
        "id"
    ]
    first_persona = store.get_persona(persona_id)
    account = next(
        claim
        for claim in first_persona["claims"]
        if claim["field_name"] == "social_account"
    )
    assert store.review_claim(account["id"], "approved", "analyst") == persona_id

    refresh_job_id = store.repeat_persona_investigation(persona_id)
    store.claim_next("worker:refresh")
    refresh_result = result_for(refresh_job_id, 11)
    store.finish(refresh_job_id, refresh_result)
    assert store.sync_persona_claims(refresh_job_id, refresh_result) == 3

    refreshed = store.get_persona(persona_id)
    assert [claim["field_name"] for claim in refreshed["claims"]].count(
        "social_account"
    ) == 1
    assert [claim["field_name"] for claim in refreshed["claims"]].count(
        "platform_identifier"
    ) == 1
    assert [claim["field_name"] for claim in refreshed["claims"]].count(
        "linked_profile_lead"
    ) == 1
    account = next(
        claim
        for claim in refreshed["claims"]
        if claim["field_name"] == "social_account"
    )
    assert account["review_status"] == "approved"
    assert len(account["evidence"]) == 2
    assert {
        evidence["details"]["account_metadata"]["follower_count"]
        for evidence in account["evidence"]
    } == {10, 11}
    lineage = store.get_claim_lineage(account["id"])
    assert len(lineage) == 2
    assert [
        item["details"]["observation"]["account_metadata"]["follower_count"]
        for item in lineage
    ] == [10, 11]


def test_stable_ids_and_linked_profile_leads_never_create_shared_relationships(
    store,
):
    job_id = store.create_investigation(["alice", "bob"], {})
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "usernames": ["alice", "bob"],
        "individual_reports": [
            {
                "username": username,
                "claimed_profiles": [
                    {
                        "site_name": "Example Social",
                        "url": f"https://social.example.test/{username}",
                        "confidence": "strong",
                        "evidence": {
                            "uid": "collision-123",
                            "links": ["https://linked.example.test/shared"],
                        },
                    }
                ],
            }
            for username in ("alice", "bob")
        ],
    }
    store.finish(job_id, result)
    assert store.sync_persona_claims(job_id, result) == 6
    case = store.get_case(store.get_job(job_id)["case_id"])
    for persona_summary in case["personas"]:
        persona = store.get_persona(persona_summary["id"])
        for claim in persona["claims"]:
            if claim["field_name"] in {
                "platform_identifier",
                "linked_profile_lead",
            }:
                store.review_claim(claim["id"], "approved", "analyst")

    graph = store.build_relationship_graph(case["id"])
    assert graph["nodes"] == []
    assert graph["edges"] == []
    assert graph["stats"]["field_counts"] == {}


def test_approved_coordinates_are_validated_and_serialized(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "usernames": ["alice"],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": "https://example.test/alice",
                        "confidence": "strong",
                        "evidence": {"location": "Jakarta, Indonesia"},
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    persona_id = store.get_case(store.get_job(job_id)["case_id"])["personas"][0]["id"]
    location = next(
        claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "current_location"
    )

    with pytest.raises(ValueError, match="provided together"):
        store.review_claim(location["id"], "approved", "analyst", latitude="-6.2")
    with pytest.raises(ValueError, match="between -90 and 90"):
        store.review_claim(
            location["id"],
            "approved",
            "analyst",
            latitude="91",
            longitude="106.8",
        )

    store.review_claim(
        location["id"],
        "approved",
        "analyst",
        latitude="-6.1754",
        longitude="106.8272",
    )
    reviewed = next(
        claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["id"] == location["id"]
    )
    assert reviewed["review_status"] == "approved"
    assert reviewed["latitude"] == pytest.approx(-6.1754)
    assert reviewed["longitude"] == pytest.approx(106.8272)


def test_ai_location_coordinates_remain_pending_until_human_approval(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": ["alice"],
            "individual_reports": [],
        },
    )
    synced = store.sync_ai_persona_claims(
        job_id,
        [
            {
                "username": "alice",
                "field_name": "current_location",
                "value": "Jakarta, Indonesia",
                "confidence": 75,
                "source_url": "https://example.test/alice",
                "source_title": "Biography",
                "reason": "The biography explicitly names Jakarta.",
                "latitude": -6.1754,
                "longitude": 106.8272,
                "coordinate_precision": "city",
            }
        ],
        sources=[{"title": "Biography", "url": "https://example.test/alice"}],
        usernames=["alice"],
        model="gpt-5.6-terra",
    )
    persona_id = store.get_case(store.get_job(job_id)["case_id"])["personas"][0][
        "id"
    ]
    claim = store.get_persona(persona_id)["claims"][0]
    assert synced["diagnostics"]["accepted"] == 1
    assert claim["review_status"] == "pending"
    assert claim["latitude"] == pytest.approx(-6.1754)

    store.review_claim(claim["id"], "approved", "analyst")
    approved = store.get_persona(persona_id)["claims"][0]
    assert approved["review_status"] == "approved"
    assert approved["latitude"] == pytest.approx(-6.1754)


def test_ai_coordinates_do_not_silently_map_an_already_approved_location(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice"],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Profile",
                        "url": "https://example.test/alice",
                        "confidence": "strong",
                        "evidence": {"location": "Jakarta, Indonesia"},
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    persona_id = store.get_case(store.get_job(job_id)["case_id"])["personas"][0][
        "id"
    ]
    location = store.get_persona(persona_id)["claims"][0]
    store.review_claim(location["id"], "approved", "analyst")

    store.sync_ai_persona_claims(
        job_id,
        [
            {
                "username": "alice",
                "field_name": "current_location",
                "value": "Jakarta, Indonesia",
                "confidence": 75,
                "source_url": "https://example.test/alice",
                "source_title": "Profile",
                "reason": "The public profile names Jakarta.",
                "latitude": -6.1754,
                "longitude": 106.8272,
                "coordinate_precision": "city",
            }
        ],
        sources=[{"title": "Profile", "url": "https://example.test/alice"}],
        usernames=["alice"],
        model="gpt-5.6-terra",
    )

    refreshed = store.get_persona(persona_id)["claims"][0]
    assert refreshed["review_status"] == "approved"
    assert refreshed["latitude"] is None
    ai_evidence = next(
        item
        for item in refreshed["evidence"]
        if item["evidence_type"] == "cited_public_web"
    )
    assert ai_evidence["details"]["proposed_latitude"] == pytest.approx(-6.1754)


def test_relationship_graph_uses_only_exact_shared_approved_claims(store):
    job_id = store.create_investigation(["alice", "bob"], {})
    store.claim_next("worker:test")
    reports = []
    for username in ("alice", "bob"):
        reports.append(
            {
                "username": username,
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": f"https://example.test/{username}",
                        "confidence": "strong",
                        "evidence": {
                            "location": "Jakarta, Indonesia",
                            "email": f"{username}@example.test",
                        },
                    }
                ],
            }
        )
    result = {
        "status": "completed",
        "usernames": ["alice", "bob"],
        "individual_reports": reports,
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    case = store.get_case(store.get_job(job_id)["case_id"])
    location_claims = []
    for persona_summary in case["personas"]:
        persona = store.get_persona(persona_summary["id"])
        location = next(
            claim
            for claim in persona["claims"]
            if claim["field_name"] == "current_location"
        )
        location_claims.append(location)
        store.review_claim(location["id"], "approved", "analyst")

    graph = store.build_relationship_graph()
    assert graph["stats"] == {
        "persona_count": 2,
        "shared_attribute_count": 1,
        "connection_count": 2,
        "field_counts": {"current_location": 1},
    }
    attribute = next(node for node in graph["nodes"] if node["kind"] == "attribute")
    assert attribute["label"] == "Jakarta, Indonesia"
    assert all(edge["field_name"] == "current_location" for edge in graph["edges"])

    store.review_claim(location_claims[1]["id"], "rejected", "analyst")
    suppressed = store.build_relationship_graph()
    assert suppressed["nodes"] == []
    assert suppressed["edges"] == []


def _create_case_with_company_claim(store, username, company, *, decision="approved"):
    job_id = store.create_investigation([username], {})
    store.claim_next(f"worker:{username}")
    result = {
        "status": "completed",
        "usernames": [username],
        "individual_reports": [
            {
                "username": username,
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": f"https://example.test/{username}",
                        "confidence": "strong",
                        "evidence": {"company": company},
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    job = store.get_job(job_id)
    case = store.get_case(job["case_id"])
    persona = store.get_persona(case["personas"][0]["id"])
    company_claim = next(
        claim for claim in persona["claims"] if claim["field_name"] == "company"
    )
    store.review_claim(company_claim["id"], decision, "analyst")
    return case["id"], company_claim


def test_combined_investigation_snapshots_only_approved_cross_case_links(store):
    alice_case_id, alice_claim = _create_case_with_company_claim(
        store, "alice", "Nexorus"
    )
    bob_case_id, bob_claim = _create_case_with_company_claim(store, "bob", "Nexorus")
    pending_case_id, pending_claim = _create_case_with_company_claim(
        store, "charlie", "Nexorus", decision="pending"
    )

    fusion_job_id = store.create_combined_investigation(
        [alice_case_id, bob_case_id, pending_case_id],
        title="Nexorus network",
        purpose="Identify exact reviewed links across three source cases.",
        created_by="analyst",
    )
    fusion_job = store.claim_next("worker:fusion")
    assert fusion_job["job_id"] == fusion_job_id

    first = store.build_case_fusion_snapshot(fusion_job_id)
    second = store.build_case_fusion_snapshot(fusion_job_id)

    assert first["snapshot"]["sha256"] == second["snapshot"]["sha256"]
    assert first["source_case_count"] == 3
    assert first["approved_claim_count"] == 2
    assert first["shared_attribute_count"] == 1
    assert first["connection_count"] == 2
    assert {item["id"] for item in first["snapshot"]["approved_claims"]} == {
        alice_claim["id"],
        bob_claim["id"],
    }
    assert pending_claim["id"] not in {
        item["id"] for item in first["snapshot"]["approved_claims"]
    }
    graph = first["relationship_graph"]
    assert {
        node["case_id"] for node in graph["nodes"] if node["kind"] == "persona"
    } == {
        alice_case_id,
        bob_case_id,
    }
    assert all(
        edge["sources"][0]["url"].startswith("https://example.test/")
        for edge in graph["edges"]
    )


def test_combined_ai_claim_cap_is_balanced_before_truncation():
    records = [
        {"case_id": "case-a", "id": f"a-{index}"} for index in range(600)
    ] + [{"case_id": "case-b", "id": f"b-{index}"} for index in range(10)]

    bounded = _bounded_round_robin_case_records(
        records, ["case-a", "case-b"], 500
    )

    assert len(bounded) == 500
    assert [item["case_id"] for item in bounded[:20]] == [
        value for _ in range(10) for value in ("case-a", "case-b")
    ]
    assert sum(item["case_id"] == "case-b" for item in bounded) == 10


def test_combined_ai_proposals_are_snapshot_bound_and_reviewed_separately(store):
    alice_case_id, _ = _create_case_with_company_claim(store, "alice", "Nexorus")
    bob_case_id, _ = _create_case_with_company_claim(store, "bob", "Nexorus")
    fusion_job_id = store.create_combined_investigation(
        [alice_case_id, bob_case_id],
        title="AI relationship review",
        purpose="Test governed cross-case hypotheses.",
        created_by="analyst",
    )
    fusion_job = store.claim_next("worker:fusion-ai")
    snapshot = store.build_case_fusion_snapshot(fusion_job_id)
    context = snapshot["analysis_context"]
    entities = {
        entity["case_id"]: entity
        for entity in context["entities"]
        if entity["entity_type"] == "persona"
    }
    evidence = [
        {
            "reference_id": claim["reference_id"],
            "reference_type": "approved_claim",
            "claim_id": claim["claim_id"],
            "case_id": claim["case_id"],
            "case_title": claim["case_title"],
            "persona_id": claim["persona_id"],
            "persona_name": claim["persona_name"],
            "field_name": claim["field_name"],
            "display_value": claim["display_value"],
            "confidence": claim["confidence"],
            "sources": claim["sources"],
        }
        for claim in context["approved_claims"]
    ]
    run_id = store.start_combined_analysis_run(
        fusion_job_id,
        snapshot["snapshot"]["sha256"],
        model="gpt-5.6-terra",
        web_search_enabled=True,
    )
    assert store.complete_combined_analysis_run(
        run_id,
        {
            "executive_summary": "The approved affiliations match exactly.",
            "key_findings": [
                {
                    "summary": "Both approved records name Nexorus.",
                    "evidence": evidence,
                }
            ],
            "contradictions": [],
            "information_gaps": ["The relationship type is not independently known."],
            "next_steps": ["Review official organization records."],
            "sources": [],
            "proposals": [
                {
                    "title": "Shared Nexorus affiliation",
                    "relationship_type": "affiliation",
                    "subject_ref": entities[alice_case_id]["reference_id"],
                    "subject_entity": entities[alice_case_id],
                    "object_ref": entities[bob_case_id]["reference_id"],
                    "object_entity": entities[bob_case_id],
                    "explanation": "Both approved claims name the same organization.",
                    "confidence": 75,
                    "evidence": evidence,
                    "contradictory_evidence": [],
                    "limitations": ["A shared affiliation does not prove coordination."],
                }
            ],
        },
    ) == 1
    snapshot.pop("analysis_context")
    store.finish(
        fusion_job_id,
        {"status": "completed", "kind": "case_fusion", **snapshot},
    )

    combined = store.get_case(fusion_job["case_id"])
    analysis = combined["analysis_runs"][0]
    assert analysis["snapshot_sha256"] == snapshot["snapshot"]["sha256"]
    proposal = analysis["proposals"][0]
    assert proposal["review_status"] == "pending"
    source_versions = {
        case_id: store.get_case(case_id)["updated_at"]
        for case_id in (alice_case_id, bob_case_id)
    }

    store.review_combined_relationship_proposal(
        combined["id"],
        proposal["id"],
        "approved",
        "reviewer.one",
        "Verified against the cited approved records.",
    )

    reviewed = store.get_case(combined["id"])["analysis_runs"][0]["proposals"][0]
    assert reviewed["review_status"] == "approved"
    assert reviewed["reviewed_by"] == "reviewer.one"
    assert reviewed["reviews"][0]["note"].startswith("Verified")
    assert {
        case_id: store.get_case(case_id)["updated_at"]
        for case_id in (alice_case_id, bob_case_id)
    } == source_versions


def test_combined_chat_relationship_proposals_retain_message_lineage(store):
    alice_case_id, _ = _create_case_with_company_claim(store, "alice", "Nexorus")
    bob_case_id, _ = _create_case_with_company_claim(store, "bob", "Nexorus")
    job_id = store.create_combined_investigation(
        [alice_case_id, bob_case_id],
        title="Chat relationship lineage",
        purpose="Question and extend a governed assessment.",
        created_by="analyst",
    )
    job = store.claim_next("worker:combined-chat-lineage")
    snapshot = store.build_case_fusion_snapshot(job_id)
    context = snapshot.pop("analysis_context")
    personas = {
        item["case_id"]: item
        for item in context["entities"]
        if item["entity_type"] == "persona"
    }
    evidence = [
        {
            "reference_id": item["reference_id"],
            "reference_type": "approved_claim",
            "claim_id": item["claim_id"],
            "case_id": item["case_id"],
            "case_title": item["case_title"],
            "persona_id": item["persona_id"],
            "persona_name": item["persona_name"],
            "field_name": item["field_name"],
            "display_value": item["display_value"],
            "confidence": item["confidence"],
            "sources": item["sources"],
        }
        for item in context["approved_claims"]
    ]
    run_id = store.start_combined_analysis_run(
        job_id,
        snapshot["snapshot"]["sha256"],
        model="gpt-5.6-sol",
        web_search_enabled=True,
    )
    store.complete_combined_analysis_run(
        run_id,
        {
            "executive_summary": "Initial assessment.",
            "key_findings": [],
            "contradictions": [],
            "information_gaps": [],
            "next_steps": [],
            "sources": [],
            "proposals": [],
        },
    )
    store.finish(job_id, {"status": "completed", "kind": "case_fusion", **snapshot})
    message = store.append_case_chat_message(
        job["case_id"],
        role="assistant",
        author="OpenLedger AI",
        content="The cited evidence supports a reviewable affiliation hypothesis.",
        research_enabled=True,
        sources=[{"title": "Public source", "url": "https://example.test/source"}],
        proposals={"status": "processing", "kind": "relationship", "count": 0},
        model="gpt-5.6-sol",
    )
    proposal = {
        "title": "Chat-derived affiliation",
        "relationship_type": "affiliation",
        "subject_ref": personas[alice_case_id]["reference_id"],
        "subject_entity": personas[alice_case_id],
        "object_ref": personas[bob_case_id]["reference_id"],
        "object_entity": personas[bob_case_id],
        "explanation": "The approved affiliations name the same organization.",
        "confidence": 70,
        "evidence": evidence,
        "contradictory_evidence": [],
        "limitations": ["Shared affiliation does not prove direct contact."],
    }

    inserted = store.append_combined_relationship_proposals(
        job["case_id"], run_id, message["id"], [proposal]
    )
    assert len(inserted) == 1
    assert (
        store.append_combined_relationship_proposals(
            job["case_id"], run_id, message["id"], [proposal]
        )
        == []
    )
    retained = store.get_case(job["case_id"])["analysis_runs"][0]["proposals"]
    assert retained[0]["id"] == inserted[0]
    assert retained[0]["chat_message_id"] == message["id"]
    assert retained[0]["review_status"] == "pending"


def test_combined_case_references_protect_sources_and_delete_independently(store):
    first_case_id, _ = _create_case_with_company_claim(store, "alice", "Nexorus")
    second_case_id, _ = _create_case_with_company_claim(store, "bob", "Nexorus")
    fusion_job_id = store.create_combined_investigation(
        [first_case_id, second_case_id],
        title="Protected sources",
        purpose="Preserve provenance while correlating cases.",
        created_by="analyst",
    )
    fusion_case_id = store.get_job(fusion_job_id)["case_id"]

    with pytest.raises(ReferencedCaseError) as captured:
        store.delete_case(first_case_id)
    assert captured.value.references == [
        {"id": fusion_case_id, "title": "Protected sources"}
    ]
    first_source_case = store.get_case(first_case_id)
    first_source_job_id = first_source_case["jobs"][0]["job_id"]
    with pytest.raises(ReferencedCaseError) as job_deletion:
        store.delete_job(
            first_source_job_id, confirmation_name=first_source_case["title"]
        )
    assert job_deletion.value.references == captured.value.references

    assert store.request_cancel(fusion_job_id) is True
    assert store.delete_case(fusion_case_id) is True
    assert store.get_case(first_case_id) is not None
    assert store.get_case(second_case_id) is not None


def test_combined_snapshot_links_approved_affiliation_to_confirmed_org_case(store):
    person_case_id, company_claim = _create_case_with_company_claim(
        store, "alice", "Example Organization"
    )
    affiliation_job_id = store.create_affiliation_investigation(
        "Example Organization", official_website="https://example.org"
    )
    affiliation_job = store.claim_next("worker:affiliation")
    candidate = {
        "candidate_key": "official_website_public_content:example.org",
        "label": "Example Organization",
        "source_engine": "official_website_public_content",
        "source_name": "Supplied organization website",
        "source_record_id": "official-website:example.org",
        "source_url": "https://example.org/",
        "identity_scope": "first_party_operating_identity",
        "identity_scope_label": "First-party operating identity",
        "match_status": "operator_supplied_domain",
        "selectable": True,
        "basis": "The supplied website names the organization.",
        "limitation": "This does not establish legal registration.",
    }
    store.finish(
        affiliation_job_id,
        {"status": "completed", "organization_resolution_candidates": [candidate]},
    )
    store.select_affiliation_organization(
        affiliation_job["case_id"], candidate["candidate_key"], "analyst"
    )
    fusion_job_id = store.create_combined_investigation(
        [person_case_id, affiliation_job["case_id"]],
        title="Person and organization",
        purpose="Test an exact approved affiliation link.",
        created_by="analyst",
    )
    store.claim_next("worker:fusion")

    snapshot = store.build_case_fusion_snapshot(fusion_job_id)

    graph = snapshot["relationship_graph"]
    assert graph["stats"]["organization_count"] == 1
    assert graph["stats"]["connection_count"] == 1
    organization = next(
        node for node in graph["nodes"] if node["kind"] == "organization"
    )
    assert organization["source_url"] == "https://example.org/"
    assert graph["edges"][0]["claim_id"] == company_claim["id"]
    assert graph["edges"][0]["sources"][-1]["url"] == "https://example.org/"


def test_combined_investigation_rejects_nested_and_duplicate_only_scopes(store):
    first_case_id, _ = _create_case_with_company_claim(store, "alice", "One")
    second_case_id, _ = _create_case_with_company_claim(store, "bob", "Two")
    fusion_job_id = store.create_combined_investigation(
        [first_case_id, second_case_id],
        title="First combined case",
        purpose="Test combined-case scope rules.",
        created_by="analyst",
    )
    fusion_case_id = store.get_job(fusion_job_id)["case_id"]

    with pytest.raises(ValueError, match="at least two"):
        store.create_combined_investigation(
            [first_case_id, first_case_id],
            title="Duplicate scope",
            purpose="This must not create a one-case fusion.",
            created_by="analyst",
        )
    with pytest.raises(ValueError, match="cannot be nested"):
        store.create_combined_investigation(
            [fusion_case_id, first_case_id],
            title="Nested scope",
            purpose="Nested scopes are deliberately excluded.",
            created_by="analyst",
        )


def test_combined_case_marks_changed_sources_and_refreshes_explicitly(store):
    first_case_id, first_claim = _create_case_with_company_claim(
        store, "alice", "One"
    )
    second_case_id, _ = _create_case_with_company_claim(store, "bob", "Two")
    fusion_job_id = store.create_combined_investigation(
        [first_case_id, second_case_id],
        title="Versioned scope",
        purpose="Keep completed snapshots stable until an explicit refresh.",
        created_by="analyst",
    )
    fusion_job = store.claim_next("worker:fusion")
    snapshot = store.build_case_fusion_snapshot(fusion_job_id)
    store.finish(
        fusion_job_id,
        {"status": "completed", "kind": "case_fusion", **snapshot},
    )

    combined = store.get_case(fusion_job["case_id"])
    assert combined["source_changed_count"] == 0
    original_snapshot_sha = combined["jobs"][0]["snapshot"]["sha256"]
    store.review_claim(first_claim["id"], "rejected", "analyst")
    changed = store.get_case(fusion_job["case_id"])
    assert changed["source_changed_count"] == 1
    assert changed["jobs"][0]["snapshot"]["sha256"] == original_snapshot_sha
    assert (
        next(item for item in changed["source_cases"] if item["id"] == first_case_id)[
            "changed_since_snapshot"
        ]
        is True
    )

    refresh_job_id = store.queue_combined_investigation_refresh(
        fusion_job["case_id"], requested_by="analyst"
    )
    refresh = store.get_job(refresh_job_id)
    assert refresh["status"] == "queued"
    assert refresh["options"]["investigation_spec"]["source_case_ids"] == [
        first_case_id,
        second_case_id,
    ]


def test_case_timeline_projects_existing_audit_records_without_new_state(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:timeline-test")
    result = {
        "status": "completed",
        "usernames": ["alice"],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Example Social",
                        "url": "https://example.test/alice",
                        "confidence": "strong",
                        "evidence": {
                            "_extractor": "example_profile",
                            "created_at": "2020-01-02 03:04:05",
                            "is_verified": "true",
                            "follower_count": "321",
                        },
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    assert store.sync_persona_claims(job_id, result) == 1
    case = store.get_case(store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    claim = store.get_persona(persona_id)["claims"][0]
    store.review_claim(
        claim["id"],
        "approved",
        "analyst",
        "Matched the cited public profile",
    )

    timeline = store.build_case_timeline(case["id"])

    assert timeline["case_id"] == case["id"]
    assert timeline["stats"] == {
        "displayed_count": 4,
        "investigation_count": 2,
        "evidence_count": 1,
        "review_count": 1,
        "truncated": False,
        "limit": 300,
    }
    assert {event["event_type"] for event in timeline["events"]} == {
        "investigation",
        "evidence",
        "review",
    }
    evidence_event = next(
        event for event in timeline["events"] if event["event_type"] == "evidence"
    )
    assert evidence_event["persona"]["id"] == persona_id
    assert evidence_event["claim"]["id"] == claim["id"]
    assert evidence_event["account_metadata"] == {
        "created_at": "2020-01-02 03:04:05",
        "is_verified": True,
        "follower_count": 321,
    }
    assert evidence_event["extractor"] == "example_profile"
    review_event = next(
        event for event in timeline["events"] if event["event_type"] == "review"
    )
    assert review_event["decision"] == "approved"
    assert review_event["note"] == "Matched the cited public profile"
    assert [event["timestamp"] for event in timeline["events"]] == sorted(
        (event["timestamp"] for event in timeline["events"]), reverse=True
    )

    persona_evidence = store.build_case_timeline(
        case["id"],
        persona_id=persona_id,
        event_type="evidence",
        order="oldest",
    )
    assert persona_evidence["selected_persona"]["id"] == persona_id
    assert [event["event_type"] for event in persona_evidence["events"]] == [
        "evidence"
    ]


def test_case_timeline_enforces_case_boundary_and_bounded_results(store):
    first_job_id = store.create_investigation(["alice"], {})
    first_case_id = store.get_job(first_job_id)["case_id"]
    foreign_job_id = store.create_investigation(["bob"], {})
    foreign_job = store.get_job(foreign_job_id)
    foreign_persona_id = store.get_case(foreign_job["case_id"])["personas"][0][
        "id"
    ]

    with pytest.raises(ValueError, match="does not belong"):
        store.build_case_timeline(
            first_case_id,
            persona_id=foreign_persona_id,
        )

    bounded = store.build_case_timeline(first_case_id, limit=1)
    assert bounded["stats"]["displayed_count"] == 1
    assert bounded["stats"]["truncated"] is False
    assert all(
        event.get("job_id") != foreign_job_id for event in bounded["events"]
    )

    with pytest.raises(ValueError, match="event type"):
        store.build_case_timeline(first_case_id, event_type="untrusted")
    with pytest.raises(ValueError, match="order"):
        store.build_case_timeline(first_case_id, order="sideways")


def test_grouped_identifier_variants_feed_one_reviewable_persona(store):
    options = {
        "investigation_spec": {
            "schema_version": 1,
            "processing_mode": "same_subject",
            "subject_label": "Jati Pratomo",
            "identifiers": [{"type": "full_name", "value": "Jati Pratomo"}],
            "search_targets": [
                {"value": "jatipratomo", "source_type": "generated_name_variant"},
                {"value": "jati.pratomo", "source_type": "generated_name_variant"},
            ],
        }
    }
    job_id = store.create_investigation(["jatipratomo", "jati.pratomo"], options)
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "usernames": ["jatipratomo", "jati.pratomo"],
        "individual_reports": [
            {
                "username": "jatipratomo",
                "claimed_profiles": [
                    {
                        "site_name": "Example One",
                        "url": "https://example.test/jatipratomo",
                        "confidence": "strong",
                        "evidence": {"fullname": "Jati Pratomo"},
                    }
                ],
            },
            {
                "username": "jati.pratomo",
                "claimed_profiles": [
                    {
                        "site_name": "Example Two",
                        "url": "https://example.test/jati.pratomo",
                        "confidence": "moderate",
                        "evidence": {"location": "Jakarta"},
                    }
                ],
            },
        ],
    }
    store.finish(job_id, result)

    assert store.sync_persona_claims(job_id, result) == 4
    case = store.get_case(store.get_job(job_id)["case_id"])
    assert [persona["display_name"] for persona in case["personas"]] == [
        "Jati Pratomo"
    ]
    claims = store.get_persona(case["personas"][0]["id"])["claims"]
    assert {claim["field_name"] for claim in claims} == {
        "full_name",
        "current_location",
        "social_account",
    }

    refresh_id = store.repeat_persona_investigation(case["personas"][0]["id"])
    assert store.get_job(refresh_id)["usernames"] == [
        "jatipratomo",
        "jati.pratomo",
    ]


def test_independent_identifier_mode_keeps_separate_personas(store):
    job_id = store.create_investigation(
        ["alice", "bob"],
        {
            "investigation_spec": {
                "schema_version": 1,
                "processing_mode": "independent",
                "subject_label": "alice",
            }
        },
    )

    case = store.get_case(store.get_job(job_id)["case_id"])
    assert [persona["display_name"] for persona in case["personas"]] == [
        "alice",
        "bob",
    ]


def _affiliation_observation():
    return {
        "source_engine": "wikidata_affiliation",
        "status": "observed",
        "organization": {
            "id": "Q95", "label": "Example Organization",
            "url": "https://www.wikidata.org/wiki/Q95",
            "official_websites": ["https://example.org"],
        },
        "people": [
            {
                "id": "Q1001", "label": "Alice Example",
                "url": "https://www.wikidata.org/wiki/Q1001",
                "relations": [{"property_id": "P108", "label": "employer", "direction": "person_to_organization"}],
            },
            {
                "id": "Q1002", "label": "Bob Example",
                "url": "https://www.wikidata.org/wiki/Q1002",
                "relations": [{"property_id": "P3320", "label": "board member", "direction": "organization_to_person"}],
            },
        ],
    }


def test_affiliation_discovery_is_pending_until_relationship_review(store):
    job_id = store.create_affiliation_investigation("Example Organization")
    job = store.claim_next("worker:affiliation")
    assert job["kind"] == "affiliation"
    assert store.sync_affiliation_discovery(job_id, _affiliation_observation()) == {"personas": 2, "claims": 6}
    case = store.get_case(job["case_id"])
    assert store.build_relationship_graph(case["id"])["edges"] == []
    for summary in case["personas"]:
        persona = store.get_persona(summary["id"])
        assert all(claim["review_status"] == "pending" for claim in persona["claims"])
        company = next(claim for claim in persona["claims"] if claim["field_name"] == "company")
        store.review_claim(company["id"], "approved", "analyst")
    assert len(store.build_relationship_graph(case["id"])["edges"]) == 2


def test_affiliation_selection_accepts_only_a_stored_candidate(store):
    job_id = store.create_affiliation_investigation(
        "Example Organization",
        source_claim_id="approved-role-claim",
        source_claim_field="occupation",
        target_basis="analyst_confirmed_role_organization",
    )
    job = store.claim_next("worker:test")
    store.finish(job_id, {"status": "completed", "organization_candidates": [{"id": "Q95", "label": "Example Organization", "organization_eligible": True}]})
    with pytest.raises(ValueError, match="not a stored candidate"):
        store.queue_affiliation_entity(job["case_id"], "Q999")
    selected = store.get_job(store.queue_affiliation_entity(job["case_id"], "Q95"))
    specification = selected["options"]["investigation_spec"]
    assert specification["wikidata_entity_id"] == "Q95"
    assert specification["source_claim_id"] == "approved-role-claim"
    assert specification["source_claim_field"] == "occupation"
    assert specification["target_basis"] == (
        "analyst_confirmed_role_organization"
    )


def test_affiliation_reruns_preserve_optional_google_places_search(store):
    job_id = store.create_affiliation_investigation(
        "Example Organization", enable_google_places_search=True
    )
    queued = store.get_job(job_id)
    assert queued["progress"]["total"] == 3
    assert queued["options"]["investigation_spec"][
        "enable_google_places_search"
    ] is True
    job = store.claim_next("worker:google-places-flag")
    store.finish(job_id, {"status": "completed"})

    rerun = store.get_job(store.queue_affiliation_context(job["case_id"]))

    assert rerun["progress"]["total"] == 5
    assert rerun["options"]["investigation_spec"][
        "enable_google_places_search"
    ] is True


def test_source_neutral_organization_selection_is_persisted_and_audited(store):
    job_id = store.create_affiliation_investigation(
        "Example Organization",
        jurisdiction="ID",
        official_website="https://example.org",
    )
    job = store.claim_next("worker:organization-selection")
    website_candidate = {
        "candidate_key": "official_website_public_content:example.org",
        "label": "Example Organization",
        "source_engine": "official_website_public_content",
        "source_name": "Supplied organization website",
        "source_record_id": "official-website:example.org",
        "source_url": "https://example.org/",
        "identity_scope": "first_party_operating_identity",
        "identity_scope_label": "First-party operating identity",
        "match_status": "operator_supplied_domain",
        "selectable": True,
        "basis": "The supplied website names the organization.",
        "limitation": "This does not establish legal registration.",
    }
    store.finish(
        job_id,
        {
            "status": "completed",
            "organization_resolution_candidates": [website_candidate],
        },
    )

    selected = store.select_affiliation_organization(
        job["case_id"], website_candidate["candidate_key"], "analyst"
    )

    assert selected["source_engine"] == "official_website_public_content"
    assert selected["review_status"] == "approved"
    assert selected["automatic_approval_allowed"] is False
    completed = store.get_job(job_id)
    assert completed["selected_organization"]["candidate_key"] == website_candidate[
        "candidate_key"
    ]
    assert completed["options"]["investigation_spec"]["selected_organization"][
        "reviewed_by"
    ] == "analyst"
    assert completed["organization_resolution_candidates"][0]["selected"] is True
    assert store.get_case(job["case_id"])["title"] == (
        "Affiliation: Example Organization · ID"
    )
    assert store.get_events(job_id)[-1]["event"]["type"] == (
        "organization_selected"
    )
    rerun = store.get_job(store.queue_affiliation_context(job["case_id"]))
    assert rerun["options"]["investigation_spec"]["selected_organization"][
        "candidate_key"
    ] == website_candidate["candidate_key"]


def test_source_neutral_selection_rejects_non_organization_candidate(store):
    job_id = store.create_affiliation_investigation("Example Organization")
    job = store.claim_next("worker:non-organization-selection")
    store.finish(
        job_id,
        {
            "status": "completed",
            "organization_resolution_candidates": [
                {
                    "candidate_key": "wikidata_affiliation:Q127199078",
                    "label": "Example Organization article",
                    "source_engine": "wikidata_affiliation",
                    "source_name": "Wikidata",
                    "identity_scope": "public_knowledge_entity",
                    "selectable": False,
                }
            ],
        },
    )

    with pytest.raises(ValueError, match="not verified as an organization"):
        store.select_affiliation_organization(
            job["case_id"], "wikidata_affiliation:Q127199078", "analyst"
        )


def test_source_neutral_selection_rejects_candidate_from_older_result(store):
    first_job_id = store.create_affiliation_investigation(
        "Example Organization"
    )
    first_job = store.claim_next("worker:first-organization-result")
    old_candidate = {
        "candidate_key": "wikidata_affiliation:Q95",
        "label": "Example Organization",
        "source_engine": "wikidata_affiliation",
        "source_name": "Wikidata",
        "identity_scope": "public_knowledge_entity",
        "selectable": True,
    }
    store.finish(
        first_job_id,
        {
            "status": "completed",
            "organization_resolution_candidates": [old_candidate],
        },
    )
    rerun_id = store.queue_affiliation_context(first_job["case_id"])
    store.claim_next("worker:current-organization-result")
    store.finish(
        rerun_id,
        {"status": "completed", "organization_resolution_candidates": []},
    )

    with pytest.raises(ValueError, match="current completed affiliation"):
        store.select_affiliation_organization(
            first_job["case_id"], old_candidate["candidate_key"], "analyst"
        )

    assert store.get_job(first_job_id).get("selected_organization") is None


def test_source_neutral_selection_waits_for_active_affiliation_job(store):
    job_id = store.create_affiliation_investigation("Example Organization")
    job = store.claim_next("worker:active-organization-result")
    candidate = {
        "candidate_key": "wikidata_affiliation:Q95",
        "label": "Example Organization",
        "source_engine": "wikidata_affiliation",
        "source_name": "Wikidata",
        "identity_scope": "public_knowledge_entity",
        "selectable": True,
    }
    store.finish(
        job_id,
        {
            "status": "completed",
            "organization_resolution_candidates": [candidate],
        },
    )
    store.queue_affiliation_context(job["case_id"])

    with pytest.raises(ValueError, match="current affiliation investigation"):
        store.select_affiliation_organization(
            job["case_id"], candidate["candidate_key"], "analyst"
        )


def test_analyst_selected_registry_entity_proposes_people_for_review(store):
    job_id = store.create_affiliation_investigation(
        "Unistellar", jurisdiction="FR"
    )
    job = store.claim_next("worker:registry-selection")
    registry_observation = _fr_affiliation_observation()
    registry_observation["selected_entity"] = None
    registry_observation["candidates"][0]["exact_name_match"] = False
    registry_candidate = {
        "candidate_key": "fr_company_registry:812339356",
        "label": "UNISTELLAR",
        "source_engine": "fr_company_registry",
        "source_name": "French National Enterprise Directory",
        "source_record_id": "fr-company-search:unistellar",
        "source_url": (
            "https://annuaire-entreprises.data.gouv.fr/entreprise/812339356"
        ),
        "entity_id": "812339356",
        "identity_scope": "registered_legal_entity",
        "identity_scope_label": "Registered legal entity",
        "match_status": "requires_review",
        "selectable": True,
        "basis": "The public registry returned this candidate.",
        "limitation": "The analyst must confirm the identity match.",
    }
    store.finish(
        job_id,
        {
            "status": "completed",
            "registry_observations": [registry_observation],
            "organization_resolution_candidates": [registry_candidate],
        },
    )

    store.select_affiliation_organization(
        job["case_id"], registry_candidate["candidate_key"], "analyst"
    )

    case = store.get_case(job["case_id"])
    assert len(case["personas"]) == 2
    for persona_summary in case["personas"]:
        persona = store.get_persona(persona_summary["id"])
        assert all(
            claim["review_status"] == "pending" for claim in persona["claims"]
        )
        assert all(
            claim["evidence"][0]["details"]["analyst_selected_entity"] is True
            for claim in persona["claims"]
        )


def test_registry_selection_and_people_sync_are_not_france_specific(store):
    job_id = store.create_affiliation_investigation(
        "Example Global Organization", jurisdiction="ID"
    )
    job = store.claim_next("worker:global-registry-selection")
    entity = {
        "id": "9695005MSX1OYEMGDF46",
        "identifier_type": "lei",
        "legal_name": "EXAMPLE GLOBAL ORGANIZATION",
        "legal_jurisdiction": "ID",
        "entity_status": "active",
        "exact_name_match": False,
        "source_url": (
            "https://api.gleif.org/api/v1/lei-records/"
            "9695005MSX1OYEMGDF46"
        ),
        "people": [
            {"display_name": "Ayu Example", "role": "Managing Director"}
        ],
    }
    registry_observation = {
        "source_engine": "gleif_lei_registry",
        "status": "observed",
        "candidates": [entity],
        "selected_entity": None,
    }
    registry_candidate = {
        "candidate_key": "gleif_lei_registry:9695005MSX1OYEMGDF46",
        "label": "EXAMPLE GLOBAL ORGANIZATION",
        "source_engine": "gleif_lei_registry",
        "source_name": "GLEIF Global LEI Index",
        "source_record_id": "gleif-search:example-global-organization",
        "source_url": entity["source_url"],
        "entity_id": entity["id"],
        "identity_scope": "registered_legal_entity",
        "identity_scope_label": "Registered legal entity",
        "match_status": "requires_review",
        "selectable": True,
        "basis": "The governed registry returned this candidate.",
        "limitation": "The analyst must confirm the identity match.",
    }
    store.finish(
        job_id,
        {
            "status": "completed",
            "registry_observations": [registry_observation],
            "organization_resolution_candidates": [registry_candidate],
        },
    )

    store.select_affiliation_organization(
        job["case_id"], registry_candidate["candidate_key"], "analyst"
    )

    case = store.get_case(job["case_id"])
    assert [persona["display_name"] for persona in case["personas"]] == [
        "Ayu Example"
    ]
    persona = store.get_persona(case["personas"][0]["id"])
    assert {claim["field_name"] for claim in persona["claims"]} == {
        "full_name",
        "company",
        "occupation",
    }
    assert all(
        claim["source_engine"] == "gleif_lei_registry"
        for claim in persona["claims"]
    )
    assert all(
        claim["review_status"] == "pending" for claim in persona["claims"]
    )


def _fr_affiliation_observation():
    entity = {
        "id": "812339356",
        "identifier_type": "siren",
        "legal_name": "UNISTELLAR",
        "legal_jurisdiction": "FR",
        "headquarters_identifier": "81233935600030",
        "entity_status": "active",
        "last_update_date": "2026-08-01T00:00:00Z",
        "exact_name_match": True,
        "source_url": (
            "https://annuaire-entreprises.data.gouv.fr/entreprise/812339356"
        ),
        "people": [
            {"display_name": "Arnaud Malvache", "role": "Président de SAS"},
            {"display_name": "Laurent Marfisi", "role": "Directeur Général"},
        ],
    }
    return {
        "source_engine": "fr_company_registry",
        "status": "observed",
        "jurisdiction": {
            "code": "FR",
            "label": "France",
            "country_code": "FR",
        },
        "candidates": [entity],
        "selected_entity": entity,
    }


def test_jurisdiction_registry_people_are_pending_and_provenance_linked(store):
    job_id = store.create_affiliation_investigation(
        "Unistellar", jurisdiction="France"
    )
    job = store.claim_next("worker:fr-registry")
    specification = job["options"]["investigation_spec"]
    assert specification["schema_version"] == 2
    assert specification["legal_jurisdiction"] == {
        "code": "FR",
        "label": "France",
        "country_code": "FR",
    }
    assert job["progress"]["total"] == 4

    wikidata_unavailable = {
        "source_engine": "wikidata_affiliation",
        "status": "unavailable",
        "organization": None,
        "people": [],
    }
    synchronized = store.sync_affiliation_discovery(
        job_id,
        wikidata_unavailable,
        registry_observations=[_fr_affiliation_observation()],
    )
    assert synchronized == {"personas": 2, "claims": 6}
    assert store.sync_affiliation_discovery(
        job_id,
        wikidata_unavailable,
        registry_observations=[_fr_affiliation_observation()],
    ) == {"personas": 0, "claims": 6}

    case = store.get_case(job["case_id"])
    assert case["title"] == "Affiliation: UNISTELLAR · FR"
    assert store.build_relationship_graph(case["id"])["edges"] == []
    for summary in case["personas"]:
        persona = store.get_persona(summary["id"])
        assert {claim["field_name"] for claim in persona["claims"]} == {
            "full_name",
            "company",
            "occupation",
        }
        assert all(
            claim["review_status"] == "pending" for claim in persona["claims"]
        )
        assert all(
            claim["source_engine"] == "fr_company_registry"
            for claim in persona["claims"]
        )
        assert all(
            claim["evidence"][0]["details"]["human_review_required"] is True
            for claim in persona["claims"]
        )


def test_affiliation_entity_selection_preserves_jurisdiction(store):
    job_id = store.create_affiliation_investigation(
        "Example Organization", jurisdiction="US-DE"
    )
    job = store.claim_next("worker:selection-jurisdiction")
    store.finish(
        job_id,
        {
            "status": "completed",
            "organization_candidates": [
                {
                    "id": "Q95",
                    "label": "Example Organization",
                    "organization_eligible": True,
                }
            ],
        },
    )
    selected = store.get_job(
        store.queue_affiliation_entity(job["case_id"], "Q95")
    )
    specification = selected["options"]["investigation_spec"]
    assert specification["schema_version"] == 2
    assert specification["legal_jurisdiction"]["code"] == "US-DE"
    assert selected["progress"]["total"] == 3


def test_affiliation_domain_context_is_explicit_and_preserved_on_rerun(store):
    job_id = store.create_affiliation_investigation(
        "Example Organization",
        jurisdiction="FR",
        enable_domain_context=True,
        official_website="https://example.org/about",
    )
    job = store.claim_next("worker:domain-context")
    specification = job["options"]["investigation_spec"]
    assert specification["enable_domain_context"] is True
    assert specification["official_website"] == {
        "url": "https://example.org/about",
        "domain": "example.org",
    }
    assert job["progress"]["total"] == 6
    store.finish(job_id, {"status": "completed"})

    rerun = store.get_job(store.queue_affiliation_context(job["case_id"]))
    rerun_specification = rerun["options"]["investigation_spec"]
    assert rerun_specification["enable_domain_context"] is True
    assert rerun_specification["official_website"]["domain"] == "example.org"
    assert rerun["progress"]["total"] == 6


def _approved_full_name(store, name="Alice Example"):
    job_id = store.create_investigation(["alice"], {})
    job = store.claim_next("worker:identity-source")
    result = {
        "status": "completed",
        "usernames": ["alice"],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Example Social",
                        "url": "https://example.test/alice",
                        "confidence": "strong",
                        "evidence": {"fullname": name},
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    persona_id = store.get_case(job["case_id"])["personas"][0]["id"]
    claim = next(
        claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "full_name"
    )
    store.review_claim(claim["id"], "approved", "analyst")
    return persona_id, claim["id"]


def _wikipedia_identity_observation(*, candidates=None):
    return {
        "source_engine": "wikipedia_public_biography",
        "status": "observed",
        "page_candidates": list(candidates or []),
        "page": {
            "page_id": "123",
            "title": "Alice Example",
            "url": "https://en.wikipedia.org/wiki/Alice_Example",
            "extract": "Alice Example is a public figure.",
            "thumbnail_url": "https://upload.wikimedia.org/alice.jpg",
        },
    }


def _icij_identity_observation():
    return {
        "source_engine": "icij_offshore_leaks",
        "status": "potential_match",
        "matches": [
            {
                "node_id": "12126782",
                "name": "Alice Example",
                "description": "Officer record",
                "score": 100,
                "url": "https://offshoreleaks.icij.org/nodes/12126782",
            }
        ],
    }


def test_confirmed_name_enrichment_persists_only_pending_review_candidates(store):
    persona_id, name_claim_id = _approved_full_name(store)
    enrichment_id = store.create_identity_enrichment(persona_id, name_claim_id)
    job = store.claim_next("worker:identity")

    assert job["job_id"] == enrichment_id
    assert job["kind"] == "identity_enrichment"
    assert job["options"]["investigation_spec"]["confirmed_name"] == "Alice Example"
    assert store.sync_identity_enrichment(
        enrichment_id,
        _wikipedia_identity_observation(),
        _icij_identity_observation(),
    ) == {"wikipedia_claims": 3, "offshore_alerts": 1}

    claims = store.get_persona(persona_id)["claims"]
    enriched = [
        claim
        for claim in claims
        if claim["source_engine"]
        in {"wikipedia_public_biography", "icij_offshore_leaks"}
    ]
    assert {claim["field_name"] for claim in enriched} == {
        "summary",
        "platform_identifier",
        "photograph",
        "offshore_database_match",
    }
    assert all(claim["review_status"] == "pending" for claim in enriched)
    offshore = next(
        claim for claim in enriched if claim["field_name"] == "offshore_database_match"
    )
    assert "not sufficient to confirm identity" in offshore["evidence"][0][
        "details"
    ]["identity_warning"]
    assert "does not imply" in offshore["evidence"][0]["details"][
        "identity_warning"
    ]


def test_wikipedia_selection_accepts_only_a_stored_candidate_for_same_persona(store):
    persona_id, name_claim_id = _approved_full_name(store)
    enrichment_id = store.create_identity_enrichment(persona_id, name_claim_id)
    store.claim_next("worker:identity")
    store.finish(
        enrichment_id,
        {
            "status": "completed",
            "wikipedia_candidates": [
                {
                    "page_id": "123",
                    "title": "Alice Example",
                    "url": "https://en.wikipedia.org/wiki/Alice_Example",
                }
            ],
        },
    )

    with pytest.raises(ValueError, match="not a stored candidate"):
        store.create_identity_enrichment(
            persona_id,
            name_claim_id,
            selected_wikipedia_page_id="999",
        )
    selected = store.get_job(
        store.create_identity_enrichment(
            persona_id,
            name_claim_id,
            selected_wikipedia_page_id="123",
        )
    )
    assert selected["options"]["investigation_spec"][
        "selected_wikipedia_page_id"
    ] == "123"
