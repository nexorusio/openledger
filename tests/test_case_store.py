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
