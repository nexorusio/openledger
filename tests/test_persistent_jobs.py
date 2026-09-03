import asyncio
import json
import os
import time
from threading import Timer

import pytest

from maigret.web.case_store import CaseStore
from maigret.web import app as web_app_module


@pytest.fixture
def web_app(tmp_path):
    web_app_module.app.config["TESTING"] = True
    web_app_module.app.config["REPORTS_FOLDER"] = str(tmp_path / "reports")
    web_app_module.app.config["SETTINGS_FILE"] = str(tmp_path / "settings.json")
    web_app_module.app.config["OPENAI_API_KEY_FILE"] = str(
        tmp_path / "secrets" / "openai_api_key"
    )
    web_app_module.app.config["GOOGLE_MAPS_API_KEY_FILE"] = str(
        tmp_path / "secrets" / "google_maps_api_key"
    )
    web_app_module.app.config["AUTH_REQUIRED"] = False
    web_app_module.job_results.clear()
    web_app_module.live_jobs.clear()
    web_app_module.google_places_live_requests.clear()
    yield web_app_module
    web_app_module.job_results.clear()
    web_app_module.live_jobs.clear()
    web_app_module.google_places_live_requests.clear()


@pytest.fixture
def client(web_app):
    return web_app.app.test_client()


@pytest.fixture
def persistent_store(tmp_path, web_app, monkeypatch):
    store = CaseStore(
        f"sqlite:///{tmp_path / 'persistent-web.db'}",
        create_schema=True,
    )
    monkeypatch.setattr(web_app, "case_store", store)
    yield store
    store.dispose()


def test_live_job_is_queued_without_browser_owned_thread(
    client, web_app, persistent_store, monkeypatch
):
    def forbidden_thread(*_args, **_kwargs):
        raise AssertionError("the web process must not start an investigation thread")

    monkeypatch.setattr(web_app, "Thread", forbidden_thread)
    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "investigation-csrf"
    response = client.post(
        "/live",
        data={"usernames": "alice", "csrf_token": "investigation-csrf"},
    )
    assert response.status_code == 302
    job_id = response.location.rsplit("/", 1)[-1]
    stored = persistent_store.get_job(job_id)
    assert stored["status"] == "queued"

    history = client.get("/history").get_data(as_text=True)
    assert "Queued" in history
    assert f"/live/{job_id}" in history
    assert "Open live progress" in history


def test_stream_reconnect_replays_events_without_deleting_job(client, persistent_store):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
    persistent_store.append_event(
        job_id, {"type": "start", "username": "alice", "total": 2}
    )
    persistent_store.append_event(
        job_id, {"type": "progress", "checked": 1, "total": 2}
    )
    persistent_store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": ["alice"],
            "individual_reports": [],
            "graph_file": f"search_{job_id}/graph.html",
            "found_count": 0,
        },
    )
    final_id = persistent_store.append_event(
        job_id,
        {
            "type": "done",
            "status": "completed",
            "redirect": f"/results/search_{job_id}",
        },
    )

    first = client.get(f"/api/scan/{job_id}/stream").get_data(as_text=True)
    assert '"type": "queued"' in first
    assert '"type": "done"' in first
    assert persistent_store.get_job(job_id)["status"] == "completed"

    replay = client.get(f"/api/scan/{job_id}/stream?after={final_id - 1}").get_data(
        as_text=True
    )
    assert '"type": "done"' in replay
    assert '"type": "start"' not in replay


def test_stop_route_sets_durable_cancel_request(client, persistent_store):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "stop-csrf"
    response = client.post(
        f"/api/scan/{job_id}/stop",
        headers={"X-OpenLedger-CSRF": "stop-csrf"},
    )
    assert response.status_code == 200
    assert persistent_store.get_job(job_id)["status"] == "cancel_requested"


def test_stop_route_rejects_missing_csrf(client, persistent_store):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
    response = client.post(f"/api/scan/{job_id}/stop")
    assert response.status_code == 403
    assert persistent_store.get_job(job_id)["status"] == "running"


def test_persistent_worker_actively_interrupts_inflight_search_after_stop(
    web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_investigation(["alice"], {})
    job = persistent_store.claim_next("worker:test")

    async def slow_search(*_args, **_kwargs):
        await asyncio.sleep(2)
        return {}

    monkeypatch.setattr(web_app, "maigret_search", slow_search)
    monkeypatch.setattr(web_app, "persist_job_result", lambda *_args: None)
    monkeypatch.setattr(web_app, "PERSISTENT_CANCEL_POLL_SECONDS", 0.01)
    request_stop = Timer(0.05, persistent_store.request_cancel, args=(job_id,))

    started_at = time.monotonic()
    request_stop.start()
    try:
        web_app.run_persistent_job(persistent_store, job)
    finally:
        request_stop.join()
    elapsed = time.monotonic() - started_at

    assert elapsed < 1.5
    cancelled = persistent_store.get_job(job_id)
    assert cancelled["status"] == "cancelled"
    assert cancelled["completed_at"] is not None
    assert persistent_store.get_events(job_id)[-1]["event"]["type"] == "done"


def test_queued_job_does_not_store_proxy_credentials(
    client, web_app, persistent_store, monkeypatch
):
    protected_proxy = "http://operator:secret@proxy.internal:8080"
    settings = dict(web_app.DEFAULT_SETTINGS)
    settings["proxy"] = protected_proxy
    monkeypatch.setattr(web_app, "load_settings", lambda: dict(settings))
    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "investigation-csrf"

    response = client.post(
        "/live",
        data={"usernames": "alice", "csrf_token": "investigation-csrf"},
    )
    job_id = response.location.rsplit("/", 1)[-1]
    stored_options = persistent_store.get_job(job_id)["options"]

    assert protected_proxy not in str(stored_options)
    assert "proxy" not in stored_options
    assert stored_options["proxy_configured"] is True
    hydrated = web_app.hydrate_persistent_options(stored_options)
    assert hydrated["proxy"] == protected_proxy


def test_worker_execution_persists_terminal_result(
    web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_investigation(["alice"], {})
    job = persistent_store.claim_next("worker:test")

    async def fake_stream(runtime_job, usernames, options, cancellation_check=None):
        runtime_job["queue"].put(
            {"type": "start", "username": usernames[0], "total": 1}
        )
        return [("alice", "username", {"Example": object()})]

    monkeypatch.setattr(web_app, "_stream_search", fake_stream)
    monkeypatch.setattr(
        web_app,
        "build_reports",
        lambda _results, usernames, session_key: {
            "status": "completed",
            "session_folder": f"search_{session_key}",
            "usernames": usernames,
            "individual_reports": [],
            "graph_file": f"search_{session_key}/graph.html",
            "found_count": 1,
        },
    )
    monkeypatch.setattr(web_app, "persist_job_result", lambda *_args: None)

    web_app.run_persistent_job(persistent_store, job)
    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["found_count"] == 1
    assert any(
        item["event"]["type"] == "done" for item in persistent_store.get_events(job_id)
    )


def test_worker_shutdown_saves_partial_findings_as_interrupted_collection(
    web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_investigation(["alice"], {})
    job = persistent_store.claim_next("worker:test")

    async def fake_stream(runtime_job, usernames, options, cancellation_check=None):
        return [("alice", "username", {"Example": object()})]

    monkeypatch.setattr(web_app, "_stream_search", fake_stream)
    monkeypatch.setattr(
        web_app,
        "build_reports",
        lambda _results, usernames, session_key: {
            "status": "completed",
            "session_folder": f"search_{session_key}",
            "usernames": usernames,
            "individual_reports": [],
            "graph_file": f"search_{session_key}/graph.html",
            "found_count": 1,
        },
    )
    monkeypatch.setattr(web_app, "persist_job_result", lambda *_args: None)

    web_app.run_persistent_job(
        persistent_store,
        job,
        shutdown_check=lambda: True,
    )
    result = persistent_store.get_job(job_id)
    assert result["status"] == "completed"
    assert result["collection_status"] == "interrupted"
    assert result["found_count"] == 1
    done = persistent_store.get_events(job_id)[-1]["event"]
    assert done["type"] == "done"
    assert done["status"] == "partial"


def test_terminal_case_can_be_deleted_with_all_report_artifacts(
    client, web_app, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {})
    job = persistent_store.claim_next("worker:test")
    result = {
        "status": "cancelled",
        "session_folder": f"search_{job_id}",
        "usernames": job["usernames"],
        "error": "Stopped for test.",
    }
    persistent_store.finish(job_id, result)
    case_id = job["case_id"]
    report_directory = os.path.join(
        web_app.app.config["REPORTS_FOLDER"], f"search_{job_id}"
    )
    os.makedirs(report_directory)
    with open(
        os.path.join(report_directory, "partial.json"), "w", encoding="utf-8"
    ) as report_file:
        report_file.write("{}")
    web_app.job_results[job_id] = result
    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "delete-case-csrf"

    response = client.post(
        f"/cases/{case_id}/delete",
        data={"csrf_token": "delete-case-csrf", "confirmation_name": "alice"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "permanently deleted" in response.get_data(as_text=True)
    assert persistent_store.get_case(case_id) is None
    assert persistent_store.get_job(job_id) is None
    assert not os.path.exists(report_directory)
    assert job_id not in web_app.job_results


def test_active_case_delete_is_refused_without_losing_data(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {})
    case_id = persistent_store.get_job(job_id)["case_id"]
    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "delete-case-csrf"

    response = client.post(
        f"/cases/{case_id}/delete",
        data={"csrf_token": "delete-case-csrf", "confirmation_name": "alice"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "Stop the active investigation" in response.get_data(as_text=True)
    assert persistent_store.get_case(case_id) is not None
    assert persistent_store.get_job(job_id) is not None


def test_case_delete_requires_the_exact_case_name(client, persistent_store):
    job_id = persistent_store.create_investigation(["alice"], {})
    job = persistent_store.claim_next("worker:test")
    persistent_store.finish(job_id, {"status": "cancelled", "usernames": ["alice"]})
    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "delete-case-csrf"

    response = client.post(
        f'/cases/{job["case_id"]}/delete',
        data={"csrf_token": "delete-case-csrf", "confirmation_name": "Alice"},
        follow_redirects=True,
    )

    assert "Type the exact case name" in response.get_data(as_text=True)
    assert persistent_store.get_case(job["case_id"]) is not None


def test_case_delete_requires_csrf(client, persistent_store):
    job_id = persistent_store.create_investigation(["alice"], {})
    job = persistent_store.claim_next("worker:test")
    persistent_store.finish(
        job_id,
        {"status": "cancelled", "usernames": job["usernames"]},
    )

    response = client.post(
        f'/cases/{job["case_id"]}/delete',
        data={"csrf_token": "invalid"},
    )

    assert response.status_code == 302
    assert persistent_store.get_case(job["case_id"]) is not None
    assert persistent_store.get_job(job_id) is not None


def test_case_delete_rejects_symlinked_report_directory(
    web_app, persistent_store, tmp_path
):
    job_id = persistent_store.create_investigation(["alice"], {})
    job = persistent_store.claim_next("worker:test")
    persistent_store.finish(
        job_id,
        {"status": "cancelled", "usernames": job["usernames"]},
    )
    outside_directory = tmp_path / "outside-reports"
    outside_directory.mkdir()
    sentinel = outside_directory / "preserve.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    os.makedirs(web_app.app.config["REPORTS_FOLDER"], exist_ok=True)
    os.symlink(
        outside_directory,
        os.path.join(web_app.app.config["REPORTS_FOLDER"], f"search_{job_id}"),
    )

    with pytest.raises(ValueError, match="Invalid report session directory"):
        web_app.delete_persisted_case(job["case_id"])

    assert persistent_store.get_case(job["case_id"]) is not None
    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_child_worker_cancellation_is_not_recorded_as_completed(
    web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_investigation(["alice"], {})
    job = persistent_store.claim_next("worker:test")

    async def fake_search(_username, _options, query_notify=None):
        persistent_store.request_cancel(job_id)
        try:
            query_notify.update(object())
        except asyncio.CancelledError:
            # Match the executor behavior that originally swallowed the child
            # worker cancellation before the outer search task could see it.
            pass
        return {}

    monkeypatch.setattr(web_app, "maigret_search", fake_search)
    monkeypatch.setattr(web_app, "persist_job_result", lambda *_args: None)

    web_app.run_persistent_job(persistent_store, job)

    result = persistent_store.get_job(job_id)
    assert result["status"] == "cancelled"
    assert "individual_reports" not in result
    assert "cancelled before finding" in result["error"]
    done = persistent_store.get_events(job_id)[-1]["event"]
    assert done["type"] == "done"
    assert done["status"] == "cancelled"


def test_case_and_persona_workspaces_render_reviewable_evidence(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
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
                        "url": "https://example.test/alice",
                        "confidence": "strong",
                        "evidence": {"fullname": "Alice Example"},
                    }
                ],
            }
        ],
    }
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]

    cases_page = client.get("/cases").get_data(as_text=True)
    assert "Cases" in cases_page
    assert "alice" in cases_page
    assert f'/cases/{case["id"]}' in cases_page
    assert f'/cases/{case["id"]}/delete' in cases_page

    case_page = client.get(f'/cases/{case["id"]}').get_data(as_text=True)
    assert "Open structured profile" in case_page
    assert f"/personas/{persona_id}" in case_page
    assert "Delete case" in case_page
    assert 'name="confirmation_name"' in case_page
    assert 'data-case-title="alice"' in case_page
    assert 'id="caseDeleteModal"' in case_page
    assert 'id="caseDeleteCopy"' in case_page
    assert 'id="caseDeleteConfirmation"' in case_page
    assert 'id="caseDeleteConfirm"' in case_page

    deletion_script = client.get("/static/openledger.js").get_data(as_text=True)
    assert "navigator.clipboard.writeText" in deletion_script
    assert "window.prompt" not in deletion_script
    assert "window.alert" not in deletion_script

    persona_page = client.get(f"/personas/{persona_id}").get_data(as_text=True)
    assert "Alice Example" in persona_page
    assert "90% confidence" in persona_page
    assert f'/personas/{persona_id}/export.pdf' in persona_page
    assert "Export persona PDF" in persona_page
    assert "No evidence extracted." in persona_page
    assert "AI proposes; the analyst decides" in persona_page
    assert "Review queue" in persona_page
    assert "Relationships" in persona_page


def test_persona_pdf_route_exports_only_curated_records(client, persistent_store):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:pdf-test")
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
                            "fullname": "Alice Example",
                            "email": "pending@example.test",
                        },
                    }
                ],
            }
        ],
    }
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    full_name = next(
        claim
        for claim in persistent_store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "full_name"
    )
    persistent_store.review_claim(full_name["id"], "approved", "analyst")

    response = client.get(f"/personas/{persona_id}/export.pdf")

    assert response.status_code == 200
    assert response.mimetype == "application/pdf"
    assert response.data.startswith(b"%PDF-")
    assert "attachment" in response.headers["Content-Disposition"]
    assert "openledger-persona-alice-" in response.headers["Content-Disposition"]
    assert response.headers["Cache-Control"] == "private, no-store, max-age=0"


def test_case_timeline_renders_bounded_provenance_and_escapes_evidence(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:timeline-test")
    result = {
        "status": "completed",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice"],
        "found_count": 1,
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
                            "fullname": '<script>alert("timeline")</script>',
                            "created_at": "2020-01-02 03:04:05",
                            "follower_count": "321",
                        },
                    }
                ],
            }
        ],
    }
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    full_name = next(
        claim
        for claim in persistent_store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "full_name"
    )
    persistent_store.review_claim(
        full_name["id"],
        "uncertain",
        "analyst",
        '<img src=x onerror="alert(1)">',
    )

    case_page = client.get(f'/cases/{case["id"]}').get_data(as_text=True)
    persona_page = client.get(f"/personas/{persona_id}").get_data(as_text=True)
    assert f'/cases/{case["id"]}/timeline' in case_page
    assert f'/cases/{case["id"]}/timeline?persona_id={persona_id}' in persona_page

    response = client.get(f'/cases/{case["id"]}/timeline')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Recorded chronology, not inferred behavior" in body
    assert "Investigation started" in body
    assert "Investigation completed" in body
    assert "Evidence observed" in body
    assert "Claim marked uncertain" in body
    assert "Source-reported account creation" in body
    assert "2020-01-02 03:04:05" in body
    assert "example_profile" in body
    assert '<script>alert("timeline")</script>' not in body
    assert "&lt;script&gt;alert" in body
    assert '<img src=x onerror="alert(1)">' not in body
    assert "&lt;img src=x onerror=&#34;alert(1)&#34;&gt;" in body

    before = persistent_store.build_case_timeline(case["id"])
    mutation = client.post(
        f'/cases/{case["id"]}/timeline', data={"action": "rewrite"}
    )
    assert mutation.status_code == 405
    assert persistent_store.build_case_timeline(case["id"]) == before

    filtered = client.get(
        f'/cases/{case["id"]}/timeline'
        f"?persona_id={persona_id}&event_type=review&order=oldest"
    ).get_data(as_text=True)
    assert "Claim marked uncertain" in filtered
    assert "Investigation started" not in filtered
    assert "multi-subject investigation events are excluded" in filtered


def test_case_timeline_rejects_persona_from_another_case(client, persistent_store):
    first_job_id = persistent_store.create_investigation(["alice"], {})
    first_case_id = persistent_store.get_job(first_job_id)["case_id"]
    foreign_job_id = persistent_store.create_investigation(["bob"], {})
    foreign_case = persistent_store.get_case(
        persistent_store.get_job(foreign_job_id)["case_id"]
    )
    foreign_persona_id = foreign_case["personas"][0]["id"]

    response = client.get(
        f"/cases/{first_case_id}/timeline?persona_id={foreign_persona_id}"
    )

    assert response.status_code == 302
    assert response.location.endswith(f"/cases/{first_case_id}/timeline")
    followed = client.get(response.location).get_data(as_text=True)
    assert "bob" not in followed


def test_claim_review_requires_csrf_and_records_operator_decision(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
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
                        "evidence": {},
                    }
                ],
            }
        ],
    }
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    claim_id = persistent_store.get_persona(persona_id)["claims"][0]["id"]

    rejected = client.post(
        f"/claims/{claim_id}/review",
        data={"persona_id": persona_id, "decision": "approved"},
    )
    assert rejected.status_code == 302
    assert (
        persistent_store.get_persona(persona_id)["claims"][0]["review_status"]
        == "pending"
    )

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "review-csrf"
        browser_session["username"] = "analyst"
    accepted = client.post(
        f"/claims/{claim_id}/review",
        data={
            "csrf_token": "review-csrf",
            "persona_id": persona_id,
            "decision": "approved",
            "note": "Verified against the linked public profile",
        },
    )
    assert accepted.status_code == 302
    reviewed = persistent_store.get_persona(persona_id)["claims"][0]
    assert reviewed["review_status"] == "approved"
    assert reviewed["reviewed_by"] == "analyst"
    assert reviewed["reviews"][0]["note"].startswith("Verified")


def test_approving_place_without_coordinates_generates_and_persists_centroid(
    client, persistent_store, monkeypatch
):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
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
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    persona_id = persistent_store.get_case(
        persistent_store.get_job(job_id)["case_id"]
    )["personas"][0]["id"]
    location = persistent_store.get_persona(persona_id)["claims"][0]
    captured = {}

    def fake_geocode(place, **kwargs):
        captured.update(place=place, kwargs=kwargs)
        return {
            "latitude": -6.1841,
            "longitude": 106.831,
            "display_name": "Jakarta, Indonesia",
            "precision": "place",
        }

    monkeypatch.setattr(web_app_module, "geocode_place_center", fake_geocode)
    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "review-csrf"
        browser_session["username"] = "analyst"

    response = client.post(
        f"/claims/{location['id']}/review",
        data={
            "csrf_token": "review-csrf",
            "persona_id": persona_id,
            "decision": "approved",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert captured["place"] == "Jakarta, Indonesia"
    reviewed = persistent_store.get_persona(persona_id)["claims"][0]
    assert reviewed["review_status"] == "approved"
    assert reviewed["latitude"] == pytest.approx(-6.1841)
    assert reviewed["longitude"] == pytest.approx(106.831)
    page = response.get_data(as_text=True)
    assert "generated place centroid" in page
    assert 'id="personaLocationMap"' in page


def test_legacy_persona_refresh_requires_configuration_before_queueing(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {"top_sites": 250})
    persistent_store.claim_next("worker:test")
    persistent_store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": ["alice"],
            "individual_reports": [],
            "graph_file": f"search_{job_id}/graph.html",
            "found_count": 0,
        },
    )
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "refresh-csrf"

    response = client.post(
        f"/personas/{persona_id}/refresh",
        data={"csrf_token": "refresh-csrf"},
    )

    assert response.status_code == 302
    assert response.location.endswith(f"/personas/{persona_id}/investigate")
    assert len(persistent_store.get_case(case["id"])["jobs"]) == 1


def test_persona_rerun_uses_full_investigation_builder_and_explicit_target(
    client, persistent_store
):
    subject = "Ferdinata Suryanto"
    job_id = persistent_store.create_investigation(
        ["ferdinatasuryanto"],
        {
            "investigation_spec": {
                "processing_mode": "same_subject",
                "subject_label": subject,
            }
        },
    )
    source_job = persistent_store.claim_next("worker:persona-builder-source")
    persistent_store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": ["ferdinatasuryanto"],
            "individual_reports": [],
            "found_count": 0,
        },
    )
    persona_id = persistent_store.get_case(source_job["case_id"])["personas"][0][
        "id"
    ]

    builder = client.get(f"/personas/{persona_id}/investigate")
    body = builder.get_data(as_text=True)
    assert builder.status_code == 200
    assert "Configure this person investigation" in body
    assert f'value="{subject}"' in body
    assert "Cited public-web research" in body
    assert "Case source filters" in body
    assert "Full check" in body
    assert "Pending or uncertain evidence is not silently reused" in body

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "configured-persona-csrf"
    response = client.post(
        f"/personas/{persona_id}/investigate",
        data={
            "csrf_token": "configured-persona-csrf",
            "identifier_type": ["full_name", "social_handle", "email"],
            "identifier_value": [subject, "@ferdinata", "f@example.test"],
            "processing_mode": "same_subject",
            "generate_name_variants": "on",
            "allow_ai_context": "on",
            "enable_github_profile_enrichment": "on",
            "enable_archived_url_evidence": "on",
            "tags": ["social"],
            "mode": "full",
        },
    )

    assert response.status_code == 302
    refresh_job_id = response.location.rsplit("/", 1)[-1]
    refresh_job = persistent_store.get_job(refresh_job_id)
    specification = refresh_job["options"]["investigation_spec"]
    assert refresh_job["kind"] == "refresh"
    assert refresh_job["case_id"] == source_job["case_id"]
    assert refresh_job["options"]["all_sites"] is True
    assert refresh_job["options"]["tags"] == ["social"]
    assert specification["target_persona_id"] == persona_id
    assert specification["subject_label"] == subject
    assert specification["processing_mode"] == "same_subject"
    assert specification["allow_ai_context"] is True
    assert specification["enable_github_profile_enrichment"] is True
    assert specification["enable_archived_url_evidence"] is True
    assert "ferdinata" in refresh_job["usernames"]
    assert "f@example.test" not in refresh_job["usernames"]


def test_persona_rerun_preserves_exact_username_origin(client, persistent_store):
    username = "john.doe"
    job_id = persistent_store.create_investigation(
        [username],
        {
            "investigation_spec": {
                "processing_mode": "independent",
                "subject_label": username,
                "identifiers": [{"type": "username", "value": username}],
            }
        },
    )
    source_job = persistent_store.claim_next("worker:username-rerun-source")
    persistent_store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": [username],
            "individual_reports": [],
            "found_count": 0,
        },
    )
    persona_id = persistent_store.get_case(source_job["case_id"])["personas"][0][
        "id"
    ]

    page = client.get(f"/personas/{persona_id}/investigate").get_data(
        as_text=True
    )
    assert '<option value="username" selected>Username</option>' in page
    assert f'value="{username}"' in page

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "username-rerun-csrf"
    response = client.post(
        f"/personas/{persona_id}/investigate",
        data={
            "csrf_token": "username-rerun-csrf",
            "identifier_type": "username",
            "identifier_value": username,
            "processing_mode": "same_subject",
            "generate_name_variants": "on",
            "mode": "fast",
        },
    )

    refresh = persistent_store.get_job(response.location.rsplit("/", 1)[-1])
    assert refresh["usernames"] == [username]
    assert refresh["options"]["investigation_spec"]["identifiers"] == [
        {"type": "username", "value": username}
    ]


def test_persona_prefill_ignores_another_personas_targeted_refresh(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(
        ["alice", "bob"],
        {
            "investigation_spec": {
                "processing_mode": "independent",
                "subject_label": "alice",
                "identifiers": [
                    {"type": "username", "value": "alice"},
                    {"type": "username", "value": "bob"},
                ],
            }
        },
    )
    source_job = persistent_store.claim_next("worker:prefill-scope-source")
    persistent_store.finish(
        job_id,
        {
            "status": "completed",
            "usernames": ["alice", "bob"],
            "individual_reports": [],
            "found_count": 0,
        },
    )
    personas = {
        persona["display_name"]: persona["id"]
        for persona in persistent_store.get_case(source_job["case_id"])[
            "personas"
        ]
    }
    persistent_store.repeat_persona_investigation(
        personas["alice"],
        ["bob"],
        {
            "investigation_spec": {
                "processing_mode": "same_subject",
                "identifiers": [{"type": "full_name", "value": "bob"}],
            }
        },
    )

    bob_builder = client.get(
        f"/personas/{personas['bob']}/investigate"
    ).get_data(as_text=True)
    assert '<option value="username" selected>Username</option>' in bob_builder
    assert '<option value="full_name" selected>' not in bob_builder


def test_rejected_claim_is_suppressed_from_profile_but_available_for_reversal(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
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
                        "evidence": {"email": "collision@example.test"},
                    }
                ],
            }
        ],
    }
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    email = next(
        claim
        for claim in persistent_store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "email"
    )
    persistent_store.review_claim(email["id"], "rejected", "analyst", "Collision")

    page = client.get(f"/personas/{persona_id}").get_data(as_text=True)
    assert page.count("collision@example.test") == 1
    assert 'data-review-item="rejected"' in page
    assert "excluded from the default profile, map, and relationship graph" in page


def test_approved_location_and_photo_render_in_persona_workspace(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
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
                        "evidence": {
                            "location": "Jakarta, Indonesia",
                            "photo": "https://images.example.test/alice.jpg",
                        },
                    }
                ],
            }
        ],
    }
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    claims = persistent_store.get_persona(persona_id)["claims"]
    location = next(c for c in claims if c["field_name"] == "current_location")
    photo = next(c for c in claims if c["field_name"] == "photograph")
    persistent_store.review_claim(
        location["id"],
        "approved",
        "analyst",
        latitude="-6.1754",
        longitude="106.8272",
    )
    persistent_store.review_claim(photo["id"], "approved", "analyst")

    page = client.get(f"/personas/{persona_id}").get_data(as_text=True)
    assert 'id="personaLocationMap"' in page
    assert "-6.1754" in page
    assert "106.8272" in page
    assert page.count('src="https://images.example.test/alice.jpg"') >= 2
    assert 'class="persona-photo-frame"' in page
    assert "Amend approved record" in page
    assert "AI evidence pipeline" in page
    assert "Affiliations" in page
    assert "Professional and corporate" not in page
    assert "Organization, institution or company" in page
    assert "OpenLedger does not infer a private or residential address" in page


def test_approved_location_requires_saving_ai_map_center_before_mapping(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice"], {})
    persistent_store.claim_next("worker:test")
    result = {
        "status": "completed",
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
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    persona_id = case["personas"][0]["id"]
    location = persistent_store.get_persona(persona_id)["claims"][0]
    persistent_store.review_claim(location["id"], "approved", "analyst")
    persistent_store.sync_ai_persona_claims(
        job_id,
        [
            {
                "username": "alice",
                "field_name": "current_location",
                "value": "Jakarta, Indonesia",
                "confidence": 75,
                "source_url": "https://example.test/alice",
                "source_title": "Profile",
                "reason": "The profile names Jakarta.",
                "latitude": -6.1754,
                "longitude": 106.8272,
                "coordinate_precision": "city",
            }
        ],
        sources=[{"title": "Profile", "url": "https://example.test/alice"}],
        usernames=["alice"],
        model="gpt-5.6-terra",
    )

    page = client.get(f"/personas/{persona_id}").get_data(as_text=True)
    assert "0 mapped" in page
    assert 'value="-6.1754"' in page
    assert 'value="106.8272"' in page
    assert "AI proposed an approximate city map center" in page


def test_relationship_workspace_renders_shared_approved_attributes(
    client, persistent_store
):
    job_id = persistent_store.create_investigation(["alice", "bob"], {})
    persistent_store.claim_next("worker:test")
    result = {
        "status": "completed",
        "usernames": ["alice", "bob"],
        "individual_reports": [
            {
                "username": username,
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": f"https://example.test/{username}",
                        "confidence": "strong",
                        "evidence": {"company": "Nexorus"},
                    }
                ],
            }
            for username in ("alice", "bob")
        ],
    }
    persistent_store.finish(job_id, result)
    persistent_store.sync_persona_claims(job_id, result)
    case = persistent_store.get_case(persistent_store.get_job(job_id)["case_id"])
    for persona_summary in case["personas"]:
        company = next(
            claim
            for claim in persistent_store.get_persona(persona_summary["id"])["claims"]
            if claim["field_name"] == "company"
        )
        persistent_store.review_claim(company["id"], "approved", "analyst")

    page = client.get("/relationships?mode=shared").get_data(as_text=True)
    assert "Cross-Persona relationship leads" in page
    assert "Nexorus" in page
    assert "exact normalized matches across personas" in page
    assert "/static/vendor/vis-network-10.1.1.min.js" in page
    assert "https://unpkg.com/vis-network" not in page

    persona_page = client.get("/relationships?mode=persona").get_data(
        as_text=True
    )
    assert "Persona evidence network" in persona_page
    assert "Example" in persona_page
    assert "Review status remains visible" in persona_page
    assert "/static/vendor/vis-network-10.1.1.min.js" in persona_page
    assert "/static/relationships.js" in persona_page


def test_case_fusion_worker_publishes_versioned_snapshot(web_app, persistent_store):
    source_case_ids = []
    for username in ("alice", "bob"):
        source_job_id = persistent_store.create_investigation([username], {})
        source_job = persistent_store.claim_next(f"worker:{username}")
        persistent_store.finish(
            source_job_id,
            {
                "status": "completed",
                "usernames": [username],
                "individual_reports": [],
            },
        )
        source_case_ids.append(source_job["case_id"])
    fusion_job_id = persistent_store.create_combined_investigation(
        source_case_ids,
        title="Combined worker test",
        purpose="Verify the persistent worker snapshot path.",
        created_by="analyst",
    )
    fusion_job = persistent_store.claim_next("worker:fusion")

    web_app.run_persistent_job(persistent_store, fusion_job)

    completed = persistent_store.get_job(fusion_job_id)
    assert completed["status"] == "completed"
    assert completed["source_case_count"] == 2
    assert len(completed["snapshot"]["sha256"]) == 64
    assert completed["relationship_graph"]["scope"] == ("combined_case_snapshot")
    assert "analysis_context" not in completed
    assert persistent_store.get_events(fusion_job_id)[-1]["event"] == {
        "type": "done",
        "status": "completed",
        "redirect": f"/cases/{fusion_job['case_id']}",
    }
    combined = persistent_store.get_case(fusion_job["case_id"])
    assert combined["analysis_runs"][0]["status"] == "unavailable"


def test_case_fusion_worker_runs_cited_ai_analysis_with_existing_connection(
    client, web_app, persistent_store, monkeypatch
):
    source_case_ids = []
    for username in ("alice", "bob"):
        source_job_id = persistent_store.create_investigation([username], {})
        source_job = persistent_store.claim_next(f"worker:{username}")
        persistent_store.finish(
            source_job_id,
            {
                "status": "completed",
                "usernames": [username],
                "individual_reports": [],
            },
        )
        source_case_ids.append(source_job["case_id"])
    fusion_job_id = persistent_store.create_combined_investigation(
        source_case_ids,
        title="Cited AI worker test",
        purpose="Find a public relationship if the evidence supports one.",
        created_by="analyst",
    )
    fusion_job = persistent_store.claim_next("worker:fusion-ai")
    captured = {}

    async def fake_research(**kwargs):
        captured["research"] = kwargs
        return {
            "analysis": "No defensible connection is established.",
            "sources": [
                {
                    "title": "Public source",
                    "url": "https://example.test/public",
                }
            ],
            "web_search_completed": True,
        }

    async def fake_insights(**kwargs):
        captured["insights"] = kwargs
        return {
            "executive_summary": "No defensible connection is established.",
            "key_findings": [],
            "contradictions": [],
            "information_gaps": ["The source cases have no approved claims."],
            "next_steps": ["Approve relevant evidence before retrying."],
            "proposals": [],
        }

    monkeypatch.setattr(web_app, "get_openai_api_key", lambda: "existing-key")
    monkeypatch.setattr(
        web_app,
        "load_settings",
        lambda: {"openai_model": "gpt-5.6-terra", "ai_web_enrichment": True},
    )
    monkeypatch.setattr(web_app, "get_case_chat_response", fake_research)
    monkeypatch.setattr(
        web_app, "get_combined_investigation_insights", fake_insights
    )

    web_app.run_persistent_job(persistent_store, fusion_job)

    completed = persistent_store.get_job(fusion_job_id)
    assert completed["status"] == "completed"
    assert completed["ai_analysis"]["status"] == "completed"
    assert completed["ai_analysis"]["web_search_completed"] is True
    assert captured["research"]["web_search_enabled"] is True
    assert captured["insights"]["model"] == "gpt-5.6-terra"
    combined = persistent_store.get_case(fusion_job["case_id"])
    analysis = combined["analysis_runs"][0]
    assert analysis["status"] == "completed"
    assert analysis["sources"][0]["url"] == "https://example.test/public"
    assert analysis["information_gaps"] == [
        "The source cases have no approved claims."
    ]
    workspace = client.get(f"/cases/{fusion_job['case_id']}").get_data(as_text=True)
    assert "Why these cases may be connected" in workspace
    assert "No defensible connection is established." in workspace
    assert "Cited public-web research" in workspace
    assert "https://example.test/public" in workspace


def test_case_fusion_worker_honors_cancellation_during_ai_analysis(
    web_app, persistent_store, monkeypatch
):
    source_case_ids = []
    for username in ("alice", "bob"):
        source_job_id = persistent_store.create_investigation([username], {})
        source_job = persistent_store.claim_next(f"worker:{username}")
        persistent_store.finish(
            source_job_id,
            {
                "status": "completed",
                "usernames": [username],
                "individual_reports": [],
            },
        )
        source_case_ids.append(source_job["case_id"])
    fusion_job_id = persistent_store.create_combined_investigation(
        source_case_ids,
        title="Cancelled AI worker test",
        purpose="Do not publish output after cancellation.",
        created_by="analyst",
    )
    fusion_job = persistent_store.claim_next("worker:fusion-cancel")

    async def fake_research(**_kwargs):
        assert persistent_store.request_cancel(fusion_job_id) is True
        return {
            "analysis": "This output must be discarded.",
            "sources": [
                {
                    "title": "Public source",
                    "url": "https://example.test/public",
                }
            ],
            "web_search_completed": True,
        }

    async def forbidden_insights(**_kwargs):
        raise AssertionError("the structured stage must not start after cancellation")

    monkeypatch.setattr(web_app, "get_openai_api_key", lambda: "existing-key")
    monkeypatch.setattr(
        web_app,
        "load_settings",
        lambda: {"openai_model": "gpt-5.6-terra", "ai_web_enrichment": True},
    )
    monkeypatch.setattr(web_app, "get_case_chat_response", fake_research)
    monkeypatch.setattr(
        web_app, "get_combined_investigation_insights", forbidden_insights
    )

    web_app.run_persistent_job(persistent_store, fusion_job)

    completed = persistent_store.get_job(fusion_job_id)
    assert completed["status"] == "cancelled"
    assert "snapshot" not in completed
    analysis = persistent_store.get_case(fusion_job["case_id"])["analysis_runs"][0]
    assert analysis["status"] == "cancelled"
    assert analysis["proposals"] == []


def test_combined_case_selection_and_workspace_flow(client, web_app, persistent_store):
    source_case_ids = []
    for username in ("alice", "bob"):
        source_job_id = persistent_store.create_investigation([username], {})
        source_job = persistent_store.claim_next(f"worker:{username}")
        persistent_store.finish(
            source_job_id,
            {
                "status": "completed",
                "usernames": [username],
                "individual_reports": [],
            },
        )
        source_case_ids.append(source_job["case_id"])

    cases_page = client.get("/cases").get_data(as_text=True)
    assert 'href="/cases/combine"' in cases_page
    selection_page = client.get("/cases/combine").get_data(as_text=True)
    assert "Create one auditable investigation view" in selection_page
    assert "Select source cases" in selection_page
    assert "/static/combine-cases.js" in selection_page

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "combine-csrf"
        browser_session["username"] = "field.analyst"
    response = client.post(
        "/cases/combine",
        data={
            "csrf_token": "combine-csrf",
            "case_id": source_case_ids,
            "title": "Two-case picture",
            "purpose": "Identify exact reviewed evidence shared by these cases.",
        },
    )
    assert response.status_code == 302
    fusion_job_id = response.location.rsplit("/", 1)[-1]
    fusion_job = persistent_store.claim_next("worker:fusion")
    assert fusion_job["job_id"] == fusion_job_id
    live_page = client.get(f"/live/{fusion_job_id}").get_data(as_text=True)
    assert "Combined investigation" in live_page
    assert "Evidence scope" in live_page
    assert "Approved only" in live_page
    web_app.run_persistent_job(persistent_store, fusion_job)

    combined_case = persistent_store.get_case(fusion_job["case_id"])
    workspace = client.get(f"/cases/{combined_case['id']}").get_data(as_text=True)
    assert "Two-case picture" in workspace
    assert "Source evidence remains canonical" in workspace
    assert "Snapshot SHA-256" in workspace
    assert "Open source case" in workspace
    relationships = client.get(
        f"/relationships?mode=shared&case_id={combined_case['id']}"
    ).get_data(as_text=True)
    assert "Versioned combined-case snapshot" in relationships
    source_case = persistent_store.get_case(source_case_ids[0])
    source_job_id = source_case["jobs"][0]["job_id"]
    protected_delete = client.post(
        f"/history/search_{source_job_id}/delete",
        data={
            "csrf_token": "combine-csrf",
            "confirmation_name": source_case["title"],
        },
        follow_redirects=True,
    )
    assert protected_delete.status_code == 200
    assert "retained by a combined investigation" in protected_delete.get_data(
        as_text=True
    )
    assert persistent_store.get_case(source_case_ids[0]) is not None


def test_combined_relationship_review_controls_workspace_and_graph(
    client, persistent_store, monkeypatch
):
    source_case_ids = []
    for username in ("alice", "bob"):
        source_job_id = persistent_store.create_investigation([username], {})
        source_job = persistent_store.claim_next(f"worker:{username}")
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
                            "evidence": {"company": "Nexorus"},
                        }
                    ],
                }
            ],
        }
        persistent_store.finish(source_job_id, result)
        persistent_store.sync_persona_claims(source_job_id, result)
        source_case = persistent_store.get_case(source_job["case_id"])
        persona = persistent_store.get_persona(source_case["personas"][0]["id"])
        company_claim = next(
            claim for claim in persona["claims"] if claim["field_name"] == "company"
        )
        persistent_store.review_claim(company_claim["id"], "approved", "analyst")
        source_case_ids.append(source_job["case_id"])

    fusion_job_id = persistent_store.create_combined_investigation(
        source_case_ids,
        title="Governed AI graph",
        purpose="Review an AI-proposed shared affiliation.",
        created_by="analyst",
    )
    fusion_job = persistent_store.claim_next("worker:fusion-review")
    snapshot = persistent_store.build_case_fusion_snapshot(fusion_job_id)
    context = snapshot.pop("analysis_context")
    persona_entities = {
        entity["case_id"]: entity
        for entity in context["entities"]
        if entity["entity_type"] == "persona"
    }
    proposal_evidence = [
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
    run_id = persistent_store.start_combined_analysis_run(
        fusion_job_id,
        snapshot["snapshot"]["sha256"],
        model="gpt-5.6-terra",
        web_search_enabled=True,
    )
    persistent_store.complete_combined_analysis_run(
        run_id,
        {
            "executive_summary": "The two approved affiliations may be connected.",
            "key_findings": [],
            "contradictions": [],
            "information_gaps": ["Coordination is not established."],
            "next_steps": ["Review the official staff directory."],
            "sources": [
                {
                    "reference_id": "web:1",
                    "reference_type": "public_web",
                    "title": "Nexorus staff",
                    "url": "https://example.test/staff",
                }
            ],
            "proposals": [
                {
                    "title": "Shared affiliation",
                    "relationship_type": "affiliation",
                    "subject_ref": persona_entities[source_case_ids[0]][
                        "reference_id"
                    ],
                    "subject_entity": persona_entities[source_case_ids[0]],
                    "object_ref": persona_entities[source_case_ids[1]][
                        "reference_id"
                    ],
                    "object_entity": persona_entities[source_case_ids[1]],
                    "explanation": "Both reviewed claims name Nexorus.",
                    "confidence": 70,
                    "evidence": proposal_evidence,
                    "contradictory_evidence": [],
                    "limitations": ["Shared affiliation is not proof of contact."],
                }
            ],
        },
    )
    persistent_store.finish(
        fusion_job_id,
        {"status": "completed", "kind": "case_fusion", **snapshot},
    )
    combined_case_id = fusion_job["case_id"]
    proposal_id = persistent_store.get_case(combined_case_id)["analysis_runs"][0][
        "proposals"
    ][0]["id"]

    workspace = client.get(f"/cases/{combined_case_id}").get_data(as_text=True)
    assert "Shared affiliation" in workspace
    assert "Both reviewed claims name Nexorus." in workspace
    assert "Approve relationship" in workspace
    assert "Ask AI" in workspace
    assert "https://example.test/alice" in workspace

    monkeypatch.setenv("OPENAI_API_KEY", "server-only-test-key")

    async def combined_chat_response(**kwargs):
        assert kwargs["case_context"]["scope"] == "combined_investigation"
        assert kwargs["case_context"]["snapshot_current"] is True
        assert kwargs["case_context"]["latest_ai_assessment"][
            "executive_summary"
        ].startswith("The two approved")
        assert kwargs["web_search_enabled"] is True
        return {
            "analysis": "The matching affiliation is a lead, not proof of contact.",
            "sources": [
                {
                    "title": "Independent directory",
                    "url": "https://example.test/directory",
                }
            ],
        }

    async def combined_chat_proposals(**kwargs):
        assert kwargs["research_answer"].startswith("The matching affiliation")
        return {
            "executive_summary": "Chat follow-up.",
            "key_findings": [],
            "contradictions": [],
            "information_gaps": [],
            "next_steps": [],
            "proposals": [
                {
                    "title": "Follow-up shared affiliation",
                    "relationship_type": "affiliation",
                    "subject_ref": persona_entities[source_case_ids[0]]["reference_id"],
                    "object_ref": persona_entities[source_case_ids[1]]["reference_id"],
                    "explanation": "Both approved records name Nexorus.",
                    "confidence": 65,
                    "evidence_reference_ids": [
                        claim["reference_id"] for claim in context["approved_claims"]
                    ],
                    "contradictory_reference_ids": [],
                    "limitations": ["This does not prove direct contact."],
                }
            ],
        }

    monkeypatch.setattr(
        web_app_module, "get_combined_case_chat_response", combined_chat_response
    )
    monkeypatch.setattr(
        web_app_module,
        "get_combined_investigation_insights",
        combined_chat_proposals,
    )

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "relationship-review-csrf"
        browser_session["username"] = "relationship.reviewer"
    chat_page = client.get(f"/cases/{combined_case_id}/chat").get_data(as_text=True)
    assert "Persistent cross-case assistant" in chat_page
    assert "Propose supported relationships" in chat_page
    chat_response = client.post(
        f"/api/cases/{combined_case_id}/chat",
        headers={"X-OpenLedger-CSRF": "relationship-review-csrf"},
        json={
            "message": "Explain the proposed link and test it against public sources.",
            "research_enabled": True,
            "propose_relationships": True,
        },
    )
    assert chat_response.status_code == 200
    chat_payload = chat_response.get_json()
    assert chat_payload["proposal_summary"]["status"] == "pending_review"
    assert chat_payload["proposal_summary"]["kind"] == "relationship"
    assert chat_payload["proposal_summary"]["count"] == 1
    chat_proposal_id = chat_payload["proposal_summary"]["proposal_ids"][0]
    retained_chat_proposal = next(
        item
        for item in persistent_store.get_case(combined_case_id)["analysis_runs"][0][
            "proposals"
        ]
        if item["id"] == chat_proposal_id
    )
    assert retained_chat_proposal["chat_message_id"] == (
        chat_payload["assistant_message"]["id"]
    )

    response = client.post(
        f"/cases/{combined_case_id}/relationships/{proposal_id}/review",
        data={
            "csrf_token": "relationship-review-csrf",
            "decision": "approved",
            "note": "The approved evidence anchors both source cases.",
        },
        follow_redirects=True,
    )
    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Relationship approved and added to the combined graph" in body
    assert "Analyst-approved AI link" in body
    assert f"ai-proposal:{proposal_id}" in body
    assert response.request.path == "/relationships"
    assert response.request.args["proposal_id"] == proposal_id

    graph = client.get(
        f"/relationships?mode=shared&case_id={combined_case_id}"
    ).get_data(as_text=True)
    assert "Analyst-approved AI link" in graph
    assert f"ai-proposal:{proposal_id}" in graph
    assert '"review_status": "approved"' in graph

    client.post(
        f"/cases/{combined_case_id}/relationships/{proposal_id}/review",
        data={
            "csrf_token": "relationship-review-csrf",
            "decision": "rejected",
            "note": "Alternative explanation is more likely.",
        },
    )
    rejected_graph = client.get(
        f"/relationships?mode=shared&case_id={combined_case_id}"
    ).get_data(as_text=True)
    assert f"ai-proposal:{proposal_id}" not in rejected_graph
    retained = persistent_store.get_case(combined_case_id)["analysis_runs"][0][
        "proposals"
    ][0]
    assert retained["review_status"] == "rejected"
    assert len(retained["reviews"]) == 2


def _affiliation_worker_observation():
    return {
        "source_engine": "wikidata_affiliation",
        "source_record_id": "wikidata-organization:Q95",
        "status": "observed",
        "reason": "Explicit public statements.",
        "organization_candidates": [],
        "organization": {
            "id": "Q95",
            "label": "Example Organization",
            "description": "Example",
            "url": "https://www.wikidata.org/wiki/Q95",
            "official_websites": ["https://example.org"],
        },
        "people": [
            {
                "id": "Q1001",
                "label": "Alice Example",
                "url": "https://www.wikidata.org/wiki/Q1001",
                "relations": [
                    {
                        "property_id": "P108",
                        "label": "employer",
                        "direction": "person_to_organization",
                    }
                ],
            }
        ],
    }


def _official_website_worker_observation(*, linked_profiles=None):
    return {
        "source_engine": "official_website_public_content",
        "source_record_id": "official-website:example-org",
        "status": "observed",
        "source_url": "https://example.org",
        "reason": "Bounded public content was collected.",
        "organization": {
            "name": "Example Organization",
            "domain": "example.org",
            "website_url": "https://example.org",
            "page_title": "Example Organization",
            "description": "Public organization description.",
        },
        "addresses": [],
        "contacts": [],
        "people": [],
        "linked_company_profiles": list(linked_profiles or []),
        "extra": {
            "human_review_required": True,
            "automatic_approval_allowed": False,
            "self_published_source": True,
        },
    }


def test_affiliation_worker_persists_pending_people(
    web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation("Example Organization")
    job = persistent_store.claim_next("worker:affiliation")

    async def fake_discovery(*_args, **_kwargs):
        return _affiliation_worker_observation()

    monkeypatch.setattr(web_app, "run_wikidata_affiliation_discovery", fake_discovery)
    web_app.run_persistent_job(persistent_store, job)
    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["affiliated_person_count"] == 1
    persona = persistent_store.get_persona(
        persistent_store.get_case(job["case_id"])["personas"][0]["id"]
    )
    assert all(claim["review_status"] == "pending" for claim in persona["claims"])
    assert (
        persistent_store.get_events(job_id)[-1]["event"]["redirect"]
        == f"/cases/{job['case_id']}"
    )


def test_affiliation_worker_retains_entity_when_people_lookup_is_partial(
    web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation("Example Organization")
    job = persistent_store.claim_next("worker:affiliation-partial")
    observation = _affiliation_worker_observation()
    observation.update(
        {
            "status": "partial",
            "reason": (
                "The organization resolved, but the bounded Wikidata affiliation "
                "lookup timed out. No zero-result conclusion was recorded."
            ),
            "people": [],
        }
    )

    async def fake_discovery(*_args, **_kwargs):
        return observation

    monkeypatch.setattr(web_app, "run_wikidata_affiliation_discovery", fake_discovery)
    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["affiliation_status"] == "partial"
    assert completed["organization"]["id"] == "Q95"
    assert completed["affiliated_person_count"] == 0
    assert persistent_store.get_case(job["case_id"])["personas"] == []
    events = [item["event"] for item in persistent_store.get_events(job_id)]
    assert any(
        event.get("type") == "affiliation_entity"
        and event.get("entity_id") == "Q95"
        for event in events
    )
    source_event = next(
        event
        for event in events
        if event.get("collector") == "wikidata-affiliation"
        and event.get("type") == "collector_error"
    )
    assert "No zero-result conclusion" in source_event["message"]


def test_affiliation_domain_context_is_observation_only_and_explains_limits(
    client, web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation(
        "Example Organization", enable_domain_context=True
    )
    job = persistent_store.claim_next("worker:domain-context")

    async def fake_discovery(*_args, **_kwargs):
        return _affiliation_worker_observation()

    async def fake_dns(website, *_args, **_kwargs):
        assert website["domain"] == "example.org"
        return {
            "source_engine": "cloudflare_dns_context",
            "source_url": "https://cloudflare-dns.com/dns-query",
            "status": "observed",
            "reason": "Current public DNS records.",
            "domain": "example.org",
            "website_url": "https://example.org",
            "registration_lookup_url": (
                "https://lookup.icann.org/en/lookup?name=example.org"
            ),
            "records": {
                "a": [{"value": "93.184.216.34", "ttl": 300}],
                "aaaa": [],
                "mx": [{"value": "mail.example.org", "priority": 10, "ttl": 300}],
                "ns": [{"value": "ns1.example.org", "ttl": 300}],
            },
            "record_count": 3,
        }

    async def fake_website(*_args, **_kwargs):
        return _official_website_worker_observation()

    monkeypatch.setattr(web_app, "run_wikidata_affiliation_discovery", fake_discovery)
    monkeypatch.setattr(web_app, "run_cloudflare_dns_context", fake_dns)
    monkeypatch.setattr(
        web_app, "run_official_website_public_content", fake_website
    )
    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["website_context_source"] == "wikidata_official_website"
    assert completed["dns_observation"]["record_count"] == 3
    assert any(
        finding["category"] == "technical_domain_context"
        and "do not establish" in finding["limitation"]
        for finding in completed["business_context_findings"]
    )
    case_page = client.get(f"/cases/{job['case_id']}").get_data(as_text=True)
    assert "Business context audit" in case_page
    assert "DNS and registration metadata describe technical administration" in case_page
    assert "Research operating context" in case_page

    chat_page = client.get(
        f"/cases/{job['case_id']}/chat?mode=business_context"
    ).get_data(as_text=True)
    assert "Research the public operating context" in chat_page
    assert 'data-initial-research="true"' in chat_page
    assert "Do not infer where the business operates from DNS" in chat_page


def test_dns_context_failure_does_not_discard_affiliation_people(
    web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation(
        "Example Organization",
        enable_domain_context=True,
        official_website="https://example.org",
    )
    job = persistent_store.claim_next("worker:dns-failure")

    async def fake_discovery(*_args, **_kwargs):
        return _affiliation_worker_observation()

    async def failed_dns(*_args, **_kwargs):
        raise RuntimeError("private DNS upstream diagnostic")

    async def fake_website(*_args, **_kwargs):
        return _official_website_worker_observation()

    monkeypatch.setattr(web_app, "run_wikidata_affiliation_discovery", fake_discovery)
    monkeypatch.setattr(web_app, "run_cloudflare_dns_context", failed_dns)
    monkeypatch.setattr(
        web_app, "run_official_website_public_content", fake_website
    )
    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["affiliation_status"] == "partial"
    assert completed["affiliated_person_count"] == 1
    assert completed["dns_observation"]["status"] == "unavailable"
    assert "private DNS upstream diagnostic" not in json.dumps(completed)
    assert len(persistent_store.get_case(job["case_id"])["personas"]) == 1


def test_supplied_website_evidence_survives_wrong_wikidata_candidate(
    client, web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation(
        "Unistellar",
        jurisdiction="ID",
        enable_domain_context=True,
        official_website="https://unistellar.co",
    )
    job = persistent_store.claim_next("worker:official-website")

    async def wrong_wikidata(*_args, **_kwargs):
        return {
            "source_engine": "wikidata_affiliation",
            "status": "needs_selection",
            "reason": "The exact-name Wikidata candidate has a different website.",
            "organization_candidates": [
                {
                    "id": "Q65073466",
                    "label": "Unistellar",
                    "description": "French telescope company",
                    "url": "https://www.wikidata.org/wiki/Q65073466",
                    "official_websites": ["https://unistellaroptics.com"],
                }
            ],
            "organization": None,
            "people": [],
        }

    async def no_registry_match(*_args, **_kwargs):
        return {
            "source_engine": "gleif_lei_registry",
            "status": "not_found",
            "reason": "No jurisdiction-matched LEI record.",
            "candidates": [],
            "selected_entity": None,
        }

    async def no_dns_records(*_args, **_kwargs):
        return {
            "source_engine": "cloudflare_dns_context",
            "status": "observed",
            "reason": "Current public DNS records.",
            "domain": "unistellar.co",
            "records": {},
            "record_count": 0,
        }

    async def official_website(*_args, **_kwargs):
        observation = _official_website_worker_observation(
            linked_profiles=["https://www.linkedin.com/company/unistellar"]
        )
        observation.update(
            {
                "source_record_id": "official-website:unistellar",
                "source_url": "https://www.unistellar.co/",
                "organization": {
                    "name": "Unistellar",
                    "domain": "unistellar.co",
                    "website_url": "https://www.unistellar.co/",
                    "page_title": "Unistellar Business Group",
                    "description": "A group of companies in advisory and investment.",
                },
                "contacts": [
                    {"type": "email", "value": "corporate@unistellar.co"}
                ],
                "people": [
                    {"display_name": "Pascal Sembel", "role": "Finance & Investment"},
                    {
                        "display_name": "Ferdinata Suryanto",
                        "role": "Corporate Finance & Investment",
                    },
                ],
            }
        )
        return observation

    monkeypatch.setattr(web_app, "run_wikidata_affiliation_discovery", wrong_wikidata)
    monkeypatch.setattr(web_app, "run_gleif_legal_entity_search", no_registry_match)
    monkeypatch.setattr(web_app, "run_cloudflare_dns_context", no_dns_records)
    monkeypatch.setattr(
        web_app, "run_official_website_public_content", official_website
    )
    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["affiliation_status"] == "partial"
    assert completed["website_address_count"] == 0
    assert completed["affiliated_person_count"] == 2
    resolution_candidates = completed["organization_resolution_candidates"]
    website_candidate = next(
        candidate
        for candidate in resolution_candidates
        if candidate["source_engine"] == "official_website_public_content"
    )
    wikidata_candidate = next(
        candidate
        for candidate in resolution_candidates
        if candidate["source_engine"] == "wikidata_affiliation"
    )
    assert website_candidate["candidate_key"] == (
        "official_website_public_content:unistellar.co"
    )
    assert website_candidate["selectable"] is True
    assert wikidata_candidate["selectable"] is False
    assert any(
        finding["category"] == "linked_company_profile_lead"
        and finding["source_url"]
        == "https://www.linkedin.com/company/unistellar"
        and "did not fetch or copy" in finding["limitation"]
        for finding in completed["business_context_findings"]
    )
    personas = persistent_store.get_case(job["case_id"])["personas"]
    assert {persona["display_name"] for persona in personas} == {
        "Pascal Sembel",
        "Ferdinata Suryanto",
    }
    for persona_summary in personas:
        persona = persistent_store.get_persona(persona_summary["id"])
        assert all(claim["review_status"] == "pending" for claim in persona["claims"])

    case_page = client.get(f"/cases/{job['case_id']}").get_data(as_text=True)
    assert "First-party website evidence collected independently" in case_page
    assert "No organization-published address was present" in case_page
    assert "Open profile for manual review" in case_page
    assert "Pascal Sembel" in case_page
    assert "Source-neutral resolution" in case_page
    assert "Confirm operating identity" in case_page
    assert "Not verified as an organization" in case_page
    assert "leave Wikidata unselected" in case_page

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "organization-selection-csrf"
    selected_response = client.post(
        f"/cases/{job['case_id']}/affiliation/select",
        data={
            "csrf_token": "organization-selection-csrf",
            "candidate_key": website_candidate["candidate_key"],
        },
        follow_redirects=True,
    )
    assert selected_response.status_code == 200
    assert "Case organization confirmed" in selected_response.get_data(
        as_text=True
    )
    selected_job = persistent_store.get_job(job_id)
    assert selected_job["selected_organization"]["source_engine"] == (
        "official_website_public_content"
    )
    assert selected_job["selected_organization"]["reviewed_by"] == (
        "local-operator"
    )
    assert len(persistent_store.get_case(job["case_id"])["jobs"]) == 1


def test_affiliation_job_retains_cited_linkedin_and_map_observations_without_scraping(
    client, web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation(
        "Unistellar",
        enable_public_web_research=True,
        official_website="https://www.unistellar.co/",
    )
    job = persistent_store.claim_next("worker:public-web-organization")
    linkedin_url = "https://www.linkedin.com/company/unistellar/"
    maps_url = (
        "https://www.google.com/maps/place/Unistellar/"
        "@-6.2585928,106.8205345,980m/data=!3m1!1e3"
    )

    async def no_wikidata_match(*_args, **_kwargs):
        return {
            "source_engine": "wikidata_affiliation",
            "status": "not_found",
            "reason": "No suitable knowledge entity.",
            "organization_candidates": [],
            "organization": None,
            "people": [],
        }

    async def no_dns_records(*_args, **_kwargs):
        return {
            "source_engine": "cloudflare_dns_context",
            "status": "observed",
            "reason": "Current public DNS records.",
            "domain": "unistellar.co",
            "records": {},
            "record_count": 0,
        }

    async def official_website_without_address(*_args, **_kwargs):
        return _official_website_worker_observation(
            linked_profiles=["https://www.linkedin.com/company/unistellar"]
        )

    async def cited_research(**kwargs):
        assert kwargs["web_search_enabled"] is True
        assert "LinkedIn" in kwargs["user_message"]
        assert "Google Maps" in kwargs["user_message"]
        return {
            "analysis": (
                "The cited LinkedIn company page publishes the Jakarta business "
                "address, and the cited map listing points to the same organization. "
                "Home address: 8 Private Road belongs to an employee."
            ),
            "sources": [
                {"title": "Unistellar | LinkedIn", "url": linkedin_url},
                {"title": "Unistellar - Google Maps", "url": maps_url},
            ],
            "web_search_completed": True,
        }

    async def organization_proposals(**kwargs):
        assert kwargs["sources"][0]["url"] == linkedin_url
        return [
            {
                "observation_type": "business_address",
                "value": "Jl Kemang Timur No. 28, Jakarta 12730, ID",
                "source_url": linkedin_url,
                "source_title": "Unistellar | LinkedIn",
                "source_role": "professional_profile",
                "identity_match_basis": "exact_name_and_official_website",
                "reason": (
                    "The company page uses the exact name, links to unistellar.co, "
                    "and publishes this primary business address."
                ),
                "confidence": 80,
                "latitude": None,
                "longitude": None,
            },
            {
                "observation_type": "business_address",
                "value": "Jl. Kemang Timur No.28, Jakarta 12730, Indonesia",
                "source_url": maps_url,
                "source_title": "Unistellar - Google Maps",
                "source_role": "map_listing",
                "identity_match_basis": "exact_name_and_location",
                "reason": "The cited map listing publishes this business address.",
                "confidence": 75,
                "latitude": -6.2585928,
                "longitude": 106.8231094,
            },
        ]

    monkeypatch.setattr(web_app, "get_openai_api_key", lambda: "existing-key")
    monkeypatch.setattr(
        web_app,
        "load_settings",
        lambda: {"openai_model": "gpt-5.6-terra", "ai_web_enrichment": True},
    )
    monkeypatch.setattr(
        web_app, "run_wikidata_affiliation_discovery", no_wikidata_match
    )
    monkeypatch.setattr(web_app, "run_cloudflare_dns_context", no_dns_records)
    monkeypatch.setattr(
        web_app,
        "run_official_website_public_content",
        official_website_without_address,
    )
    monkeypatch.setattr(web_app, "get_case_chat_response", cited_research)
    monkeypatch.setattr(
        web_app, "get_organization_context_proposals", organization_proposals
    )

    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["public_web_research"]["status"] == "observed"
    assert completed["public_web_research"]["analysis"] == ""
    assert "8 Private Road" not in json.dumps(completed)
    assert completed["public_web_finding_count"] == 2
    findings = completed["public_web_research"]["findings"]
    assert {finding["source_role"] for finding in findings} == {
        "professional_profile",
        "map_listing",
    }
    assert all(finding["review_status"] == "pending" for finding in findings)
    assert all(
        finding["direct_platform_fetch_performed"] is False
        for finding in findings
    )
    assert persistent_store.get_case(job["case_id"])["personas"] == []

    page = client.get(f"/cases/{job['case_id']}").get_data(as_text=True)
    assert "Cited external company-profile research" in page
    assert "Jl Kemang Timur No. 28, Jakarta 12730, ID" in page
    assert linkedin_url in page
    assert maps_url.replace("&", "&amp;") in page
    assert "did not send direct scraping requests" in page


def test_cited_research_extraction_failure_is_partial_not_an_evidence_gap(
    web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation(
        "Unistellar", enable_public_web_research=True
    )
    job = persistent_store.claim_next("worker:public-web-extraction-failure")

    async def no_wikidata_match(*_args, **_kwargs):
        return {
            "source_engine": "wikidata_affiliation",
            "status": "not_found",
            "reason": "No suitable knowledge entity.",
            "organization_candidates": [],
            "organization": None,
            "people": [],
        }

    async def cited_research(**_kwargs):
        return {
            "analysis": (
                "A cited source may describe a Jakarta business location. "
                "Employee personal data: alice@example.test."
            ),
            "sources": [
                {
                    "title": "Unistellar company profile",
                    "url": "https://example.org/unistellar",
                }
            ],
            "web_search_completed": True,
        }

    async def failed_extraction(**_kwargs):
        raise RuntimeError("private structured extraction diagnostic")

    monkeypatch.setattr(web_app, "get_openai_api_key", lambda: "existing-key")
    monkeypatch.setattr(
        web_app,
        "load_settings",
        lambda: {"openai_model": "gpt-5.6-terra", "ai_web_enrichment": True},
    )
    monkeypatch.setattr(
        web_app, "run_wikidata_affiliation_discovery", no_wikidata_match
    )
    monkeypatch.setattr(web_app, "get_case_chat_response", cited_research)
    monkeypatch.setattr(
        web_app, "get_organization_context_proposals", failed_extraction
    )

    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    research = completed["public_web_research"]
    assert research["status"] == "partial"
    assert research["analysis"] == ""
    assert research["findings"] == []
    assert "without recording a zero-result conclusion" in research["reason"]
    assert "unvalidated model narrative was discarded" in research["reason"]
    assert "alice@example.test" not in json.dumps(completed)
    assert "private structured extraction diagnostic" not in json.dumps(completed)
    events = [item["event"] for item in persistent_store.get_events(job_id)]
    assert any(
        event.get("collector") == "cited-public-web-organization-research"
        and event.get("type") == "collector_error"
        for event in events
    )


def test_google_places_job_requires_explicit_rate_limited_live_details_action(
    client, web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation(
        "Unistellar", enable_google_places_search=True
    )
    job = persistent_store.claim_next("worker:google-places")
    place_id = "ChIJCzjlUUSv4S4RCiu9uL4NlvE"

    async def no_wikidata_match(*_args, **_kwargs):
        return {
            "source_engine": "wikidata_affiliation",
            "status": "not_found",
            "reason": "No suitable knowledge entity.",
            "organization_candidates": [],
            "organization": None,
            "people": [],
        }

    async def google_search(*_args, **_kwargs):
        return {
            "source_engine": "google_places_business_search",
            "status": "observed",
            "reason": "One bounded Google Places lead.",
            "subject_value": "Unistellar",
            "candidates": [
                {
                    "place_id": place_id,
                    "source_url": "https://www.google.com/maps/search/?api=1",
                    "review_status": "pending",
                    "automatic_approval_allowed": False,
                    "durable_google_content_stored": False,
                }
            ],
            "candidate_count": 1,
            "durable_google_content_stored": False,
        }

    live_detail_calls = []

    async def google_live_details(organization_name, place_ids, api_key):
        assert organization_name == "Unistellar"
        assert place_ids == [place_id]
        assert api_key == "restricted-server-key"
        live_detail_calls.append(place_ids)
        return {
            "status": "observed",
            "reason": "Live Google Maps business details.",
            "places": [
                {
                    "place_id": place_id,
                    "display_name": "Unistellar",
                    "formatted_address": (
                        "Jl. Kemang Timur No. 28, Jakarta 12730, Indonesia"
                    ),
                    "business_status": "OPERATIONAL",
                    "identity_match": "exact_name",
                    "source_url": "https://maps.google.com/?cid=12345",
                    "review_status": "pending",
                    "limitation": "Live research lead only.",
                }
            ],
            "attribution": "Google Maps",
            "durable_google_content_stored": False,
        }

    monkeypatch.setattr(
        web_app, "run_wikidata_affiliation_discovery", no_wikidata_match
    )
    monkeypatch.setattr(
        web_app, "run_google_places_business_search", google_search
    )
    monkeypatch.setattr(
        web_app, "run_google_places_live_details", google_live_details
    )
    monkeypatch.setattr(
        web_app, "get_google_maps_api_key", lambda: "restricted-server-key"
    )

    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    assert completed["google_places_candidate_count"] == 1
    assert completed["google_places_search"]["candidates"][0]["place_id"] == place_id
    assert "Jl. Kemang Timur" not in json.dumps(completed)
    assert persistent_store.get_case(job["case_id"])["personas"] == []
    events = [item["event"] for item in persistent_store.get_events(job_id)]
    assert any(
        event.get("collector") == "google-places-business-search"
        and event.get("type") == "collector_completed"
        for event in events
    )

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "places-live-csrf"

    page = client.get(f"/cases/{job['case_id']}").get_data(as_text=True)
    assert "Google Places business leads" in page
    assert "Jl. Kemang Timur No. 28" not in page
    assert "Load live Google details" in page
    assert live_detail_calls == []

    loaded = client.post(
        f"/cases/{job['case_id']}/google-places-live",
        data={"csrf_token": "places-live-csrf"},
    )
    assert loaded.status_code == 200
    loaded_page = loaded.get_data(as_text=True)
    assert "Jl. Kemang Timur No. 28" in loaded_page
    assert live_detail_calls == [[place_id]]

    repeated = client.post(
        f"/cases/{job['case_id']}/google-places-live",
        data={"csrf_token": "places-live-csrf"},
    )
    assert repeated.status_code == 429
    assert "Wait one minute" in repeated.get_data(as_text=True)
    assert live_detail_calls == [[place_id]]
    assert "durably retains only Place IDs" in page
    assert "never becomes a Persona address" in page


def test_google_places_live_display_does_not_reuse_older_job_candidates(
    web_app, monkeypatch
):
    called = False

    async def google_live_details(*_args, **_kwargs):
        nonlocal called
        called = True
        return {"status": "observed", "places": [{"formatted_address": "stale"}]}

    monkeypatch.setattr(
        web_app, "run_google_places_live_details", google_live_details
    )
    monkeypatch.setattr(
        web_app, "get_google_maps_api_key", lambda: "restricted-server-key"
    )
    case = {
        "jobs": [
            {
                "id": "latest",
                "kind": "affiliation",
                "google_places_search": {
                    "status": "not_found",
                    "candidates": [],
                },
            },
            {
                "id": "older",
                "kind": "affiliation",
                "google_places_search": {
                    "status": "observed",
                    "subject_value": "Unistellar",
                    "candidates": [{"place_id": "ChIJCzjlUUSv4S4RCiu9uL4NlvE"}],
                },
            },
        ]
    }

    result = web_app.load_case_google_places_live(case)

    assert result["status"] == "not_run"
    assert result["places"] == []
    assert called is False


def test_history_describes_investigation_type_target_and_context(
    client, persistent_store
):
    persistent_store.create_affiliation_investigation(
        "Unistellar",
        jurisdiction="ID",
        official_website="https://www.unistellar.co/",
    )

    page = client.get("/history").get_data(as_text=True)

    assert "<th>Investigation</th>" in page
    assert "<th>Findings</th>" in page
    assert "<th>Usernames</th>" not in page
    assert "Organization affiliation" in page
    assert "Unistellar" in page
    assert "Indonesia · ID" in page
    assert "unistellar.co" in page


def test_history_counts_identity_enrichment_proposal_fields(web_app):
    context = web_app.build_investigation_history_context(
        {
            "kind": "identity_enrichment",
            "status": "completed",
            "options": {
                "investigation_spec": {"confirmed_name": "Alice Example"}
            },
            "wikipedia_claim_count": 2,
            "offshore_alert_count": 1,
        }
    )

    assert context["finding_summary"] == "3 claim proposals"


def test_jurisdiction_registry_survives_wikidata_failure_and_proposes_people(
    client, web_app, persistent_store, monkeypatch
):
    job_id = persistent_store.create_affiliation_investigation(
        "Unistellar", jurisdiction="FR"
    )
    job = persistent_store.claim_next("worker:jurisdiction")

    async def failed_wikidata(*_args, **_kwargs):
        raise RuntimeError("private upstream diagnostic")

    async def empty_gleif(*_args, **_kwargs):
        return {
            "source_engine": "gleif_lei_registry",
            "status": "not_found",
            "reason": (
                "GLEIF returned no jurisdiction-matched LEI record. This does "
                "not prove that the entity is not registered."
            ),
            "candidates": [],
            "selected_entity": None,
        }

    async def france_registry(*_args, **_kwargs):
        entity = {
            "id": "812339356",
            "identifier_type": "siren",
            "legal_name": "UNISTELLAR",
            "legal_jurisdiction": "FR",
            "jurisdiction_label": "France",
            "headquarters_identifier": "81233935600030",
            "entity_status": "active",
            "last_update_date": "2026-08-01T00:00:00Z",
            "legal_address": {
                "lines": ["5 AVENUE DU GENERAL LECLERC"],
                "city": "Marseille",
                "region": "93",
                "country": "FR",
                "postal_code": "13003",
            },
            "people": [
                {
                    "display_name": "Arnaud Malvache",
                    "role": "Président de SAS",
                },
                {
                    "display_name": "Laurent Marfisi",
                    "role": "Directeur Général",
                },
            ],
            "exact_name_match": True,
            "source_url": (
                "https://annuaire-entreprises.data.gouv.fr/entreprise/812339356"
            ),
        }
        return {
            "source_engine": "fr_company_registry",
            "status": "observed",
            "reason": "Public records from the French National Enterprise Directory.",
            "candidates": [entity],
            "selected_entity": entity,
        }

    monkeypatch.setattr(
        web_app, "run_wikidata_affiliation_discovery", failed_wikidata
    )
    monkeypatch.setattr(web_app, "run_gleif_legal_entity_search", empty_gleif)
    monkeypatch.setattr(
        web_app, "run_fr_business_registry_search", france_registry
    )

    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["affiliation_status"] == "partial"
    assert completed["registry_candidate_count"] == 1
    assert completed["affiliated_person_count"] == 2
    assert completed["claim_proposal_count"] == 6
    assert "private upstream diagnostic" not in json.dumps(completed)
    case = persistent_store.get_case(job["case_id"])
    assert len(case["personas"]) == 2
    for persona_summary in case["personas"]:
        persona = persistent_store.get_persona(persona_summary["id"])
        assert all(
            claim["review_status"] == "pending" for claim in persona["claims"]
        )

    page = client.get(f"/cases/{job['case_id']}").get_data(as_text=True)
    assert "French National Enterprise Directory" in page
    assert "SIREN 812339356" in page
    assert "GLEIF covers entities issued a Legal Entity Identifier" in page
    assert "automatically approved" in page


@pytest.mark.parametrize("discovery_status", ["rate_limited", "partial"])
def test_affiliation_source_failure_is_not_rendered_as_zero_people(
    client, persistent_store, discovery_status
):
    job_id = persistent_store.create_affiliation_investigation(
        "Example Organization"
    )
    job = persistent_store.claim_next("worker:affiliation")
    persistent_store.finish(
        job_id,
        {
            "status": "completed",
            "discovery_status": discovery_status,
            "source_message": (
                "The organization resolved, but affiliation relations were unavailable."
            ),
            "organization_candidates": [
                {"id": "Q95", "label": "Example Organization"}
            ],
            "organization": {
                "id": "Q95",
                "label": "Example Organization",
                "description": "Example",
                "url": "https://www.wikidata.org/wiki/Q95",
                "official_websites": ["https://example.org"],
            },
            "affiliated_person_count": 0,
            "claim_proposal_count": 0,
        },
    )

    page = client.get(f"/cases/{job['case_id']}").get_data(as_text=True)
    assert "Affiliation source check incomplete" in page
    assert "No zero-result conclusion was recorded" in page
    assert "Retry affiliation lookup" in page
    assert "people observed" not in page


def test_approved_affiliation_opens_a_separate_case(client, persistent_store):
    source_job_id = persistent_store.create_investigation(["alice"], {})
    source_job = persistent_store.claim_next("worker:source")
    result = {"status": "completed", "usernames": ["alice"], "individual_reports": [{"username": "alice", "claimed_profiles": [{"site_name": "Example", "url": "https://example.test/alice", "confidence": "strong", "evidence": {"company": "Example Organization"}}]}]}
    persistent_store.finish(source_job_id, result)
    persistent_store.sync_persona_claims(source_job_id, result)
    source_case = persistent_store.get_case(source_job["case_id"])
    persona = persistent_store.get_persona(source_case["personas"][0]["id"])
    company = next(claim for claim in persona["claims"] if claim["field_name"] == "company")
    assert "Open affiliation case" not in client.get(f"/personas/{persona['id']}").get_data(as_text=True)
    persistent_store.review_claim(company["id"], "approved", "analyst")
    assert "Open affiliation case" in client.get(f"/personas/{persona['id']}").get_data(as_text=True)
    with client.session_transaction() as session:
        session["csrf_token"] = "affiliation-csrf"
    response = client.post(
        f"/claims/{company['id']}/investigate-affiliation",
        data={
            "csrf_token": "affiliation-csrf",
            "jurisdiction": "France",
            "enable_domain_context": "1",
            "official_website": "https://example.org",
        },
    )
    job_id = response.location.rsplit("/", 1)[-1]
    affiliation_job = persistent_store.get_job(job_id)
    assert affiliation_job["kind"] == "affiliation"
    assert affiliation_job["options"]["investigation_spec"][
        "legal_jurisdiction"
    ]["code"] == "FR"
    assert affiliation_job["options"]["investigation_spec"][
        "official_website"
    ]["domain"] == "example.org"
    assert persistent_store.get_case(affiliation_job["case_id"])["personas"] == []


def test_approved_role_can_open_an_analyst_confirmed_organization_case(
    client, persistent_store
):
    source_job_id = persistent_store.create_investigation(["imanul"], {})
    source_job = persistent_store.claim_next("worker:role-source")
    role = "Head of the Digital Economy Division, ILUNI FEB UI"
    result = {
        "status": "completed",
        "usernames": ["imanul"],
        "individual_reports": [
            {
                "username": "imanul",
                "claimed_profiles": [
                    {
                        "site_name": "FEB UI",
                        "url": "https://feb.ui.ac.id/example",
                        "confidence": "strong",
                        "evidence": {"occupation": role},
                    }
                ],
            }
        ],
    }
    persistent_store.finish(source_job_id, result)
    persistent_store.sync_persona_claims(source_job_id, result)
    persona = persistent_store.get_persona(
        persistent_store.get_case(source_job["case_id"])["personas"][0]["id"]
    )
    occupation = next(
        claim for claim in persona["claims"] if claim["field_name"] == "occupation"
    )
    unreviewed_page = client.get(f"/personas/{persona['id']}").get_data(
        as_text=True
    )
    assert "Investigate organization from role" not in unreviewed_page

    persistent_store.review_claim(occupation["id"], "approved", "analyst")
    reviewed_page = client.get(f"/personas/{persona['id']}").get_data(
        as_text=True
    )
    assert "Investigate organization from role" in reviewed_page
    assert 'value="ILUNI FEB UI"' in reviewed_page
    assert "does not silently turn the role text into an approved affiliation fact" in (
        reviewed_page
    )

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "role-organization-csrf"
    rejected = client.post(
        f"/claims/{occupation['id']}/investigate-affiliation",
        data={
            "csrf_token": "role-organization-csrf",
            "organization_name": "ILUNI",
        },
        follow_redirects=True,
    )
    assert "must match the bounded organization segment" in rejected.get_data(
        as_text=True
    )
    assert len(persistent_store.list_jobs()) == 1

    response = client.post(
        f"/claims/{occupation['id']}/investigate-affiliation",
        data={
            "csrf_token": "role-organization-csrf",
            "organization_name": "ILUNI FEB UI",
            "jurisdiction": "ID",
        },
    )

    assert response.status_code == 302
    affiliation_job = persistent_store.get_job(response.location.rsplit("/", 1)[-1])
    specification = affiliation_job["options"]["investigation_spec"]
    assert specification["affiliation_name"] == "ILUNI FEB UI"
    assert specification["source_claim_id"] == occupation["id"]
    assert specification["source_claim_field"] == "occupation"
    assert specification["target_basis"] == (
        "analyst_confirmed_role_organization"
    )
    assert specification["legal_jurisdiction"]["code"] == "ID"
    queued_event = persistent_store.get_events(affiliation_job["job_id"])[0][
        "event"
    ]
    assert queued_event["source_claim_id"] == occupation["id"]
    assert queued_event["source_claim_field"] == "occupation"
    assert queued_event["target_basis"] == (
        "analyst_confirmed_role_organization"
    )


def _persistent_approved_full_name(store, name="Alice Example"):
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
    name_claim = next(
        claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "full_name"
    )
    store.review_claim(name_claim["id"], "approved", "analyst")
    return persona_id, name_claim["id"]


def test_completed_identity_enrichment_opens_persona_instead_of_results(
    client, persistent_store
):
    persona_id, name_claim_id = _persistent_approved_full_name(persistent_store)
    job_id = persistent_store.create_identity_enrichment(persona_id, name_claim_id)
    persistent_store.claim_next("worker:identity-history")
    persistent_store.finish(
        job_id,
        {
            "status": "completed",
            "usernames": [],
            "persona_id": persona_id,
            "confirmed_name": "Alice Example",
            "wikipedia_claim_count": 0,
            "offshore_alert_count": 0,
        },
    )

    history = client.get("/history").get_data(as_text=True)
    assert f'href="/personas/{persona_id}"' in history
    assert ">Open persona</a>" in history
    assert f'href="/results/search_{job_id}"' not in history

    legacy_results_url = client.get(f"/results/search_{job_id}")
    assert legacy_results_url.status_code == 302
    assert legacy_results_url.location.endswith(f"/personas/{persona_id}")


def test_identity_worker_degrades_sources_and_persists_review_gated_alerts(
    client, web_app, persistent_store, monkeypatch
):
    persona_id, name_claim_id = _persistent_approved_full_name(persistent_store)
    job_id = persistent_store.create_identity_enrichment(persona_id, name_claim_id)
    job = persistent_store.claim_next("worker:identity")

    async def fake_wikipedia(*_args, **_kwargs):
        raise RuntimeError("Wikipedia is temporarily unavailable")

    async def fake_icij(*_args, **_kwargs):
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

    monkeypatch.setattr(web_app, "run_wikipedia_person_enrichment", fake_wikipedia)
    monkeypatch.setattr(web_app, "run_icij_offshore_match", fake_icij)
    web_app.run_persistent_job(persistent_store, job)

    completed = persistent_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["offshore_alert_count"] == 1
    assert len(completed["source_errors"]) == 1
    offshore = next(
        claim
        for claim in persistent_store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "offshore_database_match"
    )
    assert offshore["review_status"] == "pending"
    events = [item["event"] for item in persistent_store.get_events(job_id)]
    assert any(event["type"] == "risk_alert" for event in events)
    assert events[-1]["redirect"] == f"/personas/{persona_id}"
    page = client.get(f"/personas/{persona_id}").get_data(as_text=True)
    assert "Potential ICIJ Offshore Leaks name match" in page
    assert "not confirmed identity or evidence of wrongdoing" in page
    assert "Review ICIJ source" in page


def test_approving_full_name_queues_confirmed_name_enrichment(
    client, persistent_store
):
    source_job_id = persistent_store.create_investigation(["alice"], {})
    source_job = persistent_store.claim_next("worker:identity-source")
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
    persistent_store.finish(source_job_id, result)
    persistent_store.sync_persona_claims(source_job_id, result)
    persona_id = persistent_store.get_case(source_job["case_id"])["personas"][0][
        "id"
    ]
    name_claim = next(
        claim
        for claim in persistent_store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "full_name"
    )
    with client.session_transaction() as session:
        session["csrf_token"] = "identity-review-csrf"
    response = client.post(
        f"/claims/{name_claim['id']}/review",
        data={
            "csrf_token": "identity-review-csrf",
            "persona_id": persona_id,
            "decision": "approved",
        },
    )

    assert response.status_code == 302
    enrichment = persistent_store.get_persona_identity_enrichment(persona_id)
    assert enrichment["kind"] == "identity_enrichment"
    assert enrichment["status"] == "queued"
    assert enrichment["options"]["investigation_spec"]["confirmed_name"] == (
        "Alice Example"
    )
    live_page = client.get(f"/live/{enrichment['job_id']}")
    assert live_page.status_code == 200
    assert "Confirmed-name enrichment" in live_page.get_data(as_text=True)
