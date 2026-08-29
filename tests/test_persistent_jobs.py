import asyncio

import pytest

from maigret.web.case_store import CaseStore
from maigret.web import app as web_app_module


@pytest.fixture
def web_app(tmp_path):
    web_app_module.app.config["TESTING"] = True
    web_app_module.app.config["REPORTS_FOLDER"] = str(tmp_path / "reports")
    web_app_module.app.config["SETTINGS_FILE"] = str(tmp_path / "settings.json")
    web_app_module.app.config["AUTH_REQUIRED"] = False
    web_app_module.job_results.clear()
    web_app_module.live_jobs.clear()
    yield web_app_module
    web_app_module.job_results.clear()
    web_app_module.live_jobs.clear()


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

    case_page = client.get(f'/cases/{case["id"]}').get_data(as_text=True)
    assert "Open structured profile" in case_page
    assert f"/personas/{persona_id}" in case_page

    persona_page = client.get(f"/personas/{persona_id}").get_data(as_text=True)
    assert "Alice Example" in persona_page
    assert "90% confidence" in persona_page
    assert "No evidence extracted." in persona_page
    assert "AI proposes; the analyst decides" in persona_page
    assert "Review queue" in persona_page
    assert "Relationships" in persona_page


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


def test_persona_refresh_queues_new_collection_in_the_same_case(
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
    refresh_job_id = response.location.rsplit("/", 1)[-1]
    refresh_job = persistent_store.get_job(refresh_job_id)
    assert refresh_job["case_id"] == case["id"]
    assert refresh_job["status"] == "queued"
    assert refresh_job["kind"] == "refresh"
    assert refresh_job["options"]["top_sites"] == 250


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
    assert "vis-network@10.1.1" in page

    persona_page = client.get("/relationships?mode=persona").get_data(as_text=True)
    assert "Persona evidence network" in persona_page
    assert "Example" in persona_page
    assert "Review status remains visible" in persona_page
    assert "vis-network@10.1.1" in persona_page
