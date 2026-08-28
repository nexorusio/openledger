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
