"""Playwright acceptance journey for P3R collection presentation.

This test intentionally needs a CI-provisioned Chromium.  It never invokes a
collector or external source: events and completed results are injected into a
disposable store while the browser uses the real Flask templates and SSE
endpoint.  When CI supplies its PostgreSQL service, the same journey uses that
production-shaped persistence path.
"""

from __future__ import annotations

import json
import os
from threading import Thread
from typing import Iterator
from urllib.parse import urlsplit

import pytest
from sqlalchemy import text
from werkzeug.serving import make_server

from maigret.web import app as web_app_module
from maigret.web.case_store import CaseStore
from maigret.web.profile_reliability import PROFILE_RELIABILITY_VERSION


pytestmark = pytest.mark.browser
POSTGRES_URL = os.getenv("OPENLEDGER_TEST_POSTGRES_URL", "")


ACCOUNTING_RUNNING = {
    "schema_version": 1,
    "revision": 2,
    "known": True,
    "state": "running",
    "stages": [
        {
            "stage_id": "native",
            "engine_id": "native-profile-search",
            "label": "server-only label",
            "unit": "queries",
            "status": "completed",
            "reason": None,
            "planned": 1,
            "started": 1,
            "terminal": 1,
            "completed": 1,
            "errors": 0,
            "timeouts": 0,
            "cancelled": 0,
            "interrupted": 0,
            "unattempted": 0,
            "unknown": 0,
            "observations": 1,
        },
        {
            "stage_id": "maigret",
            "engine_id": "maigret",
            "label": "ignored by the renderer",
            "unit": "site_checks",
            "status": "running",
            "reason": None,
            "planned": 4,
            "started": 4,
            "terminal": 3,
            "completed": 2,
            "errors": 1,
            "timeouts": 0,
            "cancelled": 0,
            "interrupted": 0,
            "unattempted": 0,
            "unknown": 1,
            "observations": 2,
        },
        {
            "stage_id": "github",
            "engine_id": "github-public-profile",
            "label": "ignored by the renderer",
            "unit": "queries",
            "status": "failed",
            "reason": "provider_unavailable",
            "planned": 1,
            "started": 1,
            "terminal": 1,
            "completed": 0,
            "errors": 1,
            "timeouts": 0,
            "cancelled": 0,
            "interrupted": 0,
            "unattempted": 0,
            "unknown": 0,
            "observations": 0,
        },
        {
            "stage_id": "unfurl", "engine_id": "unfurl-url-analysis",
            "label": "ignored by the renderer", "unit": "targets",
            "status": "cancelled", "reason": "parent_cancelled", "planned": 1,
            "started": 1, "terminal": 1, "completed": 0, "errors": 0,
            "timeouts": 0, "cancelled": 1, "interrupted": 0, "unattempted": 0,
            "unknown": 0, "observations": 0,
        },
        {
            "stage_id": "wayback", "engine_id": "wayback-cdx",
            "label": "ignored by the renderer", "unit": "queries",
            "status": "interrupted", "reason": "interrupted", "planned": 1,
            "started": 1, "terminal": 0, "completed": 0, "errors": 0,
            "timeouts": 0, "cancelled": 0, "interrupted": 1, "unattempted": 0,
            "unknown": 0, "observations": 0,
        },
        {
            "stage_id": "user_scanner_username", "engine_id": "user-scanner-username",
            "label": "ignored by the renderer", "unit": "targets",
            "status": "unknown", "reason": "not_admitted", "planned": 1,
            "started": 0, "terminal": 0, "completed": 0, "errors": 0,
            "timeouts": 0, "cancelled": 0, "interrupted": 0, "unattempted": 1,
            "unknown": 0, "observations": 0,
        },
        {
            "stage_id": "user_scanner_email", "engine_id": "user-scanner",
            "label": "ignored by the renderer", "unit": "targets",
            "status": "pending", "reason": "dependency_not_ready", "planned": 1,
            "started": 0, "terminal": 0, "completed": 0, "errors": 0,
            "timeouts": 0, "cancelled": 0, "interrupted": 0, "unattempted": 1,
            "unknown": 0, "observations": 0,
        },
    ],
}

ACCOUNTING_PARTIAL = {
    **ACCOUNTING_RUNNING,
    "revision": 3,
    "state": "partial",
    "stages": [
        ACCOUNTING_RUNNING["stages"][0],
        {**ACCOUNTING_RUNNING["stages"][1], "status": "timed_out", "reason": "timeout",
         "terminal": 4, "completed": 2, "errors": 1, "timeouts": 1,
         "unattempted": 0, "unknown": 0},
        *ACCOUNTING_RUNNING["stages"][2:],
    ],
}


def _require_browser():
    if os.getenv("OPENLEDGER_P3R_BROWSER") != "1":
        pytest.skip("Set OPENLEDGER_P3R_BROWSER=1 after CI provisions Chromium.")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as error:  # CI must install this when the gate is enabled.
        pytest.fail(f"P3R browser gate was enabled without Playwright: {error}")
    return sync_playwright


@pytest.fixture
def p3r_browser_app(tmp_path, monkeypatch) -> Iterator[tuple[str, CaseStore, str]]:
    """Serve the real Flask app against a disposable job/event store."""
    # The default is the legacy builder.  Exercise the current visible form
    # whose server-owned preview enables the real submission control.
    monkeypatch.setenv("OPENLEDGER_UNIFIED_INVESTIGATION_INPUT_ENABLED", "1")
    if POSTGRES_URL:
        store = CaseStore(POSTGRES_URL)
        with store.engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE cases RESTART IDENTITY CASCADE"))
    else:
        store = CaseStore(f"sqlite:///{tmp_path / 'browser.db'}", create_schema=True)
    app = web_app_module.app
    app.config.update(
        TESTING=True,
        AUTH_REQUIRED=False,
        REPORTS_FOLDER=str(tmp_path / "reports"),
        SETTINGS_FILE=str(tmp_path / "settings.json"),
    )
    web_app_module.job_results.clear()
    monkeypatch.setattr(web_app_module, "case_store", store)
    job_id = store.create_investigation(["alice"], {"execution_mode": "focused"})
    job = store.claim_next("worker:p3r-browser")
    assert job and job["job_id"] == job_id
    monkeypatch.setattr(web_app_module, "start_live_job", lambda *_args, **_kwargs: job_id)

    server = make_server("127.0.0.1", 0, app, threaded=True)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", store, job_id
    finally:
        server.shutdown()
        thread.join(timeout=5)
        if POSTGRES_URL:
            with store.engine.begin() as connection:
                connection.execute(text("TRUNCATE TABLE cases RESTART IDENTITY CASCADE"))
        store.dispose()
        web_app_module.job_results.clear()


def _completed_result(job_id: str) -> dict:
    return {
        "status": "completed",
        "kind": "live",
        "session_folder": f"search_{job_id}",
        "usernames": ["alice"],
        "graph_file": f"search_{job_id}/graph.html",
        "individual_reports": [],
        "found_count": 2,
        "candidate_count": 1,
        "suppressed_count": 1,
        "raw_claimed_count": 4,
        "profile_reliability_version": PROFILE_RELIABILITY_VERSION,
        "collection_status": "budget_exhausted",
        "collection_message": "Partial evidence retained.",
        "collection_accounting": ACCOUNTING_PARTIAL,
    }


def _confine_browser_to_fixture(page, base_url: str) -> None:
    """Allow only the local Flask origin; fixture browsing never uses sources."""
    fixture_url = urlsplit(base_url)
    fixture_origin = (fixture_url.scheme, fixture_url.netloc)

    def handle(route):
        request = urlsplit(route.request.url)
        if (request.scheme, request.netloc) == fixture_origin:
            route.continue_()
        else:
            route.abort()

    page.route("**/*", handle)


def _accounting_cells(rows, index: int) -> list[str]:
    return [cell.strip() for cell in rows.nth(index).locator("td").all_inner_texts()]


def test_browser_submission_progress_refresh_and_partial_results(p3r_browser_app):
    sync_playwright = _require_browser()
    base_url, store, job_id = p3r_browser_app
    store.append_event(job_id, {"type": "start", "username": "alice", "total": 4})
    store.append_event(job_id, {"type": "collection_accounting", "collection_accounting": ACCOUNTING_RUNNING})
    # A stale replay must not replace the revision-2 snapshot in the browser.
    store.append_event(job_id, {"type": "collection_accounting", "collection_accounting": {**ACCOUNTING_RUNNING, "revision": 1, "state": "completed"}})

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(10_000)
        _confine_browser_to_fixture(page, base_url)
        try:
            page.goto(base_url, wait_until="domcontentloaded")
            token_input = page.locator("#investigation-token-input")
            token_input.fill("alice")
            token_input.press("Enter")
            page.wait_for_function(
                "() => !document.getElementById('startBtn').disabled"
            )
            with page.expect_navigation(wait_until="domcontentloaded"):
                page.locator("#startBtn").click()
            page.wait_for_url(f"**/live/{job_id}")
            page.locator("#collection-accounting").wait_for()
            assert page.locator("#collection-accounting-state").inner_text() == "Running"
            rows = page.locator("#collection-accounting-body tr")
            assert rows.count() == 7
            assert _accounting_cells(rows, 0) == [
                "Native profile search", "queries", "Completed", "—", "1 / 1", "0", "0", "0", "0",
            ]
            assert _accounting_cells(rows, 1) == [
                "Maigret", "site checks", "Running", "—", "3 / 4", "1", "0", "0", "1",
            ]
            assert _accounting_cells(rows, 2) == [
                "GitHub", "queries", "Failed", "Provider unavailable", "1 / 1", "1", "0", "0", "0",
            ]
            assert _accounting_cells(rows, 3) == [
                "Unfurl", "targets", "Cancelled", "Parent cancelled", "1 / 1", "0", "0", "0", "0",
            ]
            assert _accounting_cells(rows, 4) == [
                "Wayback", "queries", "Interrupted", "Interrupted", "0 / 1", "0", "0", "0", "0",
            ]
            assert _accounting_cells(rows, 5) == [
                "User Scanner usernames", "targets", "Unknown", "Not admitted", "0 / 1", "0", "0", "1", "0",
            ]
            assert _accounting_cells(rows, 6) == [
                "User Scanner emails", "targets", "Pending", "Dependency not ready", "0 / 1", "0", "0", "1", "0",
            ]

            # Refresh reconnects to persisted SSE history; it cannot inflate counters.
            page.reload(wait_until="domcontentloaded")
            refreshed_rows = page.locator("#collection-accounting-body tr")
            refreshed_rows.first.wait_for()
            assert refreshed_rows.count() == 7
            assert _accounting_cells(
                refreshed_rows, 1
            ) == ["Maigret", "site checks", "Running", "—", "3 / 4", "1", "0", "0", "1"]

            result = _completed_result(job_id)
            web_app_module.job_results[job_id] = result
            assert store.finish(job_id, result, worker_id="worker:p3r-browser")
            store.append_event(job_id, {
                "type": "done", "status": "partial", "reason": "budget_exhausted",
                "redirect": f"/results/search_{job_id}", "collection_accounting": ACCOUNTING_PARTIAL,
            })
            page.locator("#reportsBtn").wait_for()
            assert page.locator("#stat-graph-status").inner_text() == "Partial evidence retained"
            page.locator("#reportsBtn").click()
            page.wait_for_url(f"**/results/search_{job_id}")
            assert page.locator("#results-collection-accounting-heading").inner_text() == "Declared collection accounting"
            rows = page.locator("#results-collection-accounting-heading").locator("xpath=../../following-sibling::div//tbody/tr")
            assert rows.count() == 7
            assert _accounting_cells(rows, 1) == [
                "Maigret", "site checks", "Timed out", "Timed out", "4 / 4", "1", "1", "0", "0",
            ]
            assert _accounting_cells(rows, 5) == [
                "User Scanner usernames", "targets", "Unknown", "Not admitted", "0 / 1", "0", "0", "1", "0",
            ]
        finally:
            browser.close()


def _seed_persona_with_many_citations(store: CaseStore, username: str) -> tuple[str, str]:
    job_id = store.create_investigation([username], {})
    store.claim_next(f"worker:p3r-browser-{username}")
    profiles = [
        {
            "site_name": f"Source {number}",
            "url": f"https://evidence.test/{username}/{number}",
            "confidence": "strong",
            "evidence": {
                "fullname": "Shared Exact Name",
                "email": "shared@example.test",
            },
        }
        for number in range(12)
    ]
    result = {
        "status": "completed", "session_folder": f"search_{job_id}",
        "usernames": [username], "graph_file": f"search_{job_id}/graph.html",
        "found_count": len(profiles),
        "individual_reports": [{"username": username, "claimed_profiles": profiles}],
        "profile_reliability_version": PROFILE_RELIABILITY_VERSION,
    }
    assert store.finish(job_id, result, worker_id=f"worker:p3r-browser-{username}")
    assert store.sync_persona_claims(job_id, result)
    persona = store.get_case(store.get_job(job_id)["case_id"])["personas"][0]
    email_claim = next(
        claim for claim in store.get_persona(persona["id"])["claims"]
        if claim["field_name"] == "email"
    )
    return persona["id"], email_claim["id"]


def _visible_claim_record(page, claim_id: str):
    """Resolve the visible occurrence of the explicit review claim.

    A pending record is intentionally shown in both its subject category and
    the hidden review queue.  Scope assertions and review actions to the
    server-assigned claim identifier rather than matching duplicated text.
    """
    return page.locator(
        f'article.claim-record:has(form[action="/claims/{claim_id}/review"]):visible'
    )


def _show_contact_claim(page, claim_id: str):
    page.locator('button[data-persona-tab="contact"]').click()
    claim = _visible_claim_record(page, claim_id)
    claim.wait_for()
    return claim


def test_browser_pending_evidence_requires_approval_before_shared_graph(p3r_browser_app):
    sync_playwright = _require_browser()
    base_url, store, job_id = p3r_browser_app
    # The first journey starts a claimed job for its live SSE path.  Complete
    # it here so relationship state reflects the two pending Persona claims,
    # rather than correctly prioritizing an unrelated active collection.
    assert store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": ["alice"],
            "graph_file": f"search_{job_id}/graph.html",
            "individual_reports": [],
        },
        worker_id="worker:p3r-browser",
    )
    _first_persona, first_claim = _seed_persona_with_many_citations(store, "alice")
    _second_persona, second_claim = _seed_persona_with_many_citations(store, "bob")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(10_000)
        _confine_browser_to_fixture(page, base_url)
        try:
            page.goto(base_url + f"/personas/{_first_persona}", wait_until="domcontentloaded")
            assert page.locator('[data-review-status="pending"]').count() > 0
            first_claim_record = _show_contact_claim(page, first_claim)
            assert first_claim_record.count() == 1
            assert first_claim_record.get_by_text(
                "12 supporting sources", exact=True
            ).count() == 1
            assert first_claim_record.locator(
                'a[href^="https://evidence.test/alice/"]'
            ).count() == 12

            page.goto(base_url + "/relationships?mode=shared", wait_until="domcontentloaded")
            assert page.locator('[data-relationship-state="pending_review"]').count() == 1
            assert page.locator("#relationshipGraphData").count() == 0

            page.goto(base_url + f"/personas/{_first_persona}", wait_until="domcontentloaded")
            first_claim_record = _show_contact_claim(page, first_claim)
            assert first_claim_record.count() == 1
            with page.expect_navigation(wait_until="domcontentloaded"):
                first_claim_record.get_by_role("button", name="Approve").click()
            assert store.get_claim(first_claim)["review_status"] == "approved"

            page.goto(base_url + f"/personas/{_second_persona}", wait_until="domcontentloaded")
            second_claim_record = _show_contact_claim(page, second_claim)
            assert second_claim_record.count() == 1
            with page.expect_navigation(wait_until="domcontentloaded"):
                second_claim_record.get_by_role("button", name="Approve").click()
            assert store.get_claim(second_claim)["review_status"] == "approved"

            page.goto(base_url + "/relationships?mode=shared", wait_until="domcontentloaded")
            page.reload(wait_until="domcontentloaded")
            graph_data = page.locator("#relationshipGraphData")
            graph_data.wait_for(state="attached")
            graph_text = graph_data.text_content()
            assert graph_text
            graph = json.loads(graph_text)
            assert graph["stats"]["connection_count"] == 2
            assert len(graph["edges"]) == 2
            assert all(edge["source_count"] == 12 for edge in graph["edges"])
            assert all(len(edge["sources"]) == 10 for edge in graph["edges"])
            for edge in graph["edges"]:
                assert edge["provenance_url"].startswith("/personas/")
                claim_id = edge["claim_id"]
                page.goto(
                    base_url + edge["provenance_url"], wait_until="domcontentloaded"
                )
                provenance_claim = _show_contact_claim(page, claim_id)
                assert provenance_claim.locator(
                    'a[href^="https://evidence.test/"]'
                ).count() == 12
        finally:
            browser.close()
