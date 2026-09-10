"""Chromium continuation of the full P3R worker acceptance gate.

The old P3R browser journey seeds a result after bypassing ``start_live_job``.
This journey obtains its job through the Flask form and finishes it through the
real persistent worker boundary defined in ``test_p3r_full_acceptance``.
"""

from __future__ import annotations

import os
from threading import Thread
from urllib.parse import urlsplit

import pytest
from werkzeug.serving import make_server

from tests.test_p3r_full_acceptance import (
    FULL_ACCEPTANCE_ENV,
    _run_claimed_job,
    full_app,
    postgres_store,
)

pytestmark = [
    pytest.mark.browser,
    pytest.mark.skipif(
        os.getenv(FULL_ACCEPTANCE_ENV) != "1",
        reason="P3R full Chromium acceptance is enabled only with the full isolated gate.",
    ),
]


def _require_chromium():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as error:
        pytest.fail(f"P3R full acceptance was enabled without Playwright: {error}")
    return sync_playwright


def _confine_browser(page, base_url: str) -> None:
    origin = urlsplit(base_url)
    allowed = (origin.scheme, origin.netloc)

    def route(request_route):
        parsed = urlsplit(request_route.request.url)
        if (parsed.scheme, parsed.netloc) == allowed:
            request_route.continue_()
        else:
            request_route.abort()

    page.route("**/*", route)


@pytest.fixture
def served_full_app(full_app):
    app, store, reports = full_app
    server = make_server("127.0.0.1", 0, app, threaded=True)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", app, store, reports
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_browser_real_submission_worker_sse_and_published_report(served_full_app):
    sync_playwright = _require_chromium()
    base_url, _app, store, reports = served_full_app
    worker = None

    # Submission is made through the rendered form; this reaches the real
    # Flask route that creates the persisted queue entry before the worker runs.
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(15_000)
        _confine_browser(page, base_url)
        try:
            page.goto(base_url, wait_until="domcontentloaded")
            token_input = page.locator("#investigation-token-input")
            token_input.fill("browserfixture")
            token_input.press("Enter")
            page.wait_for_function(
                "() => !document.getElementById('startBtn').disabled"
            )
            with page.expect_navigation(wait_until="domcontentloaded"):
                page.locator("#startBtn").click()
            page.wait_for_url("**/live/*")
            job_id = urlsplit(page.url).path.rsplit("/", 1)[-1]
            assert store.get_job(job_id)["status"] == "queued"
            worker = Thread(target=_run_claimed_job, args=(store, job_id), daemon=True)
            worker.start()
            page.locator("#collection-accounting").wait_for()
            page.locator("#reportsBtn").wait_for()
            page.locator("#reportsBtn").click()
            page.wait_for_url(f"**/results/search_{job_id}")
            assert (
                page.get_by_text("Declared collection accounting", exact=True).count()
                == 1
            )
            assert page.locator("#results-collection-accounting-heading").count() == 1
            assert page.locator('a[href$="report_browserfixture.pdf"]').count() == 1
            assert (
                reports / f"search_{job_id}" / "report_browserfixture.pdf"
            ).is_file()
        finally:
            browser.close()
            if worker is not None:
                worker.join(timeout=20)
                assert not worker.is_alive()
