"""Chromium acceptance of the real Flask assessment and report screens.

Only collection responses use the shared deterministic fixture. Chromium talks to
an actual loopback Flask server; external browser requests are always blocked.
CI requires this test and rejects missing Chromium/dependencies rather than skip.
"""

import os
from pathlib import Path
import threading

import pytest
from werkzeug.serving import make_server

from tests.test_pipeline_app_journey import application_journey, form, run_queued


pytestmark = pytest.mark.skipif(
    os.getenv("OPENLEDGER_REQUIRE_BROWSER") != "true",
    reason="Chromium acceptance is required in the release CI environment",
)


def test_browser_four_inputs_assessment_reject_approve_and_report(application_journey, tmp_path):
    from playwright.sync_api import sync_playwright, expect

    journey = application_journey
    pipeline, store = journey["pipeline"], journey["store"]
    identifiers = [
        ("username", "synthetic.person"), ("full_name", "Synthetic Person"),
        ("email", "synthetic@example.test"), ("phone", "+628123456789"),
    ]
    # HTTP collection entrypoint and real worker service; synthetic source only.
    queued = journey["client"].post("/api/scan", data=form(journey, identifiers))
    assert queued.status_code == 200, queued.get_data(as_text=True)
    result = run_queued(journey, queued.get_json()["job_id"])
    case_id = result["case_id"]
    subjects = store.get_case(case_id)["personas"]
    assert len(subjects) == 1
    persona_id = subjects[0]["id"]
    base = f"/cases/{case_id}/pipeline/{persona_id}"
    originals = pipeline.list_observations(case_id, persona_id, limit=500)
    source = next(row for row in originals if row.get("source_url") and row["outcome"] == "found")
    server = make_server("127.0.0.1", 0, journey["web"].app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_port}"
    evidence = Path(os.getenv("OPENLEDGER_BROWSER_EVIDENCE_DIR", str(tmp_path / "browser")))
    evidence.mkdir(parents=True, exist_ok=True)
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context(viewport={"width": 1440, "height": 1000})
            context.route("**/*", lambda route: route.continue_() if route.request.url.startswith(origin + "/") else route.abort())
            page = context.new_page()
            failures = []
            page.on("pageerror", lambda error: failures.append(str(error)))
            page.goto(origin + "/login")
            page.locator('input[name="username"]').fill("pipeline-reviewer")
            page.locator('input[name="password"]').fill("Synthetic fixture password 2026!")
            page.locator('button[type="submit"]').click()
            page.goto(origin + base + "/observations")
            manual = page.locator('form[action$="/manual-evidence"]')
            manual.locator('[name="predicate"]').select_option("full_name")
            manual.locator('[name="value"]').fill("Synthetic Person")
            manual.locator('[name="source_url"]').fill(source["source_url"])
            manual.locator('[name="observation_ids"]').fill(source["id"])
            manual.locator('[name="reason"]').fill("The retained directory explicitly names this subject.")
            manual.locator('button[type="submit"]').click()
            page.goto(origin + base)
            expect(page.get_by_text("Review consolidated evidence by subject area.", exact=False)).to_be_visible()
            for tab_name in (
                "Identity", "Contact & location", "Digital presence",
                "Affiliations", "Assets & risk", "Review queue", "Engine status",
            ):
                expect(page.get_by_role("tab", name=tab_name, exact=False)).to_be_visible()

            # Reject and then approve the same finding to prove both controls work
            # while retaining the complete decision trail used by report snapshots.
            decision = page.locator('form[action$="/decision"]:visible').first
            decision.locator('[name="reason"]').fill("Rejected pending closer source review.")
            decision.get_by_role("button", name="Reject").click()
            decision = page.locator('form[action$="/decision"]:visible').first
            decision.locator('[name="reason"]').fill("Approved after reviewing the cited source.")
            decision.get_by_role("button", name="Approve").click()

            workspace = pipeline.get_workspace(case_id, persona_id)
            assert any(item.get("latest_decision") == "include" for item in workspace["shortlist"])
            assert {row["id"] for row in originals} <= {
                row["id"] for row in pipeline.list_observations(case_id, persona_id, limit=500)
            }

            with page.expect_download() as download_info:
                page.get_by_role("button", name="Export report").click()
            download = download_info.value
            report_path = download.path()
            assert report_path and Path(report_path).read_bytes().startswith(b"%PDF")
            assert download.suggested_filename.startswith("OpenLedger-Persona-v")

            versions = pipeline.get_workspace(case_id, persona_id)["versions"]
            assert len(versions) == 1 and versions[0]["status"] == "submitted"
            assert pipeline.get_final_version(case_id, persona_id) is None
            assert len(store.get_case(case_id)["personas"]) == 1
            page.goto(origin + base)
            page.screenshot(path=str(evidence / "operator-assessment.png"), full_page=True)
            assert not failures, failures
            context.close()
            browser.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
