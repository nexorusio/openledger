"""Chromium acceptance of the real Flask evidence/QC screens.

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


def test_browser_four_inputs_review_research_qc_and_final(application_journey, tmp_path):
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
            expect(page.get_by_role("heading", name="Automated ranked curated findings")).to_be_visible()
            decision = page.locator('form[action$="/decision"]').first
            decision.locator('button[type="submit"]').click()
            version_form = page.locator('form[action$="/versions"]')
            version_form.locator('[name="scope"]').fill("Public name supported by the retained directory.")
            version_form.locator('button[type="submit"]').click()
            first = pipeline.get_workspace(case_id, persona_id)["versions"][0]
            page.goto(origin + base + f'/versions/{first["id"]}')
            expect(page.get_by_role("heading", name="Explicit quality control")).to_be_visible()
            reject = page.locator('form').filter(has=page.locator('input[value="changes_required"]'))
            reject.locator('[name="question"]').fill("Does the public email record support this name?")
            reject.locator('[name="reason"]').fill("Additional public email linkage is required.")
            reject.locator('[name="completion_criteria"]').fill("Retrieve the email record and cite its retained observation.")
            reject.locator('[name="research_email"]').fill("followup@example.test")
            reject.locator('button[type="submit"]').click()
            page.goto(origin + base)
            requirement = pipeline.get_workspace(case_id, persona_id)["requirements"][0]
            page.locator(f'form[action$="/requirements/{requirement["id"]}/launch"] button').click()
            request_id = pipeline.get_requirement(requirement["id"])["request_ids"][-1]
            followup = pipeline.get_request(request_id, case_id=case_id, persona_id=persona_id)
            run_queued(journey, followup["job_id"])
            after = pipeline.list_observations(case_id, persona_id, limit=500)
            assert {row["id"] for row in originals} <= {row["id"] for row in after}
            evidence_row = next(row for row in after if row["request_id"] == request_id and row.get("source_url") and row["outcome"] == "found")
            page.goto(origin + base)
            resolve = page.locator(f'form[action$="/requirements/{requirement["id"]}/resolve"]')
            resolve.locator('[name="evidence_ids"]').fill(evidence_row["id"])
            resolve.locator('[name="reason"]').fill("The retained public email record meets the recorded criterion.")
            resolve.locator('button[type="submit"]').click()
            version_form = page.locator('form[action$="/versions"]')
            version_form.locator('[name="scope"]').fill("Reviewed name and completed email research.")
            version_form.locator('[name="parent_version_id"]').select_option(first["id"])
            version_form.locator('button[type="submit"]').click()
            versions = pipeline.get_workspace(case_id, persona_id)["versions"]
            successor = next(version for version in versions if version["id"] != first["id"])
            page.goto(origin + base + f'/versions/{successor["id"]}')
            approve = page.locator('form').filter(has=page.locator('input[name="decision"][value="approved"]'))
            approve.locator('[name="finding"]').fill("Reviewed evidence, scope and completed targeted research.")
            approve.locator('[name="qc_confirmed"]').check()
            approve.locator('button[type="submit"]').click()
            page.goto(origin + base + "/final")
            expect(page.get_by_role("heading", name="Final Persona", exact=False).first).to_be_visible()
            page.screenshot(path=str(evidence / "final-persona.png"), full_page=True)
            final = pipeline.get_final_version(case_id, persona_id)
            assert final["id"] == successor["id"]
            assert len(store.get_case(case_id)["personas"]) == 1
            assert pipeline.get_version(first["id"])["status"] == "changes_required"
            response = context.request.get(origin + base + f'/versions/{final["id"]}/export.pdf')
            assert response.status == 200 and response.body().startswith(b"%PDF")
            assert response.headers["x-openledger-version"] == final["id"]
            assert not failures, failures
            context.close()
            browser.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
