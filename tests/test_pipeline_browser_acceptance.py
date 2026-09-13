"""Chromium acceptance of the real Flask assessment and report screens.

Only collection responses use the shared deterministic fixture. Chromium talks to
an actual loopback Flask server; external browser requests are always blocked.
CI requires this test and rejects missing Chromium/dependencies rather than skip.
"""

import os
from pathlib import Path
import re
import threading

import pytest
from werkzeug.serving import make_server

from tests.test_pipeline_app_journey import application_journey, form, run_queued


pytestmark = pytest.mark.skipif(
    os.getenv("OPENLEDGER_REQUIRE_BROWSER") != "true",
    reason="Chromium acceptance is required in the release CI environment",
)


def assert_no_page_overflow(page, label):
    dimensions = page.evaluate(
        """() => ({
            viewport: document.documentElement.clientWidth,
            document: document.documentElement.scrollWidth,
            body: document.body.scrollWidth,
        })"""
    )
    assert dimensions["document"] <= dimensions["viewport"] + 1, (
        label,
        dimensions,
    )
    assert dimensions["body"] <= dimensions["viewport"] + 1, (
        label,
        dimensions,
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
    job_id = queued.get_json()["job_id"]
    result = run_queued(journey, job_id)
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
            for viewport, label in (
                ({"width": 1440, "height": 1000}, "desktop login"),
                ({"width": 768, "height": 1024}, "tablet login"),
                ({"width": 390, "height": 844}, "mobile login"),
            ):
                page.set_viewport_size(viewport)
                page.goto(origin + "/login")
                expect(page.locator('.login-visual img')).to_be_visible()
                expect(page.get_by_role("link", name="OpenLedger")).to_be_visible()
                expect(page.locator('form.login-form')).to_be_visible()
                assert page.locator('.login-visual img').evaluate(
                    "image => image.complete && image.naturalWidth > 0"
                )
                assert_no_page_overflow(page, label)
                if viewport["width"] != 768:
                    page.screenshot(
                        path=str(evidence / f'{label.replace(" ", "-")}.png'),
                        full_page=True,
                    )

            page.set_viewport_size({"width": 1440, "height": 1000})
            page.goto(origin + "/login")
            page.locator('input[name="username"]').fill("pipeline-reviewer")
            page.locator('input[name="password"]').fill("Synthetic fixture password 2026!")
            page.locator('button[type="submit"]').click()

            # Live progress keeps fixed proportional columns while changing
            # activity text, exposes the full value on hover, remains sortable,
            # and belongs to New investigation rather than History.
            page.goto(origin + f"/live/{job_id}")
            expect(page.get_by_role("link", name="New investigation")).to_have_class(
                re.compile(r"\bactive\b")
            )
            expect(page.get_by_role("link", name="History")).not_to_have_class(
                re.compile(r"\bactive\b")
            )
            long_activity = (
                "A deliberately long current activity message that must truncate "
                "without resizing any column while remaining available on hover."
            )
            page.evaluate(
                """activity => {
                    updateCollector(
                        {collector: 'zeta-engine', platform: 'threads'},
                        'running', activity, 'Collecting'
                    );
                    updateCollector(
                        {collector: 'alpha-engine', input_type: 'email'},
                        'queued', 'Waiting for an execution slot', '—'
                    );
                }""",
                long_activity,
            )
            progress_table = page.locator("#engine-progress-table")
            activity_cell = progress_table.locator("tbody tr").first.locator("td").nth(2)
            activity_text = activity_cell.locator(".table-cell-ellipsis")
            expect(activity_text).to_have_attribute("title", long_activity)
            assert activity_text.evaluate(
                "element => getComputedStyle(element).textOverflow"
            ) == "ellipsis"
            assert activity_text.evaluate(
                "element => getComputedStyle(element).whiteSpace"
            ) == "nowrap"
            widths_before = progress_table.locator("tbody tr").first.locator("td").evaluate_all(
                "cells => cells.map(cell => cell.getBoundingClientRect().width)"
            )
            page.evaluate(
                """() => updateCollector(
                    {collector: 'zeta-engine', platform: 'threads'},
                    'running', 'Short update', 'Collecting'
                )"""
            )
            widths_after = progress_table.locator("tbody tr").first.locator("td").evaluate_all(
                "cells => cells.map(cell => cell.getBoundingClientRect().width)"
            )
            assert all(
                abs(before - after) <= 1
                for before, after in zip(widths_before, widths_after)
            )
            page.get_by_role("button", name=re.compile(r"^Engine")).click()
            engine_names = progress_table.locator("tbody tr td:first-child").all_text_contents()
            assert engine_names == sorted(engine_names, key=str.casefold)

            page.goto(origin + base + "/observations")
            manual = page.locator('form[action$="/manual-evidence"]')
            manual.locator('[name="predicate"]').select_option("full_name")
            manual.locator('[name="value"]').fill("Synthetic Person")
            manual.locator('[name="source_url"]').fill(source["source_url"])
            manual.locator('[name="observation_ids"]').fill(source["id"])
            manual.locator('[name="reason"]').fill("The retained directory explicitly names this subject.")
            manual.locator('button[type="submit"]').click()
            page.goto(origin + base)
            expect(
                page.get_by_role(
                    "heading", name="Confirm digital presence and evidence"
                )
            ).to_be_visible()
            for tab_name in ("Review queue", "Engine log"):
                expect(page.get_by_role("tab", name=tab_name, exact=False)).to_be_visible()
            expect(page.get_by_text("Step 1", exact=True)).to_be_visible()
            expect(page.get_by_text("Step 2", exact=True)).to_be_visible()
            review_table = page.locator("table.assessment-review-table")
            expect(review_table).to_be_visible()
            assert review_table.locator("thead th").all_text_contents() == [
                "Finding ↕", "Category ↕", "Assessment ↕", "Evidence ↕",
                "Decision ↕", "Actions",
            ]

            # Reject and then approve the same finding to prove both controls work
            # while retaining the complete decision trail used by report snapshots.
            decision = page.locator('form[action$="/decision"]:visible').first
            assert decision.locator('[name="reason"]').get_attribute("required") is None
            decision.get_by_role("button", name="Reject").click()
            expect(page.get_by_text("Rejected", exact=True).first).to_be_visible()
            page.get_by_role("button", name="Edit decision").first.click()
            decision = page.locator('form[action$="/decision"]:visible').first
            decision.locator('[name="reason"]').fill("Approved after reviewing the cited source.")
            decision.get_by_role("button", name="Approve").click()
            expect(page.get_by_text("Approved", exact=True).first).to_be_visible()

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
            responsive_paths = (
                ("investigation", "/"),
                ("cases", "/cases"),
                ("combined case", "/cases/combine"),
                ("case", f"/cases/{case_id}"),
                ("persona", f"/personas/{persona_id}"),
                ("live results", f"/live/{job_id}"),
                ("assessment", base),
                ("evidence", base + "/observations"),
                ("history", "/history"),
                ("settings", "/settings"),
                ("security", "/security"),
                (
                    "relationships",
                    f"/relationships?mode=persona&case_id={case_id}&persona_id={persona_id}&view=working",
                ),
            )
            for viewport, viewport_name in (
                ({"width": 1280, "height": 900}, "desktop"),
                ({"width": 768, "height": 1024}, "tablet"),
                ({"width": 390, "height": 844}, "mobile"),
            ):
                page.set_viewport_size(viewport)
                for page_name, path in responsive_paths:
                    page.goto(origin + path)
                    expect(page.locator("main")).to_be_visible()
                    assert_no_page_overflow(page, f"{viewport_name} {page_name}")
                if viewport_name == "mobile":
                    page.goto(origin + "/")
                    page.locator("#sidebarToggle").click()
                    expect(page.locator("#appShell")).to_have_class(
                        re.compile(r"\bsidebar-open\b")
                    )
                    backdrop = page.locator("#sidebarBackdrop")
                    backdrop_box = backdrop.bounding_box()
                    assert backdrop_box
                    backdrop.click(
                        position={"x": backdrop_box["width"] - 8, "y": 8}
                    )
                    expect(page.locator("#appShell")).not_to_have_class(
                        re.compile(r"\bsidebar-open\b")
                    )
            page.set_viewport_size({"width": 1440, "height": 1000})
            page.goto(origin + base)
            page.screenshot(path=str(evidence / "operator-assessment.png"), full_page=True)
            assert not failures, failures
            context.close()
            browser.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
