"""Chromium acceptance of the real Flask assessment and report screens.

Only collection responses use the shared deterministic fixture. Chromium talks to
an actual loopback Flask server; external browser requests are always blocked.
CI requires this test and rejects missing Chromium/dependencies rather than skip.
"""

import os
from pathlib import Path
import re
import threading
import uuid
from urllib.parse import urlsplit

import pytest
from sqlalchemy import insert
from werkzeug.serving import make_server

from maigret.web.case_store import claim_evidence, persona_claims, utcnow
from maigret.web.pipeline_ingestion import converge_legacy_persona
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


def test_browser_four_inputs_assessment_reject_approve_and_report(
    application_journey, tmp_path, monkeypatch
):
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
    approved_urls = [
        "https://www.linkedin.com/in/synthetic-person/",
        "https://www.threads.com/@synthetic.person",
    ]
    approved_live_job_id = store.create_investigation(
        ["synthetic.person"],
        {
            "investigation_spec": {
                "pipeline_id": "p2-e2e-v1",
                "processing_mode": "same_subject",
                "subject_label": "Synthetic Person",
                "identifiers": [
                    {"type": "username", "value": "synthetic.person"}
                ],
                "discovery_basis": "approved_source_fetch",
                "approved_source_urls": approved_urls,
                "enable_approved_source_fetch": True,
            }
        },
        kind="refresh",
    )
    legacy_job_id = store.create_investigation(
        ["legacy.synthetic"],
        {"subject_label": "Legacy Synthetic Person"},
    )
    legacy_case_id = store.get_job(legacy_job_id)["case_id"]
    legacy_persona_id = store.get_case(legacy_case_id)["personas"][0]["id"]
    legacy_claim_id = str(uuid.uuid4())
    legacy_evidence_id = str(uuid.uuid4())
    legacy_summary_claim_id = str(uuid.uuid4())
    legacy_summary_evidence_id = str(uuid.uuid4())
    now = utcnow()
    with store.engine.begin() as connection:
        connection.execute(
            insert(persona_claims).values(
                id=legacy_claim_id,
                persona_id=legacy_persona_id,
                field_name="occupation",
                value="Synthetic researcher",
                display_value="Synthetic researcher",
                normalized_value="synthetic researcher",
                confidence=80,
                review_status="approved",
                source_engine="synthetic_public_document",
                source_job_id=None,
                fingerprint=uuid.uuid4().hex * 2,
                first_seen_at=now,
                last_seen_at=now,
                created_at=now,
                updated_at=now,
                reviewed_at=now,
                reviewed_by="legacy-analyst",
            )
        )
        connection.execute(
            insert(claim_evidence).values(
                id=legacy_evidence_id,
                claim_id=legacy_claim_id,
                evidence_type="public_document",
                source_name="Synthetic retained source",
                source_url="https://example.test/legacy-source",
                details={"fixture": True},
                fingerprint=uuid.uuid4().hex * 2,
                observed_at=now,
            )
        )
        connection.execute(
            insert(persona_claims).values(
                id=legacy_summary_claim_id,
                persona_id=legacy_persona_id,
                field_name="summary",
                value="Retained legacy profile summary",
                display_value="Retained legacy profile summary",
                normalized_value="retained legacy profile summary",
                confidence=80,
                review_status="approved",
                source_engine="synthetic_public_document",
                source_job_id=None,
                fingerprint=uuid.uuid4().hex * 2,
                first_seen_at=now,
                last_seen_at=now,
                created_at=now,
                updated_at=now,
                reviewed_at=now,
                reviewed_by="legacy-analyst",
            )
        )
        connection.execute(
            insert(claim_evidence).values(
                id=legacy_summary_evidence_id,
                claim_id=legacy_summary_claim_id,
                evidence_type="public_document",
                source_name="Synthetic summary source",
                source_url="https://example.test/legacy-summary-source",
                details={"fixture": True},
                fingerprint=uuid.uuid4().hex * 2,
                observed_at=now,
            )
        )
    converted_job_id = store.create_investigation(
        ["converted.synthetic"],
        {"subject_label": "Converted Synthetic Person"},
    )
    converted_case_id = store.get_job(converted_job_id)["case_id"]
    converted_persona_id = store.get_case(converted_case_id)["personas"][0]["id"]
    converted_location_claim_id = str(uuid.uuid4())
    with store.engine.begin() as connection:
        connection.execute(
            insert(persona_claims).values(
                id=converted_location_claim_id,
                persona_id=converted_persona_id,
                field_name="current_location",
                value="Bandar Lampung, Indonesia",
                display_value="Bandar Lampung, Indonesia",
                normalized_value="bandar lampung, indonesia",
                confidence=80,
                review_status="approved",
                source_engine="synthetic_public_document",
                source_job_id=None,
                fingerprint=uuid.uuid4().hex * 2,
                first_seen_at=now,
                last_seen_at=now,
                created_at=now,
                updated_at=now,
                reviewed_at=now,
                reviewed_by="legacy-analyst",
                latitude=-5.3971,
                longitude=105.2668,
            )
        )
        connection.execute(
            insert(claim_evidence).values(
                id=str(uuid.uuid4()),
                claim_id=converted_location_claim_id,
                evidence_type="public_document",
                source_name="Synthetic location source",
                source_url="https://example.test/legacy-location-source",
                details={"fixture": True, "coordinate_precision": "city"},
                fingerprint=uuid.uuid4().hex * 2,
                observed_at=now,
            )
        )
    from maigret.web import pipeline_evidence

    coordinate_adapter = pipeline_evidence.legacy_claim_with_coordinates
    monkeypatch.setattr(
        pipeline_evidence,
        "legacy_claim_with_coordinates",
        lambda claim, **_kwargs: dict(claim),
    )
    convergence = converge_legacy_persona(
        store, converted_case_id, converted_persona_id, dry_run=False
    )
    assert convergence["auto_finalized"] is False
    assert convergence["qc_created"] is False
    monkeypatch.setattr(
        pipeline_evidence, "legacy_claim_with_coordinates", coordinate_adapter
    )
    repair = converge_legacy_persona(
        store, converted_case_id, converted_persona_id, dry_run=False
    )
    assert repair["evidence"]["pending_coordinate_repair_count"] == 1
    assert len(repair["evidence"]["coordinate_repair_request_ids"]) == 1
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
            map_tile_requests = []
            external_tile_requests = []
            leaflet_stub = """
                window.L = {
                  map(element) { return { element, setView() {}, fitBounds() {} }; },
                  tileLayer(template) {
                    return { addTo(map) {
                      const image = document.createElement('img');
                      image.className = 'leaflet-tile';
                      image.src = template.replace('{z}', '8').replace('{x}', '0').replace('{y}', '0');
                      map.element.appendChild(image);
                      return this;
                    }};
                  },
                  marker() {
                    const marker = { addTo() { return marker; }, bindPopup() { return marker; } };
                    return marker;
                  }
                };
            """
            context.add_init_script(leaflet_stub)

            def route_browser_request(route):
                url = route.request.url
                parsed = urlsplit(url)
                if url.startswith(origin + "/map-tiles/"):
                    map_tile_requests.append(url)
                    route.fulfill(
                        status=200,
                        content_type="image/png",
                        body=(
                            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
                            b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
                            b"\x00\x00\x00\x0dIDAT\x08\xd7c\xf8\xcf\xc0\xf0\x1f\x00\x05\x00\x01\xff\x89\x99=\x1d"
                            b"\x00\x00\x00\x00IEND\xaeB`\x82"
                        ),
                    )
                else:
                    if parsed.hostname == "tile.openstreetmap.org":
                        external_tile_requests.append(url)
                    if url.startswith(origin + "/"):
                        route.continue_()
                    else:
                        route.abort()

            context.route("**/*", route_browser_request)
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

            # Approved-source progress has its own runtime view. It must not
            # touch absent profile-discovery stats, and two URLs using the same
            # collector must remain two independently auditable rows.
            page.goto(origin + f"/live/{approved_live_job_id}")
            expect(page.get_by_role("heading", name="Approved source fetch")).to_be_visible()
            page.evaluate(
                """urls => {
                    urls.forEach((sourceUrl, index) => onScanEvent({data: JSON.stringify({
                        type: 'collector_planned',
                        collector: 'approved_public_source_fetch',
                        task_id: 'approved-task-' + index,
                        input_type: 'public_url',
                        input_value: sourceUrl,
                        source_url: sourceUrl,
                    })}));
                    onScanEvent({data: JSON.stringify({
                        type: 'approved_source_stage',
                        collector: 'approved_public_source_fetch',
                        task_id: 'approved-task-0',
                        input_type: 'public_url',
                        input_value: urls[0],
                        source_url: urls[0],
                        stage: 'maigret',
                        status: 'running',
                        message: 'Running exact-site Maigret parser.',
                    })});
                    onScanEvent({data: JSON.stringify({
                        type: 'collector_completed',
                        collector: 'approved_public_source_fetch',
                        task_id: 'approved-task-0',
                        input_type: 'public_url',
                        input_value: urls[0],
                        source_url: urls[0],
                        outcome: 'candidate',
                        observations: 2,
                    })});
                }""",
                approved_urls,
            )
            expect(page.locator("#engine-progress-body tr")).to_have_count(2)
            expect(page.locator("#stat-approved-completed")).to_have_text("1")
            expect(page.locator("#engine-progress-body")).to_contain_text(approved_urls[0])
            expect(page.locator("#engine-progress-body")).to_contain_text(approved_urls[1])

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
                    "heading", name="Review submitted evidence and discoveries"
                )
            ).to_be_visible()
            for tab_name in ("Review queue", "Engine log"):
                expect(page.get_by_role("tab", name=tab_name, exact=False)).to_be_visible()
            draft_persona_tabs = page.locator("[data-persona-tab]").evaluate_all(
                "tabs => tabs.map(tab => tab.dataset.personaTab)"
            )
            assert draft_persona_tabs == [
                "identity",
                "contact",
                "digital",
                "affiliations",
                "public_exposure",
                "records",
                "review",
                "engines",
            ]
            expect(page.get_by_text("Step 1", exact=True)).to_be_visible()
            expect(page.get_by_text("Step 2", exact=True)).to_be_visible()
            review_table = page.locator("table.assessment-review-table")
            expect(review_table).to_be_visible()
            assert review_table.locator("thead th").all_text_contents() == [
                "Finding ↕", "Category ↕", "Assessment ↕", "Evidence ↕",
                "Decision ↕", "Actions",
            ]
            page.get_by_role("button", name="Finding", exact=True).click()
            expect(page).to_have_url(re.compile(r"[?&]sort=finding&direction=ascending"))
            expect(page.locator("table.assessment-review-table")).to_be_visible()

            # Reject and then approve the same finding to prove both controls work
            # while retaining the complete decision trail used by report snapshots.
            decision = page.locator('form[action$="/decision"]:visible').first
            decision_action = decision.get_attribute("action")
            assert decision_action
            assert decision.locator('[name="reason"]').get_attribute("required") is None
            decision.get_by_role("button", name="Reject").click()
            expect(
                page.locator(".badge-soft.danger:visible", has_text="Rejected").first
            ).to_be_visible()
            decision_cell = page.locator(
                f'form[action="{decision_action}"]'
            ).locator("xpath=ancestor::td")
            decision_cell.get_by_role("button", name="Edit decision").click()
            decision = decision_cell.locator('form[action$="/decision"]:visible')
            decision.locator("summary").click()
            decision.locator('[name="reason"]').fill("Approved after reviewing the cited source.")
            decision.get_by_role("button", name="Approve").click()
            expect(
                page.locator(".badge-soft.success:visible", has_text="Approved").first
            ).to_be_visible()

            workspace = pipeline.get_workspace(case_id, persona_id)
            assert any(item.get("latest_decision") == "include" for item in workspace["shortlist"])
            assert {row["id"] for row in originals} <= {
                row["id"] for row in pipeline.list_observations(case_id, persona_id, limit=500)
            }

            # Submitted inputs and every positive engine candidate now enter the
            # same queue. Resolve the remaining rows before the wizard can open
            # the approved Persona or export its immutable report snapshot.
            for _ in range(workspace["review_pending_count"]):
                pending = page.locator('form[action$="/decision"]:visible').first
                expect(pending).to_be_visible()
                pending.get_by_role("button", name="Reject").click()
            workspace = pipeline.get_workspace(case_id, persona_id)
            assert workspace["review_pending_count"] == 0

            page.get_by_role("button", name="Proceed to Persona").click()
            expect(
                page.locator(".persona-profile-header .eyebrow", has_text="Persona")
            ).to_be_visible()
            expect(page).to_have_url(origin + base + "/persona")
            approved_persona_tabs = page.locator("[data-persona-tab]").evaluate_all(
                "tabs => tabs.map(tab => tab.dataset.personaTab)"
            )
            assert approved_persona_tabs == draft_persona_tabs

            # A legacy archive, a converted legacy Persona, and a native P2
            # Persona must render the same page/component contract at every
            # supported viewport. State may change actions and badges, never
            # the product layout.
            for viewport, label in (
                ({"width": 1440, "height": 1000}, "desktop"),
                ({"width": 768, "height": 1024}, "tablet"),
                ({"width": 390, "height": 844}, "mobile"),
            ):
                page.set_viewport_size(viewport)
                for model, path in (
                    ("legacy", f"/personas/{legacy_persona_id}"),
                    ("converted", f"/personas/{converted_persona_id}"),
                    ("p2", base + "/persona"),
                ):
                    page.goto(origin + path)
                    expect(page.locator(".persona-profile-header")).to_be_visible()
                    expect(page.locator(".persona-context-nav")).to_be_visible()
                    expect(page.locator(".persona-tabs")).to_be_visible()
                    assert page.locator("[data-persona-tab]").evaluate_all(
                        "tabs => tabs.map(tab => tab.dataset.personaTab)"
                    ) == draft_persona_tabs
                    if model == "converted":
                        expect(
                            page.get_by_role("heading", name="Approved locations")
                        ).to_be_visible()
                        expect(page.locator("#personaLocationMap")).to_be_visible()
                        assert "-5.3971" in page.content()
                        assert "105.2668" in page.content()
                        assert map_tile_requests
                        assert not external_tile_requests
                    if model == "legacy":
                        summary_record = page.locator(
                            ".approved-persona-record",
                            has_text="Retained legacy profile summary",
                        )
                        expect(summary_record).to_be_visible()
                        summary_record.get_by_role(
                            "button", name="View evidence"
                        ).click()
                        expect(
                            page.locator("dialog[open]", has_text="Summary")
                        ).to_be_visible()
                        page.get_by_role("button", name="Close evidence").click()
                    assert_no_page_overflow(page, f"{label} {model} Persona")
                    page.screenshot(
                        path=str(evidence / f"persona-{model}-{label}.png"),
                        full_page=True,
                    )

            with page.expect_download() as download_info:
                page.get_by_role("button", name="Export Persona PDF").click()
            download = download_info.value
            report_path = download.path()
            assert report_path and Path(report_path).read_bytes().startswith(b"%PDF")
            assert download.suggested_filename.startswith("OpenLedger-Investigation-")

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
