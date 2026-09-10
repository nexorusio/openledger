"""Real Leaflet browser acceptance for reviewed Persona locations.

This is part of the isolated P3R PostgreSQL/Chromium gate.  It deliberately
uses the normal store review path to create coordinate selections and permits
only the local Flask server, pinned Leaflet runtime files, and blank map tiles.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

import pytest

from tests.p3r_full_browser_journey import _require_chromium, served_full_app
from tests.test_p3r_full_acceptance import FULL_ACCEPTANCE_ENV, full_app


pytestmark = [
    pytest.mark.browser,
    pytest.mark.skipif(
        os.getenv(FULL_ACCEPTANCE_ENV) != "1",
        reason="P3R Leaflet Chromium acceptance is enabled only with the full isolated gate.",
    ),
]


# Valid 1×1 transparent PNG. Tile contents are irrelevant to map geometry.
BLANK_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\x0dIDAT\x08\xd7c\xf8"
    b"\xff\xff?\x00\x05\xfe\x02\xfe\xa7^\xf6\x85\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _leaflet_runtime() -> tuple[Path, Path]:
    javascript = Path(os.getenv("OPENLEDGER_TEST_LEAFLET_JS", ""))
    stylesheet = Path(os.getenv("OPENLEDGER_TEST_LEAFLET_CSS", ""))
    assert javascript.is_file(), "CI must provide pinned Leaflet JS via OPENLEDGER_TEST_LEAFLET_JS"
    assert stylesheet.is_file(), "CI must provide pinned Leaflet CSS via OPENLEDGER_TEST_LEAFLET_CSS"
    return javascript, stylesheet


def _confine_map_browser(page, base_url: str, javascript: Path, stylesheet: Path) -> None:
    """Keep the browser in the fixture boundary while serving pinned Leaflet."""
    local_origin = (urlsplit(base_url).scheme, urlsplit(base_url).netloc)

    def route(request_route):
        request = request_route.request
        parsed = urlsplit(request.url)
        origin = (parsed.scheme, parsed.netloc)
        if origin == local_origin:
            request_route.continue_()
        elif parsed.netloc == "unpkg.com" and parsed.path.endswith("/leaflet.js"):
            request_route.fulfill(path=str(javascript), content_type="application/javascript")
        elif parsed.netloc == "unpkg.com" and parsed.path.endswith("/leaflet.css"):
            request_route.fulfill(path=str(stylesheet), content_type="text/css")
        elif parsed.netloc == "unpkg.com" and parsed.path.startswith("/leaflet@1.9.4/dist/images/"):
            request_route.fulfill(body=BLANK_PNG, content_type="image/png")
        elif parsed.netloc == "tiles.fixture.invalid":
            request_route.fulfill(body=BLANK_PNG, content_type="image/png")
        else:
            request_route.abort()

    page.route("**/*", route)


def _create_reviewed_locations(store, *, username: str, points: Iterable[tuple[str, float, float]]) -> str:
    """Persist real claims and append analyst coordinate selections for one Persona."""
    points = list(points)
    job_id = store.create_investigation([username], {})
    assert store.claim_next(f"worker:p3r-map-{username}")
    result = {
        "status": "completed",
        "usernames": [username],
        "individual_reports": [
            {
                "username": username,
                "claimed_profiles": [
                    {
                        "site_name": f"Map fixture {index}",
                        "url": f"https://fixture.invalid/{username}/{index}",
                        "confidence": "strong",
                        "evidence": {"location": label},
                    }
                    for index, (label, _latitude, _longitude) in enumerate(points)
                ],
            }
        ],
    }
    assert store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    persona_id = store.get_case(store.get_job(job_id)["case_id"])["personas"][0]["id"]
    claims_by_label = {
        claim["display_value"]: claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "current_location"
    }
    assert set(claims_by_label) == {label for label, _latitude, _longitude in points}
    for label, latitude, longitude in points:
        assert store.review_claim(
            claims_by_label[label]["id"],
            "approved",
            "map-fixture-reviewer",
            "Reviewed fixture coordinate.",
            latitude=str(latitude),
            longitude=str(longitude),
            coordinate_metadata={"method": "analyst_selected", "precision": "city"},
        ) == persona_id
    return persona_id


def _map_snapshot(page, points: list[tuple[str, float, float]]) -> dict:
    return page.evaluate(
        """points => {
            const map = window.personaMap;
            const size = map.getSize();
            const projected = points.map(([, latitude, longitude]) => {
                const point = map.latLngToContainerPoint([latitude, longitude]);
                return {x: point.x, y: point.y, reachable: point.x >= 0 && point.x <= size.x && point.y >= 0 && point.y <= size.y};
            });
            return {
                width: size.x,
                height: size.y,
                zoom: map.getZoom(),
                center: map.getCenter(),
                markerCount: document.querySelectorAll('.leaflet-marker-icon').length,
                projected,
            };
        }""",
        points,
    )


def _open_persona_map(page, base_url: str, persona_id: str, points: list[tuple[str, float, float]]) -> dict:
    page.goto(f"{base_url}/personas/{persona_id}", wait_until="networkidle")
    if not points:
        assert page.get_by_text("0 mapped", exact=True).count() == 1
        assert page.locator("#personaLocationMap").count() == 0
        return {}
    map_element = page.locator("#personaLocationMap")
    assert map_element.is_visible() is False
    assert page.evaluate("() => window.personaMap === undefined")
    page.locator('[data-persona-tab="contact"]').click()
    page.wait_for_function(
        """() => window.personaMap
            && window.personaMap.getSize().x > 0
            && window.personaMap.getSize().y > 0"""
    )
    return _map_snapshot(page, points)


def test_leaflet_persona_map_handles_hidden_mobile_marker_ranges(served_full_app, monkeypatch):
    """Exercise 0/1/2/10/100 reviewed locations in a real mobile Leaflet page."""
    javascript, stylesheet = _leaflet_runtime()
    base_url, _app, store, _reports = served_full_app
    monkeypatch.setenv("OPENLEDGER_MAP_TILE_URL", "https://tiles.fixture.invalid/{z}/{x}/{y}.png")
    scenarios = [
        [],
        [("Jakarta", -6.2088, 106.8456)],
        [("Jakarta", -6.2088, 106.8456), ("Depok", -6.4025, 106.7942)],
        [(f"Ten {index}", -6.5 + index / 100, 106.5 + index / 100) for index in range(10)],
        [(f"Hundred {index}", -7 + index / 100, 106 + index / 100) for index in range(100)],
    ]
    personas = [
        _create_reviewed_locations(store, username=f"mapcount{index}", points=points)
        if points else _create_reviewed_locations(store, username=f"mapcount{index}", points=[])
        for index, points in enumerate(scenarios)
    ]
    errors: list[str] = []
    sync_playwright = _require_chromium()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 740})
        page.set_default_timeout(15_000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        _confine_map_browser(page, base_url, javascript, stylesheet)
        try:
            for persona_id, points in zip(personas, scenarios, strict=True):
                snapshot = _open_persona_map(page, base_url, persona_id, points)
                if not points:
                    continue
                assert snapshot["width"] > 0 and snapshot["height"] > 0
                assert snapshot["markerCount"] == len(points)
                assert all(point["reachable"] for point in snapshot["projected"])
        finally:
            browser.close()
    assert errors == []


def test_leaflet_persona_map_groups_duplicates_keeps_view_on_resize_and_wraps_dateline(served_full_app, monkeypatch):
    javascript, stylesheet = _leaflet_runtime()
    base_url, _app, store, _reports = served_full_app
    monkeypatch.setenv("OPENLEDGER_MAP_TILE_URL", "https://tiles.fixture.invalid/{z}/{x}/{y}.png")
    duplicate_points = [
        ("Jakarta office", -6.2088, 106.8456),
        ("Jakarta duplicate provenance", -6.2088, 106.8456),
        ("Depok", -6.4025, 106.7942),
    ]
    dateline_points = [("East dateline", 10.0, 179.0), ("West dateline", 11.0, -179.0)]
    duplicate_persona = _create_reviewed_locations(store, username="mapduplicate", points=duplicate_points)
    dateline_persona = _create_reviewed_locations(store, username="mapdateline", points=dateline_points)
    errors: list[str] = []
    sync_playwright = _require_chromium()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 740})
        page.set_default_timeout(15_000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        _confine_map_browser(page, base_url, javascript, stylesheet)
        try:
            duplicate = _open_persona_map(page, base_url, duplicate_persona, duplicate_points)
            assert duplicate["markerCount"] == 2
            page.locator(".leaflet-marker-icon").first.click()
            popup = page.locator(".leaflet-popup-content")
            assert "Jakarta office" in popup.inner_text()
            assert "Jakarta duplicate provenance" in popup.inner_text()
            before_resize = page.evaluate("() => ({center: window.personaMap.getCenter(), zoom: window.personaMap.getZoom()})")
            page.set_viewport_size({"width": 740, "height": 390})
            page.wait_for_timeout(100)
            after_resize = page.evaluate("() => ({center: window.personaMap.getCenter(), zoom: window.personaMap.getZoom()})")
            assert after_resize == before_resize

            dateline = _open_persona_map(page, base_url, dateline_persona, dateline_points)
            assert dateline["markerCount"] == 2
            assert all(point["reachable"] for point in dateline["projected"])
            assert dateline["zoom"] >= 4
        finally:
            browser.close()
    assert errors == []
