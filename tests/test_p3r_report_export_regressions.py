"""Offline regressions for P3R legacy and Persona report export boundaries."""

from __future__ import annotations

import copy
import io
import sys
import types
from datetime import datetime, timezone

from PIL import Image

from maigret.report import (
    _BLANK_IMAGE_PATH,
    _pdf_report_link_callback,
    generate_report_context,
    save_html_report,
    save_pdf_report,
)
from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.sites import MaigretSite
from maigret.web import persona_pdf


def _png_bytes() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (920, 360), "#DCE8E8").save(output, format="PNG")
    return output.getvalue()


def _legacy_context(image_url: str) -> dict:
    result = MaigretCheckResult(
        "alice",
        "Example",
        "https://profiles.example.test/alice",
        MaigretCheckStatus.CLAIMED,
    )
    result.ids_data = {"image": image_url}
    return generate_report_context(
        [
            (
                "alice",
                "username",
                {
                    "Example": {
                        "username": "alice",
                        "url_main": "https://profiles.example.test",
                        "url_user": "https://profiles.example.test/alice",
                        "status": result,
                        "is_similar": False,
                        "site": MaigretSite("Example", {}),
                    }
                },
            )
        ]
    )


def _approved_location(
    claim_id: str,
    value: str,
    confidence: int,
    latitude=None,
    longitude=None,
) -> dict:
    return {
        "id": claim_id,
        "field_name": "current_location",
        "value": value,
        "display_value": value,
        "confidence": confidence,
        "review_status": "approved",
        "reviewed_by": "analyst",
        "reviewed_at": "2026-09-02T11:00:00+00:00",
        "first_seen_at": "2026-09-01T09:00:00+00:00",
        "last_seen_at": "2026-09-02T10:00:00+00:00",
        "latitude": latitude,
        "longitude": longitude,
        "reviews": [],
        "evidence": [
            {
                "source_name": "Approved public record",
                "source_url": f"https://example.test/{claim_id}",
                "evidence_type": "cited_public_web",
                "observed_at": "2026-09-02T10:30:00+00:00",
            }
        ],
    }


def _persona_with_locations(*locations: dict) -> dict:
    return {
        "id": "persona-report-regression",
        "case_id": "case-report-regression",
        "case_title": "Public integrity inquiry",
        "display_name": "Alice Example",
        "claims": list(locations),
    }


def test_legacy_pdf_never_hands_profile_media_to_xhtml2pdf(monkeypatch, tmp_path):
    captured = {}

    def pisa_document(source, dest, **kwargs):
        captured["html"] = source.getvalue()
        captured["callback"] = kwargs["link_callback"]
        dest.write(b"%PDF-offline-fixture")

    fake_xhtml2pdf = types.ModuleType("xhtml2pdf")
    fake_xhtml2pdf.pisa = types.SimpleNamespace(pisaDocument=pisa_document)
    monkeypatch.setitem(sys.modules, "xhtml2pdf", fake_xhtml2pdf)

    hostile_media = "https://media.invalid/redirect-to-private-or-stall.png"
    target = tmp_path / "legacy.pdf"
    save_pdf_report(str(target), _legacy_context(hostile_media))

    assert hostile_media not in captured["html"]
    assert "i.imgur.com/040fmbw.png" not in captured["html"]
    for resource in (
        hostile_media,
        "http://127.0.0.1/private.png",
        "https://media.invalid/very-large.png",
        "file:///etc/passwd",
    ):
        assert captured["callback"](resource, "") == _BLANK_IMAGE_PATH
        assert _pdf_report_link_callback(resource, "") == _BLANK_IMAGE_PATH


def test_legacy_html_uses_the_bundled_placeholder_for_a_missing_photo(tmp_path):
    context = _legacy_context("")
    target = tmp_path / "legacy.html"

    save_html_report(str(target), context)

    rendered = target.read_text(encoding="utf-8")
    assert "i.imgur.com/040fmbw.png" not in rendered
    assert "data:image/png;base64," in rendered


def test_persona_pdf_labels_one_map_as_excerpt_and_lists_every_location(monkeypatch):
    persona = _persona_with_locations(
        _approved_location("jakarta", "Jakarta, Indonesia", 95, -6.2088, 106.8456),
        _approved_location("depok", "Depok, Indonesia", 90, -6.4025, 106.7942),
        _approved_location("unmapped", "Reviewed area without coordinates", 80),
    )
    map_calls = []
    map_cards = []
    listed_locations = []
    original_map_card = persona_pdf._location_map_card
    original_item_flowables = persona_pdf._report_item_flowables

    def render_map(latitude, longitude):
        map_calls.append((latitude, longitude))
        return _png_bytes()

    def map_card(location, map_bytes, styles, *, location_count):
        card = original_map_card(
            location, map_bytes, styles, location_count=location_count
        )
        map_cards.append((location, location_count, card))
        return card

    def item_flowables(item, styles):
        if item.get("label") == "Current location":
            listed_locations.append(item["value"])
        return original_item_flowables(item, styles)

    monkeypatch.setattr(persona_pdf, "render_location_map", render_map)
    monkeypatch.setattr(persona_pdf, "_location_map_card", map_card)
    monkeypatch.setattr(persona_pdf, "_report_item_flowables", item_flowables)

    pdf_bytes = persona_pdf.generate_persona_pdf(
        persona,
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert pdf_bytes.startswith(b"%PDF-")
    assert map_calls == [(-6.2088, 106.8456)]
    assert map_cards[0][1] == 3
    assert persona_pdf._location_map_caption(
        map_cards[0][0],
        location_count=map_cards[0][1],
        map_available=True,
        coordinates="",
    ).startswith(
        "Map excerpt (1 of 3 approved locations)"
    )
    assert listed_locations == [
        "Jakarta, Indonesia",
        "Depok, Indonesia",
        "Reviewed area without coordinates",
    ]


def test_persona_pdf_leaves_unmapped_approved_locations_unmapped(monkeypatch):
    persona = _persona_with_locations(
        _approved_location("area", "Reviewed area", 95),
        _approved_location("invalid", "Invalid legacy coordinate", 90, float("nan"), 1),
    )
    listed_locations = []
    original_item_flowables = persona_pdf._report_item_flowables
    monkeypatch.setattr(
        persona_pdf,
        "render_location_map",
        lambda *args: (_ for _ in ()).throw(AssertionError("map should not render")),
    )

    def item_flowables(item, styles):
        if item.get("label") == "Current location":
            listed_locations.append(item["value"])
        return original_item_flowables(item, styles)

    monkeypatch.setattr(persona_pdf, "_report_item_flowables", item_flowables)

    pdf_bytes = persona_pdf.generate_persona_pdf(
        copy.deepcopy(persona),
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert pdf_bytes.startswith(b"%PDF-")
    assert listed_locations == ["Reviewed area", "Invalid legacy coordinate"]
