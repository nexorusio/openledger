import io
from datetime import datetime, timezone

from PIL import Image

from maigret.web import persona_pdf as persona_pdf_module
from maigret.web.persona_pdf import (
    build_investigation_report_view,
    build_persona_export_snapshot,
    generate_persona_pdf,
    persona_pdf_filename,
)


def _persona():
    common = {
        "first_seen_at": "2026-09-01T09:00:00+00:00",
        "last_seen_at": "2026-09-02T10:00:00+00:00",
        "latitude": None,
        "longitude": None,
        "reviews": [],
    }
    return {
        "id": "persona-123",
        "case_id": "case-456",
        "case_title": "Public integrity inquiry",
        "display_name": "Alice Example",
        "claims": [
            {
                **common,
                "id": "claim-approved",
                "field_name": "summary",
                "value": "Approved summary",
                "display_value": "Approved summary with public context",
                "confidence": 90,
                "review_status": "approved",
                "reviewed_by": "analyst",
                "reviewed_at": "2026-09-02T11:00:00+00:00",
                "reviews": [
                    {
                        "decision": "approved",
                        "reviewer": "analyst",
                        "note": "Compared with the cited page.",
                        "created_at": "2026-09-02T11:00:00+00:00",
                    }
                ],
                "evidence": [
                    {
                        "source_name": "Example public profile",
                        "source_url": "https://example.test/alice?source=public",
                        "evidence_type": "cited_public_web",
                        "observed_at": "2026-09-02T10:30:00+00:00",
                    }
                ],
            },
            {
                **common,
                "id": "claim-pending",
                "field_name": "email",
                "value": "pending@example.test",
                "display_value": "pending@example.test",
                "confidence": 70,
                "review_status": "pending",
                "reviewed_by": None,
                "reviewed_at": None,
                "evidence": [],
            },
            {
                **common,
                "id": "claim-rejected",
                "field_name": "address",
                "value": "Rejected private address",
                "display_value": "Rejected private address",
                "confidence": 60,
                "review_status": "rejected",
                "reviewed_by": "analyst",
                "reviewed_at": "2026-09-02T11:10:00+00:00",
                "evidence": [],
            },
        ],
    }


def _approved_claim(
    claim_id,
    field_name,
    value,
    confidence,
    *,
    source_url="https://example.test/public-record",
    display_value=None,
    latitude=None,
    longitude=None,
):
    return {
        "id": claim_id,
        "field_name": field_name,
        "value": value,
        "display_value": display_value if display_value is not None else value,
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
                "source_url": source_url,
                "evidence_type": "cited_public_web",
                "observed_at": "2026-09-02T10:30:00+00:00",
            }
        ],
    }


def _rich_persona():
    shared_affiliation_source = "https://example.test/curriculum-vitae"
    persona = {
        "id": "persona-rich",
        "case_id": "case-rich",
        "case_title": "Public integrity inquiry",
        "display_name": "Alice Example",
        "claims": [
            _approved_claim(
                "name",
                "full_name",
                "Alice Example",
                96,
                source_url="https://example.test/alice",
            ),
            _approved_claim(
                "photo",
                "photograph",
                "https://media.example.test/alice.jpg",
                91,
                source_url="https://example.test/alice",
            ),
            _approved_claim(
                "summary",
                "summary",
                "Jakarta-based technology executive with a public corporate profile.",
                84,
            ),
            _approved_claim(
                "location",
                "current_location",
                "Jakarta, Indonesia",
                88,
                latitude=-6.2088,
                longitude=106.8456,
            ),
            _approved_claim(
                "position",
                "occupation",
                "Chief Technology Officer",
                86,
                source_url=shared_affiliation_source,
            ),
            _approved_claim(
                "company",
                "company",
                "Example Teknologi",
                82,
                source_url=shared_affiliation_source,
            ),
            _approved_claim("email", "email", "alice@example.test", 78),
            _approved_claim(
                "social",
                "social_account",
                {
                    "platform": "linkedin.com",
                    "username": "alice-example",
                    "url": "https://linkedin.com/in/alice-example",
                },
                89,
                display_value="Alice Example on LinkedIn",
            ),
            _approved_claim(
                "asset",
                "company_ownership",
                "Declared shareholder in Example Holdings",
                73,
            ),
            _approved_claim(
                "risk",
                "offshore_database_match",
                "Name match requiring analyst disambiguation",
                61,
            ),
        ],
    }
    return persona


def _png_bytes(size, color):
    output = io.BytesIO()
    Image.new("RGB", size, color).save(output, format="PNG")
    return output.getvalue()


def test_persona_export_snapshot_contains_only_approved_records_and_provenance():
    generated_at = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
    snapshot = build_persona_export_snapshot(
        _persona(), generated_at=generated_at, generated_by="analyst"
    )

    assert snapshot["approved_count"] == 1
    assert snapshot["source_count"] == 1
    assert len(snapshot["snapshot_sha256"]) == 64
    exported_claims = [
        claim
        for group in snapshot["groups"]
        for field in group["fields"]
        for claim in field["claims"]
    ]
    assert [claim["id"] for claim in exported_claims] == ["claim-approved"]
    assert exported_claims[0]["evidence"][0]["source_url"] == (
        "https://example.test/alice?source=public"
    )
    assert exported_claims[0]["approval_note"] == ("Compared with the cited page.")


def test_investigation_view_is_human_centred_and_scores_every_fact():
    snapshot = build_persona_export_snapshot(
        _rich_persona(),
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
        generated_by="analyst",
    )

    report = build_investigation_report_view(snapshot)

    assert report["name"] == "Alice Example"
    assert report["name_confidence"] == 96
    assert report["photograph"]["confidence"] == 91
    assert report["locations"][0]["value"] == "Jakarta, Indonesia"
    assert report["affiliations"][0]["value"] == "Chief Technology Officer"
    assert report["affiliations"][0]["secondary"] == "Example Teknologi"
    assert report["affiliations"][0]["confidence"] == 86
    assert report["affiliations"][0]["secondary_confidence"] == 82
    assert report["contacts"][0]["value"] == "alice@example.test"
    assert report["digital_presence"][0]["value"] == "LinkedIn - @alice-example"
    assert report["assets"][0]["confidence"] == 73
    assert report["risk_indicators"][0]["confidence"] == 61
    for section in (
        "alternate_names",
        "locations",
        "addresses",
        "affiliations",
        "contacts",
        "digital_presence",
        "assets",
        "risk_indicators",
    ):
        assert all(isinstance(item["confidence"], int) for item in report[section])


def test_affiliation_pairing_accepts_the_same_url_observed_at_different_times():
    persona = _rich_persona()
    company = next(
        claim for claim in persona["claims"] if claim["field_name"] == "company"
    )
    company["evidence"][0]["observed_at"] = "2026-09-02T10:31:00+00:00"
    snapshot = build_persona_export_snapshot(
        persona,
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
        generated_by="analyst",
    )

    report = build_investigation_report_view(snapshot)

    assert len(report["affiliations"]) == 1
    assert report["affiliations"][0]["secondary"] == "Example Teknologi"


def test_pending_photo_and_location_never_trigger_media_requests(monkeypatch):
    persona = _persona()
    pending_photo = _approved_claim(
        "pending-photo",
        "photograph",
        "https://media.example.test/pending.jpg",
        99,
        latitude=None,
        longitude=None,
    )
    pending_photo["review_status"] = "pending"
    pending_location = _approved_claim(
        "pending-location",
        "current_location",
        "Pending City",
        99,
        latitude=1.0,
        longitude=2.0,
    )
    pending_location["review_status"] = "pending"
    persona["claims"].extend([pending_photo, pending_location])
    monkeypatch.setattr(
        persona_pdf_module,
        "load_approved_portrait",
        lambda url: (_ for _ in ()).throw(AssertionError("pending photo fetched")),
    )
    monkeypatch.setattr(
        persona_pdf_module,
        "render_location_map",
        lambda latitude, longitude: (_ for _ in ()).throw(
            AssertionError("pending location fetched")
        ),
    )

    pdf_bytes = generate_persona_pdf(
        persona,
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert pdf_bytes.startswith(b"%PDF-")


def test_approved_media_failures_never_block_the_report(monkeypatch):
    monkeypatch.setattr(
        persona_pdf_module,
        "load_approved_portrait",
        lambda url: (_ for _ in ()).throw(OSError("photo unavailable")),
    )
    monkeypatch.setattr(
        persona_pdf_module,
        "render_location_map",
        lambda latitude, longitude: (_ for _ in ()).throw(OSError("map unavailable")),
    )

    pdf_bytes = generate_persona_pdf(
        _rich_persona(),
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert pdf_bytes.startswith(b"%PDF-")
    assert b"%%EOF" in pdf_bytes[-1024:]


def test_long_approved_name_does_not_break_the_hero_layout():
    persona = _persona()
    long_name = "Alice Example " * 120
    persona["claims"].append(
        _approved_claim("long-name", "full_name", long_name, 95)
    )

    pdf_bytes = generate_persona_pdf(
        persona,
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert pdf_bytes.startswith(b"%PDF-")
    assert b"%%EOF" in pdf_bytes[-1024:]


def test_investigation_pdf_embeds_approved_photo_and_map(monkeypatch):
    photo_calls = []
    map_calls = []

    def approved_photo(url):
        photo_calls.append(url)
        return _png_bytes((240, 320), "#315D7D")

    def approved_map(latitude, longitude):
        map_calls.append((latitude, longitude))
        return _png_bytes((920, 360), "#DCE8E8")

    monkeypatch.setattr(persona_pdf_module, "load_approved_portrait", approved_photo)
    monkeypatch.setattr(persona_pdf_module, "render_location_map", approved_map)

    pdf_bytes = generate_persona_pdf(
        _rich_persona(),
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert photo_calls == ["https://media.example.test/alice.jpg"]
    assert map_calls == [(-6.2088, 106.8456)]
    assert pdf_bytes.startswith(b"%PDF-")
    assert pdf_bytes.count(b"/Subtype /Image") >= 2


def test_persona_export_preserves_meaningful_symbols_and_script_join_controls():
    persona = _persona()
    approved_value = "37° © ™ ❤ می\u200cرود\u202e\ue000👤"
    persona["claims"][0]["display_value"] = approved_value

    snapshot = build_persona_export_snapshot(
        persona,
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
        generated_by="analyst",
    )

    assert snapshot["approved_claims"][0]["value"] == "37° © ™ ❤ می\u200cرود"


def test_persona_pdf_is_self_contained_and_uses_safe_filename():
    generated_at = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
    pdf_bytes = generate_persona_pdf(
        _persona(), generated_by="analyst", generated_at=generated_at
    )

    assert pdf_bytes.startswith(b"%PDF-")
    assert b"%%EOF" in pdf_bytes[-1024:]
    assert len(pdf_bytes) > 5000
    assert persona_pdf_filename(_persona(), generated_at=generated_at) == (
        "openledger-investigation-report-alice-example-20260902T120000Z.pdf"
    )


def test_persona_pdf_paginates_long_approved_values():
    persona = _persona()
    persona["claims"][0]["display_value"] = " ".join(["Approved context"] * 350)

    pdf_bytes = generate_persona_pdf(
        persona,
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert pdf_bytes.startswith(b"%PDF-")
    assert b"%%EOF" in pdf_bytes[-1024:]


def test_persona_pdf_paginates_long_rtl_approved_values():
    persona = _persona()
    persona["claims"][0]["display_value"] = " ".join(
        ["معلومات عامة موثقة من مصدر عام"] * 90
    )

    pdf_bytes = generate_persona_pdf(
        persona,
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert pdf_bytes.startswith(b"%PDF-")
    assert b"%%EOF" in pdf_bytes[-1024:]


def test_persona_export_uses_only_the_current_approval_note():
    persona = _persona()
    persona["claims"][0]["reviews"] = [
        {
            "decision": "approved",
            "reviewer": "second-analyst",
            "note": None,
            "created_at": "2026-09-02T12:00:00+00:00",
        },
        {
            "decision": "rejected",
            "reviewer": "first-analyst",
            "note": "Rejected pending clarification.",
            "created_at": "2026-09-02T11:30:00+00:00",
        },
        {
            "decision": "approved",
            "reviewer": "first-analyst",
            "note": "Superseded approval note.",
            "created_at": "2026-09-02T11:00:00+00:00",
        },
    ]

    snapshot = build_persona_export_snapshot(
        persona,
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
        generated_by="second-analyst",
    )
    exported_claim = next(
        claim
        for group in snapshot["groups"]
        for field in group["fields"]
        for claim in field["claims"]
    )

    assert exported_claim["approval_note"] == ""


def test_persona_export_preserves_cjk_text_for_font_fallback():
    persona = _persona()
    persona["display_name"] = "公開人物"
    persona["claims"][0]["display_value"] = "公開情報に基づく承認済み記録"

    snapshot = build_persona_export_snapshot(
        persona,
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
        generated_by="analyst",
    )
    pdf_bytes = generate_persona_pdf(
        persona,
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert snapshot["display_name"] == "公開人物"
    assert pdf_bytes.startswith(b"%PDF-")


def test_persona_export_preserves_multiscript_and_rtl_text():
    persona = _persona()
    persona["display_name"] = "علي כהן"
    persona["claims"][0][
        "display_value"
    ] = "सार्वजनिक তথ্য สาธารณะ — معلومات عامة — מידע ציבורי"

    snapshot = build_persona_export_snapshot(
        persona,
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
        generated_by="analyst",
    )
    pdf_bytes = generate_persona_pdf(
        persona,
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert snapshot["display_name"] == "علي כהן"
    assert snapshot["approved_claims"][0]["value"] == (
        "सार्वजनिक তথ্য สาธารณะ — معلومات عامة — מידע ציבורי"
    )
    assert pdf_bytes.startswith(b"%PDF-")


def test_font_markup_uses_actual_fallback_glyph_coverage():
    style = persona_pdf_module.ParagraphStyle(
        "CoverageTest", fontName="Helvetica", fontSize=10
    )
    style.openledger_primary_coverage = frozenset(range(32, 127))
    style.openledger_fallback_fonts = (
        ("DevanagariFallback", frozenset({ord("न"), ord("म")})),
        ("ThaiFallback", frozenset({ord("ไ"), ord("ท")})),
    )

    markup = persona_pdf_module._escaped_paragraph_text("Name नम ไทย", style)

    assert '<font name="DevanagariFallback">नम</font>' in markup
    assert '<font name="ThaiFallback">ไท</font>' in markup


def test_font_markup_keeps_join_controls_inside_the_script_fallback_run():
    style = persona_pdf_module.ParagraphStyle(
        "JoinControlTest", fontName="Helvetica", fontSize=10
    )
    style.openledger_primary_coverage = frozenset(
        {*range(32, 127), ord("\u200c"), ord("\u200d")}
    )
    style.openledger_fallback_fonts = (
        (
            "DevanagariFallback",
            frozenset({ord("क"), ord("्"), ord("ष")}),
        ),
    )

    markup = persona_pdf_module._escaped_paragraph_text("क्\u200dष", style)

    assert markup == '<font name="DevanagariFallback">क्\u200dष</font>'


def test_register_fonts_computes_primary_coverage_once_per_snapshot(monkeypatch):
    monkeypatch.setattr(persona_pdf_module, "_font_paths", lambda: (None, None))
    monkeypatch.setattr(persona_pdf_module, "_fallback_font_paths", lambda: ())
    coverage_calls = []

    def record_coverage(font_name):
        coverage_calls.append(font_name)
        return frozenset(range(32, 127))

    monkeypatch.setattr(persona_pdf_module, "_font_coverage", record_coverage)

    fonts = persona_pdf_module._register_fonts("A" * 5000 + "न" * 5000)

    assert fonts == ("Helvetica", "Helvetica-Bold", ())
    assert coverage_calls == ["Helvetica"]


def test_mixed_rtl_paragraph_shapes_only_non_rtl_visual_words(
    monkeypatch,
):
    class IdentityReshaper:
        @staticmethod
        def reshape(value):
            return value

    monkeypatch.setattr(persona_pdf_module, "_arabic_reshaper", IdentityReshaper())
    monkeypatch.setattr(persona_pdf_module, "_bidi_get_display", lambda value: value)
    regular_font, bold_font, fallback_fonts = persona_pdf_module._register_fonts("नम")
    styles = persona_pdf_module._styles(
        regular_font,
        bold_font,
        fallback_fonts,
    )
    shaped_words = []

    def record_shaped_word(word):
        shaped_words.append(
            "".join(
                str(fragment_text)
                for fragment, fragment_text in word[1:]
                if not hasattr(fragment, "cbDefn")
            )
        )
        return word

    monkeypatch.setattr(persona_pdf_module, "shapeFragWord", record_shaped_word)

    assert all(style.shaping == 1 for style in styles.values())
    paragraph = persona_pdf_module._paragraph(
        "सार्वजनिक তথ্য สาธารณะ — مرحبا بالعالم",
        styles["body"],
    )
    paragraph.wrap(200, 400)
    assert paragraph.style.shaping == 0
    assert any("सार्वजनिक" in word for word in shaped_words)
    assert any("তথ্য" in word for word in shaped_words)
    assert any("สาธารณะ" in word for word in shaped_words)
    assert all(not persona_pdf_module._contains_rtl(word) for word in shaped_words)


def test_rtl_display_shapes_arabic_before_bidi_reordering(monkeypatch):
    calls = []

    class Reshaper:
        @staticmethod
        def reshape(value):
            calls.append(("reshape", value))
            return f"shaped:{value}"

    def reorder(value):
        calls.append(("bidi", value))
        return f"display:{value}"

    monkeypatch.setattr(persona_pdf_module, "_arabic_reshaper", Reshaper())
    monkeypatch.setattr(persona_pdf_module, "_bidi_get_display", reorder)

    result = persona_pdf_module._rtl_display_line("مرحبا بالعالم")

    assert result == "display:shaped:مرحبا بالعالم"
    assert calls == [
        ("reshape", "مرحبا بالعالم"),
        ("bidi", "shaped:مرحبا بالعالم"),
    ]


def test_persona_export_degrades_safely_without_optional_rtl_extras(monkeypatch):
    monkeypatch.setattr(persona_pdf_module, "_arabic_reshaper", None)
    monkeypatch.setattr(persona_pdf_module, "_bidi_get_display", None)
    persona = _persona()
    persona["claims"][0]["display_value"] = "معلومات عامة"

    pdf_bytes = generate_persona_pdf(
        persona,
        generated_by="analyst",
        generated_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert pdf_bytes.startswith(b"%PDF-")


def test_exact_provenance_url_is_not_bidi_reordered(monkeypatch):
    def reject_reordering(value):
        raise AssertionError(f"URL was passed to bidi rendering: {value}")

    monkeypatch.setattr(persona_pdf_module, "_bidi_get_display", reject_reordering)
    style = persona_pdf_module.ParagraphStyle(
        "URLTest", fontName="Helvetica", fontSize=8
    )
    style.openledger_primary_coverage = frozenset(range(32, 127))
    style.openledger_fallback_fonts = ()
    source_url = "https://example.test/public?id=123&lang=ar"

    paragraph = persona_pdf_module._source_url_paragraph(source_url, style)

    assert "".join(fragment.text for fragment in paragraph.frags) == source_url
    assert {
        link[1]
        for fragment in paragraph.frags
        for link in getattr(fragment, "link", [])
    } == {source_url}


def test_persona_export_survives_an_unusable_optional_fallback_font(monkeypatch):
    monkeypatch.setattr(persona_pdf_module, "_font_paths", lambda: (None, None))
    monkeypatch.setattr(
        persona_pdf_module,
        "_fallback_font_paths",
        lambda: ("/invalid/fallback-font.ttf",),
    )

    def reject_font(*args, **kwargs):
        raise persona_pdf_module.TTFError("unsupported font outlines")

    monkeypatch.setattr(persona_pdf_module, "TTFont", reject_font)

    fonts = persona_pdf_module._register_fonts("न")

    assert fonts == ("Helvetica", "Helvetica-Bold", ())
