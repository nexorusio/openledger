from datetime import datetime, timezone

from maigret.web.persona_pdf import (
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


def test_persona_pdf_is_self_contained_and_uses_safe_filename():
    generated_at = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
    pdf_bytes = generate_persona_pdf(
        _persona(), generated_by="analyst", generated_at=generated_at
    )

    assert pdf_bytes.startswith(b"%PDF-")
    assert b"%%EOF" in pdf_bytes[-1024:]
    assert len(pdf_bytes) > 5000
    assert persona_pdf_filename(_persona(), generated_at=generated_at) == (
        "openledger-persona-alice-example-20260902T120000Z.pdf"
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
