"""Human-readable PDF projection of an immutable reviewed investigation snapshot."""

from __future__ import annotations

import io
import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    Image as ReportImage,
    PageBreak,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from maigret.web.persona_pdf import (
    _paragraph,
    _register_fonts,
    _source_url_paragraph,
    _styles,
)
from maigret.web.persona_report_media import load_approved_portrait


_PAGE_WIDTH = A4[0] - 36 * mm - 12
_NAVY = colors.HexColor("#09090B")
_MUTED = colors.HexColor("#57534E")
_LINE = colors.HexColor("#E7E5E4")
_PANEL = colors.HexColor("#FAFAF9")
_PURPLE = colors.HexColor("#6D28D9")
_SOFT_PURPLE = colors.HexColor("#F3E8FF")
_LOGO_PATH = Path(__file__).with_name("static") / "openledger-icon-white.png"


def _canonical_field(item: dict[str, Any]) -> str:
    """Give the readable report the same field structure as the Persona UI."""
    if item.get("kind") == "account":
        return "social_account"
    normalized = item.get("normalized") or {}
    predicate = str(
        normalized.get("predicate") or normalized.get("field_name") or "other"
    ).casefold()
    return {
        "about": "summary",
        "bio": "summary",
        "biography": "summary",
        "description": "summary",
        "display_name": "full_name",
        "name": "full_name",
        "employer": "company",
        "organization": "company",
        "organisation": "company",
        "affiliation": "company",
        "job_title": "occupation",
        "role": "occupation",
        "location": "current_location",
        "city": "current_location",
    }.get(predicate, predicate)


def _readable_fields(items: list[dict[str, Any]]) -> OrderedDict[str, list[dict[str, Any]]]:
    """Deduplicate the reader-facing profile without losing its audit records."""
    fields: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    seen: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        field = _canonical_field(item)
        value = _fact_value(item).strip()
        identity = (field, value.casefold())
        existing = seen.get(identity)
        if existing is None:
            existing = {"value": value, "items": [item]}
            seen[identity] = existing
            fields.setdefault(field, []).append(existing)
        else:
            existing["items"].append(item)
    return fields


def _display_value(value: Any) -> str:
    if isinstance(value, dict):
        preferred = (
            value.get("display_value")
            or value.get("name")
            or value.get("url")
            or value.get("canonical_url")
            or value.get("handle")
        )
        if preferred:
            return str(preferred)
        return ", ".join(f"{key}: {item}" for key, item in value.items())
    if isinstance(value, (list, tuple, set)):
        return ", ".join(_display_value(item) for item in value)
    return str(value or "")


def _fact_value(item: dict[str, Any]) -> str:
    normalized = item.get("normalized") or {}
    return _display_value(
        normalized.get("display_value")
        or normalized.get("value")
        or normalized.get("canonical_url")
        or normalized.get("url")
        or normalized.get("handle")
        or "Retained finding"
    )


def _fact_label(item: dict[str, Any]) -> str:
    normalized = item.get("normalized") or {}
    return str(
        normalized.get("predicate") or normalized.get("field_name") or item.get("kind")
    ).replace("_", " ").title()


def _json_lines(value: Any) -> list[str]:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str).splitlines()


def _narrative_summary(items: list[dict[str, Any]], subject_name: str) -> str:
    """Describe the reviewed record without promoting raw social snippets."""
    fields = _readable_fields(items)

    def first(field: str) -> str:
        values = fields.get(field) or []
        return str(values[0]["value"]) if values else ""

    role, organization, location = (
        first("occupation"), first("company"), first("current_location")
    )
    clauses = []
    if role and organization:
        clauses.append(f"Approved evidence describes {subject_name} as {role} associated with {organization}")
    elif role:
        clauses.append(f"Approved evidence describes {subject_name} as {role}")
    elif organization:
        clauses.append(f"Approved evidence associates {subject_name} with {organization}")
    if location:
        clauses.append(f"the recorded location is {location}")
    identifiers = [
        fact["value"]
        for field in ("social_account", "username", "website")
        for fact in (fields.get(field) or [])[:2]
    ][:3]
    if identifiers:
        clauses.append("recorded public identifiers include " + ", ".join(identifiers))
    return ". ".join(clauses).rstrip(".") + "." if clauses else ""


def _portrait_bytes(items: list[dict[str, Any]]) -> bytes | None:
    for item in items:
        if _canonical_field(item) != "photograph":
            continue
        try:
            return load_approved_portrait(_fact_value(item))
        except Exception:
            continue
    return None


def generate_pipeline_pdf(projection):
    """Render a concise investigator brief from an immutable review snapshot."""
    rendered = json.dumps(projection, ensure_ascii=False, default=str)
    regular, bold, fallbacks = _register_fonts(rendered)
    styles = _styles(regular, bold, fallbacks)
    identifier_style = ParagraphStyle(
        "PipelineAuditIdentifier",
        fontName="Courier",
        fontSize=7.2,
        leading=9,
        textColor=_MUTED,
        spaceAfter=0.8 * mm,
        shaping=0,
    )
    output = io.BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=27 * mm,
        bottomMargin=19 * mm,
        title=f'{projection["subject_name"]} · reviewed investigation report',
        author="OpenLedger",
        subject=f'Reviewed Persona snapshot {projection["sequence"]}',
    )
    story = []

    def text(value: Any, style="body", *, space=2):
        story.append(_paragraph(value, styles[style]))
        if space:
            story.append(Spacer(1, space * mm))

    def title(value: str):
        text(value, "title", space=1)

    def section(value: str):
        text(value, "field", space=1)

    def audit_payload(value: Any):
        for line in _json_lines(value):
            text(line, "small", space=0.6)

    def identifier(value: Any):
        # Audit identifiers must survive independent PDF text extraction byte
        # for byte.  Do not pass UUIDs through the shaped proportional-font
        # paragraph path, where ligatures can change extracted text.
        value = str(value or "").replace("\r", " ").replace("\n", " ")
        story.append(Preformatted(value or "-", identifier_style))

    def page(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(_NAVY)
        canvas.rect(0, A4[1] - 19 * mm, A4[0], 19 * mm, stroke=0, fill=1)
        canvas.setFillColor(_PURPLE)
        canvas.rect(0, A4[1] - 19.8 * mm, A4[0], 0.8 * mm, stroke=0, fill=1)
        if _LOGO_PATH.is_file():
            canvas.drawImage(
                str(_LOGO_PATH),
                18 * mm,
                A4[1] - 15.3 * mm,
                width=7.2 * mm,
                height=7.2 * mm,
                preserveAspectRatio=True,
                mask="auto",
            )
        canvas.setFillColor(colors.white)
        canvas.setFont(bold, 10)
        canvas.drawString(27.5 * mm, A4[1] - 12 * mm, "OPENLEDGER")
        canvas.setFillColor(colors.HexColor("#DDD6FE"))
        canvas.setFont(regular, 7.5)
        canvas.drawRightString(
            A4[0] - 18 * mm,
            A4[1] - 12 * mm,
            "REVIEWED INVESTIGATION REPORT",
        )
        canvas.setStrokeColor(_LINE)
        canvas.line(18 * mm, 12 * mm, A4[0] - 18 * mm, 12 * mm)
        canvas.setFillColor(_MUTED)
        canvas.setFont(regular, 7)
        canvas.drawString(18 * mm, 7.5 * mm, "Approved evidence only · detailed provenance remains in OpenLedger")
        canvas.drawRightString(A4[0] - 18 * mm, 7.5 * mm, f"Page {doc.page}")
        canvas.restoreState()

    status = str(projection.get("status") or "reviewed").replace("_", " ").title()
    items = list(projection.get("items") or [])
    portrait_bytes = _portrait_bytes(items)
    narrative = _narrative_summary(items, str(projection["subject_name"]))
    title(projection["subject_name"])
    text(f"Investigation brief · Case: {projection['case_title']} · Reviewed Persona {projection['sequence']} · {status}", "small")
    if portrait_bytes or narrative:
        portrait = (
            ReportImage(io.BytesIO(portrait_bytes), width=35 * mm, height=35 * mm)
            if portrait_bytes
            else _paragraph("No approved public photograph", styles["small"])
        )
        overview = _paragraph(
            narrative or "No concise narrative can be drawn from the approved findings yet.",
            styles["body"],
        )
        overview_table = Table([[portrait, overview]], colWidths=[42 * mm, _PAGE_WIDTH - 42 * mm])
        overview_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), _PANEL),
            ("BOX", (0, 0), (-1, -1), 0.5, _LINE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ]))
        story.extend([overview_table, Spacer(1, 5 * mm)])
    story.append(
        Table(
            [[_paragraph(
                "This report presents the findings an operator explicitly approved. "
                "It does not establish identity, ownership, wrongdoing, assets, or risk beyond the cited evidence. "
                "New source fetches and research results remain pending until reviewed.",
                styles["notice"],
            )]],
            colWidths=[_PAGE_WIDTH],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), _SOFT_PURPLE),
                ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#C4B5FD")),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]),
        )
    )
    story.append(Spacer(1, 5 * mm))

    section_counts: OrderedDict[str, int] = OrderedDict()
    from maigret.web.pipeline_store import SHORTLIST_SECTIONS, _shortlist_section
    from maigret.web.persona_intelligence import field_display_label

    for key, title in SHORTLIST_SECTIONS:
        section_counts[title] = sum(
            1
            for item in items
            if _shortlist_section(item.get("kind"), item.get("normalized") or {}) == key
        )
    summary_rows = [[
        _paragraph("Approved findings", styles["table_header"]),
        _paragraph("Evidence categories", styles["table_header"]),
        _paragraph("Report scope", styles["table_header"]),
    ], [
        _paragraph(str(len(items)), styles["item_value"]),
        _paragraph(" · ".join(f"{title}: {count}" for title, count in section_counts.items()), styles["small"]),
        _paragraph("Explicit analyst approvals in this frozen snapshot", styles["small"]),
    ]]
    summary = Table(summary_rows, colWidths=[34 * mm, 85 * mm, _PAGE_WIDTH - 119 * mm])
    summary.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), _NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("BOX", (0, 0), (-1, -1), 0.5, _LINE),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, _LINE),
        ("BACKGROUND", (0, 1), (-1, -1), _PANEL),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.extend([summary, Spacer(1, 5 * mm)])

    for section_key, section_title in SHORTLIST_SECTIONS:
        section(section_title)
        section_items = [
            item for item in items
            if _shortlist_section(item.get("kind"), item.get("normalized") or {}) == section_key
        ]
        if section_key == "records":
            text(
                "Risk is not numerically scored without an approved, versioned model. "
                "Social profiles, employers and summaries do not establish assets, misconduct, or risk.",
                "small",
            )
        if not section_items:
            text("No approved findings in this category. This is not proof of absence.", "body")
            continue
        for field, facts in _readable_fields(section_items).items():
            text(field_display_label(field), "field", space=0.7)
            for fact in facts[:5]:
                text(
                    "Approved public photograph" if field == "photograph" else fact["value"],
                    "body",
                    space=0.45,
                )
                supporting = len(
                    {
                        str(observation.get("id"))
                        for item in fact["items"]
                        for observation in item.get("evidence") or []
                        if observation.get("id")
                    }
                )
                duplicates = len(fact["items"])
                text(
                    f"{supporting} supporting observation{'s' if supporting != 1 else ''}"
                    + (f" across {duplicates} approved records" if duplicates > 1 else ""),
                    "small",
                    space=1.1,
                )
            if len(facts) > 5:
                text(
                    f"{len(facts) - 5} additional distinct approved value(s) are retained in OpenLedger.",
                    "small",
                    space=1.1,
                )

    section("Limitations and unknowns")
    limitations = list(projection.get("limitations") or [])
    if limitations:
        for limitation in limitations:
            text("• " + str(limitation), "body")
    else:
        text("No additional limitations were recorded in this snapshot.", "body")

    section("Evidence and audit access")
    text(
        "This brief intentionally contains only distinct, approved investigation findings. "
        "Open the Persona profile to inspect every retained observation, external source link, decision history and immutable version record.",
        "body",
    )
    text("Version: " + str(projection["version_id"]), "small")
    text("Manifest SHA-256: " + str(projection["content_hash"]), "small")
    document.build(story, onFirstPage=page, onLaterPages=page)
    return output.getvalue()
