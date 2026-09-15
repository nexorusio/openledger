"""Human-readable PDF projection of an immutable reviewed investigation snapshot."""

from __future__ import annotations

import io
import json
from collections import OrderedDict
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import PageBreak, SimpleDocTemplate, Spacer, Table, TableStyle

from maigret.web.persona_pdf import (
    _paragraph,
    _register_fonts,
    _source_url_paragraph,
    _styles,
)


_PAGE_WIDTH = A4[0] - 36 * mm - 12
_NAVY = colors.HexColor("#09090B")
_MUTED = colors.HexColor("#57534E")
_LINE = colors.HexColor("#E7E5E4")
_PANEL = colors.HexColor("#FAFAF9")
_PURPLE = colors.HexColor("#6D28D9")
_SOFT_PURPLE = colors.HexColor("#F3E8FF")


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


def generate_pipeline_pdf(projection):
    """Render a readable report first and a complete technical audit appendix last.

    The supplied projection is an immutable version manifest. This function never
    looks up live claims, performs network I/O, or changes reviewed evidence.
    """
    rendered = json.dumps(projection, ensure_ascii=False, default=str)
    regular, bold, fallbacks = _register_fonts(rendered)
    styles = _styles(regular, bold, fallbacks)
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

    def page(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(_NAVY)
        canvas.rect(0, A4[1] - 19 * mm, A4[0], 19 * mm, stroke=0, fill=1)
        canvas.setFillColor(_PURPLE)
        canvas.rect(0, A4[1] - 19.8 * mm, A4[0], 0.8 * mm, stroke=0, fill=1)
        canvas.setFillColor(colors.white)
        canvas.setFont(bold, 10)
        canvas.drawString(18 * mm, A4[1] - 12 * mm, "OPENLEDGER")
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
        canvas.drawString(18 * mm, 7.5 * mm, "Approved evidence only · full audit appendix included")
        canvas.drawRightString(A4[0] - 18 * mm, 7.5 * mm, f"Page {doc.page}")
        canvas.restoreState()

    status = str(projection.get("status") or "reviewed").replace("_", " ").title()
    items = list(projection.get("items") or [])
    title(projection["subject_name"])
    text(f"Reviewed Persona · Snapshot {projection['sequence']}", "field")
    text(f"Case: {projection['case_title']}", "small")
    text(f"Status: {status}", "small")
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

    evidence: dict[str, dict[str, Any]] = {}
    evidence_uses: dict[str, list[dict[str, Any]]] = {}
    from maigret.web.pipeline_evidence import observation_evidence_role

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
        for item in section_items:
            text(_fact_label(item), "field", space=0.7)
            text(_fact_value(item), "body", space=0.8)
            decision = item.get("decision") or {}
            reason = str(decision.get("reason") or "").strip()
            if reason:
                text("Analyst review note: " + reason, "small", space=0.8)
            assessment = item.get("assessment") or {}
            assessment_reason = str(
                assessment.get("explanation") or assessment.get("reason") or ""
            ).strip()
            if assessment_reason:
                text("Evidence assessment: " + assessment_reason, "small", space=0.8)
            observations = list(item.get("evidence") or [])
            text(
                f"Supporting observations: {len(observations)} · Group: {item['group_id']}",
                "small",
                space=1.5,
            )
            for observation in observations:
                observation_id = str(observation["id"])
                evidence[observation_id] = observation
                evidence_uses.setdefault(observation_id, []).append({
                    "group_id": item["group_id"],
                    "role": observation_evidence_role(observation, item.get("normalized") or {}),
                    "operator_disposition": observation.get("operator_disposition"),
                    "source_state": observation.get("source_state"),
                })

    section("Limitations and unknowns")
    limitations = list(projection.get("limitations") or [])
    if limitations:
        for limitation in limitations:
            text("• " + str(limitation), "body")
    else:
        text("No additional limitations were recorded in this snapshot.", "body")

    story.append(PageBreak())
    section("Evidence register")
    text(
        f"{len(evidence)} distinct supporting observations are retained below. "
        "Each record is linked to its approved finding(s); source restrictions and excluded support remain visible.",
        "body",
    )
    for observation_id, observation in evidence.items():
        text("Observation " + observation_id, "field", space=0.8)
        payload = observation.get("payload") or {}
        source_label = str(
            payload.get("source_name")
            or payload.get("site_name")
            or observation.get("engine")
            or "Source record"
        )
        outcome = str(observation.get("status") or payload.get("status") or "observed")
        text(f"{source_label} · {outcome.replace('_', ' ')}", "small", space=0.8)
        url = observation.get("source_url") or observation.get("original_url") or ""
        if url:
            from maigret.web.pipeline_routes import public_url

            if public_url(url):
                story.append(_source_url_paragraph(url, styles["small"]))
                story.append(Spacer(1, 1 * mm))
        reason = str(payload.get("reason") or "").strip()
        if reason:
            text(reason, "small", space=0.8)
        text("Use of this observation in each curated group", "small_bold", space=0.7)
        for use in evidence_uses[observation_id]:
            role = str(use["role"] or "evidence").replace("_", " ")
            text(f"Group: {use['group_id']} · {role}", "small", space=0.5)
            disposition = use.get("operator_disposition") or {}
            if disposition:
                text(
                    "Evidence disposition: " + str(disposition.get("reason") or "reviewed"),
                    "small",
                    space=0.5,
                )
        story.append(Spacer(1, 1.2 * mm))

    story.append(PageBreak())
    section("Technical audit appendix")
    text(
        "The following immutable manifest details support reproducibility and review. "
        "They are intentionally separated from the investigation report.",
        "body",
    )
    text("Final Persona version: " + str(projection["version_id"]), "small")
    text("Manifest SHA-256: " + str(projection["content_hash"]), "small")
    text("Case ID: " + str(projection["case_id"]), "small")
    text("Persona ID: " + str(projection["persona_id"]), "small")
    text("Frozen scope", "field")
    audit_payload(projection.get("scope") or {})
    text("Frozen exclusions", "field")
    audit_payload(projection.get("exclusions") or [])
    text("Research requirements and dispositions", "field")
    audit_payload(projection.get("requirements") or [])
    text("Decision audit", "field")
    audit_payload(projection.get("qc") or [])
    document.build(story, onFirstPage=page, onLaterPages=page)
    return output.getvalue()
