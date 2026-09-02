"""Generate bounded, review-aware PDF snapshots of curated Personas."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
import threading
import unicodedata
from copy import deepcopy
from datetime import datetime, timezone
from glob import glob
from typing import Any, Dict, FrozenSet, Iterable, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse
from xml.sax.saxutils import escape, quoteattr

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.geomutils import normalizeTRBL
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont, TTFError
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from maigret.web.persona_intelligence import FIELD_GROUPS

try:
    import arabic_reshaper as _arabic_reshaper
except ImportError:  # PDF extras are optional outside the production web image.
    _arabic_reshaper = None

try:
    from bidi import get_display as _bidi_get_display
except ImportError:  # pragma: no cover - compatibility with older python-bidi
    try:
        from bidi.algorithm import get_display as _bidi_get_display
    except ImportError:  # PDF extras are optional outside the production web image.
        _bidi_get_display = None

_NAVY = colors.HexColor("#0C1B2A")
_NAVY_LIGHT = colors.HexColor("#13283A")
_TEAL = colors.HexColor("#12B8B0")
_INK = colors.HexColor("#172533")
_MUTED = colors.HexColor("#526578")
_LINE = colors.HexColor("#D8E1E8")
_PANEL = colors.HexColor("#F3F7F9")
_APPROVED = colors.HexColor("#087A55")
_PAGE_WIDTH = A4[0] - 36 * mm
_FONT_REGISTRATION_LOCK = threading.Lock()


def _font_paths() -> tuple[Optional[str], Optional[str]]:
    regular_candidates = (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSansCondensed.ttf",
    )
    bold_candidates = (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSansCondensed-Bold.ttf",
    )
    regular = next((path for path in regular_candidates if os.path.isfile(path)), None)
    bold = next((path for path in bold_candidates if os.path.isfile(path)), None)
    return regular, bold


def _fallback_font_paths() -> tuple[str, ...]:
    """Return deterministic font candidates for code points missing from DejaVu."""
    preferred = (
        "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
        "/usr/share/fonts-droid-fallback/truetype/DroidSansFallback.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansArabic-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansHebrew-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansDevanagari-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansBengali-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansGurmukhi-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansGujarati-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansTamil-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansTelugu-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansKannada-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansMalayalam-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansSinhala-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansThai-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansLao-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansMyanmar-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansKhmer-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansArmenian-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansGeorgian-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansEthiopic-Regular.ttf",
    )
    candidates = [*preferred, *sorted(glob("/usr/share/fonts/truetype/noto/*.ttf"))]
    return tuple(dict.fromkeys(path for path in candidates if os.path.isfile(path)))


def _font_coverage(font_name: str) -> FrozenSet[int]:
    face = getattr(pdfmetrics.getFont(font_name), "face", None)
    char_to_glyph = getattr(face, "charToGlyph", None)
    if isinstance(char_to_glyph, dict):
        return frozenset(
            codepoint for codepoint, glyph in char_to_glyph.items() if glyph
        )
    # Built-in PDF fonts have no Unicode cmap. Treat only their portable ASCII
    # repertoire as covered so installed fallbacks can handle everything else.
    return frozenset(range(32, 127))


FontFallback = Tuple[str, FrozenSet[int]]


def _register_fonts(
    required_text: str = "",
) -> tuple[str, str, tuple[FontFallback, ...]]:
    """Register only the embedded fallback fonts needed by this snapshot."""
    regular_path, bold_path = _font_paths()
    regular_name, bold_name = "Helvetica", "Helvetica-Bold"
    fallbacks = []
    with _FONT_REGISTRATION_LOCK:
        registered = pdfmetrics.getRegisteredFontNames()
        if regular_path and bold_path:
            if "OpenLedgerSans" not in registered:
                pdfmetrics.registerFont(TTFont("OpenLedgerSans", regular_path))
            if "OpenLedgerSans-Bold" not in registered:
                pdfmetrics.registerFont(TTFont("OpenLedgerSans-Bold", bold_path))
            regular_name, bold_name = "OpenLedgerSans", "OpenLedgerSans-Bold"

        missing = {
            ord(character)
            for character in required_text
            if ord(character) not in _font_coverage(regular_name)
        }
        for fallback_path in _fallback_font_paths():
            if not missing:
                break
            digest = hashlib.sha256(fallback_path.encode("utf-8")).hexdigest()[:12]
            fallback_name = f"OpenLedgerFallback-{digest}"
            try:
                if fallback_name not in pdfmetrics.getRegisteredFontNames():
                    pdfmetrics.registerFont(TTFont(fallback_name, fallback_path))
                coverage = _font_coverage(fallback_name)
            except TTFError as error:
                logging.warning(
                    "Persona PDF fallback font %s could not be registered: %s",
                    fallback_path,
                    error,
                )
                continue
            if coverage & missing:
                fallbacks.append((fallback_name, coverage))
                missing -= coverage
    return regular_name, bold_name, tuple(fallbacks)


def _clean_text(value: Any) -> str:
    """Keep readable text while dropping control/private-use/emoji glyphs."""
    text = unicodedata.normalize("NFKC", str(value or ""))
    cleaned = []
    for character in text:
        if character in "\n\t":
            cleaned.append(character)
            continue
        if unicodedata.category(character) in {
            "Cc",
            "Cf",
            "Cs",
            "Co",
            "Cn",
            "So",
        }:
            cleaned.append(" ")
        else:
            cleaned.append(character)
    return re.sub(r"[ \t]+", " ", "".join(cleaned)).strip()


def _display_value(claim: Mapping[str, Any]) -> str:
    display_value = _clean_text(claim.get("display_value"))
    if display_value:
        return display_value
    value = claim.get("value")
    if isinstance(value, Mapping):
        return _clean_text(", ".join(f"{key}: {item}" for key, item in value.items()))
    if isinstance(value, (list, tuple, set)):
        return _clean_text(", ".join(str(item) for item in value))
    return _clean_text(value)


def _approved_review_note(claim: Mapping[str, Any]) -> str:
    reviews = claim.get("reviews") or []
    if not reviews or reviews[0].get("decision") != "approved":
        return ""
    return _clean_text(reviews[0].get("note"))


def build_persona_export_snapshot(
    persona: Mapping[str, Any],
    *,
    generated_at: datetime,
    generated_by: str,
) -> Dict[str, Any]:
    """Create the exact approved-only record represented by the PDF."""
    approved_claims = [
        claim
        for claim in persona.get("claims") or []
        if claim.get("review_status") == "approved"
    ]
    groups = []
    source_count = 0
    canonical_claims = []
    for group_definition in FIELD_GROUPS:
        fields = []
        for field_name, field_label in group_definition["fields"]:
            field_claims = []
            for claim in approved_claims:
                if claim.get("field_name") != field_name:
                    continue
                evidence_items = []
                for evidence in claim.get("evidence") or []:
                    source_count += 1
                    evidence_items.append(
                        {
                            "source_name": _clean_text(evidence.get("source_name")),
                            "source_url": str(evidence.get("source_url") or "").strip(),
                            "evidence_type": _clean_text(
                                str(evidence.get("evidence_type") or "").replace(
                                    "_", " "
                                )
                            ),
                            "observed_at": str(evidence.get("observed_at") or ""),
                        }
                    )
                item = {
                    "id": str(claim.get("id") or ""),
                    "field_name": field_name,
                    "value": _display_value(claim),
                    "confidence": int(claim.get("confidence") or 0),
                    "reviewed_by": _clean_text(claim.get("reviewed_by")),
                    "reviewed_at": str(claim.get("reviewed_at") or ""),
                    "first_seen_at": str(claim.get("first_seen_at") or ""),
                    "last_seen_at": str(claim.get("last_seen_at") or ""),
                    "latitude": claim.get("latitude"),
                    "longitude": claim.get("longitude"),
                    "approval_note": _approved_review_note(claim),
                    "evidence": evidence_items,
                }
                field_claims.append(item)
                canonical_claims.append(item)
            fields.append(
                {
                    "key": field_name,
                    "label": field_label,
                    "claims": field_claims,
                }
            )
        groups.append(
            {
                "key": group_definition["key"],
                "title": group_definition["title"],
                "description": group_definition.get("description", ""),
                "fields": fields,
                "approved_count": sum(len(field["claims"]) for field in fields),
            }
        )

    canonical_record = {
        "persona_id": str(persona.get("id") or ""),
        "case_id": str(persona.get("case_id") or ""),
        "case_title": _clean_text(persona.get("case_title")),
        "display_name": _clean_text(persona.get("display_name")),
        "approved_claims": canonical_claims,
    }
    record_bytes = json.dumps(
        canonical_record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        **canonical_record,
        "generated_at": generated_at.astimezone(timezone.utc).isoformat(),
        "generated_by": _clean_text(generated_by) or "local-operator",
        "approved_count": len(canonical_claims),
        "source_count": source_count,
        "groups": groups,
        "snapshot_sha256": hashlib.sha256(record_bytes).hexdigest(),
    }


def _safe_http_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
    except ValueError:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.hostname)


def _contains_rtl(text: str) -> bool:
    return any(
        unicodedata.bidirectional(character) in {"R", "AL"} for character in text
    )


def _contains_arabic(text: str) -> bool:
    return any("ARABIC" in unicodedata.name(character, "") for character in text)


def _rtl_display_line(text: str) -> str:
    """Shape and reorder one already-wrapped logical RTL line for ReportLab."""
    if not text or not _contains_rtl(text) or _bidi_get_display is None:
        return text
    if _contains_arabic(text):
        if _arabic_reshaper is None:
            return text
        text = _arabic_reshaper.reshape(text)
    return _bidi_get_display(text)


def _font_for_character(character: str, style: ParagraphStyle) -> Optional[str]:
    if character.isspace():
        return None
    codepoint = ord(character)
    primary_coverage = getattr(style, "openledger_primary_coverage", frozenset())
    if codepoint in primary_coverage:
        return None
    for font_name, coverage in getattr(style, "openledger_fallback_fonts", ()):  # type: ignore[attr-defined]
        if codepoint in coverage:
            return font_name
    return None


def _font_markup(cleaned: str, style: ParagraphStyle) -> str:
    if not cleaned:
        return ""
    fragments = []
    start = 0
    current_font = _font_for_character(cleaned[0], style)
    for index, character in enumerate(cleaned[1:], start=1):
        font_name = _font_for_character(character, style)
        if font_name == current_font:
            continue
        fragment = escape(cleaned[start:index], entities={"'": "&apos;", '"': "&quot;"})
        fragments.append(
            f'<font name="{current_font}">{fragment}</font>'
            if current_font
            else fragment
        )
        start = index
        current_font = font_name
    fragment = escape(cleaned[start:], entities={"'": "&apos;", '"': "&quot;"})
    fragments.append(
        f'<font name="{current_font}">{fragment}</font>' if current_font else fragment
    )
    return "".join(fragments)


def _escaped_paragraph_text(value: Any, style: ParagraphStyle) -> str:
    cleaned = _clean_text(value)
    return _font_markup(cleaned, style)


def _rendered_width(text: str, style: ParagraphStyle) -> float:
    width = 0.0
    start = 0
    current_font = _font_for_character(text[0], style) if text else None
    for index, character in enumerate(text[1:], start=1):
        font_name = _font_for_character(character, style)
        if font_name == current_font:
            continue
        width += pdfmetrics.stringWidth(
            text[start:index], current_font or style.fontName, style.fontSize
        )
        start = index
        current_font = font_name
    if text:
        width += pdfmetrics.stringWidth(
            text[start:], current_font or style.fontName, style.fontSize
        )
    return width


def _split_oversized_rtl_word(
    word: str, style: ParagraphStyle, max_width: float
) -> list[str]:
    chunks = []
    current = ""
    for character in word:
        candidate = current + character
        if current and _rendered_width(_rtl_display_line(candidate), style) > max_width:
            chunks.append(current)
            current = character
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _wrap_rtl_lines(
    text: str, style: ParagraphStyle, available_width: float
) -> list[str]:
    top, right, bottom, left = normalizeTRBL(getattr(style, "borderPadding", 0))
    del top, bottom
    max_width = max(
        1.0,
        available_width - style.leftIndent - style.rightIndent - left - right,
    )
    rendered_lines = []
    for explicit_line in text.split("\n"):
        words = explicit_line.split()
        if not words:
            rendered_lines.append("")
            continue
        logical_lines = []
        current = ""
        for word in words:
            candidate = f"{current} {word}" if current else word
            if _rendered_width(_rtl_display_line(candidate), style) <= max_width:
                current = candidate
                continue
            if current:
                logical_lines.append(current)
                current = ""
            word_chunks = _split_oversized_rtl_word(word, style, max_width)
            logical_lines.extend(word_chunks[:-1])
            current = word_chunks[-1]
        if current:
            logical_lines.append(current)
        rendered_lines.extend(_rtl_display_line(line) for line in logical_lines)
    return rendered_lines


class _RTLParagraph(Paragraph):
    """Delay RTL shaping until the table or page provides the real line width."""

    def __init__(
        self,
        logical_text: Optional[str],
        style: ParagraphStyle,
        bulletText: Optional[str] = None,
        frags: Optional[Sequence[Any]] = None,
        caseSensitive: int = 1,
        encoding: str = "utf8",
    ):
        self._openledger_logical_text = logical_text
        self._openledger_source_style = style
        self._openledger_prepared_width = None
        super().__init__(
            "" if logical_text is not None else None,
            style,
            bulletText=bulletText,
            frags=frags,
            caseSensitive=caseSensitive,
            encoding=encoding,
        )

    def _prepare(self, available_width: float) -> None:
        if (
            self._openledger_logical_text is None
        ) or self._openledger_prepared_width == available_width:
            return
        rtl_style = deepcopy(self._openledger_source_style)
        rtl_style.alignment = TA_RIGHT
        rtl_style.wordWrap = "LTR"
        lines = _wrap_rtl_lines(
            self._openledger_logical_text,
            rtl_style,
            available_width,
        )
        markup = "<br/>".join(_font_markup(line, rtl_style) for line in lines) or "-"
        Paragraph.__init__(self, markup, rtl_style)
        self._openledger_prepared_width = available_width

    def wrap(
        self, available_width: float, available_height: float
    ) -> tuple[float, float]:
        self._prepare(available_width)
        return super().wrap(available_width, available_height)

    def split(self, available_width: float, available_height: float) -> list[Any]:
        self._prepare(available_width)
        return super().split(available_width, available_height)


def _paragraph(value: Any, style: ParagraphStyle) -> Paragraph:
    cleaned = _clean_text(value)
    if _contains_rtl(cleaned) and _bidi_get_display is not None:
        if not _contains_arabic(cleaned) or _arabic_reshaper is not None:
            return _RTLParagraph(cleaned, style)
    text = _font_markup(cleaned, style)
    return Paragraph(text.replace("\n", "<br/> ") or "-", style)


def _source_url_paragraph(value: str, style: ParagraphStyle) -> Paragraph:
    cleaned = str(value or "").strip()
    visible = _escaped_paragraph_text(cleaned, style)
    if not visible:
        return Paragraph("No public URL recorded", style)
    if _safe_http_url(cleaned):
        return Paragraph(
            f"<link href={quoteattr(cleaned)} color='#087D87'>{visible}</link>",
            style,
        )
    return Paragraph(visible, style)


def _format_time(value: str) -> str:
    cleaned = _clean_text(value)
    return cleaned.replace("T", " ").replace("+00:00", " UTC") if cleaned else "-"


def _iter_snapshot_text(value: Any) -> Iterable[str]:
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _iter_snapshot_text(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _iter_snapshot_text(item)
    elif isinstance(value, str):
        cleaned = _clean_text(value)
        yield cleaned
        if _contains_rtl(cleaned):
            yield from (_rtl_display_line(line) for line in cleaned.split("\n"))


def _styles(
    regular_font: str,
    bold_font: str,
    fallback_fonts: Sequence[FontFallback],
) -> Dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    styles = {
        "title": ParagraphStyle(
            "PersonaTitle",
            parent=sample["Title"],
            fontName=bold_font,
            fontSize=22,
            leading=27,
            textColor=_NAVY,
            alignment=TA_LEFT,
            spaceAfter=4 * mm,
        ),
        "subtitle": ParagraphStyle(
            "PersonaSubtitle",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=9.5,
            leading=13,
            textColor=_MUTED,
        ),
        "section": ParagraphStyle(
            "PersonaSection",
            parent=sample["Heading2"],
            fontName=bold_font,
            fontSize=12,
            leading=15,
            textColor=colors.white,
        ),
        "field": ParagraphStyle(
            "PersonaField",
            parent=sample["Heading3"],
            fontName=bold_font,
            fontSize=10.5,
            leading=14,
            textColor=_NAVY,
            spaceBefore=3 * mm,
            spaceAfter=1.5 * mm,
        ),
        "value": ParagraphStyle(
            "PersonaValue",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=10,
            leading=14,
            textColor=_INK,
            wordWrap="CJK",
            splitLongWords=1,
        ),
        "claim_value": ParagraphStyle(
            "PersonaClaimValue",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=10,
            leading=14,
            textColor=_INK,
            wordWrap="LTR",
            splitLongWords=1,
            backColor=colors.white,
            borderColor=_LINE,
            borderWidth=0.5,
            borderPadding=7,
            spaceBefore=1.5 * mm,
            spaceAfter=0,
        ),
        "body": ParagraphStyle(
            "PersonaBody",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=8.5,
            leading=12,
            textColor=_INK,
            wordWrap="CJK",
            splitLongWords=1,
        ),
        "small": ParagraphStyle(
            "PersonaSmall",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=7.3,
            leading=9.5,
            textColor=_MUTED,
            wordWrap="CJK",
            splitLongWords=1,
        ),
        "small_bold": ParagraphStyle(
            "PersonaSmallBold",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=7.3,
            leading=9.5,
            textColor=_INK,
        ),
        "table_header": ParagraphStyle(
            "PersonaTableHeader",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=7.3,
            leading=9.5,
            textColor=colors.white,
        ),
        "badge": ParagraphStyle(
            "PersonaBadge",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=7.5,
            leading=10,
            textColor=_APPROVED,
            alignment=TA_CENTER,
        ),
        "notice": ParagraphStyle(
            "PersonaNotice",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=8.3,
            leading=12,
            textColor=_INK,
        ),
    }
    for style in styles.values():
        style.openledger_primary_coverage = _font_coverage(style.fontName)
        style.openledger_fallback_fonts = tuple(fallback_fonts)
    return styles


def _metadata_table(snapshot: Mapping[str, Any], styles: Mapping[str, Any]) -> Table:
    rows = [
        [
            _paragraph("Case", styles["small_bold"]),
            _paragraph(snapshot["case_title"] or snapshot["case_id"], styles["small"]),
            _paragraph("Persona ID", styles["small_bold"]),
            _paragraph(snapshot["persona_id"], styles["small"]),
        ],
        [
            _paragraph("Generated", styles["small_bold"]),
            _paragraph(_format_time(snapshot["generated_at"]), styles["small"]),
            _paragraph("Generated by", styles["small_bold"]),
            _paragraph(snapshot["generated_by"], styles["small"]),
        ],
        [
            _paragraph("Approved records", styles["small_bold"]),
            _paragraph(str(snapshot["approved_count"]), styles["small"]),
            _paragraph("Supporting sources", styles["small_bold"]),
            _paragraph(str(snapshot["source_count"]), styles["small"]),
        ],
        [
            _paragraph("Record snapshot SHA-256", styles["small_bold"]),
            _paragraph(snapshot["snapshot_sha256"], styles["small"]),
            "",
            "",
        ],
    ]
    table = Table(rows, colWidths=[27 * mm, 60 * mm, 30 * mm, 57 * mm])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), _PANEL),
                ("BOX", (0, 0), (-1, -1), 0.5, _LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.35, _LINE),
                ("SPAN", (1, 3), (3, 3)),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def _claim_flowables(
    claim: Mapping[str, Any], styles: Mapping[str, ParagraphStyle]
) -> Iterable[Any]:
    badge = Table(
        [[_paragraph(f"APPROVED · {claim['confidence']}%", styles["badge"])]],
        colWidths=[30 * mm],
    )
    badge.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#E8F6F1")),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#A7DCC9")),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    metadata = [
        [
            _paragraph("Claim ID", styles["small_bold"]),
            _paragraph(claim["id"], styles["small"]),
            _paragraph("Reviewed by", styles["small_bold"]),
            _paragraph(claim["reviewed_by"] or "-", styles["small"]),
        ],
        [
            _paragraph("Reviewed at", styles["small_bold"]),
            _paragraph(_format_time(claim["reviewed_at"]), styles["small"]),
            _paragraph("Observed", styles["small_bold"]),
            _paragraph(
                f"{_format_time(claim['first_seen_at'])} to {_format_time(claim['last_seen_at'])}",
                styles["small"],
            ),
        ],
    ]
    if claim.get("latitude") is not None and claim.get("longitude") is not None:
        metadata.append(
            [
                _paragraph("Map center", styles["small_bold"]),
                _paragraph(
                    f"{claim['latitude']}, {claim['longitude']}", styles["small"]
                ),
                _paragraph("Location use", styles["small_bold"]),
                _paragraph("Approved approximate map center", styles["small"]),
            ]
        )
    metadata_table = Table(
        metadata,
        colWidths=[24 * mm, 62 * mm, 25 * mm, 63 * mm],
    )
    metadata_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), _PANEL),
                ("BOX", (0, 0), (-1, -1), 0.5, _LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, _LINE),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    yield badge
    yield _paragraph(claim["value"], styles["claim_value"])
    yield metadata_table
    if claim.get("approval_note"):
        yield Table(
            [
                [
                    _paragraph("Approval note", styles["small_bold"]),
                    _paragraph(claim["approval_note"], styles["small"]),
                ]
            ],
            colWidths=[27 * mm, _PAGE_WIDTH - 27 * mm],
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FFF8E8")),
                    ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#E9D7A4")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            ),
        )
    evidence_rows = [
        [
            _paragraph("Source", styles["table_header"]),
            _paragraph("Evidence type", styles["table_header"]),
            _paragraph("Observed", styles["table_header"]),
            _paragraph("Exact public provenance URL", styles["table_header"]),
        ]
    ]
    for evidence in claim.get("evidence") or []:
        evidence_rows.append(
            [
                _paragraph(
                    evidence["source_name"] or "Unnamed source", styles["small"]
                ),
                _paragraph(evidence["evidence_type"] or "-", styles["small"]),
                _paragraph(_format_time(evidence["observed_at"]), styles["small"]),
                _source_url_paragraph(evidence["source_url"], styles["small"]),
            ]
        )
    if len(evidence_rows) == 1:
        evidence_rows.append(
            [
                _paragraph("No supporting source record", styles["small"]),
                _paragraph("-", styles["small"]),
                _paragraph("-", styles["small"]),
                _paragraph("No public URL recorded", styles["small"]),
            ]
        )
    sources = Table(
        evidence_rows,
        colWidths=[38 * mm, 27 * mm, 32 * mm, 77 * mm],
        repeatRows=1,
        splitByRow=True,
    )
    sources.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), _NAVY_LIGHT),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("BOX", (0, 0), (-1, -1), 0.5, _LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, _LINE),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _PANEL]),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    yield sources
    yield Spacer(1, 3 * mm)


def generate_persona_pdf(
    persona: Mapping[str, Any],
    *,
    generated_by: str,
    generated_at: Optional[datetime] = None,
) -> bytes:
    """Return a self-contained PDF of approved Persona records."""
    generated_at = generated_at or datetime.now(timezone.utc)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    snapshot = build_persona_export_snapshot(
        persona,
        generated_at=generated_at,
        generated_by=generated_by,
    )
    required_text = "".join(_iter_snapshot_text(snapshot))
    regular_font, bold_font, fallback_fonts = _register_fonts(required_text)
    styles = _styles(
        regular_font,
        bold_font,
        fallback_fonts,
    )
    output = io.BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=28 * mm,
        bottomMargin=18 * mm,
        title=f"Curated Persona - {snapshot['display_name']}",
        author="OpenLedger",
        subject="Analyst-approved Persona evidence snapshot",
    )

    def draw_page(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(_NAVY)
        canvas.rect(0, A4[1] - 19 * mm, A4[0], 19 * mm, stroke=0, fill=1)
        canvas.setFillColor(_TEAL)
        canvas.rect(0, A4[1] - 19.8 * mm, A4[0], 0.8 * mm, stroke=0, fill=1)
        canvas.setFont(bold_font, 10)
        canvas.setFillColor(colors.white)
        canvas.drawString(18 * mm, A4[1] - 12 * mm, "OPENLEDGER")
        canvas.setFont(regular_font, 7.5)
        canvas.setFillColor(colors.HexColor("#C4D5E2"))
        canvas.drawRightString(
            A4[0] - 18 * mm,
            A4[1] - 12 * mm,
            "CURATED PERSONA REPORT",
        )
        canvas.setStrokeColor(_LINE)
        canvas.line(18 * mm, 12 * mm, A4[0] - 18 * mm, 12 * mm)
        canvas.setFont(regular_font, 7)
        canvas.setFillColor(_MUTED)
        canvas.drawString(18 * mm, 7.5 * mm, "Analyst-approved records only")
        canvas.drawRightString(A4[0] - 18 * mm, 7.5 * mm, f"Page {doc.page}")
        canvas.restoreState()

    story = [
        _paragraph(snapshot["display_name"] or "Unnamed Persona", styles["title"]),
        _paragraph(
            "Evidence-backed Persona snapshot generated from OpenLedger's canonical approved records.",
            styles["subtitle"],
        ),
        Spacer(1, 4 * mm),
        _metadata_table(snapshot, styles),
        Spacer(1, 4 * mm),
        Table(
            [
                [
                    _paragraph(
                        "Scope: this export contains only records that were approved at generation time. Pending, uncertain, and rejected proposals are excluded. Every available exact public provenance URL remains attached to its approved record.",
                        styles["notice"],
                    )
                ]
            ],
            colWidths=[_PAGE_WIDTH],
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#EAF7F7")),
                    ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#9DD9D5")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 7),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ]
            ),
        ),
        Spacer(1, 5 * mm),
    ]

    approved_groups = [group for group in snapshot["groups"] if group["approved_count"]]
    if not approved_groups:
        story.extend(
            [
                _paragraph("Approved Persona records", styles["field"]),
                _paragraph(
                    "No records had been approved when this report was generated.",
                    styles["body"],
                ),
                Spacer(1, 4 * mm),
            ]
        )
    for group in approved_groups:
        section_header = Table(
            [
                [
                    _paragraph(group["title"], styles["section"]),
                    _paragraph(
                        f"{group['approved_count']} approved",
                        ParagraphStyle(
                            "SectionCount",
                            parent=styles["small"],
                            textColor=colors.HexColor("#C4D5E2"),
                            alignment=TA_CENTER,
                        ),
                    ),
                ]
            ],
            colWidths=[_PAGE_WIDTH - 30 * mm, 30 * mm],
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), _NAVY),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 7),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ]
            ),
        )
        story.append(section_header)
        if group.get("description"):
            story.extend(
                [
                    Spacer(1, 1.5 * mm),
                    _paragraph(group["description"], styles["small"]),
                ]
            )
        for field in group["fields"]:
            if not field["claims"]:
                continue
            story.append(_paragraph(field["label"], styles["field"]))
            for claim in field["claims"]:
                story.extend(_claim_flowables(claim, styles))
        story.append(Spacer(1, 3 * mm))

    story.extend(
        [
            Spacer(1, 2 * mm),
            _paragraph(
                "This report records curated investigative information and its provenance. It does not independently establish identity, wrongdoing, or legal responsibility.",
                styles["small"],
            ),
        ]
    )
    document.build(story, onFirstPage=draw_page, onLaterPages=draw_page)
    return output.getvalue()


def persona_pdf_filename(persona: Mapping[str, Any], *, generated_at: datetime) -> str:
    name = _clean_text(persona.get("display_name")).casefold()
    slug = re.sub(r"[^a-z0-9]+", "-", name).strip("-")[:80] or "persona"
    timestamp = generated_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"openledger-persona-{slug}-{timestamp}.pdf"
