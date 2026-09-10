"""Generate bounded, review-aware PDF snapshots of curated Personas."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import math
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
from reportlab.pdfbase.ttfonts import TTFont, TTFError, shapeFragWord
from reportlab.platypus import (
    CondPageBreak,
    Image as ReportImage,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.paragraph import _getFragWords

from maigret.web.persona_intelligence import FIELD_GROUPS
from maigret.web.persona_report_media import (
    load_approved_portrait,
    render_location_map,
)

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
_AMBER = colors.HexColor("#A45C08")
_RISK = colors.HexColor("#A23A3A")
_SOFT_TEAL = colors.HexColor("#EAF7F7")
_SOFT_AMBER = colors.HexColor("#FFF5DF")
_SOFT_RISK = colors.HexColor("#FBECEC")
# SimpleDocTemplate's content frame has six-point padding on both sides in
# addition to the document margins. Keep every full-width flowable inside that
# real frame width so nested media cards cannot bleed into the margins.
_PAGE_WIDTH = A4[0] - 36 * mm - 12
_HERO_NAME_LIMIT = 180
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
    text = unicodedata.normalize("NFC", str(value or ""))
    cleaned = []
    for index, character in enumerate(text):
        if character in "\n\t":
            cleaned.append(character)
            continue
        codepoint = ord(character)
        category = unicodedata.category(character)
        unsupported_emoji = (0x1F000 <= codepoint <= 0x1FFFF) or codepoint in {
            0x20E3,
            0xFE0E,
            0xFE0F,
        }
        if character in {"\u200c", "\u200d"}:
            previous_category = unicodedata.category(text[index - 1]) if index else ""
            next_category = (
                unicodedata.category(text[index + 1]) if index + 1 < len(text) else ""
            )
            if previous_category[:1] in {"L", "M"} and next_category[:1] in {
                "L",
                "M",
            }:
                cleaned.append(character)
            else:
                cleaned.append(" ")
        elif category in {"Cc", "Cf", "Cs", "Co", "Cn"} or unsupported_emoji:
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
    return next(
        (
            _clean_text(review.get("note"))
            for review in reviews
            if review.get("decision") == "approved"
            and review.get("reviewer") != "openledger-reliability-migration"
        ),
        "",
    )


def _claim_display_and_link(
    field_name: str, claim: Mapping[str, Any]
) -> tuple[str, str]:
    """Return a human-readable value and its optional approved public URL."""
    raw_value = claim.get("value")
    display_value = _display_value(claim)
    link_url = ""
    if isinstance(raw_value, Mapping):
        candidate_url = str(raw_value.get("url") or "").strip()
        if _safe_http_url(candidate_url):
            link_url = candidate_url
        if field_name == "social_account":
            platform = _clean_text(raw_value.get("platform")).removeprefix("www.")
            username = _clean_text(
                raw_value.get("username") or raw_value.get("handle")
            ).lstrip("@")
            if platform and username:
                display_value = f"{_platform_label(platform)} - @{username}"
            elif platform:
                display_value = _platform_label(platform)
            elif username:
                display_value = f"@{username}"
    elif field_name in {"photograph", "social_account", "website"}:
        candidate_url = str(raw_value or display_value).strip()
        if _safe_http_url(candidate_url):
            link_url = candidate_url
            if field_name == "social_account":
                parsed = urlparse(candidate_url)
                handle = parsed.path.strip("/").split("/")[-1]
                display_value = (
                    f"{_platform_label(parsed.hostname or 'Public account')}"
                    + (f" - @{handle.lstrip('@')}" if handle else "")
                )
    return display_value, link_url


def _platform_label(value: str) -> str:
    hostname = _clean_text(value).casefold().removeprefix("www.")
    labels = {
        "facebook.com": "Facebook",
        "github.com": "GitHub",
        "instagram.com": "Instagram",
        "linkedin.com": "LinkedIn",
        "tiktok.com": "TikTok",
        "twitter.com": "X / Twitter",
        "x.com": "X",
        "youtube.com": "YouTube",
    }
    return labels.get(hostname, _clean_text(value))


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
    evidence_count = 0
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
                    evidence_count += 1
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
                display_value, link_url = _claim_display_and_link(field_name, claim)
                item = {
                    "id": str(claim.get("id") or ""),
                    "field_name": field_name,
                    "value": display_value,
                    "link_url": link_url,
                    "confidence": int(claim.get("confidence") or 0),
                    "reviewed_by": _clean_text(claim.get("reviewed_by")),
                    "reviewed_at": str(claim.get("reviewed_at") or ""),
                    "first_seen_at": str(claim.get("first_seen_at") or ""),
                    "last_seen_at": str(claim.get("last_seen_at") or ""),
                    "latitude": claim.get("latitude"),
                    "longitude": claim.get("longitude"),
                    "approval_note": _approved_review_note(claim),
                    "coordinate_selection": claim.get('coordinate_selection'),
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

    source_catalog = []
    source_references: Dict[tuple[str, str, str, str], str] = {}
    for claim in canonical_claims:
        claim_source_refs = []
        for evidence in claim["evidence"]:
            source_key = (
                evidence["source_url"],
                evidence["source_name"],
                evidence["evidence_type"],
                evidence["observed_at"],
            )
            reference = source_references.get(source_key)
            if reference is None:
                reference = f"S{len(source_catalog) + 1:02d}"
                source_references[source_key] = reference
                source_catalog.append({"reference": reference, **evidence})
            if reference not in claim_source_refs:
                claim_source_refs.append(reference)
        claim["source_refs"] = claim_source_refs

    approved_sites = []
    for site in persona.get('affiliation_sites') or []:
        if site.get('review_status') != 'approved' or site.get('origin_review_status', 'approved') != 'approved':
            continue
        evidence = site.get('evidence') or {}
        resolution = site.get('resolution') or {}
        candidate = resolution.get('candidate') or {}
        decision = next((record for record in site.get('history') or []
                         if record.get('snapshot', {}).get('action') != 'lookup'), {})
        reference = f"S{len(source_catalog) + 1:02d}"
        source_catalog.append({'reference': reference, 'source_url': evidence.get('source_url') or '',
            'source_name': evidence.get('organization') or 'Affiliation published address',
            'evidence_type': 'Public organizational address', 'observed_at': evidence.get('retrieved_at') or ''})
        value = evidence.get('address') or ''
        if resolution.get('status') == 'resolved':
            value += f" — {candidate.get('latitude')}, {candidate.get('longitude')} ({candidate.get('precision')}; {candidate.get('method')})"
        else:
            value += ' — unmapped'
        approved_sites.append({'id': site['id'], 'field_name': 'affiliation_site', 'value': value,
            'address_type': resolution.get('address_type') or evidence.get('address_type') or 'unknown',
            'link_url': evidence.get('source_url'), 'source_refs': [reference], 'confidence': 0,
            'reviewed_by': decision.get('reviewer'), 'reviewed_at': decision.get('created_at'),
            'approval_note': decision.get('reason'), 'resolution': resolution,
            'source_date': evidence.get('source_date'), 'is_person_location': False})
        evidence_count += 1
    canonical_record = {
        'affiliation_sites': approved_sites,
        "persona_id": str(persona.get("id") or ""),
        "case_id": str(persona.get("case_id") or ""),
        "case_title": _clean_text(persona.get("case_title")),
        "display_name": _clean_text(persona.get("display_name")),
        "approved_claims": canonical_claims,
        "sources": source_catalog,
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
        "source_count": len(source_catalog),
        "evidence_count": evidence_count,
        "groups": groups,
        "snapshot_sha256": hashlib.sha256(record_bytes).hexdigest(),
    }


def _claims_by_field(snapshot: Mapping[str, Any]) -> Dict[str, list[Dict[str, Any]]]:
    claims: Dict[str, list[Dict[str, Any]]] = {}
    for claim in snapshot.get("approved_claims") or []:
        field_name = str(claim.get("field_name") or "")
        claims.setdefault(field_name, []).append(dict(claim))
    for field_claims in claims.values():
        field_claims.sort(
            key=lambda claim: (
                -int(claim.get("confidence") or 0),
                str(claim.get("value") or "").casefold(),
                str(claim.get("id") or ""),
            )
        )
    return claims


def _report_item(
    claim: Mapping[str, Any],
    *,
    label: str,
    secondary: str = "",
) -> Dict[str, Any]:
    return {
        "label": label,
        "value": str(claim.get("value") or "-"),
        "secondary": secondary,
        "confidence": int(claim.get("confidence") or 0),
        "claim_ids": [str(claim.get("id") or "")],
        "source_refs": list(claim.get("source_refs") or []),
        "reviewed_at": str(claim.get("reviewed_at") or ""),
        "reviewed_by": str(claim.get("reviewed_by") or ""),
        "approval_note": str(claim.get("approval_note") or ""),
        "link_url": str(claim.get("link_url") or ""),
        "latitude": claim.get("latitude"),
        "longitude": claim.get("longitude"),
    }


def _shared_source_count(first: Mapping[str, Any], second: Mapping[str, Any]) -> int:
    def source_keys(claim: Mapping[str, Any]) -> set[str]:
        urls = {
            str(evidence.get("source_url") or "").strip()
            for evidence in claim.get("evidence") or []
            if str(evidence.get("source_url") or "").strip()
        }
        return urls or {
            str(reference) for reference in claim.get("source_refs") or [] if reference
        }

    return len(source_keys(first) & source_keys(second))


def _affiliation_items(
    claims: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[Dict[str, Any]]:
    """Pair a role and organization only when the same approved source supports both."""
    roles = list(claims.get("occupation") or [])
    organizations = list(claims.get("company") or [])
    used_organizations = set()
    items = []
    for role in roles:
        candidates = [
            (index, organization, _shared_source_count(role, organization))
            for index, organization in enumerate(organizations)
            if index not in used_organizations
        ]
        candidates = [candidate for candidate in candidates if candidate[2] > 0]
        if candidates:
            index, organization, _ = max(
                candidates,
                key=lambda candidate: (
                    candidate[2],
                    int(candidate[1].get("confidence") or 0),
                    str(candidate[1].get("value") or "").casefold(),
                ),
            )
            used_organizations.add(index)
            item = _report_item(
                role,
                label="Position",
                secondary=str(organization.get("value") or "-"),
            )
            item["claim_ids"].append(str(organization.get("id") or ""))
            item["secondary_label"] = "Affiliation"
            item["secondary_confidence"] = int(organization.get("confidence") or 0)
            item["secondary_source_refs"] = list(organization.get("source_refs") or [])
            item["secondary_reviewed_at"] = str(organization.get("reviewed_at") or "")
            item["secondary_approval_note"] = str(
                organization.get("approval_note") or ""
            )
            items.append(item)
        else:
            items.append(_report_item(role, label="Position or occupation"))
    for index, organization in enumerate(organizations):
        if index not in used_organizations:
            items.append(_report_item(organization, label="Affiliation"))
    return items


def build_investigation_report_view(
    snapshot: Mapping[str, Any],
) -> Dict[str, Any]:
    """Build a CV-like, human-centred projection of approved evidence."""
    claims = _claims_by_field(snapshot)
    full_name_claim = next(iter(claims.get("full_name") or []), None)
    alternate_names = [
        _report_item(claim, label="Other approved name")
        for claim in list(claims.get("full_name") or [])[1:]
    ]
    photograph_claim = next(iter(claims.get("photograph") or []), None)
    summary_claim = next(iter(claims.get("summary") or []), None)
    locations = [
        _report_item(claim, label="Current location")
        for claim in claims.get("current_location") or []
    ]
    address_items = [
        _report_item(claim, label="Approved public address")
        for claim in claims.get("address") or []
    ]
    contacts = [
        *(_report_item(claim, label="Email") for claim in claims.get("email") or []),
        *(
            _report_item(claim, label="Telephone")
            for claim in claims.get("phone") or []
        ),
    ]
    digital_presence = [
        *(
            _report_item(claim, label="Social account")
            for claim in claims.get("social_account") or []
        ),
        *(
            _report_item(claim, label="Website")
            for claim in claims.get("website") or []
        ),
        *(
            _report_item(claim, label="Platform identifier")
            for claim in claims.get("platform_identifier") or []
        ),
        *(
            _report_item(claim, label="Linked public profile")
            for claim in claims.get("linked_profile_lead") or []
        ),
        *(
            _report_item(claim, label="Account-registration record")
            for claim in claims.get("account_registration") or []
        ),
    ]
    assets = [
        *(
            _report_item(claim, label="Ownership or leadership interest")
            for claim in claims.get("company_ownership") or []
        ),
        *(
            _report_item(claim, label="Financial profile")
            for claim in claims.get("financial_profile") or []
        ),
        *(
            _report_item(claim, label="Vehicle ownership")
            for claim in claims.get("vehicle_ownership") or []
        ),
    ]
    risk_indicators = [
        *(
            _report_item(claim, label="Offshore database match")
            for claim in claims.get("offshore_database_match") or []
        ),
        *(
            _report_item(claim, label="Criminal-record reference")
            for claim in claims.get("criminal_record") or []
        ),
    ]
    photograph = (
        _report_item(photograph_claim, label="Approved photograph")
        if photograph_claim
        else None
    )
    return {
        "name": (
            str(full_name_claim.get("value") or snapshot.get("display_name") or "")
            if full_name_claim
            else str(snapshot.get("display_name") or "Unnamed person")
        ),
        "name_confidence": (
            int(full_name_claim.get("confidence") or 0) if full_name_claim else None
        ),
        "name_source_refs": (
            list(full_name_claim.get("source_refs") or []) if full_name_claim else []
        ),
        "alternate_names": alternate_names,
        "photograph": photograph,
        "summary": (
            _report_item(summary_claim, label="Investigative summary")
            if summary_claim
            else None
        ),
        "locations": locations,
        "affiliation_sites": [_report_item(site, label='Affiliation site · ' + site['address_type'])
                               for site in snapshot.get('affiliation_sites') or []],
        "addresses": address_items,
        "affiliations": _affiliation_items(claims),
        "contacts": contacts,
        "digital_presence": digital_presence,
        "assets": assets,
        "risk_indicators": risk_indicators,
        "sources": list(snapshot.get("sources") or []),
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


def _font_sequence(text: str, style: ParagraphStyle) -> list[Optional[str]]:
    """Choose fonts while keeping join controls inside their script cluster."""
    fonts = [_font_for_character(character, style) for character in text]
    for index, character in enumerate(text):
        if character not in {"\u200c", "\u200d"}:
            continue
        previous_font = fonts[index - 1] if index else None
        next_font = fonts[index + 1] if index + 1 < len(fonts) else None
        fonts[index] = (
            previous_font
            if previous_font == next_font or previous_font is not None
            else next_font
        )
    return fonts


def _font_markup(cleaned: str, style: ParagraphStyle) -> str:
    if not cleaned:
        return ""
    fonts = _font_sequence(cleaned, style)
    fragments = []
    start = 0
    current_font = fonts[0]
    for index, font_name in enumerate(fonts[1:], start=1):
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
    fonts = _font_sequence(text, style)
    current_font = fonts[0] if fonts else None
    for index, font_name in enumerate(fonts[1:], start=1):
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


def _shape_visual_ltr_words(
    fragments: Sequence[Any], available_width: float
) -> list[Any]:
    """Shape non-RTL words after bidi conversion without reshaping RTL glyphs."""
    shaped_words = []
    for word in _getFragWords(fragments, available_width):
        text = "".join(
            str(fragment_text)
            for fragment, fragment_text in word[1:]
            if not hasattr(fragment, "cbDefn")
        )
        shaped_words.append(
            word if not text or _contains_rtl(text) else shapeFragWord(word)
        )
    return shaped_words


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
        # Arabic is explicitly reshaped and bidi-reordered above; applying
        # HarfBuzz again to the visual presentation forms would double-shape it.
        rtl_style.shaping = 0
        rtl_style.wordWrap = "LTR"
        lines = _wrap_rtl_lines(
            self._openledger_logical_text,
            rtl_style,
            available_width,
        )
        markup = "<br/>".join(_font_markup(line, rtl_style) for line in lines) or "-"
        Paragraph.__init__(self, markup, rtl_style)
        self.frags = _shape_visual_ltr_words(self.frags, available_width)
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
    visible_text = (
        cleaned if len(cleaned) <= 320 else f"{cleaned[:200]} ... {cleaned[-100:]}"
    )
    visible = _escaped_paragraph_text(visible_text, style)
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
        "eyebrow": ParagraphStyle(
            "ReportEyebrow",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=7.2,
            leading=9,
            textColor=_TEAL,
            spaceAfter=1.5 * mm,
        ),
        "title": ParagraphStyle(
            "InvestigationTitle",
            parent=sample["Title"],
            fontName=bold_font,
            fontSize=24,
            leading=29,
            textColor=_NAVY,
            alignment=TA_LEFT,
            spaceAfter=2 * mm,
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
            "InvestigationSection",
            parent=sample["Heading2"],
            fontName=bold_font,
            fontSize=11.5,
            leading=14,
            textColor=colors.white,
        ),
        "section_intro": ParagraphStyle(
            "InvestigationSectionIntro",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=8,
            leading=11,
            textColor=_MUTED,
            spaceAfter=2.5 * mm,
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
        "item_label": ParagraphStyle(
            "InvestigationItemLabel",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=7.2,
            leading=9,
            textColor=_MUTED,
            spaceAfter=1 * mm,
        ),
        "item_value": ParagraphStyle(
            "InvestigationItemValue",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=10.2,
            leading=14,
            textColor=_INK,
            wordWrap="CJK",
            splitLongWords=1,
            spaceAfter=1 * mm,
        ),
        "item_secondary": ParagraphStyle(
            "InvestigationItemSecondary",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=8.2,
            leading=11,
            textColor=_INK,
            wordWrap="CJK",
            splitLongWords=1,
            spaceAfter=1 * mm,
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
            "CertaintyBadge",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=7.1,
            leading=9,
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
        style.shaping = 1
        style.openledger_primary_coverage = _font_coverage(style.fontName)
        style.openledger_fallback_fonts = tuple(fallback_fonts)
    return styles


def _certainty_tier(score: int) -> tuple[str, Any, Any]:
    if score >= 80:
        return "HIGH", _APPROVED, colors.HexColor("#E8F6F1")
    if score >= 60:
        return "MODERATE", colors.HexColor("#087D87"), _SOFT_TEAL
    return "LIMITED", _AMBER, _SOFT_AMBER


def _certainty_badge(
    score: Optional[int], styles: Mapping[str, ParagraphStyle]
) -> Table:
    if score is None:
        text, foreground, background = "CERTAINTY N/A", _MUTED, _PANEL
    else:
        tier, foreground, background = _certainty_tier(int(score))
        text = f"CERTAINTY {int(score)}% - {tier}"
    style = ParagraphStyle(
        f"Certainty-{text}",
        parent=styles["badge"],
        textColor=foreground,
    )
    badge = Table([[_paragraph(text, style)]], colWidths=[39 * mm])
    badge.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), background),
                ("BOX", (0, 0), (-1, -1), 0.5, foreground),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return badge


def _source_refs(item: Mapping[str, Any]) -> str:
    references = [str(value) for value in item.get("source_refs") or [] if value]
    return ", ".join(references) if references else "No attached source reference"


def _linked_item_value(
    item: Mapping[str, Any], styles: Mapping[str, ParagraphStyle]
) -> Paragraph:
    value = _clean_text(item.get("value")) or "-"
    url = str(item.get("link_url") or "").strip()
    if _safe_http_url(url):
        visible = _escaped_paragraph_text(value, styles["item_value"])
        return Paragraph(
            f"<link href={quoteattr(url)} color='#087D87'>{visible}</link>",
            styles["item_value"],
        )
    return _paragraph(value, styles["item_value"])


def _item_heading(
    label: str,
    score: Optional[int],
    styles: Mapping[str, ParagraphStyle],
) -> Table:
    heading = Table(
        [
            [
                _paragraph(label or "Approved information", styles["item_label"]),
                _certainty_badge(score, styles),
            ]
        ],
        colWidths=[_PAGE_WIDTH - 43 * mm, 43 * mm],
    )
    heading.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                ("LEFTPADDING", (0, 0), (0, 0), 0),
                ("RIGHTPADDING", (0, 0), (0, 0), 6),
                ("LEFTPADDING", (1, 0), (1, 0), 0),
                ("RIGHTPADDING", (1, 0), (1, 0), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )
    return heading


def _report_item_flowables(
    item: Mapping[str, Any],
    styles: Mapping[str, ParagraphStyle],
    *,
    risk: bool = False,
) -> list[Any]:
    background = _SOFT_RISK if risk else colors.white
    border = colors.HexColor("#E4BBBB") if risk else _LINE
    value_style = deepcopy(styles["item_value"])
    value_style.backColor = background
    value_style.borderColor = border
    value_style.borderWidth = 0.6
    value_style.borderPadding = 7
    value_style.spaceBefore = 0
    value_style.spaceAfter = 0
    value = dict(item)
    value_paragraph = _linked_item_value(value, {**styles, "item_value": value_style})
    confidence = item.get("confidence")
    heading = _item_heading(
        str(item.get("label") or "Approved information"),
        int(confidence) if confidence is not None else None,
        styles,
    )
    flowables: list[Any] = [CondPageBreak(22 * mm), heading, value_paragraph]
    flowables.extend(
        [
            Spacer(1, 1 * mm),
            _paragraph(
                f"Sources: {_source_refs(item)} | Reviewed {_format_time(str(item.get('reviewed_at') or ''))}",
                styles["small"],
            ),
        ]
    )
    if item.get("approval_note"):
        flowables.extend(
            [
                Spacer(1, 0.8 * mm),
                _paragraph(
                    f"Analyst note: {item['approval_note']}",
                    styles["item_secondary"],
                ),
            ]
        )
    if item.get("secondary"):
        secondary_confidence = item.get("secondary_confidence")
        secondary_value = {
            "value": item["secondary"],
            "link_url": "",
        }
        secondary_source = {
            "source_refs": item.get("secondary_source_refs") or [],
        }
        flowables.extend(
            [
                Spacer(1, 2 * mm),
                _item_heading(
                    str(item.get("secondary_label") or "Related approved fact"),
                    (
                        int(secondary_confidence)
                        if secondary_confidence is not None
                        else None
                    ),
                    styles,
                ),
                _linked_item_value(
                    secondary_value,
                    {**styles, "item_value": value_style},
                ),
                Spacer(1, 1 * mm),
                _paragraph(
                    f"Sources: {_source_refs(secondary_source)} | Reviewed {_format_time(str(item.get('secondary_reviewed_at') or ''))}",
                    styles["small"],
                ),
            ]
        )
        if item.get("secondary_approval_note"):
            flowables.extend(
                [
                    Spacer(1, 0.8 * mm),
                    _paragraph(
                        f"Analyst note: {item['secondary_approval_note']}",
                        styles["item_secondary"],
                    ),
                ]
            )
    return flowables


def _section_header(
    title: str,
    styles: Mapping[str, ParagraphStyle],
    *,
    risk: bool = False,
) -> Table:
    background = _RISK if risk else _NAVY
    table = Table(
        [[_paragraph(title, styles["section"])]],
        colWidths=[_PAGE_WIDTH],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), background),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    return table


def _append_report_section(
    story: list[Any],
    *,
    title: str,
    introduction: str,
    items: Sequence[Mapping[str, Any]],
    empty_message: str,
    styles: Mapping[str, ParagraphStyle],
    risk: bool = False,
) -> None:
    story.append(CondPageBreak(42 * mm))
    story.extend(
        [
            _section_header(title, styles, risk=risk),
            Spacer(1, 1.5 * mm),
            _paragraph(introduction, styles["section_intro"]),
        ]
    )
    if not items:
        story.append(
            Table(
                [[_paragraph(empty_message, styles["body"])]],
                colWidths=[_PAGE_WIDTH],
                style=TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, -1), _PANEL),
                        ("BOX", (0, 0), (-1, -1), 0.5, _LINE),
                        ("LEFTPADDING", (0, 0), (-1, -1), 8),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                        ("TOPPADDING", (0, 0), (-1, -1), 7),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                    ]
                ),
            )
        )
    else:
        for index, item in enumerate(items):
            if index:
                story.append(Spacer(1, 1.5 * mm))
            story.extend(_report_item_flowables(item, styles, risk=risk))
    story.append(Spacer(1, 4 * mm))


def _initials(name: str) -> str:
    words = [word for word in _clean_text(name).split() if word]
    return "".join(word[0].upper() for word in words[:2]) or "?"


def _portrait_box(
    report: Mapping[str, Any],
    portrait_bytes: Optional[bytes],
    styles: Mapping[str, ParagraphStyle],
) -> Table:
    if portrait_bytes:
        visual: Any = ReportImage(
            io.BytesIO(portrait_bytes), width=40 * mm, height=40 * mm
        )
        caption = _paragraph("Approved public photograph", styles["small"])
    else:
        initials_style = ParagraphStyle(
            "PortraitInitials",
            parent=styles["title"],
            fontSize=22,
            leading=26,
            alignment=TA_CENTER,
            textColor=colors.white,
        )
        visual = Table(
            [[_paragraph(_initials(str(report.get("name") or "")), initials_style)]],
            colWidths=[40 * mm],
            rowHeights=[40 * mm],
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), _NAVY_LIGHT),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ]
            ),
        )
        caption = _paragraph(
            "No approved photograph was available to embed", styles["small"]
        )
    photo = report.get("photograph")
    certainty = _certainty_badge(
        int(photo.get("confidence") or 0) if isinstance(photo, Mapping) else None,
        styles,
    )
    table = Table(
        [[visual], [certainty], [caption]],
        colWidths=[46 * mm],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), _PANEL),
                ("BOX", (0, 0), (-1, -1), 0.6, _LINE),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return table


def _hero(
    report: Mapping[str, Any],
    portrait_bytes: Optional[bytes],
    styles: Mapping[str, ParagraphStyle],
) -> Table:
    full_name = str(report.get("name") or "Unnamed person")
    displayed_name = full_name
    name_note = None
    if len(full_name) > _HERO_NAME_LIMIT:
        displayed_name = f"{full_name[:_HERO_NAME_LIMIT].rstrip()}..."
        name_note = _paragraph(
            "Name abbreviated in the profile header; the complete approved value is retained in the evidence and audit register.",
            styles["small"],
        )
    name_details = [
        _paragraph("INVESTIGATION SUBJECT", styles["eyebrow"]),
        _paragraph(displayed_name, styles["title"]),
        _certainty_badge(report.get("name_confidence"), styles),
    ]
    if name_note is not None:
        name_details.extend([Spacer(1, 1.5 * mm), name_note])
    name_details.extend(
        [
            Spacer(1, 3 * mm),
            _paragraph(
                "Human-centred profile compiled from analyst-approved identity, location, affiliation, contact, asset, and risk records.",
                styles["subtitle"],
            ),
        ]
    )
    hero = Table(
        [[_portrait_box(report, portrait_bytes, styles), name_details]],
        colWidths=[50 * mm, _PAGE_WIDTH - 50 * mm],
    )
    hero.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (0, 0), 0),
                ("RIGHTPADDING", (0, 0), (0, 0), 8),
                ("LEFTPADDING", (1, 0), (1, 0), 6),
                ("RIGHTPADDING", (1, 0), (1, 0), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    return hero


def _location_map_card(
    location: Mapping[str, Any],
    map_bytes: Optional[bytes],
    styles: Mapping[str, ParagraphStyle],
    *,
    location_count: int,
) -> Table:
    if map_bytes:
        visual: Any = ReportImage(
            io.BytesIO(map_bytes),
            width=_PAGE_WIDTH - 12,
            height=(_PAGE_WIDTH - 12) * 360 / 920,
        )
    else:
        visual = Table(
            [
                [
                    _paragraph(
                        "Map unavailable - the approved location remains listed without inferred coordinates.",
                        styles["body"],
                    )
                ]
            ],
            colWidths=[_PAGE_WIDTH - 12],
            rowHeights=[28 * mm],
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), _PANEL),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            ),
        )
    coordinates = ""
    if location.get("latitude") is not None and location.get("longitude") is not None:
        coordinates = (
            f" | Approved coordinates: {location['latitude']}, {location['longitude']}"
        )
    caption = _paragraph(
        _location_map_caption(
            location,
            location_count=location_count,
            map_available=bool(map_bytes),
            coordinates=coordinates,
        ),
        styles["small"],
    )
    confidence = location.get("confidence")
    return Table(
        [
            [visual],
            [
                _certainty_badge(
                    int(confidence) if confidence is not None else None,
                    styles,
                )
            ],
            [caption],
        ],
        colWidths=[_PAGE_WIDTH],
        style=TableStyle(
            [
                ("BOX", (0, 0), (-1, -1), 0.6, _LINE),
                ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                ("ALIGN", (0, 1), (0, 1), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        ),
    )


def _location_map_caption(
    location: Mapping[str, Any],
    *,
    location_count: int,
    map_available: bool,
    coordinates: str,
) -> str:
    map_label = (
        f"Map excerpt (1 of {location_count} approved locations)"
        if map_available
        else "Map unavailable"
    )
    return (
        f"{map_label}: {location.get('value') or 'Approved location'}{coordinates} | "
        f"Sources: {_source_refs(location)}"
    )


def _approved_location_coordinates(
    location: Mapping[str, Any],
) -> Optional[tuple[float, float]]:
    """Return finite, range-valid coordinates from an approved location item."""
    try:
        latitude = float(location["latitude"])
        longitude = float(location["longitude"])
    except (KeyError, TypeError, ValueError):
        return None
    if not (
        math.isfinite(latitude)
        and math.isfinite(longitude)
        and -90 <= latitude <= 90
        and -180 <= longitude <= 180
    ):
        return None
    return latitude, longitude


def _audit_metadata(snapshot: Mapping[str, Any], styles: Mapping[str, Any]) -> Table:
    rows = [
        ["Case", snapshot["case_title"] or snapshot["case_id"]],
        ["Persona ID", snapshot["persona_id"]],
        ["Generated", _format_time(snapshot["generated_at"])],
        ["Generated by", snapshot["generated_by"]],
        [
            "Coverage",
            f"{snapshot['approved_count']} approved facts | {snapshot['source_count']} unique sources | {snapshot['evidence_count']} evidence links",
        ],
        ["Record snapshot SHA-256", snapshot["snapshot_sha256"]],
    ]
    table = Table(
        [
            [
                _paragraph(label, styles["small_bold"]),
                _paragraph(value, styles["small"]),
            ]
            for label, value in rows
        ],
        colWidths=[42 * mm, _PAGE_WIDTH - 42 * mm],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), _PANEL),
                ("BOX", (0, 0), (-1, -1), 0.5, _LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, _LINE),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def _claim_register(
    snapshot: Mapping[str, Any], styles: Mapping[str, Any]
) -> list[Any]:
    flowables: list[Any] = []
    for claim in snapshot.get("approved_claims") or []:
        if flowables:
            flowables.append(Spacer(1, 1.5 * mm))
        field_name = str(claim.get("field_name") or "")
        claim_value = claim.get("value") or "-"
        if field_name == "photograph":
            claim_value = "Embedded approved public photograph"
        flowables.extend(
            _report_item_flowables(
                {
                    "label": field_name.replace("_", " ").title(),
                    "value": claim_value,
                    "confidence": int(claim.get("confidence") or 0),
                    "source_refs": list(claim.get("source_refs") or []),
                    "reviewed_at": str(claim.get("reviewed_at") or ""),
                    "reviewed_by": str(claim.get("reviewed_by") or ""),
                    "approval_note": str(claim.get("approval_note") or ""),
                    "link_url": str(claim.get("link_url") or ""),
                },
                styles,
            )
        )
    if not flowables:
        flowables.append(
            _paragraph(
                "No approved facts were present at generation time.", styles["body"]
            )
        )
    return flowables


def _source_register(snapshot: Mapping[str, Any], styles: Mapping[str, Any]) -> Table:
    rows = [
        [
            _paragraph("Ref", styles["table_header"]),
            _paragraph("Source", styles["table_header"]),
            _paragraph("Type / observed", styles["table_header"]),
            _paragraph("Exact public provenance", styles["table_header"]),
        ]
    ]
    for source in snapshot.get("sources") or []:
        rows.append(
            [
                _paragraph(source["reference"], styles["small_bold"]),
                _paragraph(source["source_name"] or "Unnamed source", styles["small"]),
                _paragraph(
                    f"{source['evidence_type'] or '-'}\n{_format_time(source['observed_at'])}",
                    styles["small"],
                ),
                _source_url_paragraph(source["source_url"], styles["small"]),
            ]
        )
    if len(rows) == 1:
        rows.append(
            [
                _paragraph("-", styles["small"]),
                _paragraph("No supporting source record", styles["small"]),
                _paragraph("-", styles["small"]),
                _paragraph("No public URL recorded", styles["small"]),
            ]
        )
    table = Table(
        rows,
        colWidths=[12 * mm, 40 * mm, 36 * mm, _PAGE_WIDTH - 88 * mm],
        repeatRows=1,
        splitByRow=True,
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), _NAVY_LIGHT),
                ("BOX", (0, 0), (-1, -1), 0.5, _LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, _LINE),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _PANEL]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def generate_persona_pdf(
    persona: Mapping[str, Any],
    *,
    generated_by: str,
    generated_at: Optional[datetime] = None,
) -> bytes:
    """Return a self-contained, human-centred approved-evidence report."""
    generated_at = generated_at or datetime.now(timezone.utc)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    snapshot = build_persona_export_snapshot(
        persona,
        generated_at=generated_at,
        generated_by=generated_by,
    )
    report = build_investigation_report_view(snapshot)
    required_text = "".join(_iter_snapshot_text(snapshot))
    regular_font, bold_font, fallback_fonts = _register_fonts(required_text)
    styles = _styles(regular_font, bold_font, fallback_fonts)

    portrait_bytes = None
    photograph = report.get("photograph")
    if isinstance(photograph, Mapping) and photograph.get("link_url"):
        try:
            portrait_bytes = load_approved_portrait(str(photograph["link_url"]))
        except Exception:
            portrait_bytes = None

    map_bytes = None
    mapped_location = next(
        (
            location
            for location in report["locations"]
            if _approved_location_coordinates(location) is not None
        ),
        None,
    )
    if mapped_location:
        try:
            coordinates = _approved_location_coordinates(mapped_location)
            if coordinates is not None:
                map_bytes = render_location_map(*coordinates)
        except Exception:
            map_bytes = None

    output = io.BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=28 * mm,
        bottomMargin=18 * mm,
        title=f"Investigation Report - {report['name']}",
        author="OpenLedger",
        subject="Human-centred analyst-approved investigation report",
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
            "INVESTIGATION REPORT",
        )
        canvas.setStrokeColor(_LINE)
        canvas.line(18 * mm, 12 * mm, A4[0] - 18 * mm, 12 * mm)
        canvas.setFont(regular_font, 7)
        canvas.setFillColor(_MUTED)
        canvas.drawString(18 * mm, 7.5 * mm, "Approved evidence only")
        canvas.drawRightString(A4[0] - 18 * mm, 7.5 * mm, f"Page {doc.page}")
        canvas.restoreState()

    story: list[Any] = [
        _hero(report, portrait_bytes, styles),
        Spacer(1, 4 * mm),
        Table(
            [
                [
                    _paragraph(
                        "Reading guide: certainty is the stored evidence score for each approved fact. It is not a probability of identity, guilt, ownership, or legal responsibility. Pending, uncertain, rejected, and reliability-untriaged records are excluded.",
                        styles["notice"],
                    )
                ]
            ],
            colWidths=[_PAGE_WIDTH],
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), _SOFT_TEAL),
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

    executive_items = [
        *([report["summary"]] if report.get("summary") else []),
        *report["alternate_names"],
    ]
    _append_report_section(
        story,
        title="Executive profile",
        introduction=(
            "The profile summary is an approved synthesis. Each supporting fact retains its own certainty and source references in the sections and audit register below."
        ),
        items=executive_items,
        empty_message="No executive summary has been approved for this person.",
        styles=styles,
    )

    story.extend(
        [
            CondPageBreak(100 * mm),
            _section_header("Location", styles),
            Spacer(1, 1.5 * mm),
            _paragraph(
                "All analyst-approved locations are listed below. A map, when available, is an explicitly labelled excerpt for one approved location and does not replace the complete list.",
                styles["section_intro"],
            ),
        ]
    )
    if mapped_location:
        story.append(
            _location_map_card(
                mapped_location,
                map_bytes,
                styles,
                location_count=len(report["locations"]),
            )
        )
    elif report["locations"]:
        story.append(
            _paragraph(
                "No map excerpt is available because none of the approved locations has usable approved coordinates. No coordinates were inferred for this report.",
                styles["small"],
            )
        )
    else:
        story.append(
            _location_map_card(
                {
                    "value": "No approved location",
                    "confidence": 0,
                    "source_refs": [],
                },
                None,
                styles,
                location_count=0,
            )
        )
    if report["locations"]:
        story.extend(
            [
                Spacer(1, 2 * mm),
                _paragraph("Complete approved location list", styles["section_intro"]),
            ]
        )
    for item in [*report["locations"], *report["addresses"]]:
        story.append(Spacer(1, 1.5 * mm))
        story.extend(_report_item_flowables(item, styles))
    story.append(Spacer(1, 4 * mm))

    if report['affiliation_sites']:
        _append_report_section(story, title='Reviewed affiliation sites',
            introduction='Published organizational sites are separate from personal location or branch assignment. All approved sites, including unmapped records, are listed with their source and review.',
            items=report['affiliation_sites'], empty_message='', styles=styles)
    _append_report_section(
        story,
        title="Affiliations and positions",
        introduction=(
            "Roles and organizations are paired only when the same approved source supports both; otherwise they remain separate facts."
        ),
        items=report["affiliations"],
        empty_message="No affiliation or position has been approved for this person.",
        styles=styles,
    )
    _append_report_section(
        story,
        title="Known contacts and public presence",
        introduction=(
            "Approved contact points and public accounts are listed as investigative reference data, not proof that the subject currently controls them."
        ),
        items=[*report["contacts"], *report["digital_presence"]],
        empty_message="No contact point or public account has been approved.",
        styles=styles,
    )
    _append_report_section(
        story,
        title="Assets and economic interests",
        introduction=(
            "Only approved ownership, financial-profile, and vehicle records appear here. Absence means no approved record, not absence of assets."
        ),
        items=report["assets"],
        empty_message="No asset or economic-interest record has been approved.",
        styles=styles,
    )
    _append_report_section(
        story,
        title="Risk indicators",
        introduction=(
            "Risk indicators are review prompts. A database match or record reference does not independently establish wrongdoing, criminality, or legal responsibility."
        ),
        items=report["risk_indicators"],
        empty_message="No risk-indicator record has been approved.",
        styles=styles,
        risk=True,
    )

    story.extend(
        [
            PageBreak(),
            _section_header("Evidence and audit register", styles),
            Spacer(1, 2 * mm),
            _paragraph(
                "The readable profile above is a projection of this exact approved record. Source references connect every displayed fact to the public provenance retained at generation time.",
                styles["section_intro"],
            ),
            _audit_metadata(snapshot, styles),
            Spacer(1, 4 * mm),
            _paragraph("Approved fact register", styles["field"]),
        ]
    )
    story.extend(_claim_register(snapshot, styles))
    story.extend(
        [
            CondPageBreak(105 * mm),
            Spacer(1, 5 * mm),
            _paragraph("Public source register", styles["field"]),
            _source_register(snapshot, styles),
            Spacer(1, 4 * mm),
            _paragraph(
                "This report records curated investigative information and provenance. It does not independently establish identity, wrongdoing, ownership, criminality, or legal responsibility.",
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
    return f"openledger-investigation-report-{slug}-{timestamp}.pdf"
