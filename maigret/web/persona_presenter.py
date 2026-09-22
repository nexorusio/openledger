"""Read-only adapters for the single Persona presentation contract.

P2 is the canonical runtime model.  This legacy adapter exists only so a code
deployment can safely precede the separately authorized production conversion;
it does not write, consolidate, or reinterpret stored legacy records.
"""

from __future__ import annotations

import math
from typing import Any
from urllib.parse import urlsplit

from maigret.web.persona_schema import (
    PERSONA_SECTIONS,
    display_label,
    presentation_predicate,
    section_for,
)


def _public_url(value: Any) -> str:
    value = str(value or "").strip()
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme in {"http", "https"}
            and parsed.hostname
            and not parsed.username
            and not parsed.password
        ):
            return value
    except ValueError:
        pass
    return ""


def _claim_value(claim: dict[str, Any]) -> str:
    value = claim.get("display_value") or claim.get("value")
    if isinstance(value, dict):
        value = value.get("url") or value.get("name") or value.get("title") or value
    return str(value or claim.get("field_name") or "Approved finding")


def legacy_persona_projection(persona: dict[str, Any]) -> dict[str, Any]:
    """Expose approved legacy claims through the same contract as P2.

    The adapter deliberately leaves every original row untouched.  Reviewable
    claims are supplied separately to the shared review panel; only approved
    records enter the reader-facing Persona sections.
    """
    raw_items = []
    for claim in persona.get("claims") or []:
        if claim.get("review_status") != "approved":
            continue
        normalized = {
            "predicate": claim.get("field_name"),
            "field_name": claim.get("field_name"),
            "value": claim.get("value"),
            "display_value": claim.get("display_value"),
            "qualifiers": {
                "latitude": claim.get("latitude"),
                "longitude": claim.get("longitude"),
                "coordinate_precision": next(
                    (
                        (evidence.get("details") or {}).get("coordinate_precision")
                        for evidence in claim.get("evidence") or []
                        if (evidence.get("details") or {}).get("coordinate_precision")
                    ),
                    None,
                ),
            },
        }
        field_key = presentation_predicate("claim", normalized)
        label = _claim_value(claim)
        item_url = _public_url(
            claim.get("source_url")
            or (claim.get("value") if isinstance(claim.get("value"), str) else "")
            or label
        )
        evidence_rows = []
        for evidence in claim.get("evidence") or []:
            evidence_rows.append(
                {
                    "id": str(evidence.get("id") or evidence.get("fingerprint") or ""),
                    "label": str(
                        evidence.get("source_name")
                        or evidence.get("evidence_type")
                        or "Retained legacy source"
                    ),
                    "url": _public_url(evidence.get("source_url")),
                    "outcome": str(
                        evidence.get("native_status")
                        or evidence.get("evidence_type")
                        or "observed"
                    ).replace("_", " "),
                }
            )
        raw_items.append(
            {
                "id": claim["id"],
                "kind": "claim",
                "normalized": normalized,
                "field_key": field_key,
                "label": label,
                "url": item_url,
                "section": section_for("claim", normalized),
                "decision_actor": claim.get("reviewed_by"),
                "decision_reason": next(
                    (
                        review.get("note")
                        for review in reversed(claim.get("reviews") or [])
                        if review.get("note")
                    ),
                    "",
                ),
                "group_ids": [claim["id"]],
                "evidence": evidence_rows,
                "legacy_claim": claim,
                "source_fetch": None,
            }
        )

    # Match the P2 reader projection: exact field/value duplicates share one
    # record while all claim IDs and evidence remain visible in its audit view.
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for item in raw_items:
        identity = (item["field_key"], item["label"].strip().casefold())
        record = merged.setdefault(identity, item)
        if record is item:
            continue
        if item["id"] not in record["group_ids"]:
            record["group_ids"].append(item["id"])
        evidence_ids = {entry["id"] for entry in record["evidence"]}
        record["evidence"].extend(
            entry for entry in item["evidence"] if entry["id"] not in evidence_ids
        )
    items = list(merged.values())

    def hero_value(*predicates: str) -> str:
        wanted = {value.casefold() for value in predicates}
        for item in items:
            if item["field_key"] in wanted and item["label"].strip():
                return item["label"].strip()[:700]
        return ""

    hero = {
        "summary": "",
        "location": hero_value("current_location"),
        "affiliation": hero_value("company"),
        "occupation": hero_value("occupation"),
    }
    subject = hero_value("full_name") or persona.get("display_name") or "This person"
    clauses = []
    if hero["occupation"] and hero["affiliation"]:
        clauses.append(f"{subject} is {hero['occupation']} at {hero['affiliation']}")
    elif hero["occupation"]:
        clauses.append(f"{subject} is {hero['occupation']}")
    elif hero["affiliation"]:
        clauses.append(f"{subject} is associated with {hero['affiliation']}")
    if hero["location"]:
        clauses.append(f"Based in {hero['location']}")
    if not clauses:
        summary = hero_value("summary")
        if summary:
            clauses.append(summary)
    hero["summary"] = ". ".join(clauses).rstrip(".") + "." if clauses else ""

    for index, item in enumerate(items, start=1):
        item["modal_id"] = f"persona-evidence-{index}"
        item["evidence_count"] = len(item["evidence"])

    map_points = []
    for item in items:
        if item["field_key"] not in {
            "address",
            "current_location",
            "organization_location",
        }:
            continue
        qualifiers = item["normalized"].get("qualifiers") or {}
        try:
            latitude = float(qualifiers.get("latitude"))
            longitude = float(qualifiers.get("longitude"))
        except (TypeError, ValueError):
            continue
        if not (
            math.isfinite(latitude)
            and math.isfinite(longitude)
            and -90 <= latitude <= 90
            and -180 <= longitude <= 180
        ):
            continue
        map_points.append(
            {
                "id": item["id"],
                "label": item["label"],
                "latitude": latitude,
                "longitude": longitude,
                "predicate": item["field_key"],
                "precision": qualifiers.get("coordinate_precision") or "place",
            }
        )

    fields_by_section = {
        section["key"]: tuple(section["fields"]) for section in PERSONA_SECTIONS
    }
    sections = []
    for section in PERSONA_SECTIONS:
        key = section["key"]
        section_items = [item for item in items if item["section"] == key]
        known = {field_name for field_name, _label in fields_by_section[key]}
        field_names = [field_name for field_name, _label in fields_by_section[key]]
        field_names.extend(
            sorted({item["field_key"] for item in section_items} - known)
        )
        sections.append(
            {
                "key": key,
                "title": section["title"],
                "items": section_items,
                "fields": [
                    {
                        "key": field_name,
                        "label": (
                            "Other approved findings"
                            if field_name == "other"
                            else display_label(field_name)
                        ),
                        "items": [
                            item
                            for item in section_items
                            if item["field_key"] == field_name
                        ],
                    }
                    for field_name in field_names
                ],
            }
        )

    photographs = [
        item["url"]
        for item in items
        if item["field_key"] == "photograph" and item["url"]
    ]
    source_urls = list(dict.fromkeys(item["url"] for item in items if item.get("url")))
    return {
        "items": items,
        "photograph": photographs[0] if photographs else "",
        "hero": hero,
        "map_points": map_points,
        "source_urls": source_urls,
        "source_fetches": {},
        "sections": sections,
    }


def legacy_workspace_projection(persona: dict[str, Any]) -> dict[str, Any]:
    """Expose legacy counts to the shared shell without fabricating P2 rows."""
    claims = list(persona.get("claims") or [])
    counts = {
        status: sum(claim.get("review_status") == status for claim in claims)
        for status in ("pending", "approved", "uncertain", "rejected")
    }
    unresolved = counts["pending"] + counts["uncertain"]
    return {
        "approved_count": counts["approved"],
        "review_pending_count": unresolved,
        "shortlist_count": len(claims),
        "decision_filter_counts": {
            "all": len(claims),
            "pending": counts["pending"],
            "include": counts["approved"],
            "rejected": counts["rejected"],
            "unresolved": counts["uncertain"],
        },
        "section_states": {},
        "legacy_counts": counts,
    }
