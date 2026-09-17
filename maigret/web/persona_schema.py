"""Canonical reader-facing Persona taxonomy.

Storage remains evidence-native: legacy claims keep their original ``field_name``
and P2 groups keep their normalized predicate.  Every Persona projection uses
this module so a historical label cannot move between sections depending on
which route rendered it.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

PERSONA_SECTIONS = (
    {
        "key": "identity",
        "title": "Identity",
        "description": "Approved identity information and evidence awaiting review.",
        "fields": (
            ("summary", "Summary of the target"),
            ("full_name", "Full name"),
            ("alias", "Known aliases"),
            ("date_of_birth", "Date of birth"),
            ("photograph", "Photograph"),
        ),
    },
    {
        "key": "contact",
        "title": "Contact and location",
        "description": "Public contact details and coarse, source-supported locations.",
        "fields": (
            ("email", "Email address"),
            ("phone", "Phone number"),
            ("address", "Address"),
            ("current_location", "Current location"),
        ),
    },
    {
        "key": "digital",
        "title": "Digital presence",
        "description": "Public accounts, identifiers, profile leads and websites.",
        "fields": (
            ("social_account", "Social media and public accounts"),
            ("username", "Known usernames"),
            ("platform_identifier", "Stable platform identifiers"),
            ("linked_profile_lead", "Linked profile leads"),
            ("account_registration", "Email registration evidence"),
            ("website", "Website"),
        ),
    },
    {
        "key": "affiliations",
        "title": "Affiliations",
        "description": "Employment, education, membership, institutional and ownership links.",
        "fields": (
            ("occupation", "Role or occupation"),
            ("company", "Organization, institution or company"),
            ("organization_location", "Organization location"),
            ("company_ownership", "Ownership or leadership"),
        ),
    },
    {
        "key": "public_exposure",
        "title": "Public exposure",
        "description": "Publicly documented news, events, speaking, interviews and authored work.",
        "fields": (
            ("news_mention", "News and media coverage"),
            ("event_appearance", "Events and public appearances"),
            ("speaking_engagement", "Speaking engagements"),
            ("interview", "Interviews and podcasts"),
            ("publication", "Publications and authored work"),
            ("award", "Awards and recognition"),
        ),
    },
    {
        "key": "records",
        "title": "Assets and risk records",
        "description": "Explicitly sourced public records; absence is never inferred.",
        "fields": (
            ("offshore_database_match", "Offshore Leaks record match"),
            ("financial_profile", "Financial profile"),
            ("vehicle_ownership", "Vehicle ownership"),
            ("criminal_record", "Criminal record"),
        ),
    },
)

FIELD_LABELS = {
    field_name: label
    for section in PERSONA_SECTIONS
    for field_name, label in section["fields"]
}

PREDICATE_ALIASES = {
    "about": "summary",
    "bio": "summary",
    "biography": "summary",
    "description": "summary",
    "displayname": "full_name",
    "display_name": "full_name",
    "fullname": "full_name",
    "name": "full_name",
    "birth_date": "date_of_birth",
    "employer": "company",
    "organization": "company",
    "organisation": "company",
    "affiliation": "company",
    "job_title": "occupation",
    "reviewed_occupation": "occupation",
    "role": "occupation",
    "location": "current_location",
    "city": "current_location",
    "news": "news_mention",
    "media_mention": "news_mention",
    "news_article": "news_mention",
    "event": "event_appearance",
    "public_event": "event_appearance",
    "public_appearance": "event_appearance",
    "conference_appearance": "event_appearance",
    "panel_appearance": "event_appearance",
    "talk": "speaking_engagement",
    "speaker": "speaking_engagement",
    "podcast_appearance": "interview",
    "authored_article": "publication",
    "article": "publication",
    "public_profile_description": "summary",
    "public_fact": "news_mention",
}

FIELD_SECTIONS = {
    field_name: section["key"]
    for section in PERSONA_SECTIONS
    for field_name, _label in section["fields"]
}

# Predicates emitted by specialist engines that intentionally sit outside the
# fixed display rows.  Keeping this allow-list separate makes the safety rule
# explicit: an unfamiliar public claim must never be presented as a risk
# record merely because its predicate is new.
EXTRA_FIELD_SECTIONS = {
    "ai_hypothesis": "public_exposure",
    "sanctions_record": "records",
}

_PUBLIC_EXPOSURE_LEGACY_PREDICATES = frozenset(
    {"affiliation", "organization", "organisation", "company", "employer"}
)
_PUBLIC_EXPOSURE_TITLE_PATTERNS = (
    (
        "speaking_engagement",
        re.compile(r"\b(keynote|speaker|speaking|presenter|panelist)\b", re.I),
    ),
    ("event_appearance", re.compile(r"\b(ph\.?d|doctoral)\s+defen[cs]e\b", re.I)),
    (
        "event_appearance",
        re.compile(
            r"\b(conference|symposium|seminar|workshop|webinar|public event|panel)\b",
            re.I,
        ),
    ),
    ("interview", re.compile(r"\b(interview|podcast)\b", re.I)),
    (
        "publication",
        re.compile(r"\b(publication|journal|authored article|research paper)\b", re.I),
    ),
    ("award", re.compile(r"\b(award|awardee|recipient)\b", re.I)),
)


def display_label(field_name: Any) -> str:
    key = str(field_name or "").strip()
    return FIELD_LABELS.get(key, key.replace("_", " ").title())


def presentation_predicate(kind: str, normalized: Mapping[str, Any] | None) -> str:
    """Return one presentation predicate without mutating retained evidence."""
    if kind == "account":
        return "social_account"
    normalized = normalized or {}
    predicate = (
        str(normalized.get("predicate") or normalized.get("field_name") or "")
        .strip()
        .casefold()
    )
    value = normalized.get("display_value") or normalized.get("value") or ""
    if predicate in _PUBLIC_EXPOSURE_LEGACY_PREDICATES:
        if isinstance(value, Mapping):
            value = value.get("title") or value.get("name") or ""
        text = " ".join(str(value).split())
        for corrected, pattern in _PUBLIC_EXPOSURE_TITLE_PATTERNS:
            if pattern.search(text):
                return corrected
    return PREDICATE_ALIASES.get(predicate, predicate or "other")


def section_for(kind: str, normalized: Mapping[str, Any] | None) -> str:
    field_name = presentation_predicate(kind, normalized)
    return FIELD_SECTIONS.get(
        field_name,
        EXTRA_FIELD_SECTIONS.get(field_name, "public_exposure"),
    )


def section_spec(key: str) -> Mapping[str, Any]:
    return next(section for section in PERSONA_SECTIONS if section["key"] == key)
