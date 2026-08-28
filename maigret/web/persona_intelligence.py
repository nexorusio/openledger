"""Deterministic, evidence-only persona claim extraction for OpenLedger."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

FIELD_GROUPS: tuple[Dict[str, Any], ...] = (
    {
        "key": "identity",
        "title": "Identity",
        "fields": (
            ("summary", "Summary of the target"),
            ("full_name", "Full name"),
            ("photograph", "Photograph"),
        ),
    },
    {
        "key": "contact",
        "title": "Contact and location",
        "fields": (
            ("email", "Email address"),
            ("phone", "Phone number"),
            ("address", "Address"),
            ("current_location", "Current location"),
        ),
    },
    {
        "key": "online",
        "title": "Digital presence",
        "fields": (
            ("social_account", "Social media and public accounts"),
            ("website", "Website"),
        ),
    },
    {
        "key": "professional",
        "title": "Professional and corporate",
        "fields": (
            ("occupation", "Occupation"),
            ("company", "Company"),
            ("company_ownership", "Company ownership"),
        ),
    },
    {
        "key": "assets",
        "title": "Assets and risk records",
        "fields": (
            ("financial_profile", "Financial profile"),
            ("vehicle_ownership", "Vehicle ownership"),
            ("criminal_record", "Criminal record"),
        ),
    },
)


FIELD_ALIASES = {
    "about": "summary",
    "bio": "summary",
    "biography": "summary",
    "description": "summary",
    "displayname": "full_name",
    "display_name": "full_name",
    "fullname": "full_name",
    "full_name": "full_name",
    "name": "full_name",
    "email": "email",
    "emails": "email",
    "e-mail": "email",
    "mail": "email",
    "mobile": "phone",
    "phone": "phone",
    "phone_number": "phone",
    "telephone": "phone",
    "address": "address",
    "street_address": "address",
    "city": "current_location",
    "country": "current_location",
    "location": "current_location",
    "current_location": "current_location",
    "avatar": "photograph",
    "avatar_url": "photograph",
    "image": "photograph",
    "image_url": "photograph",
    "photo": "photograph",
    "photo_url": "photograph",
    "picture": "photograph",
    "employer": "company",
    "organization": "company",
    "organisation": "company",
    "company": "company",
    "job": "occupation",
    "job_title": "occupation",
    "occupation": "occupation",
    "profession": "occupation",
    "title": "occupation",
    "homepage": "website",
    "url": "website",
    "website": "website",
}


CONFIDENCE_SCORES = {
    "strong": 80,
    "moderate": 65,
    "weak": 35,
    "unverified": 25,
}


def _public_url(value: Any) -> str:
    candidate = str(value or "").strip()
    try:
        parsed = urlparse(candidate)
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return candidate[:2000]


def _display_value(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("url") or value.get("value") or "").strip()[:4000]
    return str(value).strip()[:4000]


def _normalized_value(value: Any) -> str:
    if isinstance(value, dict):
        normalized = json.dumps(value, sort_keys=True, ensure_ascii=False)
    else:
        normalized = " ".join(str(value).split()).casefold()
    return normalized[:4000]


def claim_fingerprint(field_name: str, value: Any) -> str:
    content = f"{field_name}\0{_normalized_value(value)}".encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def evidence_fingerprint(evidence: Dict[str, Any]) -> str:
    content = json.dumps(evidence, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _iter_scalar_values(value: Any) -> Iterable[str]:
    if isinstance(value, (str, int, float, bool)):
        candidate = str(value).strip()
        if candidate:
            yield candidate[:4000]
        return
    if isinstance(value, (list, tuple, set)):
        for item in list(value)[:20]:
            yield from _iter_scalar_values(item)


def _claim(
    *,
    field_name: str,
    value: Any,
    confidence: int,
    username: str,
    source_name: str,
    source_url: str,
    evidence_type: str,
) -> Dict[str, Any]:
    safe_url = _public_url(source_url)
    evidence = {
        "evidence_type": evidence_type,
        "source_name": str(source_name or "Public source")[:300],
        "source_url": safe_url,
        "details": {"investigated_username": username[:500]},
    }
    return {
        "field_name": field_name,
        "value": value,
        "display_value": _display_value(value),
        "normalized_value": _normalized_value(value),
        "confidence": max(0, min(100, int(confidence))),
        "fingerprint": claim_fingerprint(field_name, value),
        "source_engine": "openledger_profile_discovery",
        "evidence": [dict(evidence, fingerprint=evidence_fingerprint(evidence))],
    }


def extract_persona_claims(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert one normalized report into traceable claims without inference."""
    username = str(report.get("username") or "").strip()
    claims: List[Dict[str, Any]] = []
    for profile in report.get("claimed_profiles") or []:
        source_name = str(profile.get("site_name") or "Public source")
        source_url = _public_url(profile.get("url"))
        if not source_url:
            continue
        base_score = CONFIDENCE_SCORES.get(
            str(profile.get("confidence") or "unverified").lower(), 25
        )
        account_value = {
            "platform": source_name[:300],
            "url": source_url,
            "username": username[:500],
        }
        claims.append(
            _claim(
                field_name="social_account",
                value=account_value,
                confidence=base_score,
                username=username,
                source_name=source_name,
                source_url=source_url,
                evidence_type="observed_profile",
            )
        )

        evidence = profile.get("evidence") or {}
        if not isinstance(evidence, dict):
            continue
        for raw_key, raw_value in evidence.items():
            key = str(raw_key).strip().lower().replace(" ", "_")
            field_name = FIELD_ALIASES.get(key)
            if not field_name:
                continue
            for value in _iter_scalar_values(raw_value):
                if field_name in {"website", "photograph"}:
                    value = _public_url(value)
                    if not value:
                        continue
                claims.append(
                    _claim(
                        field_name=field_name,
                        value=value,
                        confidence=min(95, base_score + 10),
                        username=username,
                        source_name=source_name,
                        source_url=source_url,
                        evidence_type="extracted_profile_field",
                    )
                )
    return claims


def group_claims(claims: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return the fixed persona form, including empty evidence categories."""
    by_field: Dict[str, List[Dict[str, Any]]] = {}
    for claim in claims:
        by_field.setdefault(str(claim["field_name"]), []).append(claim)
    groups = []
    for group in FIELD_GROUPS:
        fields = []
        for key, label in group["fields"]:
            fields.append(
                {
                    "key": key,
                    "label": label,
                    "claims": sorted(
                        by_field.get(key, []),
                        key=lambda item: (
                            item.get("review_status") != "approved",
                            -int(item.get("confidence") or 0),
                            item.get("display_value") or "",
                        ),
                    ),
                }
            )
        groups.append({"key": group["key"], "title": group["title"], "fields": fields})
    return groups
