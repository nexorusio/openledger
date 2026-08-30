"""Deterministic, evidence-only persona claim extraction for OpenLedger."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Optional
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
            ("account_registration", "Email registration evidence"),
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

AI_PROPOSAL_FIELDS = {
    "summary",
    "full_name",
    "current_location",
    "occupation",
    "company",
    "social_account",
    "website",
    "photograph",
}

AI_FIELD_LIMITS = {
    "summary": 1200,
    "full_name": 300,
    "current_location": 300,
    "occupation": 500,
    "company": 500,
    "social_account": 2000,
    "website": 2000,
    "photograph": 2000,
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


def extract_ai_persona_claims(
    raw_proposals: Any,
    *,
    sources: Iterable[Dict[str, Any]],
    usernames: Iterable[str],
    model: str,
    diagnostics: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Validate cited AI suggestions and convert them to pending claim inputs.

    AI output is untrusted even when Structured Outputs is enabled. Only a
    deliberately narrow public-biographical allowlist is accepted, and every
    proposal must cite one URL returned by the preceding web-search response.
    """
    if diagnostics is not None:
        diagnostics.clear()
        diagnostics.update(received=0, accepted=0, rejected={})

    def reject(reason: str) -> None:
        if diagnostics is None:
            return
        rejected = diagnostics["rejected"]
        rejected[reason] = int(rejected.get(reason, 0)) + 1

    source_catalog: Dict[str, Dict[str, str]] = {}
    for source_item in sources:
        if not isinstance(source_item, dict):
            continue
        safe_url = _public_url(source_item.get("url"))
        if not safe_url:
            continue
        source_catalog[safe_url] = {
            "url": safe_url,
            "title": str(source_item.get("title") or urlparse(safe_url).netloc).strip()[
                :300
            ],
        }
    usernames_by_key = {
        str(username).strip().casefold(): str(username).strip()[:500]
        for username in usernames
        if str(username).strip()
    }
    if not isinstance(raw_proposals, list):
        return []
    if diagnostics is not None:
        diagnostics["received"] = min(len(raw_proposals), 100)
    if not source_catalog:
        return []
    if not usernames_by_key:
        return []

    candidates: List[Dict[str, Any]] = []
    seen = set()
    for raw in raw_proposals[:100]:
        if not isinstance(raw, dict):
            reject("invalid_record")
            continue
        username = usernames_by_key.get(
            str(raw.get("username") or "").strip().casefold()
        )
        field_name = str(raw.get("field_name") or "").strip()
        source_url = _public_url(raw.get("source_url"))
        source_record = source_catalog.get(source_url)
        if not username:
            reject("unknown_username")
            continue
        if field_name not in AI_PROPOSAL_FIELDS:
            reject("unsupported_field")
            continue
        if not source_record:
            reject("uncited_source")
            continue
        value = str(raw.get("value") or "").strip()
        if not value:
            reject("empty_value")
            continue
        value = value[: AI_FIELD_LIMITS[field_name]]
        if field_name in {"social_account", "website", "photograph"}:
            value = _public_url(value)
            if not value or value not in source_catalog:
                reject("invalid_public_url")
                continue
        else:
            value = " ".join(value.split())
        try:
            confidence = int(str(raw.get("confidence") or ""))
        except (TypeError, ValueError):
            reject("invalid_confidence")
            continue
        if confidence < 40:
            reject("low_confidence")
            continue
        confidence = min(confidence, 85)
        reason = " ".join(str(raw.get("reason") or "").split())[:1000]
        if not reason:
            reject("missing_reason")
            continue

        latitude = raw.get("latitude")
        longitude = raw.get("longitude")
        coordinate_precision = raw.get("coordinate_precision")
        coordinates_supplied = any(
            item is not None
            for item in (latitude, longitude, coordinate_precision)
        )
        if coordinates_supplied:
            if (
                field_name != "current_location"
                or latitude is None
                or longitude is None
                or coordinate_precision not in {"city", "region"}
            ):
                reject("invalid_coordinate_proposal")
                continue
            try:
                latitude = float(latitude)
                longitude = float(longitude)
            except (TypeError, ValueError):
                reject("invalid_coordinate_proposal")
                continue
            if (
                not math.isfinite(latitude)
                or not math.isfinite(longitude)
                or not -90 <= latitude <= 90
                or not -180 <= longitude <= 180
            ):
                reject("invalid_coordinate_proposal")
                continue
        else:
            latitude = None
            longitude = None
            coordinate_precision = None

        stored_value: Any = value
        if field_name == "social_account":
            hostname = urlparse(value).hostname or "Public account"
            stored_value = {
                "platform": hostname.removeprefix("www.")[:300],
                "url": value,
                "username": username,
            }
        fingerprint = claim_fingerprint(field_name, stored_value)
        deduplication_key = (username.casefold(), fingerprint, source_url)
        if deduplication_key in seen:
            reject("duplicate_proposal")
            continue
        seen.add(deduplication_key)
        evidence: Dict[str, Any] = {
            "evidence_type": "cited_public_web",
            "source_name": source_record["title"],
            "source_url": source_url,
            "details": {
                "investigated_username": username,
                "proposal_reason": reason,
                "model": str(model).strip()[:100],
                "human_review_required": True,
            },
        }
        if coordinate_precision:
            evidence["details"].update(
                coordinate_precision=coordinate_precision,
                coordinate_role="approximate_map_center",
                proposed_latitude=latitude,
                proposed_longitude=longitude,
            )
        candidates.append(
            {
                "username": username,
                "field_name": field_name,
                "value": stored_value,
                "display_value": _display_value(stored_value),
                "normalized_value": _normalized_value(stored_value),
                "confidence": confidence,
                "fingerprint": fingerprint,
                "source_engine": "openai_web_research",
                "latitude": latitude,
                "longitude": longitude,
                "evidence": [
                    dict(evidence, fingerprint=evidence_fingerprint(evidence))
                ],
            }
        )
    if diagnostics is not None:
        diagnostics["accepted"] = len(candidates)
    return candidates


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
