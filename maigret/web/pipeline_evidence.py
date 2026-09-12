"""Pure, replay-safe observation adapters for the complete P2 evidence pipeline.

An observation reports what one source attempt returned. It never assigns an
account to a person or turns a collection outcome into an identity probability.
Callers commit these documents in the same transaction as task progress.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SCHEMA_VERSION = "p2-e2e-v1"
NORMALIZER_VERSION = "p2-observation-v2"
_RETENTION_RANK = {
    "retained": 0,
    "metadata_only": 1,
    "transient": 2,
    "live_only": 3,
    "prohibited": 4,
}
_RETENTION_ALIASES = {
    "bounded_source_evidence": "retained",
    "permitted_place_ids_and_status": "metadata_only",
    "transient_display_only": "transient",
}
OUTCOMES = frozenset(
    {
        "found",
        "not_found",
        "candidate",
        "partial",
        "blocked",
        "inconclusive",
        "timeout",
        "error",
        "cancelled",
        "not_executed",
    }
)
_STATUSES = {
    "claimed": "found",
    "registered": "found",
    "observed": "found",
    "available": "not_found",
    "not registered": "not_found",
    "unregistered": "not_found",
    "not found": "not_found",
    "not_registered": "not_found",
    "absent": "not_found",
    "unknown": "inconclusive",
    "partial": "partial",
    "completed": "inconclusive",
    "rate_limited": "blocked",
    "captcha": "blocked",
    "forbidden": "blocked",
    "timed_out": "timeout",
    "timed out": "timeout",
    "failed": "error",
    "stopped": "cancelled",
    "canceled": "cancelled",
    "skipped": "not_executed",
    "excluded": "not_executed",
    "disabled": "not_executed",
    "illegal": "not_executed",
    "unavailable": "not_executed",
    "conditional": "not_executed",
    "archived": "found",
    "analyzed": "candidate",
    "not_archived": "not_found",
}
_TRACKING_PARAMS = frozenset({"fbclid", "gclid", "igshid", "igsh", "ref_src"})
_DERIVED_ENGINES = frozenset(
    {
        "unfurl_url_analysis",
        "wayback_cdx",
        "openai_web_research",
        "openai_public_web_research",
        "native_profile_search_review",
        "case_chat",
        "ai_research",
    }
)
_FIELD_ALIASES = {
    "fullname": "full_name",
    "name": "full_name",
    "location": "current_location",
    "description": "summary",
    "company": "affiliation",
    "bio": "summary",
}


class ObservationContractError(ValueError):
    """A document cannot be safely or reproducibly represented in the ledger."""


def resolve_retention(*policies: Any, has_locator: bool = False) -> Dict[str, Any]:
    """Intersect source, task and record policy; no layer can grant an exception.

    Historical manifests use the three named aliases above. Unknown modes fail
    closed, including at the ledger boundary for callers bypassing normalization.
    """
    mode, eligible = "retained", bool(has_locator)
    pending = list(policies)
    while pending:
        policy = pending.pop()
        if isinstance(policy, (list, tuple)):
            pending.extend(policy)
            continue
        if policy is None or policy == {}:
            continue
        if isinstance(policy, str):
            policy = {"mode": policy}
        if not isinstance(policy, Mapping):
            raise ObservationContractError("Retention policy must be a mode or object")
        candidate = policy.get("mode", "retained")
        if not isinstance(candidate, str):
            raise ObservationContractError("Unknown retention mode")
        if any(
            key in policy and not isinstance(policy[key], bool)
            for key in ("retained", "retainable", "final_eligible")
        ):
            raise ObservationContractError("Retention flags must be booleans")
        candidate = _RETENTION_ALIASES.get(candidate, candidate)
        if candidate not in _RETENTION_RANK:
            raise ObservationContractError("Unknown retention mode")
        if any(policy.get(key) is False for key in ("retained", "retainable")):
            candidate = max((candidate, "metadata_only"), key=_RETENTION_RANK.get)
        mode = max((mode, candidate), key=_RETENTION_RANK.get)
        eligible = eligible and policy.get("final_eligible") is not False
    return {
        "mode": mode,
        "final_eligible": eligible and mode == "retained",
        "reason": (
            "Retainable observation; attribution and explicit QC are still required."
            if mode == "retained"
            else "Source policy prohibits durable detail; obtain retainable supporting evidence."
        ),
    }


def enforce_observation_retention(
    raw: Mapping[str, Any], *, policy=None
) -> Dict[str, Any]:
    """Sanitize even direct ledger callers, before immutable-content hashing.

    Restricted records retain an execution receipt, never claims, URLs, content
    hashes, diagnostics, artifacts or nested native payloads. Only metadata-only
    sources may retain their explicitly allowed opaque provider identifiers.
    """
    raw = dict(raw)
    engine = str(raw.get("engine") or raw.get("source_engine") or "")
    provider_policy = (
        "metadata_only"
        if any('google_places' in str(value or '').replace('-', '_')
               for value in (raw.get('engine'), raw.get('source_engine')))
        else None
    )
    retention = resolve_retention(
        provider_policy,
        policy,
        raw.get("retention"),
        {"retained": raw.get("retained", True)},
        has_locator=bool(
            raw.get("source_url")
            or raw.get("canonical_url")
            or raw.get("url")
            or raw.get("artifact_ref")
            or raw.get("locator")
        ),
    )
    if retention["mode"] == "retained":
        return {**raw, "retention": retention}
    # IDs here belong to our persisted execution, not arbitrary provider fields.
    allowed = {
        key: raw[key]
        for key in (
            "id",
            "schema_version",
            "case_id",
            "subject_id",
            "persona_id",
            "request_id",
            "task_id",
            "attempt_id",
            "engine",
            "observed_at",
            "legacy",
            "engine_version",
            "parser_version",
            "normalizer_version",
            "provenance_incomplete",
        )
        if key in raw
    }
    for key, value in allowed.items():
        if key in {"legacy", "provenance_incomplete"}:
            if not isinstance(value, bool):
                raise ObservationContractError(
                    "Restricted receipt flags must be booleans"
                )
        elif key == "observed_at":
            allowed[key] = _timestamp(value)
        elif (
            not isinstance(value, str)
            or len(value) > 200
            or any(ord(char) < 32 for char in value)
        ):
            raise ObservationContractError(
                "Restricted receipt identifiers and versions must be bounded strings"
            )
    status, _ = normalize_status(raw.get("status") or raw.get("outcome"))
    payload = raw.get("payload") if isinstance(raw.get("payload"), Mapping) else raw
    metadata = {"status": status}
    if retention["mode"] == "metadata_only":
        for key in ("place_id", "source_record_id"):
            if key not in payload:
                continue
            value = payload[key]
            if (
                not isinstance(value, str)
                or not 1 <= len(value) <= 512
                or any(ord(char) < 32 for char in value)
            ):
                raise ObservationContractError(
                    "Restricted provider identifiers must be bounded strings"
                )
            metadata[key] = value
    allowed.update(
        status=status,
        native_status=status,
        retained=False,
        retention=retention,
        source_url=None,
        canonical_url=None,
        artifact_ref=None,
        origin_family_id=None,
        content_fingerprint=None,
        original_evidence_id=None,
        derived_from=[],
        dependence={"status": "unknown", "origin_url": None},
        claims=[],
        account=None,
        evidence_signals={},
        normalization_findings=[],
        payload=metadata,
        payload_fingerprint=fingerprint(metadata),
    )
    # Do not retain a provider-controlled ID containing live detail. The
    # observation's ledger ID remains available for idempotent delivery.
    allowed["native_record_id"] = "restricted-record"
    return allowed


def observation_evidence_role(observation: Mapping[str, Any], claim=None) -> str:
    """Assertion semantics are distinct from having appeared in a collection.

    Execution failures cannot assert facts even if a malformed provider declares
    them supporting. Candidate findings are proposed support, not confirmations.
    """
    if (observation.get("operator_disposition") or {}).get(
        "disposition"
    ) == "exclude_from_support":
        return "excluded_evidence"
    if observation.get("support_eligible") is False:
        return "historical_evidence"
    document = observation.get("payload") or observation
    native = document.get("payload") or document
    outcome = (
        observation.get("outcome")
        or document.get("status")
        or observation.get("status")
    )
    if outcome not in {"found", "candidate"}:
        return "absence_observation" if outcome == "not_found" else "collection_outcome"
    role = (
        document.get("evidence_role")
        or native.get("evidence_role")
        or native.get("assertion_role")
    )
    if claim:
        for candidate in document.get("claims", []):
            if (candidate.get("predicate") or candidate.get("field_name")) == claim.get(
                "predicate"
            ) and candidate.get("value") == claim.get("value"):
                role = (
                    candidate.get("evidence_role")
                    or candidate.get("assertion_role")
                    or role
                )
    if role in {"contradicts", "contradiction", "refutes"}:
        return "contradicts"
    if role in {"context", "mentions", "contextual"}:
        return "context"
    if role and role not in {"supports", "support"}:
        return "context"
    return "supports" if outcome == "found" else "candidate_support"


def _json_value(value: Any, depth: int = 0) -> Any:
    if depth > 20:
        raise ObservationContractError("Observation exceeds JSON nesting limit")
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ObservationContractError("Observation contains a non-finite number")
        return value
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, Enum):
        return _json_value(value.value, depth + 1)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item, depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item, depth + 1) for item in value]
    if callable(getattr(value, "as_dict", None)):
        return _json_value(value.as_dict(), depth + 1)
    if callable(getattr(value, "json", None)):
        return _json_value(value.json(), depth + 1)
    raise ObservationContractError("Observation contains an unsupported object")


def fingerprint(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


def _timestamp(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
    except ValueError as exc:
        raise ObservationContractError(
            "Observation timestamp must be ISO-8601"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ObservationContractError("Observation timestamp must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_source_url(value: Any) -> Optional[str]:
    """Conservative source URL normalization; path case and semantic query survive.

    This validates a locator, not network access. Fetchers still apply their own
    DNS/redirect checks. Unknown URL patterns never acquire a guessed identity.
    """
    if not isinstance(value, str) or not value.strip() or len(value) > 8000:
        return None
    value = value.strip()
    if any(ord(char) < 32 for char in value) or "\\" in value:
        return None
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or "").lower().rstrip(".")
        port = parsed.port
        if (
            parsed.scheme.lower() not in {"http", "https"}
            or not host
            or parsed.username
            or parsed.password
        ):
            return None
        if host == "localhost" or host.endswith(
            (".localhost", ".local", ".internal", ".lan")
        ):
            return None
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        if address is not None and not address.is_global:
            return None
        authority = "[" + host + "]" if ":" in host else host.encode("idna").decode()
        if port and (parsed.scheme.lower(), port) not in {("http", 80), ("https", 443)}:
            authority += ":" + str(port)
        # Do not sort unknown query parameters: some sources use their ordering.
        query = urlencode(
            [
                (key, val)
                for key, val in parse_qsl(parsed.query, keep_blank_values=True)
                if not key.lower().startswith("utm_")
                and key.lower() not in _TRACKING_PARAMS
            ]
        )
        return urlunsplit(
            (parsed.scheme.lower(), authority, parsed.path or "/", query, "")
        )
    except (ValueError, UnicodeError):
        return None


def canonical_origin_url(value: Any) -> Optional[str]:
    """Resolve known profile URL aliases so three fetchers share one page root."""
    url = canonical_source_url(value)
    if url:
        from maigret.web.pipeline_consolidation import _profile_reference

        reference = _profile_reference(url)
        if reference:
            return reference[1]
    return url


def normalize_status(value: Any, *, error: Any = None) -> tuple[str, str]:
    if isinstance(value, Mapping):
        error = value.get("error") or error
        value = value.get("status")
    if hasattr(value, "status"):
        error = getattr(value, "error", None) or error
        value = value.status
    if isinstance(value, Enum):
        value = value.value
    native = str(value or "unknown").strip()
    outcome = _STATUSES.get(native.casefold(), native.casefold())
    if outcome not in OUTCOMES:
        outcome = "inconclusive"
    # A detector's Unknown plus a timeout/captcha diagnostic is not a negative.
    if error and outcome == "inconclusive":
        code = str(
            error.get("code", "") if isinstance(error, Mapping) else error
        ).casefold()
        if "timeout" in code or "timed_out" in code:
            outcome = "timeout"
        elif any(
            term in code
            for term in ("captcha", "block", "rate_limit", "forbidden", "403", "429")
        ):
            outcome = "blocked"
        else:
            outcome = "error"
    return outcome, native


def _account(
    raw: Mapping[str, Any], engine: str, status: str
) -> Optional[Dict[str, Any]]:
    account = (
        dict(raw.get("account") or {})
        if isinstance(raw.get("account"), Mapping)
        else {}
    )
    if not account:
        # Existing Persona/AI/manual claims retain the account descriptor in
        # value rather than an outer account object. Recover that same account
        # hypothesis; this does not transfer a legacy approval into attribution.
        descriptors = [
            claim.get("value")
            for claim in raw.get("claims") or []
            if isinstance(claim, Mapping)
            and (claim.get("predicate") or claim.get("field_name")) == "social_account"
            and isinstance(claim.get("value"), Mapping)
        ]
        if len(descriptors) == 1:
            descriptor = descriptors[0]
            if descriptor.get("platform") and (
                descriptor.get("url") or descriptor.get("stable_id")
            ):
                account = dict(descriptor)
    account_engines = {
        "maigret",
        "maigret_report",
        "user_scanner_username",
        "github_public_profile",
        "native_profile_search",
        "native_profile_search_review",
    }
    if not account and engine not in account_engines and not raw.get("profile_url"):
        return None
    # An email registration probe may return the site's root, never an account.
    if (
        raw.get("subject_type") in {"email", "phone"}
        and not account
        and not raw.get("profile_url")
    ):
        return None
    extra = raw.get("extra") if isinstance(raw.get("extra"), Mapping) else {}
    account.setdefault(
        "platform",
        raw.get("platform") or raw.get("site_name") or raw.get("source_name"),
    )
    account.setdefault(
        "profile_url",
        account.get("canonical_url")
        or account.get("url")
        or raw.get("profile_url")
        or raw.get("url")
        or raw.get("source_url"),
    )
    account.setdefault(
        "handle",
        raw.get("handle")
        or raw.get("username")
        or (
            raw.get("subject_value") if raw.get("subject_type") == "username" else None
        ),
    )
    account.setdefault(
        "stable_id",
        raw.get("platform_account_id")
        or raw.get("stable_account_id")
        or extra.get("github_id"),
    )
    if engine == "github_public_profile":
        account["platform"] = "github"
    # Let the canonicalizer decide whether a generic public page is an account.
    from maigret.web.pipeline_consolidation import canonical_account

    return canonical_account(account, observed_at=raw.get("observed_at"))


def _claim_rows(
    raw: Mapping[str, Any], account: Optional[Dict[str, Any]], status: str
) -> list:
    if isinstance(raw.get("claims"), list):
        return [dict(item) for item in raw["claims"] if isinstance(item, Mapping)]
    if raw.get("predicate") or raw.get("field_name"):
        return [
            {
                key: raw[key]
                for key in (
                    "id",
                    "predicate",
                    "field_name",
                    "value",
                    "normalized_value",
                    "qualifiers",
                    "valid_from",
                    "valid_to",
                    "role",
                    "organization",
                    "account_key",
                    "subject_id",
                )
                if key in raw
            }
        ]
    if status not in {"found", "candidate"}:
        return []
    result = []
    if account:
        result.append(
            {
                "predicate": "social_account",
                "value": {
                    "platform": account["platform"],
                    "stable_id": account.get("stable_id"),
                    "url": account.get("canonical_url"),
                },
            }
        )
    if (
        raw.get("subject_type") == "email"
        and str(raw.get("status", "")).casefold() == "registered"
    ):
        result.append(
            {
                "predicate": "account_registration",
                "value": {
                    "platform": raw.get("site_name"),
                    "email": raw.get("subject_value"),
                },
                "qualifiers": {"ownership": "not_established"},
            }
        )
    # Extract only literal reported values; richer provider extractors can supply
    # their existing candidate dictionaries in raw.claims without losing fields.
    fields = raw.get("evidence")
    if isinstance(fields, Mapping):
        for name, value in fields.items():
            if name in _FIELD_ALIASES and value not in (None, "", [], {}):
                result.append({"predicate": _FIELD_ALIASES[name], "value": value})
    return result


def _collector_claims(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """Bridge the existing provider extractors without changing their evidence.

    Candidate account observations are retained even where legacy promotion rules
    elect not to create a Persona claim. This is a proposal, never approval.
    """
    engine = raw.get("source_engine") or raw.get("engine")
    supported = {
        "github_public_profile",
        "user_scanner_email",
        "user_scanner_username",
        "unfurl_url_analysis",
        "wayback_cdx",
        "wikipedia_public_biography",
        "icij_offshore_leaks",
    }
    if engine not in supported or "claims" in raw:
        return dict(raw)
    from maigret.web import collector_adapters

    adapters = {
        "github_public_profile": (
            collector_adapters.extract_github_profile_claims,
            True,
        ),
        "user_scanner_email": (collector_adapters.extract_user_scanner_claims, True),
        "user_scanner_username": (
            collector_adapters.extract_user_scanner_username_claims,
            True,
        ),
        "unfurl_url_analysis": (
            collector_adapters.extract_profile_url_evidence_claims,
            True,
        ),
        "wayback_cdx": (collector_adapters.extract_profile_url_evidence_claims, True),
        "wikipedia_public_biography": (
            collector_adapters.extract_wikipedia_person_claims,
            False,
        ),
        "icij_offshore_leaks": (collector_adapters.extract_icij_offshore_claims, False),
    }
    extractor, many = adapters[engine]
    try:
        claims = extractor([dict(raw)] if many else dict(raw))
    except (ValueError, TypeError, KeyError) as exc:
        # Native evidence survives an extractor/schema mismatch. The operator
        # sees why no structured claim was produced and can assess it directly.
        return {
            **raw,
            "claims": [],
            "normalization_findings": [
                {
                    "code": "legacy_extractor_failed",
                    "engine": engine,
                    "error_type": type(exc).__name__,
                    "reason": "Native payload retained; provider extraction requires review.",
                }
            ],
        }
    return {**raw, **({"claims": claims} if claims else {})}


def normalize_observation(
    raw: Any,
    *,
    case_id: str,
    subject_id: str,
    request_id: str,
    task_id: str,
    attempt_id: str,
    engine: Optional[str] = None,
    observed_at: Any = None,
    native_record_id: Optional[str] = None,
    legacy: bool = False,
    retention_policy: Any = None,
    engine_version: Optional[str] = None,
    parser_version: Optional[str] = None,
) -> Dict[str, Any]:
    """Normalize one source record. Missing historic times remain explicitly null.

    Supply the *same persisted attempt ID/time* on replay. A new attempt preserves
    a later observation, including an unchanged result. Stable source record ID
    collisions with a changed payload are rejected by persistence, not overwritten.
    """
    if not all(
        str(value or "").strip()
        for value in (case_id, subject_id, request_id, task_id, attempt_id)
    ):
        raise ObservationContractError(
            "case, subject, request, task and attempt IDs are required"
        )
    raw = _json_value(raw)
    if not isinstance(raw, dict):
        raise ObservationContractError("Observation must be an object")
    for key, expected in (("case_id", case_id), ("subject_id", subject_id)):
        if raw.get(key) is not None and str(raw[key]) != str(expected):
            raise ObservationContractError(
                "Observation scope conflicts with its persisted task"
            )
    source_engine = str(
        engine
        or raw.get("engine")
        or raw.get("source_engine")
        or raw.get("source_id")
        or "legacy_unknown"
    )
    raw = _collector_claims({**raw, "source_engine": source_engine})
    provenance = (
        raw.get("provenance") if isinstance(raw.get("provenance"), dict) else {}
    )
    outcome, native_status = normalize_status(
        raw.get("native_status")
        or raw.get("status")
        or raw.get("outcome")
        or raw.get("account_status"),
        error=raw.get("error"),
    )
    at = _timestamp(
        raw.get("observed_at")
        or raw.get("retrieved_at")
        or provenance.get("retrieved_at")
        or observed_at
    )
    if at is None and not legacy:
        raise ObservationContractError("Persisted attempt observation time is required")
    page = raw.get("page") if isinstance(raw.get("page"), dict) else {}
    original_url = (
        raw.get("source_url")
        or raw.get("url")
        or raw.get("profile_url")
        or page.get("url")
    )
    canonical_url = canonical_origin_url(original_url)
    retention = resolve_retention(
        "metadata_only" if "google_places" in source_engine.replace("-", "_") else None,
        retention_policy,
        raw.get("retention"),
        {"retained": raw.get("retained", True)},
        has_locator=bool(
            canonical_url or raw.get("locator") or raw.get("artifact_ref")
        ),
    )
    metadata_only = retention["mode"] != "retained"
    if metadata_only:
        # Explicit allowlist: live name/address/contact/coordinates, snippets and
        # their hashes are not durable evidence under the existing Places policy.
        original_url = canonical_url = None
        raw = {
            key: raw[key]
            for key in (
                "place_id",
                "source_record_id",
            )
            if key in raw and retention["mode"] == "metadata_only"
        }
        raw["status"] = outcome
        native_status = outcome
        provenance = {}
    derived_from = raw.get("derived_from") or provenance.get("derived_from") or []
    if isinstance(derived_from, str):
        derived_from = [derived_from]
    extra = raw.get("extra") if isinstance(raw.get("extra"), dict) else {}
    origin_url = canonical_origin_url(
        raw.get("original_url")
        or raw.get("origin_url")
        or provenance.get("original_url")
        or provenance.get("origin_url")
        or (
            extra.get("queried_profile_url") if source_engine == "wayback_cdx" else None
        )
        or (canonical_url if source_engine == "unfurl_url_analysis" else None)
    )
    is_derived = bool(
        derived_from
        or raw.get("is_copy")
        or raw.get("independence") == "derivative"
        or raw.get("source_dependence") == "search_excerpt_of_source"
        or source_engine in _DERIVED_ENGINES
        or raw.get("evidence_type")
        in {"search_snippet", "model_summary", "cached_copy", "mirror"}
    )
    if not origin_url and not is_derived:
        origin_url = canonical_url
    explicit_origin = (
        raw.get("origin_family_id")
        or raw.get("source_origin_family")
        or provenance.get("origin_family_id")
    )
    origin_id = (
        str(explicit_origin)
        if explicit_origin
        else ("origin:" + fingerprint(origin_url) if origin_url else None)
    )
    content_hash = raw.get("content_hash") or raw.get("content_fingerprint")
    if not content_hash and isinstance(raw.get("content"), str) and raw["content"]:
        content_hash = "sha256:" + hashlib.sha256(raw["content"].encode()).hexdigest()
    native_id = str(
        native_record_id
        or raw.get("native_record_id")
        or raw.get("source_record_id")
        or raw.get("candidate_id")
        or raw.get("id")
        or "record:" + fingerprint(raw)
    )
    identity = [
        SCHEMA_VERSION,
        str(case_id),
        str(subject_id),
        str(request_id),
        str(task_id),
        str(attempt_id),
        source_engine,
        native_id,
    ]
    findings = list(raw.get("normalization_findings") or [])
    try:
        account = None if metadata_only else _account(raw, source_engine, outcome)
    except ObservationContractError as exc:
        account = None
        findings.append(
            {
                "code": "account_identity_conflict",
                "reason": str(exc),
                "requires_operator_review": True,
            }
        )
    if account:
        account["observed_at"] = at
    payload_hash = fingerprint(raw)
    if len(json.dumps(raw, ensure_ascii=False).encode()) > 8_000_000:
        raise ObservationContractError(
            "Observation payload exceeds 8 MB; use a retained artifact reference"
        )
    result = {
        "id": "obs:" + fingerprint(identity),
        "schema_version": SCHEMA_VERSION,
        "case_id": str(case_id),
        "subject_id": str(subject_id),
        "request_id": str(request_id),
        "task_id": str(task_id),
        "attempt_id": str(attempt_id),
        "engine": source_engine,
        "engine_version": str(
            engine_version
            or raw.get("engine_version")
            or raw.get("source_version")
            or provenance.get("engine_version")
            or "unknown"
        ),
        "parser_version": str(
            parser_version
            or raw.get("parser_version")
            or provenance.get("parser_version")
            or NORMALIZER_VERSION
        ),
        "normalizer_version": NORMALIZER_VERSION,
        "native_record_id": native_id,
        "status": outcome,
        "native_status": native_status,
        "observed_at": at,
        "published_at": raw.get("published_at") or provenance.get("published_at"),
        "effective_at": raw.get("effective_at"),
        "source_url": original_url,
        "canonical_url": canonical_url,
        "origin_family_id": origin_id,
        "dependence": {
            "status": (
                ("derived" if is_derived else "known_origin")
                if origin_id
                else "unknown"
            ),
            "origin_url": origin_url,
        },
        "derived_from": list(derived_from),
        "content_fingerprint": content_hash,
        "payload_fingerprint": payload_hash,
        "artifact_ref": raw.get("artifact_ref") or raw.get("locator"),
        "original_evidence_id": raw.get("original_evidence_id")
        or raw.get("evidence_id"),
        "retention": retention,
        "payload": raw,
        "account": account,
        "claims": [] if metadata_only else _claim_rows(raw, account, outcome),
        "evidence_signals": raw.get("evidence_signals") or {},
        "normalization_findings": findings,
        "legacy": bool(legacy),
        "provenance_incomplete": at is None or source_engine == "legacy_unknown",
    }
    result["provenance_incomplete"] = (
        result["provenance_incomplete"]
        or result["engine_version"] == "unknown"
        or result["parser_version"] == "unknown"
    )
    return enforce_observation_retention(result, policy=retention_policy)


def _maigret_records(results: Iterable) -> Iterator[Dict[str, Any]]:
    for entry in results:
        if isinstance(entry, (tuple, list)) and len(entry) == 3:
            username, kind, sites = entry
            for site, result in sites.items():
                if not isinstance(result, Mapping):
                    continue
                status = result.get("status")
                checked = (
                    status.json()
                    if callable(getattr(status, "json", None))
                    else (dict(status) if isinstance(status, Mapping) else {})
                )
                # Retain source evidence and detector diagnostics, not sessions,
                # cookies, response objects or the full site configuration.
                record = {
                    key: result[key]
                    for key in (
                        "url",
                        "url_user",
                        "http_status",
                        "error",
                        "evidence",
                        "ids_data",
                        "ids",
                        "observed_at",
                        "engine_version",
                        "parser_version",
                        "source_record_id",
                        "content_hash",
                        "artifact_ref",
                    )
                    if key in result
                }
                record.update(checked)
                record.update(
                    source_engine="maigret",
                    site_name=site,
                    subject_type=kind,
                    subject_value=username,
                    username=username,
                )
                record["status"] = checked.get("status") or str(status or "unknown")
                record["url"] = record.get("url") or record.get("url_user")
                if getattr(status, "error", None):
                    record["error"] = str(status.error)
                yield record
        elif isinstance(entry, Mapping):
            yield dict(entry)


def _profile_audit_records(audit: Mapping[str, Any]) -> Iterator[Dict[str, Any]]:
    document = audit.get("document") or audit
    document = _json_value(document)
    audit_prefix = (
        str(audit.get("id") or audit.get("document_sha256") or fingerprint(document))
        + ":"
    )
    executed = set()
    for run in document.get("runs") or []:
        query = run.get("query") or {}
        query_id = query.get("query_id") or fingerprint(query)
        executed.add(query_id)
        common = {
            "source_engine": "native_profile_search",
            "platform": query.get("platform"),
            "provenance": run.get("provenance") or {},
            "query": query,
            "observed_at": audit.get("created_at"),
            "audit_id": audit.get("id"),
        }
        error = run.get("error")
        evidence = run.get("evidence") or []
        if not evidence:
            yield {
                **common,
                "native_record_id": audit_prefix + "query:" + str(query_id),
                "status": "unknown" if error else "not_found",
                "error": error,
            }
        for index, item in enumerate(evidence):
            yield {
                **common,
                **item,
                "native_record_id": audit_prefix
                + str(query_id)
                + ":"
                + str(item.get("result_id") or fingerprint(item)),
                "status": "candidate",
                "evidence_type": "search_snippet",
                "original_url": item.get("source_url"),
            }
    for query in document.get("queries") or []:
        query_id = query.get("query_id") or fingerprint(query)
        if query_id not in executed:
            yield {
                "source_engine": "native_profile_search",
                "native_record_id": audit_prefix + "query:" + str(query_id),
                "platform": query.get("platform"),
                "query": query,
                "status": "not_executed",
                "reason": "The persisted audit contains no execution for this planned query.",
                "audit_id": audit.get("id"),
                "observed_at": audit.get("created_at"),
            }
    # Grouped candidate documents can contain several original engine runs.
    # Retain the original run observations above; candidate grouping is derived.
    for candidate in document.get("candidates") or []:
        yield {
            **candidate,
            "source_engine": "native_profile_search_review",
            "status": "candidate",
            "native_record_id": audit_prefix
            + "candidate:"
            + str(candidate.get("candidate_id") or fingerprint(candidate)),
            "audit_id": audit.get("id"),
            "observed_at": audit.get("created_at"),
            "evidence_type": "model_summary",
        }


def iter_result_observations(
    result: Mapping[str, Any],
    *,
    case_id: str,
    subject_id: str,
    request_id: str,
    task_id: str,
    attempt_id: str,
    engine: Optional[str] = None,
    observed_at: Any = None,
    legacy: bool = False,
    retention_policy: Any = None,
    engine_version: Optional[str] = None,
    parser_version: Optional[str] = None,
) -> Iterator[Dict[str, Any]]:
    """Stream legacy report, collector, search-audit and manual evidence envelopes.

    No finding-count cap or success-only filter is used. Each native stream can
    also be passed separately while collection is running. ``source_observations``
    provides the common extension point for all new adapters.
    """
    scope = dict(
        case_id=case_id,
        subject_id=subject_id,
        request_id=request_id,
        task_id=task_id,
        attempt_id=attempt_id,
        engine=engine,
        observed_at=observed_at,
        retention_policy=retention_policy,
        engine_version=engine_version,
        parser_version=parser_version,
        legacy=legacy,
    )
    for raw in _maigret_records(result.get("general_results") or []):
        yield normalize_observation(raw, **scope)
    for report in result.get("individual_reports") or []:
        for profile in report.get("claimed_profiles") or []:
            yield normalize_observation(
                {
                    **profile,
                    "source_engine": "maigret_report",
                    "status": profile.get("status") or "claimed",
                    "username": report.get("username"),
                    "subject_type": "username",
                    "subject_value": report.get("username"),
                },
                **scope,
            )
    for key in (
        "collector_observations",
        "source_observations",
        "manual_evidence",
        "external_evidence",
        "claim_observations",
    ):
        records = result.get(key) or []
        if isinstance(records, Mapping):
            records = [records]
        for raw in records:
            if not isinstance(raw, Mapping):
                raise ObservationContractError("Source observation must be an object")
            # Native claim/evidence rows retain their original IDs as lineage.
            if key in {"external_evidence", "manual_evidence"}:
                raw = {
                    "source_engine": (
                        "external_evidence"
                        if key == "external_evidence"
                        else "manual_evidence"
                    ),
                    "status": "candidate",
                    **raw,
                }
            yield normalize_observation(
                _collector_claims(raw) if key == "collector_observations" else raw,
                **scope,
            )
    audits = result.get("profile_search_audits") or []
    if result.get("profile_search_audit"):
        audits = [*audits, result["profile_search_audit"]]
    for audit in audits:
        for raw in _profile_audit_records(audit):
            yield normalize_observation(raw, **scope)
    # A source error is evidence about execution even when no account was found.
    for error in result.get("source_errors") or []:
        raw = dict(error) if isinstance(error, Mapping) else {"error": str(error)}
        yield normalize_observation({"status": "unknown", **raw}, **scope)


def iter_legacy_claim_observations(
    claims: Iterable[Mapping[str, Any]],
    *,
    case_id: str,
    subject_id: str,
    request_id: str,
    task_id: str,
    attempt_id: str,
    observed_at: Any = None,
) -> Iterator[Dict[str, Any]]:
    """Backfill existing Persona claim/evidence rows with explicit legacy lineage.

    This also covers organization, address, AI and manually attached claims whose
    original provider envelopes were not persisted. Missing originals remain
    incomplete; a historic review is retained as metadata and never auto-finalized.
    Native rows may be supplied in batches, using stable backfill attempt IDs.
    """
    for claim in claims:
        if claim.get("persona_id") and str(claim["persona_id"]) != str(subject_id):
            raise ObservationContractError(
                "Legacy claim belongs to a different subject"
            )
        if claim.get("case_id") and str(claim["case_id"]) != str(case_id):
            raise ObservationContractError("Legacy claim belongs to a different case")
        evidence_rows = claim.get("evidence") or [{}]
        for evidence in evidence_rows:
            lineage = claim.get("observations") or claim.get("lineage") or []
            raw = {
                "source_engine": claim.get("source_engine") or "legacy_unknown",
                "native_record_id": "legacy:"
                + str(claim.get("id") or fingerprint(claim))
                + ":"
                + str(
                    evidence.get("id")
                    or evidence.get("fingerprint")
                    or fingerprint(evidence)
                ),
                "status": evidence.get("native_status") or "candidate",
                "source_url": evidence.get("source_url"),
                "source_name": evidence.get("source_name"),
                "evidence_type": evidence.get("evidence_type"),
                "evidence": evidence,
                "claims": [dict(claim)],
                "original_evidence_id": evidence.get("id"),
                "derived_from": [str(row["id"]) for row in lineage if row.get("id")],
                "legacy_claim_id": claim.get("id"),
                "legacy_review_status": claim.get("review_status"),
                "legacy_lineage": lineage,
                "observed_at": evidence.get("observed_at")
                or claim.get("observed_at")
                or observed_at,
            }
            yield normalize_observation(
                raw,
                case_id=case_id,
                subject_id=subject_id,
                request_id=request_id,
                task_id=task_id,
                attempt_id=attempt_id,
                legacy=True,
            )
