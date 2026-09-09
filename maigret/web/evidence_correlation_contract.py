"""Provider-neutral contracts for case-scoped evidence correlation."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlsplit

from maigret.web.profile_search_facebook import parse_facebook_profile_url
from maigret.web.profile_search_instagram import parse_instagram_profile_url
from maigret.web.profile_search_threads import parse_threads_profile_url
from maigret.web.profile_search_tiktok import parse_tiktok_profile_url
from maigret.web.profile_search_x import parse_x_profile_url


EVIDENCE_CORRELATION_SCHEMA_VERSION = 1
EVIDENCE_OUTCOMES = frozenset(
    {
        "observed",
        "absent",
        "private",
        "blocked",
        "rate_limited",
        "parser_error",
        "provider_error",
        "indeterminate",
    }
)
EVIDENCE_RELATIONSHIPS = frozenset(
    {"supporting", "duplicate", "conflicting", "unrelated"}
)
EVIDENCE_CONFIDENCE_SCOPES = frozenset({"correlation", "review_priority"})
# Descriptive aliases retained for callers that prefer contract-qualified names.
EVIDENCE_CORRELATION_OUTCOMES = EVIDENCE_OUTCOMES
EVIDENCE_RELATIONSHIP_KINDS = EVIDENCE_RELATIONSHIPS

MAX_CITATIONS = 32
MAX_CONFIDENCE_BASIS_ITEMS = 16
MAX_ENVELOPE_BYTES = 64 * 1024

_IDENTIFIER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,99}$")
_CLAIM_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9._:-]{0,99}$")
_CASE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_OBSERVATION_ID_PATTERN = re.compile(r"^evidence-observation:[0-9a-f]{64}$")
_CLUSTER_ID_PATTERN = re.compile(r"^evidence-cluster:[0-9a-f]{64}$")
_RELATIONSHIP_ID_PATTERN = re.compile(r"^evidence-relationship:[0-9a-f]{64}$")
_DNS_LABEL_PATTERN = re.compile(r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)$")
_UNSAFE_HOST_SUFFIXES = (
    ".corp",
    ".example",
    ".home",
    ".internal",
    ".invalid",
    ".lan",
    ".local",
    ".localhost",
    ".test",
)
_CREDENTIAL_KEYS = frozenset(
    {
        "accesskey",
        "accesstoken",
        "apikey",
        "authorization",
        "clientsecret",
        "connectionstring",
        "cookie",
        "databaseurl",
        "password",
        "passwd",
        "privatekey",
        "refreshtoken",
        "secret",
        "sessiontoken",
    }
)
_PROFILE_PARSERS = (
    ("facebook", parse_facebook_profile_url),
    ("instagram", parse_instagram_profile_url),
    ("threads", parse_threads_profile_url),
    ("tiktok", parse_tiktok_profile_url),
    ("x", parse_x_profile_url),
)

_OBSERVATION_FIELDS = frozenset(
    {
        "schema_version",
        "case_id",
        "claim_type",
        "claim_value",
        "source_id",
        "source_version",
        "source_record_id",
        "outcome",
        "native_outcome",
        "native_status",
        "citations",
        "retrieved_at",
        "originating_query",
        "originating_query_fingerprint",
        "source_snapshot_sha256",
        "source_snapshot_ref",
        "confidence",
        "observation_id",
        "cluster_id",
        "canonical_profile_identity",
    }
)
_RELATIONSHIP_FIELDS = frozenset(
    {
        "schema_version",
        "case_id",
        "left_observation_id",
        "left_case_id",
        "right_observation_id",
        "right_case_id",
        "relationship_kind",
        "basis",
        "relationship_id",
    }
)


class CorrelationContractError(ValueError):
    """Raised when evidence-correlation data violates the contract."""


def _bounded_text(
    value: Any,
    field_name: str,
    *,
    max_chars: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise CorrelationContractError(f"{field_name} must be text")
    candidate = unicodedata.normalize("NFKC", value).strip()
    if not candidate and not allow_empty:
        raise CorrelationContractError(f"{field_name} is required")
    if len(candidate) > max_chars:
        raise CorrelationContractError(f"{field_name} is too large")
    if any(
        ord(character) < 32 and character not in "\n\r\t" for character in candidate
    ):
        raise CorrelationContractError(
            f"{field_name} contains prohibited control characters"
        )
    return candidate


def _normalized_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _reject_credential_fields(value: Any, path: str = "payload") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if _normalized_key(key) in _CREDENTIAL_KEYS:
                raise CorrelationContractError(
                    f"{path} contains prohibited credential field {key!r}"
                )
            _reject_credential_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_credential_fields(item, f"{path}[{index}]")


def _reject_unexpected_fields(
    payload: Any, allowed: frozenset[str], record_name: str
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise CorrelationContractError(f"{record_name} must be an object")
    _reject_credential_fields(payload)
    unexpected = set(payload) - allowed
    if unexpected:
        raise CorrelationContractError(f"{record_name} contains unsupported fields")
    if payload.get("schema_version") != EVIDENCE_CORRELATION_SCHEMA_VERSION:
        raise CorrelationContractError("Unsupported evidence-correlation schema")
    return payload


def _identifier(value: Any, field_name: str) -> str:
    candidate = _bounded_text(value, field_name, max_chars=100).casefold()
    if not _IDENTIFIER_PATTERN.fullmatch(candidate):
        raise CorrelationContractError(f"Invalid {field_name}")
    return candidate


def _claim_type(value: Any) -> str:
    candidate = _bounded_text(value, "claim_type", max_chars=100).casefold()
    if not _CLAIM_TYPE_PATTERN.fullmatch(candidate):
        raise CorrelationContractError("Invalid claim_type")
    return candidate


def _case_id(value: Any) -> str:
    candidate = _bounded_text(value, "case_id", max_chars=128)
    if not _CASE_ID_PATTERN.fullmatch(candidate):
        raise CorrelationContractError("Invalid case_id")
    return candidate


def _sha256(value: Any, field_name: str) -> str:
    candidate = _bounded_text(value, field_name, max_chars=71).casefold()
    if not _SHA256_PATTERN.fullmatch(candidate):
        raise CorrelationContractError(
            f"{field_name} must be sha256:<64 lowercase hex>"
        )
    return candidate


def _timestamp(value: Any, field_name: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        candidate = _bounded_text(value, field_name, max_chars=64)
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CorrelationContractError(f"Invalid {field_name}") from exc
    else:
        raise CorrelationContractError(f"{field_name} must be an ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CorrelationContractError(f"{field_name} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _credential_query_key(value: str) -> bool:
    normalized = _normalized_key(value)
    return normalized in _CREDENTIAL_KEYS or any(
        normalized.endswith(key) for key in _CREDENTIAL_KEYS
    )


def _require_public_hostname(hostname: str, field_name: str) -> None:
    if hostname == "localhost" or hostname.endswith(_UNSAFE_HOST_SUFFIXES):
        raise CorrelationContractError(f"{field_name} must be a public HTTPS URL")
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address is not None:
        if not address.is_global:
            raise CorrelationContractError(f"{field_name} must be a public HTTPS URL")
        return
    if "." not in hostname or any(
        not _DNS_LABEL_PATTERN.fullmatch(label) for label in hostname.split(".")
    ):
        raise CorrelationContractError(f"{field_name} must be a public HTTPS URL")


def _public_https_url(value: Any, field_name: str) -> str:
    candidate = _bounded_text(value, field_name, max_chars=2_000)
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
        query = parse_qsl(parsed.query, keep_blank_values=True)
    except ValueError as exc:
        raise CorrelationContractError(f"Invalid {field_name}") from exc
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if (
        parsed.scheme.casefold() != "https"
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
    ):
        raise CorrelationContractError(f"{field_name} must be a public HTTPS URL")
    _require_public_hostname(hostname, field_name)
    if any(_credential_query_key(key) for key, _ in query):
        raise CorrelationContractError(f"{field_name} must not contain credentials")
    return f"https:{candidate.split(':', 1)[1]}"


def _source_snapshot_ref(value: Any) -> str:
    candidate = _bounded_text(value, "source_snapshot_ref", max_chars=2_000)
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as exc:
        raise CorrelationContractError("Invalid source_snapshot_ref") from exc
    scheme = parsed.scheme.casefold()
    if (
        parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or port not in {None, 443}
    ):
        raise CorrelationContractError(
            "source_snapshot_ref must be an immutable credential-free locator"
        )
    if scheme == "https":
        return _public_https_url(candidate, "source_snapshot_ref")
    if scheme == "evidence" and parsed.netloc and parsed.path.strip("/"):
        return f"evidence:{candidate.split(':', 1)[1]}"
    if scheme == "urn" and parsed.path:
        return f"urn:{candidate.split(':', 1)[1]}"
    raise CorrelationContractError("Unsupported source_snapshot_ref locator")


def _stable_id(prefix: str, value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"{prefix}:{hashlib.sha256(encoded).hexdigest()}"


def _check_document_size(value: Dict[str, Any], record_name: str) -> None:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CorrelationContractError(
            f"{record_name} contains a non-JSON value"
        ) from exc
    if len(encoded) > MAX_ENVELOPE_BYTES:
        raise CorrelationContractError(f"{record_name} is too large")


def canonical_profile_identity(value: Any) -> Optional[Dict[str, str]]:
    """Return the canonical platform, handle, and URL for a profile URL."""
    if not isinstance(value, str):
        return None
    candidate = unicodedata.normalize("NFKC", value).strip()
    if not candidate or len(candidate) > 2_000:
        return None
    for platform, parser in _PROFILE_PARSERS:
        reference = parser(candidate)
        if reference is not None:
            return {
                "platform": platform,
                "handle": reference.handle.casefold(),
                "canonical_url": reference.canonical_url,
            }
    return None


def _normalize_citations(value: Any) -> list[Dict[str, str]]:
    if not isinstance(value, list):
        raise CorrelationContractError("citations must be a list")
    if len(value) > MAX_CITATIONS:
        raise CorrelationContractError("citations contains too many items")
    citations: Dict[str, Dict[str, str]] = {}
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != {"url", "title"}:
            raise CorrelationContractError("citation must contain only url and title")
        url = _public_https_url(item.get("url"), f"citations[{index}].url")
        title = _bounded_text(
            item.get("title"),
            f"citations[{index}].title",
            max_chars=300,
            allow_empty=True,
        )
        existing = citations.get(url)
        if existing is not None and existing["title"] != title:
            raise CorrelationContractError(
                "Duplicate citation URLs must have the same title"
            )
        citations[url] = {"url": url, "title": title}
    return [citations[url] for url in sorted(citations)]


def _normalize_confidence(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"scope", "score", "basis"}:
        raise CorrelationContractError(
            "confidence must contain only scope, score, and basis"
        )
    scope = _identifier(value.get("scope"), "confidence.scope")
    if scope not in EVIDENCE_CONFIDENCE_SCOPES:
        raise CorrelationContractError(
            "confidence.scope must be correlation or review_priority"
        )
    score = value.get("score")
    if isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 100:
        raise CorrelationContractError("confidence.score must be between 0 and 100")
    raw_basis = value.get("basis")
    if (
        not isinstance(raw_basis, list)
        or not raw_basis
        or len(raw_basis) > MAX_CONFIDENCE_BASIS_ITEMS
    ):
        raise CorrelationContractError(
            "confidence.basis must be a non-empty bounded list"
        )
    basis = sorted(
        {
            _bounded_text(
                item,
                "confidence.basis item",
                max_chars=500,
            )
            for item in raw_basis
        }
    )
    return {"scope": scope, "score": score, "basis": basis}


def _claim_cluster_material(
    case_id: str, claim_type: str, claim_value: str
) -> Dict[str, Any]:
    profile_identity = canonical_profile_identity(claim_value)
    if profile_identity is not None:
        return {
            "case_id": case_id,
            "claim_identity": {
                "claim_type": claim_type,
                "profile": profile_identity,
            },
        }
    normalized_value = " ".join(claim_value.split()).casefold()
    return {
        "case_id": case_id,
        "claim_identity": f"{claim_type}:{normalized_value}",
    }


def _observation_stable_material(normalized: Dict[str, Any]) -> Dict[str, Any]:
    """Exclude retrieval/query context and prioritization from rerun identity."""
    return {
        "case_id": normalized["case_id"],
        "claim_identity": _claim_cluster_material(
            normalized["case_id"],
            normalized["claim_type"],
            normalized["claim_value"],
        )["claim_identity"],
        "source_id": normalized["source_id"],
        "source_version": normalized["source_version"],
        "source_record_id": normalized["source_record_id"],
        "outcome": normalized["outcome"],
        "native_outcome": normalized["native_outcome"],
        "native_status": normalized["native_status"],
        "source_snapshot_sha256": normalized["source_snapshot_sha256"],
    }


def normalize_evidence_observation(payload: Any) -> Dict[str, Any]:
    """Validate and normalize one immutable, case-scoped source observation."""
    record = _reject_unexpected_fields(
        payload, _OBSERVATION_FIELDS, "Evidence observation"
    )
    case_id = _case_id(record.get("case_id"))
    claim_type = _claim_type(record.get("claim_type"))
    claim_value = _bounded_text(
        record.get("claim_value"), "claim_value", max_chars=2_000
    )
    profile_identity = canonical_profile_identity(claim_value)
    if claim_type == "profile" and profile_identity is None:
        raise CorrelationContractError(
            "profile claim_value must be a supported public profile URL"
        )
    source_id = _identifier(record.get("source_id"), "source_id")
    source_version = _bounded_text(
        record.get("source_version"), "source_version", max_chars=200
    )
    source_record_id = _bounded_text(
        record.get("source_record_id"), "source_record_id", max_chars=500
    )
    outcome = _identifier(record.get("outcome"), "outcome")
    if outcome not in EVIDENCE_CORRELATION_OUTCOMES:
        raise CorrelationContractError("Unsupported evidence outcome")
    normalized: Dict[str, Any] = {
        "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
        "case_id": case_id,
        "claim_type": claim_type,
        "claim_value": claim_value,
        "source_id": source_id,
        "source_version": source_version,
        "source_record_id": source_record_id,
        "outcome": outcome,
        "native_outcome": _bounded_text(
            record.get("native_outcome"), "native_outcome", max_chars=200
        ),
        "native_status": _bounded_text(
            record.get("native_status"), "native_status", max_chars=200
        ),
        "citations": _normalize_citations(record.get("citations")),
        "retrieved_at": _timestamp(record.get("retrieved_at"), "retrieved_at"),
        "originating_query": _bounded_text(
            record.get("originating_query"),
            "originating_query",
            max_chars=1_000,
        ),
        "originating_query_fingerprint": _sha256(
            record.get("originating_query_fingerprint"),
            "originating_query_fingerprint",
        ),
        "source_snapshot_sha256": _sha256(
            record.get("source_snapshot_sha256"),
            "source_snapshot_sha256",
        ),
        "source_snapshot_ref": _source_snapshot_ref(record.get("source_snapshot_ref")),
    }
    if "confidence" in record:
        normalized["confidence"] = _normalize_confidence(record["confidence"])

    cluster_id = _stable_id(
        "evidence-cluster",
        _claim_cluster_material(case_id, claim_type, claim_value),
    )
    observation_id = _stable_id(
        "evidence-observation", _observation_stable_material(normalized)
    )
    for field_name, derived, pattern in (
        ("observation_id", observation_id, _OBSERVATION_ID_PATTERN),
        ("cluster_id", cluster_id, _CLUSTER_ID_PATTERN),
    ):
        supplied = record.get(field_name)
        if supplied is not None:
            supplied = _bounded_text(supplied, field_name, max_chars=96).casefold()
            if not pattern.fullmatch(supplied) or supplied != derived:
                raise CorrelationContractError(f"{field_name} is inconsistent")
    if "canonical_profile_identity" in record and (
        record["canonical_profile_identity"] != profile_identity
    ):
        raise CorrelationContractError("canonical_profile_identity is inconsistent")

    output = {
        "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
        "observation_id": observation_id,
        "cluster_id": cluster_id,
        "case_id": case_id,
        "claim_type": claim_type,
        "claim_value": claim_value,
        "canonical_profile_identity": profile_identity,
        "source_id": normalized["source_id"],
        "source_version": normalized["source_version"],
        "source_record_id": normalized["source_record_id"],
        "outcome": normalized["outcome"],
        "native_outcome": normalized["native_outcome"],
        "native_status": normalized["native_status"],
        "citations": normalized["citations"],
        "retrieved_at": normalized["retrieved_at"],
        "originating_query": normalized["originating_query"],
        "originating_query_fingerprint": normalized["originating_query_fingerprint"],
        "source_snapshot_sha256": normalized["source_snapshot_sha256"],
        "source_snapshot_ref": normalized["source_snapshot_ref"],
    }
    if "confidence" in normalized:
        output["confidence"] = normalized["confidence"]
    _check_document_size(output, "Evidence observation")
    return output


def evidence_observation_id(payload: Any) -> str:
    """Return the deterministic identity of one normalized observation."""
    return normalize_evidence_observation(payload)["observation_id"]


def evidence_cluster_id(payload: Any) -> str:
    """Return the case-scoped semantic cluster identity for an observation."""
    return normalize_evidence_observation(payload)["cluster_id"]


def _observation_reference(value: Any, field_name: str) -> str:
    candidate = _bounded_text(value, field_name, max_chars=96).casefold()
    if not _OBSERVATION_ID_PATTERN.fullmatch(candidate):
        raise CorrelationContractError(f"Invalid {field_name}")
    return candidate


def normalize_evidence_relationship(payload: Any) -> Dict[str, Any]:
    """Validate a symmetric relationship between two case-scoped observations."""
    record = _reject_unexpected_fields(
        payload, _RELATIONSHIP_FIELDS, "Evidence relationship"
    )
    case_id = _case_id(record.get("case_id"))
    left_case_id = _case_id(record.get("left_case_id"))
    right_case_id = _case_id(record.get("right_case_id"))
    if left_case_id != case_id or right_case_id != case_id:
        raise CorrelationContractError(
            "Evidence relationship endpoints must belong to case_id"
        )
    relationship_kind = _identifier(
        record.get("relationship_kind"), "relationship_kind"
    )
    if relationship_kind not in EVIDENCE_RELATIONSHIP_KINDS:
        raise CorrelationContractError("Unsupported evidence relationship")
    endpoints = sorted(
        (
            _observation_reference(
                record.get("left_observation_id"), "left_observation_id"
            ),
            _observation_reference(
                record.get("right_observation_id"), "right_observation_id"
            ),
        )
    )
    if endpoints[0] == endpoints[1]:
        raise CorrelationContractError("Evidence relationships cannot self-reference")
    basis = _bounded_text(record.get("basis"), "basis", max_chars=1_000)
    relationship_id = _stable_id(
        "evidence-relationship",
        {
            "case_id": case_id,
            "endpoints": endpoints,
            "relationship_kind": relationship_kind,
        },
    )
    supplied = record.get("relationship_id")
    if supplied is not None:
        supplied = _bounded_text(supplied, "relationship_id", max_chars=97).casefold()
        if (
            not _RELATIONSHIP_ID_PATTERN.fullmatch(supplied)
            or supplied != relationship_id
        ):
            raise CorrelationContractError("relationship_id is inconsistent")
    output = {
        "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
        "relationship_id": relationship_id,
        "case_id": case_id,
        "left_observation_id": endpoints[0],
        "left_case_id": case_id,
        "right_observation_id": endpoints[1],
        "right_case_id": case_id,
        "relationship_kind": relationship_kind,
        "basis": basis,
    }
    _check_document_size(output, "Evidence relationship")
    return output


def evidence_relationship_id(payload: Any) -> str:
    """Return the deterministic identity of a symmetric relationship."""
    return normalize_evidence_relationship(payload)["relationship_id"]
