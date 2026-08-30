"""Fail-closed contracts for case-scoped evidence from an external datamart."""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.parse import urlsplit


EXTERNAL_EVIDENCE_SCHEMA_VERSION = 1
MAX_PREVIEW_CHARS = 12_000
MAX_DOCUMENT_BYTES = 64 * 1024
MAX_STRING_CHARS = 4_000
MAX_COLLECTION_ITEMS = 128
MAX_DOCUMENT_DEPTH = 6

SOURCE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,99}$")
TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9._:-]{0,99}$")
CLASSIFICATION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
ALLOWED_LOCATOR_SCHEMES = {"datamart", "evidence", "https", "urn"}

_SECRET_KEYS = {
    "access_key",
    "access_token",
    "api_key",
    "authorization",
    "client_secret",
    "connection_string",
    "cookie",
    "database_url",
    "password",
    "passwd",
    "private_key",
    "refresh_token",
    "secret",
    "session_token",
}
_SECRET_KEY_TOKEN_SEQUENCES = tuple(
    tuple(secret_key.split("_")) for secret_key in _SECRET_KEYS
)
_COMPACT_SECRET_KEYS = {
    "".join(tokens) for tokens in _SECRET_KEY_TOKEN_SEQUENCES
}


class ExternalEvidenceValidationError(ValueError):
    """Raised when an external result violates the storage contract."""


def stable_fingerprint(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalize_source_id(value: Any) -> str:
    candidate = str(value or "").strip().casefold()
    if not SOURCE_ID_PATTERN.fullmatch(candidate):
        raise ExternalEvidenceValidationError("Invalid external source identifier")
    return candidate


def normalize_classification(value: Any, field_name: str) -> str:
    classification = bounded_text(value, field_name, max_chars=64)
    if not CLASSIFICATION_PATTERN.fullmatch(classification):
        raise ExternalEvidenceValidationError(f"Invalid {field_name}")
    return classification


def bounded_text(
    value: Any,
    field_name: str,
    *,
    max_chars: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise ExternalEvidenceValidationError(f"{field_name} must be text")
    candidate = value.strip()
    if not candidate and not allow_empty:
        raise ExternalEvidenceValidationError(f"{field_name} is required")
    if len(candidate) > max_chars:
        raise ExternalEvidenceValidationError(f"{field_name} is too large")
    if any(ord(character) < 32 and character not in "\n\r\t" for character in candidate):
        raise ExternalEvidenceValidationError(
            f"{field_name} contains prohibited control characters"
        )
    return candidate


def parse_external_timestamp(value: Any, field_name: str) -> datetime:
    candidate = bounded_text(value, field_name, max_chars=64)
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError as error:
        raise ExternalEvidenceValidationError(
            f"{field_name} must be an ISO-8601 timestamp"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidenceValidationError(
            f"{field_name} must include a timezone"
        )
    return parsed.astimezone(timezone.utc)


def _secret_key(key: str) -> bool:
    separated = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", key)
    separated = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", separated)
    normalized = re.sub(r"[^a-z0-9]+", "_", separated.casefold()).strip("_")
    tokens = tuple(token for token in normalized.split("_") if token)
    for secret_tokens in _SECRET_KEY_TOKEN_SEQUENCES:
        width = len(secret_tokens)
        if any(
            tokens[index : index + width] == secret_tokens
            for index in range(len(tokens) - width + 1)
        ):
            return True
    compact = "".join(tokens)
    return any(
        compact == secret
        or compact.startswith(secret)
        or compact.endswith(secret)
        for secret in _COMPACT_SECRET_KEYS
    )


def _bounded_json_value(value: Any, path: str, depth: int) -> Any:
    if depth > MAX_DOCUMENT_DEPTH:
        raise ExternalEvidenceValidationError(f"{path} exceeds maximum depth")
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ExternalEvidenceValidationError(f"{path} contains a non-finite number")
        return value
    if isinstance(value, str):
        return bounded_text(
            value,
            path,
            max_chars=MAX_STRING_CHARS,
            allow_empty=True,
        )
    if isinstance(value, list):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise ExternalEvidenceValidationError(f"{path} contains too many items")
        return [
            _bounded_json_value(item, f"{path}[{index}]", depth + 1)
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise ExternalEvidenceValidationError(f"{path} contains too many fields")
        normalized: Dict[str, Any] = {}
        for raw_key, item in value.items():
            key = bounded_text(
                raw_key,
                f"{path} key",
                max_chars=100,
            )
            if _secret_key(key):
                raise ExternalEvidenceValidationError(
                    f"{path} contains prohibited credential field {key!r}"
                )
            normalized[key] = _bounded_json_value(
                item,
                f"{path}.{key}",
                depth + 1,
            )
        return normalized
    raise ExternalEvidenceValidationError(f"{path} contains a non-JSON value")


def normalize_bounded_document(value: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ExternalEvidenceValidationError(f"{field_name} must be an object")
    normalized = _bounded_json_value(value, field_name, 0)
    serialized = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(serialized) > MAX_DOCUMENT_BYTES:
        raise ExternalEvidenceValidationError(f"{field_name} is too large")
    return normalized


def normalize_locator(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"uri"}:
        raise ExternalEvidenceValidationError(
            "locator must contain only a stable uri"
        )
    uri = bounded_text(value.get("uri"), "locator.uri", max_chars=2_000)
    parsed = urlsplit(uri)
    if parsed.scheme.casefold() not in ALLOWED_LOCATOR_SCHEMES:
        raise ExternalEvidenceValidationError("Unsupported evidence locator scheme")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ExternalEvidenceValidationError(
            "Evidence locators must not contain credentials, query strings, or fragments"
        )
    if parsed.scheme.casefold() == "https" and not parsed.hostname:
        raise ExternalEvidenceValidationError("HTTPS evidence locator requires a host")
    if parsed.scheme.casefold() in {"datamart", "evidence"} and not parsed.netloc:
        raise ExternalEvidenceValidationError(
            "Datamart evidence locator requires an authority"
        )
    return {"uri": uri}


def validate_locator_authority(locator: Dict[str, str], authority: Any) -> None:
    """Bind governed locator namespaces to the registered source authority."""
    expected = bounded_text(authority, "source.authority", max_chars=200)
    parsed = urlsplit(locator["uri"])
    if (
        parsed.scheme.casefold() in {"datamart", "evidence"}
        and parsed.netloc.casefold() != expected.casefold()
    ):
        raise ExternalEvidenceValidationError(
            "Evidence locator authority does not match the registered source"
        )


def normalize_handling(value: Any) -> Dict[str, Any]:
    handling = normalize_bounded_document(value, "handling")
    classification = normalize_classification(
        handling.get("classification"), "handling.classification"
    )
    authority = bounded_text(
        handling.get("authority"),
        "handling.authority",
        max_chars=200,
    )
    policy_tags = handling.get("policy_tags", [])
    if not isinstance(policy_tags, list) or len(policy_tags) > 32:
        raise ExternalEvidenceValidationError("handling.policy_tags must be a bounded list")
    handling.update(
        classification=classification,
        authority=authority,
        policy_tags=[
            bounded_text(tag, "handling.policy_tags item", max_chars=64)
            for tag in policy_tags
        ],
    )
    return handling


def normalize_policy_context(
    value: Any,
    *,
    requested_by: str,
    purpose: str,
) -> Dict[str, Any]:
    context = normalize_bounded_document(value, "policy_context")
    required = {
        "principal_id": requested_by,
        "purpose": purpose,
    }
    for key, expected in required.items():
        actual = bounded_text(context.get(key), f"policy_context.{key}", max_chars=500)
        if actual != expected:
            raise ExternalEvidenceValidationError(
                f"policy_context.{key} does not match the request"
            )
    context["authority"] = bounded_text(
        context.get("authority"),
        "policy_context.authority",
        max_chars=200,
    )
    context["classification_ceiling"] = normalize_classification(
        context.get("classification_ceiling"),
        "policy_context.classification_ceiling",
    )
    return context


def normalize_external_evidence(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ExternalEvidenceValidationError("External evidence must be an object")
    allowed_fields = {
        "schema_version",
        "source_id",
        "source_record_id",
        "source_version",
        "record_type",
        "content_hash",
        "observed_at",
        "validity",
        "handling",
        "locator",
        "attributes",
        "preview",
    }
    unexpected_fields = set(payload) - allowed_fields
    if unexpected_fields:
        raise ExternalEvidenceValidationError(
            "External evidence contains unsupported fields"
        )
    if payload.get("schema_version") != EXTERNAL_EVIDENCE_SCHEMA_VERSION:
        raise ExternalEvidenceValidationError("Unsupported external evidence schema")

    source_id = normalize_source_id(payload.get("source_id"))
    record_type = bounded_text(payload.get("record_type"), "record_type", max_chars=100)
    if not TYPE_PATTERN.fullmatch(record_type):
        raise ExternalEvidenceValidationError("Invalid external record type")
    content_hash = bounded_text(payload.get("content_hash"), "content_hash", max_chars=71).casefold()
    if not SHA256_PATTERN.fullmatch(content_hash):
        raise ExternalEvidenceValidationError("content_hash must be sha256:<64 lowercase hex>")

    validity = payload["validity"] if "validity" in payload else {}
    if not isinstance(validity, dict) or set(validity) - {"from", "to"}:
        raise ExternalEvidenceValidationError("validity may contain only from and to")
    valid_from = (
        parse_external_timestamp(validity["from"], "validity.from")
        if validity.get("from") is not None
        else None
    )
    valid_to = (
        parse_external_timestamp(validity["to"], "validity.to")
        if validity.get("to") is not None
        else None
    )
    if valid_from and valid_to and valid_from > valid_to:
        raise ExternalEvidenceValidationError("validity.from must not follow validity.to")

    return {
        "schema_version": EXTERNAL_EVIDENCE_SCHEMA_VERSION,
        "source_id": source_id,
        "source_record_id": bounded_text(
            payload.get("source_record_id"),
            "source_record_id",
            max_chars=500,
        ),
        "source_version": bounded_text(
            payload.get("source_version"),
            "source_version",
            max_chars=200,
        ),
        "record_type": record_type,
        "content_hash": content_hash,
        "observed_at": parse_external_timestamp(payload.get("observed_at"), "observed_at"),
        "valid_from": valid_from,
        "valid_to": valid_to,
        "handling": normalize_handling(payload.get("handling")),
        "locator": normalize_locator(payload.get("locator")),
        "attributes": normalize_bounded_document(
            payload["attributes"] if "attributes" in payload else {},
            "attributes",
        ),
        "preview": bounded_text(
            payload.get("preview", ""),
            "preview",
            max_chars=MAX_PREVIEW_CHARS,
            allow_empty=True,
        ),
    }
