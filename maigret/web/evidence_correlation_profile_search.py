"""Adapt immutable profile-search audits to the evidence-correlation contract."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import hashlib
import hmac
import json
import re
from typing import Any, Dict

from maigret.web.evidence_correlation_contract import (
    EVIDENCE_CORRELATION_SCHEMA_VERSION,
    canonical_profile_identity,
    normalize_evidence_observation,
)
from maigret.web.profile_search_contract import MAX_PROFILE_SEARCH_RESULTS
from maigret.web.profile_search_planner import MAX_PROFILE_SEARCH_QUERIES


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_FINGERPRINT_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")

_ERROR_OUTCOMES = {
    "private": "private",
    "blocked": "blocked",
    "circuit_open": "blocked",
    "rate_limited": "rate_limited",
    "invalid_response": "parser_error",
    "malformed_response": "parser_error",
    "provider_error": "provider_error",
    "credential_rejected": "provider_error",
    "oversized_response": "provider_error",
}


def _document_sha256(document: Dict[str, Any]) -> str:
    try:
        encoded = json.dumps(
            document,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("Profile-search audit document is not canonical JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def _object(value: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Profile-search audit {field_name} must be an object")
    return value


def _collection(value: Any, field_name: str, maximum: int) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"Profile-search audit {field_name} must be a list")
    if len(value) > maximum:
        raise ValueError(f"Profile-search audit {field_name} exceeds its limit")
    return value


def _integer(value: Any, field_name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Profile-search audit {field_name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"Profile-search audit {field_name} is out of bounds")
    return value


def _text(value: Any, field_name: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Profile-search audit {field_name} must be text")
    candidate = value.strip()
    if not candidate or len(candidate) > maximum:
        raise ValueError(f"Profile-search audit {field_name} is invalid")
    return candidate


def _fingerprint(value: Any, field_name: str) -> str:
    candidate = _text(value, field_name, 71).casefold()
    if not _FINGERPRINT_PATTERN.fullmatch(candidate):
        raise ValueError(f"Profile-search audit {field_name} is invalid")
    return candidate


def _record_id(kind: str, material: Any) -> str:
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"profile-search-{kind}:{hashlib.sha256(encoded).hexdigest()}"


def _snapshot_sha256(material: Any) -> str:
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _snapshot_ref(audit_id: str, source_record_id: str, snapshot_sha256: str) -> str:
    return (
        "evidence://openledger/profile-search-audit/"
        f"{audit_id}/records/{source_record_id}/snapshots/"
        f"{snapshot_sha256.removeprefix('sha256:')}"
    )


def _base_observation(
    *,
    case_id: str,
    claim_type: str,
    claim_value: str,
    source_id: str,
    source_version: str,
    source_record_id: str,
    outcome: str,
    native_outcome: str,
    native_status: str,
    citations: list[Dict[str, str]],
    retrieved_at: str,
    query: Dict[str, Any],
    snapshot_sha256: str,
    snapshot_ref: str,
) -> Dict[str, Any]:
    return normalize_evidence_observation(
        {
            "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
            "case_id": case_id,
            "claim_type": claim_type,
            "claim_value": claim_value,
            "source_id": source_id,
            "source_version": source_version,
            "source_record_id": source_record_id,
            "outcome": outcome,
            "native_outcome": native_outcome,
            "native_status": native_status,
            "citations": citations,
            "retrieved_at": retrieved_at,
            "originating_query": query["query_text"],
            "originating_query_fingerprint": query["query_fingerprint"],
            "source_snapshot_sha256": snapshot_sha256,
            "source_snapshot_ref": snapshot_ref,
        }
    )


def _validated_queries(document: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    queries = _collection(
        document.get("queries"), "queries", MAX_PROFILE_SEARCH_QUERIES
    )
    planned = _integer(
        document.get("planned_query_count"),
        "planned_query_count",
        0,
        MAX_PROFILE_SEARCH_QUERIES,
    )
    if len(queries) != planned:
        raise ValueError("Profile-search audit query count is inconsistent")
    indexed: Dict[str, Dict[str, Any]] = {}
    for raw_query in queries:
        query = _object(raw_query, "query")
        query_id = _text(query.get("query_id"), "query_id", 100).casefold()
        if query_id in indexed:
            raise ValueError("Profile-search audit contains duplicate query IDs")
        query_text = _text(query.get("query_text"), "query_text", 500)
        query_fingerprint = _fingerprint(
            query.get("query_fingerprint"), "query_fingerprint"
        )
        max_results = _integer(
            query.get("max_results"),
            "query.max_results",
            1,
            MAX_PROFILE_SEARCH_RESULTS,
        )
        indexed[query_id] = {
            **query,
            "query_id": query_id,
            "query_text": query_text,
            "query_fingerprint": query_fingerprint,
            "max_results": max_results,
        }
    return indexed


def profile_search_audit_observations(
    *,
    case_id: str,
    audit_id: str,
    document_sha256: str,
    document: Dict[str, Any],
    retrieved_at: str,
) -> tuple[Dict[str, Any], ...]:
    """Return deterministic observations from one integrity-checked audit.

    A successful empty query records only that the provider returned no usable
    profile result. It does not assert that an account does not exist.
    """
    document = _object(document, "document")
    expected_sha256 = str(document_sha256 or "").strip().casefold()
    if not _SHA256_PATTERN.fullmatch(expected_sha256):
        raise ValueError("Profile-search audit sha256 is invalid")
    actual_sha256 = _document_sha256(document)
    if not hmac.compare_digest(actual_sha256, expected_sha256):
        raise ValueError("Profile-search audit integrity hash does not match")

    orchestration_version = _integer(
        document.get("orchestration_version"),
        "orchestration_version",
        1,
        1_000_000,
    )
    queries = _validated_queries(document)
    status = str(document.get("status") or "").strip().casefold()
    if status not in {"completed", "partial", "failed", "stopped"}:
        raise ValueError("Profile-search audit status is invalid")
    stopped = document.get("stopped")
    if not isinstance(stopped, bool) or stopped != (status == "stopped"):
        raise ValueError("Profile-search audit stop status is inconsistent")
    runs = _collection(document.get("runs"), "runs", MAX_PROFILE_SEARCH_QUERIES)
    executed = _integer(
        document.get("executed_query_count"),
        "executed_query_count",
        0,
        len(queries),
    )
    if len(runs) != executed:
        raise ValueError("Profile-search audit run count is inconsistent")
    skipped = _integer(
        document.get("skipped_query_count"),
        "skipped_query_count",
        0,
        len(queries),
    )
    if skipped != len(queries) - executed:
        raise ValueError("Profile-search audit skipped query count is inconsistent")
    candidates = _collection(
        document.get("candidates"),
        "candidates",
        len(queries) * MAX_PROFILE_SEARCH_RESULTS,
    )
    candidate_count = _integer(
        document.get("candidate_count"),
        "candidate_count",
        0,
        len(queries) * MAX_PROFILE_SEARCH_RESULTS,
    )
    if len(candidates) != candidate_count:
        raise ValueError("Profile-search audit candidate count is inconsistent")

    source_version = str(orchestration_version)
    observations = []
    seen_run_ids = set()
    error_count = 0

    for raw_run in runs:
        run = _object(raw_run, "run")
        run_query = _object(run.get("query"), "run.query")
        query_id = _text(run_query.get("query_id"), "run.query_id", 100).casefold()
        if query_id in seen_run_ids:
            raise ValueError("Profile-search audit contains duplicate run IDs")
        seen_run_ids.add(query_id)
        query = queries.get(query_id)
        if query is None or run_query != query:
            raise ValueError("Profile-search audit run does not match its query")

        provenance = run.get("provenance")
        error = run.get("error")
        evidence = _collection(
            run.get("evidence"), "run.evidence", query["max_results"]
        )
        if error is not None:
            error_count += 1
            if provenance is not None or evidence:
                raise ValueError("Failed profile-search run has conflicting evidence")
            error = _object(error, "run.error")
            if (
                _text(error.get("query_id"), "error.query_id", 100).casefold()
                != query_id
            ):
                raise ValueError("Profile-search error does not match its query")
            provider = _text(error.get("provider"), "error.provider", 100).casefold()
            code = _text(error.get("code"), "error.code", 64).casefold()
            outcome = _ERROR_OUTCOMES.get(code, "indeterminate")
            occurred_at = _text(error.get("occurred_at"), "error.occurred_at", 64)
            http_status = error.get("http_status")
            native_status = (
                f"{code}:http_{http_status}"
                if isinstance(http_status, int) and not isinstance(http_status, bool)
                else code
            )
            source_record_id = _record_id(
                "query", {"provider": provider, "query_id": query_id}
            )
            snapshot_sha256 = _snapshot_sha256(
                {
                    "provider": provider,
                    "error": {
                        key: value
                        for key, value in error.items()
                        if key != "occurred_at"
                    },
                }
            )
            observations.append(
                _base_observation(
                    case_id=case_id,
                    claim_type="profile_search_query",
                    claim_value=query["query_text"],
                    source_id=provider,
                    source_version=source_version,
                    source_record_id=source_record_id,
                    outcome=outcome,
                    native_outcome=code,
                    native_status=native_status,
                    citations=[],
                    retrieved_at=occurred_at,
                    query=query,
                    snapshot_sha256=snapshot_sha256,
                    snapshot_ref=_snapshot_ref(
                        audit_id, source_record_id, snapshot_sha256
                    ),
                )
            )
            continue

        provenance = _object(provenance, "run.provenance")
        provider = _text(
            provenance.get("provider"), "provenance.provider", 100
        ).casefold()
        if (
            _text(provenance.get("query_id"), "provenance.query_id", 100).casefold()
            != query_id
            or _fingerprint(
                provenance.get("query_fingerprint"),
                "provenance.query_fingerprint",
            )
            != query["query_fingerprint"]
        ):
            raise ValueError("Profile-search provenance does not match its query")
        provenance_retrieved_at = _text(
            provenance.get("retrieved_at"), "provenance.retrieved_at", 64
        )
        retained = 0
        for raw_result in evidence:
            result = _object(raw_result, "run evidence result")
            rank = _integer(
                result.get("result_rank"),
                "result_rank",
                1,
                100,
            )
            source_url = _text(result.get("source_url"), "source_url", 2_000)
            profile_identity = canonical_profile_identity(source_url)
            if profile_identity is None:
                continue
            title = result.get("title")
            if not isinstance(title, str) or len(title) > 500:
                raise ValueError("Profile-search audit result title is invalid")
            snippet = result.get("snippet")
            if not isinstance(snippet, str) or len(snippet) > 2_000:
                raise ValueError("Profile-search audit result snippet is invalid")
            retained += 1
            source_record_id = _record_id(
                "result",
                {
                    "provider": provider,
                    "query_id": query_id,
                    "result_rank": rank,
                    "source_url": source_url.casefold(),
                },
            )
            snapshot_sha256 = _snapshot_sha256(
                {
                    "source_url": profile_identity["canonical_url"],
                    "title": title.strip(),
                    "snippet": snippet.strip(),
                }
            )
            observations.append(
                _base_observation(
                    case_id=case_id,
                    claim_type="profile",
                    claim_value=source_url,
                    source_id=provider,
                    source_version=source_version,
                    source_record_id=source_record_id,
                    outcome="observed",
                    native_outcome="returned_profile_result",
                    native_status="public_search_result",
                    citations=[
                        {
                            "url": profile_identity["canonical_url"],
                            "title": title.strip()[:300],
                        }
                    ],
                    retrieved_at=provenance_retrieved_at,
                    query=query,
                    snapshot_sha256=snapshot_sha256,
                    snapshot_ref=_snapshot_ref(
                        audit_id, source_record_id, snapshot_sha256
                    ),
                )
            )
        if retained == 0:
            source_record_id = _record_id(
                "query", {"provider": provider, "query_id": query_id}
            )
            snapshot_sha256 = _snapshot_sha256(
                {"provider": provider, "supported_profile_results": []}
            )
            observations.append(
                _base_observation(
                    case_id=case_id,
                    claim_type="profile_search_query",
                    claim_value=query["query_text"],
                    source_id=provider,
                    source_version=source_version,
                    source_record_id=source_record_id,
                    outcome="absent",
                    native_outcome="no_returned_profile_result",
                    native_status=("successful_search_no_supported_profile_result"),
                    citations=[],
                    retrieved_at=provenance_retrieved_at,
                    query=query,
                    snapshot_sha256=snapshot_sha256,
                    snapshot_ref=_snapshot_ref(
                        audit_id, source_record_id, snapshot_sha256
                    ),
                )
            )

    recorded_errors = _integer(document.get("error_count"), "error_count", 0, len(runs))
    if error_count != recorded_errors:
        raise ValueError("Profile-search audit error count is inconsistent")
    return tuple(sorted(observations, key=lambda item: item["observation_id"]))
