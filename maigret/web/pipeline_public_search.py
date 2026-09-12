"""Bounded exact-match public-web evidence through the existing search backend.

Search snippets are derivative candidate evidence. Finding an email/phone/name
in indexed material does not establish its owner's identity or current control.
The adapter does not fetch result pages, perform subscriber lookup, or infer a
phone country. Errors and cancellation never become a negative account result.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit, urlunsplit

from maigret.web.investigation_input import normalize_email, normalize_phone
from maigret.web.pipeline_contract import (
    PIPELINE_ID,
    canonical_digest,
    stable_id,
    validate_task,
)
from maigret.web.profile_search_backend import (
    ProfileSearchClient,
    ProfileSearchConfig,
    ProfileSearchConfigurationError,
    ProfileSearchRun,
    load_profile_search_config,
)

PUBLIC_SEARCH_ADAPTER_VERSION = "public-exact-match-v1"
MAX_RESULTS = 10


@dataclass(frozen=True)
class PublicExactMatchQuery:
    """Provider-compatible query without mislabelling it as a social platform."""

    query_id: str
    query_text: str
    seed_kind: str
    seed_value: str
    max_results: int = 5
    platform: str = "public_web"

    def __post_init__(self):
        if self.seed_kind not in {"email", "phone", "full_name"}:
            raise ValueError("Unsupported public-search input type.")
        if not self.seed_value or len(self.seed_value) > 500:
            raise ValueError(
                "Public-search input must be between 1 and 500 characters."
            )
        if (
            isinstance(self.max_results, bool)
            or not 1 <= self.max_results <= MAX_RESULTS
        ):
            raise ValueError("Public-search results must be bounded to 1–10.")
        if not re.fullmatch(r"[a-z0-9][a-z0-9._:-]{0,99}", self.query_id):
            raise ValueError("Public-search query ID is invalid.")
        if (
            len(self.query_text) > 500
            or not self.query_text.startswith('"')
            or not self.query_text.endswith('"')
        ):
            raise ValueError("Public search requires a bounded exact phrase.")

    @property
    def fingerprint(self) -> str:
        return canonical_digest(
            {
                "adapter_version": PUBLIC_SEARCH_ADAPTER_VERSION,
                "query_text": self.query_text,
                "seed_kind": self.seed_kind,
                "seed_value": self.seed_value,
                "max_results": self.max_results,
            }
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "query_id": self.query_id,
            "platform": self.platform,
            "query_text": self.query_text,
            "seed_kind": self.seed_kind,
            "seed_value": self.seed_value,
            "max_results": self.max_results,
            "query_fingerprint": self.fingerprint,
        }


def query_for_task(
    task: Mapping[str, Any], *, max_results: int = 5
) -> PublicExactMatchQuery:
    validate_task(task)
    if task["engine_id"] != "public_exact_match":
        raise ValueError("Public-search adapter requires a public_exact_match task.")
    kind, value = task["input_type"], task["input_value"]
    if kind == "email":
        value = normalize_email(value)
    elif kind == "phone":
        value = normalize_phone(value)
        if not value.startswith("+") and not (task.get("execution_context") or {}).get(
            "phone_country"
        ):
            raise ValueError("National phone input requires explicit country context.")
    elif kind == "full_name":
        value = " ".join(str(value).split())
    else:
        raise ValueError(
            "Public-search adapter accepts email, phone or full-name input."
        )
    if any(character in value for character in ('"', "\\")) or any(
        ord(character) < 32 for character in value
    ):
        raise ValueError(
            "Exact-match input contains search syntax; operator correction is required."
        )
    if len(value) > 498:
        raise ValueError("Exact-match input is too long.")
    return PublicExactMatchQuery(
        query_id=stable_id("public", task["task_id"]),
        query_text=f'"{value}"',
        seed_kind=kind,
        seed_value=value,
        max_results=max_results,
    )


def _contains_exact(query: PublicExactMatchQuery, text: str) -> bool:
    if query.seed_kind == "phone":
        # Formatting may differ, but adjacent digits must not extend the number.
        digits = re.sub(r"\D", "", query.seed_value)
        pattern = (
            r"(?<!\d)" + r"[\s().-]*".join(re.escape(c) for c in digits) + r"(?!\d)"
        )
        return re.search(pattern, text) is not None
    escaped = re.escape(query.seed_value)
    boundary = r"[A-Z0-9.!#$%&'*+/=?^_`{|}~@-]" if query.seed_kind == "email" else r"\w"
    return (
        re.search(
            r"(?<!" + boundary + ")" + escaped + r"(?!" + boundary + ")",
            text,
            re.IGNORECASE,
        )
        is not None
    )


def _source_identity(url: str) -> str:
    parsed = urlsplit(url)
    # Preserve meaningful query parameters and case-sensitive paths. Fragments
    # never reach the source and cannot create an independent evidence origin.
    return urlunsplit(
        (
            parsed.scheme.casefold(),
            parsed.netloc.casefold(),
            parsed.path,
            parsed.query,
            "",
        )
    )


def _result(
    outcome: str,
    *,
    observations=None,
    diagnostic="",
    request_count=0,
    query=None,
    error=None,
):
    return {
        "pipeline_id": PIPELINE_ID,
        "source_engine": "public_exact_match",
        "adapter_version": PUBLIC_SEARCH_ADAPTER_VERSION,
        "outcome": outcome,
        "observations": list(observations or []),
        "diagnostic": diagnostic,
        "request_count": request_count,
        "query": query.as_dict() if query else None,
        "error": error,
        "retryable": bool((error or {}).get("retryable", False)),
    }


async def collect_public_exact_matches(
    task: Mapping[str, Any],
    *,
    client: Any = None,
    config: ProfileSearchConfig | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Execute one already-authorized source query; return ledger-ready records.

    The worker persists task/attempt identity around this call and appends its
    returned observations transactionally before reporting progress. This adapter
    performs zero retries; worker attempts govern retries and request ceilings.
    """
    validate_task(task)
    if task["route_state"] != "active":
        return _result(
            "not_executed", diagnostic=str(task.get("reason", "Route is not active."))
        )
    if cancelled and cancelled():
        return _result(
            "cancelled", diagnostic="Operator stopped collection before this query."
        )
    if task["request_budget"] < 1:
        return _result(
            "not_executed", diagnostic="Public-search request budget exhausted."
        )
    try:
        if client is None:
            config = config or load_profile_search_config()
            if not config.enabled:
                return _result(
                    "not_executed", diagnostic="No public search provider is enabled."
                )
            client = ProfileSearchClient(config)
        config = config or getattr(client, "config", None)
        query = query_for_task(
            task, max_results=min(getattr(config, "max_results", 5), MAX_RESULTS)
        )
    except (ValueError, ProfileSearchConfigurationError) as exc:
        return _result("not_executed", diagnostic=str(exc))
    try:
        run = await asyncio.wait_for(
            client.search(query), timeout=task["timeout_seconds"]
        )
    except asyncio.CancelledError:
        return _result(
            "cancelled",
            query=query,
            request_count=1,
            diagnostic="Collection interrupted during the provider request.",
        )
    except TimeoutError:
        return _result(
            "timeout",
            query=query,
            request_count=1,
            diagnostic="Public search reached its bounded deadline.",
            error={"code": "timeout", "retryable": True},
        )
    except ProfileSearchConfigurationError:
        return _result(
            "not_executed",
            query=query,
            diagnostic="Public search server configuration is unavailable.",
        )
    except Exception:
        # Do not disclose provider exception strings: they can contain request
        # URLs, credentials or other private server configuration.
        return _result(
            "error",
            query=query,
            request_count=1,
            diagnostic="Public search provider failed.",
            error={"code": "provider_failure", "retryable": False},
        )
    if (
        not isinstance(run, ProfileSearchRun)
        or run.query.fingerprint != query.fingerprint
    ):
        return _result(
            "error",
            query=query,
            request_count=1,
            diagnostic="Search response does not match its query.",
        )
    if run.error:
        code = run.error.code
        outcome = (
            "blocked"
            if code in {"credential_rejected", "rate_limited", "circuit_open"}
            else "error"
        )
        if code == "timeout":
            outcome = "timeout"
        return _result(
            outcome,
            query=query,
            request_count=1,
            diagnostic=run.error.message,
            error=run.error.as_dict(),
        )
    if (
        run.provenance is None
        or run.provenance.query_fingerprint != query.fingerprint
        or run.provenance.query_id != query.query_id
    ):
        return _result(
            "error",
            query=query,
            request_count=1,
            diagnostic="Search response is missing matching provenance.",
        )
    observations = []
    for evidence in run.evidence[: query.max_results]:
        original_url = evidence.source_url
        canonical_url = _source_identity(original_url)
        exact_match = _contains_exact(
            query, " ".join((evidence.title, evidence.snippet, original_url))
        )
        observations.append(
            {
                "source_engine": "public_exact_match",
                "engine_version": PUBLIC_SEARCH_ADAPTER_VERSION,
                "parser_version": PUBLIC_SEARCH_ADAPTER_VERSION,
                "case_id": task.get("case_id", ""),
                "subject_id": task.get("subject_id", ""),
                "request_id": task.get("request_id", ""),
                "task_id": task["task_id"],
                "input_id": task["input_id"],
                "source_record_id": stable_id(
                    "public-result", query.query_id, evidence.result_rank, original_url
                ),
                "status": "candidate" if exact_match else "inconclusive",
                "outcome": "candidate" if exact_match else "inconclusive",
                "source_url": original_url,
                "original_url": original_url,
                "canonical_url": canonical_url,
                "source_origin_family": canonical_digest({"origin_url": canonical_url}),
                "source_dependence": "search_excerpt_of_source",
                "independence": "derivative",
                "observed_at": run.provenance.retrieved_at,
                "retrieved_at": run.provenance.retrieved_at,
                "content_fingerprint": canonical_digest(evidence.as_dict()),
                "provenance": run.provenance.as_dict(),
                "query": query.as_dict(),
                "evidence_type": "indexed_public_excerpt",
                "evidence": evidence.as_dict(),
                "payload": {
                    "title": evidence.title,
                    "snippet": evidence.snippet,
                    "result_rank": evidence.result_rank,
                    "exact_literal_visible": exact_match,
                },
                "eligible_for_assessment": exact_match,
                "identity_status": "unverified",
                "retention": "bounded_source_evidence",
                "probability": None,
                "limitations": [
                    "Indexed excerpt was not independently fetched.",
                    "An exact mention does not establish identity, ownership or current control.",
                ],
            }
        )
    if any(item["eligible_for_assessment"] for item in observations):
        outcome, diagnostic = (
            "candidate",
            "Exact indexed mentions require operator attribution review.",
        )
    elif observations:
        outcome, diagnostic = (
            "inconclusive",
            "Provider returned material without the exact literal visible in retained excerpts.",
        )
    else:
        outcome, diagnostic = (
            "not_found",
            "This bounded public-index query returned no results at the recorded time.",
        )
    return _result(
        outcome,
        observations=observations,
        query=query,
        request_count=1,
        diagnostic=diagnostic,
    )
