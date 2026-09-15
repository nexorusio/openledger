"""Bounded, review-safe crawl diagnostics for operator export."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, Iterable


AUDIT_SCHEMA = "openledger-crawl-audit-1"
MAX_AUDIT_EVENTS = 10000
MAX_AUDIT_STRING = 10000
MAX_AUDIT_LIST = 10000
MAX_AUDIT_DEPTH = 12

_REDACTED_KEYS = {
    "api_key",
    "authorization",
    "bearer_token",
    "cookie",
    "cookies",
    "credential",
    "credentials",
    "csrf_token",
    "i2p_proxy",
    "openai_api_key",
    "password",
    "proxy",
    "refresh_token",
    "secret",
    "token",
    "tor_proxy",
    "worker_lease_token",
}
_OMITTED_CONTENT_KEYS = {
    "body",
    "document_body",
    "html",
    "raw_body",
    "raw_content",
    "response_body",
}
_JOB_FIELDS = (
    "job_id",
    "case_id",
    "case_title",
    "kind",
    "status",
    "usernames",
    "progress",
    "attempts",
    "budget_seconds",
    "budget_policy_version",
    "deadline_at",
    "created_at",
    "started_at",
    "heartbeat_at",
    "completed_at",
    "cancel_requested",
    "cancel_requested_at",
    "collection_status",
    "collection_message",
    "error",
)


def _sensitive_key(key: Any) -> bool:
    normalized = str(key or "").strip().casefold()
    return normalized in _REDACTED_KEYS or normalized.endswith(
        ("_api_key", "_password", "_secret", "_token", "_cookie", "_cookies", "_proxy")
    )


def sanitize_audit_value(value: Any, *, depth: int = 0) -> Any:
    """Bound nested diagnostics and remove credential-bearing material."""
    if depth >= MAX_AUDIT_DEPTH:
        return "[maximum audit depth reached]"
    if isinstance(value, dict):
        sanitized: Dict[str, Any] = {}
        for key, item in list(value.items())[:MAX_AUDIT_LIST]:
            label = str(key)
            if _sensitive_key(label):
                sanitized[label] = "[redacted]"
            elif label.strip().casefold() in _OMITTED_CONTENT_KEYS:
                sanitized[label] = "[raw content omitted]"
            else:
                sanitized[label] = sanitize_audit_value(item, depth=depth + 1)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [
            sanitize_audit_value(item, depth=depth + 1)
            for item in list(value)[:MAX_AUDIT_LIST]
        ]
    if isinstance(value, str):
        return value[:MAX_AUDIT_STRING]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)[:MAX_AUDIT_STRING]


def _collector_key(event: Dict[str, Any]) -> str:
    collector = str(event.get("collector") or event.get("site") or "discovery")
    qualifier = str(event.get("platform") or event.get("input_type") or "")
    return collector + (":" + qualifier if qualifier else "")


def _engine_summaries(events: Iterable[Dict[str, Any]], requests) -> list[Dict[str, Any]]:
    engines: Dict[str, Dict[str, Any]] = {}
    task_keys: Dict[str, str] = {}
    for request in requests:
        for task in request.get("tasks") or []:
            spec = task.get("spec") if isinstance(task.get("spec"), dict) else {}
            task_input = task.get("input") if isinstance(task.get("input"), dict) else {}
            engine = str(task.get("engine") or spec.get("engine_id") or "unknown")
            qualifier = str(
                task.get("platform")
                or spec.get("platform")
                or task_input.get("type")
                or ""
            )
            task_id = str(task.get("id") or "")
            key = "task:" + task_id if task_id else _collector_key(task)
            if task_id:
                task_keys[task_id] = key
            attempts = list(task.get("attempts") or [])
            engines[key] = {
                "engine": engine,
                "platform_or_input": qualifier or None,
                "input_type": task_input.get("type") or spec.get("input_type"),
                "input_value": task_input.get("value") or spec.get("input_value"),
                "task_id": task_id or None,
                "availability": task.get("availability"),
                "route_reason": task.get("reason"),
                "status": task.get("status"),
                "outcome": task.get("outcome"),
                "attempt_count": task.get("attempt_count", len(attempts)),
                "retry_limit": task.get("retry_limit"),
                "attempt_errors": [
                    attempt.get("error")
                    for attempt in attempts
                    if attempt.get("error")
                ],
                "evidence_observations": 0,
                "warning_count": 0,
                "diagnostic": task.get("reason"),
            }
    for stored_event in events:
        event = stored_event.get("event") or {}
        event_type = str(event.get("type") or "")
        if not event_type.startswith("collector_"):
            continue
        task_id = str(event.get("task_id") or "")
        key = task_keys.get(task_id) or _collector_key(event)
        row = engines.setdefault(
            key,
            {
                "engine": str(event.get("collector") or event.get("site") or "discovery"),
                "platform_or_input": event.get("platform") or event.get("input_type"),
                "task_id": event.get("task_id"),
                "availability": "active",
                "route_reason": None,
                "status": "planned",
                "outcome": None,
                "attempt_count": 0,
                "retry_limit": None,
                "attempt_errors": [],
                "evidence_observations": 0,
                "warning_count": 0,
                "diagnostic": None,
            },
        )
        row["last_event_at"] = stored_event.get("created_at")
        if event_type == "collector_started":
            row["status"] = "running"
            row["attempt_count"] = max(1, int(row.get("attempt_count") or 0))
        elif event_type == "collector_error":
            row["status"] = "completed"
            row["outcome"] = "unavailable"
            row["diagnostic"] = event.get("message") or event.get("diagnostic")
        elif event_type == "collector_completed":
            row["status"] = "completed"
            row["outcome"] = (
                event.get("display_status")
                or event.get("outcome")
                or event.get("status")
                or "completed"
            )
            row["evidence_observations"] = int(
                event.get("evidence_observations")
                or event.get("observations")
                or event.get("found")
                or 0
            )
            row["warning_count"] = int(event.get("warning_count") or 0)
            row["outcome_counts"] = event.get("outcome_counts") or {}
            row["diagnostic"] = event.get("diagnostic") or row.get("diagnostic")
    return [sanitize_audit_value(engines[key]) for key in sorted(engines)]


def build_crawl_audit(
    job: Dict[str, Any],
    *,
    events,
    events_truncated: bool,
    pipeline_audit: Dict[str, Any],
    profile_search_audits,
    generated_at: datetime | None = None,
) -> Dict[str, Any]:
    """Build a portable audit without modifying the investigation ledger."""
    generated_at = generated_at or datetime.now(timezone.utc)
    safe_events = [sanitize_audit_value(event) for event in events]
    safe_pipeline = sanitize_audit_value(pipeline_audit)
    safe_search_audits = sanitize_audit_value(profile_search_audits)
    requests = safe_pipeline.get("requests") or []
    engines = _engine_summaries(safe_events, requests)
    outcomes = Counter(
        str(item.get("outcome") or item.get("status") or "unknown")
        for item in engines
    )
    job_summary = {
        field: sanitize_audit_value(job.get(field))
        for field in _JOB_FIELDS
        if field in job
    }
    options = job.get("options") if isinstance(job.get("options"), dict) else {}
    job_summary["execution_configuration"] = sanitize_audit_value(
        {
            "execution_mode": options.get("execution_mode"),
            "all_sites": options.get("all_sites"),
            "profile_discovery_policy": options.get("profile_discovery_policy"),
            "pipeline_source_status": options.get("pipeline_source_status"),
            "investigation_spec": options.get("investigation_spec"),
            "proxy_configured": options.get("proxy_configured"),
            "tor_proxy_configured": options.get("tor_proxy_configured"),
            "i2p_proxy_configured": options.get("i2p_proxy_configured"),
        }
    )
    return {
        "schema": AUDIT_SCHEMA,
        "generated_at": generated_at.astimezone(timezone.utc).isoformat(),
        "purpose": (
            "Diagnose routing, collection coverage and engine failures; "
            "this export does not change evidence or approval state."
        ),
        "handling_notice": (
            "Contains investigation identifiers and public-source metadata. "
            "Credentials and raw response bodies are redacted or omitted."
        ),
        "job": job_summary,
        "summary": {
            "engine_count": len({item.get("engine") for item in engines}),
            "route_count": len(engines),
            "engine_outcomes": dict(sorted(outcomes.items())),
            "event_count": len(safe_events),
            "events_truncated": bool(events_truncated),
            "request_count": len(requests),
            "observation_count": int(safe_pipeline.get("observation_count") or 0),
            "exported_observation_count": len(safe_pipeline.get("observations") or []),
            "observations_truncated": bool(safe_pipeline.get("observations_truncated")),
            "native_profile_search_audit_count": len(safe_search_audits),
        },
        "engines": engines,
        "pipeline": safe_pipeline,
        "native_profile_search_audits": safe_search_audits,
        "events": safe_events,
    }
