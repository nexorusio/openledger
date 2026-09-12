"""Deterministic query routing for the complete P2 evidence/QC workflow.

This module plans only. The worker owns dispatch and transactional persistence.
An unavailable source or unmet prerequisite is a visible route, not a negative
finding. No previous-pipeline fallback exists in this contract.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from maigret.web.pipeline_contract import (
    ENGINE_CAPABILITIES,
    PIPELINE_CONTRACT_VERSION,
    PIPELINE_ID,
    PIPELINE_SCHEMA_REVISION,
    canonical_digest,
    stable_id,
    validate_task,
)

DEFAULT_BUDGETS = {
    "max_requests": 100000,
    "max_tasks": 512,
    "max_depth": 3,
    "timeout_seconds": 1800,
    "retry_ceiling": 1,
}
_CONTEXT_INPUTS = {
    "organization_names": "organization",
    "official_websites": "official_website",
    "public_urls": "public_url",
    "selected_place_ids": "place_id",
    "research_questions": "research_question",
    "operator_messages": "operator_message",
    "external_evidence_ids": "external_evidence",
    "case_references": "case_reference",
    "snapshot_references": "snapshot_reference",
}


def runtime_source_status() -> dict[str, Any]:
    """Snapshot policy and adapter availability, without opening credentials."""
    from maigret.web.profile_discovery_policy import profile_discovery_flags
    from maigret.web.profile_search_backend import (
        BRAVE_SEARCH_API_URL,
        SEARXNG_SEARCH_URL,
        ProfileSearchConfigurationError,
        load_profile_search_config,
    )

    flags = profile_discovery_flags()
    try:
        config = load_profile_search_config()
        search = {
            "enabled": config.enabled,
            "reason": (
                "Provider configured; credentials are checked at collection."
                if config.enabled
                else "No public search provider is configured."
            ),
            "provider": config.provider,
            "provider_configuration_revision": canonical_digest(
                {
                    "provider": config.provider,
                    "timeout_seconds": config.timeout_seconds,
                    "max_results": config.max_results,
                    "endpoint": (
                        SEARXNG_SEARCH_URL
                        if config.provider == "searxng"
                        else BRAVE_SEARCH_API_URL
                    ),
                }
            ),
        }
    except ProfileSearchConfigurationError:
        search = {
            "enabled": False,
            "reason": "Search provider configuration is invalid.",
        }
    native = dict(search)
    if not flags["search_first_enabled"]:
        native.update(
            enabled=False, reason="Native profile search is disabled by server policy."
        )
        search.update(
            enabled=False,
            reason="Public search is disabled by the existing search-provider opt-in policy.",
        )
    return {
        "discovery_enabled": flags["profile_discovery_enabled"],
        "maigret_enabled": flags["maigret_enabled"],
        "scanner_enabled": flags["user_scanner_enabled"],
        "scanner_available": importlib.util.find_spec("user_scanner") is not None,
        "enrichment_enabled": flags["enrichment_providers_enabled"],
        "native_search": native,
        "public_search": search,
        # The application supplies authenticated server-side settings for these.
        "google_places_enabled": False,
        "ai_enabled": False,
        "engines": {},
    }


def _catalog() -> dict[str, dict[str, Any]]:
    path = Path(__file__).resolve().parents[2] / "config" / "osint-sources.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {item["id"]: item for item in payload["sources"]}


def _bounded_budgets(values: Mapping[str, Any] | None) -> dict[str, int]:
    result = dict(DEFAULT_BUDGETS)
    for key, value in (values or {}).items():
        if key not in result:
            raise ValueError("Unknown pipeline budget.")
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= DEFAULT_BUDGETS[key]
        ):
            raise ValueError(f"Pipeline {key} exceeds the server budget.")
        if key in {"max_tasks", "timeout_seconds"} and value == 0:
            raise ValueError(f"Pipeline {key} must be positive.")
        result[key] = value
    return result


def _source_snapshot(
    engine, platform: str, source_status: Mapping[str, Any], catalog: Mapping[str, Any]
) -> dict[str, Any]:
    from maigret.web.connectors.registry import get_connector_registry

    spec = get_connector_registry().get(engine.engine_id)
    row = catalog.get(engine.engine_id, {})
    result = {
        "enabled": row.get("status") == "active",
        "reason": "Source available; its prerequisites and selections apply.",
        "health": "unknown",
        "catalog_status": row.get("status", "missing"),
    }
    reasons = []
    if not result["enabled"]:
        reasons.append("Source catalog does not mark this adapter active.")
    if source_status.get("discovery_enabled") is False and engine.trigger == "query":
        reasons.append("Collection is disabled by server policy.")
    group = spec.policy_group
    configuration = dict(source_status.get("engines", {}).get(engine.engine_id, {}))
    if group == "maigret" and not source_status.get("maigret_enabled", True):
        reasons.append("Maigret is disabled by server policy.")
    elif group == "scanner" and not (
        source_status.get("scanner_enabled", True)
        and source_status.get("scanner_available", False)
    ):
        reasons.append("User Scanner is unavailable or disabled by server policy.")
    elif group in {"native_search", "public_search"}:
        search = source_status.get(group, {})
        configuration.setdefault("paid_access_enabled", bool(search.get("enabled")))
        if not search.get("enabled", False):
            reasons.append(
                str(search.get("reason") or "No search provider is available.")[:500]
            )
        result["provider"] = str(search.get("provider", ""))[:80]
        result["provider_configuration_revision"] = str(
            search.get("provider_configuration_revision", "")
        )[:100]
    elif group in {"google_places", "ai"}:
        enabled = bool(source_status.get(group + "_enabled", False))
        configuration.update(
            credentials_configured=enabled, paid_access_enabled=enabled
        )
        if not enabled:
            reasons.append(
                "Google Places is not configured or is disabled."
                if group == "google_places"
                else "Cited AI research is not configured or is disabled."
            )
    elif group == "enrichment" and source_status.get("enrichment_enabled") is False:
        reasons.append("Enrichment adapters are disabled by server policy.")
    configuration = spec.configuration_snapshot(configuration)
    if configuration.get("enabled") is False:
        reasons.append("Connector is disabled by its server environment configuration.")
    try:
        spec.validate_configuration(row, configuration)
    except ValueError as error:
        reasons.append(str(error)[:500])
    if reasons:
        result.update(enabled=False, reason=reasons[0])
    overrides = source_status.get("engines", {})
    override = overrides.get(
        f"{engine.engine_id}:{platform}", overrides.get(engine.engine_id, {})
    )
    # Explicit per-source overrides can further constrain policy, never enable
    # an engine disabled by a global or provider-level switch.
    if override.get("enabled") is False:
        result.update(
            enabled=False,
            reason=str(
                override.get("reason") or "Source disabled in server configuration."
            )[:500],
        )
    if override.get("health"):
        result["health"] = str(override["health"])[:80]
    if result["health"] in {"disabled", "quarantined", "broken"}:
        result.update(
            enabled=False,
            reason=str(
                override.get("reason") or "Source detector is quarantined or unhealthy."
            )[:500],
        )
    result["configuration_revision"] = canonical_digest(
        {
            "adapter": engine.as_dict(),
            "catalog": row,
            "snapshot": result,
            "operator_revision": str(override.get("revision", ""))[:200],
        }
    )
    return result


def _values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)):
        raise ValueError("Query context inputs must be a bounded list.")
    if len(value) > 24:
        raise ValueError("Query context accepts at most 24 values per input type.")
    return [str(item).strip()[:2000] for item in value if str(item).strip()]


def _inputs(
    plan: Mapping[str, Any], context: Mapping[str, Any], case_id: str, subject_id: str
) -> list[dict[str, Any]]:
    inputs: dict[tuple[str, str], dict[str, Any]] = {}
    raw_rows = list(plan.get("input_provenance") or [])

    def add(kind: str, value: str, *, derived_from: Any = None):
        # Preserve case-sensitive identifiers. Engine/platform canonicalization
        # occurs later; the query handler does not strip plus/dot email aliases.
        key = (kind, value)
        if key not in inputs:
            inputs[key] = {
                "input_id": stable_id("input", case_id, subject_id, kind, value),
                "case_id": case_id,
                "subject_id": subject_id,
                "type": kind,
                "value": value,
                "provenance": [
                    dict(row)
                    for row in raw_rows
                    if row.get("type") == kind and row.get("value") == value
                ],
                "derived_from": [],
            }
        if derived_from and derived_from not in inputs[key]["derived_from"]:
            inputs[key]["derived_from"].append(derived_from)

    for row in plan.get("identifiers", []):
        if isinstance(row, dict) and row.get("value"):
            kind = (
                "username"
                if row.get("type") == "social_handle"
                else str(row.get("type"))
            )
            add(kind, str(row["value"]))
    for row in plan.get("search_targets", []):
        if isinstance(row, dict) and row.get("value"):
            add(
                "username",
                str(row["value"]),
                derived_from={
                    "type": row.get("source_type"),
                    "value": row.get("source_value"),
                },
            )
    for url, usernames in (plan.get("profile_url_usernames") or {}).items():
        for username in usernames:
            add(
                "username",
                str(username),
                derived_from={"type": "profile_url", "value": url},
            )
    for name, kind in _CONTEXT_INPUTS.items():
        for value in _values(context.get(name)):
            add(kind, value, derived_from={"context": name, "operator_selected": True})
    return list(inputs.values())


def _missing_prerequisites(
    engine, item: Mapping[str, Any], plan: Mapping[str, Any], context: Mapping[str, Any]
) -> list[str]:
    missing = []
    value = item.get("value", "")
    for prerequisite in engine.prerequisites:
        met = False
        if prerequisite == "approved_full_name":
            met = value in _values(context.get("approved_full_names"))
        elif prerequisite == "approved_organization":
            met = value in _values(context.get("approved_organizations"))
        elif prerequisite in {"legal_jurisdiction", "fr_jurisdiction"}:
            jurisdiction = context.get("legal_jurisdiction")
            jurisdiction = (
                jurisdiction.get("code")
                if isinstance(jurisdiction, dict)
                else jurisdiction
            )
            met = (
                bool(jurisdiction)
                if prerequisite == "legal_jurisdiction"
                else jurisdiction == "FR"
            )
        elif prerequisite == "official_website":
            met = value in _values(context.get("official_websites"))
        elif prerequisite == "website_organization_binding":
            met = bool((context.get("official_website_organizations") or {}).get(value))
        elif prerequisite == "github_profile_target":
            parsed = urlsplit(value)
            met = (
                item.get("type") == "profile_url"
                and (parsed.hostname or "").casefold()
                in {"github.com", "www.github.com"}
                and len([part for part in parsed.path.split("/") if part]) == 1
            )
            met = met or value in _values(context.get("github_usernames"))
        elif prerequisite == "approved_research_question":
            met = value in _values(context.get("approved_research_questions"))
        elif prerequisite == "operator_live_detail_request":
            met = context.get("operator_live_detail_request") is True
        elif prerequisite == "operator_submission":
            met = context.get("operator_submission") is True
        elif prerequisite == "approved_case_selection":
            met = (
                context.get("approved_case_selection") is True
                and len(context.get("source_case_ids") or []) >= 2
                and context.get("evidence_scope") == "approved_only"
            )
        elif prerequisite == "validated_snapshot_reference":
            met = context.get(
                "validated_snapshot_reference"
            ) is True and value in _values(context.get("snapshot_references"))
        if not met:
            missing.append(prerequisite)
    if engine.engine_id == "public_exact_match" and item.get("type") == "phone":
        phone = (plan.get("phone_context") or {}).get(value, {})
        if not str(value).startswith("+") and not phone.get("country"):
            missing.append("phone_country_context")
    return missing


def build_query_plan(
    investigation_plan: Mapping[str, Any],
    *,
    case_id: str = "",
    subject_id: str = "",
    request_id: str = "",
    source_status: Mapping[str, Any] | None = None,
    context: Mapping[str, Any] | None = None,
    origin: Mapping[str, Any] | None = None,
    budgets: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the full durable plan, including every unavailable/conditional route.

    ``source_status`` and ``context`` are server-owned. Do not bind them directly
    from an HTTP body: reviewed names, organization scope and source activation
    must come from persisted decisions and authenticated server configuration.
    ``origin`` records parent_request_id, qc_id, version_id, requirement_ids and
    depth; it never changes case/subject identity.
    """
    if not isinstance(investigation_plan, Mapping):
        raise ValueError("An investigation input plan is required.")
    context, origin = dict(context or {}), dict(origin or {})
    limits = _bounded_budgets(budgets)
    depth = origin.get("depth", 0)
    if isinstance(depth, bool) or not isinstance(depth, int) or depth < 0:
        raise ValueError("Query research depth must be a nonnegative integer.")
    for key, expected in (("case_id", case_id), ("subject_id", subject_id)):
        if origin.get(key) and origin[key] != expected:
            raise ValueError("Research origin cannot change the case or subject.")
    sources = runtime_source_status()
    sources.update(dict(source_status or {}))
    catalog = _catalog()
    inputs = _inputs(investigation_plan, context, case_id, subject_id)
    if not inputs:
        raise ValueError("A query requires an input or explicit research requirement.")
    selection = {
        "tags": list(investigation_plan.get("tags") or []),
        "excluded_tags": list(investigation_plan.get("excluded_tags") or []),
    }
    requested_engines = context.get("requested_engines")
    from maigret.web.execution_budget import execution_budget_spec_from_options

    collection_options = dict(context.get("collection_options") or {})
    execution_budget = execution_budget_spec_from_options(collection_options)
    selected_sites = context.get("selected_maigret_sites")
    if selected_sites is not None:
        if not isinstance(selected_sites, (list, tuple)):
            raise ValueError(
                "Selected Maigret sources must come from the server selection."
            )
        maigret_requests = len(selected_sites)
    elif (
        collection_options.get("all_sites") or execution_budget["mode"] == "exhaustive"
    ):
        database_path = Path(__file__).resolve().parents[1] / "resources" / "data.json"
        maigret_requests = len(
            json.loads(database_path.read_text(encoding="utf-8"))["sites"]
        )
    else:
        maigret_requests = int(collection_options.get("top_sites") or 500)
    if not 0 <= maigret_requests <= 100000:
        raise ValueError("Maigret source selection exceeds the server request ceiling.")
    tasks = []
    active_requests = 0
    active_count = 0
    # Reserve the smaller source requests first so a large Maigret scan cannot
    # silently consume the budget of every other compatible engine.
    from maigret.web.connectors.registry import get_connector_registry

    registry = (
        get_connector_registry()
    )  # All declared implementations validate before work is saved.
    for engine in sorted(
        (item for item in ENGINE_CAPABILITIES if item.trigger != "machine"),
        key=lambda item: item.engine_id == "maigret",
    ):
        compatible = [item for item in inputs if item["type"] in engine.input_types]
        for item in compatible or [{}]:
            for platform in engine.platforms or ("",):
                snapshot = _source_snapshot(engine, platform, sources, catalog)
                request_budget = (
                    maigret_requests
                    if engine.engine_id == "maigret"
                    else engine.request_budget
                )
                state, reason = (
                    "active",
                    "Compatible input and enabled collection route.",
                )
                prerequisites = _missing_prerequisites(
                    engine, item, investigation_plan, context
                )
                if not item:
                    state, reason = (
                        "conditional",
                        "Requires "
                        + ", ".join(engine.input_types)
                        + " input or a reviewed research lead.",
                    )
                elif prerequisites:
                    state, reason = (
                        "conditional",
                        "Requires " + ", ".join(prerequisites) + ".",
                    )
                if not snapshot["enabled"]:
                    state, reason = "unavailable", snapshot["reason"]
                if engine.option and not investigation_plan.get(engine.option):
                    state, reason = (
                        "excluded",
                        "The operator has not selected this collection option.",
                    )
                if (
                    engine.engine_id == "user_scanner_username"
                    and platform
                    not in investigation_plan.get("user_scanner_username_platforms", [])
                ):
                    state, reason = (
                        "excluded",
                        "This User Scanner platform is excluded by the operator.",
                    )
                if (
                    requested_engines is not None
                    and engine.engine_id not in requested_engines
                ):
                    state, reason = (
                        "excluded",
                        "Outside the engines selected for this research requirement.",
                    )
                if depth > limits["max_depth"]:
                    state, reason = (
                        "excluded",
                        "Research depth budget exhausted; operator assessment is required.",
                    )
                if engine.engine_id == "maigret" and maigret_requests == 0:
                    state, reason = (
                        "unavailable",
                        "No eligible Maigret sources remain after selection and detector health checks.",
                    )
                if state == "active" and (
                    active_count >= limits["max_tasks"]
                    or active_requests + request_budget > limits["max_requests"]
                ):
                    state, reason = (
                        "conditional",
                        "Collection request/task budget exhausted; revise the scoped research plan.",
                    )
                if state == "active":
                    active_requests += request_budget
                    active_count += 1
                task = {
                    "pipeline_id": PIPELINE_ID,
                    "task_id": stable_id(
                        "task",
                        case_id,
                        subject_id,
                        request_id,
                        engine.engine_id,
                        platform,
                        item.get("input_id", ""),
                    ),
                    "case_id": case_id,
                    "subject_id": subject_id,
                    "request_id": request_id,
                    "engine_id": engine.engine_id,
                    "execution_key": engine.execution_key,
                    "label": engine.label,
                    "platform": platform,
                    "input_id": item.get("input_id", ""),
                    "input_type": item.get("type", ""),
                    "input_value": item.get("value", ""),
                    "route_state": state,
                    "reason": reason,
                    "prerequisites": prerequisites,
                    "timeout_seconds": min(
                        (
                            execution_budget["total_seconds"]
                            if engine.engine_id == "maigret"
                            else engine.timeout_seconds
                        ),
                        limits["timeout_seconds"],
                    ),
                    "request_timeout_seconds": max(
                        1, min(int(collection_options.get("timeout") or 30), 60)
                    ),
                    "retry_ceiling": min(engine.retry_ceiling, limits["retry_ceiling"]),
                    "request_budget": request_budget,
                    "retention": engine.retention,
                    **registry.get(engine.engine_id).task_metadata(
                        snapshot.get("provider", "")
                    ),
                    "budget_estimate": bool(
                        (engine.engine_id == "maigret" and selected_sites is None)
                        or engine.engine_id
                        in {
                            "user_scanner_email",
                            "user_scanner_username",
                            "wikidata_affiliation",
                            "wikipedia_public_biography",
                            "cloudflare_dns_context",
                            "official_website_public_content",
                            "ai_cited_research",
                            "case_fusion_synthesis",
                        }
                    ),
                    "source_config_revision": snapshot["configuration_revision"],
                    "source_health": snapshot["health"],
                    "source_status": snapshot,
                    "source_selection": selection,
                    "origin": origin,
                    "execution_context": {
                        "legal_jurisdiction": context.get("legal_jurisdiction"),
                        "phone_country": (investigation_plan.get("phone_context") or {})
                        .get(item.get("value", ""), {})
                        .get("country", ""),
                        "organization_name": context.get("organization_name", ""),
                        "allow_user_scanner_vxtwitter": bool(
                            investigation_plan.get("allow_user_scanner_vxtwitter")
                        ),
                    },
                }
                validate_task(task)
                tasks.append(task)
    result = {
        "pipeline_id": PIPELINE_ID,
        "schema_version": PIPELINE_CONTRACT_VERSION,
        "schema_revision": PIPELINE_SCHEMA_REVISION,
        "case_id": case_id,
        "subject_id": subject_id,
        "request_id": request_id,
        "inputs": inputs,
        "tasks": tasks,
        "origin": origin,
        "budgets": limits,
        "execution_budget": execution_budget,
        "active_task_count": active_count,
        "estimated_request_count": active_requests,
        "research_needed": active_count == 0,
        "source_configuration_revision": canonical_digest(
            [task["source_config_revision"] for task in tasks]
        ),
    }
    result["plan_hash"] = canonical_digest(result)
    return result


def revalidate_query_plan(
    saved: Mapping[str, Any], investigation_plan: Mapping[str, Any], **kwargs: Any
) -> dict[str, Any]:
    """Replan on dispatch and expose drift; never execute stale source settings."""
    if saved.get("pipeline_id") != PIPELINE_ID:
        raise ValueError("Saved request belongs to a different pipeline.")
    for key in ("case_id", "subject_id", "request_id", "origin", "budgets"):
        kwargs.setdefault(key, saved.get(key))
    current = build_query_plan(investigation_plan, **kwargs)
    old_tasks = {item["task_id"]: item for item in saved.get("tasks", [])}
    changes = []
    for task in current["tasks"]:
        old = old_tasks.get(task["task_id"])
        if old is None or any(
            old.get(key) != task.get(key)
            for key in ("source_config_revision", "route_state", "reason")
        ):
            changes.append(
                {
                    "task_id": task["task_id"],
                    "previous_state": (old or {}).get("route_state"),
                    "current_state": task["route_state"],
                    "reason": task["reason"],
                }
            )
    return {
        "plan": current,
        "changed": saved.get("plan_hash") != current["plan_hash"],
        "changes": changes,
        "previous_plan_hash": saved.get("plan_hash"),
        "current_plan_hash": current["plan_hash"],
    }
