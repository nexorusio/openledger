"""Compatibility wrappers for the existing collectors.

New connectors live in their own package and are declared in the manifest;
this compatibility module and the central worker do not need new branches.
"""

from __future__ import annotations


def _runtime():
    from maigret.web import pipeline_execution

    return pipeline_execution


def _adapters():
    from maigret.web import collector_adapters

    return collector_adapters


def _timeout(task):
    return min(120, max(1, int(task.get("request_timeout_seconds", 30))))


def _emit(result, context):
    from maigret.web.pipeline_evidence import normalize_status

    rows = result if isinstance(result, list) else [result]
    context.raw_collector_observations.extend(rows)
    context.emit_observations(rows)
    from collections import Counter

    counts = Counter(normalize_status(row)[0] for row in rows if isinstance(row, dict))
    failed = set(counts) & {"blocked", "timeout", "error", "cancelled", "partial"}
    successful = set(counts) & {"found", "candidate", "not_found"}
    outcome = (
        "partial" if failed and successful else _runtime()._aggregate_outcomes(counts)
    )
    result = {"outcome": outcome, "outcome_counts": dict(counts)}
    explicit = [
        row.get("retryable")
        for row in rows
        if isinstance(row, dict) and isinstance(row.get("retryable"), bool)
    ]
    if explicit:
        result["retryable"] = any(explicit)
    # Never invent retry permission for a partial batch or blocked credentials.
    if outcome in {"partial", "blocked"}:
        result.setdefault("retryable", False)
    cooldowns = [
        row.get("retry_after_seconds", row.get("retry_after"))
        for row in rows
        if isinstance(row, dict)
    ]
    delays = [
        float(value)
        for value in cooldowns
        if isinstance(value, (float, int))
        and not isinstance(value, bool)
        and value >= 0
    ]
    if delays:
        result["retry_after_seconds"] = max(delays)
    return result


async def collect_maigret(task, context):
    return await _runtime()._maigret_adapter(task, context)


async def collect_native(task, context):
    return await _runtime()._native_adapter(task, context)


async def collect_public_exact(task, context):
    from maigret.web.pipeline_public_search import collect_public_exact_matches

    result = await collect_public_exact_matches(task, cancelled=context.cancelled)
    context.emit_observations(result.get("observations") or [])
    return result


async def collect_scanner_email(task, context):
    return _emit(
        await _adapters().run_user_scanner_email(
            task["input_value"],
            timeout_seconds=_timeout(task),
            cancellation_check=context.cancelled,
        ),
        context,
    )


async def collect_scanner_username(task, context):
    specification = context.options.get("investigation_spec") or {}
    return _emit(
        await _adapters().run_user_scanner_usernames(
            [task["input_value"]],
            platforms=[task["platform"]],
            timeout_seconds=_timeout(task),
            allow_vxtwitter=bool(specification.get("allow_user_scanner_vxtwitter")),
            cancellation_check=context.cancelled,
            observation_sink=context.emit_observations,
        ),
        context,
    )


async def collect_github(task, context):
    from urllib.parse import urlsplit

    value = task["input_value"]
    login = urlsplit(value).path.strip("/") if "://" in value else value
    return _emit(
        await _adapters().run_github_public_profile(
            {
                "github_login": login,
                "profile_url": "https://github.com/" + login,
                "investigated_username": login,
                "site_name": "GitHub",
            },
            timeout_seconds=_timeout(task),
        ),
        context,
    )


async def collect_url(task, context):
    return _emit(
        await getattr(_adapters(), task["execution_key"])(
            {
                "profile_url": task["input_value"],
                "investigated_username": task.get("originating_username", ""),
                "site_name": "Supplied public source",
            },
            timeout_seconds=_timeout(task),
        ),
        context,
    )


async def collect_wikipedia(task, context):
    return _emit(
        await _adapters().run_wikipedia_person_enrichment(
            task["input_value"],
            selected_page_id=context.context.get("selected_wikipedia_page_id"),
            timeout_seconds=_timeout(task),
        ),
        context,
    )


async def collect_name(task, context):
    return _emit(
        await getattr(_adapters(), task["execution_key"])(
            task["input_value"],
            timeout_seconds=_timeout(task),
        ),
        context,
    )


async def collect_wikidata(task, context):
    extra = context.context
    return _emit(
        await _adapters().run_wikidata_affiliation_discovery(
            task["input_value"],
            selected_entity_id=extra.get("wikidata_entity_id"),
            official_website=extra.get("official_website"),
            legal_jurisdiction=extra.get("legal_jurisdiction"),
        ),
        context,
    )


async def collect_registry(task, context):
    adapters = _adapters()
    jurisdiction = context.context.get("legal_jurisdiction")
    if not isinstance(jurisdiction, dict):
        jurisdiction = adapters.normalize_legal_jurisdiction(jurisdiction)
    return _emit(
        await getattr(adapters, task["execution_key"])(
            task["input_value"],
            jurisdiction,
            timeout_seconds=_timeout(task),
        ),
        context,
    )


async def collect_website(task, context):
    value = task["input_value"]
    organization = (context.context.get("official_website_organizations") or {}).get(
        value
    )
    if not organization:
        raise ValueError(
            "Official website research requires its explicitly bound organization"
        )
    return _emit(
        await _adapters().run_official_website_public_content(
            organization,
            value,
            timeout_seconds=_timeout(task),
        ),
        context,
    )


async def collect_places(task, context):
    return _emit(
        await _adapters().run_google_places_business_search(
            task["input_value"],
            _runtime()._app().get_google_maps_api_key(),
            legal_jurisdiction=context.context.get("legal_jurisdiction"),
            timeout_seconds=_timeout(task),
        ),
        context,
    )


async def collect_ai(task, context):
    return await _runtime()._cited_research_adapter(task, context)


async def collect_fusion_snapshot(task, context):
    from maigret.web.pipeline_case_fusion import collect_case_fusion_snapshot

    return await collect_case_fusion_snapshot(task, context)


async def collect_fusion_synthesis(task, context):
    from maigret.web.pipeline_case_fusion import collect_case_fusion_synthesis

    return await collect_case_fusion_synthesis(task, context)


async def collect_live_places(task, context):
    # Explicit caller authorizes transient display; never send details to ledger.
    return await _adapters().run_google_places_live_details(
        context.context.get("organization_name", ""),
        [task["input_value"]],
        _runtime()._app().get_google_maps_api_key(),
        timeout_seconds=_timeout(task),
    )


async def collect_operator_evidence(task, context):
    rows = context.context.get("operator_observations")
    if not isinstance(rows, list) or not rows:
        raise ValueError("An explicit operator evidence submission is required")
    return _emit(rows, context)


async def collect_feed(task, context):
    from maigret.web.pipeline_connector_ingestion import process_feed

    return await process_feed(task, context)
