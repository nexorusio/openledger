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
    # The reviewed manifest owns the per-engine deadline. The earlier 120-second
    # compatibility cap and the Maigret per-site request timeout silently
    # shortened User Scanner's whole-batch contract.
    return min(420, max(1, int(task.get("timeout_seconds", 30))))


def _emit(result, context):
    from maigret.web.pipeline_evidence import normalize_status

    rows = result if isinstance(result, list) else [result]
    context.raw_collector_observations.extend(rows)
    context.emit_observations(rows)
    from collections import Counter

    counts = Counter(normalize_status(row)[0] for row in rows if isinstance(row, dict))
    failed = set(counts) & {"blocked", "timeout", "error", "cancelled", "partial"}
    successful = set(counts) & {"found", "candidate", "not_found"}
    # A positive result remains usable when independent modules fail. Surface
    # the failed checks as warnings; do not downgrade retained findings to an
    # opaque engine-wide "partial" status. Negative-only mixed batches remain
    # partial because failed checks make an absence conclusion incomplete.
    outcome = (
        "found"
        if counts.get("found") and failed
        else "partial" if failed and successful else _runtime()._aggregate_outcomes(counts)
    )
    warning_count = sum(counts[value] for value in failed) if outcome == "found" else 0
    result = {
        "outcome": outcome,
        "outcome_counts": dict(counts),
        "warning_count": warning_count,
        "display_status": (
            "completed_with_warnings" if warning_count else outcome
        ),
    }
    if failed:
        failed_count = sum(counts[value] for value in failed)
        result["diagnostic"] = (
            f"{failed_count} provider check"
            f"{'s were' if failed_count != 1 else ' was'} unavailable; "
            "recorded evidence retains the individual reasons."
        )
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
    from urllib.parse import urlsplit

    from maigret.web.investigation_input import extract_profile_usernames

    if task.get("engine_id") == "approved_public_source_fetch":
        return await collect_approved_source(task, context)

    profile_url = task["input_value"]
    usernames = extract_profile_usernames(profile_url)
    hostname = (urlsplit(profile_url).hostname or "Public source").removeprefix(
        "www."
    )
    return _emit(
        await getattr(_adapters(), task["execution_key"])(
            {
                "profile_url": profile_url,
                "investigated_username": usernames[0] if usernames else "",
                "site_name": hostname,
            },
            timeout_seconds=_timeout(task),
        ),
        context,
    )


def _approved_maigret_target(profile_url, sites):
    """Resolve one approved URL to one exact Maigret site and username."""
    from urllib.parse import urlsplit

    from maigret.web.investigation_input import (
        extract_profile_usernames,
        normalize_username,
    )

    def normalized_host(value):
        return (urlsplit(value).hostname or "").casefold().removeprefix("www.")

    def same_host(left, right):
        if left == right:
            return True
        # LinkedIn uses country-localized public hosts for the same /in/ path.
        def is_linkedin(host):
            return host == "linkedin.com" or host.endswith(".linkedin.com")

        return is_linkedin(left) and is_linkedin(right)

    approved = urlsplit(profile_url)
    approved_host = normalized_host(profile_url)
    approved_path = approved.path.rstrip("/").casefold()
    usernames = extract_profile_usernames(profile_url)
    segments = [segment for segment in approved.path.split("/") if segment]
    if (
        not usernames
        and (approved_host == "linkedin.com" or approved_host.endswith(".linkedin.com"))
        and len(segments) == 2
        and segments[0].casefold() == "in"
    ):
        try:
            usernames = [normalize_username(segments[1])]
        except ValueError:
            usernames = []
    for username in usernames:
        for site in sites:
            if (
                getattr(site, "disabled", False)
                or getattr(site, "type", "username") != "username"
                or getattr(site, "url_probe", None)
                or getattr(site, "get_params", None)
                or getattr(site, "request_payload", None)
                or str(getattr(site, "request_method", "") or "").casefold()
                not in {"", "get", "head"}
            ):
                continue
            try:
                generated_url = str(site.url).format(username=username)
            except (AttributeError, KeyError, ValueError):
                continue
            generated = urlsplit(generated_url)
            if same_host(normalized_host(generated_url), approved_host) and (
                generated.path.rstrip("/").casefold() == approved_path
            ):
                return site.name, username
    return None


def _approved_maigret_public_headers(site):
    """Return site headers for later allowlist filtering by the safe fetcher."""
    headers = getattr(site, "headers", None)
    return dict(headers) if isinstance(headers, dict) else {}


def _approved_rich_claims(rows):
    predicates = {
        "photograph",
        "current_location",
        "address",
        "organization_location",
        "occupation",
        "affiliation",
        "company",
        "organization",
        "email",
        "phone",
    }
    return [
        claim
        for row in rows
        if isinstance(row, dict)
        for claim in (row.get("claims") or [])
        if isinstance(claim, dict)
        and (claim.get("predicate") or claim.get("field_name")) in predicates
    ]


def _approved_source_stage(context, source_url, stage, status, message, **counts):
    context.sink.put(
        {
            "type": "approved_source_stage",
            "collector": "approved_public_source_fetch",
            "task_id": context.task["id"],
            "source_url": source_url,
            "input_value": source_url,
            "stage": stage,
            "status": status,
            "message": message,
            **counts,
            "pipeline_id": "p2-e2e-v1",
        }
    )


def _reusable_maigret_observations(context, source_url):
    """Re-propose structured fields from prior exact-URL Maigret evidence."""
    from maigret.web.pipeline_execution import _same_approved_profile_source

    rows = []
    for observation in context.pipeline.iter_observations(
        context.job["case_id"], context.request["persona_id"]
    ):
        if observation.get("engine") != "maigret" or not _same_approved_profile_source(
            observation.get("canonical_url") or observation.get("source_url"),
            source_url,
        ):
            continue
        claims = _approved_rich_claims([observation])
        if not claims:
            continue
        rows.append(
            {
                "source_engine": "maigret",
                "source_record_id": "approved-reuse:" + observation["id"],
                "source_url": observation.get("source_url") or source_url,
                "origin_family_id": observation.get("origin_family_id"),
                "original_evidence_id": observation["id"],
                "derived_from": [observation["id"]],
                "status": "candidate",
                "claims": [
                    {
                        **claim,
                        "qualifiers": {
                            **dict(claim.get("qualifiers") or {}),
                            "evidence_basis": "existing_exact_url_maigret_evidence",
                            "human_review_required": True,
                            "automatic_approval_allowed": False,
                        },
                    }
                    for claim in claims
                ],
            }
        )
    return rows


async def collect_approved_source(task, context):
    """Enrich one exact approved URL with Maigret-first, review-only evidence."""
    from maigret.sites import MaigretDatabase
    from maigret.web.pipeline_evidence import normalize_status

    app_module = _runtime()._app()
    source_url = task["input_value"]
    outcomes = []
    warnings = 0

    reused = _reusable_maigret_observations(context, source_url)
    if reused:
        context.emit_observations(reused, engine="maigret")
        _approved_source_stage(
            context,
            source_url,
            "maigret_reuse",
            "completed",
            "Reused structured fields from prior exact-URL Maigret evidence.",
            claim_count=len(_approved_rich_claims(reused)),
        )

    _approved_source_stage(
        context,
        source_url,
        "maigret_catalogue",
        "running",
        "Matching the approved URL to one exact Maigret site definition.",
    )
    matched = None
    matched_site = None
    try:
        database = MaigretDatabase().load_from_path(
            app_module.app.config["MAIGRET_DB_FILE"]
        )
        matched = _approved_maigret_target(source_url, database.sites)
        if matched:
            matched_site = next(
                (site for site in database.sites if site.name == matched[0]), None
            )
            _approved_source_stage(
                context,
                source_url,
                "maigret_catalogue",
                "completed",
                "Matched the approved URL to the exact Maigret site parser: "
                + matched[0]
                + ".",
            )
        else:
            _approved_source_stage(
                context,
                source_url,
                "maigret_catalogue",
                "unsupported",
                "No exact Maigret site definition matches this approved URL.",
            )
    except Exception as error:
        warnings += 1
        diagnostic = app_module.record_internal_error(
            "Exact-site Maigret catalogue matching was unavailable",
            error,
            session=context.job["job_id"],
        )
        _approved_source_stage(
            context,
            source_url,
            "maigret_catalogue",
            "unavailable",
            diagnostic,
        )

    _approved_source_stage(
        context,
        source_url,
        "literal_page",
        "running",
        "Reading bounded public HTML metadata and JSON-LD without login or scripts.",
    )
    try:
        direct = await _adapters().run_approved_public_source_fetch(
            {
                "profile_url": source_url,
                "investigated_username": matched[1] if matched else "",
                "site_name": matched[0] if matched else "Public source",
                "maigret_site_name": matched[0] if matched else "",
                "maigret_public_headers": (
                    _approved_maigret_public_headers(matched_site)
                    if matched_site
                    else {}
                ),
            },
            timeout_seconds=min(30, _timeout(task)),
        )
    except Exception as error:
        diagnostic = app_module.record_internal_error(
            "Bounded literal approved-source fetch was unavailable",
            error,
            session=context.job["job_id"],
        )
        direct = {
            "source_engine": "approved_public_source_fetch",
            "source_record_id": "approved-source-unavailable",
            "subject_type": "approved_public_url",
            "subject_value": source_url,
            "source_url": source_url,
            "status": "unavailable",
            "reason": diagnostic,
            "claims": [],
        }
    direct_summary = _emit(direct, context)
    direct_outcome = direct_summary.get("outcome") or normalize_status(direct)[0]
    outcomes.append(direct_outcome)
    if direct_outcome in {
        "blocked",
        "timeout",
        "error",
        "partial",
        "inconclusive",
        "not_executed",
    }:
        warnings += 1
    _approved_source_stage(
        context,
        source_url,
        "literal_page",
        "completed",
        direct.get("reason") or "Bounded literal page extraction completed.",
        http_status=direct.get("http_status"),
        claim_count=len(direct.get("claims") or []),
    )
    if matched:
        parser_fields = list(
            (direct.get("extra") or {}).get("maigret_parser_fields") or []
        )
        _approved_source_stage(
            context,
            source_url,
            "maigret_parser",
            "completed" if direct_outcome in {"found", "candidate"} else "unavailable",
            (
                "Applied Maigret's structured parser to the bounded exact-page response."
                if direct_outcome in {"found", "candidate"}
                else "The exact page returned no public body for Maigret's parser."
            ),
            claim_count=len(parser_fields),
        )

    rich_claims = [
        *_approved_rich_claims(reused),
        *_approved_rich_claims([direct]),
    ]
    research_result = None
    if not rich_claims:
        persona = context.store.get_persona(context.request["persona_id"]) or {}
        subject = str(persona.get("display_name") or context.request["persona_id"])
        question = (
            "Research public indexed evidence for only this exact approved profile URL: "
            + source_url
            + ". The target Persona is "
            + subject
            + ". Use only citations that resolve to this same profile path, including a localized host alias. "
            "Propose literal profile fields such as photograph, location, occupation, or affiliation only when the cited excerpt directly supports them. "
            "Do not infer identity, use login-only content, or use unrelated same-name results."
        )
        _approved_source_stage(
            context,
            source_url,
            "cited_fallback",
            "running",
            "Direct parsing yielded no useful Persona fields; checking public indexed excerpts for the exact profile.",
        )
        try:
            research_result = await _runtime()._cited_research_adapter(
                {
                    "input_value": question,
                    "timeout_seconds": max(30, _timeout(task) - 30),
                    "approved_source_url": source_url,
                },
                context,
            )
            outcomes.append(research_result.get("outcome") or "inconclusive")
            _approved_source_stage(
                context,
                source_url,
                "cited_fallback",
                "completed",
                "Exact-profile cited research completed; all proposals remain pending review.",
                citation_count=research_result.get("citation_count", 0),
                proposal_count=research_result.get("proposal_count", 0),
            )
        except Exception as error:
            warnings += 1
            diagnostic = app_module.record_internal_error(
                "Approved-source cited fallback was unavailable",
                error,
                session=context.job["job_id"],
            )
            context.emit_observations(
                [
                    {
                        "source_engine": "openai_web_research",
                        "source_record_id": "approved-fallback-unavailable",
                        "source_url": source_url,
                        "origin_url": source_url,
                        "derived_from": [source_url],
                        "status": "unavailable",
                        "reason": diagnostic,
                        "claims": [],
                    }
                ],
                engine="openai_web_research",
            )
            outcomes.append("inconclusive")
            _approved_source_stage(
                context,
                source_url,
                "cited_fallback",
                "unavailable",
                diagnostic,
            )

    positive = any(value in {"found", "candidate"} for value in outcomes)
    outcome = "candidate" if positive else _runtime()._aggregate_outcomes(outcomes)
    return {
        "outcome": outcome,
        "outcome_counts": {
            value: outcomes.count(value) for value in sorted(set(outcomes))
        },
        "warning_count": warnings,
        "display_status": "completed_with_warnings" if warnings and positive else outcome,
        "diagnostic": (
            "One or more collection paths were unavailable; retained evidence and exact-source fallback results remain auditable."
            if warnings
            else None
        ),
        "retryable": False,
    }


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
