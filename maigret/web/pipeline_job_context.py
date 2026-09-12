"""Translate persisted P2 job intent into explicitly scoped pipeline inputs.

These helpers accept store records and server-owned context, never request JSON.
An approved claim is a research lead here; it does not become a final Persona
fact. Existing operation controls and combined-snapshot boundaries are retained.
"""

from __future__ import annotations

import copy
import hmac
import re
import uuid
from typing import Any, Mapping

from sqlalchemy import insert, select, update

from maigret.web.case_store import investigation_jobs, personas, utcnow
from maigret.web.investigation_input import (
    IDENTIFIER_TYPES,
    build_investigation_plan,
    normalize_profile_url,
)
from maigret.web.pipeline_contract import ENGINE_REGISTRY, canonical_digest
from maigret.web.pipeline_query import _bounded_budgets

CASE_RESEARCH_KINDS = frozenset({"affiliation", "case_fusion", "case_fusion_ai"})
_SOURCE_OPTIONS = (
    "enable_user_scanner_email",
    "enable_user_scanner_username",
    "enable_github_profile_enrichment",
    "enable_archived_url_evidence",
    "enable_domain_context",
    "enable_google_places_search",
    "allow_ai_context",
    "allow_user_scanner_vxtwitter",
)


def _spec(job):
    return copy.deepcopy((job.get("options") or {}).get("investigation_spec") or {})


def _scope(record, case_id, persona_id=None):
    if str(record.get("case_id", "")) != str(case_id):
        raise ValueError("Pipeline context belongs to a different case.")
    actual_subject = record.get("persona_id", record.get("subject_id"))
    if (
        persona_id is not None
        and actual_subject is not None
        and str(actual_subject) != str(persona_id)
    ):
        raise ValueError("Pipeline context belongs to a different subject.")


def resolve_case_subjects(store, job: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Use persisted subject bindings, creating one case-level shell if needed.

    Affiliation and combined-case operations get an explicitly bound organization
    or case research shell. They never fan every prior identifier into every
    Persona in the case. The job row lock makes shell creation idempotent.
    """
    specification = _spec(job)
    case_id = str(job["case_id"])
    with store.engine.begin() as connection:
        statement = select(investigation_jobs).where(
            investigation_jobs.c.id == job["job_id"]
        )
        if store.engine.dialect.name == "postgresql":
            statement = statement.with_for_update()
        stored_job = connection.execute(statement).mappings().first()
        if stored_job is None or str(stored_job["case_id"]) != case_id:
            raise ValueError("The pipeline job does not belong to this case.")
        # A restarted worker reads the binding committed by the prior attempt.
        stored_options = copy.deepcopy(stored_job["options"] or {})
        stored_spec = stored_options.get("investigation_spec") or {}
        if stored_spec.get("pipeline_subject_id"):
            specification["pipeline_subject_id"] = stored_spec["pipeline_subject_id"]
        rows = list(
            connection.execute(
                select(personas).where(personas.c.case_id == case_id)
            ).mappings()
        )
        by_id = {str(row["id"]): row for row in rows}
        bindings = [dict(item) for item in specification.get("persona_bindings") or []]
        if bindings:
            if any(str(item.get("persona_id")) not in by_id for item in bindings):
                raise ValueError("Stored subject bindings do not belong to this case.")
            return bindings
        target = (
            specification.get("target_persona_id")
            or specification.get("persona_id")
            or specification.get("pipeline_subject_id")
        )
        if not target and job.get("kind") == "case_fusion_ai":
            snapshot = (
                connection.execute(
                    select(investigation_jobs).where(
                        investigation_jobs.c.id == specification.get("snapshot_job_id")
                    )
                )
                .mappings()
                .first()
            )
            if snapshot is None or str(snapshot["case_id"]) != case_id:
                raise ValueError(
                    "Combined synthesis snapshot does not belong to this case."
                )
            target = ((snapshot["options"] or {}).get("investigation_spec") or {}).get(
                "pipeline_subject_id"
            )
        if not target and job.get("kind") in CASE_RESEARCH_KINDS:
            research_kind = (
                "case_fusion" if job.get("kind") == "case_fusion_ai" else job["kind"]
            )
            previous_specs = connection.execute(
                select(investigation_jobs.c.options)
                .where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.kind == research_kind,
                )
                .order_by(investigation_jobs.c.created_at.desc())
            ).scalars()
            target = next(
                (
                    candidate
                    for options in previous_specs
                    if (
                        candidate := (
                            (options or {}).get("investigation_spec") or {}
                        ).get("pipeline_subject_id")
                    )
                    in by_id
                ),
                None,
            )
        if target:
            if str(target) not in by_id:
                raise ValueError("The pipeline subject does not belong to this case.")
            row = by_id[str(target)]
        elif job.get("kind") in CASE_RESEARCH_KINDS:
            persona_id = str(uuid.uuid4())
            label = str(
                specification.get("affiliation_name")
                or specification.get("subject_label")
                or "Combined case research"
            )[:500]
            connection.execute(
                insert(personas).values(
                    id=persona_id,
                    case_id=case_id,
                    display_name=label,
                    created_at=utcnow(),
                )
            )
            stored_spec = dict(stored_spec, pipeline_subject_id=persona_id)
            stored_options["investigation_spec"] = stored_spec
            connection.execute(
                update(investigation_jobs)
                .where(investigation_jobs.c.id == job["job_id"])
                .values(options=stored_options)
            )
            row = {"id": persona_id, "display_name": label}
        elif len(rows) == 1:
            row = rows[0]
        else:
            raise ValueError("This query requires an explicit stored subject binding.")
        return [
            {
                "persona_id": str(row["id"]),
                "subject_label": row["display_name"],
                "subject_kind": (
                    "organization"
                    if job.get("kind") == "affiliation"
                    else (
                        "case_research"
                        if job.get("kind") in CASE_RESEARCH_KINDS
                        else "person"
                    )
                ),
                "usernames": list(job.get("usernames") or []),
                "identifiers": list(specification.get("identifiers") or []),
            }
        ]


def subject_spec(job: Mapping[str, Any], binding: Mapping[str, Any]) -> dict[str, Any]:
    """Restrict inputs, aliases and URL provenance to one stored binding."""
    specification = _spec(job)
    identifiers = list(
        binding.get("identifiers", specification.get("identifiers") or [])
    )
    if not identifiers:
        identifiers = [
            {"type": "username", "value": value}
            for value in binding.get("usernames") or []
        ]
    if job.get("kind") == "identity_enrichment":
        name = str(specification.get("confirmed_name") or "").strip()
        if not name:
            raise ValueError("Identity enrichment requires its approved name.")
        identifiers = [{"type": "full_name", "value": name}]
    if job.get("kind") in CASE_RESEARCH_KINDS:
        identifiers = []  # Case/organization references are added by approved_context.
    bound = {str(value).casefold() for value in binding.get("usernames") or []}
    specification["search_targets"] = [
        target
        for target in specification.get("search_targets") or []
        if str(target.get("value", "")).casefold() in bound
    ]
    keys = {(row["type"], row["value"]) for row in identifiers}
    specification["input_provenance"] = [
        row
        for row in specification.get("input_provenance") or []
        if (row.get("type"), row.get("value")) in keys
    ]
    specification["profile_url_usernames"] = {
        url: values
        for url, values in (specification.get("profile_url_usernames") or {}).items()
        if ("profile_url", url) in keys
    }
    specification["unresolved_profile_urls"] = [
        url
        for url in specification.get("unresolved_profile_urls") or []
        if ("profile_url", url) in keys
    ]
    specification["phone_context"] = {
        number: item
        for number, item in (specification.get("phone_context") or {}).items()
        if ("phone", number) in keys
    }
    specification.update(
        identifiers=identifiers,
        processing_mode="same_subject",
        target_persona_id=binding["persona_id"],
        subject_label=binding.get("subject_label", ""),
        persona_bindings=[dict(binding)],
    )
    if job.get("kind") == "affiliation":
        # This existing stored control authorizes the particular organization
        # research request; it does not enable AI for other case queries.
        specification["allow_ai_context"] = bool(
            specification.get("enable_public_web_research")
        )
    return specification


def _claim_text(claim):
    value = claim.get("value")
    if isinstance(value, dict):
        return str(
            value.get("name") or value.get("label") or value.get("value") or ""
        ).strip()
    return str(value or claim.get("display_value") or "").strip()


def _append_unique(values, value):
    value = str(value or "").strip()
    if value and value not in values:
        values.append(value)


def approved_context(
    store, job: Mapping[str, Any], binding: Mapping[str, Any], *, pipeline=None
) -> dict[str, Any]:
    """Merge existing approved claims and latest included pipeline decisions.

    Inclusion grants research targeting, not QC/final factual status. Latest
    corrections replace old targeting text; excluded/unresolved groups cannot
    silently activate a source. Efficient store streaming avoids loading whole
    workspaces, reports or all task histories.
    """
    case_id, persona_id = str(job["case_id"]), str(binding["persona_id"])
    specification = _spec(job)
    persona = store.get_persona(persona_id)
    if not persona:
        raise ValueError("The pipeline subject is unavailable.")
    _scope(persona, case_id)
    context: dict[str, Any] = {
        "case_id": case_id,
        "persona_id": persona_id,
        "subject_id": persona_id,
        "approved_full_names": [],
        "approved_organizations": [],
        "organization_names": [],
        "organizations": [],
        "organization_name": "",
        "official_websites": [],
        "official_website_organizations": {},
        "public_urls": [],
        "github_usernames": [],
        "research_questions": [],
        "approved_research_questions": [],
        "source_claim_ids": [],
        "collection_options": copy.deepcopy(job.get("options") or {}),
        "collection_controls": {
            key: bool(specification.get(key)) for key in _SOURCE_OPTIONS
        },
        "user_scanner_username_platforms": list(
            specification.get("user_scanner_username_platforms") or []
        ),
        "legal_jurisdiction": specification.get("legal_jurisdiction"),
        "selected_wikipedia_page_id": specification.get("selected_wikipedia_page_id"),
        "wikidata_entity_id": specification.get("wikidata_entity_id"),
    }
    legacy = [
        item
        for item in persona.get("claims", [])
        if item.get("review_status") == "approved"
    ]
    if pipeline is None:
        from maigret.web.pipeline_store import PipelineStore

        pipeline = PipelineStore(store)
    included = list(pipeline.iter_included_groups(case_id, persona_id))
    claims = list(legacy)
    for group in included:
        decision = group.get("latest_decision") or {}
        if decision.get("decision") != "include":
            continue
        normalized = dict(
            (decision.get("details") or {}).get("corrected_claim")
            or group.get("normalized")
            or {}
        )
        if group.get("kind") == "claim":
            claims.append(normalized)
        elif group.get("kind") == "account":
            if str(normalized.get("platform") or "").casefold() == "github":
                _append_unique(
                    context["github_usernames"],
                    normalized.get("username") or normalized.get("handle"),
                )
            _append_unique(
                context["public_urls"],
                normalized.get("canonical_url") or normalized.get("profile_url"),
            )
    for claim in claims:
        field = claim.get("field_name", claim.get("predicate"))
        value = _claim_text(claim)
        if field in {"full_name", "name"}:
            _append_unique(context["approved_full_names"], value)
        elif field in {"company", "organization", "affiliation"}:
            _append_unique(context["approved_organizations"], value)
        if claim.get("id"):
            _append_unique(context["source_claim_ids"], claim["id"])
    context["organization_names"] = list(context["approved_organizations"])
    context["organizations"] = list(context["approved_organizations"])
    if job.get("kind") == "identity_enrichment":
        _append_unique(
            context["approved_full_names"], specification.get("confirmed_name")
        )
        context["requested_engines"] = [
            "wikipedia_public_biography",
            "icij_offshore_leaks",
        ]
        context["organization_names"] = []
        context["public_urls"] = []
    elif job.get("kind") == "affiliation":
        name = str(specification.get("affiliation_name") or "").strip()
        if not name:
            raise ValueError("Affiliation research requires its organization name.")
        context["approved_organizations"] = [name]
        context["organization_names"] = [name]
        context["organizations"] = [name]
        context["organization_name"] = name
        context["requested_engines"] = [
            "wikidata_affiliation",
            "gleif_lei_registry",
            "fr_company_registry",
        ]
        website = specification.get("official_website")
        website = website.get("url") if isinstance(website, dict) else website
        if website and specification.get("enable_domain_context"):
            context["official_websites"] = [website]
            context["official_website_organizations"] = {website: name}
            context["official_website"] = website
        if specification.get("enable_domain_context"):
            context["requested_engines"].extend(
                ["cloudflare_dns_context", "official_website_public_content"]
            )
        if specification.get("enable_google_places_search"):
            context["requested_engines"].append("google_places_business_search")
        if specification.get("enable_public_web_research"):
            question = f"Research the public operating context of {name}, using cited public organization sources. Separate claims from evidence and identify uncertainty."
            context["research_questions"] = [question]
            context["approved_research_questions"] = [question]
            context["requested_engines"].append("ai_cited_research")
            context["collection_controls"]["allow_ai_context"] = True
    elif job.get("kind") == "case_fusion":
        source_ids = list(specification.get("source_case_ids") or [])
        if (
            len(set(source_ids)) < 2
            or specification.get("evidence_scope") != "approved_only"
        ):
            raise ValueError(
                "Combined evidence requires two selected cases and approved-only scope."
            )
        for source_id in source_ids:
            if source_id == case_id or not store.get_case(source_id):
                raise ValueError(
                    "A selected source case is unavailable or refers to the combined case itself."
                )
        context.update(
            source_case_ids=source_ids,
            evidence_scope="approved_only",
            approved_case_selection=True,
            case_references=[
                canonical_digest(
                    {
                        "source_case_ids": source_ids,
                        "purpose": specification.get("purpose", ""),
                    }
                )
            ],
            purpose=specification.get("purpose", ""),
            requested_engines=["case_fusion_snapshot"],
            organization_names=[],
            public_urls=[],
        )
    elif job.get("kind") == "case_fusion_ai":
        snapshot_job_id = str(specification.get("snapshot_job_id") or "")
        digest = str(specification.get("snapshot_sha256") or "").casefold()
        snapshot_job = store.get_job(snapshot_job_id)
        analysis = specification.get("analysis_context")
        if (
            not snapshot_job
            or snapshot_job.get("kind") != "case_fusion"
            or snapshot_job.get("status") != "completed"
        ):
            raise ValueError("Combined synthesis requires a completed case snapshot.")
        _scope(snapshot_job, case_id)
        stored_digest = str(
            (snapshot_job.get("snapshot") or {}).get("sha256") or ""
        ).casefold()
        if (
            not re.fullmatch(r"[0-9a-f]{64}", digest)
            or not hmac.compare_digest(digest, stored_digest)
            or not isinstance(analysis, dict)
            or analysis.get("snapshot_sha256") != digest
        ):
            raise ValueError(
                "Combined synthesis context does not match the immutable snapshot."
            )
        context.update(
            snapshot_references=[snapshot_job_id + ":" + digest],
            validated_snapshot_reference=True,
            snapshot_job_id=snapshot_job_id,
            snapshot_sha256=digest,
            analysis_context=copy.deepcopy(analysis),
            requested_engines=["case_fusion_synthesis"],
            organization_names=[],
            public_urls=[],
        )
    return context


def build_research_context(
    requirement: Mapping[str, Any], current_approved_context: Mapping[str, Any]
) -> dict[str, Any]:
    """Convert one persisted QC finding into a targeted same-case query plan.

    Caller must obtain ``requirement`` with PipelineStore.get_requirement scoped
    to the route case/persona, and context from approved_context. No input or
    alias is copied from the previous investigation. Empty research inputs stay
    visible as a manual research question; they do not become an invented scan.
    """
    if not isinstance(requirement, Mapping) or not isinstance(
        current_approved_context, Mapping
    ):
        raise ValueError(
            "Research planning requires persisted requirement and server context."
        )
    case_id = str(requirement.get("case_id") or "")
    persona_id = str(requirement.get("persona_id") or "")
    if not case_id or not persona_id or not requirement.get("id"):
        raise ValueError(
            "Research requirement must have persisted case, subject and requirement IDs."
        )
    if not current_approved_context.get("case_id") or not current_approved_context.get(
        "persona_id", current_approved_context.get("subject_id")
    ):
        raise ValueError(
            "Server research context requires explicit case and subject scope."
        )
    _scope(current_approved_context, case_id, persona_id)
    if requirement.get("status") not in {"open", "contested"}:
        raise ValueError("Only unresolved research requirements may create a query.")
    spec = requirement.get("spec")
    if not isinstance(spec, Mapping):
        raise ValueError("Research requirement needs a structured specification.")
    for key in ("case_id", "persona_id", "subject_id"):
        if spec.get(key) and str(spec[key]) != (
            case_id if key == "case_id" else persona_id
        ):
            raise ValueError("Research specification cannot change case or subject.")
    text = {}
    for key in ("question", "reason", "completion_criteria"):
        value = spec.get(key)
        if not isinstance(value, str) or not value.strip() or len(value) > 10000:
            raise ValueError(f"Research {key} must contain specific nonempty text.")
        text[key] = value.strip()
    run_budget = spec.get("request_budget", 3)
    if (
        isinstance(run_budget, bool)
        or not isinstance(run_budget, int)
        or not 1 <= run_budget <= 10
    ):
        raise ValueError("Research request budget must be between one and ten runs.")
    prior_requests = list(requirement.get("request_ids") or [])
    if len(set(prior_requests)) >= run_budget:
        raise ValueError("Research requirement run budget is exhausted.")
    engines = spec.get("engines", [])
    if (
        not isinstance(engines, list)
        or len(engines) > len(ENGINE_REGISTRY)
        or any(
            not isinstance(engine, str) or engine not in ENGINE_REGISTRY
            for engine in engines
        )
    ):
        raise ValueError(
            "Research engines must identify registered collection adapters."
        )
    if any(
        engine in {"case_fusion_snapshot", "case_fusion_synthesis"}
        for engine in engines
    ):
        raise ValueError(
            "Case combination requires the explicit case-selection operation."
        )
    raw_inputs = spec.get("inputs", [])
    if not isinstance(raw_inputs, list) or len(raw_inputs) > 24:
        raise ValueError("Research requires at most 24 typed inputs.")
    primary, supplementary = [], []
    countries = set()
    for item in raw_inputs:
        if (
            not isinstance(item, Mapping)
            or not isinstance(item.get("value"), str)
            or not item["value"].strip()
        ):
            raise ValueError(
                "Every research input requires an explicit type and value."
            )
        for key in ("case_id", "persona_id", "subject_id"):
            if item.get(key) and str(item[key]) != (
                case_id if key == "case_id" else persona_id
            ):
                raise ValueError("Research input cannot change case or subject.")
        if item.get("type") in IDENTIFIER_TYPES:
            primary.append({"type": item["type"], "value": item["value"]})
            if item["type"] == "phone" and item.get("country"):
                countries.add(str(item["country"]).upper())
        elif item.get("type") in {
            "organization",
            "official_website",
            "public_url",
            "place_id",
            "research_question",
            "external_evidence",
        }:
            supplementary.append(dict(item))
        else:
            raise ValueError("Unsupported typed research input.")
    if len(countries) > 1:
        raise ValueError(
            "National phone inputs with different countries require separate targeted requests."
        )
    controls = current_approved_context.get("collection_controls") or {}
    form = {
        "identifier_type": [item["type"] for item in primary],
        "identifier_value": [item["value"] for item in primary],
        "processing_mode": "same_subject",
    }
    if countries:
        form["phone_country"] = next(iter(countries))
    for key in _SOURCE_OPTIONS:
        if controls.get(key) is True:
            form[key] = "on"
    # A requirement selecting an adapter cannot fabricate consent for it. Keep
    # source controls from server context; unmatched controls yield Excluded.
    if controls.get("enable_user_scanner_email") and not any(
        item["type"] == "email" for item in primary
    ):
        form.pop("enable_user_scanner_email", None)
    if form.get("enable_user_scanner_username"):
        form["user_scanner_platform"] = list(
            current_approved_context.get("user_scanner_username_platforms") or []
        )
    investigation = (
        build_investigation_plan(form)
        if primary
        else {
            "schema_version": 2,
            "processing_mode": "same_subject",
            "identifiers": [],
            "search_targets": [],
            "profile_url_usernames": {},
            "input_provenance": [],
            "subject_groups": [],
            "phone_context": {},
            **{key: controls.get(key) is True for key in _SOURCE_OPTIONS},
        }
    )
    investigation.update({key: controls.get(key) is True for key in _SOURCE_OPTIONS})
    investigation["user_scanner_username_platforms"] = list(
        current_approved_context.get("user_scanner_username_platforms") or []
    )
    context = copy.deepcopy(dict(current_approved_context))
    # Remove all old target-bearing context; retain only reviewed prerequisite
    # lists, per-source permissions and exact target qualifications below.
    for key in (
        "organization_names",
        "organizations",
        "official_websites",
        "public_urls",
        "github_usernames",
        "research_questions",
        "selected_place_ids",
        "operator_messages",
        "external_evidence_ids",
        "case_references",
        "snapshot_references",
    ):
        context[key] = []
    context["organization_name"] = ""
    context["official_website_organizations"] = {}
    supplementary_keys = {
        "organization": "organization_names",
        "official_website": "official_websites",
        "public_url": "public_urls",
        "place_id": "selected_place_ids",
        "research_question": "research_questions",
        "external_evidence": "external_evidence_ids",
    }
    for item in supplementary:
        value = item["value"].strip()
        if item["type"] in {"official_website", "public_url"}:
            value = normalize_profile_url(value)
        context[supplementary_keys[item["type"]]].append(value)
        if item["type"] == "official_website":
            approved_binding = (
                current_approved_context.get("official_website_organizations") or {}
            ).get(value)
            if approved_binding:
                context["official_website_organizations"][value] = approved_binding
    context["organizations"] = list(context["organization_names"])
    scoped_usernames = {
        item["value"] for item in investigation.get("search_targets") or []
    }
    context["github_usernames"] = [
        value
        for value in current_approved_context.get("github_usernames") or []
        if value in scoped_usernames
    ]
    context["official_website"] = None
    context["wikidata_entity_id"] = None
    context["selected_wikipedia_page_id"] = None
    context["legal_jurisdiction"] = None
    if len(context["organization_names"]) == 1:
        context["organization_name"] = context["organization_names"][0]
        if context["organization_name"] == current_approved_context.get(
            "organization_name"
        ):
            for key in ("legal_jurisdiction", "wikidata_entity_id"):
                context[key] = current_approved_context.get(key)
            website = current_approved_context.get("official_website")
            if website in context["official_websites"]:
                context["official_website"] = website
    context["requested_engines"] = list(dict.fromkeys(engines)) if engines else None
    if not primary and not supplementary:
        context["research_questions"] = [text["question"]]
        # Only an explicitly selected research adapter can consume the exact
        # QC question itself. Otherwise missing targets stay a manual task.
        if "ai_cited_research" not in engines:
            context["requested_engines"] = []
    if "ai_cited_research" in engines and controls.get("allow_ai_context") is True:
        context["approved_research_questions"] = list(context["research_questions"])
    parent = current_approved_context.get("parent_request")
    depth, parent_id = 1, None
    if parent:
        _scope(parent, case_id, persona_id)
        if not parent.get("id") or not isinstance(parent.get("depth"), int):
            raise ValueError("Research parent must be a persisted scoped request.")
        depth, parent_id = parent["depth"] + 1, parent["id"]
    budgets = _bounded_budgets(spec.get("collection_budget"))
    origin = {
        "case_id": case_id,
        "subject_id": persona_id,
        "parent_request_id": parent_id,
        "qc_id": requirement.get("qc_id"),
        "version_id": requirement.get("version_id"),
        "requirement_ids": [requirement["id"]],
        "target_group_id": spec.get("target_group_id"),
        "depth": depth,
        "question": text["question"],
        "reason": text["reason"],
        "completion_criteria": text["completion_criteria"],
        "request_run_budget": run_budget,
    }
    investigation.update(
        target_persona_id=persona_id,
        requirement_ids=[requirement["id"]],
        parent_request_id=parent_id,
        research_origin=origin,
        subject_label=text["question"][:500],
    )
    return {
        "investigation_spec": investigation,
        "context": context,
        "origin": origin,
        "budgets": budgets,
        "case_id": case_id,
        "persona_id": persona_id,
        "requirement_ids": [requirement["id"]],
        "parent_request_id": parent_id,
        "completion_criteria": text["completion_criteria"],
    }
