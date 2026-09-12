import copy

import pytest

from maigret.web.case_store import CaseStore
from maigret.web.pipeline_job_context import (
    approved_context,
    build_research_context,
    resolve_case_subjects,
    subject_spec,
)
from maigret.web.pipeline_query import build_query_plan


class Pipeline:
    def __init__(self, groups=()):
        self.groups = groups

    def iter_included_groups(self, case_id, persona_id):
        return iter(self.groups)


class Store:
    def __init__(self, claims=(), jobs=None):
        self.claims = claims
        self.jobs = jobs or {}

    def get_persona(self, persona_id):
        return {"id": persona_id, "case_id": "case-a", "claims": self.claims}

    def get_case(self, case_id):
        return {"case_id": case_id}

    def get_job(self, job_id):
        return self.jobs.get(job_id)


def job(kind="live", **spec):
    return {
        "job_id": "job-a",
        "case_id": "case-a",
        "kind": kind,
        "usernames": [],
        "options": {"investigation_spec": spec},
    }


BINDING = {
    "persona_id": "subject-a",
    "subject_label": "Case subject",
    "identifiers": [],
    "usernames": [],
}
SOURCES = {
    "discovery_enabled": True,
    "enrichment_enabled": True,
    "native_search": {"enabled": True},
    "public_search": {"enabled": True},
    "google_places_enabled": True,
    "ai_enabled": True,
}


def active_engines(spec, context):
    plan = build_query_plan(
        spec,
        case_id="case-a",
        subject_id="subject-a",
        context=context,
        source_status=SOURCES,
    )
    return {
        task["engine_id"] for task in plan["tasks"] if task["route_state"] == "active"
    }


def requirement(**updates):
    result = {
        "id": "requirement-a",
        "case_id": "case-a",
        "persona_id": "subject-a",
        "status": "open",
        "version_id": "failed-version",
        "qc_id": "failed-qc",
        "request_ids": [],
        "spec": {
            "question": "Locate a cited account link",
            "reason": "Ownership is unresolved",
            "completion_criteria": "Obtain an independent public link to the account",
            "inputs": [{"type": "username", "value": "target_handle"}],
            "engines": ["native_profile_search"],
            "request_budget": 3,
        },
    }
    result["spec"].update(updates)
    return result


def context(**updates):
    result = {
        "case_id": "case-a",
        "persona_id": "subject-a",
        "approved_full_names": ["Earlier Name"],
        "approved_organizations": ["Earlier Company"],
        "organization_names": ["Earlier Company"],
        "organizations": ["Earlier Company"],
        "organization_name": "Earlier Company",
        "official_websites": ["https://earlier.example.test"],
        "official_website": "https://earlier.example.test",
        "legal_jurisdiction": {"code": "FR"},
        "collection_controls": {
            "enable_user_scanner_email": True,
            "enable_user_scanner_username": True,
        },
        "user_scanner_username_platforms": ["instagram", "tiktok"],
        "parent_request": {
            "id": "parent-a",
            "case_id": "case-a",
            "persona_id": "subject-a",
            "depth": 1,
        },
    }
    result.update(updates)
    return result


def test_qc_followup_uses_only_requirement_inputs_and_keeps_every_origin_identifier():
    req, current = requirement(), context()
    before = copy.deepcopy(current)
    result = build_research_context(req, current)
    assert result["investigation_spec"]["identifiers"] == [
        {"type": "username", "value": "target_handle"}
    ]
    assert result["investigation_spec"]["search_targets"][0]["value"] == "target_handle"
    assert result["context"]["organization_names"] == []
    assert result["context"]["official_websites"] == []
    assert result["context"]["legal_jurisdiction"] is None
    assert result["origin"]["qc_id"] == "failed-qc"
    assert result["origin"]["version_id"] == "failed-version"
    assert result["origin"]["parent_request_id"] == "parent-a"
    assert result["origin"]["depth"] == 2
    assert result["origin"]["completion_criteria"] == req["spec"]["completion_criteria"]
    assert active_engines(result["investigation_spec"], result["context"]) == {
        "native_profile_search"
    }
    assert current == before


@pytest.mark.parametrize(
    "mutate",
    [
        lambda req, ctx: req["spec"].update(completion_criteria=""),
        lambda req, ctx: req.update(status="resolved"),
        lambda req, ctx: ctx.update(case_id="other-case"),
        lambda req, ctx: ctx.update(persona_id="other-subject"),
        lambda req, ctx: ctx["parent_request"].update(persona_id="other-subject"),
        lambda req, ctx: req["spec"]["inputs"][0].update(case_id="other-case"),
        lambda req, ctx: req["spec"].update(engines=["unknown-engine"]),
        lambda req, ctx: req.update(request_ids=["one", "two", "three"]),
    ],
)
def test_invalid_or_completed_research_cannot_be_dispatched(mutate):
    req, current = requirement(), context()
    mutate(req, current)
    with pytest.raises(ValueError):
        build_research_context(req, current)


def test_missing_research_inputs_stay_manual_without_replaying_previous_subject():
    result = build_research_context(requirement(inputs=[], engines=[]), context())
    assert result["investigation_spec"]["identifiers"] == []
    assert result["investigation_spec"]["search_targets"] == []
    plan = build_query_plan(
        result["investigation_spec"], context=result["context"], source_status=SOURCES
    )
    assert plan["research_needed"] is True
    assert len(plan["inputs"]) == 1
    assert plan["inputs"][0]["type"] == "research_question"


def test_explicit_ai_requirement_uses_literal_question_only_with_existing_consent():
    req = requirement(inputs=[], engines=["ai_cited_research"])
    yes = build_research_context(
        req, context(collection_controls={"allow_ai_context": True})
    )
    no = build_research_context(
        req, context(collection_controls={"allow_ai_context": False})
    )
    assert active_engines(yes["investigation_spec"], yes["context"]) == {
        "ai_cited_research"
    }
    assert active_engines(no["investigation_spec"], no["context"]) == set()


def test_phone_correction_and_multi_email_targets_remain_with_same_case():
    req = requirement(
        inputs=[
            {"type": "phone", "value": "08123456789", "country": "ID"},
            {"type": "email", "value": "one@example.test"},
            {"type": "email", "value": "two@example.test"},
        ],
        engines=["public_exact_match", "user_scanner_email"],
    )
    result = build_research_context(req, context())
    assert result["investigation_spec"]["search_targets"] == []
    assert (
        result["investigation_spec"]["phone_context"]["08123456789"]["country"] == "ID"
    )
    assert result["case_id"] == "case-a" and result["persona_id"] == "subject-a"


def test_identity_enrichment_remains_scoped_to_existing_two_name_sources():
    queued = job(
        "identity_enrichment",
        persona_id="subject-a",
        confirmed_name="Approved Name",
        selected_wikipedia_page_id="123",
    )
    spec = subject_spec(queued, BINDING)
    extra = approved_context(Store(), queued, BINDING, pipeline=Pipeline())
    assert spec["identifiers"] == [{"type": "full_name", "value": "Approved Name"}]
    assert active_engines(spec, extra) == {
        "wikipedia_public_biography",
        "icij_offshore_leaks",
    }
    assert extra["selected_wikipedia_page_id"] == "123"


def test_affiliation_flags_control_exact_existing_source_families():
    queued = job(
        "affiliation",
        affiliation_name="Example Company",
        legal_jurisdiction={"code": "FR"},
        enable_domain_context=True,
        official_website={"url": "https://company.example.test"},
        enable_google_places_search=False,
        enable_public_web_research=False,
    )
    spec = subject_spec(queued, BINDING)
    extra = approved_context(Store(), queued, BINDING, pipeline=Pipeline())
    assert active_engines(spec, extra) == {
        "wikidata_affiliation",
        "gleif_lei_registry",
        "fr_company_registry",
        "cloudflare_dns_context",
        "official_website_public_content",
    }
    assert extra["official_website_organizations"] == {
        "https://company.example.test": "Example Company"
    }
    queued["options"]["investigation_spec"].update(
        enable_google_places_search=True, enable_public_web_research=True
    )
    assert active_engines(
        subject_spec(queued, BINDING),
        approved_context(Store(), queued, BINDING, pipeline=Pipeline()),
    ) >= {"google_places_business_search", "ai_cited_research"}


def test_new_included_decisions_and_corrections_supply_targeting_context():
    groups = [
        {
            "kind": "claim",
            "normalized": {"predicate": "full_name", "value": "Wrong Name"},
            "latest_decision": {
                "decision": "include",
                "details": {
                    "corrected_claim": {
                        "predicate": "full_name",
                        "value": "Correct Name",
                    }
                },
            },
        },
        {
            "kind": "claim",
            "normalized": {"predicate": "company", "value": "Excluded Company"},
            "latest_decision": {"decision": "exclude"},
        },
        {
            "kind": "claim",
            "normalized": {"predicate": "company", "value": "Included Company"},
            "latest_decision": {"decision": "include"},
        },
    ]
    result = approved_context(
        Store(
            [
                {
                    "field_name": "full_name",
                    "display_value": "Legacy Name",
                    "review_status": "approved",
                }
            ]
        ),
        job(),
        BINDING,
        pipeline=Pipeline(groups),
    )
    assert result["approved_full_names"] == ["Legacy Name", "Correct Name"]
    assert result["approved_organizations"] == ["Included Company"]


def test_subject_spec_never_copies_another_independent_subjects_profile_url_or_alias():
    queued = job(
        identifiers=[
            {"type": "username", "value": "alice"},
            {"type": "username", "value": "bob"},
        ],
        search_targets=[{"value": "alice"}, {"value": "bob"}],
        profile_url_usernames={"https://github.com/bob": ["bob"]},
    )
    binding = {
        **BINDING,
        "identifiers": [{"type": "username", "value": "alice"}],
        "usernames": ["alice"],
    }
    result = subject_spec(queued, binding)
    assert result["identifiers"] == [{"type": "username", "value": "alice"}]
    assert result["search_targets"] == [{"value": "alice"}]
    assert result["profile_url_usernames"] == {}


def test_combined_snapshot_and_synthesis_have_explicit_local_evidence_routes():
    queued = job(
        "case_fusion",
        source_case_ids=["source-a", "source-b"],
        purpose="Compare approved evidence",
        evidence_scope="approved_only",
    )
    extra = approved_context(Store(), queued, BINDING, pipeline=Pipeline())
    assert active_engines(subject_spec(queued, BINDING), extra) == {
        "case_fusion_snapshot"
    }
    digest = "a" * 64
    synthesis = job(
        "case_fusion_ai",
        snapshot_job_id="snapshot-a",
        snapshot_sha256=digest,
        analysis_context={"snapshot_sha256": digest},
    )
    store = Store(
        jobs={
            "snapshot-a": {
                "case_id": "case-a",
                "kind": "case_fusion",
                "status": "completed",
                "snapshot": {"sha256": digest},
            }
        }
    )
    extra = approved_context(store, synthesis, BINDING, pipeline=Pipeline())
    assert active_engines(subject_spec(synthesis, BINDING), extra) == {
        "case_fusion_synthesis"
    }
    synthesis["options"]["investigation_spec"]["snapshot_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="immutable snapshot"):
        approved_context(store, synthesis, BINDING, pipeline=Pipeline())


def test_affiliation_research_shell_creation_is_idempotent_against_actual_saved_job(
    tmp_path,
):
    store = CaseStore(f"sqlite:///{tmp_path / 'case.db'}", create_schema=True)
    try:
        job_id = store.create_affiliation_investigation(
            "Example Company",
            jurisdiction="FR",
            enable_domain_context=True,
            official_website="https://example.test",
        )
        queued = store.get_job(job_id)
        first = resolve_case_subjects(store, queued)
        second = resolve_case_subjects(store, queued)
        assert first[0]["persona_id"] == second[0]["persona_id"]
        assert first[0]["subject_kind"] == "organization"
        assert (
            store.get_job(job_id)["options"]["investigation_spec"][
                "pipeline_subject_id"
            ]
            == first[0]["persona_id"]
        )
    finally:
        store.dispose()
