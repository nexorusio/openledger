import ast
import json
from pathlib import Path

import pytest

from maigret.web.investigation_input import build_investigation_plan, public_ai_context
from maigret.web.pipeline_contract import (
    ENGINE_CAPABILITIES,
    ENGINE_REGISTRY,
    PIPELINE_ID,
    validate_task,
)
from maigret.web.pipeline_query import build_query_plan, revalidate_query_plan

SOURCES = {
    "discovery_enabled": True,
    "maigret_enabled": True,
    "scanner_enabled": True,
    "scanner_available": True,
    "enrichment_enabled": True,
    "native_search": {"enabled": True, "provider": "searxng"},
    "public_search": {"enabled": True, "provider": "searxng"},
    "google_places_enabled": True,
    "ai_enabled": True,
}


def plan_for(kinds, values, **options):
    return build_investigation_plan(
        {"identifier_type": kinds, "identifier_value": values, **options}
    )


def query_for(plan, **kwargs):
    return build_query_plan(
        plan,
        case_id="case-a",
        subject_id="subject-a",
        request_id="request-a",
        source_status=SOURCES,
        **kwargs
    )


@pytest.mark.parametrize(
    "kind,value,engines",
    [
        ("username", "test_person", {"maigret", "native_profile_search"}),
        ("full_name", "Test Person", {"native_profile_search", "public_exact_match"}),
        ("email", "person+research@example.test", {"public_exact_match"}),
        ("phone", "+628123456789", {"public_exact_match"}),
    ],
)
def test_each_input_routes_independently_without_spurious_username(
    kind, value, engines
):
    input_plan = plan_for([kind], [value])
    plan = query_for(input_plan)
    active = [task for task in plan["tasks"] if task["route_state"] == "active"]
    assert {task["engine_id"] for task in active} == engines
    assert len(input_plan["subject_groups"]) == 1
    if kind != "username":
        assert input_plan["search_targets"] == []
        assert not any(task["engine_id"] == "maigret" for task in active)


def test_multiple_email_tasks_share_case_and_subject_but_keep_per_address_input_ids():
    raw = plan_for(
        ["email", "email"],
        ["first@example.test", "second@example.test"],
        enable_user_scanner_email="on",
    )
    plan = query_for(raw)
    tasks = [
        task for task in plan["tasks"] if task["engine_id"] == "user_scanner_email"
    ]
    assert len(tasks) == 2
    assert all(task["route_state"] == "active" for task in tasks)
    assert len({task["input_id"] for task in tasks}) == 2
    assert {task["subject_id"] for task in tasks} == {"subject-a"}
    assert {task["case_id"] for task in tasks} == {"case-a"}


def test_every_existing_adapter_and_catalog_source_has_a_route_contract():
    root = Path(__file__).resolve().parents[1]
    module = ast.parse((root / "maigret/web/collector_adapters.py").read_text())
    executable_adapters = {
        node.name
        for node in module.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name.startswith("run_")
    }
    registered = {
        engine.execution_key
        for engine in ENGINE_CAPABILITIES
        if engine.module.endswith("collector_adapters")
    }
    assert executable_adapters == registered
    catalog = json.loads((root / "config/osint-sources.json").read_text())
    assert {row["id"] for row in catalog["sources"]} <= set(ENGINE_REGISTRY)
    query = query_for(plan_for(["username"], ["test_person"]))
    assert {task["engine_id"] for task in query["tasks"]} == {
        key
        for key, capability in ENGINE_REGISTRY.items()
        if capability.trigger != "machine"
    }
    assert all(task["reason"] for task in query["tasks"])


def test_selected_and_disabled_platforms_have_explicit_separate_outcomes():
    raw = plan_for(
        ["username"],
        ["test_person"],
        enable_user_scanner_username="on",
        user_scanner_platform=["instagram", "tiktok"],
    )
    sources = {
        **SOURCES,
        "engines": {
            "native_profile_search:instagram": {
                "enabled": False,
                "reason": "Provider blocked this route.",
            }
        },
    }
    plan = build_query_plan(raw, source_status=sources)
    native = {
        task["platform"]: task
        for task in plan["tasks"]
        if task["engine_id"] == "native_profile_search"
    }
    assert set(native) == {"facebook", "instagram", "threads", "tiktok", "x"}
    assert native["instagram"]["route_state"] == "unavailable"
    assert native["tiktok"]["route_state"] == "active"
    scanner = {
        task["platform"]: task
        for task in plan["tasks"]
        if task["engine_id"] == "user_scanner_username"
    }
    assert scanner["instagram"]["route_state"] == "active"
    assert scanner["facebook"]["route_state"] == "excluded"
    assert all("outcome" not in task for task in native.values())


def test_same_handle_and_url_preserve_all_three_raw_inputs_without_triplicate_tasks():
    url = "https://instagram.com/Test_Person/"
    raw = plan_for(["username"] * 3, ["Test_Person", "@Test_Person", url])
    query = query_for(raw)
    assert len(raw["input_provenance"]) == 3
    assert len(raw["subject_groups"]) == 1
    maigret = [task for task in query["tasks"] if task["engine_id"] == "maigret"]
    assert len(maigret) == 1
    username = next(item for item in query["inputs"] if item["type"] == "username")
    assert {row["raw_value"] for row in username["provenance"]} == {
        "Test_Person",
        "@Test_Person",
    }
    assert any(row["value"] == url for row in username["derived_from"])


@pytest.mark.parametrize(
    "url",
    [
        "https://unknown.example.test/news/person",
        "https://facebook.com/123456789",
        "https://instagram.com/p/abc",
        "https://github.com/alice/project",
    ],
)
def test_unsupported_or_nonprofile_url_is_retained_without_guessed_username(url):
    raw = plan_for(["username"], [url], allow_ai_context="on")
    assert raw["search_targets"] == []
    assert raw["unresolved_profile_urls"] == [url]
    assert public_ai_context(raw)["supplied_profile_urls"] == [
        {"url": url, "usernames": []}
    ]
    assert query_for(raw)["research_needed"] is True


def test_numeric_typed_correction_is_honored_and_ambiguous_phone_needs_country():
    username = query_for(plan_for(["username"], ["8123456789"]))
    assert any(
        task["engine_id"] == "maigret" and task["route_state"] == "active"
        for task in username["tasks"]
    )
    phone = query_for(plan_for(["phone"], ["8123456789"]))
    route = next(
        task for task in phone["tasks"] if task["engine_id"] == "public_exact_match"
    )
    assert route["route_state"] == "conditional"
    assert "phone_country_context" in route["prerequisites"]
    resolved = query_for(plan_for(["phone"], ["08123456789"], phone_country="ID"))
    route = next(
        task for task in resolved["tasks"] if task["engine_id"] == "public_exact_match"
    )
    assert route["route_state"] == "active"
    assert (
        route["input_value"] == "08123456789"
    )  # no guessed trunk/calling-code rewrite


def test_name_sources_are_conditional_until_name_is_operator_approved():
    raw = plan_for(["full_name"], ["Test Person"])
    before = query_for(raw)
    after = query_for(raw, context={"approved_full_names": ["Test Person"]})
    for engine in ("wikipedia_public_biography", "icij_offshore_leaks"):
        assert (
            next(task for task in before["tasks"] if task["engine_id"] == engine)[
                "route_state"
            ]
            == "conditional"
        )
        assert (
            next(task for task in after["tasks"] if task["engine_id"] == engine)[
                "route_state"
            ]
            == "active"
        )


def test_plan_is_reproducible_and_source_drift_requires_revalidation():
    raw = plan_for(["username"], ["test_person"])
    before = query_for(raw)
    assert before == query_for(raw)
    drift = revalidate_query_plan(
        before, raw, source_status={**SOURCES, "maigret_enabled": False}
    )
    assert drift["changed"] is True
    assert any(row["current_state"] == "unavailable" for row in drift["changes"])
    bad = dict(before, pipeline_id="old-pipeline")
    with pytest.raises(ValueError, match="different pipeline"):
        revalidate_query_plan(bad, raw)


def test_followup_retains_rejection_lineage_and_scope_and_budget():
    raw = plan_for(["email"], ["test@example.test"])
    origin = {
        "parent_request_id": "earlier",
        "qc_id": "qc-rejected",
        "version_id": "version-1",
        "requirement_ids": ["requirement-a"],
        "depth": 1,
    }
    query = query_for(
        raw,
        origin=origin,
        context={"requested_engines": ["public_exact_match"]},
        budgets={"max_requests": 1},
    )
    active = [task for task in query["tasks"] if task["route_state"] == "active"]
    assert len(active) == 1
    assert active[0]["origin"] == origin
    assert query["estimated_request_count"] == 1
    assert query_for(raw, origin={**origin, "depth": 4})["research_needed"] is True
    assert query_for(raw, budgets={"max_requests": 0})["research_needed"] is True
    with pytest.raises(ValueError, match="case or subject"):
        query_for(raw, origin={"case_id": "unrelated-case"})


def test_full_maigret_retains_all_selected_sources_and_separate_request_timeout():
    raw = plan_for(["username"], ["test_person"])
    query = query_for(
        raw,
        context={
            "collection_options": {"all_sites": True, "timeout": 12},
            "selected_maigret_sites": [str(i) for i in range(2500)],
        },
    )
    route = next(task for task in query["tasks"] if task["engine_id"] == "maigret")
    assert route["timeout_seconds"] == 1800
    assert route["request_timeout_seconds"] == 12
    assert route["request_budget"] == 2500
    assert route["budget_estimate"] is False
    assert route["route_state"] == "active"
    assert query["tasks"].index(route) > 5


def test_disabled_provider_creates_research_needed_case_without_previous_pipeline_route():
    raw = plan_for(["phone"], ["+628123456789"])
    query = build_query_plan(
        raw,
        source_status={
            **SOURCES,
            "public_search": {"enabled": False, "reason": "No provider"},
        },
    )
    assert query["pipeline_id"] == PIPELINE_ID
    assert query["research_needed"] is True
    task = next(
        task for task in query["tasks"] if task["engine_id"] == "public_exact_match"
    )
    assert task["route_state"] == "unavailable"
    assert task["reason"] == "No provider"
    assert "fallback" not in query


def test_task_contract_rejects_incompatible_dispatch_and_pipeline():
    query = query_for(plan_for(["email"], ["test@example.test"]))
    task = next(
        task for task in query["tasks"] if task["engine_id"] == "public_exact_match"
    )
    for field, value in (
        ("pipeline_id", "old"),
        ("execution_key", "maigret_search"),
        ("input_type", "username"),
    ):
        with pytest.raises(ValueError):
            validate_task({**task, field: value})
