# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import json
from pathlib import Path

import pytest

from maigret.web.investigation_input import (
    InvestigationInputError,
    ROUTE_PLAN_POLICY_VERSION,
    ROUTE_PLAN_SCHEMA_VERSION,
    TOKEN_SCHEMA_VERSION,
    UNIFIED_INVESTIGATION_SCHEMA_VERSION,
    UNIFIED_INPUT_CONTRACT,
    build_unified_investigation_plan,
    classify_investigation_token,
    classify_investigation_tokens,
    finalize_investigation_route_plan,
    investigation_has_effective_collection_route,
    normalize_public_url,
)
from maigret.web.profile_discovery_policy import (
    ProfileDiscoveryPolicyError,
    govern_profile_discovery_options,
)

ENABLED_FLAGS = {
    "profile_discovery_enabled": True,
    "focused_mode_enabled": True,
    "exhaustive_mode_enabled": True,
    "maigret_enabled": True,
    "user_scanner_enabled": True,
    "enrichment_providers_enabled": True,
    "provider_circuit_breakers_enabled": True,
    "governed_pivots_enabled": False,
    "search_first_enabled": True,
}


def _route_names(plan, key):
    return [item["route"] for item in plan["route_plan"][key]]


def test_classifier_uses_the_fixed_server_precedence():
    assert classify_investigation_token("Alice@Example.com")["type"] == "email"
    assert classify_investigation_token("+62 812-3456-7890")["type"] == "phone"
    assert classify_investigation_token("@Alice")["type"] == "social_handle"
    assert classify_investigation_token("Alice Example")["type"] == "full_name"
    assert classify_investigation_token("alice")["type"] == "username"

    profile = classify_investigation_token(
        "HTTPS://Instagram.com/Alice/#profile",
        profile_url_resolver=lambda _url: {"Alice": "username"},
    )
    assert profile["type"] == "profile_url"
    assert profile["value"] == "https://instagram.com/Alice/"
    assert profile["account_targets"] == ["Alice"]


def test_indonesian_mobile_numbers_are_predicted_as_phone_with_user_override():
    local = classify_investigation_token("0822335763")
    country = classify_investigation_token("62822335763")

    assert local["type"] == "phone"
    assert local["value"] == "+62822335763"
    assert local["ambiguous_types"] == ["phone", "username"]
    assert country["type"] == "phone"
    assert country["value"] == "+62822335763"

    overridden = classify_investigation_token("0822335763", type_override="username")
    assert overridden["predicted_type"] == "phone"
    assert overridden["type"] == "username"
    assert overridden["value"] == "0822335763"
    assert overridden["type_source"] == "analyst_override"
    assert overridden["context_only"] is False


def test_type_override_is_server_validated_and_preserves_original_input():
    mononym = classify_investigation_token("Madonna", type_override="full_name")
    assert mononym["input"] == "Madonna"
    assert mononym["predicted_type"] == "username"
    assert mononym["type"] == "full_name"

    with pytest.raises(InvestigationInputError, match="supported investigation"):
        classify_investigation_token("alice", type_override="arbitrary_fetch")

    with pytest.raises(InvestigationInputError, match="supported public account"):
        classify_investigation_token(
            "https://public.example.com/alice",
            type_override="profile_url",
            profile_url_resolver=lambda _url: {},
        )


def test_parallel_type_overrides_must_remain_aligned():
    with pytest.raises(InvestigationInputError, match="remain aligned"):
        classify_investigation_tokens(
            ["Alice Example", "0822335763"],
            type_overrides=["full_name"],
        )

    tokens = classify_investigation_tokens(
        ["Alice Example", "0822335763"],
        type_overrides=["", "username"],
    )
    assert [token["type"] for token in tokens] == ["full_name", "username"]


def test_nfkc_normalization_and_type_specific_duplicate_keys_are_bounded():
    tokens = classify_investigation_tokens(["Ａlice", "Alice", "@Alice", "@alice"])

    assert [(item["type"], item["value"]) for item in tokens] == [
        ("username", "Alice"),
        ("social_handle", "Alice"),
    ]
    assert tokens[0]["duplicate_key"].startswith("username:")
    assert tokens[1]["duplicate_key"].startswith("social_handle:")
    assert tokens[0]["duplicate_key"] != tokens[1]["duplicate_key"]

    with pytest.raises(InvestigationInputError, match="2000 characters"):
        classify_investigation_token("a" * 2001)


def test_generic_public_url_is_context_and_never_uses_a_final_path_fallback():
    token = classify_investigation_token(
        "https://Public.Example.com/people/alice?view=1#bio",
        profile_url_resolver=lambda _url: {},
    )

    assert token == {
        "schema_version": 1,
        "type": "public_url",
        "value": "https://public.example.com/people/alice?view=1",
        "duplicate_key": token["duplicate_key"],
        "context_only": True,
        "input": "https://Public.Example.com/people/alice?view=1#bio",
        "predicted_type": "public_url",
        "type_source": "automatic",
        "ambiguous_types": [],
    }
    assert "account_targets" not in token


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/profile",
        "http://127.1/profile",
        "http://10.0.0.1/profile",
        "http://[::1]/profile",
        "https://localhost/profile",
        "https://service.internal/profile",
        "https://example.test/profile",
        "https://operator:secret@example.com/profile",
        "https://example.com:8443/profile",
    ],
)
def test_public_url_contract_blocks_private_local_and_credential_targets(url):
    with pytest.raises(InvestigationInputError):
        normalize_public_url(url)


def test_unified_plan_is_same_subject_and_preserves_explicit_existing_collectors():
    plan = build_unified_investigation_plan(
        {
            "investigation_token": [
                "Alice Example",
                "@alice",
                "+62 812-3456-7890",
                "https://public.example.com/reference/alice",
            ],
            "mode": "quick",
            "search_likely_username_aliases": "on",
            # AI and source filters remain outside the ordinary contract, while
            # established opt-in collectors are preserved explicitly.
            "allow_ai_context": "on",
            "enable_github_profile_enrichment": "on",
            "tags": ["social"],
        },
        profile_url_resolver=lambda _url: {},
    )

    assert plan["schema_version"] == 2
    assert plan["input_contract"] == UNIFIED_INPUT_CONTRACT
    assert plan["processing_mode"] == "same_subject"
    assert plan["requested_mode"] == "quick"
    assert plan["execution_mode"] == "focused"
    assert plan["allow_ai_context"] is False
    assert plan["enable_github_profile_enrichment"] is True
    assert plan["enable_archived_url_evidence"] is False
    assert plan["tags"] == []
    assert plan["excluded_tags"] == []
    assert plan["subject_label"] == "Alice Example"
    assert any(item["source_type"] == "ranked_alias" for item in plan["search_targets"])
    assert {item["type"] for item in plan["tokens"]} == {
        "full_name",
        "social_handle",
        "phone",
        "public_url",
    }


def test_unified_plan_applies_exact_analyst_selected_aliases():
    automatic = build_unified_investigation_plan(
        {
            "investigation_token": ["Ferry Irwandi"],
            "search_likely_username_aliases": "on",
        }
    )
    candidate_values = [
        candidate["value"] for candidate in automatic["alias_candidates"]
    ]
    selected = [candidate_values[1], candidate_values[3]]

    reviewed = build_unified_investigation_plan(
        {
            "investigation_token": ["Ferry Irwandi"],
            "search_likely_username_aliases": "on",
            "alias_candidates_present": "1",
            "selected_alias": selected,
        }
    )

    assert [
        candidate["value"]
        for candidate in reviewed["alias_candidates"]
        if candidate["selected"]
    ] == selected
    assert [
        target["value"]
        for target in reviewed["search_targets"]
        if target["source_type"] == "ranked_alias"
    ] == selected

    with pytest.raises(InvestigationInputError, match="displayed server-ranked"):
        build_unified_investigation_plan(
            {
                "investigation_token": ["Ferry Irwandi"],
                "search_likely_username_aliases": "on",
                "alias_candidates_present": "1",
                "selected_alias": ["invented-alias"],
            }
        )


@pytest.mark.parametrize(
    ("submitted", "requested", "canonical", "seconds"),
    [
        ("quick", "quick", "focused", 600),
        ("fast", "quick", "focused", 600),
        ("focused", "quick", "focused", 600),
        ("full", "full", "exhaustive", 1800),
        ("exhaustive", "full", "exhaustive", 1800),
    ],
)
def test_quick_full_mapping_is_canonical_and_server_budgeted(
    submitted, requested, canonical, seconds
):
    plan = build_unified_investigation_plan(
        {"investigation_token": ["alice"], "mode": submitted}
    )
    planned = finalize_investigation_route_plan(
        plan,
        flags=ENABLED_FLAGS,
        execution_mode=submitted,
    )

    assert planned["route_plan"]["requested_mode"] == requested
    assert planned["route_plan"]["execution_mode"] == canonical
    assert planned["route_plan"]["budget_seconds"] == seconds
    assert planned["route_plan"]["effective_routes"][0]["coverage"] == (
        "all_eligible_enabled_non_quarantined"
        if canonical == "exhaustive"
        else "configured_top_ranked_enabled_non_quarantined"
    )


def test_route_plan_records_requested_effective_skipped_and_a_stable_digest():
    plan = build_unified_investigation_plan(
        {
            "investigation_token": [
                "Alice Example",
                "alice@example.com",
                "+62 812-3456-7890",
            ],
            "mode": "full",
            "confirm_email_route": "on",
        }
    )
    flags = {**ENABLED_FLAGS, "search_first_enabled": False}
    first = finalize_investigation_route_plan(plan, flags=flags)
    second = finalize_investigation_route_plan(
        {**plan, "route_plan": {"effective_routes": [{"route": "forged"}]}},
        flags=flags,
    )

    assert _route_names(first, "requested_routes") == [
        "native_profile_search",
        "user_scanner_email",
    ]
    assert _route_names(first, "effective_routes") == ["user_scanner_email"]
    assert _route_names(first, "skipped_routes") == [
        "maigret",
        "native_profile_search",
        "context_only",
    ]
    assert first["route_plan"]["skipped_routes"][0]["reason_code"] == (
        "no_username_targets"
    )
    assert first["route_plan"]["skipped_routes"][1]["reason_code"] == "server_disabled"
    assert first["route_plan"]["skipped_routes"][1]["planned_request_count"] == 5
    assert (
        first["route_plan"]["skipped_routes"][2]["reason_code"]
        == "context_only_no_outbound"
    )
    assert first["route_plan"]["sha256"] == second["route_plan"]["sha256"]
    assert len(first["route_plan"]["sha256"]) == 64
    assert len(first["route_plan"]["input_sha256"]) == 64
    assert investigation_has_effective_collection_route(first)


def test_email_route_requires_explicit_confirmation_on_scan_submission():
    for form in (
        {"investigation_token": ["alice@example.com"]},
        {
            "investigation_token": ["alice@example.com"],
            "confirm_email_route": "",
        },
    ):
        with pytest.raises(InvestigationInputError, match="confirm"):
            build_unified_investigation_plan(
                form,
                require_route_confirmation=True,
            )

    confirmed = build_unified_investigation_plan(
        {
            "investigation_token": ["alice@example.com"],
            "confirm_email_route": "on",
        },
        require_route_confirmation=True,
    )
    planned = finalize_investigation_route_plan(
        confirmed,
        flags=ENABLED_FLAGS,
    )

    assert _route_names(planned, "effective_routes") == ["user_scanner_email"]
    assert planned["route_plan"]["effective_routes"][0]["requires_confirmation"] is True


def test_context_only_token_set_is_refused_without_creating_a_scan_route():
    with pytest.raises(InvestigationInputError, match="context only"):
        build_unified_investigation_plan(
            {
                "investigation_token": [
                    "+62 812-3456-7890",
                    "https://public.example.com/reference/alice",
                ]
            },
            profile_url_resolver=lambda _url: {},
        )


def test_policy_refuses_when_every_requested_collection_route_is_disabled():
    plan = build_unified_investigation_plan({"investigation_token": ["Alice Example"]})

    with pytest.raises(ProfileDiscoveryPolicyError, match="No authorized"):
        govern_profile_discovery_options(
            {"investigation_spec": plan},
            environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "false"},
        )


def test_policy_allows_quick_native_fallback_but_requires_maigret_for_full_scan():
    plan = build_unified_investigation_plan(
        {"investigation_token": ["alice"], "mode": "quick"}
    )
    environment = {
        "OPENLEDGER_MAIGRET_DISCOVERY_ENABLED": "false",
        "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "true",
    }

    governed = govern_profile_discovery_options(
        {"investigation_spec": plan}, "quick", environ=environment
    )
    assert _route_names(governed["investigation_spec"], "effective_routes") == [
        "native_profile_search"
    ]

    with pytest.raises(ProfileDiscoveryPolicyError, match="Maigret"):
        govern_profile_discovery_options(
            {"investigation_spec": plan}, "full", environ=environment
        )


@pytest.mark.parametrize(
    "follow_up_capability",
    [
        "enable_github_profile_enrichment",
        "enable_archived_url_evidence",
    ],
)
def test_follow_up_route_cannot_make_seedless_quick_scan_startable(
    follow_up_capability,
):
    plan = build_unified_investigation_plan(
        {
            "investigation_token": ["Alice Example"],
            "mode": "quick",
            "search_likely_username_aliases": "on",
            "alias_candidates_present": "1",
            follow_up_capability: "on",
        }
    )

    with pytest.raises(ProfileDiscoveryPolicyError, match="No authorized"):
        govern_profile_discovery_options(
            {"investigation_spec": plan},
            environ={
                "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "false",
                "OPENLEDGER_ENRICHMENT_PROVIDERS_ENABLED": "true",
            },
        )


def test_policy_rebuilds_client_supplied_route_plan_from_server_flags():
    plan = build_unified_investigation_plan(
        {"investigation_token": ["alice"], "mode": "quick"}
    )
    plan["route_plan"] = {"effective_routes": [{"route": "arbitrary_fetch"}]}
    governed = govern_profile_discovery_options(
        {
            "investigation_spec": plan,
            "tags": ["client-filter"],
            "excluded_tags": ["other-filter"],
            "site_list": ["OneSiteOnly"],
        },
        requested_mode="quick",
        environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": "true"},
    )

    specification = governed["investigation_spec"]
    assert _route_names(specification, "effective_routes") == [
        "maigret",
        "native_profile_search",
    ]
    assert governed["execution_mode"] == "focused"
    assert governed["all_sites"] is False
    assert governed["execution_budget"]["total_seconds"] == 600
    assert governed["tags"] == []
    assert governed["excluded_tags"] == []
    assert governed["site_list"] == []


def test_invalid_unified_mode_is_visible_instead_of_silently_guessed():
    with pytest.raises(InvestigationInputError, match="Quick Scan or Full Scan"):
        build_unified_investigation_plan(
            {"investigation_token": ["alice"], "mode": "turbo"}
        )


def test_json_schema_tracks_the_persisted_runtime_contract():
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "schemas"
        / "investigation-token-plan.v1.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    planned = finalize_investigation_route_plan(
        build_unified_investigation_plan(
            {"investigation_token": ["alice"], "mode": "quick"}
        ),
        flags=ENABLED_FLAGS,
    )

    assert schema["$schema"].endswith("draft/2020-12/schema")
    assert schema["properties"]["schema_version"]["const"] == (
        UNIFIED_INVESTIGATION_SCHEMA_VERSION
    )
    assert schema["properties"]["token_schema_version"]["const"] == (
        TOKEN_SCHEMA_VERSION
    )
    assert (
        schema["$defs"]["routePlan"]["properties"]["schema_version"]["const"]
        == ROUTE_PLAN_SCHEMA_VERSION
    )
    policy_versions = schema["$defs"]["routePlan"]["properties"][
        "policy_version"
    ]["enum"]
    assert ROUTE_PLAN_POLICY_VERSION in policy_versions
    assert "investigation-routes-v1" in policy_versions
    assert set(schema["required"]).issubset(planned)
    assert set(schema["$defs"]["token"]["required"]).issubset(planned["tokens"][0])
    assert set(schema["$defs"]["routePlan"]["required"]) == set(planned["route_plan"])
