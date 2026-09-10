# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import copy

import pytest

from maigret.web.governed_pivots import (
    FULL_NAME_EXECUTION_BUDGET_SECONDS,
    FULL_NAME_MAX_PLANNED_REQUESTS,
    FULL_NAME_SOURCE_ROUTES,
    GOVERNED_PIVOT_POLICY_VERSION,
    MAX_GOVERNED_PIVOT_DEPTH,
    VERIFIED_PROFILE_EXECUTION_BUDGET_SECONDS,
    VERIFIED_PROFILE_MAX_PLANNED_REQUESTS,
    VERIFIED_PROFILE_SOURCE_ROUTES,
    GovernedPivotPolicyError,
    build_governed_pivot_plan,
)

CASE_ID = "case-01"
PERSONA_ID = "persona-01"
ACTOR = "analyst.one"
PURPOSE = "Corroborate this approved identity within the assigned case."


def _claim(field_name, value, **updates):
    claim = {
        "id": "claim-01",
        "field_name": field_name,
        "value": value,
        "review_status": "approved",
    }
    claim.update(updates)
    return claim


def _plan(claim, **updates):
    arguments = {
        "case_id": CASE_ID,
        "persona_id": PERSONA_ID,
        "requested_by": ACTOR,
        "purpose": PURPOSE,
        "scope_confirmed": True,
    }
    arguments.update(updates)
    return build_governed_pivot_plan(claim, **arguments)


def _assert_human_boundary(plan):
    assert plan["result_review_status"] == "pending"
    assert plan["requires_human_review"] is True
    assert plan["auto_approval_allowed"] is False
    assert plan["ai_allowed"] is False


def test_confirmed_name_plan_uses_only_the_two_existing_public_sources():
    plan = _plan(_claim("full_name", "  Alice   Example  "))

    assert plan["policy_version"] == GOVERNED_PIVOT_POLICY_VERSION
    assert plan["pivot_kind"] == "confirmed_name_enrichment"
    assert plan["target"] == {
        "kind": "full_name",
        "value": "Alice Example",
        "normalized_value": "alice example",
    }
    assert plan["source_budget"] == {
        "server_owned": True,
        "maximum_routes": 2,
        "allowed_routes": list(FULL_NAME_SOURCE_ROUTES),
    }
    assert plan["request_budget"] == {
        "server_owned": True,
        "maximum_planned_requests": FULL_NAME_MAX_PLANNED_REQUESTS,
    }
    assert plan["execution_budget"] == {
        "server_owned": True,
        "mode": "focused",
        "total_seconds": FULL_NAME_EXECUTION_BUDGET_SECONDS,
    }
    assert plan["execution_mode"] == "focused"
    assert plan["input_depth"] == 0
    assert plan["output_depth"] == MAX_GOVERNED_PIVOT_DEPTH == 1
    assert plan["maximum_depth"] == 1
    _assert_human_boundary(plan)


def test_verified_profile_plan_canonicalizes_the_target_and_fixes_budgets():
    profile_url = "".join(
        (
            "https://mobile.twitter.com/",
            "Alice_Example/media?utm_source=audit",
        )
    )
    plan = _plan(
        _claim(
            "social_account",
            {
                "platform": "a forged platform",
                "username": "a conflicting handle",
                "url": profile_url,
            },
        )
    )

    assert plan["pivot_kind"] == "verified_profile_discovery"
    assert plan["target"] == {
        "kind": "verified_profile",
        "platform": "x",
        "handle": "alice_example",
        "canonical_url": "https://x.com/alice_example",
    }
    assert plan["source_budget"] == {
        "server_owned": True,
        "maximum_routes": 5,
        "allowed_routes": list(VERIFIED_PROFILE_SOURCE_ROUTES),
    }
    assert plan["request_budget"] == {
        "server_owned": True,
        "maximum_planned_requests": VERIFIED_PROFILE_MAX_PLANNED_REQUESTS,
    }
    assert plan["execution_budget"] == {
        "server_owned": True,
        "mode": "focused",
        "total_seconds": VERIFIED_PROFILE_EXECUTION_BUDGET_SECONDS,
    }
    _assert_human_boundary(plan)


def test_source_claim_identity_and_request_scope_are_retained():
    plan = _plan(
        _claim(
            "full_name",
            "Alice Example",
            case_id=CASE_ID,
            persona_id=PERSONA_ID,
            source_engine="openledger_profile_discovery",
        )
    )

    assert plan["case_id"] == CASE_ID
    assert plan["persona_id"] == PERSONA_ID
    assert plan["requested_by"] == ACTOR
    assert plan["governance"] == {
        "declared_purpose": PURPOSE,
        "scope_confirmed": True,
        "confirmed_by": ACTOR,
        "authorization_basis": "analyst_confirmed_lawful_scope",
        "external_ai_consent": False,
    }
    assert plan["source_claim"] == {
        "id": "claim-01",
        "field_name": "full_name",
        "review_status": "approved",
    }
    assert set(plan["source_claim"]) == {"id", "field_name", "review_status"}


def test_exact_reruns_are_deterministic_and_do_not_mutate_the_source_claim():
    claim = _claim("full_name", "Alice Example")
    snapshot = copy.deepcopy(claim)

    first = _plan(claim)
    repeated = _plan(claim)

    assert repeated == first
    assert repeated["plan_id"] == first["plan_id"]
    assert claim == snapshot


@pytest.mark.parametrize(
    "url",
    [
        "https://twitter.com/Alice_Example",
        "https://www.x.com/alice_example/with_replies",
        "https://x.com/alice_example/status/123456",
    ],
)
def test_equivalent_profile_urls_have_one_target_and_plan_identity(url):
    plan = _plan(_claim("social_account", {"url": url}))
    canonical_url = "https://x.com/alice_example"
    canonical = _plan(_claim("social_account", {"url": canonical_url}))

    assert plan["target"] == {
        "kind": "verified_profile",
        "platform": "x",
        "handle": "alice_example",
        "canonical_url": "https://x.com/alice_example",
    }
    assert plan["plan_id"] == canonical["plan_id"]


@pytest.mark.parametrize(
    "review_status",
    ["pending", "uncertain", "rejected", None],
)
def test_only_approved_claims_may_pivot(review_status):
    with pytest.raises(GovernedPivotPolicyError, match="explicitly approved"):
        _plan(
            _claim(
                "full_name",
                "Alice Example",
                review_status=review_status,
            )
        )


@pytest.mark.parametrize(
    "field_name",
    ["linked_profile_lead", "website", "email", "summary", "FULL_NAME"],
)
def test_only_the_two_explicit_claim_types_may_pivot(field_name):
    with pytest.raises(GovernedPivotPolicyError, match="Only approved"):
        _plan(_claim(field_name, "https://x.com/alice_example"))


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/alice",
        "https://x.com/home",
        "http://x.com/alice_example",
        "https://localhost/alice",
        "https://127.0.0.1/alice",
        "https://alice:password@x.com/alice_example",
        "https://x.com/alice_example?access_token=secret",
        "https://x.com/alice_example#password=secret",
        "file:///etc/passwd",
    ],
)
def test_unsupported_private_local_and_credential_urls_fail_closed(url):
    with pytest.raises(GovernedPivotPolicyError):
        _plan(_claim("social_account", {"url": url}))


@pytest.mark.parametrize("value", [None, "", "   ", 0, {}, [], True])
def test_missing_or_invalid_targets_are_rejected(value):
    with pytest.raises(GovernedPivotPolicyError):
        _plan(_claim("social_account", value))


@pytest.mark.parametrize("depth", [True, False, "0", 0.0, None, -1, 1, 2])
def test_only_integer_root_depth_is_accepted(depth):
    with pytest.raises(GovernedPivotPolicyError, match="depth"):
        _plan(_claim("full_name", "Alice Example"), depth=depth)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("execution_budget", {"total_seconds": 999_999}),
        ("request_budget", {"maximum_planned_requests": 999_999}),
        ("source_budget", {"maximum_routes": 999_999}),
        ("execution_mode", "exhaustive"),
        ("depth", 0),
        ("input_depth", 0),
        ("requested_by", "client.actor"),
        ("result_review_status", "approved"),
        ("auto_approved", True),
        ("auto_approval_allowed", True),
        ("ai_allowed", True),
        ("policy_version", "client-policy"),
        ("feature_flags", {"governed_pivots_enabled": True}),
    ],
)
def test_source_claim_cannot_supply_policy_budgets_or_approval_fields(
    field_name, value
):
    claim = _claim("full_name", "Alice Example")
    claim[field_name] = value

    with pytest.raises(GovernedPivotPolicyError, match="client-controlled"):
        _plan(claim)


def test_social_value_cannot_hide_client_budget_or_credentials():
    for injected in (
        {"max_requests": 1_000_000},
        {"password": "secret"},
    ):
        with pytest.raises(GovernedPivotPolicyError):
            _plan(
                _claim(
                    "social_account",
                    {"url": "https://x.com/alice_example", **injected},
                )
            )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("case_id", "../case"),
        ("persona_id", "persona/01"),
        ("source_claim.id", "claim 01"),
    ],
)
def test_malformed_ids_are_rejected(field_name, value):
    claim = _claim("full_name", "Alice Example")
    arguments = {
        "case_id": CASE_ID,
        "persona_id": PERSONA_ID,
        "requested_by": ACTOR,
        "purpose": PURPOSE,
        "scope_confirmed": True,
    }
    if field_name == "source_claim.id":
        claim["id"] = value
    else:
        arguments[field_name] = value

    with pytest.raises(GovernedPivotPolicyError, match="Invalid"):
        build_governed_pivot_plan(claim, **arguments)


@pytest.mark.parametrize(
    "requested_by",
    [None, "", "   ", 7, "analyst\x00one"],
)
def test_request_actor_is_mandatory_and_bounded(requested_by):
    with pytest.raises(GovernedPivotPolicyError, match="requested_by"):
        _plan(_claim("full_name", "Alice Example"), requested_by=requested_by)


@pytest.mark.parametrize("purpose", [None, "", "   ", 7, "case\x00purpose"])
def test_declared_purpose_is_mandatory_and_bounded(purpose):
    with pytest.raises(GovernedPivotPolicyError, match="purpose"):
        _plan(_claim("full_name", "Alice Example"), purpose=purpose)


@pytest.mark.parametrize("scope_confirmed", [False, None, 1, "true"])
def test_lawful_scope_requires_an_explicit_boolean_confirmation(scope_confirmed):
    with pytest.raises(GovernedPivotPolicyError, match="confirm|scope"):
        _plan(
            _claim("full_name", "Alice Example"),
            scope_confirmed=scope_confirmed,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [("case_id", "case-02"), ("persona_id", "persona-02")],
)
def test_embedded_claim_scope_cannot_escape_requested_case_or_persona(
    field_name, value
):
    with pytest.raises(GovernedPivotPolicyError, match="does not match"):
        _plan(_claim("full_name", "Alice Example", **{field_name: value}))


def test_public_api_refuses_non_objects_and_missing_claim_identity():
    with pytest.raises(GovernedPivotPolicyError, match="object"):
        _plan([])
    with pytest.raises(GovernedPivotPolicyError, match="source_claim.id"):
        _plan(
            {
                "field_name": "full_name",
                "value": "Alice Example",
                "review_status": "approved",
            }
        )
