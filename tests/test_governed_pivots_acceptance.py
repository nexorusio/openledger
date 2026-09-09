# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Black-box acceptance coverage for P3c governed Persona pivots."""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from sqlalchemy import update

from maigret.web.case_store import CaseStore, persona_claims


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENLEDGER_GOVERNED_PIVOTS_ENABLED", "true")
    instance = CaseStore(
        f"sqlite:///{tmp_path / 'openledger.db'}",
        create_schema=True,
    )
    yield instance
    instance.dispose()


def _profile_report(username, *urls, full_name=None):
    profiles = []
    for index, url in enumerate(urls):
        evidence = {"fullname": full_name} if full_name and index == 0 else {}
        profiles.append(
            {
                "site_name": "X" if "x.com" in url else "Instagram",
                "url": url,
                "confidence": "strong",
                "evidence": evidence,
            }
        )
    return {
        "status": "completed",
        "usernames": [username],
        "individual_reports": [{"username": username, "claimed_profiles": profiles}],
        "found_count": len(profiles),
    }


def _seed_persona(
    store,
    *,
    username="alice_example",
    urls=("https://x.com/alice_example",),
    full_name=None,
):
    job_id = store.create_investigation([username], {})
    source_job = store.claim_next("worker:pivot-source")
    result = _profile_report(username, *urls, full_name=full_name)
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    persona_id = store.get_case(source_job["case_id"])["personas"][0]["id"]
    return persona_id, source_job["case_id"]


def _claim(store, persona_id, field_name, display_value=None):
    claims = [
        claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["field_name"] == field_name
    ]
    if display_value is not None:
        claims = [claim for claim in claims if claim["display_value"] == display_value]
    assert len(claims) == 1
    return claims[0]


def _approved_social_claim(
    store,
    *,
    username="alice_example",
    url="https://x.com/alice_example",
):
    persona_id, case_id = _seed_persona(
        store,
        username=username,
        urls=(url,),
    )
    claim = _claim(store, persona_id, "social_account", url)
    store.review_claim(claim["id"], "approved", "source.reviewer")
    return persona_id, case_id, claim["id"]


def _authorized_pivot(store, persona_id, claim_id, requested_by):
    return store.create_verified_link_pivot(
        persona_id,
        claim_id,
        requested_by,
        purpose="Corroborate this approved identity within the assigned case.",
        scope_confirmed=True,
    )


def _authorized_identity_enrichment(store, persona_id, claim_id, **kwargs):
    requested_by = kwargs.pop("requested_by", "identity.analyst")
    return store.create_identity_enrichment(
        persona_id,
        claim_id,
        requested_by=requested_by,
        purpose="Corroborate this approved identity within the assigned case.",
        scope_confirmed=True,
        **kwargs,
    )


def _replace_claim_url(store, claim_id, url):
    """Model an unsafe legacy row so the public pivot boundary must reject it."""
    with store.engine.begin() as connection:
        connection.execute(
            update(persona_claims)
            .where(persona_claims.c.id == claim_id)
            .values(
                value={"platform": "legacy", "url": url, "username": "alice"},
                display_value=url,
                normalized_value=url.casefold(),
            )
        )


def _assert_no_secret_material(document, *secret_values):
    serialized = json.dumps(document, sort_keys=True).casefold()
    forbidden_keys = (
        '"api_key"',
        '"authorization"',
        '"credential"',
        '"password"',
        '"secret"',
        '"token"',
    )
    assert not any(key in serialized for key in forbidden_keys)
    assert not any(value.casefold() in serialized for value in secret_values)


def test_verified_link_pivot_queues_auditable_focused_refresh_in_same_case(
    store, monkeypatch
):
    monkeypatch.setenv("OPENLEDGER_TEST_ONLY_SECRET", "must-not-be-persisted")
    persona_id, case_id, claim_id = _approved_social_claim(store)

    job_id = _authorized_pivot(
        store,
        persona_id,
        claim_id,
        "pivot.analyst",
    )

    queued = store.get_job(job_id)
    assert queued["case_id"] == case_id
    assert queued["kind"] == "refresh"
    assert queued["status"] == "queued"
    assert queued["usernames"] == ["alice_example"]
    assert queued["options"]["execution_mode"] == "focused"
    assert queued["options"]["all_sites"] is False
    assert queued["budget_seconds"] == 600
    assert queued["budget_policy_version"] == "profile-discovery-v1"

    specification = queued["options"]["investigation_spec"]
    assert specification["investigation_type"] == "verified_link_pivot"
    assert specification["processing_mode"] == "same_subject"
    assert specification["target_persona_id"] == persona_id

    plan = queued["options"]["governed_pivot_plan"]
    feature_snapshot = queued["options"]["governed_pivot_feature_snapshot"]
    assert plan["policy_version"] == "governed-pivots-v1"
    assert plan["persona_id"] == persona_id
    assert plan["case_id"] == case_id
    assert plan["requested_by"] == "pivot.analyst"
    assert plan["governance"] == {
        "declared_purpose": (
            "Corroborate this approved identity within the assigned case."
        ),
        "scope_confirmed": True,
        "confirmed_by": "pivot.analyst",
        "authorization_basis": "analyst_confirmed_lawful_scope",
        "external_ai_consent": False,
    }
    assert plan["source_claim"] == {
        "id": claim_id,
        "field_name": "social_account",
        "review_status": "approved",
    }
    assert plan["pivot_kind"] == "verified_profile_discovery"
    assert plan["target"] == {
        "kind": "verified_profile",
        "platform": "x",
        "handle": "alice_example",
        "canonical_url": "https://x.com/alice_example",
    }
    assert plan["input_depth"] == 0
    assert plan["output_depth"] == plan["maximum_depth"] == 1
    assert plan["source_budget"] == {
        "server_owned": True,
        "maximum_routes": 5,
        "allowed_routes": ["facebook", "instagram", "threads", "tiktok", "x"],
    }
    assert plan["request_budget"] == {
        "server_owned": True,
        "maximum_planned_requests": 25,
    }
    assert plan["execution_budget"] == {
        "server_owned": True,
        "mode": "focused",
        "total_seconds": 600,
    }
    assert plan["result_review_status"] == "pending"
    assert plan["requires_human_review"] is True
    assert plan["auto_approval_allowed"] is False
    assert plan["ai_allowed"] is False
    assert feature_snapshot == {"governed_pivots_enabled": True}

    event = store.get_events(job_id)[0]["event"]
    assert event["type"] == "queued"
    assert event["reason"] == "governed_verified_link_pivot"
    assert event["governed_pivot_plan"] == plan
    assert event["governed_pivot_feature_snapshot"] == feature_snapshot
    _assert_no_secret_material(
        {"options": queued["options"], "event": event},
        "must-not-be-persisted",
    )

    running = store.claim_next("worker:governed-pivot")
    assert running["job_id"] == job_id
    assert running["budget_seconds"] == 600
    assert (
        datetime.fromisoformat(running["deadline_at"])
        - datetime.fromisoformat(running["started_at"])
    ).total_seconds() == 600


@pytest.mark.parametrize("review_status", ["pending", "uncertain", "rejected"])
def test_verified_link_pivot_requires_an_approved_source_claim(store, review_status):
    persona_id, _case_id = _seed_persona(store)
    claim = _claim(
        store,
        persona_id,
        "social_account",
        "https://x.com/alice_example",
    )
    if review_status != "pending":
        store.review_claim(
            claim["id"], review_status, "source.reviewer", "Not verified"
        )

    with pytest.raises(ValueError, match="approved|verified"):
        _authorized_pivot(
            store,
            persona_id,
            claim["id"],
            "pivot.analyst",
        )


def test_verified_link_pivot_rejects_a_claim_from_another_persona_in_same_case(
    store,
):
    job_id = store.create_investigation(["alice", "bob"], {})
    source_job = store.claim_next("worker:multi-person-source")
    result = {
        "status": "completed",
        "usernames": ["alice", "bob"],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "X",
                        "url": "https://x.com/alice",
                        "confidence": "strong",
                        "evidence": {},
                    }
                ],
            },
            {
                "username": "bob",
                "claimed_profiles": [
                    {
                        "site_name": "X",
                        "url": "https://x.com/bob",
                        "confidence": "strong",
                        "evidence": {},
                    }
                ],
            },
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    personas = {
        persona["display_name"]: persona["id"]
        for persona in store.get_case(source_job["case_id"])["personas"]
    }
    bob_claim = _claim(store, personas["bob"], "social_account")
    store.review_claim(bob_claim["id"], "approved", "source.reviewer")

    with pytest.raises((KeyError, ValueError)):
        _authorized_pivot(
            store,
            personas["alice"],
            bob_claim["id"],
            "pivot.analyst",
        )


def test_verified_link_pivot_rejects_a_claim_from_another_case(store):
    alice_id, _alice_case, _alice_claim = _approved_social_claim(store)
    bob_id, _bob_case, bob_claim = _approved_social_claim(
        store,
        username="bob",
        url="https://x.com/bob",
    )
    assert bob_id != alice_id

    with pytest.raises((KeyError, ValueError)):
        _authorized_pivot(
            store,
            alice_id,
            bob_claim,
            "pivot.analyst",
        )


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "http://x.com/alice",
        "https://example.test/alice",
        "https://127.0.0.1/alice",
        "https://localhost/alice",
        "https://x.com/home",
        "https://alice:password@x.com/alice",
    ],
)
def test_verified_link_pivot_rejects_unsupported_arbitrary_or_private_urls(
    store, unsafe_url
):
    persona_id, _case_id, claim_id = _approved_social_claim(store)
    _replace_claim_url(store, claim_id, unsafe_url)

    with pytest.raises(ValueError, match="canonical|public|supported|verified"):
        _authorized_pivot(
            store,
            persona_id,
            claim_id,
            "pivot.analyst",
        )


@pytest.mark.parametrize("requested_by", [None, "", "   "])
def test_verified_link_pivot_requires_an_identified_actor(store, requested_by):
    persona_id, _case_id, claim_id = _approved_social_claim(store)

    with pytest.raises(ValueError, match="actor|requester|required"):
        _authorized_pivot(store, persona_id, claim_id, requested_by)


def test_verified_link_pivot_requires_purpose_and_scope_confirmation(store):
    persona_id, _case_id, claim_id = _approved_social_claim(store)

    with pytest.raises(ValueError, match="purpose"):
        store.create_verified_link_pivot(
            persona_id,
            claim_id,
            "pivot.analyst",
            purpose="",
            scope_confirmed=True,
        )
    with pytest.raises(ValueError, match="confirm|scope"):
        store.create_verified_link_pivot(
            persona_id,
            claim_id,
            "pivot.analyst",
            purpose="Authorized case follow-up",
            scope_confirmed=False,
        )


def test_verified_link_pivot_kill_switch_is_non_destructive(store, monkeypatch):
    persona_id, _case_id, claim_id = _approved_social_claim(store)
    jobs_before = [job["job_id"] for job in store.list_jobs()]
    monkeypatch.setenv("OPENLEDGER_GOVERNED_PIVOTS_ENABLED", "false")

    with pytest.raises(ValueError, match="disabled|policy"):
        _authorized_pivot(
            store,
            persona_id,
            claim_id,
            "pivot.analyst",
        )

    assert [job["job_id"] for job in store.list_jobs()] == jobs_before
    source_claim = store.get_claim(claim_id)
    assert source_claim["persona_id"] == persona_id
    assert source_claim["review_status"] == "approved"


def test_verified_link_pivot_rejects_an_active_case_conflict(store):
    persona_id, _case_id, claim_id = _approved_social_claim(store)
    store.repeat_persona_investigation(persona_id)

    with pytest.raises(ValueError, match="active investigation"):
        _authorized_pivot(
            store,
            persona_id,
            claim_id,
            "pivot.analyst",
        )


def test_verified_link_pivot_cannot_repeat_the_same_completed_origin(store):
    persona_id, _case_id, claim_id = _approved_social_claim(store)
    first_id = _authorized_pivot(
        store,
        persona_id,
        claim_id,
        "pivot.analyst",
    )
    store.claim_next("worker:first-pivot")
    store.finish(
        first_id,
        {
            "status": "completed",
            "usernames": ["alice_example"],
            "individual_reports": [],
            "found_count": 0,
        },
    )

    with pytest.raises(ValueError, match="already|duplicate|limit|budget"):
        _authorized_pivot(
            store,
            persona_id,
            claim_id,
            "pivot.analyst",
        )


def test_verified_link_pivot_does_not_permit_unbounded_depth(store):
    persona_id, _case_id, source_claim_id = _approved_social_claim(store)
    first_id = _authorized_pivot(
        store,
        persona_id,
        source_claim_id,
        "pivot.analyst",
    )
    store.claim_next("worker:first-pivot")
    result = _profile_report(
        "alice_example",
        "https://instagram.com/alice_example/",
    )
    store.finish(first_id, result)
    store.sync_persona_claims(first_id, result)
    child_claim = _claim(
        store,
        persona_id,
        "social_account",
        "https://instagram.com/alice_example/",
    )
    assert child_claim["review_status"] == "pending"
    store.review_claim(child_claim["id"], "approved", "source.reviewer")

    with pytest.raises(ValueError, match="depth|expansion|pivot"):
        _authorized_pivot(
            store,
            persona_id,
            child_claim["id"],
            "pivot.analyst",
        )


def test_verified_link_pivot_rerun_preserves_decisions_and_new_claims_pending(
    store,
):
    persona_id, _case_id, source_claim_id = _approved_social_claim(store)
    job_id = _authorized_pivot(
        store,
        persona_id,
        source_claim_id,
        "pivot.analyst",
    )
    store.claim_next("worker:pivot-rerun")
    result = _profile_report(
        "alice_example",
        "https://x.com/alice_example",
        "https://instagram.com/alice_example/",
    )
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)

    claims = {
        claim["display_value"]: claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "social_account"
    }
    assert claims["https://x.com/alice_example"]["review_status"] == "approved"
    assert claims["https://x.com/alice_example"]["reviews"][0]["reviewer"] == (
        "source.reviewer"
    )
    assert claims["https://instagram.com/alice_example/"]["review_status"] == (
        "pending"
    )
    assert claims["https://instagram.com/alice_example/"]["reviews"] == []


def test_verified_link_pivot_ambiguous_failure_is_not_negative_evidence(store):
    persona_id, _case_id, source_claim_id = _approved_social_claim(store)
    job_id = _authorized_pivot(
        store,
        persona_id,
        source_claim_id,
        "pivot.analyst",
    )
    store.claim_next("worker:ambiguous-pivot")
    result = {
        "status": "completed",
        "collection_status": "partial",
        "usernames": ["alice_example"],
        "individual_reports": [{"username": "alice_example", "claimed_profiles": []}],
        "source_errors": [
            {
                "source_engine": "profile_provider",
                "outcome": "provider_error",
                "error": "upstream response was indeterminate",
            }
        ],
        "found_count": 0,
    }
    store.finish(job_id, result)
    assert store.sync_persona_claims(job_id, result) == 0

    claims = store.get_persona(persona_id)["claims"]
    assert len(claims) == 1
    assert claims[0]["id"] == source_claim_id
    assert claims[0]["review_status"] == "approved"
    assert all(claim["review_status"] != "rejected" for claim in claims)


def test_verified_link_pivot_can_be_cancelled_before_execution(store):
    persona_id, _case_id, claim_id = _approved_social_claim(store)
    job_id = _authorized_pivot(
        store,
        persona_id,
        claim_id,
        "pivot.analyst",
    )

    assert store.request_cancel(job_id) is True
    cancelled = store.get_job(job_id)
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancel_requested_at"] is not None
    assert store.claim_next("worker:must-not-run-cancelled-pivot") is None
    assert [item["event"]["type"] for item in store.get_events(job_id)] == [
        "queued",
        "cancel_requested",
        "cancelled",
    ]


def test_queued_pivot_is_revalidated_against_the_current_review_decision(store):
    persona_id, _case_id, claim_id = _approved_social_claim(store)
    job_id = _authorized_pivot(
        store,
        persona_id,
        claim_id,
        "pivot.analyst",
    )
    claimed = store.claim_next("worker:revalidate-pivot")
    assert claimed["job_id"] == job_id

    store.review_claim(claim_id, "rejected", "source.reviewer", "Revoked")

    with pytest.raises(ValueError, match="approved|policy"):
        store.validate_governed_pivot_job(claimed)


def test_confirmed_name_enrichment_remains_approved_name_only_and_pending(
    store,
):
    persona_id, _case_id = _seed_persona(
        store,
        full_name="Alice Example",
    )
    social = _claim(store, persona_id, "social_account")
    name = _claim(store, persona_id, "full_name", "Alice Example")

    with pytest.raises(ValueError, match="approved full name"):
        _authorized_identity_enrichment(store, persona_id, social["id"])

    store.review_claim(name["id"], "approved", "identity.reviewer")
    with pytest.raises(ValueError, match="requested_by"):
        store.create_identity_enrichment(
            persona_id,
            name["id"],
            requested_by="",
            purpose="Authorized case follow-up",
            scope_confirmed=True,
        )
    with pytest.raises(ValueError, match="purpose"):
        store.create_identity_enrichment(
            persona_id,
            name["id"],
            requested_by="identity.analyst",
            purpose="",
            scope_confirmed=True,
        )
    enrichment_id = _authorized_identity_enrichment(store, persona_id, name["id"])
    store.claim_next("worker:identity-enrichment")
    store.sync_identity_enrichment(
        enrichment_id,
        {
            "source_engine": "wikipedia_public_biography",
            "status": "observed",
            "page_candidates": [],
            "page": {
                "page_id": "123",
                "title": "Alice Example",
                "url": "https://en.wikipedia.org/wiki/Alice_Example",
                "extract": "Alice Example is a public figure.",
                "thumbnail_url": "https://upload.wikimedia.org/alice.jpg",
            },
        },
        {
            "source_engine": "icij_offshore_leaks",
            "status": "no_match",
            "matches": [],
        },
    )

    enriched = [
        claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["source_engine"] == "wikipedia_public_biography"
    ]
    assert enriched
    assert all(claim["review_status"] == "pending" for claim in enriched)
    assert all(claim["reviews"] == [] for claim in enriched)
