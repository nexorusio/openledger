# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select, update

from maigret.web import app as web_app
from maigret.web.case_store import (
    CaseStore,
    PROFILE_SEARCH_PENDING_CLAIM_CONFIDENCE,
    persona_claims,
    profile_search_audits,
    profile_search_candidate_reviews,
)
from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_contract import (
    ProfileSearchEvidence,
    ProfileSearchProvenance,
)
from maigret.web.profile_search_orchestrator import ProfileSearchOrchestrator
from maigret.web.persona_intelligence import (
    claim_fingerprint,
    extract_ai_persona_claims,
)


class _Client:
    async def search(self, query):
        provenance = ProfileSearchProvenance.for_query(
            query,
            provider="brave",
            retrieved_at="2026-09-08T15:00:00Z",
            provider_request_id="request-review",
        )
        source_url = {
            "instagram": "https://instagram.com/alice_example/",
            "x": "https://x.com/alice_example",
        }[query.platform]
        return ProfileSearchRun(
            query=query,
            provenance=provenance,
            evidence=(
                ProfileSearchEvidence(
                    result_rank=1,
                    source_url=source_url,
                    title="Alice Example (@alice_example)",
                    snippet="Public profile search result.",
                ),
            ),
        )


@pytest.fixture
def store(tmp_path):
    instance = CaseStore(
        f"sqlite:///{tmp_path / 'profile-search-review.db'}",
        create_schema=True,
    )
    yield instance
    instance.dispose()


async def _seed_discovery(store, *, platform="instagram"):
    job_id = store.create_investigation(["alice"], {})
    job = store.claim_next("worker:profile-search-review")
    result = await ProfileSearchOrchestrator(_Client()).discover(
        {
            "identifiers": [],
            "search_targets": [
                {"value": "alice_example", "source_type": "username"}
            ],
        },
        platforms=(platform,),
    )
    audit_id = store.record_profile_search_result(
        job_id, result, worker_id=job["worker_id"]
    )
    case = store.get_case(job["case_id"])
    return {
        "job_id": job_id,
        "case_id": job["case_id"],
        "persona_id": case["personas"][0]["id"],
        "audit_id": audit_id,
        "candidate_id": result.candidates[0].candidate.candidate_id,
        "result": result,
    }


@pytest.mark.asyncio
async def test_candidate_is_visible_but_creates_no_automatic_claim(store):
    seeded = await _seed_discovery(store)

    discovery = store.get_case_profile_search_discovery(seeded["case_id"])
    persona = store.get_persona(seeded["persona_id"])

    assert discovery["candidate_count"] == 1
    assert discovery["candidates"][0]["identity_status"] == "unverified"
    assert discovery["candidates"][0]["review_status"] == "pending"
    assert discovery["candidates"][0]["reviews"] == []
    assert persona["claims"] == []


@pytest.mark.asyncio
async def test_propose_creates_only_pending_claim_with_audit_lineage(store):
    seeded = await _seed_discovery(store)

    review = store.review_profile_search_candidate(
        seeded["case_id"],
        seeded["audit_id"],
        seeded["candidate_id"],
        seeded["persona_id"],
        "proposed",
        "analyst",
        "Handle matches the known alias; verify the account content next.",
    )

    assert review["decision"] == "proposed"
    assert review["claim_review_status"] == "pending"
    persona = store.get_persona(seeded["persona_id"])
    assert len(persona["claims"]) == 1
    claim = persona["claims"][0]
    assert claim["field_name"] == "social_account"
    assert claim["review_status"] == "pending"
    assert claim["confidence"] == PROFILE_SEARCH_PENDING_CLAIM_CONFIDENCE
    discovery_score = seeded["result"].candidates[0].discovery_score
    assert claim["confidence"] != discovery_score
    assert claim["source_engine"] == "native_profile_search_review"
    evidence = claim["evidence"][0]
    assert evidence["details"]["audit_id"] == seeded["audit_id"]
    assert evidence["details"]["score_scope"] == (
        "discovery_review_priority"
    )
    assert evidence["details"]["candidate_identity_unverified"] is True
    assert evidence["details"]["human_review_required"] is True
    discovery = store.get_case_profile_search_discovery(seeded["case_id"])
    assert discovery["candidates"][0]["review_status"] == "pending"
    assert discovery["candidates"][0]["reviews"][0]["decision"] == (
        "proposed"
    )


@pytest.mark.asyncio
async def test_repeat_proposal_never_overwrites_persona_review_decision(store):
    seeded = await _seed_discovery(store)
    first = store.review_profile_search_candidate(
        seeded["case_id"],
        seeded["audit_id"],
        seeded["candidate_id"],
        seeded["persona_id"],
        "proposed",
        "analyst",
    )
    store.review_claim(first["claim_id"], "approved", "senior-analyst")

    repeated = store.review_profile_search_candidate(
        seeded["case_id"],
        seeded["audit_id"],
        seeded["candidate_id"],
        seeded["persona_id"],
        "proposed",
        "second-analyst",
    )

    assert repeated["claim_id"] == first["claim_id"]
    assert repeated["claim_review_status"] == "approved"
    claim = store.get_persona(seeded["persona_id"])["claims"][0]
    assert claim["review_status"] == "approved"
    assert claim["reviewed_by"] == "senior-analyst"


@pytest.mark.asyncio
async def test_proposal_reuses_equivalent_existing_social_account(store):
    seeded = await _seed_discovery(store)
    profile_url = "https://www.instagram.com/alice_example/"
    existing_value = {
        "site_name": "Instagram",
        "url": profile_url,
    }
    with store.engine.begin() as connection:
        store._upsert_persona_candidates(
            connection,
            persona_id=seeded["persona_id"],
            job_id=seeded["job_id"],
            candidates=(
                {
                    "field_name": "social_account",
                    "value": existing_value,
                    "display_value": profile_url,
                    "normalized_value": json.dumps(
                        existing_value, sort_keys=True
                    ),
                    "confidence": 80,
                    "fingerprint": claim_fingerprint(
                        "social_account", existing_value
                    ),
                    "source_engine": "maigret",
                    "source_record_id": "Instagram:alice_example",
                    "native_status": "claimed",
                    "evidence": [],
                },
            ),
            now=datetime.now(timezone.utc),
        )
    existing_claim = store.get_persona(seeded["persona_id"])["claims"][0]

    review = store.review_profile_search_candidate(
        seeded["case_id"],
        seeded["audit_id"],
        seeded["candidate_id"],
        seeded["persona_id"],
        "proposed",
        "analyst",
    )

    assert review["claim_id"] == existing_claim["id"]
    assert review["claim_review_status"] == "pending"
    assert len(store.get_persona(seeded["persona_id"])["claims"]) == 1


@pytest.mark.asyncio
async def test_x_proposal_reuses_existing_legacy_twitter_account(store):
    seeded = await _seed_discovery(store, platform="x")
    legacy_url = "https://twitter.com/alice_example"
    existing_value = {
        "platform": "Twitter",
        "url": legacy_url,
        "username": "alice_example",
    }
    with store.engine.begin() as connection:
        store._upsert_persona_candidates(
            connection,
            persona_id=seeded["persona_id"],
            job_id=seeded["job_id"],
            candidates=(
                {
                    "field_name": "social_account",
                    "value": existing_value,
                    "display_value": legacy_url,
                    "normalized_value": json.dumps(
                        existing_value, sort_keys=True
                    ),
                    "confidence": 80,
                    "fingerprint": claim_fingerprint(
                        "social_account", existing_value
                    ),
                    "source_engine": "maigret",
                    "source_record_id": "Twitter:alice_example",
                    "native_status": "claimed",
                    "evidence": [],
                },
            ),
            now=datetime.now(timezone.utc),
        )
    existing_claim = store.get_persona(seeded["persona_id"])["claims"][0]

    review = store.review_profile_search_candidate(
        seeded["case_id"],
        seeded["audit_id"],
        seeded["candidate_id"],
        seeded["persona_id"],
        "proposed",
        "analyst",
    )

    assert review["claim_id"] == existing_claim["id"]
    claims = store.get_persona(seeded["persona_id"])["claims"]
    assert len(claims) == 1
    assert claims[0]["display_value"] == "https://x.com/alice_example"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "legacy_url",
        "legacy_platform",
        "web_url",
        "canonical_web_url",
    ),
    [
        (
            "https://twitter.com/alice_example",
            "Twitter",
            "https://x.com/alice_example",
            "https://x.com/alice_example",
        ),
        (
            "https://twitter.com/alice_example",
            "Twitter",
            "https://mobile.twitter.com/alice_example/with_replies",
            "https://x.com/alice_example",
        ),
        (
            "https://mobile.twitter.com/alice_example",
            "mobile.twitter.com",
            "https://x.com/alice_example",
            "https://x.com/alice_example",
        ),
    ],
)
async def test_web_research_x_alias_reuses_existing_twitter_account(
    store,
    legacy_url,
    legacy_platform,
    web_url,
    canonical_web_url,
):
    seeded = await _seed_discovery(store, platform="x")
    legacy_value = {
        "platform": legacy_platform,
        "url": legacy_url,
        "username": "alice_example",
    }
    web_candidates = extract_ai_persona_claims(
        [
            {
                "username": "alice_example",
                "field_name": "social_account",
                "value": web_url,
                "confidence": 50,
                "source_url": web_url,
                "reason": "The cited result is a public X profile.",
            }
        ],
        sources=[{"title": "Alice Example on X", "url": web_url}],
        usernames=["alice_example"],
        model="test-model",
    )
    with store.engine.begin() as connection:
        store._upsert_persona_candidates(
            connection,
            persona_id=seeded["persona_id"],
            job_id=seeded["job_id"],
            candidates=(
                {
                    "field_name": "social_account",
                    "value": legacy_value,
                    "display_value": legacy_url,
                    "normalized_value": json.dumps(
                        legacy_value, sort_keys=True
                    ),
                    "confidence": 80,
                    "fingerprint": claim_fingerprint(
                        "social_account", legacy_value
                    ),
                    "source_engine": "maigret",
                    "source_record_id": "Twitter:alice_example",
                    "native_status": "claimed",
                    "evidence": [],
                },
            ),
            now=datetime.now(timezone.utc),
        )
        store._upsert_persona_candidates(
            connection,
            persona_id=seeded["persona_id"],
            job_id=seeded["job_id"],
            candidates=web_candidates,
            now=datetime.now(timezone.utc),
        )

    claims = store.get_persona(seeded["persona_id"])["claims"]
    assert len(claims) == 1
    assert claims[0]["display_value"] == canonical_web_url
    assert claims[0]["value"]["platform"] == "x"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bob_url", "canonical_bob_url"),
    [
        ("https://x.com/bob_example", "https://x.com/bob_example"),
        ("https://x.com./bob_example", "https://x.com/bob_example"),
    ],
)
async def test_web_research_x_url_cannot_overwrite_another_handle(
    store, bob_url, canonical_bob_url
):
    seeded = await _seed_discovery(store, platform="x")
    legacy_url = "https://twitter.com/alice_example"
    legacy_value = {
        "platform": "Twitter",
        "url": legacy_url,
        "username": "alice_example",
    }
    web_candidates = extract_ai_persona_claims(
        [
            {
                "username": "alice_example",
                "field_name": "social_account",
                "value": bob_url,
                "confidence": 50,
                "source_url": bob_url,
                "reason": "The cited result is a public X profile.",
            }
        ],
        sources=[{"title": "Bob Example on X", "url": bob_url}],
        usernames=["alice_example"],
        model="test-model",
    )
    assert web_candidates[0]["value"]["username"] == "bob_example"
    assert web_candidates[0]["value"]["platform"] == "x"

    with store.engine.begin() as connection:
        store._upsert_persona_candidates(
            connection,
            persona_id=seeded["persona_id"],
            job_id=seeded["job_id"],
            candidates=(
                {
                    "field_name": "social_account",
                    "value": legacy_value,
                    "display_value": legacy_url,
                    "normalized_value": json.dumps(
                        legacy_value, sort_keys=True
                    ),
                    "confidence": 80,
                    "fingerprint": claim_fingerprint(
                        "social_account", legacy_value
                    ),
                    "source_engine": "maigret",
                    "source_record_id": "Twitter:alice_example",
                    "native_status": "claimed",
                    "evidence": [],
                },
            ),
            now=datetime.now(timezone.utc),
        )
        store._upsert_persona_candidates(
            connection,
            persona_id=seeded["persona_id"],
            job_id=seeded["job_id"],
            candidates=web_candidates,
            now=datetime.now(timezone.utc),
        )

    claims = store.get_persona(seeded["persona_id"])["claims"]
    assert len(claims) == 2
    assert {claim["display_value"] for claim in claims} == {
        legacy_url,
        canonical_bob_url,
    }


@pytest.mark.asyncio
async def test_x_candidate_does_not_trust_legacy_chat_persona_username(store):
    seeded = await _seed_discovery(store, platform="x")
    bob_url = "https://mobile.twitter.com/bob_example"
    legacy_chat_value = {
        "platform": "mobile.twitter.com",
        "url": bob_url,
        "username": "alice_example",
    }
    with store.engine.begin() as connection:
        store._upsert_persona_candidates(
            connection,
            persona_id=seeded["persona_id"],
            job_id=seeded["job_id"],
            candidates=(
                {
                    "field_name": "social_account",
                    "value": legacy_chat_value,
                    "display_value": bob_url,
                    "normalized_value": json.dumps(
                        legacy_chat_value, sort_keys=True
                    ),
                    "confidence": 50,
                    "fingerprint": claim_fingerprint(
                        "social_account", legacy_chat_value
                    ),
                    "source_engine": "case_chat_user_statement",
                    "source_record_id": "user-message",
                    "native_status": "candidate_proposed",
                    "evidence": [],
                },
            ),
            now=datetime.now(timezone.utc),
        )
    existing = store.get_persona(seeded["persona_id"])["claims"][0]
    store.review_claim(existing["id"], "approved", "analyst")

    review = store.review_profile_search_candidate(
        seeded["case_id"],
        seeded["audit_id"],
        seeded["candidate_id"],
        seeded["persona_id"],
        "proposed",
        "second-analyst",
    )

    assert review["claim_id"] != existing["id"]
    claims = store.get_persona(seeded["persona_id"])["claims"]
    assert len(claims) == 2
    preserved = next(claim for claim in claims if claim["id"] == existing["id"])
    assert preserved["display_value"] == bob_url
    assert preserved["review_status"] == "approved"


@pytest.mark.asyncio
async def test_x_candidate_reuses_user_scanner_url_despite_platform_label(store):
    seeded = await _seed_discovery(store, platform="x")
    scanner_url = "https://x.com/alice_example"
    scanner_value = {
        "platform": "X (Twitter)",
        "url": scanner_url,
        "username": "alice_example",
    }
    with store.engine.begin() as connection:
        store._upsert_persona_candidates(
            connection,
            persona_id=seeded["persona_id"],
            job_id=seeded["job_id"],
            candidates=(
                {
                    "field_name": "social_account",
                    "value": scanner_value,
                    "display_value": "X (Twitter): @alice_example",
                    "normalized_value": "x (twitter)\0alice_example",
                    "confidence": 70,
                    "fingerprint": claim_fingerprint(
                        "social_account", scanner_value
                    ),
                    "source_engine": "user_scanner_username",
                    "source_record_id": "X (Twitter):alice_example",
                    "native_status": "found",
                    "evidence": [],
                },
            ),
            now=datetime.now(timezone.utc),
        )
    existing = store.get_persona(seeded["persona_id"])["claims"][0]

    review = store.review_profile_search_candidate(
        seeded["case_id"],
        seeded["audit_id"],
        seeded["candidate_id"],
        seeded["persona_id"],
        "proposed",
        "analyst",
    )

    assert review["claim_id"] == existing["id"]
    assert len(store.get_persona(seeded["persona_id"])["claims"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["rejected", "uncertain"])
async def test_reject_or_uncertain_records_decision_without_claim(
    store, decision
):
    seeded = await _seed_discovery(store)

    store.review_profile_search_candidate(
        seeded["case_id"],
        seeded["audit_id"],
        seeded["candidate_id"],
        seeded["persona_id"],
        decision,
        "analyst",
    )

    assert store.get_persona(seeded["persona_id"])["claims"] == []
    discovery = store.get_case_profile_search_discovery(seeded["case_id"])
    assert discovery["candidates"][0]["reviews"][0]["decision"] == decision


@pytest.mark.asyncio
async def test_review_rejects_wrong_ownership_and_bad_decision(store):
    seeded = await _seed_discovery(store)
    other_job = store.create_investigation(["mallory"], {})
    other_case = store.get_case(store.get_job(other_job)["case_id"])
    other_persona_id = other_case["personas"][0]["id"]

    with pytest.raises(ValueError, match="does not belong"):
        store.review_profile_search_candidate(
            seeded["case_id"],
            seeded["audit_id"],
            seeded["candidate_id"],
            other_persona_id,
            "proposed",
            "analyst",
        )
    with pytest.raises(KeyError):
        store.review_profile_search_candidate(
            seeded["case_id"],
            seeded["audit_id"],
            "profile-search:" + "0" * 64,
            seeded["persona_id"],
            "proposed",
            "analyst",
        )
    with pytest.raises(ValueError, match="Choose propose"):
        store.review_profile_search_candidate(
            seeded["case_id"],
            seeded["audit_id"],
            seeded["candidate_id"],
            seeded["persona_id"],
            "approved",
            "analyst",
        )


@pytest.mark.asyncio
async def test_review_checks_immutable_audit_integrity(store):
    seeded = await _seed_discovery(store)
    with store.engine.begin() as connection:
        audit = connection.execute(
            select(profile_search_audits).where(
                profile_search_audits.c.id == seeded["audit_id"]
            )
        ).mappings().one()
        document = dict(audit["document"])
        document["candidates"][0]["profile_url"] = (
            "https://instagram.com/tampered/"
        )
        connection.execute(
            update(profile_search_audits)
            .where(profile_search_audits.c.id == seeded["audit_id"])
            .values(document=document)
        )

    with pytest.raises(ValueError, match="integrity"):
        store.get_case_profile_search_discovery(seeded["case_id"])
    with pytest.raises(ValueError, match="integrity"):
        store.review_profile_search_candidate(
            seeded["case_id"],
            seeded["audit_id"],
            seeded["candidate_id"],
            seeded["persona_id"],
            "proposed",
            "analyst",
        )


@pytest.mark.asyncio
async def test_case_api_and_ui_are_bounded_and_review_safe(store, monkeypatch):
    seeded = await _seed_discovery(store)
    monkeypatch.setattr(web_app, "case_store", store)
    web_app.app.config["TESTING"] = True
    web_app.app.config["AUTH_REQUIRED"] = False
    client = web_app.app.test_client()

    response = client.get(
        f"/api/cases/{seeded['case_id']}/profile-search"
    )
    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    payload = response.get_json()
    candidate = payload["discovery"]["candidates"][0]
    assert candidate["identity_status"] == "unverified"
    assert payload["governance"] == {
        "candidate_identity_unverified": True,
        "discovery_score_is_not_confidence": True,
        "automatic_persona_claims": False,
        "persona_approval_required": True,
    }
    serialized = json.dumps(payload)
    assert '"queries"' not in serialized
    assert '"runs"' not in serialized
    assert "request-review" not in serialized

    page = client.get(f"/cases/{seeded['case_id']}")
    html = page.get_data(as_text=True)
    assert page.status_code == 200
    assert "Major-platform profile candidates" not in html
    assert "Send to Persona review" not in html


@pytest.mark.asyncio
async def test_review_route_requires_csrf_and_targets_owned_persona(
    store, monkeypatch
):
    seeded = await _seed_discovery(store)
    monkeypatch.setattr(web_app, "case_store", store)
    web_app.app.config["TESTING"] = True
    web_app.app.config["AUTH_REQUIRED"] = False
    client = web_app.app.test_client()
    path = (
        f"/cases/{seeded['case_id']}/profile-search/"
        f"{seeded['audit_id']}/{seeded['candidate_id']}/review"
    )

    rejected = client.post(
        path,
        data={
            "persona_id": seeded["persona_id"],
            "decision": "proposed",
        },
    )
    assert rejected.status_code == 302
    with store.engine.connect() as connection:
        assert connection.scalar(
            select(func.count()).select_from(
                profile_search_candidate_reviews
            )
        ) == 0
        assert connection.scalar(
            select(func.count()).select_from(persona_claims)
        ) == 0

    with client.session_transaction() as browser_session:
        browser_session["csrf_token"] = "profile-search-review-csrf"
    accepted = client.post(
        path,
        data={
            "csrf_token": "profile-search-review-csrf",
            "persona_id": seeded["persona_id"],
            "decision": "proposed",
            "note": "Advance for full Persona review.",
        },
    )
    assert accepted.status_code == 302
    assert accepted.location.endswith(
        f"/cases/{seeded['case_id']}/pipeline/{seeded['persona_id']}"
        "#shortlist-digital"
    )
    claim = store.get_persona(seeded["persona_id"])["claims"][0]
    assert claim["review_status"] == "pending"


@pytest.mark.asyncio
async def test_profile_search_api_requires_application_authentication(
    store, monkeypatch
):
    seeded = await _seed_discovery(store)
    monkeypatch.setattr(web_app, "case_store", store)
    monkeypatch.setitem(web_app.app.config, "AUTH_REQUIRED", True)
    client = web_app.app.test_client()

    response = client.get(
        f"/api/cases/{seeded['case_id']}/profile-search"
    )

    assert response.status_code == 401
    assert response.get_json() == {"error": "Authentication required."}
