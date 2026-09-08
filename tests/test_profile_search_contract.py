# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from datetime import datetime, timezone

import pytest

from maigret.web.profile_search_contract import (
    ProfileSearchCandidate,
    ProfileSearchContractError,
    ProfileSearchError,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)


def _query():
    return ProfileSearchQuery(
        query_id="profile-query:1",
        platform="instagram",
        query_text='site:instagram.com "alice_example"',
        seed_kind="alias",
        seed_value="alice_example",
        max_results=5,
    )


def test_contract_serializes_unverified_candidate_with_complete_lineage():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="example-search",
        provider_request_id="request-42",
        retrieved_at=datetime(2026, 9, 8, 8, 30, tzinfo=timezone.utc),
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url="https://www.instagram.com/alice_example/",
        title="Alice Example (@alice_example)",
        snippet="Public profile result.",
    )
    candidate = ProfileSearchCandidate.from_result(
        query,
        profile_url=evidence.source_url,
        handle="alice_example",
        evidence=evidence,
        provenance=provenance,
    )

    assert query.as_dict()["schema_version"] == 1
    assert provenance.as_dict()["query_fingerprint"] == query.fingerprint
    assert candidate.as_dict() == {
        "candidate_id": candidate.candidate_id,
        "query_id": "profile-query:1",
        "platform": "instagram",
        "profile_url": "https://www.instagram.com/alice_example/",
        "handle": "alice_example",
        "account_status": "candidate",
        "identity_status": "unverified",
        "review_status": "pending",
        "evidence": evidence.as_dict(),
        "provenance": provenance.as_dict(),
    }


def test_candidate_id_is_provider_independent_for_the_same_account():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="example-search",
        retrieved_at="2026-09-08T08:30:00Z",
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url="https://www.instagram.com/alice_example/",
    )

    first = ProfileSearchCandidate.from_result(
        query,
        profile_url=evidence.source_url,
        handle="alice_example",
        evidence=evidence,
        provenance=provenance,
    )
    second_provenance = ProfileSearchProvenance.for_query(
        query,
        provider="another-search",
        retrieved_at="2026-09-08T08:31:00Z",
    )
    second = ProfileSearchCandidate.from_result(
        query,
        profile_url=evidence.source_url,
        handle="Alice_Example",
        evidence=evidence,
        provenance=second_provenance,
    )

    assert first.candidate_id == second.candidate_id


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"platform": "linkedin"}, "Unsupported profile-search platform"),
        ({"seed_kind": "email"}, "Unsupported profile-search seed kind"),
        ({"max_results": 11}, "max_results must be between"),
        ({"query_text": "x" * 501}, "query_text is too large"),
    ],
)
def test_query_rejects_unsupported_or_unbounded_input(overrides, message):
    values = {
        "query_id": "profile-query:1",
        "platform": "instagram",
        "query_text": "site:instagram.com alice",
        "seed_kind": "alias",
        "seed_value": "alice",
    }
    values.update(overrides)

    with pytest.raises(ProfileSearchContractError, match=message):
        ProfileSearchQuery(**values)


@pytest.mark.parametrize(
    "url",
    [
        "http://instagram.com/alice",
        "https://user:password@instagram.com/alice",
        "https://localhost/alice",
        "https://127.0.0.1/alice",
        "https://instagram.com:8443/alice",
    ],
)
def test_evidence_rejects_non_public_or_credential_bearing_urls(url):
    with pytest.raises(ProfileSearchContractError, match="public HTTPS URL"):
        ProfileSearchEvidence(result_rank=1, source_url=url)


def test_candidate_rejects_mismatched_query_lineage():
    query = _query()
    wrong_provenance = ProfileSearchProvenance(
        query_id="profile-query:2",
        query_fingerprint=query.fingerprint,
        provider="example-search",
        retrieved_at="2026-09-08T08:30:00Z",
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url="https://www.instagram.com/alice_example/",
    )

    with pytest.raises(
        ProfileSearchContractError, match="Query does not match"
    ):
        ProfileSearchCandidate.from_result(
            query,
            profile_url=evidence.source_url,
            handle="alice_example",
            evidence=evidence,
            provenance=wrong_provenance,
        )


def test_candidate_retains_raw_evidence_url_after_profile_canonicalization():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="example-search",
        retrieved_at="2026-09-08T08:30:00Z",
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url="https://www.instagram.com/alice_example/posts/42",
    )

    candidate = ProfileSearchCandidate(
        candidate_id="profile-search:1",
        query_id=query.query_id,
        platform=query.platform,
        profile_url="https://www.instagram.com/alice_example/",
        handle="alice_example",
        evidence=evidence,
        provenance=provenance,
    )

    assert candidate.profile_url.endswith("/alice_example/")
    assert candidate.evidence.source_url.endswith("/alice_example/posts/42")


def test_error_contract_keeps_only_bounded_public_diagnostics():
    error = ProfileSearchError(
        query_id="profile-query:1",
        provider="example-search",
        code="rate_limited",
        message="Provider request was rate limited.",
        retryable=True,
        occurred_at="2026-09-08T08:30:00+00:00",
        http_status=429,
    )

    assert error.as_dict() == {
        "query_id": "profile-query:1",
        "provider": "example-search",
        "code": "rate_limited",
        "message": "Provider request was rate limited.",
        "retryable": True,
        "occurred_at": "2026-09-08T08:30:00Z",
        "http_status": 429,
    }

    with pytest.raises(ProfileSearchContractError, match="http_status"):
        ProfileSearchError(
            query_id="profile-query:1",
            provider="example-search",
            code="provider_error",
            message="Request failed.",
            retryable=False,
            occurred_at="2026-09-08T08:30:00Z",
            http_status=700,
        )
