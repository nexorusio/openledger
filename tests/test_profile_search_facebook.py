# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from datetime import datetime, timezone

import pytest

from maigret.web.profile_search_contract import (
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)
from maigret.web.profile_search_facebook import (
    facebook_candidates_from_evidence,
    parse_facebook_profile_url,
)


def _query():
    return ProfileSearchQuery(
        query_id="profile-query:facebook-1",
        platform="facebook",
        query_text='site:facebook.com "alice.example"',
        seed_kind="alias",
        seed_value="alice.example",
        seed_score=96,
        seed_reason="First and last name",
        max_results=5,
    )


@pytest.mark.parametrize(
    ("url", "canonical_url", "handle", "kind"),
    [
        (
            "https://facebook.com/Alice.Example/",
            "https://www.facebook.com/alice.example",
            "alice.example",
            "vanity_profile_or_page",
        ),
        (
            "https://m.facebook.com/Alice.Example/posts/123?ref=search",
            "https://www.facebook.com/alice.example",
            "alice.example",
            "vanity_profile_or_page",
        ),
        (
            "https://www.facebook.com/pg/Alice.Example/about/",
            "https://www.facebook.com/alice.example",
            "alice.example",
            "vanity_profile_or_page",
        ),
        (
            "https://www.facebook.com/profile.php?id=123456789&ref=search",
            "https://www.facebook.com/profile.php?id=123456789",
            "123456789",
            "numeric_profile",
        ),
        (
            "https://web.facebook.com/people/Alice-Example/123456789/posts/1",
            "https://www.facebook.com/people/alice-example/123456789",
            "123456789",
            "people_profile",
        ),
        (
            "https://facebook.com/pages/Alice-Foundation/987654321/photos",
            "https://www.facebook.com/pages/alice-foundation/987654321",
            "987654321",
            "legacy_page",
        ),
    ],
)
def test_facebook_profile_forms_are_recognized_and_canonicalized(
    url, canonical_url, handle, kind
):
    reference = parse_facebook_profile_url(url)

    assert reference.canonical_url == canonical_url
    assert reference.handle == handle
    assert reference.reference_kind == kind


@pytest.mark.parametrize(
    "url",
    [
        "https://facebook.com/",
        "https://developers.facebook.com/docs/",
        "https://l.facebook.com/l.php?u=https://example.test",
        "https://facebook.com/groups/investigators",
        "https://facebook.com/events/123456",
        "https://facebook.com/reel/123456",
        "https://facebook.com/share/abcdef",
        "https://facebook.com/story.php?id=123456",
        "https://facebook.com/profile.php?id=abc",
        "https://facebook.com/profile.php?id=123&id=456",
        "https://facebook.com:8443/alice.example",
        "https://facebook.com/pg/groups/about",
        "https://facebook.com/ab",
        "https://facebook.com/alice..example",
        "https://facebook.com/example.com",
        "http://facebook.com/alice.example",
        "https://user:secret@facebook.com/alice.example",
    ],
)
def test_non_profile_or_ambiguous_facebook_urls_are_rejected(url):
    assert parse_facebook_profile_url(url) is None


def test_adapter_retains_raw_result_and_emits_pending_unverified_candidate():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        provider_request_id="request-42",
        retrieved_at=datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc),
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url=(
            "https://m.facebook.com/Alice.Example/posts/123?ref=search"
        ),
        title="Alice Example",
        snippet="Public Facebook result.",
    )

    candidates = facebook_candidates_from_evidence(
        query, [evidence], provenance
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.profile_url == "https://www.facebook.com/alice.example"
    assert candidate.handle == "alice.example"
    assert candidate.evidence.source_url == evidence.source_url
    assert candidate.account_status == "candidate"
    assert candidate.identity_status == "unverified"
    assert candidate.review_status == "pending"


def test_adapter_filters_system_results_and_deduplicates_profile_variants():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T10:00:00Z",
    )
    evidence = [
        ProfileSearchEvidence(
            result_rank=1,
            source_url="https://facebook.com/alice.example",
        ),
        ProfileSearchEvidence(
            result_rank=2,
            source_url="https://m.facebook.com/Alice.Example/posts/42",
        ),
        ProfileSearchEvidence(
            result_rank=3,
            source_url="https://facebook.com/groups/alice.example",
        ),
    ]

    candidates = facebook_candidates_from_evidence(
        query, evidence, provenance
    )

    assert len(candidates) == 1
    assert candidates[0].evidence.result_rank == 1


def test_facebook_adapter_rejects_a_query_for_another_platform():
    query = ProfileSearchQuery(
        query_id="profile-query:instagram-1",
        platform="instagram",
        query_text='site:instagram.com "alice"',
        seed_kind="username",
        seed_value="alice",
    )
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T10:00:00Z",
    )

    with pytest.raises(ValueError, match="requires a Facebook query"):
        facebook_candidates_from_evidence(query, [], provenance)
