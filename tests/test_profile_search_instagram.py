# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from datetime import datetime, timezone

import pytest

from maigret.web.profile_search_contract import (
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)
from maigret.web.profile_search_instagram import (
    instagram_candidates_from_evidence,
    parse_instagram_profile_url,
)


def _query():
    return ProfileSearchQuery(
        query_id="profile-query:instagram-1",
        platform="instagram",
        query_text='site:instagram.com "alice_example"',
        seed_kind="alias",
        seed_value="alice_example",
        seed_score=96,
        seed_reason="First and last name",
        max_results=5,
    )


@pytest.mark.parametrize(
    ("url", "canonical_url", "handle", "kind"),
    [
        (
            "https://instagram.com/Alice_Example/",
            "https://www.instagram.com/alice_example/",
            "alice_example",
            "profile",
        ),
        (
            "https://m.instagram.com/Alice.Example/tagged/?hl=en",
            "https://www.instagram.com/alice.example/",
            "alice.example",
            "profile_tab",
        ),
        (
            "https://www.instagram.com/alice_example/reels/",
            "https://www.instagram.com/alice_example/",
            "alice_example",
            "profile_tab",
        ),
        (
            "https://instagram.com/_u/Alice_Example",
            "https://www.instagram.com/alice_example/",
            "alice_example",
            "app_profile_link",
        ),
        (
            "https://www.instagram.com/a/",
            "https://www.instagram.com/a/",
            "a",
            "profile",
        ),
    ],
)
def test_instagram_profile_forms_are_recognized_and_canonicalized(
    url, canonical_url, handle, kind
):
    reference = parse_instagram_profile_url(url)

    assert reference.canonical_url == canonical_url
    assert reference.handle == handle
    assert reference.reference_kind == kind


@pytest.mark.parametrize(
    "url",
    [
        "https://instagram.com/",
        "https://instagram.com/p/ABC123/",
        "https://instagram.com/reel/ABC123/",
        "https://instagram.com/stories/alice_example/123/",
        "https://instagram.com/accounts/login/",
        "https://instagram.com/explore/people/",
        "https://instagram.com/alice_example/posts/123",
        "https://instagram.com/_u/alice_example/extra",
        "https://instagram.com/.alice/",
        "https://instagram.com/alice./",
        "https://instagram.com/alice..example/",
        "https://instagram.com/alice-example/",
        "https://instagram.com:8443/alice_example/",
        "https://l.instagram.com/?u=https://example.test",
        "https://help.instagram.com/alice_example/",
        "http://instagram.com/alice_example/",
        "https://user:secret@instagram.com/alice_example/",
    ],
)
def test_non_profile_or_ambiguous_instagram_urls_are_rejected(url):
    assert parse_instagram_profile_url(url) is None


def test_adapter_retains_raw_result_and_emits_pending_unverified_candidate():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        provider_request_id="request-43",
        retrieved_at=datetime(2026, 9, 8, 10, 30, tzinfo=timezone.utc),
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url=(
            "https://m.instagram.com/Alice.Example/tagged/?hl=en"
        ),
        title="Alice Example (@alice.example)",
        snippet="Public Instagram result.",
    )

    candidates = instagram_candidates_from_evidence(
        query, [evidence], provenance
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.profile_url == "https://www.instagram.com/alice.example/"
    assert candidate.handle == "alice.example"
    assert candidate.evidence.source_url == evidence.source_url
    assert candidate.account_status == "candidate"
    assert candidate.identity_status == "unverified"
    assert candidate.review_status == "pending"


def test_adapter_filters_content_results_and_deduplicates_profile_variants():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T10:30:00Z",
    )
    evidence = [
        ProfileSearchEvidence(
            result_rank=1,
            source_url="https://instagram.com/alice_example/",
        ),
        ProfileSearchEvidence(
            result_rank=2,
            source_url="https://m.instagram.com/Alice_Example/reels/",
        ),
        ProfileSearchEvidence(
            result_rank=3,
            source_url="https://instagram.com/reel/ABC123/",
        ),
    ]

    candidates = instagram_candidates_from_evidence(
        query, evidence, provenance
    )

    assert len(candidates) == 1
    assert candidates[0].evidence.result_rank == 1


def test_instagram_adapter_rejects_a_query_for_another_platform():
    query = ProfileSearchQuery(
        query_id="profile-query:facebook-1",
        platform="facebook",
        query_text='site:facebook.com "alice"',
        seed_kind="username",
        seed_value="alice",
    )
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T10:30:00Z",
    )

    with pytest.raises(ValueError, match="requires an Instagram query"):
        instagram_candidates_from_evidence(query, [], provenance)
