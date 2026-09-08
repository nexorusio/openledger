# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from datetime import datetime, timezone

import pytest

from maigret.web.profile_search_contract import (
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)
from maigret.web.profile_search_tiktok import (
    parse_tiktok_profile_url,
    tiktok_candidates_from_evidence,
)


def _query():
    return ProfileSearchQuery(
        query_id="profile-query:tiktok-1",
        platform="tiktok",
        query_text='site:tiktok.com "alice_example"',
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
            "https://tiktok.com/@Alice_Example",
            "https://www.tiktok.com/@alice_example",
            "alice_example",
            "profile",
        ),
        (
            "https://m.tiktok.com/@Alice.Example?lang=id-ID",
            "https://www.tiktok.com/@alice.example",
            "alice.example",
            "profile",
        ),
        (
            "https://www.tiktok.com/@Alice_Example/video/"
            "7512345678901234567?is_from_webapp=1",
            "https://www.tiktok.com/@alice_example",
            "alice_example",
            "profile_video",
        ),
        (
            "https://tiktok.com/@alice_example/photo/7512345678901234567",
            "https://www.tiktok.com/@alice_example",
            "alice_example",
            "profile_photo",
        ),
        (
            "https://www.tiktok.com/@alice_example/live",
            "https://www.tiktok.com/@alice_example",
            "alice_example",
            "profile_live",
        ),
        (
            "https://tiktok.com/@ab",
            "https://www.tiktok.com/@ab",
            "ab",
            "profile",
        ),
    ],
)
def test_tiktok_profile_forms_are_recognized_and_canonicalized(
    url, canonical_url, handle, kind
):
    reference = parse_tiktok_profile_url(url)

    assert reference.canonical_url == canonical_url
    assert reference.handle == handle
    assert reference.reference_kind == kind


@pytest.mark.parametrize(
    "url",
    [
        "https://tiktok.com/",
        "https://tiktok.com/alice_example",
        "https://tiktok.com/explore",
        "https://tiktok.com/search?q=alice",
        "https://tiktok.com/music/song-123456",
        "https://tiktok.com/tag/alice",
        "https://tiktok.com/@alice_example/video/",
        "https://tiktok.com/@alice_example/video/ABC123",
        "https://tiktok.com/@alice_example/photo/123",
        "https://tiktok.com/@alice_example/unknown",
        "https://tiktok.com/@alice_example/video/7512345678/extra",
        "https://tiktok.com/@@alice",
        "https://tiktok.com/@a",
        "https://tiktok.com/@.alice",
        "https://tiktok.com/@alice.",
        "https://tiktok.com/@alice..example",
        "https://tiktok.com/@alice-example",
        "https://tiktok.com:8443/@alice_example",
        "https://vm.tiktok.com/ZMabcdef/",
        "https://vt.tiktok.com/ZMabcdef/",
        "http://tiktok.com/@alice_example",
        "https://user:secret@tiktok.com/@alice_example",
    ],
)
def test_non_profile_or_ambiguous_tiktok_urls_are_rejected(url):
    assert parse_tiktok_profile_url(url) is None


def test_adapter_retains_video_result_and_emits_pending_unverified_candidate():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        provider_request_id="request-45",
        retrieved_at=datetime(2026, 9, 8, 11, 30, tzinfo=timezone.utc),
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url=(
            "https://m.tiktok.com/@Alice.Example/video/7512345678901234567"
            "?is_from_webapp=1"
        ),
        title="Alice Example on TikTok",
        snippet="Public TikTok result.",
    )

    candidates = tiktok_candidates_from_evidence(
        query, [evidence], provenance
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.profile_url == "https://www.tiktok.com/@alice.example"
    assert candidate.handle == "alice.example"
    assert candidate.evidence.source_url == evidence.source_url
    assert candidate.account_status == "candidate"
    assert candidate.identity_status == "unverified"
    assert candidate.review_status == "pending"


def test_adapter_filters_non_profile_results_and_deduplicates_variants():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T11:30:00Z",
    )
    evidence = [
        ProfileSearchEvidence(
            result_rank=1,
            source_url="https://tiktok.com/@alice_example",
        ),
        ProfileSearchEvidence(
            result_rank=2,
            source_url=(
                "https://m.tiktok.com/@Alice_Example/video/7512345678901234567"
            ),
        ),
        ProfileSearchEvidence(
            result_rank=3,
            source_url="https://tiktok.com/search?q=alice_example",
        ),
    ]

    candidates = tiktok_candidates_from_evidence(query, evidence, provenance)

    assert len(candidates) == 1
    assert candidates[0].evidence.result_rank == 1


def test_tiktok_adapter_rejects_a_query_for_another_platform():
    query = ProfileSearchQuery(
        query_id="profile-query:threads-1",
        platform="threads",
        query_text='site:threads.com "alice"',
        seed_kind="username",
        seed_value="alice",
    )
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T11:30:00Z",
    )

    with pytest.raises(ValueError, match="requires a TikTok query"):
        tiktok_candidates_from_evidence(query, [], provenance)
