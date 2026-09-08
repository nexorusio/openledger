# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from datetime import datetime, timezone

import pytest

from maigret.web.profile_search_contract import (
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)
from maigret.web.profile_search_x import (
    parse_x_profile_url,
    x_candidates_from_evidence,
)


def _query():
    return ProfileSearchQuery(
        query_id="profile-query:x-1",
        platform="x",
        query_text='site:x.com "alice_example"',
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
            "https://x.com/Alice_Example",
            "https://x.com/alice_example",
            "alice_example",
            "profile",
        ),
        (
            "https://twitter.com/AliceExample?lang=en",
            "https://x.com/aliceexample",
            "aliceexample",
            "profile",
        ),
        (
            "https://mobile.twitter.com/AliceExample/with_replies",
            "https://x.com/aliceexample",
            "aliceexample",
            "profile_tab",
        ),
        (
            "https://www.x.com/AliceExample/media",
            "https://x.com/aliceexample",
            "aliceexample",
            "profile_tab",
        ),
        (
            "https://twitter.com/AliceExample/status/1912345678901234567",
            "https://x.com/aliceexample",
            "aliceexample",
            "profile_status",
        ),
        (
            "https://x.com/AliceExample/statuses/1912345678901234567/"
            "photo/1?ref_src=twsrc",
            "https://x.com/aliceexample",
            "aliceexample",
            "profile_status_media",
        ),
        (
            "https://x.com/a",
            "https://x.com/a",
            "a",
            "profile",
        ),
    ],
)
def test_x_profile_forms_are_recognized_and_canonicalized(
    url, canonical_url, handle, kind
):
    reference = parse_x_profile_url(url)

    assert reference.canonical_url == canonical_url
    assert reference.handle == handle
    assert reference.reference_kind == kind


@pytest.mark.parametrize(
    "url",
    [
        "https://x.com/",
        "https://x.com/home",
        "https://x.com/explore",
        "https://x.com/search?q=alice",
        "https://x.com/i/web/status/1912345678901234567",
        "https://twitter.com/intent/user?screen_name=alice",
        "https://twitter.com/share?url=https://example.test",
        "https://x.com/AliceExample/status/",
        "https://x.com/AliceExample/status/abc",
        "https://x.com/AliceExample/status/123",
        "https://x.com/AliceExample/status/1912345678901234567/photo/5",
        "https://x.com/AliceExample/status/1912345678901234567/extra",
        "https://x.com/AliceExample/unknown",
        "https://x.com/@alice",
        "https://x.com/alice.example",
        "https://x.com/alice-example",
        "https://x.com/abcdefghijklmnop",
        "https://x.com:8443/AliceExample",
        "https://t.co/abcdef",
        "https://api.x.com/AliceExample",
        "http://x.com/AliceExample",
        "https://user:secret@x.com/AliceExample",
    ],
)
def test_non_profile_or_ambiguous_x_urls_are_rejected(url):
    assert parse_x_profile_url(url) is None


def test_adapter_retains_legacy_status_and_emits_pending_candidate():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        provider_request_id="request-46",
        retrieved_at=datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc),
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url=(
            "https://mobile.twitter.com/AliceExample/status/"
            "1912345678901234567?ref_src=twsrc"
        ),
        title="Alice Example on X",
        snippet="Public legacy Twitter result.",
    )

    candidates = x_candidates_from_evidence(query, [evidence], provenance)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.profile_url == "https://x.com/aliceexample"
    assert candidate.handle == "aliceexample"
    assert candidate.evidence.source_url == evidence.source_url
    assert candidate.account_status == "candidate"
    assert candidate.identity_status == "unverified"
    assert candidate.review_status == "pending"


def test_adapter_filters_system_results_and_deduplicates_legacy_domains():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T12:00:00Z",
    )
    evidence = [
        ProfileSearchEvidence(
            result_rank=1,
            source_url="https://x.com/AliceExample",
        ),
        ProfileSearchEvidence(
            result_rank=2,
            source_url=(
                "https://twitter.com/aliceexample/status/1912345678901234567"
            ),
        ),
        ProfileSearchEvidence(
            result_rank=3,
            source_url="https://x.com/i/web/status/1912345678901234567",
        ),
    ]

    candidates = x_candidates_from_evidence(query, evidence, provenance)

    assert len(candidates) == 1
    assert candidates[0].evidence.result_rank == 1


def test_x_adapter_rejects_a_query_for_another_platform():
    query = ProfileSearchQuery(
        query_id="profile-query:tiktok-1",
        platform="tiktok",
        query_text='site:tiktok.com "alice"',
        seed_kind="username",
        seed_value="alice",
    )
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T12:00:00Z",
    )

    with pytest.raises(ValueError, match="requires an X query"):
        x_candidates_from_evidence(query, [], provenance)
