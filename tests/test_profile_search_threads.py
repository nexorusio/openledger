# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from datetime import datetime, timezone

import pytest

from maigret.web.profile_search_contract import (
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)
from maigret.web.profile_search_threads import (
    parse_threads_profile_url,
    threads_candidates_from_evidence,
)


def _query():
    return ProfileSearchQuery(
        query_id="profile-query:threads-1",
        platform="threads",
        query_text='site:threads.com "alice_example"',
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
            "https://threads.com/@Alice_Example",
            "https://www.threads.com/@alice_example",
            "alice_example",
            "profile",
        ),
        (
            "https://www.threads.net/@Alice.Example/?hl=en",
            "https://www.threads.com/@alice.example",
            "alice.example",
            "profile",
        ),
        (
            "https://threads.com/@alice_example/replies",
            "https://www.threads.com/@alice_example",
            "alice_example",
            "profile_tab",
        ),
        (
            "https://threads.net/@alice_example/media/",
            "https://www.threads.com/@alice_example",
            "alice_example",
            "profile_tab",
        ),
        (
            "https://www.threads.com/@Alice_Example/post/C123_abc",
            "https://www.threads.com/@alice_example",
            "alice_example",
            "profile_post",
        ),
        (
            "https://threads.com/@a",
            "https://www.threads.com/@a",
            "a",
            "profile",
        ),
    ],
)
def test_threads_profile_forms_are_recognized_and_canonicalized(
    url, canonical_url, handle, kind
):
    reference = parse_threads_profile_url(url)

    assert reference.canonical_url == canonical_url
    assert reference.handle == handle
    assert reference.reference_kind == kind


@pytest.mark.parametrize(
    "url",
    [
        "https://threads.com/",
        "https://threads.com/alice_example",
        "https://threads.com/t/C123abc",
        "https://threads.com/search?q=alice",
        "https://threads.com/intent/post?text=hello",
        "https://threads.com/@alice_example/post/",
        "https://threads.com/@alice_example/post/a",
        "https://threads.com/@alice_example/unknown",
        "https://threads.com/@alice_example/post/C123/extra",
        "https://threads.com/@@alice",
        "https://threads.com/@.alice",
        "https://threads.com/@alice.",
        "https://threads.com/@alice..example",
        "https://threads.com/@alice-example",
        "https://threads.com:8443/@alice_example",
        "https://l.threads.net/?u=https://example.test",
        "http://threads.com/@alice_example",
        "https://user:secret@threads.com/@alice_example",
    ],
)
def test_non_profile_or_ambiguous_threads_urls_are_rejected(url):
    assert parse_threads_profile_url(url) is None


def test_adapter_retains_post_result_and_emits_pending_unverified_candidate():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        provider_request_id="request-44",
        retrieved_at=datetime(2026, 9, 8, 11, 0, tzinfo=timezone.utc),
    )
    evidence = ProfileSearchEvidence(
        result_rank=1,
        source_url=(
            "https://www.threads.net/@Alice.Example/post/C123_abc?hl=en"
        ),
        title="Alice Example on Threads",
        snippet="Public Threads result.",
    )

    candidates = threads_candidates_from_evidence(
        query, [evidence], provenance
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.profile_url == "https://www.threads.com/@alice.example"
    assert candidate.handle == "alice.example"
    assert candidate.evidence.source_url == evidence.source_url
    assert candidate.account_status == "candidate"
    assert candidate.identity_status == "unverified"
    assert candidate.review_status == "pending"


def test_adapter_filters_non_profile_results_and_deduplicates_domains():
    query = _query()
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider="brave",
        retrieved_at="2026-09-08T11:00:00Z",
    )
    evidence = [
        ProfileSearchEvidence(
            result_rank=1,
            source_url="https://threads.com/@alice_example",
        ),
        ProfileSearchEvidence(
            result_rank=2,
            source_url="https://threads.net/@Alice_Example/replies",
        ),
        ProfileSearchEvidence(
            result_rank=3,
            source_url="https://threads.com/t/C123abc",
        ),
    ]

    candidates = threads_candidates_from_evidence(query, evidence, provenance)

    assert len(candidates) == 1
    assert candidates[0].evidence.result_rank == 1


def test_threads_adapter_rejects_a_query_for_another_platform():
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
        retrieved_at="2026-09-08T11:00:00Z",
    )

    with pytest.raises(ValueError, match="requires a Threads query"):
        threads_candidates_from_evidence(query, [], provenance)
