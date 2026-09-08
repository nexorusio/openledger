# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import pytest

from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_candidates import (
    PROFILE_SEARCH_ADAPTERS,
    candidates_from_profile_search_run,
    merge_profile_search_candidates,
    merge_profile_search_runs,
)
from maigret.web.profile_search_contract import (
    ProfileSearchCandidate,
    ProfileSearchError,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)


PLATFORM_URLS = {
    "facebook": "https://m.facebook.com/Alice.Example/posts/42?ref=search",
    "instagram": "https://m.instagram.com/Alice_Example/reels/?hl=en",
    "threads": "https://threads.net/@Alice_Example/post/C123abc?x=1",
    "tiktok": "https://m.tiktok.com/@Alice_Example/video/12345?lang=en",
    "x": "https://mobile.twitter.com/AliceExample/status/12345?s=20",
}


def _query(platform="instagram", query_id="profile-query:1"):
    return ProfileSearchQuery(
        query_id=query_id,
        platform=platform,
        query_text=f'site:{platform}.com "alice_example"',
        seed_kind="alias",
        seed_value="alice_example",
        max_results=5,
    )


def _run(
    *,
    platform="instagram",
    query_id="profile-query:1",
    provider="brave",
    url=None,
    rank=1,
    title="",
):
    query = _query(platform, query_id)
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider=provider,
        retrieved_at="2026-09-08T10:00:00Z",
        provider_request_id=f"{provider}-request",
    )
    evidence = ProfileSearchEvidence(
        result_rank=rank,
        source_url=url or PLATFORM_URLS[platform],
        title=title,
    )
    return ProfileSearchRun(
        query=query,
        provenance=provenance,
        evidence=(evidence,),
    )


@pytest.mark.parametrize("platform", sorted(PLATFORM_URLS))
def test_registry_dispatches_every_supported_platform(platform):
    run = _run(platform=platform)

    candidates = candidates_from_profile_search_run(run)

    assert set(PROFILE_SEARCH_ADAPTERS) == set(PLATFORM_URLS)
    assert len(candidates) == 1
    assert candidates[0].platform == platform
    assert candidates[0].evidence.source_url == PLATFORM_URLS[platform]


def test_registry_ignores_failed_or_incomplete_runs():
    run = _run()
    error = ProfileSearchError(
        query_id=run.query.query_id,
        provider="brave",
        code="request_failed",
        message="Search request failed.",
        retryable=True,
        occurred_at="2026-09-08T10:00:00Z",
    )

    assert candidates_from_profile_search_run(
        ProfileSearchRun(run.query, None, (), error)
    ) == ()
    assert candidates_from_profile_search_run(
        ProfileSearchRun(run.query, None, run.evidence)
    ) == ()


def test_merge_preserves_cross_query_and_provider_observations():
    first = _run(
        query_id="profile-query:1",
        provider="brave",
        url="https://instagram.com/alice_example/?utm_source=search",
    )
    second = _run(
        query_id="profile-query:2",
        provider="example-search",
        url="https://m.instagram.com/Alice_Example/reels/?hl=en",
    )

    merged = merge_profile_search_runs((second, first))

    assert len(merged) == 1
    assert merged[0].profile_url == (
        "https://www.instagram.com/alice_example/"
    )
    assert merged[0].source_count == 2
    assert merged[0].query_count == 2
    assert {item.provenance.provider for item in merged[0].observations} == {
        "brave",
        "example-search",
    }
    assert {item.evidence.source_url for item in merged[0].observations} == {
        "https://instagram.com/alice_example/?utm_source=search",
        "https://m.instagram.com/Alice_Example/reels/?hl=en",
    }


def test_merge_deduplicates_exact_observation_and_keeps_best_rank():
    run = _run(url="https://instagram.com/alice_example/?ref=search")
    candidate = candidates_from_profile_search_run(run)[0]
    worse = ProfileSearchCandidate(
        candidate_id=candidate.candidate_id,
        query_id=candidate.query_id,
        platform=candidate.platform,
        profile_url=candidate.profile_url,
        handle=candidate.handle,
        evidence=ProfileSearchEvidence(
            result_rank=4,
            source_url=candidate.evidence.source_url,
            title="Worse duplicate",
        ),
        provenance=candidate.provenance,
    )

    merged = merge_profile_search_candidates((worse, candidate))

    assert merged[0].source_count == 1
    assert merged[0].observations[0].evidence.result_rank == 1


def test_same_handle_on_different_platforms_remains_separate():
    instagram = _run(platform="instagram")
    x = _run(platform="x", query_id="profile-query:2")

    merged = merge_profile_search_runs((instagram, x))

    assert len(merged) == 2
    assert {item.platform for item in merged} == {"instagram", "x"}
    assert len({item.candidate_id for item in merged}) == 2


def test_canonical_variants_merge_but_raw_urls_remain_auditable():
    runs = (
        _run(
            query_id="profile-query:1",
            provider="brave",
            url="https://twitter.com/AliceExample?utm_source=one",
            platform="x",
        ),
        _run(
            query_id="profile-query:2",
            provider="example-search",
            url="https://mobile.twitter.com/aliceexample/status/12345?s=20",
            platform="x",
        ),
        _run(
            query_id="profile-query:3",
            provider="third-search",
            url="https://x.com/AliceExample/media",
            platform="x",
        ),
    )

    merged = merge_profile_search_runs(runs)
    serialized = merged[0].as_dict()

    assert len(merged) == 1
    assert serialized["profile_url"] == "https://x.com/aliceexample"
    assert serialized["alternate_profile_urls"] == []
    assert serialized["source_count"] == 3
    assert {
        item["evidence"]["source_url"] for item in serialized["observations"]
    } == {run.evidence[0].source_url for run in runs}


def test_merge_order_is_deterministic_for_candidates_and_observations():
    runs = (
        _run(
            query_id="profile-query:3",
            provider="z-search",
            url="https://instagram.com/zoe/",
            rank=3,
        ),
        _run(
            query_id="profile-query:2",
            provider="a-search",
            url="https://instagram.com/alice_example/tagged/",
            rank=2,
        ),
        _run(
            query_id="profile-query:1",
            provider="z-search",
            url="https://instagram.com/alice_example/",
            rank=1,
        ),
    )

    forward = merge_profile_search_runs(runs)
    reverse = merge_profile_search_runs(reversed(runs))

    assert [item.as_dict() for item in forward] == [
        item.as_dict() for item in reverse
    ]
    assert [item.handle for item in forward] == ["alice_example", "zoe"]
    assert [
        item.evidence.result_rank for item in forward[0].observations
    ] == [1, 2]
