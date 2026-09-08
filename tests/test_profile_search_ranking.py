# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import pytest

from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_candidates import (
    ProfileSearchCandidateGroup,
    merge_profile_search_runs,
)
from maigret.web.profile_search_contract import (
    ProfileSearchCandidate,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)
from maigret.web.profile_search_ranking import (
    rank_profile_search_candidates,
)


def _run(
    *,
    query_id,
    handle="alice_example",
    seed_kind="alias",
    seed_value="alice_example",
    seed_score=90,
    provider="brave",
    rank=1,
    source_url=None,
):
    query = ProfileSearchQuery(
        query_id=query_id,
        platform="instagram",
        query_text=f'site:instagram.com "{seed_value}"',
        seed_kind=seed_kind,
        seed_value=seed_value,
        seed_score=seed_score,
        seed_reason="Bounded test seed",
        max_results=5,
    )
    provenance = ProfileSearchProvenance.for_query(
        query,
        provider=provider,
        retrieved_at="2026-09-08T11:00:00Z",
        provider_request_id=f"{provider}-request",
    )
    evidence = ProfileSearchEvidence(
        result_rank=rank,
        source_url=(
            source_url
            or f"https://www.instagram.com/{handle}/"
        ),
    )
    return ProfileSearchRun(query, provenance, (evidence,))


def _rank(*runs):
    return rank_profile_search_candidates(
        merge_profile_search_runs(runs),
        queries=(run.query for run in runs),
    )


def test_exact_approved_username_match_has_stronger_review_priority():
    approved = _run(
        query_id="profile-query:approved",
        handle="alice_approved",
        seed_kind="confirmed_username",
        seed_value="Alice_Approved",
        seed_score=100,
    )
    alias = _run(
        query_id="profile-query:alias",
        handle="alice_alias",
        seed_kind="alias",
        seed_value="alice_alias",
        seed_score=80,
    )

    ranked = _rank(alias, approved)

    assert [item.candidate.handle for item in ranked] == [
        "alice_approved",
        "alice_alias",
    ]
    assert ranked[0].discovery_score > ranked[1].discovery_score
    assert ranked[0].ranking_signals[0].code == (
        "exact_confirmed_username_match"
    )


def test_normalized_aliases_and_additional_seeds_correlate_one_candidate():
    runs = (
        _run(
            query_id="profile-query:1",
            seed_kind="alias",
            seed_value="alice.example",
            seed_score=96,
        ),
        _run(
            query_id="profile-query:2",
            seed_kind="alias",
            seed_value="Alice Example",
            seed_score=82,
            source_url="https://m.instagram.com/Alice_Example/tagged/",
        ),
    )

    result = _rank(*runs)[0]

    assert result.candidate.query_count == 2
    assert result.ranking_signals[0].code == "normalized_alias_match"
    assert any(
        signal.code == "additional_matching_seeds"
        for signal in result.ranking_signals
    )


def test_profile_url_seed_extracts_handle_without_live_url_resolution():
    run = _run(
        query_id="profile-query:url",
        seed_kind="profile_url",
        seed_value="https://x.com/Alice_Example/status/12345",
        seed_score=100,
    )

    result = _rank(run)[0]

    assert result.ranking_signals[0].code == "exact_profile_url_match"


def test_profile_url_seed_accepts_handle_already_extracted_from_input():
    run = _run(
        query_id="profile-query:extracted-url",
        seed_kind="profile_url",
        seed_value="alice_example",
        seed_score=100,
    )

    result = _rank(run)[0]

    assert result.ranking_signals[0].code == "exact_profile_url_match"


def test_profile_url_seed_rejects_unrecognized_hosts():
    run = _run(
        query_id="profile-query:url",
        seed_kind="profile_url",
        seed_value="https://untrusted.example/alice_example",
        seed_score=100,
    )

    result = _rank(run)[0]

    assert result.ranking_signals[0].code == "search_result_only"


def test_provider_repeatability_is_bounded_and_not_identity_verification():
    runs = tuple(
        _run(
            query_id="profile-query:shared",
            provider=provider,
            source_url=f"https://instagram.com/alice_example/?ref={index}",
        )
        for index, provider in enumerate(
            ("brave", "provider-two", "provider-three", "provider-four")
        )
    )

    result = _rank(*runs)[0]
    signal = next(
        item
        for item in result.ranking_signals
        if item.code == "provider_repeatability"
    )

    assert signal.points == 8
    assert "not identity verification" in signal.detail
    assert result.candidate.identity_status == "unverified"
    assert result.candidate.review_status == "pending"


def test_nonmatching_search_result_stays_low_priority_without_fuzzy_guessing():
    run = _run(
        query_id="profile-query:name",
        handle="unrelated_account",
        seed_kind="full_name",
        seed_value="Alice Example",
        seed_score=70,
    )

    result = _rank(run)[0]

    assert result.discovery_score == 15
    assert result.review_priority == "low"
    assert result.ranking_signals[0].code == "search_result_only"


def test_search_position_breaks_otherwise_equal_candidate_priority():
    first = _run(
        query_id="profile-query:first",
        handle="alice_first",
        seed_value="alice_first",
        rank=1,
    )
    fourth = _run(
        query_id="profile-query:fourth",
        handle="alice_fourth",
        seed_value="alice_fourth",
        rank=4,
    )

    ranked = _rank(fourth, first)

    assert [item.candidate.handle for item in ranked] == [
        "alice_first",
        "alice_fourth",
    ]
    assert ranked[0].discovery_score - ranked[1].discovery_score == 3


def test_serialized_score_is_explainable_and_never_changes_identity_state():
    result = _rank(
        _run(
            query_id="profile-query:approved",
            seed_kind="confirmed_username",
            seed_score=100,
        )
    )[0]
    serialized = result.as_dict()

    assert serialized["ranking_model_version"] == 1
    assert serialized["score_scope"] == "discovery_review_priority"
    assert serialized["discovery_score"] == sum(
        item["points"] for item in serialized["ranking_signals"]
    )
    assert serialized["account_status"] == "candidate"
    assert serialized["identity_status"] == "unverified"
    assert serialized["review_status"] == "pending"


def test_ranking_is_deterministic_and_score_is_capped_at_one_hundred():
    seeds = (
        "alice_example",
        "alice.example",
        "Alice Example",
        "@alice_example",
        "aliceexample",
        "alice-example",
        "ALICE_EXAMPLE",
        "Alice.Example",
    )
    runs = tuple(
        _run(
            query_id=f"profile-query:{index}",
            seed_kind="confirmed_username",
            seed_value=seed,
            seed_score=100,
            provider=f"provider-{index}",
            source_url=(
                "https://instagram.com/alice_example/"
                f"?source={index}"
            ),
        )
        for index, seed in enumerate(seeds)
    )

    forward = _rank(*runs)
    reverse = _rank(*reversed(runs))

    assert forward[0].discovery_score == 100
    assert forward[0].discovery_score == sum(
        signal.points for signal in forward[0].ranking_signals
    )
    assert [item.as_dict() for item in forward] == [
        item.as_dict() for item in reverse
    ]


def test_ranking_rejects_missing_or_conflicting_query_lineage():
    run = _run(query_id="profile-query:1")
    candidate = merge_profile_search_runs((run,))[0]

    with pytest.raises(ValueError, match="no query definition"):
        rank_profile_search_candidates((candidate,), queries=())

    wrong_query = ProfileSearchQuery(
        query_id=run.query.query_id,
        platform="instagram",
        query_text='site:instagram.com "different"',
        seed_kind="alias",
        seed_value="different",
        seed_score=90,
    )
    with pytest.raises(ValueError, match="conflicting query lineage"):
        rank_profile_search_candidates(
            (candidate,), queries=(wrong_query,)
        )


def test_ranking_rejects_forged_cross_platform_candidate_observation():
    run = _run(query_id="profile-query:1")
    original = merge_profile_search_runs((run,))[0].observations[0]
    forged = ProfileSearchCandidate(
        candidate_id=original.candidate_id,
        query_id=original.query_id,
        platform="x",
        profile_url="https://x.com/alice_example",
        handle=original.handle,
        evidence=original.evidence,
        provenance=original.provenance,
    )

    forged_group = ProfileSearchCandidateGroup(
        candidate_id=forged.candidate_id,
        platform="instagram",
        profile_url=original.profile_url,
        handle=original.handle,
        observations=(forged,),
    )

    with pytest.raises(ValueError, match="conflicting account identity"):
        rank_profile_search_candidates(
            (forged_group,), queries=(run.query,)
        )
