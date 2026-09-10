# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Black-box acceptance tests for the P3 evidence-correlation contract."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from maigret.web.evidence_correlation_contract import (
    EVIDENCE_CORRELATION_SCHEMA_VERSION,
    EVIDENCE_OUTCOMES,
    EVIDENCE_RELATIONSHIPS,
    CorrelationContractError,
    canonical_profile_identity,
    evidence_cluster_id,
    evidence_observation_id,
    evidence_relationship_id,
    normalize_evidence_observation,
    normalize_evidence_relationship,
)

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "evidence_correlation"
EXPECTED_OUTCOMES = {
    "observed",
    "absent",
    "private",
    "blocked",
    "rate_limited",
    "parser_error",
    "provider_error",
    "indeterminate",
}
EXPECTED_RELATIONSHIPS = {
    "supporting",
    "duplicate",
    "conflicting",
    "unrelated",
}


def _load_fixture(name):
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def observations():
    return _load_fixture("observations.json")


@pytest.fixture(scope="module")
def outcomes():
    return _load_fixture("outcomes.json")


@pytest.fixture(scope="module")
def relationship_scenarios():
    return _load_fixture("relationships.json")


def _observation_with(base, **updates):
    payload = copy.deepcopy(base)
    payload.update(updates)
    return payload


def _relationship(left, right, relationship_kind, basis="Bounded basis."):
    return {
        "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
        "case_id": left["case_id"],
        "left_case_id": left["case_id"],
        "right_case_id": right["case_id"],
        "left_observation_id": evidence_observation_id(left),
        "right_observation_id": evidence_observation_id(right),
        "relationship_kind": relationship_kind,
        "basis": basis,
    }


def _assert_no_automatic_approval(value):
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in {"auto_approved", "automatic_approval"}
            if key == "review_status":
                assert item != "approved"
            _assert_no_automatic_approval(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_automatic_approval(item)


def test_contract_exposes_the_frozen_taxonomy():
    assert EVIDENCE_CORRELATION_SCHEMA_VERSION == 1
    assert set(EVIDENCE_OUTCOMES) == EXPECTED_OUTCOMES
    assert set(EVIDENCE_RELATIONSHIPS) == EXPECTED_RELATIONSHIPS


def test_instagram_url_variants_have_one_canonical_identity(observations):
    identities = [
        canonical_profile_identity(item["claim_value"])
        for item in observations["instagram_variants"]
    ]

    assert {item["platform"] for item in identities} == {"instagram"}
    assert {item["handle"] for item in identities} == {"alice.example"}
    assert {item["canonical_url"] for item in identities} == {
        "https://www.instagram.com/alice.example/"
    }


def test_x_and_legacy_twitter_urls_have_one_canonical_identity(observations):
    identities = [
        canonical_profile_identity(item["claim_value"])
        for item in observations["x_variants"]
    ]

    assert {item["platform"] for item in identities} == {"x"}
    assert {item["handle"] for item in identities} == {"alice_example"}
    assert {item["canonical_url"] for item in identities} == {
        "https://x.com/alice_example"
    }


def test_same_handle_on_different_platforms_never_merges(observations):
    instagram, x_account = observations["same_handle_different_platforms"]

    assert canonical_profile_identity(instagram["claim_value"]) != (
        canonical_profile_identity(x_account["claim_value"])
    )
    assert evidence_cluster_id(instagram) != evidence_cluster_id(x_account)


def test_cross_source_claims_share_a_cluster_but_keep_attribution(observations):
    payloads = observations["cross_source_claim"]
    normalized = [normalize_evidence_observation(item) for item in payloads]

    assert len({item["cluster_id"] for item in normalized}) == 1
    assert len({item["observation_id"] for item in normalized}) == 4
    assert all(
        item["canonical_profile_identity"]
        == canonical_profile_identity(original["claim_value"])
        for original, item in zip(payloads, normalized)
    )
    assert {item["source_id"] for item in normalized} == {
        "native.profile-search",
        "maigret",
        "user-scanner",
        "ai.cited-web",
    }
    for original, retained in zip(payloads, normalized):
        assert retained["source_id"] == original["source_id"]
        assert retained["source_record_id"] == original["source_record_id"]
        assert retained["source_version"] == original["source_version"]
        assert retained["citations"] == original["citations"]
        assert retained["retrieved_at"] == original["retrieved_at"].replace(
            "+00:00", "Z"
        )
        assert retained["originating_query"] == original["originating_query"]
        assert retained["originating_query_fingerprint"] == (
            original["originating_query_fingerprint"]
        )
        assert retained["source_snapshot_sha256"] == (
            original["source_snapshot_sha256"]
        )
        assert retained["source_snapshot_ref"] == (original["source_snapshot_ref"])
        _assert_no_automatic_approval(retained)


def test_equivalent_profile_claims_do_not_duplicate_one_source_observation(
    observations,
):
    first, variant = copy.deepcopy(observations["instagram_variants"])
    for field_name in (
        "source_id",
        "source_version",
        "source_record_id",
        "outcome",
        "native_outcome",
        "native_status",
        "source_snapshot_sha256",
        "source_snapshot_ref",
    ):
        variant[field_name] = first[field_name]

    assert evidence_cluster_id(first) == evidence_cluster_id(variant)
    assert evidence_observation_id(first) == evidence_observation_id(variant)


def test_rerun_context_does_not_inflate_stable_identity(observations):
    reruns = observations["reruns"]
    original = normalize_evidence_observation(reruns["original"])
    repeated = normalize_evidence_observation(reruns["changed_retrieval_context"])

    assert repeated["retrieved_at"] != original["retrieved_at"]
    assert repeated["originating_query"] != original["originating_query"]
    assert repeated["originating_query_fingerprint"] != (
        original["originating_query_fingerprint"]
    )
    assert repeated["observation_id"] == original["observation_id"]
    assert repeated["cluster_id"] == original["cluster_id"]
    assert len({original["observation_id"], repeated["observation_id"]}) == 1
    assert len({original["cluster_id"], repeated["cluster_id"]}) == 1


def test_changed_snapshot_is_new_observation_in_the_same_cluster(observations):
    reruns = observations["reruns"]
    original = normalize_evidence_observation(reruns["original"])
    changed = normalize_evidence_observation(reruns["changed_snapshot"])

    assert changed["source_snapshot_sha256"] != (original["source_snapshot_sha256"])
    assert changed["observation_id"] != original["observation_id"]
    assert changed["cluster_id"] == original["cluster_id"]
    assert len({original["observation_id"], changed["observation_id"]}) == 2
    assert len({original["cluster_id"], changed["cluster_id"]}) == 1


def test_stable_id_helpers_match_normalized_derived_fields(observations):
    payload = observations["reruns"]["original"]
    normalized = normalize_evidence_observation(payload)

    assert normalized["observation_id"] == evidence_observation_id(payload)
    assert normalized["cluster_id"] == evidence_cluster_id(payload)
    assert normalized["observation_id"].startswith("evidence-observation:")
    assert normalized["cluster_id"].startswith("evidence-cluster:")


def test_cluster_identity_is_case_scoped(observations):
    original = observations["reruns"]["original"]
    other_case = _observation_with(original, case_id="case:other")

    assert evidence_cluster_id(original) != evidence_cluster_id(other_case)


def test_profile_cluster_identity_is_claim_type_scoped(observations):
    canonical = observations["reruns"]["original"]
    differently_typed = _observation_with(
        canonical,
        claim_type="source_specific.profile",
    )

    assert evidence_cluster_id(canonical) != evidence_cluster_id(differently_typed)


def test_distinct_native_outcomes_remain_distinct(outcomes, observations):
    base = observations["reruns"]["original"]
    retained = []
    for index, scenario in enumerate(outcomes):
        payload = _observation_with(
            base,
            **scenario,
            source_record_id=f"outcome-{index}",
            source_snapshot_sha256=(f"sha256:{index + 1:064x}"),
        )
        retained.append(normalize_evidence_observation(payload))

    assert {item["outcome"] for item in retained} == EXPECTED_OUTCOMES
    for scenario, normalized in zip(outcomes, retained):
        assert normalized["native_outcome"] == scenario["native_outcome"]
        assert normalized["native_status"] == scenario["native_status"]
    assert (
        next(item for item in retained if item["outcome"] == "indeterminate")["outcome"]
        != "absent"
    )


def test_confidence_is_bounded_explainable_and_never_an_approval(observations):
    payload = _observation_with(
        observations["reruns"]["original"],
        confidence={
            "scope": "correlation",
            "score": 72,
            "basis": [
                "Canonical profile identity agrees.",
                "Independent source lineage is retained.",
            ],
        },
    )

    normalized = normalize_evidence_observation(payload)

    assert normalized["confidence"] == payload["confidence"]
    _assert_no_automatic_approval(normalized)


@pytest.mark.parametrize(
    "confidence",
    [
        {"scope": "identity", "score": 72, "basis": ["Unsupported scope."]},
        {"scope": "correlation", "score": 101, "basis": ["Too large."]},
        {"scope": "correlation", "score": True, "basis": ["Not numeric."]},
        {"scope": "correlation", "score": 72, "basis": []},
    ],
)
def test_confidence_rejects_unbounded_or_unexplained_values(
    observations,
    confidence,
):
    payload = _observation_with(
        observations["reruns"]["original"], confidence=confidence
    )

    with pytest.raises(CorrelationContractError, match="confidence"):
        normalize_evidence_observation(payload)


def test_relationship_taxonomy_is_normalized_and_stable(
    observations,
    relationship_scenarios,
):
    first, second = observations["cross_source_claim"][:2]
    relationship_ids = set()

    for scenario in relationship_scenarios:
        payload = _relationship(first, second, **scenario)
        normalized = normalize_evidence_relationship(payload)
        reversed_payload = {
            **payload,
            "left_observation_id": payload["right_observation_id"],
            "right_observation_id": payload["left_observation_id"],
        }

        assert normalized["relationship_kind"] == scenario["relationship_kind"]
        assert normalized["basis"] == scenario["basis"]
        assert normalized["relationship_id"] == evidence_relationship_id(payload)
        assert normalized["relationship_id"].startswith("evidence-relationship:")
        assert evidence_relationship_id(reversed_payload) == (
            normalized["relationship_id"]
        )
        assert normalized["left_observation_id"] < normalized["right_observation_id"]
        _assert_no_automatic_approval(normalized)
        relationship_ids.add(normalized["relationship_id"])

    assert len(relationship_ids) == len(EXPECTED_RELATIONSHIPS)


def test_supporting_duplicate_conflicting_and_unrelated_examples(observations):
    supporting_left, supporting_right = observations["cross_source_claim"][:2]
    duplicate_right = _observation_with(
        supporting_left,
        source_id="archive-mirror",
        source_record_id="archive-copy-42",
    )
    conflicting_right = _observation_with(
        supporting_left,
        claim_value="https://instagram.com/alice_other/",
        source_record_id="conflicting-42",
        source_snapshot_sha256=f"sha256:{'b' * 64}",
    )
    unrelated_right = observations["same_handle_different_platforms"][1]

    scenarios = (
        (supporting_left, supporting_right, "supporting"),
        (supporting_left, duplicate_right, "duplicate"),
        (supporting_left, conflicting_right, "conflicting"),
        (supporting_left, unrelated_right, "unrelated"),
    )
    normalized = [
        normalize_evidence_relationship(_relationship(left, right, kind))
        for left, right, kind in scenarios
    ]

    assert evidence_cluster_id(supporting_left) == evidence_cluster_id(supporting_right)
    assert evidence_cluster_id(supporting_left) == evidence_cluster_id(duplicate_right)
    assert evidence_cluster_id(supporting_left) != evidence_cluster_id(
        conflicting_right
    )
    assert evidence_cluster_id(supporting_left) != evidence_cluster_id(unrelated_right)
    assert [item["relationship_kind"] for item in normalized] == [
        "supporting",
        "duplicate",
        "conflicting",
        "unrelated",
    ]


@pytest.mark.parametrize(
    "value",
    [
        "http://instagram.com/alice_example/",
        "https://operator:secret@instagram.com/alice_example/",
        "https://localhost/alice_example/",
        "https://127.0.0.1/alice_example/",
        "https://instagram.com:8443/alice_example/",
        "https://example.test/alice_example/",
        "https://instagram.com/p/1234567890/",
    ],
)
def test_canonical_profile_identity_fails_closed_for_unsafe_urls(value):
    assert canonical_profile_identity(value) is None


@pytest.mark.parametrize(
    "value",
    [
        "http://instagram.com/alice_example/",
        "https://operator:secret@instagram.com/alice_example/",
        "https://localhost/alice_example/",
        "https://127.0.0.1/alice_example/",
        "https://instagram.com:8443/alice_example/",
        "https://example.test/alice_example/",
        "https://instagram.com/p/1234567890/",
    ],
)
def test_profile_observations_reject_unsafe_or_unsupported_values(
    observations,
    value,
):
    payload = _observation_with(observations["reruns"]["original"], claim_value=value)

    with pytest.raises(CorrelationContractError, match="claim_value|profile"):
        normalize_evidence_observation(payload)


@pytest.mark.parametrize(
    ("updates", "match"),
    [
        ({"schema_version": 2}, "schema"),
        ({"case_id": "x" * 129}, "case_id"),
        ({"claim_value": "x" * 2_001}, "claim_value"),
        ({"outcome": "not_found"}, "outcome"),
        ({"retrieved_at": "2026-09-09T04:00:00"}, "timezone"),
        (
            {"originating_query_fingerprint": "sha256:not-a-hash"},
            "originating_query_fingerprint",
        ),
        (
            {"source_snapshot_sha256": "sha256:not-a-hash"},
            "source_snapshot_sha256",
        ),
        ({"source_snapshot_ref": "x" * 2_001}, "source_snapshot_ref"),
        (
            {"source_snapshot_ref": "https://user:secret@example.org/snapshot"},
            "source_snapshot_ref",
        ),
        ({"citations": [{}]}, "citation"),
        ({"unexpected": "value"}, "unsupported"),
        ({"api_key": "do-not-store"}, "credential|unsupported"),
        ({"review_status": "approved"}, "unsupported|review"),
        ({"auto_approved": True}, "unsupported|approval"),
    ],
)
def test_observations_reject_invalid_unbounded_or_approval_inputs(
    observations,
    updates,
    match,
):
    payload = _observation_with(observations["reruns"]["original"], **updates)

    with pytest.raises(CorrelationContractError, match=match):
        normalize_evidence_observation(payload)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.org/evidence",
        "https://operator:secret@example.org/evidence",
        "https://localhost/evidence",
        "https://10.0.0.1/evidence",
        "https://example.org/evidence?access_token=secret",
        "https://example.org/evidence?api_key=secret",
    ],
)
def test_observations_reject_unsafe_citation_urls(observations, url):
    payload = copy.deepcopy(observations["reruns"]["original"])
    payload["citations"][0]["url"] = url

    with pytest.raises(CorrelationContractError):
        normalize_evidence_observation(payload)


def test_observations_reject_unbounded_citation_sets(observations):
    payload = copy.deepcopy(observations["reruns"]["original"])
    payload["citations"] = [
        {"url": f"https://example.org/evidence/{index}", "title": "Evidence"}
        for index in range(33)
    ]

    with pytest.raises(CorrelationContractError, match="citations"):
        normalize_evidence_observation(payload)


def test_supplied_derived_ids_cannot_be_tampered(observations):
    base = observations["reruns"]["original"]
    tampered = f"evidence-observation:{'0' * 64}"

    with pytest.raises(CorrelationContractError, match="observation_id"):
        normalize_evidence_observation(_observation_with(base, observation_id=tampered))
    with pytest.raises(CorrelationContractError, match="cluster_id"):
        normalize_evidence_observation(
            _observation_with(base, cluster_id=f"evidence-cluster:{'0' * 64}")
        )


def test_relationships_reject_self_reference(observations):
    original = observations["reruns"]["original"]
    payload = _relationship(original, original, "duplicate")

    with pytest.raises(CorrelationContractError, match="self"):
        normalize_evidence_relationship(payload)


def test_relationships_reject_cross_case_endpoints(observations):
    original = observations["reruns"]["original"]
    other_case = _observation_with(original, case_id="case:other")
    payload = _relationship(original, other_case, "unrelated")

    with pytest.raises(CorrelationContractError, match="case"):
        normalize_evidence_relationship(payload)


@pytest.mark.parametrize(
    ("updates", "match"),
    [
        ({"relationship_kind": "derived"}, "relationship"),
        ({"basis": "x" * 1_001}, "basis"),
        (
            {"left_observation_id": "evidence-observation:not-a-hash"},
            "left_observation_id",
        ),
        ({"review_status": "approved"}, "unsupported|review"),
        ({"credential": "do-not-store"}, "credential|unsupported"),
    ],
)
def test_relationships_reject_invalid_or_unbounded_inputs(
    observations,
    updates,
    match,
):
    left, right = observations["cross_source_claim"][:2]
    payload = {**_relationship(left, right, "supporting"), **updates}

    with pytest.raises(CorrelationContractError, match=match):
        normalize_evidence_relationship(payload)
