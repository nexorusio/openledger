# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import copy
import json

import pytest

from maigret.web.evidence_correlation import correlate_evidence
from maigret.web.evidence_correlation_contract import (
    EVIDENCE_OUTCOMES,
    CorrelationContractError,
    normalize_evidence_observation,
)


def observation(sequence=1, **overrides):
    payload = {
        "schema_version": 1,
        "case_id": "case-123",
        "claim_type": "profile",
        "claim_value": "https://x.com/Alice_Example",
        "source_id": f"source-{sequence}",
        "source_version": "2026.09",
        "source_record_id": f"record-{sequence}",
        "outcome": "observed",
        "native_outcome": "found",
        "native_status": "200",
        "citations": [
            {
                "url": f"https://evidence.example.org/{sequence}",
                "title": f"Evidence {sequence}",
            }
        ],
        "retrieved_at": f"2026-09-{sequence:02d}T01:00:00Z",
        "originating_query": f"Alice query {sequence}",
        "originating_query_fingerprint": f"sha256:{sequence:064x}",
        "source_snapshot_sha256": f"sha256:{(sequence + 100):064x}",
        "source_snapshot_ref": f"evidence://openledger/audit/{sequence}",
    }
    payload.update(overrides)
    return payload


def relationship(left, right, kind="supporting", **overrides):
    payload = {
        "schema_version": 1,
        "case_id": "case-123",
        "left_observation_id": left,
        "left_case_id": "case-123",
        "right_observation_id": right,
        "right_case_id": "case-123",
        "relationship_kind": kind,
        "basis": f"Explicit {kind} relationship.",
    }
    payload.update(overrides)
    return payload


def test_aliases_cluster_with_canonical_profile_representative():
    result = correlate_evidence(
        [
            observation(claim_value="https://twitter.com/Alice_Example/status/12345"),
            observation(2, claim_value="https://x.com/alice_example"),
        ]
    )

    assert len(result["clusters"]) == 1
    cluster = result["clusters"][0]
    assert cluster["canonical_value"] == "https://x.com/alice_example"
    assert cluster["canonical_profile_identity"] == {
        "platform": "x",
        "handle": "alice_example",
        "canonical_url": "https://x.com/alice_example",
    }
    assert len(cluster["observations"]) == 2
    assert result["relationships"][0]["relationship_kind"] == "supporting"


def test_independent_support_is_attributable_and_confidence_is_explainable():
    result = correlate_evidence([observation(), observation(2), observation(3)])
    cluster = result["clusters"][0]

    assert cluster["independent_observed_source_count"] == 3
    assert cluster["confidence"]["scope"] == "correlation"
    assert cluster["confidence"]["score"] == 70
    assert "3 independent observed source(s)" in cluster["confidence"]["basis"][0]
    assert len(result["relationships"]) == 3
    assert {
        relationship["relationship_kind"] for relationship in result["relationships"]
    } == {"supporting"}
    assert {item["source_id"] for item in cluster["observations"]} == {
        "source-1",
        "source-2",
        "source-3",
    }


def test_duplicate_snapshot_does_not_inflate_confidence():
    duplicate = observation(
        2,
        source_snapshot_sha256=observation()["source_snapshot_sha256"],
    )
    result = correlate_evidence([observation(), duplicate])

    cluster = result["clusters"][0]
    assert cluster["independent_observed_source_count"] == 1
    assert cluster["confidence"]["score"] == 40
    assert [item["relationship_kind"] for item in result["relationships"]] == [
        "duplicate"
    ]


def test_explicit_conflicts_remain_visible_and_reduce_confidence():
    first = normalize_evidence_observation(observation())
    second = normalize_evidence_observation(observation(2))
    result = correlate_evidence(
        [first, second],
        [
            relationship(
                first["observation_id"], second["observation_id"], "conflicting"
            )
        ],
    )

    assert {item["relationship_kind"] for item in result["relationships"]} == {
        "conflicting"
    }
    assert result["clusters"][0]["confidence"]["score"] == 35
    assert any(
        "explicit conflicting relationship" in item
        for item in result["clusters"][0]["confidence"]["basis"]
    )


def test_same_observation_retains_distinct_snapshot_and_citation_contexts():
    first = observation()
    second = copy.deepcopy(first)
    second.update(
        citations=[
            {
                "url": "https://evidence.example.org/alternate",
                "title": "Alternate immutable citation",
            }
        ],
        source_snapshot_ref="evidence://openledger/audit/alternate",
    )

    cluster = correlate_evidence([first, second])["clusters"][0]

    assert len(cluster["observations"]) == 1
    assert len(cluster["retrieval_contexts"]) == 2
    assert {item["source_snapshot_ref"] for item in cluster["retrieval_contexts"]} == {
        first["source_snapshot_ref"],
        second["source_snapshot_ref"],
    }
    assert {item["citations"][0]["url"] for item in cluster["retrieval_contexts"]} == {
        first["citations"][0]["url"],
        second["citations"][0]["url"],
    }


@pytest.mark.parametrize("outcome", sorted(EVIDENCE_OUTCOMES))
def test_all_outcomes_are_retained_exactly(outcome):
    result = correlate_evidence(
        [
            observation(
                outcome=outcome,
                native_outcome=f"native-{outcome}",
                native_status=f"status-{outcome}",
            )
        ]
    )

    cluster = result["clusters"][0]
    assert cluster["observations"][0]["outcome"] == outcome
    assert cluster["outcome_counts"] == {
        candidate: int(candidate == outcome) for candidate in sorted(EVIDENCE_OUTCOMES)
    }
    assert cluster["confidence"]["score"] == (40 if outcome == "observed" else 0)


def test_rerun_folds_observation_and_preserves_unique_retrieval_contexts():
    first = observation()
    rerun = observation(
        retrieved_at="2026-09-20T05:00:00Z",
        originating_query="Second query",
        originating_query_fingerprint=f"sha256:{999:064x}",
    )
    result = correlate_evidence([first, rerun, copy.deepcopy(rerun)])

    cluster = result["clusters"][0]
    assert len(cluster["observations"]) == 1
    assert len(cluster["retrieval_contexts"]) == 2
    assert {item["originating_query"] for item in cluster["retrieval_contexts"]} == {
        "Alice query 1",
        "Second query",
    }
    assert cluster["outcome_counts"]["observed"] == 1
    assert cluster["confidence"]["score"] == 40


def test_changed_snapshot_is_a_distinct_observation_in_the_same_cluster():
    changed = observation(
        source_snapshot_sha256=f"sha256:{999:064x}",
        source_snapshot_ref="evidence://openledger/audit/changed",
    )
    result = correlate_evidence([observation(), changed])

    assert len(result["clusters"]) == 1
    assert len(result["clusters"][0]["observations"]) == 2
    assert result["clusters"][0]["independent_observed_source_count"] == 1


def test_order_and_repeated_inputs_are_idempotent():
    observations = [observation(), observation(2), observation(3)]
    initial = correlate_evidence(observations)
    reordered = correlate_evidence(
        [observations[2], observations[0], observations[1], observations[0]],
        list(reversed(initial["relationships"])) + initial["relationships"],
    )

    assert reordered == initial
    json.dumps(initial, allow_nan=False)


def test_generic_claim_has_a_deterministic_canonical_value():
    result = correlate_evidence(
        [
            observation(claim_type="full_name", claim_value=" Alice   Example "),
            observation(2, claim_type="full_name", claim_value="alice example"),
        ]
    )

    cluster = result["clusters"][0]
    assert cluster["canonical_value"] == "alice example"
    assert cluster["canonical_profile_identity"] is None


def test_explicit_relationship_kinds_merge_without_inference_of_conflict_or_unrelated():
    first = normalize_evidence_observation(observation(outcome="private"))
    second = normalize_evidence_observation(observation(2, outcome="blocked"))
    explicit = [
        relationship(first["observation_id"], second["observation_id"], kind)
        for kind in ("duplicate", "conflicting", "unrelated")
    ]
    result = correlate_evidence([first, second], explicit)

    assert {item["relationship_kind"] for item in result["relationships"]} == {
        "duplicate",
        "conflicting",
        "unrelated",
    }


def test_confidence_is_capped_and_floored():
    many_sources = [observation(sequence) for sequence in range(1, 10)]
    capped = correlate_evidence(many_sources)
    assert capped["clusters"][0]["confidence"]["score"] == 85

    normalized = [normalize_evidence_observation(item) for item in many_sources]
    conflicts = [
        relationship(
            normalized[0]["observation_id"],
            item["observation_id"],
            "conflicting",
        )
        for item in normalized[1:]
    ]
    floored = correlate_evidence(normalized, conflicts)
    assert floored["clusters"][0]["confidence"]["score"] == 0


def test_rejects_empty_over_limit_cross_case_and_absent_endpoints():
    with pytest.raises(CorrelationContractError, match="At least one"):
        correlate_evidence([])
    with pytest.raises(CorrelationContractError, match="limit of 1000"):
        correlate_evidence([observation()] * 1_001)
    with pytest.raises(CorrelationContractError, match="one case"):
        correlate_evidence([observation(), observation(2, case_id="case-456")])

    first = normalize_evidence_observation(observation())
    absent = normalize_evidence_observation(observation(2))
    with pytest.raises(CorrelationContractError, match="endpoint is absent"):
        correlate_evidence(
            [first],
            [relationship(first["observation_id"], absent["observation_id"])],
        )
    with pytest.raises(CorrelationContractError, match="limit of 5000"):
        correlate_evidence(
            [first, absent],
            [relationship(first["observation_id"], absent["observation_id"])] * 5_001,
        )


def test_relationship_case_must_match_observations():
    first = normalize_evidence_observation(observation())
    second = normalize_evidence_observation(observation(2))
    cross_case = relationship(first["observation_id"], second["observation_id"])
    cross_case.update(
        {
            "case_id": "case-456",
            "left_case_id": "case-456",
            "right_case_id": "case-456",
        }
    )

    with pytest.raises(CorrelationContractError, match="observation case"):
        correlate_evidence([first, second], [cross_case])


def test_output_never_contains_an_approval_surface():
    result = correlate_evidence([observation(), observation(2)])

    assert "approval" not in json.dumps(result, sort_keys=True).casefold()
