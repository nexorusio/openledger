# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import copy
from itertools import combinations

import pytest

from maigret.web.evidence_correlation import (
    _auto_relationship,
    _cluster_confidence,
    correlate_evidence,
)
from maigret.web.evidence_correlation_contract import (
    CorrelationContractError,
    normalize_evidence_observation,
)


def observation(index, *, cluster=0, snapshot=None, source=None):
    return {
        "schema_version": 1,
        "case_id": "case-p3r",
        "claim_type": "profile",
        "claim_value": f"https://x.com/p3r_{cluster}",
        "source_id": source or f"source-{index}",
        "source_version": "p3r",
        "source_record_id": f"record-{index}",
        "outcome": "observed",
        "native_outcome": "found",
        "native_status": "200",
        "citations": [
            {"url": f"https://evidence.example.org/p3r/{index}", "title": "P3R"}
        ],
        "retrieved_at": f"2026-09-{index % 28 + 1:02d}T00:00:00Z",
        "originating_query": f"p3r query {index}",
        "originating_query_fingerprint": f"sha256:{index:064x}",
        "source_snapshot_sha256": f"sha256:{(snapshot if snapshot is not None else index + 10000):064x}",
        "source_snapshot_ref": f"evidence://openledger/p3r/{index}",
    }


def relationship(left, right, kind):
    return {
        "schema_version": 1,
        "case_id": "case-p3r",
        "left_observation_id": left,
        "left_case_id": "case-p3r",
        "right_observation_id": right,
        "right_case_id": "case-p3r",
        "relationship_kind": kind,
        "basis": f"Explicit {kind} P3R override.",
    }


def dense(count, *, cluster_size=None, same_snapshot=False):
    cluster_size = cluster_size or count
    return [
        observation(
            index,
            cluster=(index - 1) // cluster_size,
            snapshot=7 if same_snapshot else None,
        )
        for index in range(1, count + 1)
    ]


def test_small_pair_projection_remains_the_v1_envelope():
    result = correlate_evidence(dense(100))

    assert result["schema_version"] == 1
    assert set(result) == {"schema_version", "case_id", "clusters", "relationships"}
    assert len(result["relationships"]) == 4_950


def test_dense_101_uses_compact_support_membership_without_loss():
    result = correlate_evidence(dense(101))

    assert result["schema_version"] == 2
    assert result["relationships"] == []
    assert result["relationship_projection"] == {
        "mode": "compact",
        "automatic_relationship_counts": {
            "duplicate": 0,
            "supporting": 5_050,
            "total": 5_050,
        },
        "materialized_automatic_relationship_count": 0,
    }
    compact = result["compact_relationships"]
    assert compact["duplicate_memberships"] == []
    assert len(compact["supporting_memberships"]) == 1
    assert len(compact["supporting_memberships"][0]["observation_ids"]) == 101
    assert len(result["clusters"][0]["observations"]) == 101
    assert len(result["clusters"][0]["retrieval_contexts"]) == 101


def test_dense_duplicate_membership_preserves_confidence_without_pair_projection():
    records = dense(1_000, same_snapshot=True)
    result = correlate_evidence(records)
    cluster = result["clusters"][0]

    assert result["schema_version"] == 2
    assert result["relationship_projection"]["automatic_relationship_counts"] == {
        "duplicate": 499_500,
        "supporting": 0,
        "total": 499_500,
    }
    assert len(result["compact_relationships"]["duplicate_memberships"][0]["observation_ids"]) == 1_000
    assert cluster["independent_observed_source_count"] == 1
    assert cluster["confidence"]["score"] == 40


def test_dispersed_1000_observations_still_uses_complete_pair_projection():
    result = correlate_evidence(dense(1_000, cluster_size=10))

    assert result["schema_version"] == 1
    assert len(result["relationships"]) == 4_500
    assert sum(len(cluster["observations"]) for cluster in result["clusters"]) == 1_000


@pytest.mark.parametrize(
    "relationship_kind", ["conflicting", "unrelated", "supporting", "duplicate"]
)
def test_compact_excluded_pairs_preserve_every_explicit_override(
    relationship_kind,
):
    records = dense(101, same_snapshot=True)
    first, second = (normalize_evidence_observation(item) for item in records[:2])
    explicit = relationship(
        first["observation_id"], second["observation_id"], relationship_kind
    )
    result = correlate_evidence(records, [explicit])

    excluded = result["compact_relationships"]["excluded_pairs"]
    assert len(result["relationships"]) == 1
    assert result["relationships"][0]["relationship_kind"] == relationship_kind
    assert excluded[0]["left_observation_id"] == min(
        first["observation_id"], second["observation_id"]
    )
    assert result["relationship_projection"]["automatic_relationship_counts"]["duplicate"] == 5_049
    assert result["clusters"][0]["confidence"]["score"] == (
        20 if relationship_kind == "conflicting" else 40
    )


def test_compact_confidence_matches_the_complete_automatic_relationship_semantics():
    records = dense(101, same_snapshot=True)
    result = correlate_evidence(records)
    normalized = [normalize_evidence_observation(item) for item in records]
    full_relationships = [
        inferred
        for left, right in combinations(normalized, 2)
        if (inferred := _auto_relationship("case-p3r", left, right)) is not None
    ]
    source_count, score, basis = _cluster_confidence(normalized, full_relationships)

    assert result["clusters"][0]["independent_observed_source_count"] == source_count
    assert result["clusters"][0]["confidence"] == {
        "scope": "correlation",
        "score": score,
        "basis": basis,
    }


def test_compact_output_is_idempotent_under_repetition_and_permutation():
    records = dense(101, same_snapshot=True)
    initial = correlate_evidence(records)
    repeated = correlate_evidence(list(reversed(records)) + [copy.deepcopy(records[0])])

    assert repeated == initial


def test_declared_1000_observation_limit_still_fails_explicitly():
    with pytest.raises(CorrelationContractError, match="limit of 1000"):
        correlate_evidence(dense(1_001))
