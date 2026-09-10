"""Deterministic, case-scoped correlation for normalized evidence envelopes."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from itertools import combinations
from typing import Any, Dict, List, Sequence

from maigret.web.evidence_correlation_contract import (
    EVIDENCE_CORRELATION_SCHEMA_VERSION,
    EVIDENCE_OUTCOMES,
    CorrelationContractError,
    normalize_evidence_observation,
    normalize_evidence_relationship,
)


MAX_CORRELATION_OBSERVATIONS = 1_000
MAX_CORRELATION_RELATIONSHIPS = 5_000
MAX_CORRELATION_OUTPUT_RELATIONSHIPS = 5_000
MAX_CORRELATION_OUTPUT_BYTES = 96 * 1024 * 1024
COMPACT_CORRELATION_RESULT_SCHEMA_VERSION = 2

_RETRIEVAL_CONTEXT_FIELDS = (
    "observation_id",
    "citations",
    "retrieved_at",
    "originating_query",
    "originating_query_fingerprint",
    "source_snapshot_sha256",
    "source_snapshot_ref",
)


def _bounded_records(value: Any, name: str, limit: int) -> List[Any]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(
        value, Iterable
    ):
        raise CorrelationContractError(f"{name} must be an iterable of objects")
    records = []
    for record in value:
        if len(records) == limit:
            raise CorrelationContractError(f"{name} exceeds the limit of {limit}")
        records.append(record)
    return records


def _json_key(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _canonical_observation(variants: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Choose one complete normalized envelope without input-order preference."""
    return min(variants, key=_json_key)


def _retrieval_context(observation: Dict[str, Any]) -> Dict[str, Any]:
    return {field: observation[field] for field in _RETRIEVAL_CONTEXT_FIELDS}


def _auto_relationship(
    case_id: str,
    left: Dict[str, Any],
    right: Dict[str, Any],
) -> Dict[str, Any] | None:
    if left["source_snapshot_sha256"] == right["source_snapshot_sha256"]:
        relationship_kind = "duplicate"
        basis = "Observations share an identical source snapshot hash."
    elif (
        left["outcome"] == "observed"
        and right["outcome"] == "observed"
        and left["source_id"] != right["source_id"]
    ):
        relationship_kind = "supporting"
        basis = "Independent observed sources support the same normalized claim."
    else:
        return None
    return normalize_evidence_relationship(
        {
            "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
            "case_id": case_id,
            "left_observation_id": left["observation_id"],
            "left_case_id": case_id,
            "right_observation_id": right["observation_id"],
            "right_case_id": case_id,
            "relationship_kind": relationship_kind,
            "basis": basis,
        }
    )


class _SourceGroups:
    """Small union-find used to keep duplicate sources from adding confidence."""

    def __init__(self, source_ids: Iterable[str]) -> None:
        self._parent = {source_id: source_id for source_id in source_ids}

    def find(self, source_id: str) -> str:
        parent = self._parent[source_id]
        while parent != self._parent[parent]:
            parent = self._parent[parent]
        while source_id != parent:
            next_source = self._parent[source_id]
            self._parent[source_id] = parent
            source_id = next_source
        return parent

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        low, high = sorted((left_root, right_root))
        self._parent[high] = low

    @property
    def count(self) -> int:
        return len({self.find(source_id) for source_id in self._parent})


def _cluster_confidence(
    observations: Sequence[Dict[str, Any]],
    relationships: Sequence[Dict[str, Any]],
    automatic_duplicate_memberships: Sequence[Sequence[str]] = (),
) -> tuple[int, int, List[str]]:
    observed = {
        observation["observation_id"]: observation
        for observation in observations
        if observation["outcome"] == "observed"
    }
    sources = _SourceGroups(
        observation["source_id"] for observation in observed.values()
    )
    observation_ids = set(observation["observation_id"] for observation in observations)
    conflict_ids = set()
    explicit_pairs = {
        tuple(
            sorted(
                (
                    relationship["left_observation_id"],
                    relationship["right_observation_id"],
                )
            )
        )
        for relationship in relationships
    }

    for relationship in relationships:
        left_id = relationship["left_observation_id"]
        right_id = relationship["right_observation_id"]
        if relationship["relationship_kind"] == "duplicate":
            left = observed.get(left_id)
            right = observed.get(right_id)
            if left is not None and right is not None:
                sources.union(left["source_id"], right["source_id"])
        if relationship["relationship_kind"] == "conflicting" and (
            left_id in observation_ids or right_id in observation_ids
        ):
            conflict_ids.add(relationship["relationship_id"])

    # Compact duplicate memberships describe the same automatic duplicate
    # semantics as the legacy pair list.  Iterate only for union-find state;
    # do not materialize relationship records.  Explicit pairs retain their
    # existing precedence over automatic inference.
    for observation_ids in automatic_duplicate_memberships:
        for left_id, right_id in combinations(observation_ids, 2):
            if tuple(sorted((left_id, right_id))) in explicit_pairs:
                continue
            left = observed.get(left_id)
            right = observed.get(right_id)
            if left is not None and right is not None:
                sources.union(left["source_id"], right["source_id"])

    source_count = sources.count
    conflict_count = len(conflict_ids)
    if source_count == 0:
        support_score = 0
        basis = ["No observed source signals contribute to correlation confidence."]
    else:
        uncapped = 40 + (15 * (source_count - 1))
        support_score = min(85, uncapped)
        basis = [
            f"{source_count} independent observed source(s) contribute "
            f"{support_score} support point(s)."
        ]
        if uncapped > 85:
            basis.append("Supporting-source confidence is capped at 85.")
    if conflict_count:
        basis.append(
            f"{conflict_count} explicit conflicting relationship(s) subtract "
            f"{20 * conflict_count} point(s)."
        )
    score = max(0, support_score - (20 * conflict_count))
    return source_count, score, basis


def _add_relationship(
    relationships_by_id: Dict[str, Dict[str, Any]],
    relationship: Dict[str, Any],
    *,
    explicit: bool,
    explicit_ids: set[str],
) -> None:
    relationship_id = relationship["relationship_id"]
    existing = relationships_by_id.get(relationship_id)
    if existing is None:
        relationships_by_id[relationship_id] = relationship
    elif explicit and (
        relationship_id not in explicit_ids or relationship["basis"] < existing["basis"]
    ):
        relationships_by_id[relationship_id] = relationship
    if explicit:
        explicit_ids.add(relationship_id)


def _validate_output(output: Dict[str, Any]) -> None:
    try:
        encoded = json.dumps(
            output,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CorrelationContractError(
            "Evidence correlation output contains a non-JSON value"
        ) from exc
    if len(encoded) > MAX_CORRELATION_OUTPUT_BYTES:
        raise CorrelationContractError("Evidence correlation output is too large")


def _automatic_relationship_counts(
    items: Sequence[Dict[str, Any]],
    explicit_pairs: set[tuple[str, str]],
) -> Dict[str, int]:
    """Count inferred pairs without constructing their relationship records."""
    duplicates_by_snapshot: Dict[str, int] = defaultdict(int)
    observed_by_source: Dict[str, int] = defaultdict(int)
    observed_by_snapshot: Dict[str, int] = defaultdict(int)
    observed_by_source_snapshot: Dict[tuple[str, str], int] = defaultdict(int)

    for item in items:
        duplicates_by_snapshot[item["source_snapshot_sha256"]] += 1
        if item["outcome"] == "observed":
            observed_by_source[item["source_id"]] += 1
            observed_by_snapshot[item["source_snapshot_sha256"]] += 1
            observed_by_source_snapshot[
                (item["source_id"], item["source_snapshot_sha256"])
            ] += 1

    duplicate_count = sum(
        count * (count - 1) // 2 for count in duplicates_by_snapshot.values()
    )
    observed_count = sum(observed_by_source.values())
    supporting_count = observed_count * (observed_count - 1) // 2
    supporting_count -= sum(
        count * (count - 1) // 2 for count in observed_by_source.values()
    )
    supporting_count -= sum(
        count * (count - 1) // 2 for count in observed_by_snapshot.values()
    )
    supporting_count += sum(
        count * (count - 1) // 2
        for count in observed_by_source_snapshot.values()
    )

    by_id = {item["observation_id"]: item for item in items}
    for left_id, right_id in explicit_pairs:
        left = by_id.get(left_id)
        right = by_id.get(right_id)
        if left is None or right is None:
            continue
        relationship_kind = _auto_relationship_kind(left, right)
        if relationship_kind == "duplicate":
            duplicate_count -= 1
        elif relationship_kind == "supporting":
            supporting_count -= 1
    return {"duplicate": duplicate_count, "supporting": supporting_count}


def _auto_relationship_kind(
    left: Dict[str, Any], right: Dict[str, Any]
) -> str | None:
    if left["source_snapshot_sha256"] == right["source_snapshot_sha256"]:
        return "duplicate"
    if (
        left["outcome"] == "observed"
        and right["outcome"] == "observed"
        and left["source_id"] != right["source_id"]
    ):
        return "supporting"
    return None


def _compact_relationships(
    cluster_observations: Mapping[str, Sequence[Dict[str, Any]]],
    explicit_pair_overrides: Mapping[tuple[str, str], Sequence[str]],
) -> Dict[str, Any]:
    """Describe automatic inference by memberships rather than every pair.

    Consumers apply the existing automatic rules to members, with any explicit
    relationship for a pair taking precedence.  Observations in clusters retain
    their full retrieval contexts and source provenance separately.
    """
    duplicate_memberships = []
    supporting_memberships = []
    for cluster_id in sorted(cluster_observations):
        items = cluster_observations[cluster_id]
        by_snapshot: Dict[str, List[str]] = defaultdict(list)
        observed_ids = []
        for item in items:
            by_snapshot[item["source_snapshot_sha256"]].append(
                item["observation_id"]
            )
            if item["outcome"] == "observed":
                observed_ids.append(item["observation_id"])
        for snapshot_sha256 in sorted(by_snapshot):
            observation_ids = sorted(by_snapshot[snapshot_sha256])
            if len(observation_ids) > 1:
                duplicate_memberships.append(
                    {
                        "cluster_id": cluster_id,
                        "source_snapshot_sha256": snapshot_sha256,
                        "observation_ids": observation_ids,
                    }
                )
        if len(observed_ids) > 1:
            supporting_memberships.append(
                {
                    "cluster_id": cluster_id,
                    "observation_ids": sorted(observed_ids),
                }
            )
    return {
        "representation": "membership-v1",
        "explicit_pair_overrides": True,
        "duplicate_memberships": duplicate_memberships,
        "supporting_memberships": supporting_memberships,
        "excluded_pairs": [
            {
                "left_observation_id": pair[0],
                "right_observation_id": pair[1],
                "relationship_ids": sorted(relationship_ids),
            }
            for pair, relationship_ids in sorted(explicit_pair_overrides.items())
        ],
    }


def correlate_evidence(
    observations: Iterable[Mapping[str, Any]],
    relationships: Iterable[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    """Correlate bounded evidence without changing any analyst review state."""
    raw_observations = _bounded_records(
        observations, "observations", MAX_CORRELATION_OBSERVATIONS
    )
    if not raw_observations:
        raise CorrelationContractError("At least one evidence observation is required")
    raw_relationships = _bounded_records(
        relationships, "relationships", MAX_CORRELATION_RELATIONSHIPS
    )

    variants_by_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    case_ids = set()
    for payload in raw_observations:
        observation = normalize_evidence_observation(payload)
        variants_by_id[observation["observation_id"]].append(observation)
        case_ids.add(observation["case_id"])
    if len(case_ids) != 1:
        raise CorrelationContractError(
            "Evidence correlation observations must belong to one case"
        )
    case_id = next(iter(case_ids))

    observations_by_id = {
        observation_id: _canonical_observation(variants)
        for observation_id, variants in variants_by_id.items()
    }
    relationship_map: Dict[str, Dict[str, Any]] = {}
    explicit_ids: set[str] = set()
    explicit_pairs: set[tuple[str, str]] = set()
    for payload in raw_relationships:
        relationship = normalize_evidence_relationship(payload)
        if relationship["case_id"] != case_id:
            raise CorrelationContractError(
                "Evidence correlation relationships must belong to the observation case"
            )
        endpoints = (
            relationship["left_observation_id"],
            relationship["right_observation_id"],
        )
        if any(endpoint not in observations_by_id for endpoint in endpoints):
            raise CorrelationContractError(
                "Evidence relationship endpoint is absent from observations"
            )
        explicit_pairs.add(tuple(sorted(endpoints)))
        _add_relationship(
            relationship_map,
            relationship,
            explicit=True,
            explicit_ids=explicit_ids,
        )

    cluster_observations: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for observation in observations_by_id.values():
        cluster_observations[observation["cluster_id"]].append(observation)
    explicit_pairs_by_cluster: Dict[str, set[tuple[str, str]]] = defaultdict(set)
    explicit_pair_relationship_ids: Dict[tuple[str, str], List[str]] = defaultdict(
        list
    )
    for relationship in relationship_map.values():
        pair = tuple(
            sorted(
                (
                    relationship["left_observation_id"],
                    relationship["right_observation_id"],
                )
            )
        )
        explicit_pair_relationship_ids[pair].append(relationship["relationship_id"])
        left_cluster_id = observations_by_id[pair[0]]["cluster_id"]
        if left_cluster_id == observations_by_id[pair[1]]["cluster_id"]:
            explicit_pairs_by_cluster[left_cluster_id].add(pair)

    automatic_counts = {"duplicate": 0, "supporting": 0}
    for items in cluster_observations.values():
        items.sort(key=lambda item: item["observation_id"])
        counts = _automatic_relationship_counts(
            items, explicit_pairs_by_cluster[items[0]["cluster_id"]]
        )
        for relationship_kind, count in counts.items():
            automatic_counts[relationship_kind] += count

    automatic_relationship_count = sum(automatic_counts.values())
    compact_projection = (
        len(relationship_map) + automatic_relationship_count
        > MAX_CORRELATION_OUTPUT_RELATIONSHIPS
    )
    if not compact_projection:
        for items in cluster_observations.values():
            for left, right in combinations(items, 2):
                pair = tuple(sorted((left["observation_id"], right["observation_id"])))
                if pair in explicit_pairs:
                    continue
                relationship = _auto_relationship(case_id, left, right)
                if relationship is not None:
                    _add_relationship(
                        relationship_map,
                        relationship,
                        explicit=False,
                        explicit_ids=explicit_ids,
                    )

    normalized_relationships = [
        relationship_map[relationship_id]
        for relationship_id in sorted(relationship_map)
    ]
    clusters = []
    for cluster_id in sorted(cluster_observations):
        items = cluster_observations[cluster_id]
        first = items[0]
        profile_identity = first["canonical_profile_identity"]
        canonical_value = (
            profile_identity["canonical_url"]
            if profile_identity is not None
            else " ".join(first["claim_value"].split()).casefold()
        )
        contexts_by_key = {}
        for variants in (
            variants_by_id[observation["observation_id"]] for observation in items
        ):
            for variant in variants:
                context = _retrieval_context(variant)
                contexts_by_key[_json_key(context)] = context
        retrieval_contexts = [contexts_by_key[key] for key in sorted(contexts_by_key)]
        outcome_counts = {
            outcome: sum(item["outcome"] == outcome for item in items)
            for outcome in sorted(EVIDENCE_OUTCOMES)
        }
        automatic_duplicate_memberships = ()
        if compact_projection:
            automatic_duplicate_memberships = tuple(
                tuple(
                    item["observation_id"]
                    for item in items
                    if item["source_snapshot_sha256"] == snapshot_sha256
                )
                for snapshot_sha256 in sorted(
                    {item["source_snapshot_sha256"] for item in items}
                )
            )
        source_count, confidence_score, confidence_basis = _cluster_confidence(
            items, normalized_relationships, automatic_duplicate_memberships
        )
        clusters.append(
            {
                "cluster_id": cluster_id,
                "case_id": case_id,
                "claim_type": first["claim_type"],
                "canonical_value": canonical_value,
                "canonical_profile_identity": profile_identity,
                "observations": items,
                "retrieval_contexts": retrieval_contexts,
                "outcome_counts": outcome_counts,
                "independent_observed_source_count": source_count,
                "confidence": {
                    "scope": "correlation",
                    "score": confidence_score,
                    "basis": confidence_basis,
                },
            }
        )

    output = {
        "schema_version": (
            COMPACT_CORRELATION_RESULT_SCHEMA_VERSION
            if compact_projection
            else EVIDENCE_CORRELATION_SCHEMA_VERSION
        ),
        "case_id": case_id,
        "clusters": clusters,
        "relationships": normalized_relationships,
    }
    if compact_projection:
        compact_overrides = {
            pair: relationship_ids
            for pair, relationship_ids in explicit_pair_relationship_ids.items()
            if _auto_relationship_kind(
                observations_by_id[pair[0]], observations_by_id[pair[1]]
            )
            is not None
        }
        output["relationship_projection"] = {
            "mode": "compact",
            "automatic_relationship_counts": {
                **automatic_counts,
                "total": automatic_relationship_count,
            },
            "materialized_automatic_relationship_count": 0,
        }
        output["compact_relationships"] = _compact_relationships(
            cluster_observations, compact_overrides
        )
    _validate_output(output)
    return output
