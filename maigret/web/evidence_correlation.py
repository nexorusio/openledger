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

_RETRIEVAL_CONTEXT_FIELDS = (
    "observation_id",
    "retrieved_at",
    "originating_query",
    "originating_query_fingerprint",
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


def _retrieval_context(observation: Dict[str, Any]) -> Dict[str, str]:
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
        _add_relationship(
            relationship_map,
            relationship,
            explicit=True,
            explicit_ids=explicit_ids,
        )

    cluster_observations: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for observation in observations_by_id.values():
        cluster_observations[observation["cluster_id"]].append(observation)
    for items in cluster_observations.values():
        items.sort(key=lambda item: item["observation_id"])
        for left, right in combinations(items, 2):
            relationship = _auto_relationship(case_id, left, right)
            if relationship is not None:
                _add_relationship(
                    relationship_map,
                    relationship,
                    explicit=False,
                    explicit_ids=explicit_ids,
                )
                if len(relationship_map) > MAX_CORRELATION_OUTPUT_RELATIONSHIPS:
                    raise CorrelationContractError(
                        "Correlated relationships exceed the output limit of "
                        f"{MAX_CORRELATION_OUTPUT_RELATIONSHIPS}"
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
        source_count, confidence_score, confidence_basis = _cluster_confidence(
            items, normalized_relationships
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
        "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
        "case_id": case_id,
        "clusters": clusters,
        "relationships": normalized_relationships,
    }
    _validate_output(output)
    return output
