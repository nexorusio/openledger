"""Interpretable assessment of consolidated evidence, separate from decisions.

An account finding or matching handle is not an attribution signal. Only explicit
source-backed signals can enter the model. Unknown origin independence remains
unknown; blocked and failed queries are displayed but never identity negatives.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import math
from typing import Any, Iterable, Mapping

from maigret.web.pipeline_probability import (
    EVENTS,
    FEATURE_NAMES,
    FEATURE_SCHEMA,
    SCOPE_FIELDS,
    abstention,
    canonical_json,
    digest,
    predict_probability,
    timestamp,
)
from maigret.web.pipeline_evidence import observation_evidence_role

ASSESSMENT_VERSION = "p2-assessment-v2"
POSITIVE_OUTCOMES = {
    "found",
    "candidate",
    "observed",
    "supported",
    "success",
    "claimed",
}
UNAVAILABLE_OUTCOMES = {
    "blocked",
    "inconclusive",
    "inconclusive/blocked",
    "timeout",
    "error",
    "cancelled",
    "not_executed",
    "not executed",
    "unavailable",
}
SIGNALS = {
    "source_link_match",
    "stable_id_match",
    "public_identifier_match",
    "temporal_match",
    "qualified_claim_support",
    "contradiction",
}
SIGNAL_METHODS = {
    "source_link_match": "explicit_profile_link",
    "stable_id_match": "stable_identifier_cross_reference",
    "public_identifier_match": "exact_public_identifier_cross_reference",
    "temporal_match": "dated_source_assertion",
    "qualified_claim_support": "qualified_fact_with_subject_binding",
    "contradiction": "conflicting_source_assertion",
}


def _observation_id(observation: Mapping[str, Any]) -> str:
    return str(
        observation.get("id")
        or observation.get("observation_id")
        or digest(observation)
    )


def _signals(observation: Mapping[str, Any], group: Mapping[str, Any]) -> set[str]:
    """Boolean fields/LLM confidence alone are insufficient provenance for signals."""
    payload = observation.get("payload") or {}
    signals = (
        observation.get("evidence_signals") or payload.get("evidence_signals") or {}
    )
    if not isinstance(signals, Mapping):
        return set()
    result = set()
    allowed_refs = {
        str(value)
        for value in (
            observation.get("id"),
            observation.get("observation_id"),
            observation.get("source_url"),
            observation.get("canonical_url"),
            observation.get("native_record_id"),
        )
        if value
    }
    for key in SIGNALS:
        signal = signals.get(key)
        if (
            isinstance(signal, Mapping)
            and signal.get("matched") is True
            and str(signal.get("evidence_ref")) in allowed_refs
            and signal.get("method")
            and signal.get("subject_id") == group.get("subject_id")
            and signal.get("hypothesis_key")
            in {group.get("key"), group.get("id"), group.get("group_id")} - {None}
            and signal.get("method") == SIGNAL_METHODS[key]
        ):
            result.add(key)
    return result


def _origin(observation: Mapping[str, Any]) -> str | None:
    dependence = observation.get("dependence") or {}
    if dependence.get("status") == "unknown":
        return None
    family = (
        observation.get("origin_family_id")
        or observation.get("source_origin_family")
        or observation.get("origin_family")
    )
    return str(family) if family else None


def evidence_set_digest(
    group: Mapping[str, Any], observations: Iterable[Mapping[str, Any]]
) -> str:
    """Bind assessment to the exact group revision and every retained observation."""
    # Assessment attachment itself must not make the next digest recursive.
    stable_group = {
        key: value
        for key, value in group.items()
        if key not in {"assessment", "assessments", "probability"}
    }
    records = {_observation_id(item): dict(item) for item in observations}
    return digest(
        {
            "group": stable_group,
            "observations": [records[key] for key in sorted(records)],
        }
    )


def assess_group(
    group: Mapping[str, Any],
    observations: Iterable[Mapping[str, Any]],
    *,
    as_of: datetime | str | None = None,
    probability_artifact: Mapping[str, Any] | None = None,
    artifact_sha256: str | None = None,
    scope: Mapping[str, Any] | None = None,
    expected_evidence_digest: str | None = None,
    fresh_days: int = 90,
) -> dict[str, Any]:
    """Create a JSON snapshot; never accept/reject a claim or finalize a Persona."""
    now = timestamp(as_of) if as_of is not None else datetime.now(timezone.utc)
    if now is None:
        raise ValueError("as_of must include a timezone")
    if fresh_days != 90:
        raise ValueError(
            "freshness rule is fixed by feature schema; change schema before changing it"
        )
    kind = group.get("kind", group.get("group_type"))
    if kind not in {"account", "claim"}:
        raise ValueError("assessment group kind must be account or claim")
    event = "account_attribution" if kind == "account" else "claim_correctness"
    records = {}
    for observation in observations:
        if group.get("case_id") and observation.get("case_id") != group["case_id"]:
            raise ValueError("cross-case assessment observation")
        if (
            group.get("subject_id")
            and observation.get("subject_id") != group["subject_id"]
        ):
            raise ValueError("cross-subject assessment observation")
        oid = _observation_id(observation)
        if oid in records and canonical_json(records[oid]) != canonical_json(
            observation
        ):
            raise ValueError("conflicting observation revision for one ID")
        records[oid] = dict(observation)
    rows = [records[key] for key in sorted(records)]
    evidence_digest = evidence_set_digest(group, rows)
    outcomes = Counter()
    family_records = defaultdict(list)
    family_signals = defaultdict(set)
    family_signal_observations = defaultdict(set)
    unknown = []
    unknown_ids = set()
    retained = []
    contradictions = []
    ages = {}
    missing = []
    warnings = []
    for row in rows:
        oid = _observation_id(row)
        outcome = (
            str(row.get("status", row.get("outcome", "unknown")))
            .lower()
            .replace(" ", "_")
        )
        outcomes[outcome] += 1
        # Consolidation resolves mirror content and explicit derivation links to
        # their original families without rewriting immutable source records.
        origin_map = group.get("origin_by_observation")
        origin = (
            origin_map.get(oid)
            if isinstance(origin_map, Mapping) and oid in origin_map
            else _origin(row)
        )
        if origin:
            family_records[origin].append(oid)
        else:
            unknown.append(oid)
            unknown_ids.add(oid)
        retention = row.get("retention") or {}
        retainable = (
            retention.get("mode") == "retained"
            and retention.get("final_eligible") is True
        )
        if retainable:
            retained.append(oid)
        observed = timestamp(row.get("observed_at") or row.get("retrieved_at"))
        # Published/effective dates, when provided, govern freshness; a new fetch
        # cannot turn an old archived assertion into evidence of present ownership.
        reference = (
            timestamp(row.get("effective_at") or row.get("published_at")) or observed
        )
        ages[oid] = (
            max(0, (now - reference).total_seconds() / 86400)
            if reference and reference <= now
            else None
        )
        signals = (
            _signals(row, group)
            if outcome in POSITIVE_OUTCOMES and retainable
            else set()
        )
        assertion_role = observation_evidence_role(
            row, group.get("normalized") or group
        )
        if assertion_role not in {"supports", "candidate_support"}:
            # A connector's refutation/context cannot contribute positive model
            # features. Generic polarity is surfaced but does not manufacture a
            # calibrated contradiction feature without the existing bound signal.
            signals.intersection_update({"contradiction"})
        if assertion_role == "contradicts":
            contradictions.append(
                {
                    "observation_id": oid,
                    "origin_family_id": origin,
                    "assertion_role": "contradicts",
                    "requires_operator_review": True,
                }
            )
        if "contradiction" in signals:
            contradictions.append(
                {
                    "observation_id": oid,
                    "origin_family_id": origin,
                    "signal": row.get(
                        "evidence_signals",
                        (row.get("payload") or {}).get("evidence_signals", {}),
                    ).get("contradiction"),
                }
            )
        # Failed queries do not enter model features, including a negative field.
        if origin and outcome in POSITIVE_OUTCOMES and retainable and signals:
            family_signals[origin].update(signals)
            family_signal_observations[origin].add(oid)
    features = {name: 0.0 for name in FEATURE_NAMES}
    support = {
        origin
        for origin, signals in family_signals.items()
        if signals - {"contradiction"}
    }
    conflict = {
        origin
        for origin, signals in family_signals.items()
        if "contradiction" in signals
    }
    features["support_origin_families"] = float(min(len(support), 10))
    features["contradiction_origin_families"] = float(min(len(conflict), 10))
    for name in SIGNALS - {"contradiction"}:
        features[name] = float(
            min(sum(name in signals for signals in family_signals.values()), 10)
        )
    # Freshness uses the oldest available assertion in an origin; repeating a fetch
    # of an origin cannot refresh or raise its evidence weight.
    for origin in support | conflict:
        origin_ages = [
            ages[oid]
            for oid in family_signal_observations[origin]
            if ages[oid] is not None
        ]
        if origin_ages:
            features[
                (
                    "fresh_origin_families"
                    if max(origin_ages) <= fresh_days
                    else "stale_origin_families"
                )
            ] += 1
    features["fresh_origin_families"] = min(features["fresh_origin_families"], 10.0)
    features["stale_origin_families"] = min(features["stale_origin_families"], 10.0)
    # Unknown query failures remain in the ledger, not identity features.
    features["unknown_dependence"] = float(
        any(
            _observation_id(row) in unknown_ids
            and _signals(row, group)
            and str(row.get("status", row.get("outcome", "unknown")))
            in POSITIVE_OUTCOMES
            for row in rows
        )
    )
    if not rows:
        missing.append("No source observations are available")
    if not support:
        missing.append(
            "No retainable, source-backed evidence connects this hypothesis to the subject"
        )
    if unknown:
        missing.append("Original-source dependence is unknown for some observations")
    if rows and not retained:
        missing.append("Only transient or metadata-only evidence is available")
    if support and features["fresh_origin_families"] == 0:
        missing.append("Current-dated supporting evidence is missing")
    if event == "claim_correctness" and not features["qualified_claim_support"]:
        missing.append(
            "No source-backed support for the joint subject and qualified claim event"
        )
    if outcomes.keys() & UNAVAILABLE_OUTCOMES:
        warnings.append(
            "Blocked, failed and interrupted collection outcomes are not evidence of absence or false attribution"
        )
    if outcomes.get("not_found"):
        warnings.append(
            "A not-found source query does not establish that the subject lacks the account"
        )
    conflicts = list(group.get("conflicts") or [])
    if conflicts or contradictions:
        warnings.append(
            "Conflicting evidence requires operator resolution; consolidation does not choose truth"
        )
    if expected_evidence_digest and expected_evidence_digest != evidence_digest:
        probability = abstention("stale_evidence_digest", event)
    elif any(item.get("assertion_role") == "contradicts" for item in contradictions):
        probability = abstention("unresolved_contradictory_source_assertion", event)
    elif not support:
        probability = abstention("insufficient_source_backed_evidence", event)
    elif features["fresh_origin_families"] == 0:
        probability = abstention("stale_or_undated_evidence", event)
    elif event == "claim_correctness" and not features["qualified_claim_support"]:
        probability = abstention("joint_subject_claim_event_unsupported", event)
    else:
        probability = predict_probability(
            features,
            event=event,
            scope=scope,
            artifact=probability_artifact,
            expected_sha256=artifact_sha256,
            as_of=now,
            evidence_digest=evidence_digest,
        )
    return {
        "schema_version": ASSESSMENT_VERSION,
        "assessment_id": digest(
            {
                "evidence": evidence_digest,
                "time": now.isoformat(),
                "artifact": artifact_sha256,
                "schema": ASSESSMENT_VERSION,
            }
        ),
        "group_id": group.get("id", group.get("group_id")),
        "case_id": group.get("case_id"),
        "subject_id": group.get("subject_id"),
        "event": event,
        "event_definition": EVENTS[event],
        "assessed_at": now.isoformat(),
        "evidence_digest": evidence_digest,
        "evidence_counts": {
            "observations": len(rows),
            "known_origin_families": len(family_records),
            "support_origin_families": len(support),
            "contradiction_origin_families": len(conflict),
            "unknown_origin_observations": len(unknown),
            "retainable_observations": len(retained),
        },
        "origin_families": [
            {
                "origin_family_id": origin,
                "observation_ids": sorted(ids),
                "signals": sorted(family_signals.get(origin, set())),
            }
            for origin, ids in sorted(family_records.items())
        ],
        "unknown_origin_observation_ids": sorted(unknown),
        "outcomes": dict(sorted(outcomes.items())),
        "contradictions": contradictions,
        "group_conflicts": conflicts,
        "freshness": {
            "rule": "90 days from effective/publication time, otherwise observed time",
            "reference_time": now.isoformat(),
            "age_days_by_observation": ages,
        },
        "missing_evidence": missing,
        "warnings": warnings,
        "feature_schema": FEATURE_SCHEMA,
        "features": features,
        "probability": probability,
        "evidence_status": (
            "conflicting"
            if conflicts or contradictions
            else ("source_supported" if support else "needs_evidence")
        ),
        "operator_review_available": True,
        "legacy_scores": "Heuristic confidence and discovery ranking are not probabilities",
    }


def assess_groups(
    groups: Iterable[Mapping[str, Any]],
    observations_by_group: Mapping[str, Any],
    **kwargs: Any,
) -> list[dict[str, Any]]:
    return [
        assess_group(
            group,
            observations_by_group.get(str(group.get("id", group.get("group_id"))), []),
            **kwargs,
        )
        for group in groups
    ]


def validate_frozen_probability(
    item: Mapping[str, Any],
    *,
    case_id: str,
    subject_id: str,
    as_of: datetime | str | None = None,
) -> str | None:
    """QC guard for an internal frozen manifest, bound to actual source payloads.

    This complements artifact validation at serving time; marker booleans alone
    cannot authorize a changed hypothesis, a different subject or stale evidence.
    It deliberately does not make probability a prerequisite for operator QC.
    """
    if item.get("probability") is None:
        return None
    try:
        assessment = item.get("assessment") or {}
        probability = assessment.get("probability") or {}
        value = probability.get("value")
        if (
            isinstance(value, bool)
            or isinstance(item.get("probability"), bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0 <= value <= 1
            or value != item.get("probability")
            or probability.get("reason") is not None
            or probability.get("serving_gate_passed") is not True
        ):
            return "Numerical probability lacks a valid serving result"
        event = (
            "account_attribution"
            if item.get("kind") == "account"
            else "claim_correctness"
        )
        if (
            probability.get("event") != event
            or assessment.get("event") != event
            or probability.get("feature_schema") != FEATURE_SCHEMA
            or assessment.get("feature_schema") != FEATURE_SCHEMA
            or not probability.get("model_id")
            or not probability.get("review", {}).get("approval_reference")
        ):
            return "Numerical probability has missing or mismatched event/model/review metadata"
        artifact_hash = probability.get("artifact_sha256")
        if (
            not isinstance(artifact_hash, str)
            or len(artifact_hash) != 64
            or any(c not in "0123456789abcdef" for c in artifact_hash)
        ):
            return "Numerical probability has no reviewed artifact digest"
        if any(not probability.get("scope", {}).get(key) for key in SCOPE_FIELDS):
            return "Numerical probability has an unknown operating scope"
        assessed_at = timestamp(assessment.get("assessed_at"))
        validated_at = timestamp(probability.get("validated_at"))
        expires = timestamp(probability.get("expires_at"))
        now = timestamp(as_of) if as_of is not None else datetime.now(timezone.utc)
        if (
            not assessed_at
            or not validated_at
            or not expires
            or not now
            or not validated_at <= assessed_at < expires
            or expires <= now
            or (expires - validated_at).total_seconds() > 31 * 86400
        ):
            return "Numerical probability requires a current, dated artifact review"
        group = item.get("assessed_group")
        if not isinstance(group, Mapping):
            return "Numerical probability lacks its frozen assessed group"
        if group.get("kind") != item.get("kind"):
            return "Numerical probability event differs from its frozen hypothesis kind"
        if (
            str(group.get("case_id")) != str(case_id)
            or str(group.get("subject_id")) != str(subject_id)
            or str(assessment.get("case_id")) != str(case_id)
            or str(assessment.get("subject_id")) != str(subject_id)
            or group.get("id") != assessment.get("group_id")
        ):
            return (
                "Numerical probability belongs to another case, subject or hypothesis"
            )
        normalized = item.get("normalized") or {}
        core = (
            "id",
            "key",
            "canonical_key",
            "kind",
            "case_id",
            "subject_id",
            "platform",
            "stable_id",
            "canonical_url",
            "physical_account_key",
            "account_key",
            "predicate",
            "value",
            "qualifiers",
            "valid_from",
            "valid_to",
            "hypothesis_key",
        )
        if any(normalized.get(key) != group.get(key) for key in core):
            return "Curated hypothesis changed after its numerical assessment"
        payloads = [row["payload"] for row in item.get("evidence", [])]
        if set(group.get("observation_ids", [])) != {
            row["id"] for row in payloads
        } or len(payloads) != len({row["id"] for row in payloads}):
            return "Numerical probability evidence membership is stale or duplicated"
        current = assess_group(group, payloads, as_of=assessed_at)
        if (
            current["evidence_digest"] != assessment.get("evidence_digest")
            or current["evidence_digest"] != probability.get("evidence_digest")
            or current["features"] != assessment.get("features")
        ):
            return "Numerical probability is not bound to the frozen source evidence"
        if current["probability"]["reason"] not in {"not_calibrated"}:
            return (
                "Numerical probability evidence is stale or does not support its event"
            )
        return None
    except (TypeError, ValueError, KeyError, AttributeError):
        return "Numerical probability snapshot is malformed or incomplete"
