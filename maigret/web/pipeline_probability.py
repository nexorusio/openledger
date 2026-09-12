"""Safe, reviewed coefficient artifacts for P2 hypothesis probabilities.

No model is shipped enabled. A hash pinned by an operator/release reviewer is
required in addition to empirical validation metadata. JSON contains coefficients,
never executable pickle or dynamically imported classes.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

FEATURE_SCHEMA = "p2-evidence-v1"
FEATURE_NAMES = (
    "support_origin_families",
    "source_link_match",
    "stable_id_match",
    "public_identifier_match",
    "temporal_match",
    "qualified_claim_support",
    "contradiction_origin_families",
    "fresh_origin_families",
    "stale_origin_families",
    "unknown_dependence",
)
EVENTS = {
    "account_attribution": "Account belonged to or was operated by this subject at the stated time",
    "claim_correctness": "Qualified claim about this subject is correct at the stated time",
}
SCOPE_FIELDS = ("platform", "input_type", "language", "claim_family", "source_revision")
ARTIFACT_SCHEMA = "openledger-probability-artifact-v1"
MAX_ARTIFACT_BYTES = 2_000_000


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    # Naive source timestamps cannot be assumed to have the server's timezone.
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def sigmoid(value: float) -> float:
    if value >= 0:
        return 1.0 / (1.0 + math.exp(-min(value, 745)))
    exp = math.exp(max(value, -745))
    return exp / (1.0 + exp)


def _number(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def artifact_digest(artifact: Mapping[str, Any]) -> str:
    return digest(
        {key: value for key, value in artifact.items() if key != "artifact_sha256"}
    )


def seal_artifact(artifact: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(artifact)
    result["artifact_sha256"] = artifact_digest(result)
    return result


def load_artifact(path: str | Path) -> dict[str, Any]:
    """Read bounded strict JSON; approval and validation are checked on serving."""
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_ARTIFACT_BYTES + 1)
    if len(raw) > MAX_ARTIFACT_BYTES:
        raise ValueError("probability artifact exceeds size limit")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate probability artifact key")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError(f"nonfinite JSON constant {value}")

    result = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
    if not isinstance(result, dict):
        raise ValueError("probability artifact must be an object")
    return result


def abstention(reason: str, event: str, **details: Any) -> dict[str, Any]:
    return {
        "value": None,
        "reason": reason,
        "event": event,
        "event_definition": EVENTS.get(event),
        "model_id": None,
        **details,
    }


def validation_report_passes(validation: Mapping[str, Any]) -> bool:
    """Cross-check reported metrics; a lone `release_eligible` flag is not proof."""
    try:
        counts = validation["counts"]
        if (
            sum(
                counts[name]["adjudicated"] for name in ("train", "calibration", "test")
            )
            < 3000
            or sum(
                counts[name]["subjects"] for name in ("train", "calibration", "test")
            )
            < 500
            or counts["test"]["adjudicated"] < 500
        ):
            return False
        calibrated, raw, prior = (
            validation[name] for name in ("calibrated", "uncalibrated", "baseline")
        )
        for name in ("brier", "log_loss"):
            if not all(
                _number(report[name]) and report[name] >= 0
                for report in (calibrated, raw, prior)
            ):
                return False
            if (
                not calibrated[name] < prior[name]
                or calibrated[name] > raw[name] + 0.005
            ):
                return False
        if not _number(calibrated["ece"]) or not 0 <= calibrated["ece"] <= 0.05:
            return False
        for report in (calibrated, validation["calibration_partition"]):
            band = report["high_band"]
            interval = band["precision_95_ci"]
            if (
                band["count"] < 300
                or not _number(band["precision"])
                or not 0.98 <= band["precision"] <= 1
                or len(interval) != 2
                or not all(_number(value) for value in interval)
                or not 0.95 <= interval[0] <= interval[1] <= 1
            ):
                return False
        if validation.get("threshold_selection", {}).get("passed") is not True:
            return False
        return True
    except (TypeError, ValueError, KeyError, AttributeError):
        return False


def _validation_error(
    artifact: Mapping[str, Any],
    expected_sha256: str | None,
    event: str,
    scope: Mapping[str, Any],
    now: datetime,
) -> str | None:
    if not expected_sha256:
        return "artifact_not_reviewed_or_pinned"
    if (
        artifact.get("artifact_sha256") != expected_sha256
        or artifact_digest(artifact) != expected_sha256
    ):
        return "artifact_digest_mismatch"
    if (
        artifact.get("schema_version") != ARTIFACT_SCHEMA
        or artifact.get("feature_schema") != FEATURE_SCHEMA
    ):
        return "unsupported_artifact_schema"
    if artifact.get("event") != event or event not in EVENTS:
        return "event_mismatch"
    if not artifact.get("model_id") or not artifact.get("review", {}).get(
        "approval_reference"
    ):
        return "artifact_not_reviewed_or_pinned"
    generated = timestamp(artifact.get("generated_at"))
    expires = timestamp(artifact.get("expires_at"))
    if not generated or not expires or generated > now or expires <= now:
        return "artifact_expired_or_invalid_dates"
    # Monthly audit is part of the approved operating protocol, not optional.
    if (expires - generated).total_seconds() > 31 * 86400:
        return "artifact_audit_window_exceeded"
    validation = artifact.get("validation", {})
    if validation.get("reference_kind") != "independent_human_labels":
        return "not_empirically_validated"
    if not validation.get("locked_test_digest") or not validation.get(
        "split_manifest_digest"
    ):
        return "missing_validation_lineage"
    if validation.get("release_eligible") is not True:
        return "empirical_validation_gates_failed"
    gates = validation.get("gates", {})
    required = {
        "label_protocol",
        "sample_size",
        "split_isolation",
        "later_time_test",
        "proper_scores",
        "calibration",
        "supported_slices",
        "high_band",
    }
    if not required.issubset(gates) or any(gates[key] is not True for key in required):
        return "empirical_validation_gates_failed"
    if not validation_report_passes(validation):
        return "empirical_report_incomplete_or_inconsistent"
    if any(not scope.get(key) for key in SCOPE_FIELDS):
        return "unknown_validation_scope"
    scoped = [
        item
        for item in artifact.get("validated_scopes", [])
        if isinstance(item, dict)
        and all(item.get(key) == scope[key] for key in SCOPE_FIELDS)
    ]
    if not scoped:
        return "outside_validated_scope"
    selected = scoped[0]
    if (
        selected.get("test_count", 0) < 100
        or selected.get("positive_count", 0) < 1
        or selected.get("negative_count", 0) < 1
        or selected.get("ece", 1) > 0.1
    ):
        return "insufficiently_validated_scope"
    if selected.get("suspended") is True:
        return "scope_suspended"
    weights = artifact.get("weights", {})
    if set(weights) != set(FEATURE_NAMES) or not all(
        _number(value) for value in weights.values()
    ):
        return "invalid_model_coefficients"
    if not _number(artifact.get("intercept")):
        return "invalid_model_coefficients"
    calibrator = artifact.get("calibrator", {})
    if (
        calibrator.get("method") != "sigmoid"
        or not _number(calibrator.get("slope"))
        or not _number(calibrator.get("intercept"))
        or calibrator["slope"] <= 0
    ):
        return "invalid_calibrator"
    ranges = artifact.get("feature_ranges", {})
    if set(ranges) != set(FEATURE_NAMES):
        return "invalid_feature_ranges"
    for bounds in ranges.values():
        if (
            not isinstance(bounds, list)
            or len(bounds) != 2
            or not all(_number(value) for value in bounds)
            or bounds[0] > bounds[1]
        ):
            return "invalid_feature_ranges"
    return None


def predict_probability(
    features: Mapping[str, Any],
    *,
    event: str,
    scope: Mapping[str, Any] | None = None,
    artifact: Mapping[str, Any] | None = None,
    expected_sha256: str | None = None,
    as_of: datetime | str | None = None,
    evidence_digest: str | None = None,
) -> dict[str, Any]:
    """Fail closed for scope, audit, schema, coefficient or approval failures."""
    if not artifact:
        return abstention("not_calibrated", event)
    now = timestamp(as_of) if as_of is not None else datetime.now(timezone.utc)
    if now is None:
        return abstention("invalid_assessment_time", event)
    try:
        error = _validation_error(artifact, expected_sha256, event, scope or {}, now)
        if error:
            return abstention(error, event)
        if set(features) != set(FEATURE_NAMES) or not all(
            _number(value) for value in features.values()
        ):
            return abstention("invalid_feature_vector", event)
        for name, value in features.items():
            low, high = artifact["feature_ranges"][name]
            if not low <= value <= high:
                return abstention(
                    "outside_validated_feature_range", event, feature=name
                )
        contributions = {
            name: float(features[name]) * artifact["weights"][name]
            for name in FEATURE_NAMES
        }
        logit = artifact["intercept"] + sum(contributions.values())
        calibrated_logit = (
            artifact["calibrator"]["slope"] * logit
            + artifact["calibrator"]["intercept"]
        )
        if not math.isfinite(logit) or not math.isfinite(calibrated_logit):
            return abstention("nonfinite_model_result", event)
        value = sigmoid(calibrated_logit)
        reliability_bin = next(
            (
                item
                for item in artifact.get("validation", {}).get("reliability", [])
                if item["lower"] <= value < item["upper"]
                or value == item["upper"] == 1.0
            ),
            None,
        )
        return {
            "value": value,
            "reason": None,
            "event": event,
            "event_definition": EVENTS[event],
            "serving_gate_passed": True,
            "model_id": artifact["model_id"],
            "review": dict(artifact["review"]),
            "validated_at": artifact["generated_at"],
            "expires_at": artifact["expires_at"],
            "artifact_sha256": expected_sha256,
            "feature_schema": FEATURE_SCHEMA,
            "evidence_digest": evidence_digest,
            "scope": dict(scope or {}),
            "population": artifact.get("validation", {}).get("population"),
            "contributions": contributions,
            "intercept": artifact["intercept"],
            "uncalibrated_probability": sigmoid(logit),
            "calibrator": dict(artifact["calibrator"]),
            "calibration_bin": reliability_bin,
            "uncertainty_note": "Calibration-bin interval describes held-out reference frequency, not certainty about this individual.",
            "high_band": value >= artifact.get("high_band_threshold", 0.95),
        }
    except (TypeError, ValueError, KeyError, OverflowError, AttributeError):
        return abstention("invalid_probability_artifact", event)
