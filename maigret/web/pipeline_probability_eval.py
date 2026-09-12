"""Offline, deterministic logistic fitting and locked probability evaluation.

The runtime needs only reviewed JSON coefficients. This reference implementation
uses the Python standard library, avoiding an unpinned training/runtime dependency
or pickle deserialization. Synthetic references always fail the empirical gate.
"""

from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from maigret.web.pipeline_probability import (
    ARTIFACT_SCHEMA,
    EVENTS,
    FEATURE_NAMES,
    FEATURE_SCHEMA,
    SCOPE_FIELDS,
    artifact_digest,
    digest,
    seal_artifact,
    sigmoid,
    timestamp,
    validation_report_passes,
)

DATASET_SCHEMA = "openledger-probability-reference-v1"
SPLIT_SCHEMA = "openledger-probability-split-v1"


def _finite_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _features(row: Mapping[str, Any]) -> list[float]:
    values = row.get("features", {})
    if set(values) != set(FEATURE_NAMES) or any(
        not _finite_number(value) or not 0 <= value <= 10 for value in values.values()
    ):
        raise ValueError(
            "reference feature vector must match schema with finite values from 0 to 10"
        )
    return [float(values[name]) for name in FEATURE_NAMES]


def validate_dataset(dataset: Mapping[str, Any], event: str) -> list[dict[str, Any]]:
    if (
        dataset.get("schema_version") != DATASET_SCHEMA
        or dataset.get("feature_schema") != FEATURE_SCHEMA
    ):
        raise ValueError("unsupported reference dataset or feature schema")
    if event not in EVENTS or not dataset.get("dataset_id"):
        raise ValueError("dataset ID and a supported event are required")
    examples = [
        dict(row) for row in dataset.get("examples", []) if row.get("event") == event
    ]
    ids = set()
    for row in examples:
        identifier = row.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in ids:
            raise ValueError("example IDs must be nonempty and unique")
        ids.add(identifier)
        if not row.get("subject_id") or not row.get("source_origin_ids"):
            raise ValueError(
                "each example requires subject and original-source identities for leakage checks"
            )
        for field in ("account_ids", "source_origin_ids"):
            identifiers = row.get(field, [])
            if not isinstance(identifiers, list) or any(
                not isinstance(value, str) or not value for value in identifiers
            ):
                raise ValueError(
                    "account/origin identities must be lists of nonempty strings"
                )
        if event == "account_attribution" and not row.get("account_ids"):
            raise ValueError("account-attribution examples require account identities")
        if row.get("label") not in (0, 1, None) or isinstance(row.get("label"), bool):
            raise ValueError("labels must be 0, 1 or null (unresolved)")
        if not timestamp(row.get("reference_time")):
            raise ValueError("each example requires a timezone-aware reference time")
        if any(
            not isinstance(row.get("scope", {}).get(key), str) or not row["scope"][key]
            for key in SCOPE_FIELDS
        ):
            raise ValueError("each example requires a complete scope")
        _features(row)
    if len(examples) < 15:
        raise ValueError("too few examples to fit three separate partitions")
    return examples


def _identity_keys(row):
    return (
        ["subject:" + str(row["subject_id"])]
        + ["account:" + str(value) for value in row.get("account_ids", [])]
        + ["origin:" + str(value) for value in row.get("source_origin_ids", [])]
    )


def build_split_manifest(dataset: Mapping[str, Any], event: str) -> dict[str, Any]:
    """Allocate connected subject/account/origin groups in temporal order.

    A component cannot straddle partitions. Long-lived components may make a
    strict later-time test impossible; validation then fails visibly.
    """
    rows = validate_dataset(dataset, event)
    parents = {row["id"]: row["id"] for row in rows}

    def root(identifier):
        while parents[identifier] != identifier:
            parents[identifier] = parents[parents[identifier]]
            identifier = parents[identifier]
        return identifier

    seen = {}
    for row in rows:
        for key in _identity_keys(row):
            if key in seen:
                parents[root(row["id"])] = root(seen[key])
            else:
                seen[key] = row["id"]
    components = defaultdict(list)
    for row in rows:
        components[root(row["id"])].append(row)
    groups = sorted(
        components.values(),
        key=lambda group: (
            max(timestamp(row["reference_time"]) for row in group),
            min(row["id"] for row in group),
        ),
    )
    if len(groups) < 3:
        raise ValueError(
            "subject/account/origin connected components cannot support three isolated splits"
        )
    splits = {"train": [], "calibration": [], "test": []}
    for index, group in enumerate(groups):
        # Leave at least one whole component for each remaining partition.
        if len(splits["train"]) < len(rows) * 0.6 and index < len(groups) - 2:
            split = "train"
        elif (
            len(splits["train"]) + len(splits["calibration"]) < len(rows) * 0.8
            and index < len(groups) - 1
        ):
            split = "calibration"
        else:
            split = "test"
        splits[split].extend(row["id"] for row in group)
    manifest = {
        "schema_version": SPLIT_SCHEMA,
        "dataset_id": dataset["dataset_id"],
        "dataset_digest": digest(dataset),
        "event": event,
        "algorithm": "connected-subject-account-origin-temporal-v1",
        "splits": {name: sorted(values) for name, values in splits.items()},
    }
    manifest["manifest_digest"] = digest(manifest)
    return manifest


def validate_split(dataset, manifest, event):
    rows = validate_dataset(dataset, event)
    if (
        manifest.get("schema_version") != SPLIT_SCHEMA
        or manifest.get("event") != event
        or manifest.get("dataset_digest") != digest(dataset)
        or manifest.get("manifest_digest")
        != digest(
            {key: value for key, value in manifest.items() if key != "manifest_digest"}
        )
    ):
        raise ValueError(
            "locked split does not match the frozen dataset or manifest digest"
        )
    splits = manifest.get("splits", {})
    if set(splits) != {"train", "calibration", "test"} or not all(splits.values()):
        raise ValueError("all three nonempty partitions are required")
    by_id = {row["id"]: row for row in rows}
    ordered_ids = [identifier for split in splits.values() for identifier in split]
    if len(ordered_ids) != len(set(ordered_ids)) or set(ordered_ids) != set(by_id):
        raise ValueError("every hypothesis must occur in exactly one partition")
    keys_seen = {}
    for name, identifiers in splits.items():
        for identifier in identifiers:
            for key in _identity_keys(by_id[identifier]):
                if key in keys_seen and keys_seen[key] != name:
                    raise ValueError(
                        "subject/account/original-source leakage across partitions"
                    )
                keys_seen[key] = name
    partitions = {
        name: [by_id[identifier] for identifier in identifiers]
        for name, identifiers in splits.items()
    }
    for name, partition in partitions.items():
        if {row["label"] for row in partition if row["label"] is not None} != {0, 1}:
            raise ValueError(
                f"{name} partition needs both independently adjudicated classes"
            )
    return partitions


def fit_logistic(
    x: Sequence[Sequence[float]],
    y: Sequence[int],
    *,
    regularization: float = 0.01,
    iterations: int = 1400,
    learning_rate: float = 0.15,
) -> tuple[list[float], float]:
    """L2-regularized binary logistic regression by deterministic batch gradient.

    Features are schema-bounded; no learned preprocessing touches calibration or
    test records. Coefficients returned are in the original feature units.
    """
    if not x or len(x) != len(y) or len(set(y)) < 2:
        raise ValueError("logistic fitting requires both classes and aligned records")
    width = len(x[0])
    scales = [max(1.0, max(abs(row[index]) for row in x)) for index in range(width)]
    scaled = [[value / scales[index] for index, value in enumerate(row)] for row in x]
    prior = sum(y) / len(y)
    intercept = math.log(prior / (1 - prior))
    weights = [0.0] * width
    for _ in range(iterations):
        grad = [0.0] * width
        grad_intercept = 0.0
        for row, label in zip(scaled, y):
            residual = (
                sigmoid(intercept + sum(w * value for w, value in zip(weights, row)))
                - label
            )
            grad_intercept += residual
            for index, value in enumerate(row):
                grad[index] += residual * value
        changes = [
            learning_rate * (value / len(y) + regularization * weights[index])
            for index, value in enumerate(grad)
        ]
        bias_change = learning_rate * grad_intercept / len(y)
        weights = [weight - change for weight, change in zip(weights, changes)]
        intercept -= bias_change
        if max([abs(bias_change)] + [abs(value) for value in changes]) < 1e-9:
            break
    return [weight / scales[index] for index, weight in enumerate(weights)], intercept


def wilson_interval(
    positive: int, count: int, z: float = 1.959963984540054
) -> list[float] | None:
    if count <= 0:
        return None
    p = positive / count
    denominator = 1 + z * z / count
    centre = (p + z * z / (2 * count)) / denominator
    radius = (
        z * math.sqrt(p * (1 - p) / count + z * z / (4 * count * count)) / denominator
    )
    return [max(0.0, centre - radius), min(1.0, centre + radius)]


def _cluster_values(rows, values):
    """Subject clusters expanded when account/origin lineage reveals dependence."""
    parents = list(range(len(rows)))

    def root(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    seen = {}
    for index, row in enumerate(rows):
        for key in _identity_keys(row):
            if key in seen:
                parents[root(index)] = root(seen[key])
            else:
                seen[key] = index
    clusters = defaultdict(list)
    for index, value in enumerate(values):
        clusters[root(index)].append(value)
    return clusters


def cluster_interval(rows, values, *, seed=7321, iterations=400):
    """95% subject/dependence-cluster bootstrap with a deterministic seed."""
    clusters = _cluster_values(rows, values)
    ids = sorted(clusters)
    if not ids:
        return None
    rng = random.Random(seed)
    estimates = []
    for _ in range(iterations):
        sample = [value for _ in ids for value in clusters[rng.choice(ids)]]
        estimates.append(sum(sample) / len(sample))
    estimates.sort()
    return [
        estimates[int(0.025 * iterations)],
        estimates[min(iterations - 1, int(0.975 * iterations))],
    ]


def _independent(rows):
    # Wilson is used only if subjects, accounts and origins are all disjoint.
    keys = set()
    for row in rows:
        identities = set(_identity_keys(row))
        if identities & keys:
            return False
        keys.update(identities)
    return True


def _rate_interval(rows, labels):
    if _independent(rows):
        return wilson_interval(sum(labels), len(labels))
    result = cluster_interval(rows, labels)
    # An all-positive clustered bootstrap is otherwise degenerate [1, 1], even
    # with a single subject. At that boundary use independent subject count for
    # the binomial bound, never the inflated observation count.
    if labels and len(set(labels)) == 1:
        clusters = len(_cluster_values(rows, labels))
        return wilson_interval(clusters * labels[0], clusters)
    return result


def score_predictions(rows, predictions, *, threshold=0.95, with_intervals=True):
    if len(rows) != len(predictions) or not rows:
        raise ValueError("evaluation requires aligned nonempty rows")
    labels = [row["label"] for row in rows]
    if any(label not in (0, 1) for label in labels):
        raise ValueError(
            "unresolved labels must be reported separately from proper scores"
        )
    if any(not _finite_number(p) or not 0 <= p <= 1 for p in predictions):
        raise ValueError("predictions must be finite probabilities")
    brier = [
        (prediction - label) ** 2 for prediction, label in zip(predictions, labels)
    ]
    losses = [
        -(
            label * math.log(max(1e-15, prediction))
            + (1 - label) * math.log(max(1e-15, 1 - prediction))
        )
        for prediction, label in zip(predictions, labels)
    ]
    reliability = []
    ece = 0.0
    for index in range(10):
        selected = [
            position
            for position, p in enumerate(predictions)
            if min(int(p * 10), 9) == index
        ]
        selected_labels = [labels[position] for position in selected]
        mean = (
            sum(predictions[position] for position in selected) / len(selected)
            if selected
            else None
        )
        rate = sum(selected_labels) / len(selected) if selected else None
        if selected:
            ece += len(selected) / len(rows) * abs(mean - rate)
        reliability.append(
            {
                "lower": index / 10,
                "upper": (index + 1) / 10,
                "count": len(selected),
                "mean_prediction": mean,
                "positive_fraction": rate,
                "positive_fraction_95_ci": (
                    _rate_interval(
                        [rows[position] for position in selected], selected_labels
                    )
                    if selected and with_intervals
                    else None
                ),
            }
        )
    band = [index for index, p in enumerate(predictions) if p >= threshold]
    true_positive = sum(labels[index] for index in band)
    band_rows = [rows[index] for index in band]
    band_labels = [labels[index] for index in band]
    band_ci = (
        _rate_interval(band_rows, band_labels) if band and with_intervals else None
    )
    return {
        "count": len(rows),
        "positive_count": sum(labels),
        "negative_count": len(rows) - sum(labels),
        "brier": sum(brier) / len(rows),
        "log_loss": sum(losses) / len(rows),
        "ece": ece,
        "brier_95_ci": cluster_interval(rows, brier) if with_intervals else None,
        "log_loss_95_ci": cluster_interval(rows, losses) if with_intervals else None,
        "reliability": reliability,
        "threshold": threshold,
        "high_band": {
            "count": len(band),
            "precision": true_positive / len(band) if band else None,
            "precision_95_ci": band_ci,
            "independent_examples": _independent(band_rows),
            "recall": true_positive / sum(labels) if sum(labels) else None,
            "false_association_rate": (
                (len(band) - true_positive) / len(band) if band else None
            ),
            "coverage": len(band) / len(rows),
        },
    }


def _labels_meet_protocol(rows):
    for row in rows:
        reviews = row.get("reviews", [])
        if (
            len(reviews) != 2
            or len({review.get("reviewer_id") for review in reviews}) != 2
            or any(
                not review.get("reviewer_id")
                or review.get("blinded_to_model") is not True
                or not review.get("evidence_refs")
                or review.get("label") not in (0, 1, None)
                or isinstance(review.get("label"), bool)
                for review in reviews
            )
            or not row.get("reference_evidence_refs")
        ):
            return False
        if reviews[0]["label"] == reviews[1]["label"]:
            if row["label"] != reviews[0]["label"]:
                return False
        else:
            adjudication = row.get("adjudication", {})
            if (
                not adjudication.get("reviewer_id")
                or not adjudication.get("reason")
                or not adjudication.get("evidence_refs")
                or adjudication.get("label") != row["label"]
            ):
                return False
    return True


def train_and_evaluate(
    dataset: Mapping[str, Any],
    split_manifest: Mapping[str, Any],
    *,
    event: str,
    model_id: str,
    approval_reference: str | None = None,
    as_of: datetime | str | None = None,
    iterations: int = 1400,
) -> dict[str, Any]:
    """Evaluate frozen hyperparameters once; no test-set threshold tuning.

    CLI enforces a consumed-lock marker. This pure function is available for
    deterministic regression tests, which cannot satisfy the human-label gate.
    """
    all_partitions = validate_split(dataset, split_manifest, event)
    partitions = {
        name: [row for row in rows if row["label"] is not None]
        for name, rows in all_partitions.items()
    }
    train, calibration, test = (
        partitions[name] for name in ("train", "calibration", "test")
    )
    weights, intercept = fit_logistic(
        [_features(row) for row in train],
        [row["label"] for row in train],
        iterations=iterations,
    )

    def logit(row):
        return intercept + sum(w * value for w, value in zip(weights, _features(row)))

    slopes, calibration_intercept = fit_logistic(
        [[logit(row)] for row in calibration],
        [row["label"] for row in calibration],
        regularization=0.0001,
        iterations=iterations,
    )
    slope = slopes[0]
    prior = sum(row["label"] for row in train) / len(train)
    uncalibrated = [sigmoid(logit(row)) for row in test]
    calibrated = [sigmoid(slope * logit(row) + calibration_intercept) for row in test]
    # Choose the operating threshold using calibration labels only, then freeze
    # it before accessing test labels. No candidate below 0.95 is permitted.
    calibration_predictions = [
        sigmoid(slope * logit(row) + calibration_intercept) for row in calibration
    ]
    threshold = 0.95
    threshold_selected = False
    threshold_candidates = []
    for candidate in (0.95, 0.96, 0.97, 0.98, 0.99):
        band_report = score_predictions(
            calibration, calibration_predictions, threshold=candidate
        )["high_band"]
        passed = (
            band_report["count"] >= 300
            and band_report["precision"] is not None
            and band_report["precision"] >= 0.98
            and band_report["precision_95_ci"] is not None
            and band_report["precision_95_ci"][0] >= 0.95
        )
        threshold_candidates.append(
            {"threshold": candidate, "passed": passed, **band_report}
        )
        if passed and not threshold_selected:
            threshold = candidate
            threshold_selected = True
    baseline_metrics = score_predictions(test, [prior] * len(test))
    raw_metrics = score_predictions(test, uncalibrated)
    metrics = score_predictions(test, calibrated, threshold=threshold)
    calibration_metrics = score_predictions(
        calibration, calibration_predictions, threshold=threshold
    )
    slices = {}
    for field in SCOPE_FIELDS:
        slices[field] = {}
        for value in sorted({row["scope"][field] for row in test}):
            indices = [
                index for index, row in enumerate(test) if row["scope"][field] == value
            ]
            report = score_predictions(
                [test[index] for index in indices],
                [calibrated[index] for index in indices],
                threshold=threshold,
            )
            report["supported"] = (
                report["count"] >= 100
                and report["positive_count"] > 0
                and report["negative_count"] > 0
                and report["ece"] <= 0.1
            )
            slices[field][value] = report
    exact_scopes = {}
    for index, row in enumerate(test):
        key = tuple(row["scope"][field] for field in SCOPE_FIELDS)
        exact_scopes.setdefault(key, []).append(index)
    validated_scopes = []
    for key, indices in exact_scopes.items():
        report = score_predictions(
            [test[index] for index in indices],
            [calibrated[index] for index in indices],
            threshold=threshold,
        )
        if (
            report["count"] >= 100
            and report["positive_count"] > 0
            and report["negative_count"] > 0
            and report["ece"] <= 0.1
        ):
            validated_scopes.append(
                {
                    **dict(zip(SCOPE_FIELDS, key)),
                    "test_count": report["count"],
                    "positive_count": report["positive_count"],
                    "negative_count": report["negative_count"],
                    "ece": report["ece"],
                    "suspended": False,
                }
            )
    later_time = max(
        timestamp(row["reference_time"]) for row in train + calibration
    ) < min(timestamp(row["reference_time"]) for row in test)
    all_rows = [row for rows in all_partitions.values() for row in rows]
    band = metrics["high_band"]
    gates = {
        "label_protocol": dataset.get("reference_kind") == "independent_human_labels"
        and dataset.get("population", {}).get("role")
        == "representative_candidate_stream"
        and bool(dataset.get("population", {}).get("definition"))
        and bool(dataset.get("population", {}).get("sampling_method"))
        and _labels_meet_protocol(all_rows),
        "sample_size": sum(len(rows) for rows in partitions.values()) >= 3000
        and len({row["subject_id"] for row in all_rows if row["label"] is not None})
        >= 500
        and len(test) >= 500,
        "split_isolation": True,
        "later_time_test": later_time,
        "proper_scores": metrics["brier"] < baseline_metrics["brier"]
        and metrics["log_loss"] < baseline_metrics["log_loss"]
        and metrics["brier"] <= raw_metrics["brier"] + 0.005
        and metrics["log_loss"] <= raw_metrics["log_loss"] + 0.005,
        "calibration": metrics["ece"] <= 0.05 and slope > 0,
        "supported_slices": bool(validated_scopes)
        and all(
            report["ece"] <= 0.1
            for field in slices.values()
            for report in field.values()
            if report["count"] >= 100
        ),
        "high_band": threshold_selected
        and band["count"] >= 300
        and band["precision"] is not None
        and band["precision"] >= 0.98
        and band["precision_95_ci"] is not None
        and band["precision_95_ci"][0] >= 0.95,
    }
    eligible_scopes = {
        tuple(scope[field] for field in SCOPE_FIELDS) for scope in validated_scopes
    }
    # Feature ranges come from training, never widened to include held-out rows.
    ranges = {
        name: [
            min(row["features"][name] for row in train),
            max(row["features"][name] for row in train),
        ]
        for name in FEATURE_NAMES
    }
    eligible_count = sum(
        tuple(row["scope"][field] for field in SCOPE_FIELDS) in eligible_scopes
        and row.get("eligible", True) is True
        and all(
            ranges[name][0] <= row["features"][name] <= ranges[name][1]
            for name in FEATURE_NAMES
        )
        for row in all_partitions["test"]
    )
    reference_count = len(all_partitions["test"])
    generated = timestamp(as_of) if as_of is not None else datetime.now(timezone.utc)
    if generated is None:
        raise ValueError("evaluation time must include timezone")
    report = {
        "reference_kind": dataset.get("reference_kind"),
        "dataset_id": dataset["dataset_id"],
        "dataset_digest": digest(dataset),
        "population": dataset.get("population"),
        "split_manifest_digest": split_manifest["manifest_digest"],
        "locked_test_digest": digest(all_partitions["test"]),
        "counts": {
            name: {
                "all": len(rows),
                "adjudicated": len(partitions[name]),
                "unresolved": len(rows) - len(partitions[name]),
                "subjects": len({row["subject_id"] for row in rows}),
            }
            for name, rows in all_partitions.items()
        },
        "baseline": baseline_metrics,
        "uncalibrated": raw_metrics,
        "calibrated": metrics,
        "calibration_partition": calibration_metrics,
        "slices": slices,
        "threshold_selection": {
            "partition": "calibration",
            "selected": threshold,
            "passed": threshold_selected,
            "candidates": threshold_candidates,
        },
        "gates": gates,
        "release_eligible": all(gates.values()),
        "reliability": metrics["reliability"],
        "numeric_scope_coverage": eligible_count / reference_count,
        "abstention_rate": 1 - eligible_count / reference_count,
        "effective_numeric_coverage": (
            eligible_count / reference_count if all(gates.values()) else 0.0
        ),
        "coverage_note": "Scope coverage is prospective; failing empirical gates means actual numeric coverage is zero. Unresolved references remain in the denominator.",
        "proper_score_noninferiority_tolerance": 0.005,
        "confidence_interval_method": "Wilson only for disjoint identities; otherwise subject-cluster bootstrap expanded for shared account/origin dependence (400 resamples). All-equal label boundary uses independent-cluster Wilson bound.",
        "limitations": (
            ["Synthetic examples cannot establish empirical calibration"]
            if dataset.get("reference_kind") != "independent_human_labels"
            else []
        ),
    }
    artifact = seal_artifact(
        {
            "schema_version": ARTIFACT_SCHEMA,
            "feature_schema": FEATURE_SCHEMA,
            "model_id": model_id,
            "event": event,
            "generated_at": generated.isoformat(),
            "expires_at": (generated + timedelta(days=30)).isoformat(),
            "weights": dict(zip(FEATURE_NAMES, weights)),
            "intercept": intercept,
            "calibrator": {
                "method": "sigmoid",
                "slope": slope,
                "intercept": calibration_intercept,
            },
            "feature_ranges": ranges,
            "high_band_threshold": threshold,
            "training": {
                "algorithm": "regularized-logistic-batch-gradient-v1",
                "l2": 0.01,
                "calibrator_l2": 0.0001,
                "iterations": iterations,
                "learning_rate": 0.15,
                "feature_scaling": "training-only max absolute scale; exported in original units",
            },
            "review": {"approval_reference": approval_reference},
            "validated_scopes": validated_scopes,
            "validation": report,
        }
    )
    return {
        "artifact": artifact,
        "report": report,
        "artifact_sha256": artifact["artifact_sha256"],
    }


def export_reviewed_artifact(
    result: Mapping[str, Any], approval_reference: str
) -> dict[str, Any]:
    """Export a passed evaluation only after a separate, explicit review action."""
    if not isinstance(approval_reference, str) or not approval_reference.strip():
        raise ValueError("an explicit review approval reference is required")
    artifact = dict(result.get("artifact", {}))
    if (
        artifact.get("artifact_sha256") != result.get("artifact_sha256")
        or artifact_digest(artifact) != result.get("artifact_sha256")
        or artifact.get("validation") != result.get("report")
    ):
        raise ValueError("evaluation output digest or embedded report mismatch")
    validation = artifact.get("validation", {})
    if (
        validation.get("reference_kind") != "independent_human_labels"
        or validation.get("release_eligible") is not True
        or not validation.get("gates")
        or not all(value is True for value in validation["gates"].values())
        or not validation_report_passes(validation)
    ):
        raise ValueError(
            "empirical gates have not passed; this artifact cannot be approved for numbers"
        )
    artifact["review"] = {"approval_reference": approval_reference.strip()}
    return seal_artifact(artifact)
