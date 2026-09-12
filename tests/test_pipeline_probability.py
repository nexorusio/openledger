from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import subprocess
import sys

import pytest

from maigret.web.pipeline_probability import (
    ARTIFACT_SCHEMA,
    FEATURE_NAMES,
    FEATURE_SCHEMA,
    load_artifact,
    predict_probability,
    seal_artifact,
)
from maigret.web.pipeline_probability_eval import (
    DATASET_SCHEMA,
    build_split_manifest,
    export_reviewed_artifact,
    fit_logistic,
    score_predictions,
    train_and_evaluate,
    validate_split,
    wilson_interval,
)

NOW = "2026-09-11T12:00:00Z"
SCOPE = {
    "platform": "fixture",
    "input_type": "username",
    "language": "id",
    "claim_family": "account",
    "source_revision": "fixture-parser-v1",
}


def serving_fixture():
    """Constructed parser fixture, NOT a validated production model or report."""
    return seal_artifact(
        {
            "schema_version": ARTIFACT_SCHEMA,
            "feature_schema": FEATURE_SCHEMA,
            "model_id": "TEST-ONLY-NOT-EMPIRICAL",
            "event": "account_attribution",
            "generated_at": "2026-09-10T00:00:00Z",
            "expires_at": "2026-10-10T00:00:00Z",
            "review": {"approval_reference": "TEST-ONLY"},
            "weights": {name: 0.1 for name in FEATURE_NAMES},
            "intercept": -1.0,
            "calibrator": {"method": "sigmoid", "slope": 1.2, "intercept": -0.2},
            "feature_ranges": {name: [0, 10] for name in FEATURE_NAMES},
            "validated_scopes": [
                {
                    **SCOPE,
                    "test_count": 100,
                    "positive_count": 50,
                    "negative_count": 50,
                    "ece": 0.01,
                }
            ],
            "validation": {
                "reference_kind": "independent_human_labels",
                "locked_test_digest": "test-only",
                "split_manifest_digest": "test-only",
                "release_eligible": True,
                "counts": {
                    "train": {"adjudicated": 1800, "subjects": 300},
                    "calibration": {"adjudicated": 600, "subjects": 100},
                    "test": {"adjudicated": 600, "subjects": 100},
                },
                "calibrated": {
                    "brier": 0.10,
                    "log_loss": 0.30,
                    "ece": 0.02,
                    "high_band": {
                        "count": 300,
                        "precision": 1.0,
                        "precision_95_ci": [0.98, 1.0],
                    },
                },
                "uncalibrated": {"brier": 0.11, "log_loss": 0.32},
                "baseline": {"brier": 0.25, "log_loss": 0.69},
                "calibration_partition": {
                    "high_band": {
                        "count": 300,
                        "precision": 1.0,
                        "precision_95_ci": [0.98, 1.0],
                    }
                },
                "threshold_selection": {"passed": True},
                "gates": {
                    key: True
                    for key in (
                        "label_protocol",
                        "sample_size",
                        "split_isolation",
                        "later_time_test",
                        "proper_scores",
                        "calibration",
                        "supported_slices",
                        "high_band",
                    )
                },
            },
        }
    )


def predict(artifact=None, **kwargs):
    artifact = artifact or serving_fixture()
    params = {
        "event": "account_attribution",
        "scope": SCOPE,
        "artifact": artifact,
        "expected_sha256": artifact["artifact_sha256"],
        "as_of": NOW,
    }
    params.update(kwargs)
    return predict_probability({name: 1.0 for name in FEATURE_NAMES}, **params)


def test_safe_coefficient_inference_exposes_contributions_and_event():
    result = predict()
    assert 0 < result["value"] < 1
    assert result["reason"] is None
    assert set(result["contributions"]) == set(FEATURE_NAMES)
    assert result["event"] == "account_attribution"


@pytest.mark.parametrize(
    "modification,reason",
    [
        (
            lambda a: a["validation"].update(reference_kind="synthetic"),
            "not_empirically_validated",
        ),
        (
            lambda a: a["validation"].update(release_eligible=False),
            "empirical_validation_gates_failed",
        ),
        (
            lambda a: a["validation"]["gates"].update(high_band=False),
            "empirical_validation_gates_failed",
        ),
        (
            lambda a: a["review"].update(approval_reference=None),
            "artifact_not_reviewed_or_pinned",
        ),
        (
            lambda a: a.update(expires_at="2026-09-11T00:00:00Z"),
            "artifact_expired_or_invalid_dates",
        ),
        (
            lambda a: a.update(expires_at="2027-09-11T00:00:00Z"),
            "artifact_audit_window_exceeded",
        ),
        (lambda a: a.update(event="claim_correctness"), "event_mismatch"),
        (lambda a: a["validated_scopes"][0].update(suspended=True), "scope_suspended"),
        (
            lambda a: a["validated_scopes"][0].update(test_count=99),
            "insufficiently_validated_scope",
        ),
        (lambda a: a["calibrator"].update(slope=-1), "invalid_calibrator"),
        (
            lambda a: a["weights"].update(unknown_feature=1),
            "invalid_model_coefficients",
        ),
    ],
)
def test_unvalidated_or_unsafe_artifacts_abstain(modification, reason):
    artifact = serving_fixture()
    modification(artifact)
    artifact = seal_artifact(artifact)
    result = predict(artifact)
    assert result["value"] is None
    assert result["reason"] == reason


def test_artifact_cannot_self_authorize_with_its_own_embedded_digest():
    assert predict(expected_sha256=None)["reason"] == "artifact_not_reviewed_or_pinned"
    artifact = serving_fixture()
    artifact["weights"]["source_link_match"] = 9
    assert predict(artifact)["reason"] == "artifact_digest_mismatch"


def test_gate_boolean_cannot_replace_missing_empirical_report():
    artifact = serving_fixture()
    del artifact["validation"]["calibrated"]
    assert (
        predict(seal_artifact(artifact))["reason"]
        == "empirical_report_incomplete_or_inconsistent"
    )


@pytest.mark.parametrize(
    "scope,reason",
    [
        ({}, "unknown_validation_scope"),
        ({**SCOPE, "language": "en"}, "outside_validated_scope"),
        ({**SCOPE, "source_revision": "changed-parser"}, "outside_validated_scope"),
    ],
)
def test_unknown_scope_or_changed_source_requires_revalidation(scope, reason):
    assert predict(scope=scope)["reason"] == reason


def test_feature_extrapolation_is_withheld():
    artifact = serving_fixture()
    artifact["feature_ranges"]["source_link_match"] = [0, 0]
    assert (
        predict(seal_artifact(artifact))["reason"] == "outside_validated_feature_range"
    )


@pytest.mark.parametrize("text", ['{"x": 1, "x": 2}', '{"x": NaN}', "[1, 2]"])
def test_strict_json_loader_rejects_ambiguous_or_nonfinite_artifacts(tmp_path, text):
    path = tmp_path / "artifact.json"
    path.write_text(text)
    with pytest.raises(ValueError):
        load_artifact(path)


def reference_fixture(count=150):
    """Deterministic engineering fixture, explicitly synthetic."""
    examples = []
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index in range(count):
        label = index % 2
        examples.append(
            {
                "id": f"example:{index:04}",
                "event": "account_attribution",
                "subject_id": f"subject:{index}",
                "account_ids": [f"account:{index}"],
                "source_origin_ids": [f"origin:{index}"],
                "reference_time": (start + timedelta(hours=index)).isoformat(),
                "scope": dict(SCOPE),
                "features": {
                    name: float(label) if name == "source_link_match" else 0.0
                    for name in FEATURE_NAMES
                },
                "label": label,
                "eligible": True,
                "reviews": [
                    {
                        "reviewer_id": "fixture-a",
                        "blinded_to_model": True,
                        "label": label,
                        "evidence_refs": [f"fixture:{index}"],
                    },
                    {
                        "reviewer_id": "fixture-b",
                        "blinded_to_model": True,
                        "label": label,
                        "evidence_refs": [f"fixture:{index}"],
                    },
                ],
                "reference_evidence_refs": [f"fixture:{index}"],
            }
        )
    return {
        "schema_version": DATASET_SCHEMA,
        "feature_schema": FEATURE_SCHEMA,
        "reference_kind": "synthetic",
        "dataset_id": "TEST-ONLY",
        "examples": examples,
    }


def test_grouped_split_keeps_subject_account_and_copy_origin_together():
    dataset = reference_fixture()
    dataset["examples"][1]["subject_id"] = dataset["examples"][0]["subject_id"]
    dataset["examples"][2]["account_ids"] = dataset["examples"][1]["account_ids"]
    dataset["examples"][3]["source_origin_ids"] = dataset["examples"][2][
        "source_origin_ids"
    ]
    split = build_split_manifest(dataset, "account_attribution")
    partitions = validate_split(dataset, split, "account_attribution")
    memberships = {row["id"]: name for name, rows in partitions.items() for row in rows}
    assert len({memberships[row["id"]] for row in dataset["examples"][:4]}) == 1


def test_changing_frozen_dataset_fails_before_fitting():
    dataset = reference_fixture()
    split = build_split_manifest(dataset, "account_attribution")
    dataset["examples"][0]["label"] = 1
    with pytest.raises(ValueError, match="frozen dataset"):
        validate_split(dataset, split, "account_attribution")


def test_unresolved_labels_remain_visible_but_are_not_fit_as_negatives():
    dataset = reference_fixture()
    dataset["examples"][-1]["label"] = None
    for review in dataset["examples"][-1]["reviews"]:
        review["label"] = None
    split = build_split_manifest(dataset, "account_attribution")
    result = train_and_evaluate(
        dataset,
        split,
        event="account_attribution",
        model_id="fixture",
        as_of=NOW,
        iterations=150,
    )
    assert result["report"]["counts"]["test"]["unresolved"] == 1
    assert not result["report"]["release_eligible"]
    assert not result["report"]["gates"]["label_protocol"]
    assert result["report"]["effective_numeric_coverage"] == 0
    artifact = result["artifact"]
    assert predict(artifact)["value"] is None


def test_logistic_and_sigmoid_fit_predictive_direction_without_external_service():
    weights, intercept = fit_logistic(
        [[0], [0], [1], [1]], [0, 0, 1, 1], iterations=600
    )
    assert weights[0] > 0
    assert intercept < 0


def test_metrics_report_proper_scores_reliability_intervals_and_coverage():
    rows = reference_fixture(20)["examples"]
    predictions = [0.01 if row["label"] == 0 else 0.99 for row in rows]
    result = score_predictions(rows, predictions)
    assert result["brier"] == pytest.approx(0.0001)
    assert result["ece"] == pytest.approx(0.01)
    assert result["high_band"]["precision"] == 1
    assert result["high_band"]["recall"] == 1
    assert result["high_band"]["coverage"] == 0.5
    assert len(result["reliability"]) == 10
    assert result["high_band"]["precision_95_ci"][0] < 1
    assert wilson_interval(300, 300)[0] >= 0.95


@pytest.mark.parametrize("field", ["subject_id", "source_origin_ids", "account_ids"])
def test_copied_or_clustered_positive_band_does_not_inflate_precision_interval(field):
    rows = reference_fixture(300)["examples"]
    for row in rows:
        row["label"] = 1
        row[field] = "one-subject" if field == "subject_id" else ["one-shared-root"]
    report = score_predictions(rows, [0.99] * len(rows))
    assert report["high_band"]["precision"] == 1
    assert report["high_band"]["precision_95_ci"][0] < 0.95


def test_synthetic_evaluation_cannot_be_exported_as_reviewed_production_model():
    dataset = reference_fixture(30)
    split = build_split_manifest(dataset, "account_attribution")
    result = train_and_evaluate(
        dataset,
        split,
        event="account_attribution",
        model_id="test-only",
        as_of=NOW,
        iterations=80,
    )
    with pytest.raises(ValueError, match="empirical gates"):
        export_reviewed_artifact(result, "human-review")


def test_altered_evaluation_output_cannot_be_approved():
    artifact = serving_fixture()
    result = {
        "artifact": artifact,
        "artifact_sha256": artifact["artifact_sha256"],
        "report": deepcopy(artifact["validation"]),
    }
    result["report"]["release_eligible"] = False
    with pytest.raises(ValueError, match="digest or embedded report"):
        export_reviewed_artifact(result, "test-only")


def test_cli_refuses_to_repeat_consumed_locked_test(tmp_path):
    dataset = reference_fixture(30)
    reference = tmp_path / "reference.json"
    reference.write_text(json.dumps(dataset))
    split = tmp_path / "split.json"
    common = [sys.executable, "-m", "utils.evaluate_pipeline_probability"]
    options = [
        "--dataset",
        str(reference),
        "--split",
        str(split),
        "--event",
        "account_attribution",
    ]
    frozen = subprocess.run(
        common + ["freeze"] + options, capture_output=True, text=True
    )
    assert frozen.returncode == 0, frozen.stderr
    split.with_name("split.json.evaluation-used").write_text("already evaluated")
    evaluated = subprocess.run(
        common
        + ["evaluate"]
        + options
        + ["--output", str(tmp_path / "result.json"), "--model-id", "test-only"],
        capture_output=True,
        text=True,
    )
    assert evaluated.returncode != 0
    assert not (tmp_path / "result.json").exists()
