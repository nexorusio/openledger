"""Actual store-to-assessment-to-QC binding with explicitly synthetic artifacts."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import func, select

from maigret.web.pipeline_assessment import validate_frozen_probability
from maigret.web.pipeline_assessment_runtime import (
    assess_consolidated,
    assess_consolidated_groups,
    configured_artifact,
    derive_assessment_scope,
)
from maigret.web.pipeline_consolidation import consolidate_observations
from maigret.web.pipeline_evidence import normalize_observation
from maigret.web.pipeline_probability import seal_artifact
from maigret.web.pipeline_schema import PIPELINE_ID
from tests.test_pipeline_store import pair, subject, approve  # noqa: F401
from tests.test_pipeline_probability import serving_fixture


def collected(pair):
    case_id, persona_id, job_id = subject(pair)
    pipeline = pair[1]
    request = pipeline.create_request(
        case_id,
        persona_id,
        [{"type": "username", "value": "fixture"}],
        {
            "pipeline_id": PIPELINE_ID,
            "tasks": [
                {
                    "task_id": "fixture-route",
                    "engine_id": "github_public_profile",
                    "platform": "github",
                    "route_state": "active",
                    "input_type": "username",
                    "input_value": "fixture",
                    "source_config_revision": "fixture-config-v1",
                }
            ],
        },
        actor="operator",
        job_id=job_id,
    )
    task = request["tasks"][0]
    attempt = pipeline.start_attempt(task["id"], "worker:test")
    now = datetime.now(timezone.utc)
    observation = normalize_observation(
        {
            "status": "found",
            "source_url": "https://github.com/fixture",
            "source_engine": "github_public_profile",
            "language": "id",
            "engine_version": "fixture-engine-v1",
            "parser_version": "fixture-parser-v1",
            "account": {
                "platform": "github",
                "stable_id": "9990001",
                "profile_url": "https://github.com/fixture",
            },
        },
        case_id=case_id,
        subject_id=persona_id,
        request_id=request["id"],
        task_id=task["id"],
        attempt_id=attempt["id"],
        observed_at=now.isoformat(),
    )
    account = consolidate_observations([observation])["accounts"][0]
    observation["evidence_signals"] = {
        "source_link_match": {
            "matched": True,
            "evidence_ref": observation["source_url"],
            "subject_id": persona_id,
            "hypothesis_key": account["id"],
            "method": "explicit_profile_link",
        }
    }
    pipeline.record_observations(
        attempt["id"], [observation], outcome="found", worker_id="worker:test"
    )
    return request, task, observation


def artifact_config(tmp_path, group, observation, task):
    """TEST-ONLY report fixture exercises gates; it is never a production model."""
    artifact = serving_fixture()
    scope = derive_assessment_scope(
        group, [observation], tasks_by_id={task["id"]: task}
    )
    artifact["validated_scopes"][0].update(scope)
    now = datetime.now(timezone.utc)
    artifact["generated_at"] = (now - timedelta(days=1)).isoformat()
    artifact["expires_at"] = (now + timedelta(days=29)).isoformat()
    artifact = seal_artifact(artifact)
    path = tmp_path / "TEST-ONLY-artifact.json"
    path.write_text(json.dumps(artifact))
    return {
        "OPENLEDGER_PROBABILITY_ARTIFACT": str(path),
        "OPENLEDGER_PROBABILITY_ARTIFACT_SHA256": artifact["artifact_sha256"],
    }


def curated_snapshot(pair, tmp_path, *, mutate=None):
    request, task, observation = collected(pair)
    pipeline = pair[1]
    consolidated = consolidate_observations([observation])
    account = consolidated["accounts"][0]
    config = artifact_config(tmp_path, account, observation, task)
    assessments = assess_consolidated(
        consolidated, [observation], tasks_by_id={task["id"]: task}, environ=config
    )
    snapshot = assessments[account["canonical_key"]]
    assert snapshot["probability"]["value"] is not None
    if mutate:
        mutate(snapshot)
    groups = pipeline.materialize_groups(
        request["case_id"],
        request["persona_id"],
        consolidated,
        projection_revision=pipeline.projection_revision(
            request["case_id"], request["persona_id"]
        ),
        assessments=assessments,
    )
    stored = next(group for group in groups if group["kind"] == "account")
    pipeline.decide(
        request["case_id"],
        request["persona_id"],
        stored["id"],
        "include",
        actor="operator",
        reason="TEST-ONLY explicit source link reviewed",
    )
    version = pipeline.create_version(
        request["case_id"],
        request["persona_id"],
        actor="operator",
        scope="TEST-ONLY account attribution",
    )
    return request, stored, version


def test_runtime_consolidates_and_persists_assessment_under_correct_canonical_group(
    pair,
):
    request, task, observation = collected(pair)
    pipeline = pair[1]
    groups = assess_consolidated_groups(
        pipeline, request["case_id"], request["persona_id"], environ={}
    )
    assert groups
    for group in groups:
        expanded = pipeline.get_group(
            request["case_id"], request["persona_id"], group["id"]
        )
        assessment = expanded["assessment"]
        assert assessment["group_id"] == expanded["normalized"]["id"]
        assert assessment["case_id"] == request["case_id"]
        assert assessment["subject_id"] == request["persona_id"]
        assert assessment["evidence_counts"]["observations"] == 1
        assert assessment["probability"]["value"] is None
        assert assessment["operating_scope"]["input_type"] == "username"
        assert assessment["operating_scope"]["language"] == "id"
        assert assessment["operating_scope"]["source_revision"].startswith("sources:")


def test_replayed_refresh_does_not_create_new_assessments_or_invalidate_submitted_version(
    pair,
):
    request, task, observation = collected(pair)
    pipeline = pair[1]
    groups = assess_consolidated_groups(
        pipeline, request["case_id"], request["persona_id"], environ={}
    )
    selected = next(group for group in groups if group["kind"] == "account")
    pipeline.decide(
        request["case_id"],
        request["persona_id"],
        selected["id"],
        "include",
        actor="operator",
        reason="Source reviewed",
    )
    version = pipeline.create_version(
        request["case_id"],
        request["persona_id"],
        actor="operator",
        scope="Account evidence",
    )
    table = pipeline._table("assessments")
    with pipeline.engine.connect() as connection:
        before = connection.scalar(select(func.count()).select_from(table))
    assess_consolidated_groups(
        pipeline, request["case_id"], request["persona_id"], environ={}
    )
    with pipeline.engine.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(table)) == before
    assert approve(pipeline, version)["status"] == "approved"


def test_missing_source_language_and_revision_are_not_guessed(pair):
    request, task, observation = collected(pair)
    observation = deepcopy(observation)
    observation["payload"].pop("language")
    observation["parser_version"] = "unknown"
    consolidated = consolidate_observations([observation])
    scope = derive_assessment_scope(
        consolidated["accounts"][0], [observation], tasks_by_id={task["id"]: task}
    )
    assert scope["language"] is None
    assert scope["source_revision"] is None


def test_missing_member_is_an_error_not_silently_omitted_source_evidence(pair):
    _, _, observation = collected(pair)
    consolidated = consolidate_observations([observation])
    with pytest.raises(ValueError, match="membership"):
        assess_consolidated(consolidated, [], environ={})


@pytest.mark.parametrize(
    "config",
    [
        {"OPENLEDGER_PROBABILITY_ARTIFACT": "/hidden/server/path"},
        {
            "OPENLEDGER_PROBABILITY_ARTIFACT": "/hidden/server/path",
            "OPENLEDGER_PROBABILITY_ARTIFACT_SHA256": "missing",
        },
    ],
)
def test_invalid_runtime_artifact_configuration_abstains_without_exposing_paths(config):
    artifact, pin, error = configured_artifact(config)
    assert artifact is None and pin is None
    assert error and "/hidden" not in error


def test_event_specific_incomplete_configuration_cannot_silently_use_generic_model(
    tmp_path,
):
    path = tmp_path / "TEST-ONLY-generic.json"
    path.write_text(json.dumps(serving_fixture()))
    config = {
        "OPENLEDGER_PROBABILITY_ARTIFACT": str(path),
        "OPENLEDGER_PROBABILITY_ARTIFACT_SHA256": "generic-pin",
        "OPENLEDGER_PROBABILITY_CLAIM_CORRECTNESS_ARTIFACT": "/missing/claim-artifact",
    }
    assert configured_artifact(config, event="account_attribution")[2] is None
    assert (
        configured_artifact(config, event="claim_correctness")[2]
        == "incomplete_probability_configuration"
    )


def test_valid_bound_snapshot_reaches_explicit_qc_with_review_metadata(pair, tmp_path):
    request, group, version = curated_snapshot(pair, tmp_path)
    item = version["manifest"]["items"][0]
    assert item["assessed_group"]["id"] == item["assessment"]["group_id"]
    assert (
        item["assessment"]["probability"]["review"]["approval_reference"] == "TEST-ONLY"
    )
    assert (
        validate_frozen_probability(
            item, case_id=request["case_id"], subject_id=request["persona_id"]
        )
        is None
    )
    assert approve(pair[1], version)["status"] == "approved"


@pytest.mark.parametrize(
    "mutate,reason",
    [
        (
            lambda snapshot: snapshot["probability"].update(evidence_digest="stale"),
            "frozen source evidence",
        ),
        (lambda snapshot: snapshot.update(subject_id="foreign"), "another case"),
        (lambda snapshot: snapshot["probability"].update(review={}), "review metadata"),
        (
            lambda snapshot: snapshot["probability"].update(event="claim_correctness"),
            "event/model",
        ),
        (
            lambda snapshot: snapshot["probability"].update(serving_gate_passed=False),
            "valid serving",
        ),
        (
            lambda snapshot: snapshot["features"].update(source_link_match=10),
            "frozen source evidence",
        ),
        (
            lambda snapshot: snapshot["probability"].update(
                expires_at="2000-01-01T00:00:00Z"
            ),
            "current, dated",
        ),
    ],
)
def test_actual_store_qc_rejects_stale_foreign_or_unreviewed_probability(
    pair, tmp_path, mutate, reason
):
    _, _, version = curated_snapshot(pair, tmp_path, mutate=mutate)
    with pytest.raises(ValueError, match=reason):
        approve(pair[1], version)


def test_account_identity_cannot_be_replaced_by_claim_correction(pair, tmp_path):
    request, group, first = curated_snapshot(pair, tmp_path)
    pipeline = pair[1]
    with pytest.raises(ValueError, match="claim group"):
        pipeline.decide(
            request["case_id"],
            request["persona_id"],
            group["id"],
            "include",
            actor="operator",
            reason="Attempt to substitute another account",
            corrected_claim={
                "kind": "account",
                "platform": "github",
                "canonical_url": "https://github.com/another-fixture",
            },
        )
    assert pipeline.get_version(first["id"])["manifest"] == first["manifest"]
