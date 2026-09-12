"""Offline contract checks against the real lease, source and delivery ledgers."""

import hashlib
import json
import os
import uuid

import pytest
from flask import Flask
from sqlalchemy import func, select, update

from maigret.web.case_store import CaseStore, investigation_jobs, utcnow
from maigret.web.pipeline_connector_ingestion import (
    ConnectorIngestionStore,
    authenticate_connector,
    superseded_observation_ids,
    validate_batch,
)
from maigret.web.pipeline_connector_routes import register_connector_ingestion_routes
from maigret.web.pipeline_store import PipelineStore


@pytest.fixture(params=["sqlite", "postgres"])
def env(tmp_path, request):
    postgres = os.getenv("OPENLEDGER_TEST_POSTGRES_URL")
    if request.param == "postgres" and not postgres:
        pytest.skip("OPENLEDGER_TEST_POSTGRES_URL is not configured")
    case = CaseStore(
        (
            postgres
            if request.param == "postgres"
            else f"sqlite:///{tmp_path}/ingestion.db"
        ),
        create_schema=True,
    )
    if request.param == "postgres":
        # The CI PostgreSQL service is shared across parametrized tests. Clear
        # abandoned queued feed jobs so claim_next cannot lease a prior test's
        # receipt instead of the case created below.
        with case.engine.begin() as connection:
            connection.execute(
                update(investigation_jobs)
                .where(investigation_jobs.c.status == "queued")
                .values(
                    status="cancelled",
                    cancel_requested=True,
                    cancel_requested_at=utcnow(),
                    completed_at=utcnow(),
                    updated_at=utcnow(),
                    error="Test fixture isolated the shared PostgreSQL queue",
                )
            )
    pipeline = PipelineStore(case)
    job_id = case.create_investigation(["synthetic-connector"], {})
    scope = case.get_job(job_id)["case_id"]
    subject = case.get_case(scope)["personas"][0]["id"]
    with case.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(status="running", worker_id="worker:test", heartbeat_at=utcnow())
        )
    yield case, pipeline, ConnectorIngestionStore(pipeline), scope, subject, job_id
    # PostgreSQL parametrizations share the CI database. Tests above intentionally
    # exercise durable acknowledgement without always consuming the queued job;
    # do not let those synthetic jobs leak into a later worker's global claim_next().
    # Production queue ordering is unchanged: this is fixture-scope cleanup only.
    now = utcnow()
    with case.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(
                investigation_jobs.c.case_id == scope,
                investigation_jobs.c.status.in_(
                    ("queued", "running", "cancel_requested")
                ),
            )
            .values(
                status="cancelled",
                cancel_requested=True,
                cancel_requested_at=now,
                completed_at=now,
                updated_at=now,
            )
        )
    case.dispose()


def attempt(env):
    _, p, _, case_id, persona_id, job_id = env
    request = p.create_request(
        case_id,
        persona_id,
        [{"type": "username", "value": "synthetic"}],
        {
            "tasks": [
                {
                    "task_id": str(uuid.uuid4()),
                    "engine_id": "fixture",
                    "route_state": "active",
                    "retry_ceiling": 2,
                }
            ]
        },
        actor="operator",
        job_id=job_id,
        idempotency_key=str(uuid.uuid4()),
    )
    task = request["tasks"][0]
    return p.start_attempt(task["id"], "worker:test"), task


def observation(key="record-a", value="Researcher", **kwargs):
    return {
        "id": key,
        "source_url": "https://example.org/public/a",
        "status": "found",
        "claims": [{"predicate": "occupation", "value": value}],
        **kwargs,
    }


def version(number="1", prior=None, operation="upsert"):
    return {
        "record_id": "a",
        "source_version": number,
        "supersedes_version": prior,
        "operation": operation,
    }


def commit(env, started, records, **kwargs):
    return env[2].checkpoint_page(
        started["id"], records, worker_id="worker:test", **kwargs
    )


def test_page_commit_advances_cursor_and_atomic_evidence(env):
    started, task = attempt(env)
    result = commit(
        env,
        started,
        [observation()],
        cursor="next:2",
        completeness="partial",
        source_versions=[version()],
        watermark="2026-09-11",
    )
    assert result["record_count"] == result["page_count"] == 1
    assert env[2].get_checkpoint(task["id"])["cursor"] == "next:2"
    assert len(env[1].list_observations(env[3], env[4])) == 1
    result = commit(
        env, started, [], cursor=None, expected_cursor="next:2", completeness="complete"
    )
    assert result["record_count"] == 1 and result["page_count"] == 2
    assert result["completeness"] == "complete"


def test_crash_retry_resumes_checkpoint_and_deduplicates_page(env):
    started, task = attempt(env)
    initial = commit(
        env,
        started,
        [observation()],
        cursor="2",
        completeness="partial",
        source_versions=[version()],
    )
    env[1].finish_attempt(started["id"], "error", worker_id="worker:test")
    successor = env[1].start_attempt(task["id"], "worker:test")
    replay = commit(
        env,
        successor,
        [observation(id="different-generated-id")],
        cursor="2",
        completeness="partial",
        source_versions=[version()],
    )
    assert (
        replay["replayed"] is True
        and replay["observation_ids"] == initial["observation_ids"]
    )
    assert replay["page_count"] == 1
    with pytest.raises(ValueError, match="Stale or completed"):
        commit(
            env, started, [], cursor=None, expected_cursor="2", completeness="complete"
        )


def test_page_and_cursor_roll_back_together_on_late_record_conflict(env):
    started, task = attempt(env)
    commit(env, started, [observation()], cursor="2", source_versions=[version()])
    with pytest.raises(ValueError, match="replay changed"):
        commit(
            env,
            started,
            [observation("new"), observation("bad", "Lawyer")],
            cursor="3",
            expected_cursor="2",
            source_versions=[{**version(), "record_id": "new"}, version()],
        )
    assert env[2].get_checkpoint(task["id"])["cursor"] == "2"
    assert len(env[1].list_observations(env[3], env[4])) == 1


def test_out_of_order_page_and_ambiguous_completeness_rejected(env):
    started, _ = attempt(env)
    for kwargs in [
        {"cursor": None, "completeness": "partial"},
        {"cursor": "2", "completeness": "complete"},
        {"cursor": "2", "expected_cursor": "missing", "completeness": "partial"},
    ]:
        with pytest.raises(ValueError):
            commit(env, started, [observation()], **kwargs)
    assert len(env[1].list_observations(env[3], env[4])) == 0


def test_stable_source_version_replayed_in_another_request(env):
    first, _ = attempt(env)
    saved = commit(
        env,
        first,
        [observation()],
        cursor=None,
        completeness="complete",
        source_versions=[version()],
    )
    second, _ = attempt(env)
    result = commit(
        env,
        second,
        [observation("new-generated-id")],
        cursor=None,
        completeness="complete",
        source_versions=[version()],
    )
    assert result["observation_ids"] == saved["observation_ids"]
    assert result["record_count"] == 0
    assert len(env[1].list_observations(env[3], env[4])) == 1


def test_source_update_and_withdrawal_retain_history_and_mark_old_support(env):
    first, _ = attempt(env)
    one = commit(
        env,
        first,
        [observation()],
        cursor=None,
        completeness="complete",
        source_versions=[version()],
    )
    second, _ = attempt(env)
    two = commit(
        env,
        second,
        [observation("v2", "Lawyer")],
        cursor=None,
        completeness="complete",
        source_versions=[version("2", "1")],
    )
    third, _ = attempt(env)
    commit(
        env,
        third,
        [{"id": "v3", "status": "inconclusive", "claims": []}],
        cursor=None,
        completeness="complete",
        source_versions=[version("3", "2", "withdraw")],
    )
    with env[0].engine.connect() as connection:
        superseded = superseded_observation_ids(connection, env[3], env[4])
    assert set(one["observation_ids"] + two["observation_ids"]) <= superseded
    assert len(superseded) == 3
    assert len(env[1].list_observations(env[3], env[4])) == 3


def test_source_update_requires_current_version_and_withdrawal_cannot_add_claims(env):
    first, _ = attempt(env)
    commit(
        env,
        first,
        [observation()],
        cursor=None,
        completeness="complete",
        source_versions=[version()],
    )
    second, _ = attempt(env)
    with pytest.raises(ValueError, match="current committed"):
        commit(
            env,
            second,
            [observation("v2")],
            cursor=None,
            completeness="complete",
            source_versions=[version("2")],
        )
    with pytest.raises(ValueError, match="withdrawal"):
        commit(
            env,
            second,
            [observation("v2")],
            cursor=None,
            completeness="complete",
            source_versions=[version("2", "1", "withdraw")],
        )


def test_updated_native_record_in_same_attempt_has_distinct_versioned_observation(env):
    started, _ = attempt(env)
    one = commit(
        env,
        started,
        [observation("same-native-id")],
        cursor="2",
        source_versions=[version()],
    )
    two = commit(
        env,
        started,
        [observation("same-native-id", "Lawyer")],
        cursor=None,
        expected_cursor="2",
        completeness="complete",
        source_versions=[version("2", "1")],
    )
    assert one["observation_ids"] != two["observation_ids"]
    assert two["record_count"] == 2


def test_context_checkpoint_reports_committed_ids_and_zero_new_records_on_replay(env):
    from maigret.web.pipeline_execution import CollectorContext

    _, config = identity(env)
    receipt = env[2].accept_batch(
        "fixture", config, batch(env), idempotency_key="ctx-replay"
    )
    job = env[0].claim_next("worker:feed")
    request = env[1].get_request(receipt["request_id"])
    stored_task = request["tasks"][0]
    task = {**stored_task["spec"], **stored_task}
    started = env[1].start_attempt(task["id"], "worker:feed")
    context = CollectorContext(
        env[0], env[1], job, request, task, started, None, lambda: False, {}, {}
    )
    source = batch(env)["records"][0]["data"]
    one = context.checkpoint_page(
        [source], None, completeness="complete", source_versions=[version()]
    )
    two = context.checkpoint_page(
        [source], None, completeness="complete", source_versions=[version()]
    )
    assert one["added_count"] == 1 and two["added_count"] == 0
    assert context.observation_count == 1 and context.persisted_ids == set(
        one["observation_ids"]
    )


def identity(env):
    token = "synthetic-test-token-" + "x" * 32
    identity = {
        "enabled": True,
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "scopes": [{"case_id": env[3], "persona_id": env[4]}],
    }
    return token, identity


def batch(env):
    return {
        "case_id": env[3],
        "persona_id": env[4],
        "records": [
            {
                **version(),
                "data": {
                    "source_url": "https://example.org/public/a",
                    "status": "found",
                    "claims": [{"predicate": "occupation", "value": "Researcher"}],
                },
            }
        ],
    }


def test_machine_credentials_require_explicit_scope_and_cannot_enter_evidence(env):
    token, config = identity(env)
    assert (
        authenticate_connector(
            "fixture",
            "Bearer " + token,
            {"PIPELINE_CONNECTOR_IDENTITIES": {"fixture": config}},
        )
        == config
    )
    for auth in ["", "Basic " + token, "Bearer invalid"]:
        with pytest.raises(PermissionError):
            authenticate_connector(
                "fixture", auth, {"PIPELINE_CONNECTOR_IDENTITIES": {"fixture": config}}
            )
    data = batch(env)
    data["records"][0]["data"]["details"] = {"Authorization": "Bearer secret"}
    with pytest.raises(ValueError, match="Credential"):
        validate_batch(data)
    data = batch(env)
    data["records"][0]["data"]["persona_id"] = "elsewhere"
    with pytest.raises(ValueError, match="override"):
        validate_batch(data)


def test_machine_endpoint_is_authenticated_even_without_browser_login(env):
    token, config = identity(env)
    app = Flask(__name__)
    app.config["PIPELINE_CONNECTOR_IDENTITIES"] = {"fixture": config}
    register_connector_ingestion_routes(app, get_case_store=lambda: env[0])
    client = app.test_client()
    assert (
        client.post("/api/connectors/fixture/batches", json=batch(env)).status_code
        == 401
    )
    headers = {"Authorization": "Bearer " + token, "Idempotency-Key": "batch-1"}
    escaped = batch(env) | {"persona_id": "other"}
    assert (
        client.post(
            "/api/connectors/fixture/batches", json=escaped, headers=headers
        ).status_code
        == 403
    )
    response = client.post(
        "/api/connectors/fixture/batches", json=batch(env), headers=headers
    )
    assert response.status_code == 202, response.json
    receipt = response.json
    assert env[0].get_job(receipt["job_id"])["status"] == "queued"
    assert env[1].get_request(receipt["request_id"])["actor"] == "connector:fixture"
    assert len(env[1].list_observations(env[3], env[4])) == 0
    replay = client.post(
        "/api/connectors/fixture/batches", json=batch(env), headers=headers
    )
    assert replay.status_code == 200 and replay.json["id"] == receipt["id"]
    altered = batch(env)
    altered["records"][0]["data"]["status"] = "inconclusive"
    assert (
        client.post(
            "/api/connectors/fixture/batches", json=altered, headers=headers
        ).status_code
        == 409
    )
    assert (
        client.get(
            "/api/connectors/fixture/batches/" + receipt["id"], headers=headers
        ).json["status"]
        == "queued"
    )
    with env[0].engine.connect() as connection:
        receipts = env[2].table("receipts")
        saved = (
            connection.execute(select(receipts).where(receipts.c.case_id == env[3]))
            .mappings()
            .all()
        )
    assert len(saved) == 1 and token not in json.dumps(
        [dict(row) for row in saved], default=str
    )


def test_failed_ack_replay_returns_one_durable_job(env):
    _, config = identity(env)
    accepted = env[2].accept_batch(
        "fixture", config, batch(env), idempotency_key="lost-ack"
    )
    replay = env[2].accept_batch(
        "fixture", config, batch(env), idempotency_key="lost-ack"
    )
    assert accepted["job_id"] == replay["job_id"] and replay["replayed"]
    with env[0].engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(investigation_jobs)
                .where(
                    investigation_jobs.c.kind == "connector_ingestion",
                    investigation_jobs.c.case_id == env[3],
                )
            )
            == 1
        )


def test_durable_inbox_cannot_bypass_retention_policy(env):
    _, config = identity(env)
    for policy in ("prohibited", "transient", "live_only"):
        with pytest.raises(ValueError, match="durable feed inbox"):
            env[2].accept_batch(
                "fixture",
                {**config, "retention": policy},
                batch(env),
                idempotency_key=policy,
            )
        payload = batch(env)
        payload["records"][0]["data"]["retention"] = {
            "mode": policy,
            "retainable": False,
        }
        with pytest.raises(ValueError, match="durable feed inbox"):
            env[2].accept_batch("fixture", config, payload, idempotency_key=policy)
    with pytest.raises(ValueError, match="Metadata-only"):
        env[2].accept_batch(
            "fixture",
            {**config, "retention": "metadata_only"},
            batch(env),
            idempotency_key="metadata",
        )
    payload = batch(env)
    payload["records"][0]["data"] = {
        "source_url": "https://example.org/public/a",
        "status": "candidate",
    }
    receipt = env[2].accept_batch(
        "fixture",
        {**config, "retention": "metadata_only"},
        payload,
        idempotency_key="metadata",
    )
    with env[0].engine.connect() as connection:
        saved = env[1]._row(connection, env[2].table("receipts"), receipt["id"])
    assert saved["payload"]["records"][0]["data"]["retention"] == {
        "mode": "metadata_only",
        "final_eligible": False,
    }


def test_concurrent_feed_delivery_queues_one_job(env):
    from concurrent.futures import ThreadPoolExecutor

    _, config = identity(env)
    # Create the subject's mutable state before racing the identical receipt.
    with env[0].engine.begin() as connection:
        env[1]._scope(connection, env[3], env[4], lock=True)
    with ThreadPoolExecutor(max_workers=2) as executor:
        deliveries = list(
            executor.map(
                lambda _: env[2].accept_batch(
                    "fixture", config, batch(env), idempotency_key="simultaneous"
                ),
                range(2),
            )
        )
    assert deliveries[0]["id"] == deliveries[1]["id"]
    assert sum(not item["replayed"] for item in deliveries) == 1


def run_feed_job(env, monkeypatch, *, configured=True):
    import importlib
    from maigret.web.pipeline_execution import execute_pipeline_job, dispatch_collector

    _, config = identity(env)
    app = importlib.import_module("maigret.web.app")
    monkeypatch.setattr(app, "case_store", env[0])
    monkeypatch.setitem(
        app.app.config,
        "PIPELINE_CONNECTOR_IDENTITIES",
        {"fixture": config} if configured else {},
    )
    job = env[0].claim_next("worker:feed")
    assert job["kind"] == "connector_ingestion"
    return execute_pipeline_job(
        env[0], job, adapters={"connector_feed": dispatch_collector}
    )


def test_machine_worker_processes_receipt_into_review_without_finalization(
    env, monkeypatch
):
    _, config = identity(env)
    receipt = env[2].accept_batch(
        "fixture", config, batch(env), idempotency_key="worker"
    )
    result = run_feed_job(env, monkeypatch)
    assert result["status"] == "completed", result
    assert (
        env[2].get_receipt(receipt["id"], connector_id="fixture", identity=config)[
            "status"
        ]
        == "completed"
    )
    workspace = env[1].get_workspace(env[3], env[4])
    assert any(group["kind"] == "claim" for group in workspace["groups"])
    assert env[1].get_final_version(env[3], env[4]) is None
    request = env[1].get_request(receipt["request_id"])
    assert (
        env[2].get_checkpoint(request["tasks"][0]["id"])["completeness"] == "complete"
    )


def test_machine_worker_honors_revoked_identity_and_reports_failed_receipt(
    env, monkeypatch
):
    _, config = identity(env)
    receipt = env[2].accept_batch(
        "fixture", config, batch(env), idempotency_key="revoked"
    )
    result = run_feed_job(env, monkeypatch, configured=False)
    assert result["status"] == "failed"
    saved = env[2].get_receipt(receipt["id"], connector_id="fixture", identity=config)
    assert (
        saved["status"] == "failed"
        and saved["error_code"] == "connector_authorization_changed"
    )
    assert not env[1].get_workspace(env[3], env[4])["groups"]


def test_metadata_only_rejects_nested_payload_before_durable_ack(env):
    _, config = identity(env)
    for field in ("source_url", "source_name", "status", "published_at"):
        payload = batch(env)
        payload["records"][0]["data"] = {
            field: {"restricted_payload": "must-not-persist"}
        }
        with pytest.raises(ValueError, match="bounded strings"):
            env[2].accept_batch(
                "fixture",
                {**config, "retention": "metadata_only"},
                payload,
                idempotency_key=field,
            )
    with env[0].engine.connect() as connection:
        table = env[2].table("receipts")
        assert (
            connection.scalar(
                select(func.count()).select_from(table).where(table.c.case_id == env[3])
            )
            == 0
        )


def test_machine_withdrawal_preserves_final_and_blocks_stale_support(env, monkeypatch):
    _, config = identity(env)
    env[2].accept_batch("fixture", config, batch(env), idempotency_key="initial")
    assert run_feed_job(env, monkeypatch)["status"] == "completed"
    p = env[1]
    group = next(
        group
        for group in p.get_workspace(env[3], env[4])["groups"]
        if group["kind"] == "claim"
    )
    p.decide(
        env[3],
        env[4],
        group["id"],
        "include",
        actor="operator",
        reason="Fixture verified",
    )
    final = p.create_version(
        env[3], env[4], actor="operator", scope="Document occupation"
    )
    p.qc(
        final["id"],
        "approved",
        actor="reviewer",
        permissions=["persona:qc"],
        expected_hash=final["content_hash"],
    )
    payload = batch(env)
    payload["records"] = [{**version("2", "1", "withdraw"), "data": {}}]
    env[2].accept_batch("fixture", config, payload, idempotency_key="withdrawal")
    assert run_feed_job(env, monkeypatch)["status"] == "completed"
    still_final = p.get_final_version(env[3], env[4])
    assert still_final["id"] == final["id"] and still_final["review_needed"]
    successor = p.create_version(
        env[3], env[4], actor="operator", scope="Review withdrawn source"
    )
    with pytest.raises(ValueError, match="evidence|support"):
        p.qc(
            successor["id"],
            "approved",
            actor="reviewer",
            permissions=["persona:qc"],
            expected_hash=successor["content_hash"],
        )
