"""Synthetic relational fixture for disposable migration/recovery acceptance only."""
from datetime import datetime, timezone
import hashlib
import json
from uuid import NAMESPACE_URL, uuid5
from sqlalchemy import MetaData, Table, insert, inspect, select, update


def table(connection, name):
    return Table(name, MetaData(), autoload_with=connection)


def snapshot(connection, names=None):
    names = sorted(names or inspect(connection).get_table_names())
    values = {}
    for name in names:
        rows = connection.execute(select(table(connection, name))).mappings()
        values[name] = sorted(json.dumps(dict(row), sort_keys=True, default=str) for row in rows)
    return {"sha256": hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest(),
            "counts": {name: len(rows) for name, rows in values.items()}}


def seed_historical(connection):
    ids = {kind: str(uuid5(NAMESPACE_URL, "openledger:recovery-fixture:" + kind))
           for kind in ("case", "persona", "job", "request", "task", "attempt", "observation", "group", "version", "decision", "qc", "receipt", "page", "record_version")}
    now = datetime(2026, 9, 11, tzinfo=timezone.utc)
    scope = {"case_id": ids["case"], "persona_id": ids["persona"]}
    def add(name, **values):
        connection.execute(insert(table(connection, name)).values(**values))
    add("cases", id=ids["case"], title="Disposable recovery acceptance", created_at=now, updated_at=now)
    add("personas", id=ids["persona"], case_id=ids["case"], display_name="Synthetic recovery subject", created_at=now)
    add("investigation_jobs", id=ids["job"], case_id=ids["case"], kind="investigation", status="completed", usernames=[], options={}, progress={}, result={}, created_at=now, updated_at=now)
    add("pipeline_requests", id=ids["request"], **scope, pipeline_id="p2-e2e-v1", job_id=ids["job"], actor="recovery-fixture", inputs=[], plan={}, plan_hash="a"*64, idempotency_key="recovery", status="completed", depth=0, created_at=now)
    add("pipeline_tasks", id=ids["task"], **scope, request_id=ids["request"], task_key="fixture", engine="fixture", input={}, spec={}, availability="active", status="completed", outcome="found", attempt_count=1, retry_limit=1, updated_at=now, created_at=now)
    add("pipeline_attempts", id=ids["attempt"], **scope, task_id=ids["task"], number=1, worker_id="recovery-fixture", status="completed", outcome="found", created_at=now, finished_at=now)
    add("pipeline_observations", id=ids["observation"], **scope, request_id=ids["request"], task_id=ids["task"], attempt_id=ids["attempt"], observation_key="source-record-v1", engine="fixture", outcome="found", source_url="https://example.test/fixture", content_hash="b"*64, retained=True, payload={"fixture": True, "source_record_id": "fixture", "value": "Retain this evidence"}, created_at=now)
    add("pipeline_groups", id=ids["group"], **scope, kind="claim", canonical_key="c"*64, normalized={"predicate": "summary", "value": "Synthetic fixture"}, created_at=now)
    add("pipeline_group_observations", **scope, group_id=ids["group"], observation_id=ids["observation"], created_at=now)
    add("pipeline_operator_decisions", id=ids["decision"], **scope, group_id=ids["group"], sequence=1, decision="include", actor="recovery-fixture", reason="Synthetic recovery fixture", details={}, created_at=now)
    add("pipeline_persona_versions", id=ids["version"], **scope, sequence=1, actor="recovery-fixture", workspace_revision=1, content_hash="d"*64, manifest={"fixture": True, "evidence": [ids["observation"]]}, created_at=now)
    add("pipeline_version_evidence", **scope, version_id=ids["version"], observation_id=ids["observation"])
    add("pipeline_qc_decisions", id=ids["qc"], **scope, version_id=ids["version"], decision="approved", actor="recovery-fixture", expected_hash="d"*64, findings=[], waivers=[], created_at=now)
    return ids


def seed_reliability(connection, ids):
    now = datetime(2026, 9, 11, tzinfo=timezone.utc)
    scope = {"case_id": ids["case"], "persona_id": ids["persona"]}
    def add(name, **values):
        connection.execute(insert(table(connection, name)).values(**values))
    add("pipeline_request_budgets", request_id=ids["request"], max_requests=3, consumed=2, updated_at=now)
    add("pipeline_provider_state", provider="fixture", consecutive_failures=3, cooldown_until=now, last_outcome="rate_limited", updated_at=now)
    add("pipeline_connector_checkpoints", **scope, task_id=ids["task"], attempt_id=ids["attempt"], cursor={"next": "page-2"}, watermark={"version": "v1"}, completeness="partial", page_count=1, record_count=1, updated_at=now)
    add("pipeline_connector_pages", id=ids["page"], **scope, task_id=ids["task"], attempt_id=ids["attempt"], page_key="e"*64, content_hash="f"*64, observation_ids=[ids["observation"]], created_at=now)
    add("pipeline_connector_record_versions", id=ids["record_version"], **scope, connector_id="fixture", record_id="record-1", source_version="v1", operation="upsert", content_hash="b"*64, observation_id=ids["observation"], created_at=now)
    add("pipeline_connector_record_heads", **scope, connector_id="fixture", record_id="record-1", version_id=ids["record_version"], updated_at=now)
    add("pipeline_connector_receipts", id=ids["receipt"], **scope, connector_id="fixture", idempotency_key="fixture-delivery", content_hash="a"*64, payload={"fixture": True}, job_id=ids["job"], request_id=ids["request"], status="completed", created_at=now, updated_at=now)
    state = table(connection, "pipeline_projection_state")
    if connection.scalar(select(state.c.persona_id).where(state.c.persona_id == ids["persona"])) is None:
        add("pipeline_projection_state", **scope, evidence_revision=1, projected_revision=0, updated_at=now)
    add("pipeline_group_summaries", **scope, group_id=ids["group"], normalized={"predicate": "summary", "value": "Synthetic fixture"}, assessment={"fixture": True}, observations=[{"id": ids["observation"]}], observation_count=1, revision_count=0, updated_at=now)
