"""End-to-end persistence invariants with synthetic source fixtures only."""

import os
import uuid

import pytest
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.exc import DBAPIError

from maigret.web.case_store import CaseStore, investigation_jobs, utcnow
from maigret.web.pipeline_schema import PIPELINE_ID
from maigret.web.pipeline_store import PipelineStore


@pytest.fixture(params=["sqlite", "postgres"])
def pair(request, tmp_path):
    postgres_url = os.getenv("OPENLEDGER_TEST_POSTGRES_URL")
    if request.param == "postgres" and not postgres_url:
        pytest.skip("OPENLEDGER_TEST_POSTGRES_URL is not configured")
    url = (
        postgres_url
        if request.param == "postgres"
        else f"sqlite:///{tmp_path}/pipeline.db"
    )
    store = CaseStore(url, create_schema=True)
    yield store, PipelineStore(store)
    store.dispose()


def subject(pair):
    store, pipeline = pair
    job_id = store.create_investigation(["synthetic-" + uuid.uuid4().hex[:8]], {})
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(status="running", worker_id="worker:test", heartbeat_at=utcnow())
        )
    case_id = store.get_job(job_id)["case_id"]
    persona_id = store.get_case(case_id)["personas"][0]["id"]
    return case_id, persona_id, job_id


def query(pair, *, scope=None, **kwargs):
    case_id, persona_id, job_id = scope or subject(pair)
    request = pair[1].create_request(
        case_id,
        persona_id,
        [{"type": "username", "value": "synthetic"}],
        {
            "pipeline_id": PIPELINE_ID,
            "tasks": [
                {
                    "task_id": "route-key",
                    "engine_id": "fixture",
                    "route_state": "active",
                    "retry_ceiling": 2,
                }
            ],
        },
        actor="operator",
        job_id=job_id,
        **kwargs,
    )
    return request


def evidence(
    pair,
    request=None,
    *,
    suffix="a",
    source_url="https://example.com/public/synthetic",
    outcome="found",
):
    request = request or query(pair)
    attempt = pair[1].start_attempt(request["tasks"][0]["id"], "worker:test")
    records = pair[1].record_observations(
        attempt["id"],
        [
            {
                "id": f"obs:{suffix}",
                "status": outcome,
                "source_url": source_url,
                "claims": [{"predicate": "occupation", "value": "Researcher"}],
                "retention": {"retainable": True},
            }
        ],
        outcome=outcome,
        worker_id="worker:test",
    )["observations"]
    return request, attempt, records


def curated(pair):
    request, attempt, records = evidence(pair)
    pipeline = pair[1]
    case_id, persona_id = request["case_id"], request["persona_id"]
    group = pipeline.upsert_groups(
        case_id,
        persona_id,
        {
            "claims": [
                {
                    "id": "clm:synthetic",
                    "predicate": "occupation",
                    "value": "Researcher",
                    "observation_ids": [records[0]["id"]],
                }
            ]
        },
        projection_revision=pipeline.projection_revision(case_id, persona_id),
    )[0]
    pipeline.decide(
        case_id,
        persona_id,
        group["id"],
        "include",
        actor="operator",
        reason="Public source identifies the subject",
    )
    version = pipeline.create_version(
        case_id, persona_id, actor="operator", scope="Assess documented occupation"
    )
    return request, records, group, version


def approve(pipeline, version, **kwargs):
    return pipeline.qc(
        version["id"],
        "approved",
        actor="reviewer",
        permissions=["persona:qc"],
        expected_hash=version["content_hash"],
        **kwargs,
    )


def test_qc_is_explicit_and_final_is_immutable(pair):
    request, records, group, version = curated(pair)
    pipeline = pair[1]
    assert version["status"] == "submitted"
    assert pipeline.get_final_version(request["case_id"], request["persona_id"]) is None
    with pytest.raises(PermissionError):
        pipeline.qc(
            version["id"],
            "approved",
            actor="analyst",
            permissions=[],
            expected_hash=version["content_hash"],
        )
    with pytest.raises(ValueError, match="hash"):
        approve(pipeline, dict(version, content_hash="0" * 64))
    approved = approve(pipeline, version)
    assert approved["status"] == "approved"
    pipeline.decide(
        request["case_id"],
        request["persona_id"],
        group["id"],
        "reject",
        actor="operator",
        reason="Later contradictory evidence",
    )
    final = pipeline.get_final_version(request["case_id"], request["persona_id"])
    assert final["content_hash"] == version["content_hash"]
    assert final["manifest"] == version["manifest"]
    assert final["review_needed"] is True
    with pytest.raises(DBAPIError):
        with pair[0].engine.begin() as connection:
            connection.execute(
                update(pipeline._table("persona_versions"))
                .where(pipeline._table("persona_versions").c.id == version["id"])
                .values(manifest={})
            )
    with pytest.raises(DBAPIError):
        with pair[0].engine.begin() as connection:
            connection.execute(
                delete(pipeline._table("observations")).where(
                    pipeline._table("observations").c.id == records[0]["id"]
                )
            )


def test_failed_qc_research_same_case_revision_then_final(pair):
    request, records, group, version = curated(pair)
    pipeline = pair[1]
    requirement = {
        "question": "Confirm occupation independently",
        "reason": "Only one origin supports this fact",
        "completion_criteria": "Independent public registry records the occupation",
        "inputs": [{"type": "full_name", "value": "Synthetic Person"}],
        "engines": ["fixture"],
        "request_budget": 1,
    }
    rejected = pipeline.qc(
        version["id"],
        "changes_required",
        actor="reviewer",
        permissions=["persona:qc"],
        expected_hash=version["content_hash"],
        findings=[{"group_id": group["id"], "reason": "Need independent origin"}],
        requirements=[requirement, requirement],
    )
    assert len(rejected["requirements"]) == 1
    requirement_id = rejected["requirements"][0]["id"]
    followup = query(
        pair,
        scope=(request["case_id"], request["persona_id"], request["job_id"]),
        idempotency_key="followup",
        parent_request_id=request["id"],
        requirement_ids=[requirement_id],
    )
    assert followup["case_id"] == request["case_id"]
    assert followup["persona_id"] == request["persona_id"]
    _, _, fresh = evidence(
        pair,
        followup,
        suffix="b",
        source_url="https://registry.example.org/public/occupation",
    )
    with pytest.raises(ValueError, match="budget"):
        query(
            pair,
            scope=(request["case_id"], request["persona_id"], request["job_id"]),
            idempotency_key="another",
            parent_request_id=request["id"],
            requirement_ids=[requirement_id],
        )
    with pytest.raises(ValueError, match="supporting observation"):
        pipeline.resolve_requirement(
            requirement_id,
            actor="operator",
            disposition="resolved",
            reason="Job finished",
        )
    pipeline.resolve_requirement(
        requirement_id,
        actor="operator",
        disposition="resolved",
        reason="Registry confirms occupation; completion criteria met",
        evidence_ids=[fresh[0]["id"]],
    )
    linked = pipeline.list_history(request["case_id"], request["persona_id"],
                                   "requests", parent_id=requirement_id)
    assert [item["id"] for item in linked["items"]] == [followup["id"]]
    history = pipeline.list_history(request["case_id"], request["persona_id"],
                                    "resolutions", parent_id=requirement_id)
    assert history["count"] == 1
    assert history["items"][0]["disposition"] == "resolved"
    # A new observation must enter consolidation before it can be frozen into
    # the successor. Completing a research requirement alone is insufficient.
    with pytest.raises(ValueError, match="awaits consolidation"):
        pipeline.create_version(
            request["case_id"],
            request["persona_id"],
            actor="operator",
            scope="Occupation",
        )
    pipeline.upsert_groups(
        request["case_id"],
        request["persona_id"],
        {
            "claims": [
                {
                    "id": "clm:synthetic",
                    "predicate": "occupation",
                    "value": "Researcher",
                    "observation_ids": [records[0]["id"], fresh[0]["id"]],
                }
            ]
        },
        projection_revision=pipeline.projection_revision(
            request["case_id"], request["persona_id"]
        ),
    )
    second = pipeline.create_version(
        request["case_id"],
        request["persona_id"],
        actor="operator",
        scope="Occupation",
        parent_version_id=version["id"],
    )
    assert second["sequence"] == 2
    assert approve(pipeline, second)["status"] == "approved"
    assert pipeline.get_version(version["id"])["status"] == "changes_required"
    assert (
        len(list(pipeline.iter_observations(request["case_id"], request["persona_id"])))
        == 2
    )


def test_idempotent_observations_preserve_source_and_attempt_lineage(pair):
    request = query(pair)
    pipeline = pair[1]
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], "worker:test")
    observation = {
        "id": "obs:source",
        "status": "found",
        "source_url": "https://example.com/source",
    }
    first = pipeline.append_observations(
        attempt["id"], [observation], worker_id="worker:test"
    )
    assert (
        pipeline.append_observations(
            attempt["id"], [observation], worker_id="worker:test"
        )
        == first
    )
    pipeline.record_observations(
        attempt["id"], [observation], outcome="found", worker_id="worker:test"
    )
    assert (
        pipeline.append_observations(
            attempt["id"], [observation], worker_id="worker:test"
        )
        == first
    )
    with pytest.raises(ValueError, match="immutable"):
        pipeline.append_observations(
            attempt["id"], [dict(observation, status="not_found")]
        )
    assert (
        len(list(pipeline.iter_observations(request["case_id"], request["persona_id"])))
        == 1
    )
    assert any(
        event["event"]["type"] == "pipeline_evidence_committed"
        for event in pair[0].get_events(request["job_id"])
    )


def test_consolidation_three_engines_preserves_all_evidence(pair):
    scope = subject(pair)
    records, pipeline = [], pair[1]
    for index in range(3):
        request = query(pair, scope=scope, idempotency_key=f"engine:{index}")
        _, _, observed = evidence(pair, request, suffix=f"engine-{index}")
        records.extend(observed)
    canonical = {
        "id": "clm:same",
        "predicate": "occupation",
        "value": "Researcher",
        "observation_ids": [item["observation_key"] for item in records],
    }
    group = pipeline.upsert_groups(
        scope[0],
        scope[1],
        {"claims": [canonical]},
        projection_revision=pipeline.projection_revision(scope[0], scope[1]),
    )[0]
    pipeline.upsert_groups(
        scope[0],
        scope[1],
        {"claims": [canonical]},
        projection_revision=pipeline.projection_revision(scope[0], scope[1]),
    )
    workspace = pipeline.get_workspace(scope[0], scope[1])
    assert workspace["group_count"] == 1
    assert pipeline.get_group(scope[0], scope[1], group["id"])["observation_count"] == 3


def test_stale_attempt_and_stale_submitted_version_cannot_finalize(pair):
    request = query(pair)
    pipeline = pair[1]
    with pair[0].engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == request["job_id"])
            .values(worker_id="worker:old")
        )
    first = pipeline.start_attempt(request["tasks"][0]["id"], "worker:old")
    pipeline.append_observations(
        first["id"],
        [{"id": "old", "status": "candidate", "source_url": "https://example.com"}],
        worker_id="worker:old",
    )
    with pair[0].engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == request["job_id"])
            .values(worker_id="worker:new")
        )
    second = pipeline.start_attempt(
        request["tasks"][0]["id"], "worker:new", resume=True
    )
    with pytest.raises(ValueError, match="Stale"):
        pipeline.finish_attempt(first["id"], "found", worker_id="worker:old")
    pipeline.finish_attempt(second["id"], "timeout", worker_id="worker:new")
    assert (
        len(list(pipeline.iter_observations(request["case_id"], request["persona_id"])))
        == 1
    )
    request, records, group, version = curated(pair)
    pipeline.decide(
        request["case_id"],
        request["persona_id"],
        group["id"],
        "include",
        actor="operator",
        reason="Additional review after submission",
    )
    with pytest.raises(ValueError, match="changed after submission"):
        approve(pipeline, version)


def test_cross_case_evidence_and_versions_rejected(pair):
    request, records, group, version = curated(pair)
    other = query(pair)
    pipeline = pair[1]
    with pytest.raises((KeyError, ValueError)):
        pipeline.upsert_groups(
            other["case_id"],
            other["persona_id"],
            {"claims": [{"id": "foreign", "observation_ids": [records[0]["id"]]}]},
            projection_revision=pipeline.projection_revision(
                other["case_id"], other["persona_id"]
            ),
        )
    with pytest.raises(KeyError):
        pipeline.get_version(version["id"], case_id=other["case_id"])
    with pytest.raises(KeyError):
        pipeline.decide(
            other["case_id"],
            other["persona_id"],
            group["id"],
            "include",
            actor="operator",
            reason="Foreign group",
        )


def test_unretained_or_missing_provenance_blocks_qc(pair):
    request, attempt, records = evidence(pair, source_url=None)
    pipeline = pair[1]
    group = pipeline.upsert_groups(
        request["case_id"],
        request["persona_id"],
        {"claims": [{"id": "missing", "observation_ids": [records[0]["id"]]}]},
        projection_revision=pipeline.projection_revision(
            request["case_id"], request["persona_id"]
        ),
    )[0]
    pipeline.decide(
        request["case_id"],
        request["persona_id"],
        group["id"],
        "include",
        actor="operator",
        reason="Try unsourced inclusion",
    )
    version = pipeline.create_version(
        request["case_id"],
        request["persona_id"],
        actor="operator",
        scope="Source validation",
    )
    with pytest.raises(ValueError, match="retained"):
        approve(pipeline, version)


def test_no_routes_research_needed_and_request_replay_atomic(pair):
    scope = subject(pair)
    pipeline = pair[1]
    inputs = [{"type": "phone", "value": "+620000000"}]
    plan = {
        "pipeline_id": PIPELINE_ID,
        "tasks": [
            {
                "engine_id": "public_web",
                "route_state": "unavailable",
                "reason": "Provider is not configured",
            }
        ],
    }
    with pair[0].engine.begin() as connection:
        first = pipeline.create_request(
            scope[0],
            scope[1],
            inputs,
            plan,
            actor="operator",
            job_id=scope[2],
            connection=connection,
        )
    second = pipeline.create_request(
        scope[0], scope[1], inputs, plan, actor="operator", job_id=scope[2]
    )
    assert first == second
    assert first["status"] == "research_needed"
    assert first["tasks"][0]["outcome"] == "not_executed"
    with pytest.raises(ValueError, match="different request"):
        pipeline.create_request(
            scope[0],
            scope[1],
            [{"type": "phone", "value": "+621111111"}],
            plan,
            actor="operator",
            job_id=scope[2],
        )


def test_withdraw_preserves_final_version_and_audit(pair):
    request, _, _, version = curated(pair)
    pipeline = pair[1]
    approve(pipeline, version)
    pipeline.withdraw_final(
        request["case_id"],
        request["persona_id"],
        actor="reviewer",
        permissions=["persona:qc"],
        reason="Evidence review required",
    )
    final = pipeline.get_final_version(request["case_id"], request["persona_id"])
    assert final["publication_status"] == "withdrawn"
    assert final["manifest"] == version["manifest"]


def test_split_preserves_original_ledger_and_does_not_auto_bind(pair):
    scope, pipeline = subject(pair), pair[1]
    request = query(pair, scope=scope)
    _, _, first = evidence(pair, request, suffix="first")
    second_request = query(pair, scope=scope, idempotency_key="second")
    _, _, second = evidence(pair, second_request, suffix="second")
    group = pipeline.upsert_groups(
        scope[0],
        scope[1],
        {
            "accounts": [
                {
                    "id": "acc:wrongly-joined",
                    "platform": "example",
                    "observation_ids": [first[0]["id"], second[0]["id"]],
                }
            ]
        },
        projection_revision=pipeline.projection_revision(scope[0], scope[1]),
    )[0]
    revision = pipeline.revise_group(
        scope[0],
        scope[1],
        group["id"],
        action="split",
        actor="operator",
        reason="Same handle was reused",
        observation_ids=[second[0]["id"]],
    )
    assert pipeline.get_group(scope[0], scope[1], group["id"])["observation_count"] == 1
    separated = pipeline.get_group(
        scope[0], scope[1], revision["details"]["target_group_id"]
    )
    assert separated["observation_count"] == 1
    assert separated["latest_decision"] is None
    assert len(list(pipeline.iter_observations(scope[0], scope[1]))) == 2


def test_explicit_cross_case_reuse_retains_original_id_and_independent_reviews(pair):
    request, records, _, _ = curated(pair)
    target = subject(pair)
    pipeline = pair[1]
    result = pipeline.reuse_evidence(
        request["case_id"],
        request["persona_id"],
        [records[0]["id"]],
        target_case_id=target[0],
        target_persona_id=target[1],
        actor="operator",
        reason="Same public registry supports this separate research question",
    )
    assert result["observations"][0]["original_observation_id"] == records[0]["id"]
    assert result["observations"][0]["case_id"] == target[0]
    assert pipeline.get_final_version(target[0], target[1]) is None
    assert all(
        group["latest_decision"] is None
        for group in pipeline.get_workspace(target[0], target[1])["groups"]
    )
    original = pipeline.list_observations(request["case_id"], request["persona_id"])
    assert original[0]["id"] == records[0]["id"]


def test_legacy_backfill_is_checkpointed_idempotent_and_never_finalizes(pair):
    scope = subject(pair)
    store, pipeline = pair
    store.sync_persona_claims(
        scope[2],
        {
            "individual_reports": [
                {
                    "username": store.get_job(scope[2])["usernames"][0],
                    "claimed_profiles": [
                        {
                            "site_name": "Example",
                            "url": "https://example.org/synthetic",
                            "confidence": "strong",
                            "evidence": {"fullname": "Synthetic Person"},
                        }
                    ],
                }
            ]
        },
    )
    dry_run = pipeline.backfill_legacy(scope[0], scope[1], actor="operator")
    assert dry_run["dry_run"] is True
    assert dry_run["claim_count"] > 0
    first = pipeline.backfill_legacy(
        scope[0], scope[1], actor="operator", dry_run=False
    )
    initial_count = len(list(pipeline.iter_observations(scope[0], scope[1])))
    second = pipeline.backfill_legacy(
        scope[0], scope[1], actor="operator", dry_run=False
    )
    assert first["request_id"] == second["request_id"]
    assert len(list(pipeline.iter_observations(scope[0], scope[1]))) == initial_count
    assert pipeline.get_final_version(scope[0], scope[1]) is None


def test_qc_contradiction_requires_explicit_exclusion_and_limitation(pair):
    request, records, first_group, _ = curated(pair)
    pipeline = pair[1]
    groups = pipeline.upsert_groups(
        request["case_id"],
        request["persona_id"],
        {
            "claims": [
                {
                    "id": "clm:synthetic",
                    "predicate": "birth_date",
                    "value": "1990-01-01",
                    "conflicts": ["conflict:birth"],
                    "observation_ids": [records[0]["id"]],
                },
                {
                    "id": "clm:conflicting",
                    "predicate": "birth_date",
                    "value": "1991-01-01",
                    "conflicts": ["conflict:birth"],
                    "observation_ids": [records[0]["id"]],
                },
            ]
        },
        projection_revision=pipeline.projection_revision(
            request["case_id"], request["persona_id"]
        ),
    )
    blocked = pipeline.create_version(
        request["case_id"],
        request["persona_id"],
        actor="operator",
        scope="Birth-date research",
    )
    with pytest.raises(ValueError, match="conflict"):
        approve(pipeline, blocked)
    pipeline.decide(
        request["case_id"],
        request["persona_id"],
        groups[1]["id"],
        "exclude",
        actor="operator",
        reason="Source identifies a different person",
    )
    resolved = pipeline.create_version(
        request["case_id"],
        request["persona_id"],
        actor="operator",
        scope="Birth-date research",
        limitations=[
            "Conflicting record was excluded because it identifies another person"
        ],
    )
    assert approve(pipeline, resolved)["status"] == "approved"


def test_expired_job_lease_fences_append_and_cancel_preserves_committed_rows(pair):
    from datetime import timedelta

    request = query(pair)
    pipeline = pair[1]
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], "worker:test")
    pipeline.append_observations(
        attempt["id"],
        [{"id": "committed", "status": "found", "source_url": "https://example.com/a"}],
        worker_id="worker:test",
    )
    with pair[0].engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == request["job_id"])
            .values(heartbeat_at=utcnow() - timedelta(minutes=10))
        )
    with pytest.raises(ValueError, match="lease"):
        pipeline.append_observations(
            attempt["id"], [{"id": "late", "status": "found"}], worker_id="worker:test"
        )
    cancelled = pipeline.cancel_request(
        request["id"], reason="Operator stopped stale collection"
    )
    assert cancelled["status"] == "cancelled"
    assert cancelled["tasks"][0]["attempts"][0]["outcome"] == "cancelled"
    assert (
        len(list(pipeline.iter_observations(request["case_id"], request["persona_id"])))
        == 1
    )


def test_additive_migration_preserves_base_and_refuses_populated_downgrade(
    tmp_path, monkeypatch
):
    import importlib.util
    from pathlib import Path
    from maigret.web.case_store import metadata
    from sqlalchemy import create_engine, inspect

    engine = create_engine(f"sqlite:///{tmp_path}/migration.db")
    legacy = [
        table
        for table in metadata.sorted_tables
        if not table.name.startswith("pipeline_")
    ]
    metadata.create_all(engine, tables=legacy)
    module_path = (
        Path(__file__).parents[1]
        / "migrations/versions/e2e1a7c9d401_add_p2_end_to_end_pipeline.py"
    )
    spec = importlib.util.spec_from_file_location("pipeline_migration", module_path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    with engine.begin() as connection:
        monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
        migration.upgrade()
        assert (
            len(
                [
                    name
                    for name in inspect(connection).get_table_names()
                    if name.startswith("pipeline_")
                ]
            )
            == 16
        )
        migration.downgrade()
        assert not any(
            name.startswith("pipeline_")
            for name in inspect(connection).get_table_names()
        )
        migration.upgrade()
    # Seed the frozen historic schema directly. Current application services may
    # legitimately require newer additive tables and must not run against e2e1.
    fixture_spec = importlib.util.spec_from_file_location(
        "historical_recovery_fixture", Path(__file__).parents[1] / "deploy/recovery-fixture.py"
    )
    fixture = importlib.util.module_from_spec(fixture_spec)
    fixture_spec.loader.exec_module(fixture)
    with engine.begin() as connection:
        fixture.seed_historical(connection)
        monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
        with pytest.raises(RuntimeError, match="preserve expanded schema"):
            migration.downgrade()
    engine.dispose()


def test_backfill_page_size_changes_do_not_repeat_imports(pair):
    scope = subject(pair)
    store, pipeline = pair
    store.sync_persona_claims(
        scope[2],
        {
            "individual_reports": [
                {
                    "username": store.get_job(scope[2])["usernames"][0],
                    "claimed_profiles": [
                        {
                            "site_name": "Example",
                            "url": "https://example.org/paging",
                            "confidence": "strong",
                            "evidence": {
                                "fullname": "Synthetic Person",
                                "description": "Synthetic public profile",
                            },
                        }
                    ],
                }
            ]
        },
    )
    cursor = None
    while True:
        result = pipeline.backfill_legacy(
            scope[0],
            scope[1],
            actor="system:legacy-workspace-import",
            dry_run=False,
            limit=1,
            after_claim_id=cursor,
            materialize=False,
        )
        cursor = result["next_after_claim_id"]
        if cursor is None:
            break
    before = len(list(pipeline.iter_observations(scope[0], scope[1])))
    assert before > 1
    result = pipeline.backfill_legacy(
        scope[0],
        scope[1],
        actor="system:legacy-workspace-import",
        dry_run=False,
        limit=500,
    )
    assert result["pending_claim_count"] == 0
    assert len(list(pipeline.iter_observations(scope[0], scope[1]))) == before


def test_ordinary_deletion_returns_clear_retention_error_before_mutation(pair):
    request, records, _, version = curated(pair)
    store, pipeline = pair
    approve(pipeline, version)
    assert store.finish(
        request["job_id"],
        {
            "status": "completed",
            "usernames": store.get_job(request["job_id"])["usernames"],
        },
        worker_id="worker:test",
    )
    with pytest.raises(ValueError, match="retained P2 pipeline"):
        store.delete_job(request["job_id"])
    with pytest.raises(ValueError, match="retained P2 pipeline"):
        store.delete_case(request["case_id"])
    assert store.get_job(request["job_id"])
    assert (
        pipeline.get_final_version(request["case_id"], request["persona_id"])[
            "content_hash"
        ]
        == version["content_hash"]
    )
    assert (
        pipeline.list_observations(request["case_id"], request["persona_id"])[0]["id"]
        == records[0]["id"]
    )


def test_legitimate_worker_acknowledges_case_store_cancel_without_losing_evidence(pair):
    request = query(pair)
    store, pipeline = pair
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], "worker:test")
    pipeline.append_observations(
        attempt["id"],
        [
            {
                "id": "before-stop",
                "status": "candidate",
                "source_url": "https://example.com/public-before-stop",
            }
        ],
        worker_id="worker:test",
    )
    assert store.request_cancel(request["job_id"])
    assert store.get_job(request["job_id"])["status"] == "cancel_requested"
    with pytest.raises(ValueError, match="Stale"):
        pipeline.append_observations(
            attempt["id"],
            [{"id": "after-stop", "status": "found"}],
            worker_id="worker:test",
        )
    cancelled = pipeline.cancel_request(
        request["id"], reason="Requested by operator", worker_id="worker:test"
    )
    assert cancelled["tasks"][0]["attempts"][0]["outcome"] == "cancelled"
    assert (
        len(list(pipeline.iter_observations(request["case_id"], request["persona_id"])))
        == 1
    )


def test_worker_shutdown_preserves_pending_tasks_and_bounded_retry(pair):
    scope = subject(pair)
    pipeline = pair[1]
    request = pipeline.create_request(
        scope[0],
        scope[1],
        [{"type": "username", "value": "synthetic"}],
        {
            "tasks": [
                {
                    "engine_id": "fixture-a",
                    "task_id": "active",
                    "route_state": "active",
                    "retry_ceiling": 1,
                },
                {
                    "engine_id": "fixture-b",
                    "task_id": "pending",
                    "route_state": "active",
                    "retry_ceiling": 0,
                },
            ]
        },
        actor="operator",
        job_id=scope[2],
    )
    active_task, pending_task = request["tasks"]
    attempt = pipeline.start_attempt(active_task["id"], "worker:test")
    pipeline.append_observations(
        attempt["id"],
        [
            {
                "id": "before-shutdown",
                "status": "found",
                "source_url": "https://example.com/shutdown",
            }
        ],
        worker_id="worker:test",
    )
    interrupted = pipeline.interrupt_request(request["id"], worker_id="worker:test")
    assert interrupted["status"] == "interrupted"
    assert pipeline.get_task(pending_task["id"])["status"] == "planned"
    recovered = pipeline.start_attempt(active_task["id"], "worker:test")
    assert recovered["number"] == 2
    exhausted = pipeline.interrupt_request(request["id"], worker_id="worker:test")
    assert "retry budget exhausted" in exhausted["tasks"][0]["reason"]
    with pytest.raises(ValueError, match="retry budget"):
        pipeline.start_attempt(active_task["id"], "worker:test")
    assert len(list(pipeline.iter_observations(scope[0], scope[1]))) == 1


def test_qc_cannot_approve_and_open_new_research_in_one_action(pair):
    request, records, group, version = curated(pair)
    pipeline = pair[1]
    with pytest.raises(ValueError, match='unresolved research'):
        approve(
            pipeline,
            version,
            requirements=[
                {
                    'question': 'Verify this occupation',
                    'reason': 'Missing independent corroboration',
                    'completion_criteria': 'Obtain an attributable independent source',
                    'inputs': [{'type': 'full_name', 'value': 'Synthetic Person'}],
                }
            ],
        )
    assert pipeline.get_final_version(request['case_id'], request['persona_id']) is None
    assert pipeline.get_version(version['id'])['status'] == 'submitted'


def test_qc_material_findings_and_structured_scope_remain_blockers(pair):
    request, records, group, version = curated(pair)
    with pytest.raises(ValueError, match='Material QC'):
        approve(
            pair[1],
            version,
            findings=[
                {'severity': 'material', 'reason': 'Attribution remains unresolved'}
            ],
        )
    scoped = pair[1].create_version(
        request['case_id'],
        request['persona_id'],
        actor='operator',
        scope={
            'description': 'Confirm identity',
            'mandatory_fields': {'full_name': False},
        },
    )
    with pytest.raises(ValueError, match='QC'):
        approve(pair[1], scoped)
    assert pair[1].get_final_version(request['case_id'], request['persona_id']) is None


def test_correction_cannot_remove_identity_conflicts(pair):
    request, attempt, records = evidence(pair)
    pipeline = pair[1]
    case_id, persona_id = request["case_id"], request["persona_id"]
    group = pipeline.upsert_groups(
        case_id,
        persona_id,
        {
            "claims": [
                {
                    "id": "conflicted",
                    "predicate": "occupation",
                    "value": "Researcher",
                    "material_identity_conflict": True,
                    "observation_ids": [records[0]["id"]],
                }
            ]
        },
        projection_revision=pipeline.projection_revision(case_id, persona_id),
    )[0]
    with pytest.raises(ValueError, match="conflict"):
        pipeline.decide(
            case_id,
            persona_id,
            group["id"],
            "include",
            actor="operator",
            reason="Change text",
            corrected_claim={
                "predicate": "occupation",
                "value": "Scientist",
                "material_identity_conflict": False,
            },
        )
    pipeline.decide(
        case_id,
        persona_id,
        group["id"],
        "include",
        actor="operator",
        reason="Correct occupation label only",
        corrected_claim={"predicate": "occupation", "value": "Scientist"},
    )
    version = pipeline.create_version(
        case_id, persona_id, actor="operator", scope="Document occupation"
    )
    assert (
        version["manifest"]["items"][0]["normalized"]["material_identity_conflict"]
        is True
    )
    with pytest.raises(ValueError, match="QC"):
        approve(pipeline, version)


def test_research_does_not_use_accounts_rebound_to_another_persona(pair):
    from maigret.web.case_store import personas

    request, attempt, records = evidence(pair)
    pipeline = pair[1]
    case_id, persona_id = request["case_id"], request["persona_id"]
    target_id = str(uuid.uuid4())
    with pair[0].engine.begin() as connection:
        connection.execute(
            insert(personas).values(
                id=target_id,
                case_id=case_id,
                display_name="Other synthetic subject",
                created_at=utcnow(),
            )
        )
    group = pipeline.upsert_groups(
        case_id,
        persona_id,
        {
            "accounts": [
                {
                    "id": "bound-account",
                    "platform": "github",
                    "canonical_url": "https://github.com/synthetic",
                    "observation_ids": [records[0]["id"]],
                }
            ]
        },
        projection_revision=pipeline.projection_revision(case_id, persona_id),
    )[0]
    pipeline.decide(
        case_id,
        persona_id,
        group["id"],
        "include",
        actor="operator",
        reason="Initial attribution",
    )
    assert list(pipeline.iter_included_groups(case_id, persona_id))
    pipeline.revise_group(
        case_id,
        persona_id,
        group["id"],
        action="bind",
        actor="operator",
        reason="Public evidence identifies the other subject",
        target_persona_id=target_id,
    )
    assert list(pipeline.iter_included_groups(case_id, persona_id)) == []
