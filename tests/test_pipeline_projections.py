"""Read-only, bounded workspace pages and atomic evidence projection checkpoints."""

from concurrent.futures import ThreadPoolExecutor
import time
import tracemalloc
import uuid

import pytest
from sqlalchemy import event, insert, select

from maigret.web.pipeline_assessment_runtime import assess_consolidated_groups
from maigret.web.pipeline_evidence import normalize_observation
from tests.test_pipeline_routes import journey, base  # noqa: F401
from tests.test_pipeline_store import pair, query  # noqa: F401


def test_workspace_50k_evidence_pages_are_bounded_and_do_not_reassess(
    journey, monkeypatch
):
    pipeline = journey["pipeline"]
    case_id, persona_id = journey["case_id"], journey["persona_id"]
    table = pipeline._table("observations")
    with pipeline.engine.connect() as connection:
        original = dict(connection.execute(select(table)).mappings().first())
    ids = [original["id"]]
    # Seed immutable source history in batches, keeping the same real attempt
    # scope. The enormous assessment source-ID list models one dense account.
    with pipeline.engine.begin() as connection:
        pipeline._scope(connection, case_id, persona_id, lock=True)
        for batch in range(100):
            rows = []
            for index in range(500):
                oid = str(uuid.uuid4())
                ids.append(oid)
                rows.append(
                    dict(
                        original,
                        id=oid,
                        observation_key=oid,
                        payload=dict(original["payload"], id=oid),
                    )
                )
            connection.execute(insert(table), rows)
        pipeline._invalidate_projection(connection, case_id, persona_id)
    pipeline.upsert_groups(
        case_id,
        persona_id,
        {
            "claims": [
                {
                    "canonical_key": "synthetic-full-name",
                    "normalized": {
                        "predicate": "full_name",
                        "value": "Synthetic Person",
                    },
                    "observation_ids": ids,
                }
            ]
        },
        projection_revision=pipeline.projection_revision(case_id, persona_id),
        assessments={
            "synthetic-full-name": {
                "evidence_counts": {"observations": len(ids)},
                "origin_families": [
                    {"origin_family_id": oid, "observation_ids": [oid]} for oid in ids
                ],
                "freshness": {"age_days_by_observation": {oid: 1 for oid in ids}},
                "probability": {"value": None, "reason": "No validated model"},
            }
        },
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("A read page attempted to load/reassess all evidence")

    monkeypatch.setattr(pipeline.__class__, "iter_observations", forbidden)
    monkeypatch.setattr(
        "maigret.web.pipeline_assessment_runtime.assess_consolidated", forbidden
    )
    monkeypatch.setattr(journey["store"], "get_persona", forbidden)
    statements = []

    def capture(connection, cursor, statement, parameters, context, many):
        statements.append(statement.lower())

    event.listen(pipeline.engine, "before_cursor_execute", capture)
    client = journey["client"]
    # Prime Flask/Jinja caches before measuring the bounded read, not fixture creation.
    response = client.get("/api" + base(journey))
    assert response.status_code == 200
    statements.clear()
    tracemalloc.start()
    started = time.monotonic()
    try:
        for offset in (0, 25, 0):
            response = client.get("/api" + base(journey) + f"?offset={offset}")
            assert response.status_code == 200
            assert len(response.data) < 100_000
            data = response.get_json()
            if offset == 0:
                group = data["groups"][0]
                assert group["observation_count"] == 50_001
                assert len(group["observations"]) == 3
                assert group["assessment"]["evidence_counts"]["observations"] == 50_001
                assert "origin_families" not in group["assessment"]
        elapsed = time.monotonic() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        event.remove(pipeline.engine, "before_cursor_execute", capture)
    assert len(statements) < 150
    assert not any("from pipeline_assessments" in sql for sql in statements)
    assert not any(
        sql.lstrip().startswith(("insert ", "update ", "delete ")) for sql in statements
    )
    assert peak < 8_000_000
    # Deliberately generous regression ceiling, not a production latency promise.
    assert elapsed < 15
    print(
        f"workspace_50k reads=3 sql={len(statements)} peak_bytes={peak} elapsed_seconds={elapsed:.3f}"
    )

    def read_independently(_):
        with journey["app"].test_client() as reader:
            with reader.session_transaction() as current:
                current.update(authenticated=True, username="reviewer", role="admin")
            response = reader.get("/api" + base(journey))
            return (
                response.status_code,
                response.get_json()["groups"][0]["observation_count"],
            )

    with ThreadPoolExecutor(max_workers=5) as readers:
        assert list(readers.map(read_independently, range(5))) == [(200, 50_001)] * 5

    last = client.get("/api" + base(journey) + "/observations?page=501").get_json()
    assert len(last["observations"]) == 1
    assert not last["has_next"]


def test_workspace_histories_have_independent_complete_pages(journey):
    pipeline = journey["pipeline"]
    initial = pipeline.get_workspace(journey["case_id"], journey["persona_id"])
    for index in range(61):
        pipeline.create_request(
            journey["case_id"],
            journey["persona_id"],
            [{"type": "username", "value": str(index)}],
            {
                "tasks": [
                    {
                        "engine": "fixture",
                        "availability": "excluded",
                        "reason": "Synthetic history fixture",
                    }
                ]
            },
            actor="operator",
            idempotency_key=f"history:{index}",
        )
    data = journey["client"].get("/api" + base(journey)).get_json()
    assert len(data["requests"]) == len(data["tasks"]) == 25
    assert data["requests_count"] == initial["requests_count"] + 61
    assert data["tasks_count"] == initial["tasks_count"] + 61
    assert all("plan" not in item for item in data["requests"])
    seen = set()
    for offset in range(0, data["tasks_count"], 25):
        response = journey["client"].get(
            "/api" + base(journey) + f"/history/tasks?offset={offset}"
        )
        assert response.status_code == 200
        page = response.get_json()
        seen.update(item["id"] for item in page["items"])
    assert len(seen) == data["tasks_count"]


def test_committed_evidence_invalidates_projection_and_failed_rebuild_is_atomic(
    pair, monkeypatch
):
    pipeline = pair[1]
    request = query(pair)
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], "worker:test")

    def document(number):
        return normalize_observation(
            {
                "status": "found",
                "native_record_id": str(number),
                "source_url": "https://example.test/profile",
                "account": {"platform": "fixture", "stable_id": str(number)},
            },
            engine="fixture",
            case_id=request["case_id"],
            subject_id=request["persona_id"],
            request_id=request["id"],
            task_id=request["tasks"][0]["id"],
            attempt_id=attempt["id"],
            observed_at=attempt["created_at"],
        )

    first = document(1)
    pipeline.append_observations(attempt["id"], [first], worker_id="worker:test")
    scope = request["case_id"], request["persona_id"]
    assert pipeline.get_workspace(*scope)["projection"]["pending"]
    assess_consolidated_groups(pipeline, *scope)
    before = pipeline.get_workspace(*scope)
    assert not before["projection"]["pending"]
    pipeline.append_observations(attempt["id"], [first], worker_id="worker:test")
    assert pipeline.get_workspace(*scope)["projection"] == before["projection"]
    pipeline.append_observations(attempt["id"], [document(2)], worker_id="worker:test")
    with pytest.raises(ValueError, match="awaits consolidation"):
        pipeline.create_version(*scope, actor="operator", scope="Review")
    original = pipeline.materialize_groups

    def fail_after_write(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("injected rollback after materialization")

    monkeypatch.setattr(pipeline, "materialize_groups", fail_after_write)
    with pytest.raises(RuntimeError, match="injected rollback"):
        assess_consolidated_groups(pipeline, *scope)
    failed = pipeline.get_workspace(*scope)
    assert failed["projection"]["pending"]
    assert failed["group_count"] == before["group_count"]
    monkeypatch.setattr(pipeline, "materialize_groups", original)
    assess_consolidated_groups(pipeline, *scope)
    recovered = pipeline.get_workspace(*scope)
    assert not recovered["projection"]["pending"]
    assert recovered["group_count"] > before["group_count"]


def test_partial_and_stale_materialization_cannot_acknowledge_new_evidence(pair):
    pipeline = pair[1]
    request = query(pair)
    scope = request["case_id"], request["persona_id"]
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], "worker:test")
    captured = pipeline.projection_revision(*scope)
    pipeline.append_observations(
        attempt["id"],
        [
            {
                "id": "new-observation",
                "status": "found",
                "source_url": "https://example.test/profile",
            }
        ],
        worker_id="worker:test",
    )
    projection = {
        "claims": [
            {
                "id": "test-claim",
                "normalized": {"predicate": "occupation", "value": "Researcher"},
                "observation_ids": ["new-observation"],
            }
        ]
    }
    pipeline.upsert_groups(*scope, projection)
    assert pipeline.get_workspace(*scope)["projection"]["pending"]
    with pytest.raises(ValueError, match="Evidence changed"):
        pipeline.upsert_groups(*scope, projection, projection_revision=captured)
    assert pipeline.get_workspace(*scope)["projection"]["pending"]
    # Trusted fixture has now read the entire current synthetic scope. The HTTP
    # mutation API never accepts a projection revision acknowledgement.
    pipeline.upsert_groups(
        *scope, projection, projection_revision=pipeline.projection_revision(*scope)
    )
    assert not pipeline.get_workspace(*scope)["projection"]["pending"]


def test_prepare_is_explicit_scoped_csrf_post_not_a_get_side_effect(journey):
    from flask import Flask, session
    from maigret.web.pipeline_routes import register_pipeline_routes

    app = Flask(__name__)
    app.config.update(TESTING=True, SECRET_KEY="fixture", AUTH_REQUIRED=True)
    calls = []

    def prepare(**kwargs):
        calls.append(kwargs)
        return {"prepared": True}

    register_pipeline_routes(
        app,
        lambda: journey["store"],
        lambda: session.get("role"),
        lambda value: value == "csrf-fixture",
        prepare_workspace=prepare,
    )
    client = app.test_client()
    with client.session_transaction() as current:
        current.update(authenticated=True, username="operator", role="admin")
    for _ in range(2):
        assert client.get("/api" + base(journey)).status_code == 200
    assert calls == []
    path = base(journey) + "/prepare"
    assert client.get(path).status_code == 405
    assert client.post(path, json={}).status_code == 403
    assert (
        client.post(
            path, json={}, headers={"X-OpenLedger-CSRF": "csrf-fixture"}
        ).status_code
        == 200
    )
    assert calls == [
        {"case_id": journey["case_id"], "persona_id": journey["persona_id"]}
    ]
    foreign = path.replace(journey["case_id"], "wrong-case")
    assert (
        client.post(
            foreign, json={}, headers={"X-OpenLedger-CSRF": "csrf-fixture"}
        ).status_code
        == 404
    )
    assert len(calls) == 1


def test_history_html_and_exact_plan_are_accessible(journey):
    client = journey["client"]
    assert client.get(base(journey) + "/history/tasks").status_code == 200
    query_id = journey["pipeline"].get_workspace(
        journey["case_id"], journey["persona_id"]
    )["requests"][0]["id"]
    result = client.get("/api" + base(journey) + "/requests/" + query_id)
    assert result.status_code == 200
    assert "plan" in result.get_json()


def test_concurrent_append_cannot_be_acknowledged_by_earlier_rebuild(pair, monkeypatch):
    from threading import Event
    import maigret.web.pipeline_assessment_runtime as runtime

    pipeline = pair[1]
    request = query(pair)
    scope = request["case_id"], request["persona_id"]
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], "worker:test")

    def observation(number):
        return normalize_observation(
            {
                "status": "found",
                "native_record_id": str(number),
                "source_url": "https://example.test/profile",
                "account": {"platform": "fixture", "stable_id": str(number)},
            },
            engine="fixture",
            case_id=scope[0],
            subject_id=scope[1],
            request_id=request["id"],
            task_id=request["tasks"][0]["id"],
            attempt_id=attempt["id"],
            observed_at=attempt["created_at"],
        )

    pipeline.append_observations(
        attempt["id"], [observation(1)], worker_id="worker:test"
    )
    entered, release, started = Event(), Event(), Event()
    original = runtime.assess_consolidated

    def pause(*args, **kwargs):
        entered.set()
        assert release.wait(10), "test release was not signalled"
        return original(*args, **kwargs)

    monkeypatch.setattr(runtime, "assess_consolidated", pause)

    def append():
        started.set()
        return pipeline.append_observations(
            attempt["id"], [observation(2)], worker_id="worker:test"
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        rebuild = pool.submit(assess_consolidated_groups, pipeline, *scope)
        assert entered.wait(10)
        writer = pool.submit(append)
        try:
            assert started.wait(10)
            assert not writer.done()
        finally:
            release.set()
        rebuild.result(timeout=10)
        writer.result(timeout=10)
    state = pipeline.get_workspace(*scope)["projection"]
    assert state["projected_revision"] == 1
    assert state["evidence_revision"] == 2
    assert state["pending"]
    monkeypatch.setattr(runtime, "assess_consolidated", original)
    assess_consolidated_groups(pipeline, *scope)
    assert not pipeline.get_workspace(*scope)["projection"]["pending"]


def test_cached_workspace_probability_expires_without_rebuilding(journey, monkeypatch):
    from datetime import datetime, timezone
    from sqlalchemy import update
    pipeline = journey['pipeline']
    table = pipeline._table('group_summaries')
    with pipeline.engine.begin() as connection:
        connection.execute(update(table).where(table.c.group_id == journey['group_id']).values(
            assessment={'probability': {'value': 0.8, 'expires_at': '2026-01-02T00:00:00+00:00'}}))
    monkeypatch.setattr('maigret.web.pipeline_store._now', lambda: datetime(2026, 1, 3, tzinfo=timezone.utc))
    data = journey['client'].get('/api' + base(journey)).get_json()
    probability = data['groups'][0]['assessment']['probability']
    assert probability['value'] is None
    assert probability['reason'] == 'probability_review_expired_or_undated'
