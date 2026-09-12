"""Production-shaped query/attempt integration with synthetic offline adapters."""

import asyncio
import importlib
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from maigret.web.case_store import CaseStore
from maigret.web.pipeline_contract import ENGINE_REGISTRY
from maigret.web.pipeline_execution import execute_pipeline_job
from maigret.web.pipeline_store import PipelineStore


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    store = CaseStore(f"sqlite:///{tmp_path}/execution.db", create_schema=True)
    app = importlib.import_module("maigret.web.app")
    monkeypatch.setattr(app, "case_store", store)
    yield store, PipelineStore(store), app
    store.dispose()


def enqueue(store, identifiers):
    from maigret.web.pipeline_execution import source_configuration

    usernames = [item["value"] for item in identifiers if item["type"] == "username"]
    job_id = store.create_investigation(
        usernames,
        {
            "pipeline_source_status": source_configuration(),
            "investigation_spec": {
                "processing_mode": "same_subject",
                "subject_label": "Synthetic Subject",
                "identifiers": identifiers,
                "search_targets": [
                    {"value": value, "source_type": "username"} for value in usernames
                ],
            },
        },
    )
    job = store.claim_next("worker:pipeline-integration")
    assert job["job_id"] == job_id
    return job


def sources(monkeypatch, enabled):
    import maigret.web.pipeline_execution as execution

    status = dict(
        discovery_enabled=True,
        maigret_enabled=True,
        scanner_enabled=True,
        scanner_available=True,
        enrichment_enabled=True,
        native_search={"enabled": True},
        public_search={"enabled": True},
        engines={
            key: {"enabled": key in enabled, "reason": "Synthetic source availability"}
            for key in ENGINE_REGISTRY
        },
    )
    monkeypatch.setattr(execution, "source_configuration", lambda: status)


def test_sync_pipeline_runner_is_safe_inside_an_existing_event_loop():
    """The Playwright sync API can invoke the worker while its loop is active."""
    from maigret.web.pipeline_execution import _run_coroutine_sync

    async def nested_call():
        async def value():
            await asyncio.sleep(0)
            return "completed"

        return _run_coroutine_sync(lambda: value())

    assert asyncio.run(nested_call()) == "completed"


async def found(task, context):
    context.emit_observations(
        [
            {
                "source_engine": task["engine_id"],
                "source_record_id": "same-account",
                "status": "found",
                "source_url": "https://github.com/synthetic-person",
                "account": {
                    "platform": "github",
                    "canonical_url": "https://github.com/synthetic-person",
                },
                "claims": [{"predicate": "occupation", "value": "Researcher"}],
            }
        ]
    )
    return {"outcome": "found"}


def test_real_query_and_attempts_reach_grouped_review_without_automatic_final(
    runtime, monkeypatch
):
    store, pipeline, app = runtime
    sources(monkeypatch, {"maigret"})
    monkeypatch.setattr(
        app, "_stream_search", lambda *a, **k: pytest.fail("Old pipeline executed")
    )
    job = enqueue(store, [{"type": "username", "value": "synthetic-person"}])
    result = execute_pipeline_job(store, job, adapters={"maigret_search": found})
    assert result["pipeline_id"] == "p2-e2e-v1"
    assert result["status"] == "completed"
    request = pipeline.requests_for_job(job["job_id"])[0]
    workspace = pipeline.get_workspace(job["case_id"], request["persona_id"])
    assert workspace["groups"]
    assert pipeline.get_final_version(job["case_id"], request["persona_id"]) is None
    assert (
        len(list(pipeline.iter_observations(job["case_id"], request["persona_id"])))
        >= 2
    )


@pytest.mark.parametrize(
    "kind,value",
    [
        ("full_name", "Synthetic Person"),
        ("email", "synthetic@example.test"),
        ("phone", "+12025550124"),
    ],
)
def test_nonusername_requests_have_compatible_tasks(runtime, monkeypatch, kind, value):
    store, pipeline, _ = runtime
    sources(monkeypatch, {"public_exact_match"})
    job = enqueue(store, [{"type": kind, "value": value}])
    result = execute_pipeline_job(store, job, adapters={"public_exact_match": found})
    assert result["status"] == "completed"
    request = pipeline.requests_for_job(job["job_id"])[0]
    assert request["inputs"][0]["type"] == kind
    assert any(
        task["engine"] == "public_exact_match" and task["outcome"] == "found"
        for task in request["tasks"]
    )


def test_no_compatible_active_source_preserves_research_case(runtime, monkeypatch):
    store, pipeline, _ = runtime
    sources(monkeypatch, set())
    job = enqueue(store, [{"type": "full_name", "value": "Synthetic Person"}])
    result = execute_pipeline_job(store, job, adapters={})
    assert result["collection_status"] == "research_needed"
    assert pipeline.requests_for_job(job["job_id"])[0]["status"] == "research_needed"
    assert store.get_case(job["case_id"]) is not None


def test_failed_engine_retries_keep_partial_evidence(runtime, monkeypatch):
    store, pipeline, _ = runtime
    sources(monkeypatch, {"maigret"})
    job = enqueue(store, [{"type": "username", "value": "synthetic-person"}])
    calls = []

    async def flaky(task, context):
        calls.append(context.attempt["id"])
        await found(task, context)
        if len(calls) == 1:
            raise TimeoutError("synthetic timeout after partial evidence")
        return {"outcome": "found"}

    result = execute_pipeline_job(store, job, adapters={"maigret_search": flaky})
    assert result["status"] == "completed"
    assert len(calls) == 2
    request = pipeline.requests_for_job(job["job_id"])[0]
    observations = list(
        pipeline.iter_observations(job["case_id"], request["persona_id"])
    )
    assert set(calls) <= {item["attempt_id"] for item in observations}


def test_operator_cancel_stops_queue_preserves_committed_evidence(runtime, monkeypatch):
    store, pipeline, _ = runtime
    sources(monkeypatch, {"maigret"})
    job = enqueue(store, [{"type": "username", "value": "synthetic-person"}])

    async def cancelling(task, context):
        await found(task, context)
        store.request_cancel(job["job_id"])
        await asyncio.sleep(0.5)
        return {"outcome": "found"}

    result = execute_pipeline_job(store, job, adapters={"maigret_search": cancelling})
    assert result["status"] == "cancelled"
    request = pipeline.requests_for_job(job["job_id"])[0]
    assert request["status"] == "cancelled"
    assert list(pipeline.iter_observations(job["case_id"], request["persona_id"]))


def test_native_negative_results_are_not_reported_as_findings(runtime, monkeypatch):
    from maigret.result import MaigretCheckResult, MaigretCheckStatus
    from maigret.web.pipeline_execution import _maigret_adapter

    store, pipeline, app = runtime
    sources(monkeypatch, {'maigret'})

    async def absent(username, options, query_notify):
        return {
            'Synthetic Site': {
                'status': MaigretCheckResult(
                    username,
                    'Synthetic Site',
                    'https://example.test/' + username,
                    MaigretCheckStatus.AVAILABLE,
                ),
                'url_user': 'https://example.test/' + username,
            }
        }

    monkeypatch.setattr(app, 'maigret_search', absent)
    job = enqueue(store, [{'type': 'username', 'value': 'synthetic-person'}])
    result = execute_pipeline_job(
        store, job, adapters={'maigret_search': _maigret_adapter}
    )
    assert result['status'] == 'completed'
    assert result['outcome_counts'].get('found', 0) == 0
    assert result['outcome_counts']['not_found'] == 1


def test_new_intake_persists_query_before_worker_and_rolls_back_invalid_plan(runtime):
    store, pipeline, _ = runtime
    job_id = store.create_investigation(['synthetic'], {})
    requests = pipeline.requests_for_job(job_id)
    assert len(requests) == 1 and requests[0]['status'] == 'planned'
    assert requests[0]['plan']['pipeline_id'] == 'p2-e2e-v1'
    assert all(task['attempt_count'] == 0 for task in requests[0]['tasks'])
    count = len(store.list_cases())
    with pytest.raises(ValueError):
        store.create_investigation(
            [],
            {
                'investigation_spec': {
                    'processing_mode': 'same_subject',
                    'identifiers': [{'type': 'unsupported', 'value': 'synthetic'}],
                }
            },
        )
    assert len(store.list_cases()) == count


def test_queued_search_cannot_switch_provider_silently(runtime, monkeypatch):
    from maigret.web.pipeline_execution import source_configuration
    from maigret.web.pipeline_query import build_query_plan, revalidate_query_plan

    monkeypatch.setenv('OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED', 'true')
    monkeypatch.setenv('OPENLEDGER_PROFILE_SEARCH_PROVIDER', 'brave')
    specification = {
        'identifiers': [{'type': 'email', 'value': 'synthetic@example.test'}]
    }
    first = build_query_plan(specification, source_status=source_configuration())
    monkeypatch.setenv('OPENLEDGER_PROFILE_SEARCH_PROVIDER', 'searxng')
    current = revalidate_query_plan(
        first, specification, source_status=source_configuration()
    )
    assert current['changed']
    old = next(
        item
        for item in first['tasks']
        if item['engine_id'] == 'public_exact_match' and item['route_state'] == 'active'
    )
    new = next(
        item for item in current['plan']['tasks'] if item['task_id'] == old['task_id']
    )
    assert old['source_config_revision'] != new['source_config_revision']


def test_existing_combined_case_operation_uses_new_attempt_and_publishes_once(
    runtime, monkeypatch
):
    import uuid
    from maigret.web.pipeline_case_fusion import collect_case_fusion_snapshot

    store, pipeline, app = runtime
    sources(monkeypatch, {'case_fusion_snapshot'})
    source_ids = []
    for name in ('synthetic-left', 'synthetic-right'):
        job_id = str(uuid.uuid4())
        store.import_legacy_result(
            job_id,
            {
                'status': 'completed',
                'usernames': [name],
                'session_folder': 'search_' + job_id,
                'individual_reports': [],
            },
        )
        source_ids.append(store.get_job(job_id)['case_id'])
    job_id = store.create_combined_investigation(
        source_ids,
        title='Synthetic relationship research',
        purpose='Compare retained public evidence',
        created_by='operator',
    )
    job = store.claim_next('worker:pipeline-integration')
    assert job['job_id'] == job_id
    monkeypatch.setattr(
        app,
        'run_persistent_case_fusion_job',
        lambda *a, **k: pytest.fail('Old combined pipeline executed'),
    )
    result = execute_pipeline_job(
        store, job, adapters={'case_fusion_snapshot': collect_case_fusion_snapshot}
    )
    assert result['pipeline_id'] == 'p2-e2e-v1' and result['kind'] == 'case_fusion'
    saved = store.get_job(job_id)
    assert (
        saved['status'] == 'completed'
        and saved['snapshot']['sha256'] == result['snapshot']['sha256']
    )
    requests = pipeline.requests_for_job(job_id)
    assert len(requests) == 1
    assert any(
        task['engine'] == 'case_fusion_snapshot' and task['outcome'] == 'candidate'
        for task in requests[0]['tasks']
    )
    assert pipeline.get_final_version(job['case_id'], requests[0]['persona_id']) is None


def test_mixed_batch_partial_never_becomes_complete(runtime, monkeypatch):
    from maigret.web.pipeline_execution import _aggregate_outcomes

    store, pipeline, _ = runtime
    sources(monkeypatch, {'maigret'})
    calls = []

    async def mixed(task, context):
        calls.append(context.attempt['id'])
        await found(task, context)
        context.emit_observations([{
            'source_engine': 'maigret', 'source_record_id': 'unanswered',
            'source_url': 'https://example.test/unanswered', 'status': 'timeout',
        }])
        return {'outcome': _aggregate_outcomes(['found', 'timeout'])}

    job = enqueue(store, [{'type': 'username', 'value': 'synthetic-person'}])
    result = execute_pipeline_job(store, job, adapters={'maigret_search': mixed})
    request = pipeline.requests_for_job(job['job_id'])[0]
    assert result['collection_status'] == request['status'] == 'partial'
    assert result['outcome_counts']['partial'] == 1
    assert len(calls) == 1  # partial does not invent safe whole-batch replay
    assert {'found', 'timeout'} <= {
        item['status'] for item in pipeline.iter_observations(job['case_id'], request['persona_id'])
    }


def test_independent_subject_without_route_stays_open(runtime, monkeypatch):
    from maigret.web.pipeline_execution import source_configuration
    from maigret.web.investigation_input import build_investigation_plan

    store, pipeline, _ = runtime
    sources(monkeypatch, {'maigret'})
    job_id = store.create_investigation(['synthetic-person'], {
        'pipeline_source_status': source_configuration(),
        'investigation_spec': build_investigation_plan({
            'processing_mode': 'independent',
            'identifier_type': ['username', 'email'],
            'identifier_value': ['synthetic-person', 'unanswered@example.test'],
        }),
    })
    job = store.claim_next('worker:pipeline-integration')
    result = execute_pipeline_job(store, job, adapters={'maigret_search': found})
    requests = pipeline.requests_for_job(job_id)
    assert len(requests) == 2
    statuses = {request['inputs'][0]['type']: request['status'] for request in requests}
    assert statuses == {'username': 'completed', 'email': 'research_needed'}
    assert result['collection_status'] == 'partial'


@pytest.mark.parametrize('outcome,metadata', [
    ('blocked', {'retryable': True}),
    ('error', {'retryable': False}),
    ('timeout', {'retry_after_seconds': 120}),
])
def test_terminal_or_deferred_failure_is_not_immediately_retried(runtime, monkeypatch, outcome, metadata):
    store, pipeline, _ = runtime
    sources(monkeypatch, {'maigret'})
    calls = []

    async def unavailable(task, context):
        calls.append(context.attempt['id'])
        return {'outcome': outcome, **metadata}

    job = enqueue(store, [{'type': 'username', 'value': 'synthetic-person'}])
    execute_pipeline_job(store, job, adapters={'maigret_search': unavailable})
    request = pipeline.requests_for_job(job['job_id'])[0]
    task = next(task for task in request['tasks'] if task['availability'] == 'active')
    assert len(calls) == 1
    state = task['spec']['_execution']
    if outcome == 'timeout':
        assert state['next_retry_at'] and state['retryable']
    else:
        assert not state['retryable']


def test_crash_recovery_is_atomic_fenced_and_resumes_same_request(runtime, monkeypatch):
    from maigret.web.pipeline_store import PipelineStore

    store, pipeline, _ = runtime
    sources(monkeypatch, {'maigret'})
    job = enqueue(store, [{'type': 'username', 'value': 'synthetic-person'}])
    request = pipeline.requests_for_job(job['job_id'])[0]
    task = next(task for task in request['tasks'] if task['availability'] == 'active')
    attempt = pipeline.start_attempt(task['id'], job['worker_id'])
    pipeline.append_observations(attempt['id'], [{
        'id': 'before-crash', 'status': 'found', 'source_url': 'https://example.test/before-crash',
    }], worker_id=job['worker_id'])
    original = PipelineStore.reconcile_interrupted_job

    def fail_after_reconciliation(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise RuntimeError('synthetic transaction failure')

    monkeypatch.setattr(PipelineStore, 'reconcile_interrupted_job', fail_after_reconciliation)
    with pytest.raises(RuntimeError, match='transaction failure'):
        store.mark_stale_running(0)
    assert store.get_job(job['job_id'])['status'] == 'running'
    assert pipeline.get_attempt(attempt['id'])['status'] == 'running'
    monkeypatch.setattr(PipelineStore, 'reconcile_interrupted_job', original)
    assert store.mark_stale_running(0) == 1
    assert pipeline.get_request(request['id'])['status'] == 'interrupted'
    assert pipeline.get_task(task['id'])['active_attempt_id'] is None
    assert pipeline.get_attempt(attempt['id'])['status'] == 'completed'
    with pytest.raises(ValueError, match='Stale|Completed attempt'):
        pipeline.append_observations(attempt['id'], [{
            'id': 'late', 'status': 'found', 'source_url': 'https://example.test/late',
        }], worker_id=job['worker_id'])
    resumed = pipeline.resume_interrupted_request(
        request['id'], actor='analyst', case_id=job['case_id'], persona_id=request['persona_id'],
    )
    assert resumed['job_id'] == job['job_id']
    replacement = store.claim_next('worker:replacement')
    assert replacement['job_id'] == job['job_id']
    assert replacement['deadline_at'] == job['deadline_at']
    result = execute_pipeline_job(store, replacement, adapters={'maigret_search': found})
    assert result['status'] == 'completed'
    restored = pipeline.get_request(request['id'])
    assert pipeline.get_task(task['id'])['attempt_count'] == 2
    observations = list(pipeline.iter_observations(job['case_id'], request['persona_id']))
    assert any(item['id'] == 'before-crash' for item in observations)
    assert all(item['id'] != 'late' for item in observations)


def test_cancelled_crash_cannot_resume(runtime, monkeypatch):
    store, pipeline, _ = runtime
    sources(monkeypatch, {'maigret'})
    job = enqueue(store, [{'type': 'username', 'value': 'synthetic-person'}])
    request = pipeline.requests_for_job(job['job_id'])[0]
    task = next(task for task in request['tasks'] if task['availability'] == 'active')
    attempt = pipeline.start_attempt(task['id'], job['worker_id'])
    store.request_cancel(job['job_id'])
    store.mark_stale_running(0)
    assert pipeline.get_request(request['id'])['status'] == 'cancelled'
    assert pipeline.get_attempt(attempt['id'])['outcome'] == 'cancelled'
    with pytest.raises(ValueError, match='uncancelled'):
        pipeline.resume_interrupted_request(
            request['id'], actor='analyst', case_id=job['case_id'], persona_id=request['persona_id'],
        )


def test_resume_does_not_replay_completed_tasks_or_renew_time_budget(runtime, monkeypatch):
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import update
    from maigret.web.case_store import investigation_jobs

    store, pipeline, _ = runtime
    sources(monkeypatch, {'maigret'})
    job = enqueue(store, [{'type': 'username', 'value': 'synthetic-one'},
                          {'type': 'username', 'value': 'synthetic-two'}])
    request = pipeline.requests_for_job(job['job_id'])[0]
    tasks = [task for task in request['tasks'] if task['availability'] == 'active']
    assert len(tasks) == 2
    completed = pipeline.start_attempt(tasks[0]['id'], job['worker_id'])
    pipeline.finish_attempt(completed['id'], 'found', worker_id=job['worker_id'])
    abandoned = pipeline.start_attempt(tasks[1]['id'], job['worker_id'])
    store.mark_stale_running(0)
    assert pipeline.get_attempt(completed['id'])['outcome'] == 'found'
    assert pipeline.get_attempt(abandoned['id'])['outcome'] == 'error'
    with store.engine.begin() as connection:
        connection.execute(update(investigation_jobs).where(investigation_jobs.c.id == job['job_id']).values(
            deadline_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        ))
    with pytest.raises(ValueError, match='time budget expired'):
        pipeline.resume_interrupted_request(
            request['id'], actor='analyst', case_id=job['case_id'], persona_id=request['persona_id'],
        )
    assert store.get_job(job['job_id'])['status'] == 'interrupted'
    assert pipeline.get_task(tasks[0]['id'])['attempt_count'] == 1
