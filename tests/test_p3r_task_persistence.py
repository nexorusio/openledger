# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Exact task identity, stop and recovery invariants in the production store."""

import hashlib
from datetime import timedelta

import pytest
from sqlalchemy import update

from maigret.web.case_store import (
    CaseStore,
    investigation_jobs,
    utcnow,
    WORKER_STALE_AFTER_SECONDS,
)
from maigret.web.collection_accounting import public_collection_accounting
from maigret.web.collection_accounting import interrupted_collection_accounting


@pytest.fixture
def job_store(tmp_path):
    store = CaseStore(f"sqlite:///{tmp_path / 'tasks.db'}", create_schema=True)
    job_id = store.create_investigation(['fixture'], {})
    store.claim_next('worker:fixture')
    yield store, job_id
    store.dispose()


def task(name, attempt=0):
    digest = lambda value: hashlib.sha256(value.encode()).hexdigest()
    return dict(
        schema_version=1,
        task_id=digest(f'{name}:{attempt}'),
        check_id=digest(name),
        source_id=digest('fixture-source'),
        target_id=digest('fixture-target'),
        attempt=attempt,
    )


def append(store, job_id, kind, items):
    return store.append_event(
        job_id,
        dict(type=kind, tasks=items),
        runtime_guard=True,
        worker_id='worker:fixture',
    )


def test_task_identity_survives_rebatching_and_rejects_conflicts(job_store):
    store, job_id = job_store
    first, second = task('first'), task('second')
    planned = append(store, job_id, 'collection_task_plan', [first, second])
    assert append(store, job_id, 'collection_task_plan', [first]) == planned
    with pytest.raises(ValueError, match='identity'):
        append(store, job_id, 'collection_task_plan', [{**first, 'attempt': 1}])
    assert append(store, job_id, 'collection_task_plan', [task('first', 1)]) > planned
    outcome = dict(disposition='error', attempted=True, cleanup_state='not_required')
    done = append(
        store,
        job_id,
        'collection_task_terminal',
        [{**first, **outcome}, {**second, **outcome}],
    )
    assert (
        append(store, job_id, 'collection_task_terminal', [{**first, **outcome}])
        == done
    )
    with pytest.raises(ValueError, match='terminal'):
        append(
            store,
            job_id,
            'collection_task_terminal',
            [{**first, **outcome, 'disposition': 'completed'}],
        )
    with pytest.raises(ValueError, match='persisted plan'):
        append(
            store,
            job_id,
            'collection_task_terminal',
            [{**task('unplanned'), **outcome}],
        )


def test_logical_attempt_cannot_be_duplicated_with_another_task_id(job_store):
    store, job_id = job_store
    first = task('first')
    append(store, job_id, 'collection_task_plan', [first])
    with pytest.raises(ValueError, match='logical collection attempt'):
        append(
            store,
            job_id,
            'collection_task_plan',
            [{**first, 'task_id': task('other')['task_id']}],
        )
    with pytest.raises(ValueError, match='check identity'):
        append(
            store,
            job_id,
            'collection_task_plan',
            [{**task('first', 1), 'source_id': task('other')['check_id']}],
        )


def test_cleanup_requires_pending_terminal_and_cannot_regress(job_store):
    store, job_id = job_store
    first = task('first')
    append(store, job_id, 'collection_task_plan', [first])
    cleanup = {**first, 'event_type': 'cleanup', 'cleanup_state': 'complete'}
    with pytest.raises(ValueError, match='pending terminal task'):
        append(store, job_id, 'collection_task_cleanup', [cleanup])
    append(
        store,
        job_id,
        'collection_task_terminal',
        [
            {
                **first,
                'disposition': 'timeout',
                'attempted': True,
                'cleanup_state': 'pending',
            }
        ],
    )
    append(
        store,
        job_id,
        'collection_task_cleanup',
        [{**cleanup, 'cleanup_state': 'incomplete'}],
    )
    complete = append(store, job_id, 'collection_task_cleanup', [cleanup])
    assert append(store, job_id, 'collection_task_cleanup', [cleanup]) == complete
    with pytest.raises(ValueError, match='terminal disposition'):
        append(
            store,
            job_id,
            'collection_task_cleanup',
            [{**cleanup, 'cleanup_state': 'incomplete'}],
        )


def test_real_stream_notifier_flushes_short_terminal_batch_before_cleanup(job_store):
    from maigret.web.app import StreamNotify

    store, job_id = job_store

    class Sink:
        def put(self, event):
            return store.append_event(
                job_id, event, runtime_guard=True, worker_id='worker:fixture'
            )

    notify = StreamNotify(Sink(), 'fixture')
    first = task('first')
    notify.task_plan([first])
    notify.task_terminal(
        {
            **first,
            'disposition': 'timeout',
            'attempted': True,
            'cleanup_state': 'pending',
        }
    )
    notify.task_cleanup_event(
        {**first, 'event_type': 'cleanup', 'cleanup_state': 'complete'}
    )
    notify.close()
    kinds = [row['event']['type'] for row in store.get_events(job_id)]
    assert kinds[-3:] == [
        'collection_task_plan',
        'collection_task_terminal',
        'collection_task_cleanup',
    ]


@pytest.mark.parametrize(
    'extra',
    [
        {
            'google_places_search': {
                'name': 'Transient name',
                'formatted_address': 'Transient details',
            }
        },
        {'provider_response': {'payload': 'Unreviewed response'}},
        {
            'collector_observations': [
                {'source_engine': 'google_places_search', 'name': 'Transient name'}
            ]
        },
    ],
)
def test_profile_checkpoint_rejects_other_source_payloads_without_writing(
    job_store, extra
):
    store, job_id = job_store
    checkpoint = {'status': 'completed', 'collector_observations': [], **extra}
    with pytest.raises(ValueError, match='unsupported'):
        store.save_collection_checkpoint(job_id, checkpoint, worker_id='worker:fixture')
    assert store.get_collection_checkpoint(job_id) == {}


@pytest.mark.parametrize(
    'observation',
    [
        {
            'source_engine': 'github_public_profile',
            'provider_response': {'authorization': 'Bearer retained-secret'},
        },
        {
            'source_engine': 'github_public_profile',
            'extra': {'name': {'authorization': 'Bearer retained-secret'}},
        },
        {
            'source_engine': 'user_scanner_username',
            'extra': {'authorization': 'Bearer retained-secret'},
        },
        {
            'source_engine': 'user_scanner_email',
            'extra': {'response_body': 'raw payload'},
        },
        {'source_engine': 'user_scanner_email', 'extra': {'description': 'x' * 2001}},
        {
            'source_engine': 'github_public_profile',
            'media': {'avatar': 'https://example.org/image?access_token=secret'},
        },
    ],
)
def test_checkpoint_rejects_nested_credentials_raw_payloads_and_unbounded_values(
    job_store, observation
):
    store, job_id = job_store
    with pytest.raises(ValueError, match='unsupported'):
        store.save_collection_checkpoint(
            job_id,
            {'collector_observations': [observation]},
            worker_id='worker:fixture',
        )
    assert store.get_collection_checkpoint(job_id) == {}


@pytest.mark.parametrize(
    'key', ['provider_payload', 'raw_payload', 'response_payload', 'payload', 'body']
)
def test_checkpoint_rejects_raw_maigret_evidence_fields(job_store, key):
    store, job_id = job_store
    result = {
        'individual_reports': [{'claimed_profiles': [{'evidence': {key: 'x' * 10000}}]}]
    }
    with pytest.raises(ValueError, match='provider-response'):
        store.save_collection_checkpoint(job_id, result, worker_id='worker:fixture')
    assert store.get_collection_checkpoint(job_id) == {}


@pytest.mark.parametrize(
    'status,disposition', [('failed', 'error'), ('timed_out', 'timeout')]
)
def test_stale_recovery_preserves_completed_source_failure(status, disposition):
    first = task('finished')
    row = dict(
        stage_id='maigret',
        engine_id='maigret',
        unit='site_checks',
        status=status,
        reason='source_error',
        planned=1,
        started=1,
        terminal=1,
        completed=0,
        errors=int(disposition == 'error'),
        timeouts=int(disposition == 'timeout'),
        cancelled=0,
        interrupted=0,
        unattempted=0,
        unknown=0,
        observations=0,
        cleanup_complete=True,
    )
    snapshot = dict(
        schema_version=1, revision=2, state='running', known=True, stages=[row]
    )
    events = [
        dict(type='collection_task_plan', tasks=[first]),
        dict(
            type='collection_task_terminal',
            tasks=[
                dict(
                    first,
                    disposition=disposition,
                    attempted=True,
                    cleanup_state='not_required',
                )
            ],
        ),
    ]
    recovered = interrupted_collection_accounting(snapshot, events)
    assert recovered['state'] == 'interrupted'
    final = recovered['stages'][0]
    assert final['status'] == status
    assert final['reason'] == 'source_error'
    assert final['terminal'] == 1
    assert final['unknown'] == 0
    assert final['cleanup_complete'] is True


@pytest.mark.parametrize(
    'disposition,attempted,cleanup',
    [
        ('unattempted', True, 'not_required'),
        ('completed', False, 'not_required'),
        ('unattempted', False, 'pending'),
    ],
)
def test_invalid_admission_cannot_enter_the_ledger(
    job_store, disposition, attempted, cleanup
):
    store, job_id = job_store
    item = task('invalid')
    append(store, job_id, 'collection_task_plan', [item])
    with pytest.raises(ValueError):
        append(
            store,
            job_id,
            'collection_task_terminal',
            [
                {
                    **item,
                    'disposition': disposition,
                    'attempted': attempted,
                    'cleanup_state': cleanup,
                }
            ],
        )


def test_stop_committed_before_finish_cannot_publish_normal_completion(job_store):
    store, job_id = job_store
    assert store.request_cancel(job_id)
    called = []
    assert not store.finish(
        job_id,
        {'status': 'completed'},
        worker_id='worker:fixture',
        synchronize_claims=True,
        publication=lambda: called.append(True),
        terminal_event={'type': 'done', 'status': 'completed'},
    )
    assert called == []
    assert store.get_job(job_id)['status'] == 'cancel_requested'


def test_stale_unreturned_task_is_unknown_not_absent_or_unattempted(job_store):
    store, job_id = job_store
    first, second = task('first'), task('second')
    append(store, job_id, 'collection_task_plan', [first, second])
    append(
        store,
        job_id,
        'collection_task_terminal',
        [
            {
                **first,
                'disposition': 'completed',
                'attempted': True,
                'cleanup_state': 'not_required',
            }
        ],
    )
    snapshot = dict(
        schema_version=1,
        revision=1,
        state='running',
        known=False,
        stages=[
            dict(
                stage_id='maigret',
                engine_id='maigret',
                unit='site_checks',
                status='running',
                reason=None,
                planned=2,
                started=None,
                terminal=1,
                completed=1,
                errors=0,
                timeouts=0,
                cancelled=0,
                interrupted=None,
                unattempted=None,
                unknown=None,
                observations=1,
            ),
        ],
    )
    store.append_event(
        job_id,
        {'type': 'collection_accounting', 'collection_accounting': snapshot},
        runtime_guard=True,
        worker_id='worker:fixture',
    )
    with store.engine.begin() as connection:
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job_id)
            .values(
                heartbeat_at=utcnow()
                - timedelta(seconds=WORKER_STALE_AFTER_SECONDS + 1)
            )
        )
    assert store.mark_stale_running() == 1
    recovered = store.get_job(job_id)['collection_accounting']
    assert public_collection_accounting(recovered) is not None
    row = recovered['stages'][0]
    assert (row['planned'], row['completed'], row['unknown'], row['unattempted']) == (
        2,
        1,
        1,
        0,
    )
    assert row['started'] is None
    assert row['cleanup_complete'] is False
    assert recovered['state'] == 'interrupted'
    assert store.mark_stale_running() == 0
    assert (
        sum(event['event']['type'] == 'done' for event in store.get_events(job_id)) == 1
    )
