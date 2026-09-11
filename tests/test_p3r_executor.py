"""P3R executor accounting and bounded-cleanup regressions."""

import asyncio
import logging
import time

import pytest

from maigret.executors import (
    AsyncioQueueGeneratorExecutor,
    ExecutorAccountingError,
    ExecutorTaskMetadataError,
)


logger = logging.getLogger(__name__)


def task_metadata(index):
    return {
        'task_id': f'opaque-task-{index}',
        'source_id': 'opaque-source',
        'target_id': 'opaque-target',
        'attempt': 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(('planned', 'failures'), [(2645, 245), (50000, 5000)])
async def test_exceptions_receive_default_and_exactly_one_terminal_event(planned, failures):
    """A source exception must be an outcome, never a missing result."""
    events = []

    async def check(index, **_kwargs):
        await asyncio.sleep(0)
        if index < failures:
            raise RuntimeError('synthetic source exception')
        return {'task_id': index, 'outcome': 'observed'}

    work = [
        (
            check,
            [index],
            {
                'default': {'task_id': index, 'outcome': 'source_error'},
                '_executor_task': task_metadata(index),
            },
        )
        for index in range(planned)
    ]
    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=32, timeout=1, task_notify=events.append
    )
    results = [value async for value in executor.run(work)]

    assert {result['task_id'] for result in results} == set(range(planned))
    assert len(results) == planned
    assert len(events) == planned
    assert {event['task_id'] for event in events} == {
        f'opaque-task-{index}' for index in range(planned)
    }
    assert sum(event['disposition'] == 'error' for event in events) == failures
    assert sum(event['disposition'] == 'completed' for event in events) == planned - failures
    assert all(event['schema_version'] == 1 for event in events)
    assert all('result' not in event and 'error' not in event for event in events)


@pytest.mark.asyncio
async def test_cooperative_cleanup_releases_capacity_and_resumes_admission():
    """Timed-out children that finish cleanup must not strand pending work."""
    events = []
    state = {'active': 0, 'peak': 0}
    planned, parallel, timeout, cleanup = 12, 3, 0.001, 0.01

    async def check(index, **_kwargs):
        state['active'] += 1
        state['peak'] = max(state['peak'], state['active'])
        try:
            if index < parallel:
                await asyncio.sleep(60)
            return index
        finally:
            if index < parallel:
                await asyncio.sleep(cleanup)
            state['active'] -= 1

    work = [
        (check, [index], {'default': index, '_executor_task': task_metadata(index)})
        for index in range(planned)
    ]
    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=parallel, timeout=timeout,
        max_outstanding=parallel, cleanup_timeout=0.1, task_notify=events.append,
    )
    results = [value async for value in executor.run(work)]
    terminal_events = [event for event in events if event.get('event_type') != 'cleanup']

    assert sorted(results) == list(range(planned))
    assert state['peak'] <= parallel
    assert state['active'] == 0
    assert executor.cleanup_incomplete is False
    assert [event['task_id'] for event in terminal_events] == [
        f'opaque-task-{index}' for index in range(planned)
    ]
    assert sum(event['disposition'] == 'timeout' for event in terminal_events) == parallel
    assert sum(event['disposition'] == 'completed' for event in terminal_events) == planned - parallel
    assert not any(event['disposition'] == 'unattempted' for event in terminal_events)
    cleanup_events = [event for event in events if event.get('event_type') == 'cleanup']
    assert len(cleanup_events) == parallel
    assert {event['cleanup_state'] for event in cleanup_events} == {'complete'}


@pytest.mark.asyncio
async def test_resistant_cleanup_stops_admission_after_one_bounded_wait():
    """A live cancellation-resistant source continues to occupy its slot."""
    events = []
    release = asyncio.Event()
    started = asyncio.Event()
    planned, parallel = 16, 2

    async def resistant(index, **_kwargs):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            await release.wait()
        return index

    work = [
        (resistant, [index], {'default': index, '_executor_task': task_metadata(index)})
        for index in range(planned)
    ]
    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=parallel, timeout=0.001,
        max_outstanding=parallel, cleanup_timeout=0.02, task_notify=events.append,
    )
    start = time.monotonic()
    results = [value async for value in executor.run(work)]
    elapsed = time.monotonic() - start
    terminal_events = [event for event in events if event.get('event_type') != 'cleanup']

    assert started.is_set()
    assert elapsed < 0.15
    assert results == list(range(planned))
    assert executor.pending_cleanup_count == parallel
    assert executor.cleanup_incomplete is True
    assert sum(event['disposition'] == 'timeout' for event in terminal_events) == parallel
    assert sum(event['disposition'] == 'unattempted' for event in terminal_events) == planned - parallel
    assert all(
        event.get('reason') == 'cleanup_incomplete'
        for event in terminal_events
        if event['disposition'] == 'unattempted'
    )
    assert len({event['task_id'] for event in terminal_events}) == planned

    release.set()
    assert await executor.drain_cleanup(0.1) is True


@pytest.mark.asyncio
async def test_mixed_cleanup_keeps_live_slots_occupied_but_uses_released_capacity():
    """One resistant child cannot block work once another child cleans up."""
    events = []
    release = asyncio.Event()
    cleanup_started = asyncio.Event()
    active = {'count': 0, 'peak': 0}

    async def check(index, **_kwargs):
        active['count'] += 1
        active['peak'] = max(active['peak'], active['count'])
        try:
            if index in (0, 1):
                await asyncio.sleep(60)
            return index
        except asyncio.CancelledError:
            if index == 0:
                cleanup_started.set()
                await asyncio.sleep(0.01)
                return index
            if index == 1:
                cleanup_started.set()
                await release.wait()
                return index
            raise
        finally:
            active['count'] -= 1

    work = [
        (check, [index], {'default': index, '_executor_task': task_metadata(index)})
        for index in range(8)
    ]
    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=2, timeout=0.001,
        max_outstanding=2, cleanup_timeout=0.05, task_notify=events.append,
    )
    results = [value async for value in executor.run(work)]
    terminal_events = [event for event in events if event.get('event_type') != 'cleanup']

    assert cleanup_started.is_set()
    assert active['peak'] <= 2
    assert results == list(range(8))
    assert sum(event['disposition'] == 'timeout' for event in terminal_events) == 2
    assert sum(event['disposition'] == 'completed' for event in terminal_events) == 6
    assert not any(event['disposition'] == 'unattempted' for event in terminal_events)
    assert executor.pending_cleanup_count == 1

    release.set()
    assert await executor.drain_cleanup(0.1) is True


@pytest.mark.asyncio
async def test_production_shaped_plan_reconciles_after_cooperative_cleanup():
    """A 15,870-task plan retains one terminal disposition for every task."""
    events = []
    planned, parallel = 15870, 4

    async def check(index, **_kwargs):
        if index < parallel:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                await asyncio.sleep(0.001)
                return index
        return index

    work = [
        (check, [index], {'default': index, '_executor_task': task_metadata(index)})
        for index in range(planned)
    ]
    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=parallel, timeout=0.001,
        max_outstanding=parallel, cleanup_timeout=0.1, task_notify=events.append,
    )
    results = [value async for value in executor.run(work)]
    terminal_events = [event for event in events if event.get('event_type') != 'cleanup']

    assert len(results) == planned
    assert set(results) == set(range(planned))
    assert len(terminal_events) == planned
    assert len({event['task_id'] for event in terminal_events}) == planned
    assert sum(event['disposition'] == 'timeout' for event in terminal_events) == parallel
    assert sum(event['disposition'] == 'completed' for event in terminal_events) == planned - parallel
    assert not any(event['disposition'] == 'unattempted' for event in terminal_events)


@pytest.mark.asyncio
async def test_cancellation_propagates_even_when_query_suppresses_cancellation():
    events = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def cancellation_resistant(**_kwargs):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            await release.wait()

    work = [
        (
            cancellation_resistant,
            [],
            {'default': index, '_executor_task': task_metadata(index)},
        )
        for index in range(3)
    ]
    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=1, timeout=30, task_notify=events.append
    )

    async def consume():
        return [value async for value in executor.run(work)]

    consumer = asyncio.create_task(consume())
    await started.wait()
    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(consumer, timeout=0.2)

    assert any(event['disposition'] == 'cancelled' for event in events)
    assert sum(event['disposition'] == 'unattempted' for event in events) == 2
    release.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize('healthy_count', (1, 3))
async def test_self_cancelled_source_does_not_cancel_healthy_siblings(healthy_count):
    """A source-level cancellation is one unknown outcome, never a job stop."""
    events = []

    async def self_cancel(**_kwargs):
        raise asyncio.CancelledError()

    async def healthy(index, **_kwargs):
        await asyncio.sleep(0)
        return f'healthy-{index}'

    work = [
        (
            self_cancel,
            [],
            {
                'default': 'source-cancelled',
                '_executor_task': task_metadata(0),
            },
        )
    ]
    work.extend(
        (
            healthy,
            [index],
            {
                'default': f'fallback-{index}',
                '_executor_task': task_metadata(index + 1),
            },
        )
        for index in range(healthy_count)
    )
    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=1, task_notify=events.append
    )
    results = [value async for value in executor.run(work)]

    assert results == ['source-cancelled'] + [
        f'healthy-{index}' for index in range(healthy_count)
    ]
    assert [event['disposition'] for event in events] == [
        'cancelled', *(['completed'] * healthy_count)
    ]
    assert all(event['attempted'] is True for event in events)
    assert len({event['task_id'] for event in events}) == healthy_count + 1


@pytest.mark.asyncio
async def test_durable_accounting_failure_is_not_silently_accepted():
    def broken_sink(_event):
        raise OSError('durable store unavailable')

    async def check(**_kwargs):
        return 'native-result'

    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=1, task_notify=broken_sink
    )
    with pytest.raises(ExecutorAccountingError):
        _ = [
            value
            async for value in executor.run(
                [(check, [], {'default': 'fallback', '_executor_task': task_metadata(1)})]
            )
        ]


@pytest.mark.asyncio
async def test_accounting_failure_cancels_active_workers_before_propagating():
    """A second sink failure during finalization cannot leak source work."""
    events = []
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    async def completed(**_kwargs):
        await started.wait()
        return 'native-result'

    async def cancellation_resistant(**_kwargs):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    def broken_sink(event):
        events.append(dict(event))
        if event.get('event_type') != 'cleanup':
            raise OSError('durable store unavailable')

    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=2, task_notify=broken_sink
    )
    work = [
        (completed, [], {'default': 'completed-fallback', '_executor_task': task_metadata(1)}),
        (
            cancellation_resistant,
            [],
            {'default': 'slow-fallback', '_executor_task': task_metadata(2)},
        ),
        (completed, [], {'default': 'pending-fallback', '_executor_task': task_metadata(3)}),
    ]

    with pytest.raises(ExecutorAccountingError):
        _ = [value async for value in executor.run(work)]

    await asyncio.wait_for(cancelled.wait(), timeout=0.1)
    assert executor.pending_cleanup_count == 1
    assert any(event['task_id'] == 'opaque-task-3' for event in events)
    release.set()
    assert await executor.drain_cleanup(0.1) is True


@pytest.mark.asyncio
async def test_generator_close_without_timeout_tracks_resistant_query_cleanup():
    """Closing a no-timeout iterator never waits for source cleanup."""
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    async def completed(**_kwargs):
        await started.wait()
        return 'native-result'

    async def cancellation_resistant(**_kwargs):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    executor = AsyncioQueueGeneratorExecutor(logger=logger, in_parallel=2)
    iterator = executor.run([
        (completed, [], {'default': 'first', '_executor_task': task_metadata(11)}),
        (
            cancellation_resistant,
            [],
            {'default': 'second', '_executor_task': task_metadata(12)},
        ),
    ])

    assert await iterator.__anext__() == 'native-result'
    await asyncio.wait_for(iterator.aclose(), timeout=0.1)
    await asyncio.wait_for(cancelled.wait(), timeout=0.1)
    assert executor.pending_cleanup_count == 1
    release.set()
    assert await executor.drain_cleanup(0.1) is True


@pytest.mark.asyncio
async def test_generator_close_propagates_cancelled_task_accounting_failure():
    """Closing early cannot hide a durable terminal-accounting rejection."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def completed(**_kwargs):
        await started.wait()
        return 'native-result'

    async def cancellation_resistant(**_kwargs):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            await release.wait()

    def broken_sink(event):
        if event.get('disposition') == 'cancelled':
            raise OSError('durable store unavailable')

    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=2, task_notify=broken_sink
    )
    iterator = executor.run([
        (completed, [], {'default': 'first', '_executor_task': task_metadata(21)}),
        (
            cancellation_resistant,
            [],
            {'default': 'second', '_executor_task': task_metadata(22)},
        ),
    ])

    assert await iterator.__anext__() == 'native-result'
    with pytest.raises(ExecutorAccountingError):
        await asyncio.wait_for(iterator.aclose(), timeout=0.1)
    release.set()
    assert await executor.drain_cleanup(0.1) is True


@pytest.mark.asyncio
async def test_duplicate_task_identity_is_rejected_before_admission():
    started = []

    async def check(**_kwargs):
        started.append(True)
        return 'result'

    duplicate = {'task_id': 'same-opaque-task', 'attempt': 1}
    executor = AsyncioQueueGeneratorExecutor(logger=logger, in_parallel=2)
    with pytest.raises(ExecutorTaskMetadataError):
        _ = [
            value
            async for value in executor.run(
                [
                    (check, [], {'default': 'first', '_executor_task': duplicate}),
                    (check, [], {'default': 'second', '_executor_task': duplicate}),
                ]
            )
        ]
    assert started == []


@pytest.mark.asyncio
async def test_bounded_drain_reports_incomplete_cleanup_and_blocks_late_notifier_updates():
    from maigret.checking import _TaskNotifyGate

    events = []
    release = asyncio.Event()
    updates = []

    class Notify:
        def update(self, value):
            updates.append(value)

    gate = _TaskNotifyGate(Notify())

    async def cancellation_resistant(_gate, **_kwargs):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            _gate.update('late update')
            await release.wait()

    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=1, timeout=0.001, task_notify=events.append
    )
    results = [
        value
        async for value in executor.run(
            [
                (
                    cancellation_resistant,
                    [gate],
                    {
                        'default': 'timeout',
                        '_executor_task': task_metadata(99),
                        '_executor_finalize': gate.close,
                    },
                )
            ]
        )
    ]

    assert results == ['timeout']
    assert await executor.drain_cleanup(0.001) is False
    assert updates == []
    assert [event['cleanup_state'] for event in events if event.get('event_type') == 'cleanup'] == ['incomplete']

    release.set()
    assert await executor.drain_cleanup(0.1) is True
    assert [event['cleanup_state'] for event in events if event.get('event_type') == 'cleanup'] == [
        'incomplete', 'complete'
    ]


@pytest.mark.asyncio
async def test_drain_cleanup_never_exceeds_configured_maximum(monkeypatch):
    """Caller-provided cleanup time cannot silently expand a stage budget."""
    import maigret.executors as executors

    release = asyncio.Event()

    async def cancellation_resistant(**_kwargs):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            await release.wait()

    monkeypatch.setattr(executors, 'DEFAULT_CLEANUP_SECONDS', 0.01)
    executor = AsyncioQueueGeneratorExecutor(
        logger=logger, in_parallel=1, timeout=0, task_notify=lambda _event: None
    )
    _ = [
        value
        async for value in executor.run(
            [
                (
                    cancellation_resistant,
                    [],
                    {'default': 'timeout', '_executor_task': task_metadata(98)},
                )
            ]
        )
    ]

    start = time.monotonic()
    assert await executor.drain_cleanup(10) is False
    assert time.monotonic() - start < 0.1
    release.set()
    assert await executor.drain_cleanup(10) is True


@pytest.mark.asyncio
async def test_checking_does_not_publish_normal_finish_after_incomplete_cleanup(monkeypatch):
    """A retained source task makes the collection interrupted, never done."""
    from types import SimpleNamespace

    from maigret import checking

    started = asyncio.Event()
    release = asyncio.Event()

    class Notify:
        def __init__(self):
            self.finished = 0
            self.cleanup = []

        def start(self, *_args):
            return None

        def finish(self, *_args):
            self.finished += 1

        def warning(self, *_args):
            return None

        def update(self, *_args):
            return None

        def task_cleanup(self, event):
            self.cleanup.append(dict(event))

    async def cancellation_resistant(*_args, **_kwargs):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            await release.wait()

    monkeypatch.setattr(checking, 'check_site_for_username', cancellation_resistant)
    notify = Notify()
    site = SimpleNamespace(similar_search=False)

    with pytest.raises(checking.ExecutorCleanupIncompleteError):
        await checking.maigret(
            'subject',
            {'Example': site},
            logger=logger,
            query_notify=notify,
            no_progressbar=True,
            timeout=-0.5,
            cleanup_timeout=0.001,
        )

    assert started.is_set()
    assert notify.finished == 0
    assert notify.cleanup == [
        {'schema_version': 1, 'cleanup_complete': False, 'pending_tasks': 1}
    ]
    release.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_checking_cancellation_does_not_publish_normal_finish(monkeypatch):
    """User cancellation retains cancellation semantics through final drain."""
    from types import SimpleNamespace

    from maigret import checking

    started = asyncio.Event()
    release = asyncio.Event()

    class Notify:
        def __init__(self):
            self.finished = 0
            self.cleanup = []

        def start(self, *_args):
            return None

        def finish(self, *_args):
            self.finished += 1

        def warning(self, *_args):
            return None

        def update(self, *_args):
            return None

        def task_cleanup(self, event):
            self.cleanup.append(dict(event))

    async def cancellation_resistant(*_args, **_kwargs):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            await release.wait()

    monkeypatch.setattr(checking, 'check_site_for_username', cancellation_resistant)
    notify = Notify()
    site = SimpleNamespace(similar_search=False)
    collection = asyncio.create_task(
        checking.maigret(
            'subject',
            {'Example': site},
            logger=logger,
            query_notify=notify,
            no_progressbar=True,
            cleanup_timeout=0.001,
        )
    )
    await started.wait()
    collection.cancel()
    with pytest.raises(asyncio.CancelledError):
        await collection

    assert notify.finished == 0
    assert notify.cleanup == [
        {'schema_version': 1, 'cleanup_complete': False, 'pending_tasks': 1}
    ]
    release.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_checking_emits_matching_planned_and_terminal_fallback_events(monkeypatch):
    """The durable plan is written before a failing task can be admitted."""
    from types import SimpleNamespace

    from maigret import checking
    from maigret.result import MaigretCheckStatus

    class Notify:
        def __init__(self):
            self.planned = []
            self.terminal = []
            self.updated = []

        def start(self, *_args):
            return None

        def finish(self, *_args):
            return None

        def warning(self, *_args):
            return None

        def update(self, result, is_similar=False):
            self.updated.append((result, is_similar))

        def task_planned(self, event):
            self.planned.append(dict(event))

        def task_terminal(self, event):
            self.terminal.append(dict(event))

    async def failed_check(*_args, **_kwargs):
        raise RuntimeError('synthetic source failure')

    monkeypatch.setattr(checking, 'check_site_for_username', failed_check)
    notify = Notify()
    site = SimpleNamespace(similar_search=False)
    results = await checking.maigret(
        'subject',
        {'Example': site},
        logger=logger,
        query_notify=notify,
        no_progressbar=True,
        execution_id='test-execution-scope',
    )

    assert len(notify.planned) == len(notify.terminal) == 1
    planned, terminal = notify.planned[0], notify.terminal[0]
    assert planned == {
        key: terminal[key]
        for key in (
            'schema_version', 'check_id', 'task_id', 'source_id', 'target_id', 'attempt'
        )
    }
    assert terminal['disposition'] == 'error'
    assert terminal['attempted'] is True
    assert all('subject' not in str(event) for event in (planned, terminal))
    assert results['Example']['status'].status == MaigretCheckStatus.UNKNOWN
    assert results['Example']['status'].error.type == 'Request failed'
    assert len(notify.updated) == 1
