# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
"""Supervise one collector process under the existing worker's lease and lock."""

from contextlib import nullcontext
import multiprocessing
import os
import time

from maigret.web.artifact_execution import reap_process, enable_subreaper
from maigret.web.execution_budget import ExecutionBudget


class IncompleteCollectionCleanup(RuntimeError):
    """Only a supervisor may reconcile after this process exits."""


STOP_GRACE_SECONDS = 20.0
FINALIZATION_GRACE_SECONDS = 40.0
_STOP_CAUSES = (
    'operator_cancel',
    'job_deadline',
    'stage_deadline',
    'cleanup_incomplete',
    'persistence_failure',
    'lease_lost',
    'worker_shutdown',
)
_STOP_CAUSE_TO_CODE = {cause: index + 1 for index, cause in enumerate(_STOP_CAUSES)}
_CODE_TO_STOP_CAUSE = {code: cause for cause, code in _STOP_CAUSE_TO_CODE.items()}


def _set_stop_cause(shared_cause, cause):
    """Record the first authoritative stop cause for both process owners."""
    code = _STOP_CAUSE_TO_CODE.get(str(cause))
    if code is None:
        raise ValueError('Unknown collector stop cause')
    with shared_cause.get_lock():
        if shared_cause.value == 0:
            shared_cause.value = code
        return _CODE_TO_STOP_CAUSE.get(shared_cause.value)


def _shared_stop_cause(shared_cause):
    with shared_cause.get_lock():
        return _CODE_TO_STOP_CAUSE.get(shared_cause.value)


def _child_shutdown_cause(shared_cause, stop_event):
    cause = _shared_stop_cause(shared_cause)
    # Operator cancellation is read from the durable row by run_persistent_job;
    # treating it as supervisor shutdown would incorrectly label it interrupted.
    if cause == 'operator_cancel':
        return False
    return cause or ('worker_shutdown' if stop_event.is_set() else False)


def _profile_child(database_url, job, config, stop_event, shared_cause,
                   fixture_adapter_factory):
    if os.name == 'posix':
        os.setsid()
    from maigret.web import app as web_app
    from maigret.web.case_store import CaseStore

    web_app.app.config.update(config)
    store = CaseStore(database_url)
    web_app.case_store = store
    if fixture_adapter_factory is not None and not config.get('TESTING'):
        raise RuntimeError('Fixture adapters require an explicit test runtime')
    fixture = fixture_adapter_factory(web_app) if fixture_adapter_factory else nullcontext()
    try:
        with fixture:
            web_app.run_persistent_job(
                store,
                job,
                shutdown_check=lambda: _child_shutdown_cause(shared_cause, stop_event),
            )
    except IncompleteCollectionCleanup:
        # No interpreter teardown with live source tasks and no late owner writes.
        os._exit(75)
    finally:
        store.dispose()


def execute_profile_process(store, job, *, shutdown_check, config,
                            fixture_adapter_factory=None):
    """No source replay or second collector; heartbeat/claim remain in parent."""
    enable_subreaper()
    context = multiprocessing.get_context('spawn')
    stop_event = context.Event()
    shared_cause = context.Value('i', 0, lock=True)
    process = context.Process(target=_profile_child, args=(
        store.engine.url.render_as_string(hide_password=False), job, config,
        stop_event, shared_cause, fixture_adapter_factory,
    ), name='openledger-collector')
    process.start()
    forced_cause = None
    stop_deadline = None
    hard_deadline = (
        time.monotonic()
        + ExecutionBudget.from_job(job).remaining_seconds()
        + FINALIZATION_GRACE_SECONDS
    )
    try:
        while process.is_alive():
            cause = shutdown_check()
            if cause:
                forced_cause = _set_stop_cause(
                    shared_cause,
                    cause if isinstance(cause, str) else 'worker_shutdown',
                )
            elif store.is_cancel_requested(job['job_id']):
                forced_cause = _set_stop_cause(shared_cause, 'operator_cancel')
            if forced_cause:
                # The child reads the typed shared cause directly.  The event
                # remains a fallback for a child which starts during shutdown.
                stop_event.set()
                if stop_deadline is None:
                    stop_deadline = time.monotonic() + STOP_GRACE_SECONDS
            if stop_deadline is not None and time.monotonic() >= stop_deadline:
                break
            if time.monotonic() >= hard_deadline:
                forced_cause = _set_stop_cause(shared_cause, 'job_deadline')
                stop_event.set()
                break
            process.join(0.1)
    finally:
        # Reconciliation is legal only after the source process has exited.
        reap_process(process, whole_session=True)
    current = store.get_job(job['job_id'])
    if current and current.get('status') in {'running', 'cancel_requested'}:
        store.mark_stale_running(
            0,
            job_id=job['job_id'],
            worker_id=job['worker_id'],
            stop_cause=forced_cause or _shared_stop_cause(shared_cause)
            or 'cleanup_incomplete',
        )
    return process.exitcode == 0
