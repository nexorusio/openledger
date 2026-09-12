"""The mandatory P2 query-to-evidence execution service.

Collectors are adapters, never owners of the investigation lifecycle. Every
attempt writes to PostgreSQL before its outcome is announced, and every output
is consolidated in the original case before an operator can curate a version.
There is deliberately no dispatch to the former ``_stream_search`` pipeline.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import json
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from sqlalchemy import insert, select, update

from maigret.web.case_store import (
    ACTIVE_STATUSES,
    investigation_jobs,
    personas,
    utcnow,
)
from maigret.web.execution_budget import ExecutionBudget


def _app():
    # Keep app imports lazy: Flask also imports this service at registration.
    import importlib

    return importlib.import_module("maigret.web.app")


def _run_coroutine_sync(factory):
    """Run one pipeline coroutine from sync code, including Playwright threads.

    Flask workers normally call this without an active event loop.  The
    operator/browser acceptance path can call it while Playwright's sync API
    owns a loop in the current thread, where ``asyncio.run`` and a second loop
    would fail.  Keep the public worker API synchronous and isolate that case
    in a short-lived helper thread.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())

    result, errors = [], []

    def run_in_thread():
        try:
            result.append(asyncio.run(factory()))
        except BaseException as error:  # preserve the original failure type
            errors.append(error)

    thread = threading.Thread(target=run_in_thread, name="openledger-pipeline-loop")
    thread.start()
    thread.join()
    if errors:
        raise errors[0]
    return result[0]


def source_configuration(app_module=None) -> dict[str, Any]:
    """Read the real server configuration, excluding credentials."""
    app_module = app_module or _app()
    from maigret.web.pipeline_query import runtime_source_status

    result = runtime_source_status()
    flags = app_module.profile_discovery_flags()
    result["scanner_available"] = app_module.user_scanner_available()
    result["public_search"] = dict(result["native_search"])
    result["google_places_enabled"] = app_module.google_places_search_enabled()
    result["ai_enabled"] = bool(app_module.get_openai_api_key())
    result["enrichment_enabled"] = flags["enrichment_providers_enabled"]
    result["revision"] = hashlib.sha256(
        json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return result


from maigret.web.pipeline_job_context import (
    resolve_case_subjects as _case_subjects,
    subject_spec as _subject_spec,
    approved_context as _approved_context,
)


def fair_task_order(tasks):
    """Round-robin adapters so one engine's aliases do not starve the rest."""
    queues = defaultdict(deque)
    for task in tasks:
        queues[str(task.get("engine_id", ""))].append(task)
    ordered = []
    while queues:
        for key in list(queues):
            ordered.append(queues[key].popleft())
            if not queues[key]:
                del queues[key]
    return ordered


class CollectorContext:
    """One leased attempt with an atomic observation sink."""

    def __init__(
        self,
        store,
        pipeline,
        job,
        request,
        task,
        attempt,
        sink,
        cancellation_check,
        options,
        context,
    ):
        self.store, self.pipeline, self.job = store, pipeline, job
        self.request, self.task, self.attempt = request, task, attempt
        self.sink, self.cancelled = sink, cancellation_check
        self.options, self.context = options, context
        self.persisted_ids: set[str] = set()
        self.observation_count = 0
        self.raw_general_results = []
        self.raw_collector_observations = []
        self.native_audits = []

    def normalize(self, result: Any, *, engine=None):
        from maigret.web.connectors.registry import normalize_connector_result

        if result is None:
            return []
        engine = engine or self.task["engine_id"]
        spec = self.task.get("spec") or {}
        scope = dict(
            case_id=self.job["case_id"],
            subject_id=self.request["persona_id"],
            request_id=self.request["id"],
            task_id=self.task["id"],
            attempt_id=self.attempt["id"],
            engine=engine,
            observed_at=self.attempt.get("created_at"),
            retention_policy=self.task.get("retention") or spec.get("retention"),
            engine_version=self.task.get("engine_version")
            or spec.get("engine_version"),
            parser_version=self.task.get("parser_version")
            or spec.get("parser_version"),
        )
        return list(
            normalize_connector_result(
                self.task,
                result,
                **{key: value for key, value in scope.items() if value is not None},
            )
        )

    def emit(self, result: Any, *, engine=None):
        observations = self.normalize(result, engine=engine)
        fresh = [item for item in observations if item["id"] not in self.persisted_ids]
        if fresh:
            self.pipeline.append_observations(
                self.attempt["id"], fresh, worker_id=self.job.get("worker_id")
            )
            self.persisted_ids.update(item["id"] for item in fresh)
            self.observation_count += len(fresh)

    def emit_observations(self, observations):
        self.emit({"collector_observations": list(observations or [])})

    def reserve_request(self, count=1, provider=None):
        from maigret.web.pipeline_runtime import PipelineRuntimeStore

        return PipelineRuntimeStore(self.store).reserve_request(
            self.request["id"],
            self.attempt["id"],
            self.job.get("worker_id"),
            count=count,
            provider=provider or self.task.get("provider_key"),
        )

    @property
    def checkpoint(self):
        from maigret.web.pipeline_connector_ingestion import ConnectorIngestionStore

        return ConnectorIngestionStore(self.pipeline).get_checkpoint(self.task["id"])

    def checkpoint_page(
        self,
        records,
        next_cursor,
        *,
        watermark=None,
        completeness="partial",
        source_versions=None,
        expected_cursor=None,
    ):
        from maigret.web.pipeline_connector_ingestion import ConnectorIngestionStore

        observations = self.normalize({"collector_observations": list(records or [])})
        result = ConnectorIngestionStore(self.pipeline).checkpoint_page(
            self.attempt["id"],
            observations,
            cursor=next_cursor,
            watermark=watermark,
            completeness=completeness,
            source_versions=source_versions,
            worker_id=self.job.get("worker_id"),
            expected_cursor=expected_cursor,
        )
        # Source versions may replace adapter IDs with durable record IDs, and
        # a replay can normalize fresh IDs while adding no evidence at all.
        self.persisted_ids.update(result["observation_ids"])
        self.observation_count += result["added_count"]
        return result


async def _maigret_adapter(task, context: CollectorContext):
    app_module = _app()
    username = task["input_value"]

    class DurableNotify(app_module.StreamNotify):
        def update(self, result, is_similar=False):
            if not is_similar:
                _, site = app_module.resolve_selected_site(self.sites, result.site_name)
                row = {
                    "status": result,
                    "url_user": result.site_url_user,
                    "http_status": getattr(result, "http_status", None),
                }
                if site is not None:
                    row["site"] = site
                # Commit before StreamNotify publishes found/progress events.
                context.emit(
                    {
                        "general_results": [
                            (username, "username", {result.site_name: row})
                        ]
                    }
                )
            super().update(result, is_similar=is_similar)

        def set_source_coverage(self, coverage):
            super().set_source_coverage(coverage)
            context.emit_observations(
                [
                    {
                        "source_engine": "maigret",
                        "source_record_id": "selection:"
                        + str(item.get("site_name") or item.get("site") or index),
                        "status": "not_executed",
                        "source_name": item.get("site_name") or item.get("site"),
                        "reason": item.get("reason") or item.get("selection_reason"),
                        "extra": {"selection": item, "selection_only": True},
                    }
                    for index, item in enumerate(coverage)
                    if not item.get("selected", True)
                ]
            )

    notify = DurableNotify(context.sink, username, cancellation_check=context.cancelled)
    results = await app_module.maigret_search(
        username, context.options, query_notify=notify
    )
    general = (username, "username", results)
    context.raw_general_results.append(general)
    context.emit({"general_results": [general]})
    from maigret.web.pipeline_evidence import normalize_status

    return {
        "outcome": _aggregate_outcomes(
            normalize_status(row)[0] for row in results.values()
        )
    }


def _aggregate_outcomes(outcomes):
    statuses = set(outcomes)
    if statuses & {"found", "candidate", "not_found"} and statuses & {
        "blocked",
        "error",
        "timeout",
        "cancelled",
        "inconclusive",
        "partial",
    }:
        return "partial"
    for value in (
        "partial",
        "found",
        "candidate",
        "blocked",
        "error",
        "timeout",
        "cancelled",
    ):
        if value in statuses:
            return value
    if (
        statuses
        and statuses <= {"not_found", "not_executed"}
        and "not_found" in statuses
    ):
        return "not_found"
    return "inconclusive"


async def _native_adapter(task, context: CollectorContext):
    app_module = _app()
    config = app_module.load_profile_search_config()
    client = app_module.GovernedProfileSearchClient(
        app_module.ProfileSearchClient(config),
        circuit_breaker_enabled=True,
    )
    identifier_type = task["input_type"]
    specification = {
        "identifiers": [{"type": identifier_type, "value": task["input_value"]}],
        "search_targets": (
            [{"value": task["input_value"], "source_type": "username"}]
            if identifier_type == "username"
            else []
        ),
    }
    result = await app_module.ProfileSearchOrchestrator(client).discover(
        specification,
        platforms=[task["platform"]],
        max_queries=1,
        max_results=config.max_results,
        cancellation_check=context.cancelled,
    )
    context.store.record_profile_search_result(
        context.job["job_id"], result, worker_id=context.job.get("worker_id")
    )
    document = result.as_dict()
    context.native_audits.append(document)
    context.emit({"profile_search_audits": [{"document": document}]})
    return {
        "outcome": (
            "cancelled"
            if result.stopped
            else (
                "error"
                if result.status == "failed"
                else "candidate" if result.candidates else "not_found"
            )
        )
    }


async def dispatch_collector(task, context: CollectorContext):
    """Execute the reviewed manifest's registered connector contract."""
    from maigret.web.connectors.registry import collect_registered

    return await collect_registered(task, context)


async def _cited_research_adapter(task, context):
    from maigret.ai import get_case_chat_response, get_case_chat_claim_proposals

    app_module = _app()
    settings = app_module.load_settings()
    api_key = app_module.get_openai_api_key()
    if not api_key:
        raise ValueError("Cited research provider is not configured")
    case_context = context.store.get_case_chat_context(context.job["case_id"])
    model = settings.get("openai_model") or app_module.DEFAULT_SETTINGS["openai_model"]
    response = await get_case_chat_response(
        api_key,
        case_context=case_context,
        conversation=[],
        user_message=task["input_value"],
        model=model,
        timeout_seconds=min(120, task["timeout_seconds"]),
        web_search_enabled=True,
        **app_module.ai_endpoint_options(),
    )
    sources = response.get("sources") or []
    rows = [
        {
            "source_engine": "openai_web_research",
            "source_record_id": "citation:" + str(index),
            "source_url": item.get("url"),
            "source_name": item.get("title"),
            "status": "candidate",
            "payload": {
                "source": item,
                "analysis": response.get("analysis"),
                "identity_status": "unverified",
                "model": model,
                "derived_from": [item.get("url")],
            },
        }
        for index, item in enumerate(sources)
    ]
    context.emit_observations(rows)
    context.raw_collector_observations.extend(rows)
    return {"outcome": "candidate" if rows else "inconclusive"}


async def _await_collector(call, *, timeout_seconds, cancelled):
    """Bound waits independently of a provider's cancellation cooperation."""
    task = asyncio.create_task(call)
    started = time.monotonic()
    cause = None
    try:
        while not task.done():
            if cancelled():
                cause = "cancelled"
                break
            if time.monotonic() - started >= timeout_seconds:
                cause = "timeout"
                break
            await asyncio.wait({task}, timeout=0.25)
        if cause:
            task.cancel()
            finished, _ = await asyncio.wait({task}, timeout=5)
            if task not in finished:
                raise RuntimeError("Collector failed bounded cancellation")
            await asyncio.gather(task, return_exceptions=True)
            if cause == "cancelled":
                raise asyncio.CancelledError()
            raise asyncio.TimeoutError()
        return task.result()
    finally:
        if not task.done():
            task.cancel()


def refresh_consolidation(store, case_id, persona_id):
    from maigret.web.pipeline_assessment_runtime import assess_consolidated_groups

    return assess_consolidated_groups(store, case_id, persona_id)


async def _execute_requests(
    store, job, requests, contexts, *, adapters=None, shutdown_check=None
):
    from maigret.web.pipeline_contract import validate_task
    from maigret.web.pipeline_runtime import ProviderCooldown, RequestBudgetExceeded
    from maigret.web.pipeline_process import supervise_collector
    from maigret.web.pipeline_store import (
        PipelineStore,
        collection_status as collection_status_for_request,
        task_can_retry,
    )

    app_module = _app()
    pipeline = PipelineStore(store)
    sink = app_module.PersistentEventSink(
        store, job["job_id"], worker_id=job.get("worker_id")
    )
    budget = ExecutionBudget.from_job(job)
    operator_stopped = lambda: store.is_cancel_requested(job["job_id"])
    shutting_down = lambda: bool(shutdown_check and shutdown_check())
    cancelled = lambda: operator_stopped() or shutting_down() or budget.is_exhausted()
    queue = fair_task_order(
        [
            dict(task.get("spec") or {}, **task, _request=request)
            for request in requests
            for task in request.get("tasks", [])
        ]
    )
    semaphore = asyncio.Semaphore(3)
    outcomes, collector_contexts = [], []

    async def execute_task(task):
        request = task.pop("_request")
        validate_task(task)
        async with semaphore:
            if task["route_state"] != "active":
                outcomes.append("not_executed")
                return
            while True:
                current = pipeline.get_task(task["id"])
                if current.get("status") == "completed" and not task_can_retry(current):
                    outcomes.append(current.get("outcome") or "inconclusive")
                    return
                retry_at = ((current.get("spec") or {}).get("_execution") or {}).get(
                    "next_retry_at"
                )
                if retry_at and datetime.fromisoformat(retry_at) > datetime.now(
                    timezone.utc
                ):
                    outcomes.append(current.get("outcome") or "inconclusive")
                    return
                if (
                    current.get("status") in {"cancelled", "not_executed"}
                    or cancelled()
                ):
                    outcomes.append("cancelled")
                    return
                attempt = pipeline.start_attempt(
                    task["id"],
                    job.get("worker_id") or "local-worker",
                    resume=bool(current.get("active_attempt_id")),
                )
                context = CollectorContext(
                    store,
                    pipeline,
                    job,
                    request,
                    task,
                    attempt,
                    sink,
                    cancelled,
                    app_module.hydrate_persistent_options(job.get("options") or {}),
                    contexts[request["persona_id"]],
                )
                collector_contexts.append(context)
                sink.put(
                    {
                        "type": "collector_started",
                        "collector": task["engine_id"],
                        "task_id": task["id"],
                        "attempt_id": attempt["id"],
                        "pipeline_id": "p2-e2e-v1",
                    }
                )
                error = None
                returned = {}
                try:
                    if task.get("_revalidation_block"):
                        context.emit_observations(
                            [
                                {
                                    "source_engine": task["engine_id"],
                                    "source_record_id": "plan-revalidation",
                                    "status": "not_executed",
                                    "reason": task["_revalidation_block"],
                                }
                            ]
                        )
                        pipeline.finish_attempt(
                            attempt["id"],
                            "not_executed",
                            worker_id=job.get("worker_id"),
                            error=task["_revalidation_block"],
                        )
                        outcomes.append("not_executed")
                        return
                    timeout = min(
                        float(task["timeout_seconds"]),
                        max(0.05, budget.remaining_seconds()),
                    )
                    # Production collectors always execute in a fenced process
                    # group. Explicit Python fixtures are the sole test hook.
                    if adapters is None:
                        returned = await supervise_collector(
                            task, context, timeout_seconds=timeout, cancelled=cancelled
                        )
                    else:
                        if task["execution_key"] not in adapters:
                            raise ValueError("No explicit fixture for active adapter")
                        returned = await _await_collector(
                            adapters[task["execution_key"]](task, context),
                            timeout_seconds=timeout,
                            cancelled=cancelled,
                        )
                    outcome = returned.get("outcome") or "inconclusive"
                    if returned.get("completeness") in {
                        "partial",
                        "truncated",
                        "unknown",
                    } and outcome in {"found", "candidate", "not_found"}:
                        outcome = "partial"
                    # Diagnostic codes are safe lifecycle metadata; arbitrary
                    # adapter result/error payloads belong in the retention sink.
                    error = returned.get("error_code")
                except RequestBudgetExceeded:
                    outcome, error = "inconclusive", "request_budget_exhausted"
                    returned = {"retryable": False, "completeness": "partial"}
                except ProviderCooldown as exc:
                    outcome, error = "error", "provider_cooldown"
                    returned = {
                        "retryable": True,
                        "retry_after_seconds": exc.retry_after_seconds,
                        "completeness": "partial",
                    }
                except asyncio.CancelledError:
                    outcome, error = (
                        "cancelled",
                        "Collection stopped; committed evidence retained",
                    )
                except (asyncio.TimeoutError, TimeoutError):
                    outcome, error = "timeout", "The task reached its deadline"
                except Exception as exc:
                    error = app_module.record_internal_error(
                        "Pipeline collector failed",
                        exc,
                        collector=task["engine_id"],
                        session=job["job_id"],
                    )
                    outcome = "error"
                retryable = bool(returned.get("retryable", outcome == "timeout"))
                # Blocks require an operator/configuration change; repeated
                # credential/CAPTCHA attempts cannot repair them.
                if outcome in {"blocked", "cancelled", "not_executed"}:
                    retryable = False
                retry_after = max(
                    0.0, min(86400.0, float(returned.get("retry_after_seconds") or 0))
                )
                retry_state = {
                    "retryable": retryable,
                    "retry_after_seconds": retry_after,
                }
                if retry_after:
                    retry_state["next_retry_at"] = (
                        datetime.now(timezone.utc) + timedelta(seconds=retry_after)
                    ).isoformat()
                if cancelled() or outcome == "cancelled":
                    # An operator cancellation fences append/finish immediately.
                    # Finalize all outstanding task states after every process
                    # group is drained, using the separate cancellation boundary.
                    outcomes.append("cancelled")
                    return
                context.emit_observations(
                    [
                        {
                            "source_engine": task["engine_id"],
                            "source_record_id": "task-outcome",
                            "status": outcome,
                            "reason": error or "Task completed",
                            "extra": {
                                "task_id": task["id"],
                                "route_state": task["route_state"],
                                "input_type": task.get("input_type"),
                                "platform": task.get("platform"),
                                "observations": context.observation_count,
                                "retry": retry_state,
                            },
                        }
                    ]
                )
                pipeline.finish_attempt(
                    attempt["id"],
                    outcome,
                    worker_id=job.get("worker_id"),
                    error=error,
                    retry_state=retry_state,
                )
                sink.put(
                    {
                        "type": "collector_completed",
                        "collector": task["engine_id"],
                        "task_id": task["id"],
                        "outcome": outcome,
                        "observations": context.observation_count,
                        "pipeline_id": "p2-e2e-v1",
                    }
                )
                if (
                    not retryable
                    or retry_after > 0
                    or attempt["number"] >= current["retry_limit"] + 1
                ):
                    outcomes.append(outcome)
                    return

    results = await asyncio.gather(
        *(execute_task(task) for task in queue), return_exceptions=True
    )
    errors = [value for value in results if isinstance(value, BaseException)]
    # No collector remains alive before cancellation/terminal job transitions.
    if cancelled():
        for request in requests:
            if shutting_down() and not operator_stopped():
                pipeline.interrupt_request(
                    request["id"],
                    worker_id=job.get("worker_id"),
                    reason="Worker shutdown; committed evidence retained",
                )
            else:
                pipeline.cancel_request(
                    request["id"],
                    worker_id=job.get("worker_id"),
                    reason=(
                        "Budget exhausted"
                        if budget.is_exhausted()
                        else "Collection stopped"
                    ),
                )
    if errors:
        # Persistence/ownership failure is fatal and must never look like empty
        # successful collection. Let the worker's fenced failure path own it.
        raise errors[0]
    for request in requests:
        refresh_consolidation(store, job["case_id"], request["persona_id"])
    active = [
        task
        for request in requests
        for task in request.get("tasks", [])
        if task.get("availability") == "active"
    ]
    substantive = any(
        item in {"found", "candidate", "not_found", "partial"} for item in outcomes
    )
    if shutting_down():
        status, collection_status = "interrupted", "interrupted"
    elif operator_stopped():
        status, collection_status = "cancelled", "cancelled"
    elif budget.is_exhausted():
        status, collection_status = "budget_exhausted", "partial"
    elif not active:
        status, collection_status = "failed", "research_needed"
    elif not substantive:
        status, collection_status = "failed", "failed"
    else:
        status = "completed"
        collection_status = (
            "partial"
            if any(
                item
                in {
                    "error",
                    "timeout",
                    "blocked",
                    "cancelled",
                    "inconclusive",
                    "partial",
                }
                for item in outcomes
            )
            else "completed"
        )
    from maigret.web.pipeline_consolidation import account_identity

    request_ids = {item["id"] for item in requests}
    observed_accounts, persisted_count = set(), 0
    for persona_id in {item["persona_id"] for item in requests}:
        for observation in pipeline.iter_observations(job["case_id"], persona_id):
            if observation.get("request_id") not in request_ids:
                continue
            persisted_count += 1
            if observation.get("status") in {"found", "candidate"}:
                account = account_identity(observation)
                if account:
                    observed_accounts.add(account["id"])
    result = {
        "status": status,
        "collection_status": collection_status,
        "pipeline_id": "p2-e2e-v1",
        "job_id": job["job_id"],
        "case_id": job["case_id"],
        "usernames": list(job.get("usernames") or []),
        "request_ids": [item["id"] for item in requests],
        "task_count": len(queue),
        "outcome_counts": {key: outcomes.count(key) for key in sorted(set(outcomes))},
        "observation_count": persisted_count,
        "found_count": len(observed_accounts),
        "account_candidate_count": len(observed_accounts),
        "successful_source_task_count": outcomes.count("found")
        + outcomes.count("candidate"),
        "individual_reports": [],
        "collector_observations": [],
        "error": (
            "No active compatible collection route; research remains open."
            if not active
            else (
                "No substantive source result; inspect the source outcomes."
                if not substantive and not cancelled()
                else None
            )
        ),
        "execution_budget": budget.as_dict(),
        "review_url": "/cases/" + job["case_id"] + "/pipeline",
    }
    request_statuses = {}
    for request in requests:
        saved = pipeline.get_request(request["id"])
        if cancelled():
            request_status = saved["status"]
        else:
            # A sibling subject's successful source cannot close this research.
            request_status = collection_status_for_request(saved["tasks"])
            if any(
                task_can_retry(task)
                and ((task.get("spec") or {}).get("_execution") or {}).get(
                    "next_retry_at"
                )
                for task in saved["tasks"]
            ):
                request_status = "interrupted"
            pipeline.update_request_status(request["id"], request_status)
        request_statuses[request["id"]] = request_status
    result["request_statuses"] = request_statuses
    if not cancelled() and "interrupted" in request_statuses.values():
        result["status"] = status = "interrupted"
        result["collection_status"] = collection_status = (
            "partial" if substantive else "interrupted"
        )
        result["error"] = (
            "A source requested a retry delay. Resume this query after its recorded next retry time; original budgets apply."
        )
    if result["collection_status"] == "completed" and any(
        value != "completed" for value in request_statuses.values()
    ):
        result["collection_status"] = collection_status = "partial"
    if status == "completed" and job.get("kind") in {"case_fusion", "case_fusion_ai"}:
        result = _publish_case_operation(store, pipeline, job, requests, result)
        sink.put(
            {
                "type": "done",
                "status": "completed",
                "pipeline_id": "p2-e2e-v1",
                "redirect": "/cases/" + job["case_id"],
            }
        )
        return result
    if store.finish(job["job_id"], result, worker_id=job.get("worker_id")):
        sink.put(
            {
                "type": "done",
                "status": collection_status,
                "pipeline_id": "p2-e2e-v1",
                "redirect": result["review_url"],
            }
        )
    return result


def _publish_case_operation(store, pipeline, job, requests, result):
    """Publish only the exact payload from successful durable attempts."""
    from maigret.web.pipeline_case_fusion import recover_pending_publication
    from maigret.web.case_store import _heartbeat_expired

    attempts, tasks = pipeline._table("attempts"), pipeline._table("tasks")
    publications = []
    for request in requests:
        with store.engine.connect() as connection:
            successful = list(
                connection.scalars(
                    select(attempts.c.id)
                    .join(tasks, tasks.c.id == attempts.c.task_id)
                    .where(
                        tasks.c.request_id == request["id"],
                        attempts.c.status == "completed",
                        attempts.c.outcome.in_(["candidate", "found"]),
                    )
                )
            )
        publication = recover_pending_publication(
            pipeline.iter_observations(job["case_id"], request["persona_id"]),
            kind=job["kind"],
            request_id=request["id"],
            completed_attempt_ids=successful,
        )
        if publication:
            publications.append(publication)
    if len(publications) != 1:
        raise ValueError(
            "Combined operation requires one successful immutable publication"
        )
    publication = publications[0]
    if job["kind"] == "case_fusion":
        result = dict(publication["snapshot_result"], **result, kind="case_fusion")
        ai_job_id = store.publish_case_fusion_snapshot(
            job["job_id"],
            result,
            publication["analysis_context"],
            worker_id=job.get("worker_id"),
        )
        if ai_job_id is None:
            raise ValueError("Combined snapshot publication lost its worker lease")
        return dict(result, ai_job_id=ai_job_id)
    # Run completion and terminal job publication share one transaction. A
    # crash cannot leave committed relationship proposals behind a running job.
    with store.engine.begin() as connection:
        current = (
            connection.execute(
                select(investigation_jobs)
                .where(investigation_jobs.c.id == job["job_id"])
                .with_for_update()
            )
            .mappings()
            .one()
        )
        if (
            current["status"] != "running"
            or current["cancel_requested"]
            or current["worker_id"] != job.get("worker_id")
            or _heartbeat_expired(current["heartbeat_at"], now=utcnow())
        ):
            raise ValueError("Combined synthesis publication lost its worker lease")
        count = store.complete_combined_analysis_run(
            publication["run_id"], publication["insights"], connection=connection
        )
        result = dict(
            result,
            kind="case_fusion_ai",
            snapshot_job_id=publication["snapshot_job_id"],
            snapshot_sha256=publication["snapshot_sha256"],
            ai_analysis={
                **{
                    key: publication.get(key)
                    for key in (
                        "run_id",
                        "model",
                        "web_search_enabled",
                        "web_search_completed",
                        "truncated_claim_count",
                    )
                },
                "status": "completed",
                "proposal_count": count,
            },
        )
        connection.execute(
            update(investigation_jobs)
            .where(investigation_jobs.c.id == job["job_id"])
            .values(
                status="completed",
                result=result,
                error=None,
                completed_at=utcnow(),
                heartbeat_at=utcnow(),
                updated_at=utcnow(),
            )
        )
    return result

def execute_pipeline_job(store, job, *, shutdown_check=None, adapters=None):
    """Execute the persisted new pipeline; never fall back on missing schema."""
    from maigret.web.pipeline_contract import PIPELINE_ID
    from maigret.web.pipeline_query import build_query_plan
    from maigret.web.pipeline_store import PipelineStore

    pipeline = PipelineStore(store)
    requests, contexts = [], {}
    if job.get("kind") == "connector_ingestion":
        requests = pipeline.requests_for_job(job["job_id"])
        if not requests:
            raise ValueError(
                "Connector ingestion requires its persisted scoped request"
            )
        return _run_coroutine_sync(
            lambda: _execute_requests(
                store,
                job,
                requests,
                {item["persona_id"]: {} for item in requests},
                adapters=adapters,
                shutdown_check=shutdown_check,
            )
        )
    statuses = source_configuration()
    bindings = _case_subjects(store, job)
    saved_requests = pipeline.requests_for_job(job["job_id"])
    for binding in bindings:
        specification = _subject_spec(job, binding)
        context = _approved_context(store, job, binding)
        if specification.get("requirement_ids") and specification.get(
            "pipeline_context"
        ):
            targeted_context = copy.deepcopy(specification["pipeline_context"])
            for key in ("approved_full_names", "approved_organizations"):
                targeted_context[key] = context.get(key, [])
            targeted_context["collection_options"] = dict(job.get("options") or {})
            context = targeted_context
        contexts[binding["persona_id"]] = context
        saved = next(
            (
                item
                for item in saved_requests
                if item["persona_id"] == binding["persona_id"]
            ),
            None,
        )
        if saved:
            from maigret.web.pipeline_query import revalidate_query_plan

            checked = revalidate_query_plan(
                saved["plan"], specification, source_status=statuses, context=context
            )
            current_tasks = {item["task_id"]: item for item in checked["plan"]["tasks"]}
            for task in saved["tasks"]:
                original = task["spec"]
                current = current_tasks.get(original["task_id"])
                if original["route_state"] == "active" and (
                    not current
                    or current["route_state"] != "active"
                    or original["source_config_revision"]
                    != current["source_config_revision"]
                ):
                    task["_revalidation_block"] = (current or {}).get(
                        "reason"
                    ) or "Source configuration changed; submit a reviewed follow-up request."
            if checked["changes"]:
                store.append_event(
                    job["job_id"],
                    {
                        "type": "plan_revalidated",
                        "pipeline_id": "p2-e2e-v1",
                        "request_id": saved["id"],
                        "changes": checked["changes"],
                        "message": "Changed routes require a new reviewed request; existing evidence is retained.",
                    },
                    runtime_guard=True,
                    worker_id=job.get("worker_id"),
                )
            requests.append(saved)
            continue
        plan = build_query_plan(
            specification,
            case_id=job["case_id"],
            subject_id=binding["persona_id"],
            source_status=statuses,
            context=context,
            origin=specification.get("research_origin") or {},
            budgets=specification.get("collection_budget"),
        )
        requests.append(
            pipeline.create_request(
                job["case_id"],
                binding["persona_id"],
                plan["inputs"],
                plan,
                job_id=job["job_id"],
                actor=(job.get("options") or {}).get("requested_by") or "case-operator",
                idempotency_key="job:" + job["job_id"] + ":" + binding["persona_id"],
                parent_request_id=specification.get("parent_request_id"),
                requirement_ids=specification.get("requirement_ids") or [],
            )
        )
    if any(item.get("pipeline_id", PIPELINE_ID) != PIPELINE_ID for item in requests):
        raise RuntimeError(
            "The queued research request belongs to an unsupported pipeline"
        )
    return _run_coroutine_sync(
        lambda: _execute_requests(
            store,
            job,
            requests,
            contexts,
            adapters=adapters,
            shutdown_check=shutdown_check,
        )
    )


def launch_research(store, *, case_id, persona_id, requirement_id, actor):
    """Atomically enqueue a specific QC requirement in its existing case."""
    from maigret.web.execution_budget import execution_budget_spec
    from maigret.web.pipeline_job_context import build_research_context
    from maigret.web.pipeline_query import build_query_plan
    from maigret.web.pipeline_store import PipelineStore

    pipeline = PipelineStore(store)
    requirement = pipeline.get_requirement(
        requirement_id, case_id=case_id, persona_id=persona_id
    )
    persona = store.get_persona(persona_id)
    if not persona or persona["case_id"] != case_id:
        raise ValueError("Research subject does not belong to the existing case")
    table = pipeline._table("requests")
    with store.engine.connect() as connection:
        row = (
            connection.execute(
                select(table)
                .where(table.c.case_id == case_id, table.c.persona_id == persona_id)
                .order_by(table.c.created_at.desc())
                .limit(1)
            )
            .mappings()
            .first()
        )
        collection_row = (
            connection.execute(
                select(table)
                .where(
                    table.c.case_id == case_id,
                    table.c.persona_id == persona_id,
                    table.c.job_id.is_not(None),
                )
                .order_by(table.c.created_at.desc())
                .limit(1)
            )
            .mappings()
            .first()
        )
    parent = pipeline.get_request(row["id"]) if row else None
    previous_job = store.get_job(collection_row["job_id"]) if collection_row else None
    base_job = previous_job or {
        "case_id": case_id,
        "kind": "research",
        "options": {},
        "usernames": [],
    }
    binding = {
        "persona_id": persona_id,
        "subject_label": persona["display_name"],
        "identifiers": [],
        "usernames": [],
    }
    context = _approved_context(store, base_job, binding)
    context.update(case_id=case_id, persona_id=persona_id, parent_request=parent)
    targeted = build_research_context(requirement, context)
    specification = targeted["investigation_spec"]
    specification["collection_budget"] = targeted["budgets"]
    specification["pipeline_context"] = targeted["context"]
    specification["persona_bindings"] = [
        dict(
            binding,
            identifiers=specification["identifiers"],
            usernames=[
                item["value"]
                for item in specification["identifiers"]
                if item["type"] == "username"
            ],
        )
    ]
    options = dict((previous_job or {}).get("options") or {})
    options.update(investigation_spec=specification, requested_by=actor)
    options.pop("pipeline_source_status", None)
    # A new research requirement starts with the normal bounded server budget;
    # no client can expand the retained requirement's source or run budget.
    budget = execution_budget_spec("focused")
    options.update(execution_budget=budget, execution_mode="focused", all_sites=False)
    job_id = str(uuid.uuid4())
    plan = build_query_plan(
        specification,
        case_id=case_id,
        subject_id=persona_id,
        source_status=source_configuration(),
        context=targeted["context"],
        origin=targeted["origin"],
        budgets=targeted["budgets"],
    )
    now = utcnow()
    with store.engine.begin() as connection:
        connection.execute(
            insert(investigation_jobs).values(
                id=job_id,
                case_id=case_id,
                kind="research",
                status="queued",
                usernames=specification["persona_bindings"][0]["usernames"],
                options=options,
                progress={"checked": 0, "total": len(plan["tasks"]), "found": 0},
                result=None,
                error=None,
                cancel_requested=False,
                attempts=0,
                budget_seconds=budget["total_seconds"],
                budget_policy_version=budget["policy_version"],
                deadline_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        queued = pipeline.create_request_with_connection(
            connection,
            case_id,
            persona_id,
            plan["inputs"],
            plan,
            actor=actor,
            job_id=job_id,
            parent_request_id=targeted["parent_request_id"],
            requirement_ids=[requirement_id],
            idempotency_key="job:" + job_id + ":" + persona_id,
        )
    store.append_event(
        job_id,
        {
            "type": "queued",
            "pipeline_id": "p2-e2e-v1",
            "request_id": queued["id"],
            "requirement_id": requirement_id,
            "case_id": case_id,
            "persona_id": persona_id,
        },
    )
    return {
        "job_id": job_id,
        "request_id": queued["id"],
        "case_id": case_id,
        "persona_id": persona_id,
    }
