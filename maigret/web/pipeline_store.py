"""Durable P2 pipeline ledger and explicit Persona/QC state machine.

All writes are transactional and case scoped. Source observations and curated
versions are immutable database records. Operator judgement and QC are distinct
human actions; collection completion cannot finalize a Persona.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, insert, select, update
from maigret.web.case_store import (
    metadata,
    cases,
    personas,
    investigation_jobs,
    investigation_events,
    persona_claims,
    claim_evidence,
    claim_reviews,
    case_chat_messages,
    _heartbeat_expired,
)
from maigret.web.pipeline_schema import PIPELINE_ID

OUTCOMES = frozenset(
    {
        "found",
        "not_found",
        "candidate",
        "partial",
        "inconclusive",
        "blocked",
        "timeout",
        "error",
        "cancelled",
        "not_executed",
    }
)
TRANSIENT_OUTCOMES = frozenset({"partial", "timeout", "error"})


def task_can_retry(task):
    state = (task.get("spec") or {}).get("_execution") or {}
    return (
        task.get("attempt_count", 0) < task.get("retry_limit", 0) + 1
        and task.get("outcome") in TRANSIENT_OUTCOMES
        and state.get("retryable", task.get("outcome") in {"timeout", "error"})
    )


def collection_status(tasks):
    """Summarize this request only; planned incompatible routes are explicit."""
    active = [task for task in tasks if task.get("availability") == "active"]
    if not active:
        return "research_needed"
    if any(task.get("status") in {"planned", "running"} for task in active):
        return "running"
    outcomes = {task.get("outcome") for task in active}
    if outcomes <= {"found", "candidate", "not_found"}:
        return "completed"
    if outcomes & {"found", "candidate", "not_found", "partial"}:
        return "partial"
    if outcomes == {"cancelled"}:
        return "cancelled"
    return "failed"


def _now():
    return datetime.now(timezone.utc)


def _id():
    return str(uuid.uuid4())


def _json(value):
    return json.loads(
        json.dumps(
            value,
            default=lambda v: (
                (v if v.tzinfo else v.replace(tzinfo=timezone.utc)).isoformat()
                if isinstance(v, datetime)
                else str(v)
            ),
            ensure_ascii=False,
        )
    )


def _digest(value):
    return hashlib.sha256(
        json.dumps(
            _json(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    ).hexdigest()


def _text(value, field, maximum=10000):
    value = str(value or "").strip()
    if not value or len(value) > maximum:
        raise ValueError(f"{field} must contain 1–{maximum} characters")
    return value


def _actor(value):
    value = _text(value, "actor", 200)
    if value.lower().startswith(("worker:", "system:", "model:", "ai:", "connector:")):
        raise PermissionError(
            "Operator/QC actions require an authenticated human actor"
        )
    return value


def _outcome(value):
    value = str(value or "inconclusive").lower().replace(" ", "_").replace("-", "_")
    value = {
        "claimed": "found",
        "unknown": "inconclusive",
        "absent": "not_found",
        "notfound": "not_found",
    }.get(value, value)
    if value not in OUTCOMES:
        raise ValueError(f"Unsupported collection outcome: {value}")
    return value


class PipelineStore:
    def __init__(self, case_store):
        self.case_store = case_store
        self.engine = case_store.engine
        self.tables = {
            name.removeprefix("pipeline_"): table
            for name, table in metadata.tables.items()
            if name.startswith("pipeline_")
        }

    def _table(self, name):
        return self.tables[name]

    def get_subject(self, case_id, persona_id):
        """Identity shell only; loading a page must not load legacy claim history."""
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(personas).where(
                        personas.c.case_id == case_id, personas.c.id == persona_id
                    )
                )
                .mappings()
                .first()
            )
            return _json(dict(row)) if row else None

    def get_case_shell(self, case_id, *, offset=0):
        with self.engine.connect() as connection:
            row = connection.execute(select(cases).where(cases.c.id == case_id)).mappings().first()
            if row is None:
                return None
            result = dict(row)
            result["personas"] = [dict(item) for item in connection.execute(select(personas).where(
                personas.c.case_id == case_id).order_by(personas.c.created_at, personas.c.id)
                .limit(100).offset(max(0, int(offset)))).mappings()]
            result["persona_count"] = connection.scalar(select(func.count()).select_from(personas).where(
                personas.c.case_id == case_id))
            result["offset"] = max(0, int(offset))
            return _json(result)

    def get_request_document(self, case_id, persona_id, request_id):
        """The exact recorded plan, without recursively expanding its history."""
        with self.engine.connect() as connection:
            row = self._row(connection, self._table("requests"), request_id)
            self._check_scope(row, case_id, persona_id)
            return _json(row)

    @staticmethod
    def _row(connection, table, record_id, *, lock=False):
        statement = select(table).where(table.c.id == record_id)
        if lock:
            statement = statement.with_for_update()
        row = connection.execute(statement).mappings().first()
        if row is None:
            raise KeyError(record_id)
        return dict(row)

    def _scope(self, connection, case_id, persona_id, *, lock=False):
        statement = select(personas).where(
            personas.c.id == persona_id, personas.c.case_id == case_id
        )
        if lock:
            statement = statement.with_for_update()
        if connection.execute(statement).first() is None:
            raise KeyError("Persona does not belong to this case")
        state = self._table("persona_state")
        row = (
            connection.execute(select(state).where(state.c.persona_id == persona_id))
            .mappings()
            .first()
        )
        if row is None and lock:
            row = dict(
                persona_id=persona_id,
                case_id=case_id,
                revision=0,
                final_version_id=None,
                final_status=None,
                review_needed=False,
                updated_at=_now(),
            )
            connection.execute(insert(state).values(**row))
        return (
            dict(row)
            if row
            else {"revision": 0, "final_version_id": None, "review_needed": False}
        )

    def _bump(self, connection, persona_id):
        table = self._table("persona_state")
        connection.execute(
            update(table)
            .where(table.c.persona_id == persona_id)
            .values(
                revision=table.c.revision + 1,
                review_needed=table.c.final_version_id.is_not(None),
                updated_at=_now(),
            )
        )

    def _projection_state(self, connection, case_id, persona_id, *, create=False):
        table = self._table("projection_state")
        row = (
            connection.execute(select(table).where(table.c.persona_id == persona_id))
            .mappings()
            .first()
        )
        if row:
            self._check_scope(row, case_id, persona_id)
            return dict(row)
        state = dict(
            case_id=case_id,
            persona_id=persona_id,
            evidence_revision=0,
            projected_revision=0,
            legacy_imported_at=None,
            updated_at=_now(),
        )
        if create:
            connection.execute(insert(table).values(**state))
        return state

    def _invalidate_projection(self, connection, case_id, persona_id):
        """Called under the subject write lock in the evidence transaction."""
        self._projection_state(connection, case_id, persona_id, create=True)
        table = self._table("projection_state")
        connection.execute(
            update(table)
            .where(table.c.persona_id == persona_id)
            .values(evidence_revision=table.c.evidence_revision + 1, updated_at=_now())
        )

    def mark_legacy_imported(self, case_id, persona_id):
        with self.engine.begin() as connection:
            self._scope(connection, case_id, persona_id, lock=True)
            self._projection_state(connection, case_id, persona_id, create=True)
            table = self._table("projection_state")
            connection.execute(
                update(table)
                .where(table.c.persona_id == persona_id)
                .values(legacy_imported_at=_now(), updated_at=_now())
            )

    def _assert_projection_current(self, connection, case_id, persona_id):
        state = self._projection_state(connection, case_id, persona_id)
        if state["evidence_revision"] != state["projected_revision"]:
            raise ValueError(
                "New evidence awaits consolidation. Prepare the working evidence before curating or approving a version."
            )

    def projection_revision(self, case_id, persona_id):
        """Internal full-snapshot writers capture this before reading inputs."""
        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            return self._projection_state(connection, case_id, persona_id)[
                "evidence_revision"
            ]

    @staticmethod
    def _check_scope(row, case_id=None, persona_id=None):
        if (case_id is not None and row["case_id"] != case_id) or (
            persona_id is not None and row["persona_id"] != persona_id
        ):
            raise KeyError("Record does not belong to this case and Persona")

    def create_request(
        self,
        case_id,
        persona_id,
        inputs,
        plan,
        *,
        actor,
        job_id=None,
        request_id=None,
        idempotency_key=None,
        parent_request_id=None,
        requirement_ids=(),
        connection=None,
    ):
        """Persist the exact query plan and all eligible/ineligible route decisions."""
        actor = _text(actor, "request actor", 200)
        inputs, plan = _json(inputs), _json(plan)
        if not isinstance(inputs, list) or not inputs:
            raise ValueError("At least one supported input is required")
        if (
            not isinstance(plan, dict)
            or plan.get("pipeline_id", PIPELINE_ID) != PIPELINE_ID
        ):
            raise ValueError("This release requires the P2 end-to-end pipeline")
        request_id = request_id or plan.get("request_id") or _id()
        if len(request_id) > 36:
            raise ValueError("request_id must be a UUID-sized identifier")
        key = idempotency_key or (
            f"job:{job_id}:{persona_id}" if job_id else request_id
        )
        key = _text(key, "idempotency key", 200)
        task_specs = plan.get("tasks", [])
        if not isinstance(task_specs, list):
            raise ValueError("Plan tasks must be a list")
        table = self._table("requests")
        with (
            self.engine.begin() if connection is None else nullcontext(connection)
        ) as connection:
            if job_id:
                self._row(connection, investigation_jobs, job_id, lock=True)
            self._scope(connection, case_id, persona_id, lock=True)
            existing = (
                connection.execute(
                    select(table).where(
                        table.c.case_id == case_id,
                        table.c.persona_id == persona_id,
                        table.c.idempotency_key == key,
                    )
                )
                .mappings()
                .first()
            )
            if existing:
                if existing["inputs"] != inputs or existing["plan"] != plan:
                    raise ValueError(
                        "Idempotency key already belongs to a different request"
                    )
                return self._request(connection, dict(existing))
            if job_id:
                job = self._row(connection, investigation_jobs, job_id)
                self._check_scope(job, case_id)
            depth = 0
            if parent_request_id:
                parent = self._row(connection, table, parent_request_id)
                self._check_scope(parent, case_id, persona_id)
                depth = parent["depth"] + 1
                if depth > 8:
                    raise ValueError(
                        "Research depth budget exhausted; operator must revise the case research scope"
                    )
            req_rows = []
            for requirement_id in dict.fromkeys(requirement_ids):
                req = self._row(
                    connection, self._table("research_requirements"), requirement_id
                )
                self._check_scope(req, case_id, persona_id)
                if req["status"] not in {"open", "contested"}:
                    raise ValueError(
                        "Only unresolved requirements can generate research"
                    )
                used = connection.scalar(
                    select(func.count())
                    .select_from(self._table("requirement_requests"))
                    .where(
                        self._table("requirement_requests").c.requirement_id
                        == requirement_id
                    )
                )
                if used >= int(req["spec"].get("request_budget", 3)):
                    raise ValueError("Research requirement request budget exhausted")
                req_rows.append(req)
            now = _now()
            data = dict(
                id=request_id,
                case_id=case_id,
                persona_id=persona_id,
                pipeline_id=PIPELINE_ID,
                job_id=job_id,
                parent_request_id=parent_request_id,
                actor=actor,
                inputs=inputs,
                plan=plan,
                plan_hash=_digest(plan),
                idempotency_key=key,
                status="planned",
                depth=depth,
                created_at=now,
            )
            connection.execute(insert(table).values(**data))
            active_count = 0
            for index, spec in enumerate(task_specs):
                availability = str(
                    spec.get("route_state", spec.get("availability", "active"))
                ).lower()
                if availability not in {
                    "active",
                    "conditional",
                    "unavailable",
                    "excluded",
                }:
                    raise ValueError("Every task requires an explicit route state")
                is_active = availability == "active"
                active_count += is_active
                reason = spec.get("reason")
                if not is_active and not reason:
                    raise ValueError("Inactive task requires a visible reason")
                retry = int(spec.get("retry_ceiling", spec.get("retry_limit", 0)))
                if not 0 <= retry <= 10:
                    raise ValueError("Retry ceiling must be between zero and ten")
                task = dict(
                    id=_id(),
                    case_id=case_id,
                    persona_id=persona_id,
                    request_id=request_id,
                    task_key=str(spec.get("task_id", spec.get("task_key", index))),
                    engine=_text(
                        spec.get("engine_id", spec.get("engine")), "engine", 100
                    ),
                    platform=spec.get("platform"),
                    input=spec.get(
                        "input",
                        {
                            "input_id": spec.get("input_id"),
                            "type": spec.get("input_type"),
                            "value": spec.get("input_value"),
                        },
                    ),
                    spec=spec,
                    availability=availability,
                    reason=reason,
                    status="planned" if is_active else "not_executed",
                    outcome=None if is_active else "not_executed",
                    attempt_count=0,
                    retry_limit=retry,
                    active_attempt_id=None,
                    created_at=now,
                    updated_at=now,
                )
                connection.execute(insert(self._table("tasks")).values(**task))
            if not active_count:
                data["status"] = "research_needed"
                connection.execute(
                    update(table)
                    .where(table.c.id == request_id)
                    .values(status="research_needed")
                )
            for req in req_rows:
                connection.execute(
                    insert(self._table("requirement_requests")).values(
                        case_id=case_id,
                        persona_id=persona_id,
                        requirement_id=req["id"],
                        request_id=request_id,
                    )
                )
            self._bump(connection, persona_id)
            return self._request(connection, data)

    def _request(self, connection, row):
        tasks = self._table("tasks")
        result = dict(row)
        result["tasks"] = [
            dict(r)
            for r in connection.execute(
                select(tasks)
                .where(tasks.c.request_id == row["id"])
                .order_by(tasks.c.created_at, tasks.c.task_key)
            ).mappings()
        ]
        attempts = self._table("attempts")
        for task in result["tasks"]:
            task["retry_remaining"] = max(
                0, task["retry_limit"] + 1 - task["attempt_count"]
            )
            task["retry_exhausted"] = (
                task["outcome"] in TRANSIENT_OUTCOMES and task["retry_remaining"] == 0
            )
            task["attempts"] = [
                dict(row)
                for row in connection.execute(
                    select(attempts)
                    .where(attempts.c.task_id == task["id"])
                    .order_by(attempts.c.number)
                ).mappings()
            ]
        return _json(result)

    def get_request(self, request_id, *, case_id=None, persona_id=None):
        with self.engine.connect() as connection:
            row = self._row(connection, self._table("requests"), request_id)
            self._check_scope(row, case_id, persona_id)
            return self._request(connection, row)

    def requests_for_job(self, job_id):
        with self.engine.connect() as connection:
            table = self._table("requests")
            return [
                self._request(connection, dict(row))
                for row in connection.execute(
                    select(table).where(table.c.job_id == job_id)
                ).mappings()
            ]

    list_requests_for_job = requests_for_job

    def create_request_with_connection(self, connection, *args, **kwargs):
        return self.create_request(*args, connection=connection, **kwargs)

    def get_task(self, task_id):
        with self.engine.connect() as connection:
            return _json(self._row(connection, self._table("tasks"), task_id))

    def get_attempt(self, attempt_id):
        with self.engine.connect() as connection:
            return _json(self._row(connection, self._table("attempts"), attempt_id))

    def update_request_status(self, request_id, status):
        if status not in {
            "planned",
            "running",
            "completed",
            "partial",
            "research_needed",
            "interrupted",
            "failed",
            "cancelled",
        }:
            raise ValueError("Invalid pipeline request status")
        with self.engine.begin() as connection:
            request = self._row(
                connection, self._table("requests"), request_id, lock=True
            )
            if status == "completed":
                tasks = self._table("tasks")
                if connection.scalar(
                    select(func.count())
                    .select_from(tasks)
                    .where(
                        tasks.c.request_id == request_id,
                        tasks.c.status.in_(["planned", "running"]),
                    )
                ):
                    raise ValueError("Request has unfinished collection tasks")
            connection.execute(
                update(self._table("requests"))
                .where(self._table("requests").c.id == request_id)
                .values(status=status)
            )
            request["status"] = status
            return self._request(connection, request)

    def iter_observations(self, case_id, persona_id, *, batch_size=1000):
        """Iterate normalized source documents with bounded database buffering."""
        table = self._table("observations")
        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            statement = (
                select(table.c.payload)
                .where(table.c.case_id == case_id, table.c.persona_id == persona_id)
                .order_by(table.c.created_at, table.c.id)
            )
            result = connection.execution_options(
                stream_results=True, yield_per=batch_size
            ).execute(statement)
            for row in result:
                yield _json(row[0])

    def _lock_request_scope(self, connection, request_id):
        """Serialize lifecycle writes in job, subject, request, task order.

        Re-read locked rows before fencing; an earlier plain read is only a lookup.
        Keeping the same order for finish and cancellation prevents lock inversion.
        """
        request = self._row(connection, self._table("requests"), request_id)
        job = (
            self._row(connection, investigation_jobs, request["job_id"], lock=True)
            if request["job_id"]
            else None
        )
        self._scope(connection, request["case_id"], request["persona_id"], lock=True)
        request = self._row(connection, self._table("requests"), request_id, lock=True)
        return request, job

    def _fence_job(self, connection, task, worker_id):
        request = self._row(connection, self._table("requests"), task["request_id"])
        if not request["job_id"]:
            return
        job = self._row(connection, investigation_jobs, request["job_id"], lock=True)
        if (
            not worker_id
            or job["status"] != "running"
            or job["worker_id"] != worker_id
            or job["cancel_requested"]
            or _heartbeat_expired(job["heartbeat_at"], now=_now())
        ):
            raise ValueError("Stale worker: linked investigation lease is not active")

    def start_attempt(self, task_id, worker_id, *, resume=False, connection=None):
        """Fence prior attempt before explicitly recovering an interrupted task."""
        worker_id = _text(worker_id, "worker", 200)
        tasks, attempts = self._table("tasks"), self._table("attempts")
        with (
            self.engine.begin() if connection is None else nullcontext(connection)
        ) as connection:
            task = self._row(connection, tasks, task_id)
            self._lock_request_scope(connection, task["request_id"])
            task = self._row(connection, tasks, task_id, lock=True)
            self._fence_job(connection, task, worker_id)
            if task["availability"] != "active":
                raise ValueError("Task is not an active compatible route")
            if task["attempt_count"] >= task["retry_limit"] + 1:
                raise ValueError("Task retry budget exhausted")
            if (
                task["status"] == "completed"
                and not task_can_retry(task)
            ):
                raise ValueError(
                    "Completed substantive task cannot be replayed as a new attempt"
                )
            if task["status"] in {"cancelled", "not_executed"}:
                raise ValueError("Terminal task cannot be resumed")
            if task["active_attempt_id"]:
                if not resume:
                    raise ValueError("Task already has an active attempt")
                prior = self._row(connection, attempts, task["active_attempt_id"])
                connection.execute(
                    update(attempts)
                    .where(attempts.c.id == prior["id"])
                    .values(
                        status="completed",
                        outcome="error",
                        error="Worker lease interrupted; superseded by explicit recovery",
                        finished_at=_now(),
                    )
                )
            attempt = dict(
                id=_id(),
                case_id=task["case_id"],
                persona_id=task["persona_id"],
                task_id=task_id,
                number=task["attempt_count"] + 1,
                worker_id=worker_id,
                status="running",
                outcome=None,
                error=None,
                finished_at=None,
                created_at=_now(),
            )
            connection.execute(insert(attempts).values(**attempt))
            connection.execute(
                update(tasks)
                .where(tasks.c.id == task_id)
                .values(
                    status="running",
                    outcome=None,
                    attempt_count=attempt["number"],
                    active_attempt_id=attempt["id"],
                    updated_at=_now(),
                )
            )
            connection.execute(
                update(self._table("requests"))
                .where(self._table("requests").c.id == task["request_id"])
                .values(status="running")
            )
            return _json(attempt)

    def _attempt_context(
        self, connection, attempt_id, worker_id=None, allow_completed=False
    ):
        attempt = self._row(connection, self._table("attempts"), attempt_id)
        task = self._row(connection, self._table("tasks"), attempt["task_id"])
        self._lock_request_scope(connection, task["request_id"])
        task = self._row(
            connection, self._table("tasks"), attempt["task_id"], lock=True
        )
        attempt = self._row(connection, self._table("attempts"), attempt_id)
        if worker_id is not None and attempt["worker_id"] != worker_id:
            raise ValueError("Stale worker cannot write this attempt")
        if attempt["status"] != "running" or task["active_attempt_id"] != attempt_id:
            if not (
                allow_completed
                and attempt["status"] == "completed"
                and task["attempt_count"] == attempt["number"]
            ):
                raise ValueError("Stale or completed attempt cannot write evidence")
        if attempt["status"] == "running":
            self._fence_job(connection, task, worker_id)
        self._scope(connection, task["case_id"], task["persona_id"], lock=True)
        return attempt, task

    def _append_observations(self, connection, attempt, task, observations):
        from maigret.web.pipeline_evidence import enforce_observation_retention

        table = self._table("observations")
        added, results = 0, []
        for raw in observations:
            raw = _json(raw)
            if not isinstance(raw, dict):
                raise ValueError("Observation must be an object")
            for field, expected in (
                ("case_id", task["case_id"]),
                ("persona_id", task["persona_id"]),
                ("subject_id", task["persona_id"]),
                ("request_id", task["request_id"]),
                ("task_id", task["id"]),
                ("attempt_id", attempt["id"]),
            ):
                if raw.get(field) and raw[field] != expected:
                    raise ValueError(
                        f"Observation {field} is outside its attempt scope"
                    )
            raw = enforce_observation_retention(
                {**raw, "engine": raw.get("engine") or task["engine"]},
                policy=[
                    (task.get("spec") or {}).get("retention"),
                    'metadata_only' if 'google_places' in task['engine'].replace('-', '_') else None,
                ],
            )
            key = str(
                raw.get("observation_key")
                or raw.get("id")
                or raw.get("native_record_id")
                or _digest(raw)
            )
            existing = (
                connection.execute(
                    select(table).where(
                        table.c.attempt_id == attempt["id"],
                        table.c.observation_key == key,
                    )
                )
                .mappings()
                .first()
            )
            content_hash = _digest(raw)
            if existing:
                if existing["content_hash"] != content_hash:
                    raise ValueError("Observation replay changed immutable content")
                results.append(dict(existing))
                continue
            if attempt["status"] != "running":
                raise ValueError("Completed attempt cannot receive new evidence")
            if (
                raw.get("original_observation_id")
                and task["engine"] != "retained_evidence_reuse"
            ):
                raise ValueError(
                    "Original observation links require explicit scoped evidence reuse"
                )
            retention = raw.get("retention", {})
            retained = raw.get("retained", True)
            if isinstance(retention, dict):
                retained = bool(
                    retention.get("retainable", retention.get("retained", retained))
                )
                if retention.get("mode") in {
                    "metadata_only",
                    "transient",
                    "live_only",
                    "prohibited",
                }:
                    retained = False
            elif retention in {"metadata_only", "transient", "live_only", "prohibited"}:
                retained = False
            source_url = raw.get("source_url") or raw.get("url")
            row = dict(
                id=_id(),
                case_id=task["case_id"],
                persona_id=task["persona_id"],
                request_id=task["request_id"],
                task_id=task["id"],
                attempt_id=attempt["id"],
                observation_key=key,
                engine=task["engine"],
                outcome=_outcome(raw.get("status", raw.get("outcome", "candidate"))),
                source_url=source_url,
                canonical_url=raw.get("canonical_url", source_url),
                origin_family=raw.get("origin_family_id", raw.get("origin_family")),
                content_hash=content_hash,
                retained=retained,
                original_observation_id=raw.get("original_observation_id"),
                artifact_ref=raw.get("artifact_ref"),
                payload=raw,
                created_at=_now(),
            )
            connection.execute(insert(table).values(**row))
            results.append(row)
            added += 1
        if added:
            self._invalidate_projection(connection, task["case_id"], task["persona_id"])
            self._bump(connection, task["persona_id"])
            request = self._row(connection, self._table("requests"), task["request_id"])
            if request["job_id"]:
                connection.execute(
                    insert(investigation_events).values(
                        job_id=request["job_id"],
                        event={
                            "type": "pipeline_evidence_committed",
                            "pipeline_id": PIPELINE_ID,
                            "request_id": request["id"],
                            "task_id": task["id"],
                            "attempt_id": attempt["id"],
                            "count": added,
                        },
                        created_at=_now(),
                    )
                )
        return results

    def append_observations(self, attempt_id, observations, *, worker_id=None):
        with self.engine.begin() as connection:
            if self.engine.dialect.name == "sqlite":
                connection.exec_driver_sql("BEGIN IMMEDIATE")
            attempt, task = self._attempt_context(
                connection, attempt_id, worker_id, allow_completed=True
            )
            return _json(
                self._append_observations(connection, attempt, task, observations)
            )

    def _finish_attempt(self, connection, attempt, task, outcome, error=None, retry_state=None):
        outcome = _outcome(outcome)
        if attempt["status"] == "completed":
            if attempt["outcome"] != outcome:
                raise ValueError("Attempt already completed with another outcome")
            return _json(task)
        now = _now()
        connection.execute(
            update(self._table("attempts"))
            .where(self._table("attempts").c.id == attempt["id"])
            .values(
                status="completed",
                outcome=outcome,
                error=str(error)[:10000] if error else None,
                finished_at=now,
            )
        )
        values = dict(
            status="cancelled" if outcome == "cancelled" else "completed",
            outcome=outcome,
            active_attempt_id=None,
            updated_at=now,
        )
        if retry_state is not None:
            values["spec"] = dict(task.get("spec") or {}, _execution=retry_state)
        connection.execute(
            update(self._table("tasks"))
            .where(self._table("tasks").c.id == task["id"])
            .values(**values)
        )
        task.update(values)
        tasks = self._table("tasks")
        pending = connection.scalar(
            select(func.count())
            .select_from(tasks)
            .where(
                tasks.c.request_id == task["request_id"],
                tasks.c.status.in_(["planned", "running"]),
            )
        )
        if not pending:
            request_tasks = list(connection.execute(select(tasks).where(
                tasks.c.request_id == task["request_id"]
            )).mappings())
            connection.execute(
                update(self._table("requests"))
                .where(self._table("requests").c.id == task["request_id"])
                .values(status=collection_status(request_tasks))
            )
        return _json(task)

    def finish_attempt(self, attempt_id, outcome, *, worker_id=None, error=None, retry_state=None):
        with self.engine.begin() as connection:
            attempt, task = self._attempt_context(
                connection, attempt_id, worker_id, allow_completed=True
            )
            return self._finish_attempt(connection, attempt, task, outcome, error, retry_state)

    def record_observations(
        self, attempt_id, observations, *, outcome, error=None, worker_id=None
    ):
        with self.engine.begin() as connection:
            attempt, task = self._attempt_context(
                connection, attempt_id, worker_id, allow_completed=True
            )
            records = self._append_observations(connection, attempt, task, observations)
            result = self._finish_attempt(connection, attempt, task, outcome, error)
            result["observations"] = _json(records)
            return result

    def interrupt_request(
        self, request_id, *, worker_id, reason="Worker shutdown interrupted collection"
    ):
        """Finish active attempts without treating an interrupted job as user stop.

        Planned tasks remain eligible. Interrupted attempts consume their existing
        retry budget; exhausted sources require a new explicitly linked request.
        This never hides already committed observations or replenishes budgets.
        """
        with self.engine.begin() as connection:
            request, locked_job = self._lock_request_scope(connection, request_id)
            tasks = self._table("tasks")
            if request["job_id"]:
                job = self._row(
                    connection, investigation_jobs, request["job_id"], lock=True
                )
                if (
                    job["status"] != "running"
                    or job["worker_id"] != worker_id
                    or job["cancel_requested"]
                    or _heartbeat_expired(job["heartbeat_at"], now=_now())
                ):
                    raise ValueError("Stale worker cannot interrupt this request")
            for row in connection.execute(
                select(tasks)
                .where(tasks.c.request_id == request_id, tasks.c.status == "running")
                .with_for_update()
            ).mappings():
                task = dict(row)
                attempt = self._row(
                    connection, self._table("attempts"), task["active_attempt_id"]
                )
                if attempt["worker_id"] != worker_id:
                    raise ValueError("Stale worker cannot interrupt another attempt")
                diagnostic = reason
                if task["attempt_count"] >= task["retry_limit"] + 1:
                    diagnostic += "; retry budget exhausted, create a linked research request to continue this source"
                self._finish_attempt(connection, attempt, task, "error", diagnostic)
                connection.execute(
                    update(tasks)
                    .where(tasks.c.id == task["id"])
                    .values(reason=diagnostic)
                )
            connection.execute(
                update(self._table("requests"))
                .where(self._table("requests").c.id == request_id)
                .values(status="interrupted")
            )
            return self._request(
                connection, self._row(connection, self._table("requests"), request_id)
            )

    def reconcile_interrupted_job(self, connection, job, *, reason):
        """Fence all abandoned attempts inside the locked job transaction.

        This is called by the lease watchdog, never by an expired collector.
        Successful tasks, observations, request IDs and retry counts survive.
        """
        requests, tasks = self._table("requests"), self._table("tasks")
        stopped = bool(job.get("cancel_requested") or job.get("status") == "cancel_requested")
        for row in connection.execute(select(requests).where(
            requests.c.job_id == job["id"]
        ).order_by(requests.c.persona_id, requests.c.id)).mappings():
            request = dict(row)
            if request["status"] not in {"planned", "running", "interrupted"}:
                continue
            self._lock_request_scope(connection, request["id"])
            for source in connection.execute(select(tasks).where(
                tasks.c.request_id == request["id"],
                tasks.c.status.in_(["planned", "running"]),
            ).with_for_update()).mappings():
                task = dict(source)
                if task["active_attempt_id"]:
                    attempt = self._row(connection, self._table("attempts"), task["active_attempt_id"])
                    self._finish_attempt(
                        connection, attempt, task, "cancelled" if stopped else "error",
                        reason, {"retryable": not stopped, "interrupted": True},
                    )
                elif stopped:
                    connection.execute(update(tasks).where(tasks.c.id == task["id"]).values(
                        status="cancelled", outcome="cancelled", reason=reason, updated_at=_now()
                    ))
            connection.execute(update(requests).where(requests.c.id == request["id"]).values(
                status="cancelled" if stopped else "interrupted"
            ))

    def resume_interrupted_request(self, request_id, *, actor, case_id, persona_id):
        """Explicitly requeue the same job without replenishing any budget."""
        actor = _actor(actor)
        from maigret.web.execution_budget import ExecutionBudget

        with self.engine.begin() as connection:
            # Lock the job before acquiring every subject scope in sorted order.
            # Locking the selected subject first would invert multi-subject order.
            request = self._row(connection, self._table("requests"), request_id)
            self._check_scope(request, case_id, persona_id)
            job = self._row(connection, investigation_jobs, request["job_id"], lock=True) if request["job_id"] else None
            if not job or job["status"] != "interrupted" or job["cancel_requested"]:
                raise ValueError("Only an interrupted, uncancelled job can be resumed")
            if ExecutionBudget.from_job(job).is_exhausted():
                raise ValueError("The original time budget expired; run new research in this case")
            requests, tasks = self._table("requests"), self._table("tasks")
            eligible = []
            for row in connection.execute(select(requests).where(
                requests.c.job_id == job["id"]
            ).order_by(requests.c.persona_id, requests.c.id)).mappings():
                self._lock_request_scope(connection, row["id"])
                request_tasks = list(connection.execute(select(tasks).where(
                    tasks.c.request_id == row["id"]
                ).with_for_update()).mappings())
                if any(task["status"] == "running" or task["active_attempt_id"] for task in request_tasks):
                    raise ValueError("An active attempt must be reconciled before resuming")
                def eligible_now(task):
                    retry_at = ((task.get("spec") or {}).get("_execution") or {}).get("next_retry_at")
                    if retry_at and datetime.fromisoformat(retry_at) > _now():
                        return False
                    return task["availability"] == "active" and (
                        task["status"] == "planned" or task_can_retry(task)
                    )
                if any(eligible_now(task) for task in request_tasks):
                    eligible.append(row["id"])
            if not eligible:
                raise ValueError("No retry is currently available. Check the recorded retry time and remaining budget; otherwise run new research in this case")
            connection.execute(update(requests).where(requests.c.id.in_(eligible)).values(status="planned"))
            connection.execute(update(investigation_jobs).where(investigation_jobs.c.id == job["id"]).values(
                status="queued", worker_id=None, heartbeat_at=None, completed_at=None,
                error=None, updated_at=_now(),
            ))
            connection.execute(insert(investigation_events).values(
                job_id=job["id"],
                event={"type": "pipeline_resumed", "actor": actor, "request_ids": eligible,
                         "message": "Resume requested; committed evidence and original budgets retained"},
                created_at=_now(),
            ))
            return {"job_id": job["id"], "request_ids": eligible, "status": "queued"}

    def cancel_request(
        self, request_id, *, reason="Operator stopped research", worker_id=None
    ):
        with self.engine.begin() as connection:
            request, locked_job = self._lock_request_scope(connection, request_id)
            if worker_id is not None and request["job_id"]:
                job = self._row(
                    connection, investigation_jobs, request["job_id"], lock=True
                )
                if (
                    job["status"] not in {"running", "cancel_requested"}
                    or job["worker_id"] != worker_id
                    or _heartbeat_expired(job["heartbeat_at"], now=_now())
                ):
                    raise ValueError("Stale worker cannot cancel this request")
            tasks = self._table("tasks")
            for row in connection.execute(
                select(tasks)
                .where(
                    tasks.c.request_id == request_id,
                    tasks.c.status.in_(["planned", "running"]),
                )
                .with_for_update()
            ).mappings():
                task = dict(row)
                if task["active_attempt_id"]:
                    attempt = self._row(
                        connection, self._table("attempts"), task["active_attempt_id"]
                    )
                    self._finish_attempt(connection, attempt, task, "cancelled", reason)
                else:
                    connection.execute(
                        update(tasks)
                        .where(tasks.c.id == task["id"])
                        .values(
                            status="cancelled",
                            outcome="cancelled",
                            reason=reason,
                            updated_at=_now(),
                        )
                    )
            connection.execute(
                update(self._table("requests"))
                .where(self._table("requests").c.id == request_id)
                .values(status="cancelled")
            )
            return self._request(
                connection, self._row(connection, self._table("requests"), request_id)
            )

    def list_observations(
        self, case_id, persona_id, *, request_id=None, limit=100, offset=0
    ):
        table = self._table("observations")
        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            statement = select(table).where(
                table.c.case_id == case_id, table.c.persona_id == persona_id
            )
            if request_id:
                request = self._row(connection, self._table("requests"), request_id)
                self._check_scope(request, case_id, persona_id)
                statement = statement.where(table.c.request_id == request_id)
            return _json(
                [
                    dict(row)
                    for row in connection.execute(
                        statement.order_by(table.c.created_at, table.c.id)
                        .limit(min(max(int(limit), 1), 5000))
                        .offset(max(int(offset), 0))
                    ).mappings()
                ]
            )

    def upsert_groups(
        self,
        case_id,
        persona_id,
        consolidated,
        *,
        assessments=None,
        connection=None,
        projection_revision=None,
    ):
        """Materialize canonical groups without modifying any source observation.

        Groups require observation_ids referring to persisted observation ids or
        their original normalized ids. All supplied memberships are case checked.
        """
        groups, memberships, observations = (
            self._table("groups"),
            self._table("group_observations"),
            self._table("observations"),
        )
        result, changed = [], False
        with (
            self.engine.begin() if connection is None else nullcontext(connection)
        ) as connection:
            self._scope(connection, case_id, persona_id, lock=True)
            if projection_revision is not None:
                state = self._projection_state(
                    connection, case_id, persona_id, create=True
                )
                if projection_revision != state["evidence_revision"]:
                    raise ValueError(
                        "Evidence changed while preparing the projection; rebuild from current evidence."
                    )
            for kind, collection in (
                ("account", consolidated.get("accounts", [])),
                ("claim", consolidated.get("claims", [])),
            ):
                for source in collection:
                    source = _json(source)
                    canonical_key = str(
                        source.get("canonical_key")
                        or source.get("key")
                        or source.get("id")
                        or _digest(source)
                    )
                    if len(canonical_key) != 64:
                        canonical_key = _digest(canonical_key)
                    normalized = source.get("normalized") or {
                        key: value
                        for key, value in source.items()
                        if key
                        not in {
                            "observations",
                            "observation_ids",
                            "assessment",
                            "origin_by_observation",
                            "observed_times",
                        }
                    }
                    row = (
                        connection.execute(
                            select(groups).where(
                                groups.c.case_id == case_id,
                                groups.c.persona_id == persona_id,
                                groups.c.kind == kind,
                                groups.c.canonical_key == canonical_key,
                            )
                        )
                        .mappings()
                        .first()
                    )
                    if not row:
                        row = dict(
                            id=_id(),
                            case_id=case_id,
                            persona_id=persona_id,
                            kind=kind,
                            canonical_key=canonical_key,
                            normalized=normalized,
                            created_at=_now(),
                        )
                        connection.execute(insert(groups).values(**row))
                        changed = True
                    else:
                        row = dict(row)
                    ids = source.get("observation_ids") or [
                        item.get("id") for item in source.get("observations", [])
                    ]
                    ids = list(dict.fromkeys(ids))
                    candidates = {}
                    for position in range(0, len(ids), 400):
                        batch = ids[position : position + 400]
                        for observation in connection.execute(
                            select(
                                observations.c.id, observations.c.observation_key
                            ).where(
                                observations.c.case_id == case_id,
                                observations.c.persona_id == persona_id,
                                observations.c.id.in_(batch)
                                | observations.c.observation_key.in_(batch),
                            )
                        ).mappings():
                            for key in (
                                observation["id"],
                                observation["observation_key"],
                            ):
                                candidates.setdefault(key, set()).add(observation["id"])
                    if any(observation_id not in candidates for observation_id in ids):
                        raise ValueError(
                            "Group evidence does not belong to this case and Persona"
                        )
                    existing_ids = set(
                        connection.scalars(
                            select(memberships.c.observation_id).where(
                                memberships.c.group_id == row["id"]
                            )
                        )
                    )
                    new_ids = sorted(
                        {value for key in ids for value in candidates[key]}
                        - existing_ids
                    )
                    for position in range(0, len(new_ids), 500):
                        connection.execute(
                            insert(memberships),
                            [
                                dict(
                                    case_id=case_id,
                                    persona_id=persona_id,
                                    group_id=row["id"],
                                    observation_id=value,
                                    created_at=_now(),
                                )
                                for value in new_ids[position : position + 500]
                            ],
                        )
                    changed = changed or bool(new_ids)
                    assessment_table = self._table("assessments")
                    assessment = None
                    if isinstance(assessments, dict):
                        assessment = assessments.get(
                            source.get("canonical_key"),
                            assessments.get(source.get("id")),
                        )
                    elif isinstance(assessments, list):
                        assessment = next(
                            (
                                value
                                for value in assessments
                                if value.get("group_id", value.get("hypothesis_id"))
                                == source.get("id")
                            ),
                            None,
                        )
                    snapshot = {"consolidated": source, "assessment": assessment}
                    evidence_hash = _digest(snapshot)
                    assessment_id = connection.scalar(
                        select(assessment_table.c.id).where(
                            assessment_table.c.group_id == row["id"],
                            assessment_table.c.evidence_hash == evidence_hash,
                        )
                    )
                    if not assessment_id:
                        assessment_id = _id()
                        connection.execute(
                            insert(assessment_table).values(
                                id=assessment_id,
                                case_id=case_id,
                                persona_id=persona_id,
                                group_id=row["id"],
                                evidence_hash=evidence_hash,
                                document=snapshot,
                                created_at=_now(),
                            )
                        )
                        changed = True
                    self._refresh_group_summary(connection, row)
                    result.append(row)
            if changed:
                self._bump(connection, persona_id)
            # Only a trusted full-snapshot writer may acknowledge its captured
            # revision. Partial upserts never establish projection completeness.
            if projection_revision is not None:
                state = self._table("projection_state")
                connection.execute(
                    update(state)
                    .where(state.c.persona_id == persona_id)
                    .values(projected_revision=projection_revision, updated_at=_now())
                )
        return _json(result)

    materialize_groups = upsert_groups

    def _refresh_group_summary(self, connection, row):
        """Compute a bounded list projection on writes, including operator splits."""
        expanded = self._group(connection, row, limit=3)
        assessment = expanded.get("prior_assessment") or expanded.get("assessment")
        compact = (
            None
            if assessment is None
            else {
                key: value
                for key, value in assessment.items()
                if key
                not in {
                    "origin_families",
                    "unknown_origin_observation_ids",
                    "freshness",
                    "contradictions",
                    "group_conflicts",
                }
            }
        )
        if compact is not None:
            compact.update(
                detail_available=True,
                contradiction_count=len(assessment.get("contradictions", [])),
                group_conflict_count=len(assessment.get("group_conflicts", [])),
            )
        revisions = expanded.get("grouping_revisions", [])
        changed_at = [
            revision["created_at"]
            for revision in revisions
            if revision["action"] in {"split", "bind"}
        ]
        values = dict(
            case_id=row["case_id"],
            persona_id=row["persona_id"],
            normalized=expanded["normalized"],
            assessment=compact,
            assessment_id=expanded.get("assessment_id"),
            observations=_json(expanded["observations"]),
            observation_count=expanded["observation_count"],
            revision_count=len(revisions),
            latest_grouping_at=max(changed_at) if changed_at else None,
            updated_at=_now(),
        )
        table = self._table("group_summaries")
        if connection.scalar(
            select(table.c.group_id).where(table.c.group_id == row["id"])
        ):
            connection.execute(
                update(table).where(table.c.group_id == row["id"]).values(**values)
            )
        else:
            connection.execute(insert(table).values(group_id=row["id"], **values))

    def _group_summary(self, connection, row):
        table = self._table("group_summaries")
        summary = (
            connection.execute(select(table).where(table.c.group_id == row["id"]))
            .mappings()
            .first()
        )
        result = dict(row)
        if not summary:
            result.update(
                assessment=None,
                observations=[],
                observation_count=0,
                projection_pending=True,
                grouping_revision_count=0,
            )
        else:
            result.update(
                normalized=summary["normalized"],
                assessment=summary["assessment"],
                assessment_id=summary["assessment_id"],
                observations=summary["observations"],
                observation_count=summary["observation_count"],
                grouping_revision_count=summary["revision_count"],
            )
        decisions = self._table("operator_decisions")
        decision_rows = [
            dict(item)
            for item in connection.execute(
                select(decisions)
                .where(decisions.c.group_id == row["id"])
                .order_by(decisions.c.sequence.desc())
                .limit(3)
            ).mappings()
        ]
        decision = decision_rows[0] if decision_rows else None
        assessment = result.get("assessment") or {}
        probability = assessment.get("probability") or {}
        if probability.get("value") is not None:
            from maigret.web.pipeline_probability import timestamp
            expires = timestamp(probability.get("expires_at"))
            if expires is None or expires <= _now():
                result["prior_assessment"] = assessment
                result["assessment"] = dict(assessment, probability={
                    "value": None,
                    "reason": "probability_review_expired_or_undated",
                })
        if decision and decision["details"].get("evidence_dispositions"):
            result["prior_assessment"] = result.get("assessment")
            result["assessment"] = {
                "probability": {
                    "value": None,
                    "reason": "operator_evidence_disposition_requires_reassessment",
                },
                "operator_review_available": True,
            }
        revisions = self._table("group_revisions")
        # Detail pages expose full records; list pages fetch only a bounded
        # audit summary, never potentially large split observation-ID arrays.
        result["grouping_revisions"] = [
            dict(item)
            for item in connection.execute(
                select(*(column for column in revisions.c if column.name != "details"))
                .where(revisions.c.group_id == row["id"])
                .order_by(revisions.c.created_at.desc(), revisions.c.id.desc())
                .limit(3)
            ).mappings()
        ]
        result.update(
            latest_decision=decision,
            decisions=decision_rows,
            decision_count=decision["sequence"] if decision else 0,
            decision_stale=bool(
                decision
                and summary
                and summary["latest_grouping_at"]
                and _json(summary["latest_grouping_at"]) > _json(decision["created_at"])
            ),
        )
        return result

    def _group(
        self,
        connection,
        row,
        *,
        limit=100,
        offset=0,
        include_assessed_group=False,
        history_limit=None,
        history_offset=0,
    ):
        memberships, observations, decisions = (
            self._table("group_observations"),
            self._table("observations"),
            self._table("operator_decisions"),
        )
        result = dict(row)
        assessment_table = self._table("assessments")
        latest = (
            connection.execute(
                select(assessment_table)
                .where(assessment_table.c.group_id == row["id"])
                .order_by(
                    assessment_table.c.created_at.desc(), assessment_table.c.id.desc()
                )
                .limit(1)
            )
            .mappings()
            .first()
        )
        if latest:
            source = latest["document"]["consolidated"]
            if include_assessed_group:
                result["assessed_group"] = source
            result["normalized"] = source.get("normalized") or {
                key: value
                for key, value in source.items()
                if key
                not in {
                    "observations",
                    "observation_ids",
                    "assessment",
                    "origin_by_observation",
                    "observed_times",
                }
            }
            result["assessment"] = latest["document"].get("assessment")
            result["assessment_id"] = latest["id"]
        revisions = self._table("group_revisions")
        result["grouping_revisions"] = [
            dict(item)
            for item in connection.execute(
                select(revisions)
                .where(revisions.c.group_id == row["id"])
                .order_by(revisions.c.created_at, revisions.c.id)
            ).mappings()
        ]
        moved_ids = {
            oid
            for revision in result["grouping_revisions"]
            if revision["action"] == "split"
            for oid in revision["details"].get("observation_ids", [])
        }
        bindings = [
            revision
            for revision in result["grouping_revisions"]
            if revision["action"] == "bind"
        ]
        if bindings:
            result["normalized"] = dict(
                result["normalized"],
                binding_target_persona_id=bindings[-1]["details"]["target_persona_id"],
            )
        if moved_ids or result["normalized"].get("operator_split") or bindings:
            result["assessment"] = {
                "probability": {
                    "value": None,
                    "reason": "Operator grouping revision requires reassessment",
                },
                "operator_review_available": True,
            }
        statement = (
            select(observations)
            .join(memberships, memberships.c.observation_id == observations.c.id)
            .where(memberships.c.group_id == row["id"])
        )
        count_statement = (
            select(func.count())
            .select_from(memberships)
            .where(memberships.c.group_id == row["id"])
        )
        if moved_ids:
            statement = statement.where(observations.c.id.not_in(moved_ids))
            count_statement = count_statement.where(
                memberships.c.observation_id.not_in(moved_ids)
            )
        result["observation_count"] = connection.scalar(count_statement)
        result["observations"] = [
            dict(item)
            for item in connection.execute(
                statement.order_by(observations.c.created_at, observations.c.id)
                .limit(limit)
                .offset(offset)
            ).mappings()
        ]
        decision = (
            connection.execute(
                select(decisions)
                .where(decisions.c.group_id == row["id"])
                .order_by(decisions.c.sequence.desc())
                .limit(1)
            )
            .mappings()
            .first()
        )
        result["latest_decision"] = dict(decision) if decision else None
        if decision and decision["details"].get("evidence_dispositions"):
            dispositions = {
                item["observation_id"]: item
                for item in decision["details"]["evidence_dispositions"]
            }
            for observation in result["observations"]:
                disposition = dispositions.get(observation["id"])
                if (
                    disposition
                    and disposition["reviewed_content_hash"]
                    == observation["content_hash"]
                ):
                    observation["operator_disposition"] = disposition
            result["prior_assessment"] = result.get("assessment")
            result["assessment"] = {
                "probability": {
                    "value": None,
                    "reason": "operator_evidence_disposition_requires_reassessment",
                },
                "operator_review_available": True,
            }
        latest_grouping = [
            revision
            for revision in result["grouping_revisions"]
            if revision["action"] in {"split", "bind"}
        ]
        result["decision_stale"] = bool(
            decision
            and latest_grouping
            and max(_json(revision["created_at"]) for revision in latest_grouping)
            > _json(decision["created_at"])
        )
        result["decision_count"] = connection.scalar(
            select(func.count())
            .select_from(decisions)
            .where(decisions.c.group_id == row["id"])
        )
        result["decisions"] = [
            dict(item)
            for item in connection.execute(
                select(decisions)
                .where(decisions.c.group_id == row["id"])
                .order_by(
                    decisions.c.sequence
                    if history_limit is None
                    else decisions.c.sequence.desc()
                )
                .limit(history_limit)
                .offset(history_offset)
            ).mappings()
        ]
        if history_limit is not None:
            result["grouping_revision_count"] = len(result["grouping_revisions"])
            result["grouping_revisions"] = list(reversed(result["grouping_revisions"]))[
                history_offset : history_offset + history_limit
            ]
        return result

    def iter_included_groups(self, case_id, persona_id, *, batch_size=500):
        """Yield latest explicitly included hypotheses as research targeting context."""
        groups, decisions = self._table("groups"), self._table("operator_decisions")
        latest = (
            select(
                decisions.c.group_id, func.max(decisions.c.sequence).label("sequence")
            )
            .group_by(decisions.c.group_id)
            .subquery()
        )
        statement = (
            select(
                groups,
                decisions.c.id.label("decision_id"),
                decisions.c.details.label("decision_details"),
                decisions.c.actor.label("decision_actor"),
                decisions.c.reason.label("decision_reason"),
            )
            .join(latest, latest.c.group_id == groups.c.id)
            .join(
                decisions,
                (decisions.c.group_id == latest.c.group_id)
                & (decisions.c.sequence == latest.c.sequence),
            )
            .where(
                groups.c.case_id == case_id,
                groups.c.persona_id == persona_id,
                decisions.c.decision == "include",
            )
            .order_by(groups.c.id)
        )
        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            for row in (
                connection.execution_options(stream_results=True, yield_per=batch_size)
                .execute(statement)
                .mappings()
            ):
                result = dict(row)
                current = self._group(connection, result, limit=0)
                if (
                    current.get('decision_stale')
                    or current['normalized'].get('binding_status') == 'unresolved'
                    or current['normalized'].get('binding_target_persona_id')
                    not in {None, persona_id}
                ):
                    continue
                result['normalized'] = current['normalized']
                corrected = result["decision_details"].get("corrected_claim")
                if corrected:
                    result["normalized"] = dict(
                        result['normalized'],
                        **{
                            key: value
                            for key, value in corrected.items()
                            if key
                            in {
                                'predicate',
                                'value',
                                'qualifiers',
                                'valid_from',
                                'valid_to',
                                'account_key',
                            }
                        },
                    )
                result["latest_decision"] = {
                    "id": result["decision_id"],
                    "decision": "include",
                    "actor": result["decision_actor"],
                    "reason": result["decision_reason"],
                }
                yield _json(result)

    def list_accounts(self, case_id, persona_id):
        table = self._table("groups")
        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            return _json(
                [
                    dict(row)
                    for row in connection.execute(
                        select(table.c.id, table.c.normalized)
                        .where(
                            table.c.case_id == case_id,
                            table.c.persona_id == persona_id,
                            table.c.kind == "account",
                        )
                        .order_by(table.c.created_at, table.c.id)
                    ).mappings()
                ]
            )

    def get_group(self, case_id, persona_id, group_id, *, limit=100, offset=0):
        with self.engine.connect() as connection:
            row = self._row(connection, self._table("groups"), group_id)
            self._check_scope(row, case_id, persona_id)
            return _json(
                self._group(
                    connection,
                    row,
                    limit=min(max(int(limit), 1), 500),
                    offset=max(int(offset), 0),
                )
            )

    def decide(
        self,
        case_id,
        persona_id,
        group_id,
        decision,
        *,
        actor,
        reason,
        corrected_claim=None,
        evidence_dispositions=None,
    ):
        actor, reason = _actor(actor), _text(reason, "Decision reason")
        if decision not in {"include", "exclude", "reject", "unresolved"}:
            raise ValueError(
                "Operator decision must be include, exclude, reject or unresolved"
            )
        with self.engine.begin() as connection:
            self._scope(connection, case_id, persona_id, lock=True)
            group = self._row(connection, self._table("groups"), group_id)
            self._check_scope(group, case_id, persona_id)
            details = {}
            decision_table = self._table("operator_decisions")
            previous = connection.execute(
                select(decision_table.c.details)
                .where(decision_table.c.group_id == group_id)
                .order_by(decision_table.c.sequence.desc())
                .limit(1)
            ).scalar()
            dispositions = {
                item["observation_id"]: item
                for item in (previous or {}).get("evidence_dispositions", [])
            }
            if evidence_dispositions is not None:
                if (
                    not isinstance(evidence_dispositions, list)
                    or len(evidence_dispositions) > 500
                ):
                    raise ValueError(
                        "Evidence dispositions must be a list of at most 500 reviewed observations"
                    )
                observations, memberships = self._table("observations"), self._table(
                    "group_observations"
                )
                for disposition in evidence_dispositions:
                    if not isinstance(disposition, dict) or disposition.get(
                        "disposition"
                    ) not in {"exclude_from_support", "restore_support"}:
                        raise ValueError(
                            "Evidence disposition must exclude_from_support or restore_support"
                        )
                    oid = disposition.get("observation_id")
                    source = (
                        connection.execute(
                            select(observations)
                            .join(
                                memberships,
                                memberships.c.observation_id == observations.c.id,
                            )
                            .where(
                                memberships.c.group_id == group_id,
                                observations.c.id == oid,
                            )
                        )
                        .mappings()
                        .first()
                    )
                    if not source:
                        raise ValueError(
                            "Evidence disposition is outside the reviewed group"
                        )
                    reason_text = _text(
                        disposition.get("reason"), "Evidence disposition reason"
                    )
                    if disposition["disposition"] == "restore_support":
                        dispositions.pop(oid, None)
                    else:
                        dispositions[oid] = {
                            "observation_id": oid,
                            "disposition": "exclude_from_support",
                            "reason": reason_text,
                            "reviewed_content_hash": source["content_hash"],
                        }
                details["evidence_disposition_changes"] = _json(evidence_dispositions)
            if dispositions:
                details["evidence_dispositions"] = [
                    dispositions[oid] for oid in sorted(dispositions)
                ]
            if corrected_claim is not None:
                if (
                    not isinstance(corrected_claim, dict)
                    or not corrected_claim
                    or group['kind'] != 'claim'
                ):
                    raise ValueError(
                        "Corrections require a nonempty claim object and a claim group"
                    )
                original = self._group(connection, group, limit=0)['normalized']
                allowed = {
                    'predicate',
                    'value',
                    'qualifiers',
                    'valid_from',
                    'valid_to',
                    'account_key',
                }
                if any(
                    key not in allowed and original.get(key) != value
                    for key, value in corrected_claim.items()
                ):
                    raise ValueError(
                        'Claim correction cannot alter identity, binding or conflict constraints'
                    )
                corrected_claim = dict(original, **corrected_claim)
                if (
                    not isinstance(corrected_claim.get('predicate'), str)
                    or not corrected_claim['predicate'].strip()
                    or corrected_claim.get('value') in (None, '', {}, [])
                ):
                    raise ValueError('Corrected claim requires a predicate and value')
                if 'qualifiers' in corrected_claim and not isinstance(
                    corrected_claim['qualifiers'], dict
                ):
                    raise ValueError('Corrected claim qualifiers must be an object')
                if original.get('account_key') and not corrected_claim.get(
                    'account_key'
                ):
                    raise ValueError(
                        'Claim correction cannot remove its account attribution'
                    )
                details["corrected_claim"] = _json(corrected_claim)
                if corrected_claim.get("account_key"):
                    account_groups = self._table("groups")
                    accounts = connection.execute(
                        select(account_groups).where(
                            account_groups.c.case_id == case_id,
                            account_groups.c.persona_id == persona_id,
                            account_groups.c.kind == "account",
                        )
                    ).mappings()
                    if not any(
                        corrected_claim["account_key"]
                        in {
                            row["id"],
                            row["normalized"].get("id"),
                            row["normalized"].get("canonical_key"),
                        }
                        for row in accounts
                    ):
                        raise ValueError(
                            "Corrected account binding is not an account hypothesis in this case and Persona"
                        )
            table = self._table("operator_decisions")
            sequence = (
                connection.scalar(
                    select(func.max(table.c.sequence)).where(
                        table.c.group_id == group_id
                    )
                )
                or 0
            ) + 1
            row = dict(
                id=_id(),
                case_id=case_id,
                persona_id=persona_id,
                group_id=group_id,
                sequence=sequence,
                decision=decision,
                actor=actor,
                reason=reason,
                details=details,
                created_at=_now(),
            )
            connection.execute(insert(table).values(**row))
            self._bump(connection, persona_id)
            return _json(row)

    def reuse_evidence(
        self,
        source_case_id,
        source_persona_id,
        observation_ids,
        *,
        target_case_id,
        target_persona_id,
        actor,
        reason,
        connection=None,
    ):
        """Explicitly reuse retained evidence with original-ID FK and scoped decisions.

        Caller authorization must cover both cases. Source observations never
        move: the new scoped observation points to the immutable original and
        retains the original source family, so reuse cannot create corroboration.
        """
        from maigret.web.pipeline_evidence import normalize_observation
        from maigret.web.pipeline_consolidation import consolidate_observations

        actor, reason = _actor(actor), _text(reason, "Evidence reuse reason")
        ids = sorted(set(observation_ids))
        if not ids:
            raise ValueError("Choose source observations to reuse")
        with (
            self.engine.begin() if connection is None else nullcontext(connection)
        ) as connection:
            for cid, pid in sorted(
                {
                    (source_case_id, source_persona_id),
                    (target_case_id, target_persona_id),
                }
            ):
                self._scope(connection, cid, pid, lock=True)
            originals = []
            for observation_id in ids:
                original = self._row(
                    connection, self._table("observations"), observation_id
                )
                self._check_scope(original, source_case_id, source_persona_id)
                if not original["retained"]:
                    raise ValueError(
                        "Provider-limited live evidence cannot be copied as retained evidence"
                    )
                originals.append(original)
            key = "reuse:" + _digest([ids, target_case_id, target_persona_id])
            request = self.create_request(
                target_case_id,
                target_persona_id,
                [{"type": "retained_evidence", "value": oid} for oid in ids],
                {
                    "pipeline_id": PIPELINE_ID,
                    "tasks": [
                        {
                            "engine_id": "retained_evidence_reuse",
                            "task_id": key,
                            "route_state": "active",
                            "reason": reason,
                        }
                    ],
                },
                actor=actor,
                idempotency_key=key,
                connection=connection,
            )
            task = request["tasks"][0]
            attempt = (
                task["attempts"][0]
                if task["attempts"]
                else self.start_attempt(
                    task["id"], "evidence-reuse:" + actor, connection=connection
                )
            )
            records = []
            for original in originals:
                raw = dict(original["payload"])
                for field in (
                    "id",
                    "case_id",
                    "subject_id",
                    "persona_id",
                    "request_id",
                    "task_id",
                    "attempt_id",
                ):
                    raw.pop(field, None)
                raw.update(
                    native_record_id="reuse:" + original["id"],
                    original_observation_id=original["id"],
                    original_evidence_id=original["payload"].get("original_evidence_id")
                    or original["id"],
                    derived_from=list(
                        dict.fromkeys(
                            [
                                *raw.get("derived_from", []),
                                original["payload"].get("id", original["id"]),
                            ]
                        )
                    ),
                    origin_family_id=original["origin_family"],
                    reuse_reason=reason,
                )
                normalized = normalize_observation(
                    raw,
                    case_id=target_case_id,
                    subject_id=target_persona_id,
                    request_id=request["id"],
                    task_id=task["id"],
                    attempt_id=attempt["id"],
                    observed_at=original["payload"].get("observed_at")
                    or _json(original["created_at"]),
                    engine=original["engine"],
                )
                # Keep direct immutable database lineage in addition to original native IDs.
                normalized["original_observation_id"] = original["id"]
                records.extend(
                    self._append_observations(connection, attempt, task, [normalized])
                )
            self._finish_attempt(connection, attempt, task, "candidate")
            observations = self._table("observations")
            all_docs = list(
                connection.scalars(
                    select(observations.c.payload).where(
                        observations.c.case_id == target_case_id,
                        observations.c.persona_id == target_persona_id,
                    )
                )
            )
            groups = self.upsert_groups(
                target_case_id,
                target_persona_id,
                consolidate_observations(all_docs),
                connection=connection,
            )
            return _json(
                {
                    "request_id": request["id"],
                    "original_observation_ids": ids,
                    "observations": records,
                    "groups": groups,
                }
            )

    def revise_group(
        self,
        case_id,
        persona_id,
        group_id,
        *,
        actor,
        reason,
        action,
        observation_ids=(),
        target_persona_id=None,
    ):
        actor, reason = _actor(actor), _text(reason, "Grouping revision reason")
        if action not in {"split", "bind"}:
            raise ValueError("Grouping action must be split or bind")
        with self.engine.begin() as connection:
            for pid in sorted({persona_id, target_persona_id or persona_id}):
                self._scope(connection, case_id, pid, lock=True)
            group = self._row(connection, self._table("groups"), group_id)
            self._check_scope(group, case_id, persona_id)
            expanded = self._group(connection, group, limit=None)
            details = {}
            if action == "split":
                ids = sorted(set(observation_ids))
                effective_ids = {row["id"] for row in expanded["observations"]}
                if (
                    not ids
                    or not set(ids).issubset(effective_ids)
                    or set(ids) == effective_ids
                ):
                    raise ValueError(
                        "Split requires a proper nonempty subset of the group's current observations"
                    )
                new_id = _id()
                normalized = dict(
                    expanded["normalized"],
                    id="split:" + new_id,
                    canonical_key="split:" + new_id,
                    source_group_id=group_id,
                    operator_split=True,
                )
                target = dict(
                    id=new_id,
                    case_id=case_id,
                    persona_id=persona_id,
                    kind=group["kind"],
                    canonical_key=_digest([group_id, ids]),
                    normalized=normalized,
                    created_at=_now(),
                )
                connection.execute(insert(self._table("groups")).values(**target))
                for observation_id in ids:
                    connection.execute(
                        insert(self._table("group_observations")).values(
                            case_id=case_id,
                            persona_id=persona_id,
                            group_id=new_id,
                            observation_id=observation_id,
                            created_at=_now(),
                        )
                    )
                details = {"observation_ids": ids, "target_group_id": new_id}
            else:
                if group["kind"] != "account" or not target_persona_id:
                    raise ValueError(
                        "An account binding requires an explicit case Persona"
                    )
                details = {"target_persona_id": target_persona_id}
                if target_persona_id != persona_id:
                    shared = self.reuse_evidence(
                        case_id,
                        persona_id,
                        [row["id"] for row in expanded["observations"]],
                        target_case_id=case_id,
                        target_persona_id=target_persona_id,
                        actor=actor,
                        reason=reason,
                        connection=connection,
                    )
                    details["target_request_id"] = shared["request_id"]
            revision = dict(
                id=_id(),
                case_id=case_id,
                persona_id=persona_id,
                group_id=group_id,
                action=action,
                actor=actor,
                reason=reason,
                details=details,
                created_at=_now(),
            )
            connection.execute(
                insert(self._table("group_revisions")).values(**revision)
            )
            self._refresh_group_summary(connection, group)
            if action == "split":
                self._refresh_group_summary(connection, target)
            self._bump(connection, persona_id)
            return _json(revision)

    def _requirements(self, connection, case_id, persona_id):
        table = self._table("research_requirements")
        rows = [
            dict(row)
            for row in connection.execute(
                select(table)
                .where(table.c.case_id == case_id, table.c.persona_id == persona_id)
                .order_by(table.c.created_at, table.c.id)
            ).mappings()
        ]
        links, resolutions = self._table("requirement_requests"), self._table(
            "requirement_resolutions"
        )
        for row in rows:
            row["request_ids"] = list(
                connection.scalars(
                    select(links.c.request_id).where(
                        links.c.requirement_id == row["id"]
                    )
                )
            )
            row["resolutions"] = [
                dict(item)
                for item in connection.execute(
                    select(resolutions)
                    .where(resolutions.c.requirement_id == row["id"])
                    .order_by(resolutions.c.created_at)
                ).mappings()
            ]
        return rows

    def get_requirement(self, requirement_id, *, case_id=None, persona_id=None):
        with self.engine.connect() as connection:
            row = self._row(
                connection, self._table("research_requirements"), requirement_id
            )
            self._check_scope(row, case_id, persona_id)
            links, resolutions = self._table("requirement_requests"), self._table("requirement_resolutions")
            row["request_ids"] = list(connection.scalars(select(links.c.request_id).where(
                links.c.requirement_id == requirement_id)))
            row["resolutions"] = [dict(item) for item in connection.execute(select(resolutions).where(
                resolutions.c.requirement_id == requirement_id).order_by(
                    resolutions.c.created_at, resolutions.c.id)).mappings()]
            return _json(row)

    def _version(self, connection, row):
        result = dict(row)
        qc = self._table("qc_decisions")
        decision = (
            connection.execute(select(qc).where(qc.c.version_id == row["id"]))
            .mappings()
            .first()
        )
        result["status"] = decision["decision"] if decision else "submitted"
        result["qc"] = dict(decision) if decision else None
        return result

    def get_version(self, version_id, *, case_id=None, persona_id=None):
        with self.engine.connect() as connection:
            row = self._row(connection, self._table("persona_versions"), version_id)
            self._check_scope(row, case_id, persona_id)
            return _json(self._version(connection, row))

    def get_final_version(self, case_id, persona_id):
        with self.engine.connect() as connection:
            state = self._scope(connection, case_id, persona_id)
            if not state.get("final_version_id"):
                return None
            result = self._version(
                connection,
                self._row(
                    connection,
                    self._table("persona_versions"),
                    state["final_version_id"],
                ),
            )
            result["publication_status"] = state["final_status"]
            result["review_needed"] = state["review_needed"]
            result["withdrawal_reason"] = state.get("withdrawal_reason")
            return _json(result)

    def list_history(
        self, case_id, persona_id, kind, *, limit=25, offset=0, parent_id=None
    ):
        """Independent bounded pages preserve access to every lifecycle record."""
        names = {
            "requests": "requests",
            "tasks": "tasks",
            "attempts": "attempts",
            "versions": "persona_versions",
            "requirements": "research_requirements",
            "decisions": "operator_decisions",
            "revisions": "group_revisions",
            "resolutions": "requirement_resolutions",
        }
        if kind not in names:
            raise ValueError("Unknown pipeline history kind")
        limit, offset = min(max(int(limit), 1), 100), max(int(offset), 0)
        table = self._table(names[kind])
        criteria = [table.c.case_id == case_id, table.c.persona_id == persona_id]
        if parent_id and kind == "requests":
            links = self._table("requirement_requests")
            criteria.append(table.c.id.in_(select(links.c.request_id).where(
                links.c.requirement_id == parent_id, links.c.case_id == case_id,
                links.c.persona_id == persona_id)))
        elif parent_id:
            parent_field = {
                "tasks": "request_id",
                "attempts": "task_id",
                "decisions": "group_id",
                "revisions": "group_id",
                "resolutions": "requirement_id",
            }.get(kind)
            if not parent_field:
                raise ValueError("This history has no parent filter")
            criteria.append(table.c[parent_field] == parent_id)
        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            columns = [
                column for column in table.c if column.name not in {"manifest", "plan"}
            ]
            if kind == "requests":
                columns.append(table.c.plan["budgets"]["max_requests"].as_integer().label("planned_request_limit"))
            rows = [
                dict(row)
                for row in connection.execute(
                    select(*columns)
                    .where(*criteria)
                    .order_by(table.c.created_at.desc(), table.c.id.desc())
                    .limit(limit)
                    .offset(offset)
                ).mappings()
            ]
            total = connection.scalar(
                select(func.count()).select_from(table).where(*criteria)
            )
            if kind == "requests":
                budgets = self.tables.get("request_budgets")
                usage = {} if budgets is None else {
                    row["request_id"]: dict(row) for row in connection.execute(select(budgets).where(
                        budgets.c.request_id.in_([row["id"] for row in rows]))).mappings()
                }
                for row in rows:
                    recorded = usage.get(row["id"], {})
                    planned = row.get("planned_request_limit")
                    maximum = recorded.get("max_requests", 100000 if planned is None else planned)
                    consumed = recorded.get("consumed", 0)
                    row["request_usage"] = dict(max_requests=maximum, consumed=consumed,
                                                remaining=max(0, maximum - consumed))
            if kind == "versions":
                rows = [self._version(connection, row) for row in rows]
            if kind == "tasks":
                attempts = self._table("attempts")
                ranked = (
                    select(
                        attempts,
                        func.row_number()
                        .over(
                            partition_by=attempts.c.task_id,
                            order_by=attempts.c.number.desc(),
                        )
                        .label("position"),
                    )
                    .where(attempts.c.task_id.in_([row["id"] for row in rows]))
                    .subquery()
                )
                by_task = {}
                for attempt in connection.execute(
                    select(ranked).where(ranked.c.position <= 3)
                ).mappings():
                    by_task.setdefault(attempt["task_id"], []).append(
                        {
                            key: value
                            for key, value in attempt.items()
                            if key != "position"
                        }
                    )
                for task in rows:
                    task["attempts"] = by_task.get(task["id"], [])
            return _json(
                dict(
                    items=rows,
                    count=total,
                    limit=limit,
                    offset=offset,
                    has_next=offset + len(rows) < total,
                )
            )

    def get_workspace(
        self, case_id, persona_id, *, limit=100, offset=0, history_offset=0
    ):
        limit, offset = min(max(int(limit), 1), 100), max(int(offset), 0)
        history_offset = max(int(history_offset), 0)
        with self.engine.connect() as connection:
            state = self._scope(connection, case_id, persona_id)
            projection = self._projection_state(connection, case_id, persona_id)
            legacy_counts = {"claims": 0, "messages": 0, "jobs": 0}
            if projection["legacy_imported_at"] is None:
                legacy_counts["claims"] = connection.scalar(select(func.count()).select_from(persona_claims).where(
                    persona_claims.c.persona_id == persona_id))
                legacy_counts["messages"] = connection.scalar(select(func.count()).select_from(case_chat_messages).where(
                    case_chat_messages.c.case_id == case_id,
                    (case_chat_messages.c.persona_id == persona_id) | case_chat_messages.c.persona_id.is_(None)))
                requests = self._table("requests")
                legacy_counts["jobs"] = connection.scalar(select(func.count()).select_from(investigation_jobs).where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.status.in_(["completed", "failed", "cancelled", "interrupted"]),
                    ~select(requests.c.id).where(requests.c.job_id == investigation_jobs.c.id).exists()))
            groups = self._table("groups")
            criteria = (groups.c.case_id == case_id, groups.c.persona_id == persona_id)
            group_rows = list(
                connection.execute(
                    select(groups)
                    .where(*criteria)
                    .order_by(groups.c.created_at, groups.c.id)
                    .limit(limit)
                    .offset(offset)
                ).mappings()
            )
            result = dict(
                pipeline_id=PIPELINE_ID,
                **state,
                projection=dict(
                    projection,
                    pending=projection["evidence_revision"]
                    != projection["projected_revision"],
                    legacy_counts=legacy_counts,
                    legacy_available=any(legacy_counts.values()),
                ),
                groups=[self._group_summary(connection, row) for row in group_rows],
                group_count=connection.scalar(
                    select(func.count()).select_from(groups).where(*criteria)
                ),
                limit=limit,
                offset=offset,
                history_offset=history_offset,
                history_limit=25,
            )
        for kind in ("requests", "tasks", "versions", "requirements"):
            page = self.list_history(
                case_id, persona_id, kind, limit=25, offset=history_offset
            )
            result[kind] = page["items"]
            result[kind + "_count"] = page["count"]
        result.update(case_id=case_id, persona_id=persona_id)
        return _json(result)

    def create_version(
        self,
        case_id,
        persona_id,
        *,
        actor,
        scope,
        limitations=(),
        parent_version_id=None,
    ):
        """Freeze the complete curated manifest and submit it for separate QC."""
        actor = _actor(actor)
        if not scope or not isinstance(scope, (dict, str)):
            raise ValueError("A curated version requires a defined research scope")
        if isinstance(scope, dict) and "mandatory_fields" in scope:
            if not isinstance(scope["mandatory_fields"], dict) or any(
                not isinstance(key, str) or not isinstance(value, bool)
                for key, value in scope["mandatory_fields"].items()
            ):
                raise ValueError(
                    "Mandatory scope fields require explicit boolean dispositions"
                )
        with self.engine.begin() as connection:
            state = self._scope(connection, case_id, persona_id, lock=True)
            self._assert_projection_current(connection, case_id, persona_id)
            groups, versions = self._table("groups"), self._table("persona_versions")
            latest = (
                connection.execute(
                    select(
                        *(column for column in versions.c if column.name != "manifest")
                    )
                    .where(versions.c.persona_id == persona_id)
                    .order_by(versions.c.sequence.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            if parent_version_id:
                parent = self._row(connection, versions, parent_version_id)
                self._check_scope(parent, case_id, persona_id)
                if latest and parent_version_id != latest["id"]:
                    raise ValueError(
                        "Successor must refer to the latest submitted version"
                    )
            elif latest:
                parent_version_id = latest["id"]
            items, exclusions, evidence_ids = [], [], set()
            superseded = self._superseded_observations(connection, case_id, persona_id)
            for group in connection.execute(
                select(groups)
                .where(groups.c.case_id == case_id, groups.c.persona_id == persona_id)
                .order_by(groups.c.kind, groups.c.canonical_key)
            ).mappings():
                expanded = self._group(
                    connection, group, limit=None, include_assessed_group=True
                )
                decision = expanded["latest_decision"]
                dispositions = {
                    item["observation_id"]: item
                    for item in ((decision or {}).get("details") or {}).get(
                        "evidence_dispositions", []
                    )
                }
                for observation in expanded["observations"]:
                    if observation["id"] in superseded:
                        observation["support_eligible"] = False
                        observation["source_state"] = "superseded_or_withdrawn"
                    if observation["id"] in dispositions:
                        disposition = dispositions[observation["id"]]
                        if (
                            disposition["reviewed_content_hash"]
                            == observation["content_hash"]
                        ):
                            observation["operator_disposition"] = disposition
                item = dict(
                    group_id=group["id"],
                    kind=group["kind"],
                    normalized=expanded["normalized"],
                    decision=decision,
                    evidence=expanded["observations"],
                    assessment=expanded.get("assessment"),
                    assessed_group=expanded.get("assessed_group"),
                    assessment_id=expanded.get("assessment_id"),
                    probability=None,
                    probability_reason="No validated probability artifact attached",
                )
                assessment = expanded.get("assessment") or {}
                probability = assessment.get("probability") or {}
                if isinstance(probability, dict):
                    item["probability"] = probability.get("value")
                    item["probability_reason"] = (
                        probability.get("reason") or item["probability_reason"]
                    )
                if dispositions:
                    item["prior_assessment"] = item["assessment"]
                    item["assessment"] = {
                        "probability": {
                            "value": None,
                            "reason": "operator_evidence_disposition_requires_reassessment",
                        },
                        "operator_review_available": True,
                    }
                    item["probability"] = None
                    item["probability_reason"] = (
                        "operator_evidence_disposition_requires_reassessment"
                    )
                if decision and decision["details"].get("corrected_claim"):
                    item["original_normalized"] = item["normalized"]
                    item["normalized"] = dict(
                        item["normalized"],
                        **{
                            key: value
                            for key, value in decision["details"][
                                "corrected_claim"
                            ].items()
                            if key
                            in {
                                "predicate",
                                "value",
                                "qualifiers",
                                "valid_from",
                                "valid_to",
                                "account_key",
                            }
                        },
                    )
                    item["prior_assessment"] = item["assessment"]
                    item["assessment"] = {
                        "probability": {
                            "value": None,
                            "reason": "operator_correction_requires_reassessment",
                        },
                        "operator_review_available": True,
                    }
                    item["probability"] = None
                    item["probability_reason"] = (
                        "operator_correction_requires_reassessment"
                    )
                if expanded.get("decision_stale"):
                    item["disposition"] = "review_required_after_group_revision"
                if (
                    decision
                    and decision["decision"] == "include"
                    and not expanded.get("decision_stale")
                ):
                    items.append(item)
                    evidence_ids.update(
                        observation["id"] for observation in expanded["observations"]
                    )
                else:
                    exclusions.append(item)
            if not items:
                raise ValueError(
                    "Curate at least one sourced account or claim before submitting a version"
                )
            manifest = _json(
                dict(
                    pipeline_id=PIPELINE_ID,
                    case_id=case_id,
                    persona_id=persona_id,
                    scope=scope,
                    limitations=list(limitations),
                    items=items,
                    exclusions=exclusions,
                    requirements=self._requirements(connection, case_id, persona_id),
                    evidence_set_hash=_digest(sorted(evidence_ids)),
                )
            )
            version = dict(
                id=_id(),
                case_id=case_id,
                persona_id=persona_id,
                sequence=(latest["sequence"] if latest else 0) + 1,
                parent_version_id=parent_version_id,
                actor=actor,
                workspace_revision=state["revision"],
                content_hash=_digest(manifest),
                manifest=manifest,
                created_at=_now(),
            )
            connection.execute(insert(versions).values(**version))
            for observation_id in sorted(evidence_ids):
                connection.execute(
                    insert(self._table("version_evidence")).values(
                        case_id=case_id,
                        persona_id=persona_id,
                        version_id=version["id"],
                        observation_id=observation_id,
                    )
                )
            return _json(self._version(connection, version))

    @staticmethod
    def _qc_permission(actor, permissions):
        actor = _actor(actor)
        if "persona:qc" not in set(permissions or ()):
            raise PermissionError("Explicit persona:qc permission is required")
        return actor

    @staticmethod
    def _research_spec(spec):
        if not isinstance(spec, dict):
            raise ValueError("Research requirement must be a structured object")
        spec = _json(spec)
        for field in ("question", "reason", "completion_criteria"):
            spec[field] = _text(spec.get(field), f"Research {field}")
        spec.setdefault("inputs", [])
        spec.setdefault("engines", [])
        if not isinstance(spec["inputs"], list) or not isinstance(
            spec["engines"], list
        ):
            raise ValueError("Research inputs and engines must be lists")
        budget = int(spec.get("request_budget", 3))
        if not 1 <= budget <= 10:
            raise ValueError("Research request budget must be between one and ten")
        spec["request_budget"] = budget
        spec.setdefault("priority", "normal")
        return spec

    @staticmethod
    def _superseded_observations(connection, case_id, persona_id):
        if "pipeline_connector_record_heads" not in metadata.tables:
            return set()
        from maigret.web.pipeline_connector_ingestion import superseded_observation_ids

        return superseded_observation_ids(connection, case_id, persona_id)

    def _qc_blockers(self, connection, version, waived_ids):
        from maigret.web.pipeline_consolidation import subject_claim_conflicts
        from maigret.web.pipeline_evidence import observation_evidence_role

        blockers, manifest = [], version["manifest"]
        superseded = self._superseded_observations(
            connection, version["case_id"], version["persona_id"]
        )
        included_accounts = {
            item["normalized"].get("id", item["normalized"].get("canonical_key"))
            for item in manifest["items"]
            if item["kind"] == "account"
        }
        all_items = manifest["items"] + manifest["exclusions"]
        blockers.extend(subject_claim_conflicts(manifest["items"], included_accounts))
        for item in manifest["items"]:
            normalized = item["normalized"]
            contradictory = [
                e["id"]
                for e in item["evidence"]
                if e["id"] not in superseded
                and observation_evidence_role(e, normalized) == "contradicts"
            ]
            if contradictory:
                blockers.append(
                    {
                        "group_id": item["group_id"],
                        "observation_ids": contradictory,
                        "reason": "Unresolved contradictory observations require an explicit evidence disposition or further research",
                    }
                )
            unresolved_conflicts = []
            for conflict in normalized.get("conflicts", []):
                others = [
                    other
                    for other in all_items
                    if other["group_id"] != item["group_id"]
                    and conflict in other["normalized"].get("conflicts", [])
                ]
                if (
                    not others
                    or not manifest["limitations"]
                    or any(
                        not other.get("decision")
                        or other["decision"]["decision"] not in {"exclude", "reject"}
                        for other in others
                    )
                ):
                    unresolved_conflicts.append(conflict)
            retained = [
                e
                for e in item["evidence"]
                if e["retained"]
                and e["id"] not in superseded
                and (e["source_url"] or e["artifact_ref"])
                and observation_evidence_role(e, normalized)
                in {"supports", "candidate_support"}
                and (e.get("payload", {}).get("retention") or {}).get("final_eligible")
                is not False
            ]
            if not retained:
                blockers.append(
                    {
                        "group_id": item["group_id"],
                        "reason": "Included fact lacks retained, attributable source evidence",
                    }
                )
            if (
                normalized.get("material_identity_conflict")
                or normalized.get("unresolved_conflict")
                or unresolved_conflicts
                or normalized.get("binding_status") == "unresolved"
            ):
                blockers.append(
                    {
                        "group_id": item["group_id"],
                        "reason": "Unresolved material identity conflict or subject binding",
                    }
                )
            if normalized.get("binding_target_persona_id") not in {
                None,
                version["persona_id"],
            }:
                blockers.append(
                    {
                        "group_id": item["group_id"],
                        "reason": "Account was explicitly bound to another subject",
                    }
                )
            account_key = normalized.get("account_key")
            if (
                item["kind"] == "claim"
                and account_key
                and account_key not in included_accounts
            ):
                blockers.append(
                    {
                        "group_id": item["group_id"],
                        "reason": "Claim account attribution has not been included by the operator",
                    }
                )
            if normalized.get("location_precision") == "exact" and normalized.get(
                "evidence_precision"
            ) in {"city", "region", "country", "approximate"}:
                blockers.append(
                    {
                        "group_id": item["group_id"],
                        "reason": "Exact location exceeds supporting evidence precision",
                    }
                )
            if item.get("probability") is not None:
                from maigret.web.pipeline_assessment import validate_frozen_probability

                probability_error = validate_frozen_probability(
                    item, case_id=version["case_id"], subject_id=version["persona_id"]
                )
                if probability_error:
                    blockers.append(
                        {"group_id": item["group_id"], "reason": probability_error}
                    )
        for requirement in self._requirements(
            connection, version["case_id"], version["persona_id"]
        ):
            if (
                requirement["status"] not in {"resolved", "waived"}
                and requirement["id"] not in waived_ids
            ):
                blockers.append(
                    {
                        "requirement_id": requirement["id"],
                        "reason": "Research completion criteria have no resolved or waived disposition",
                    }
                )
        scope = manifest["scope"]
        if isinstance(scope, dict):
            for field, disposition in scope.get("mandatory_fields", {}).items():
                if not disposition:
                    blockers.append(
                        {
                            "field": field,
                            "reason": "Mandatory scope objective has no disposition",
                        }
                    )
        return blockers

    def qc(
        self,
        version_id,
        decision,
        *,
        actor,
        permissions,
        expected_hash,
        findings=(),
        requirements=(),
        waivers=(),
    ):
        actor = self._qc_permission(actor, permissions)
        if decision not in {"approved", "changes_required"}:
            raise ValueError("QC decision must be approved or changes_required")
        requirements = [self._research_spec(spec) for spec in requirements]
        findings = _json(list(findings))
        if decision == 'approved' and any(
            not isinstance(finding, dict)
            or finding.get('severity', 'material')
            not in {'info', 'informational', 'minor', 'advisory'}
            for finding in findings
        ):
            raise ValueError('Material QC findings require changes before approval')
        if decision == 'approved' and requirements:
            raise ValueError(
                'QC approval cannot create unresolved research requirements; reject the version and complete research first'
            )
        if decision == "changes_required" and not requirements:
            requirements = [
                self._research_spec(spec)
                for spec in findings
                if isinstance(spec, dict) and spec.get("question")
            ]
            if not requirements:
                raise ValueError(
                    "Failed QC requires specific research requirements with completion criteria"
                )
        with self.engine.begin() as connection:
            version = self._row(connection, self._table("persona_versions"), version_id)
            state = self._scope(
                connection, version["case_id"], version["persona_id"], lock=True
            )
            qc_table = self._table("qc_decisions")
            if connection.execute(
                select(qc_table.c.id).where(qc_table.c.version_id == version_id)
            ).first():
                raise ValueError(
                    "This immutable version already received a QC decision"
                )
            if (
                expected_hash != version["content_hash"]
                or _digest(version["manifest"]) != expected_hash
            ):
                raise ValueError("Stale or incorrect submitted version hash")
            versions = self._table("persona_versions")
            latest = connection.scalar(
                select(func.max(versions.c.sequence)).where(
                    versions.c.persona_id == version["persona_id"]
                )
            )
            if version["sequence"] != latest:
                raise ValueError(
                    "A successor version exists; review the latest submitted version"
                )
            if (
                decision == "approved"
                and state["revision"] != version["workspace_revision"]
            ):
                raise ValueError(
                    "Evidence or decisions changed after submission; create and review a successor version"
                )
            waiver_rows = []
            for waiver in waivers:
                requirement = self._row(
                    connection,
                    self._table("research_requirements"),
                    waiver.get("requirement_id"),
                )
                self._check_scope(
                    requirement, version["case_id"], version["persona_id"]
                )
                waiver_rows.append(
                    (requirement, _text(waiver.get("reason"), "QC waiver reason"))
                )
            if decision == "approved":
                blockers = self._qc_blockers(
                    connection, version, {row[0]["id"] for row in waiver_rows}
                )
                if blockers:
                    error = ValueError(
                        "QC blocked: " + "; ".join(item["reason"] for item in blockers)
                    )
                    error.findings = blockers
                    raise error
            now = _now()
            row = dict(
                id=_id(),
                case_id=version["case_id"],
                persona_id=version["persona_id"],
                version_id=version_id,
                decision=decision,
                actor=actor,
                expected_hash=expected_hash,
                findings=_json(list(findings)),
                waivers=_json(list(waivers)),
                created_at=now,
            )
            connection.execute(insert(qc_table).values(**row))
            for requirement, reason in waiver_rows:
                self._resolve_requirement(
                    connection, requirement, actor, "waived", reason, []
                )
            for spec in requirements:
                if spec.get("target_group_id"):
                    group = self._row(
                        connection, self._table("groups"), spec["target_group_id"]
                    )
                    self._check_scope(group, version["case_id"], version["persona_id"])
                fingerprint = _digest(spec)
                table = self._table("research_requirements")
                if connection.execute(
                    select(table.c.id).where(
                        table.c.qc_id == row["id"], table.c.fingerprint == fingerprint
                    )
                ).first():
                    continue
                connection.execute(
                    insert(table).values(
                        id=_id(),
                        case_id=version["case_id"],
                        persona_id=version["persona_id"],
                        version_id=version_id,
                        qc_id=row["id"],
                        fingerprint=fingerprint,
                        spec=spec,
                        status="open",
                        created_at=now,
                    )
                )
            if decision == "approved":
                connection.execute(
                    update(self._table("persona_state"))
                    .where(
                        self._table("persona_state").c.persona_id
                        == version["persona_id"]
                    )
                    .values(
                        final_version_id=version_id,
                        final_status="final",
                        review_needed=False,
                        withdrawal_reason=None,
                        withdrawn_by=None,
                        updated_at=now,
                    )
                )
            else:
                self._bump(connection, version["persona_id"])
            result = self._version(connection, version)
            result["requirements"] = self._requirements(
                connection, version["case_id"], version["persona_id"]
            )
            return _json(result)

    def _resolve_requirement(
        self, connection, requirement, actor, disposition, reason, evidence_ids
    ):
        observations = self._table("observations")
        for evidence_id in evidence_ids:
            evidence = self._row(connection, observations, evidence_id)
            self._check_scope(
                evidence, requirement["case_id"], requirement["persona_id"]
            )
        if disposition == "resolved" and not evidence_ids:
            raise ValueError(
                "Research resolution requires supporting observation IDs; job completion alone is insufficient"
            )
        row = dict(
            id=_id(),
            case_id=requirement["case_id"],
            persona_id=requirement["persona_id"],
            requirement_id=requirement["id"],
            disposition=disposition,
            actor=actor,
            reason=reason,
            evidence_ids=list(dict.fromkeys(evidence_ids)),
            created_at=_now(),
        )
        connection.execute(insert(self._table("requirement_resolutions")).values(**row))
        connection.execute(
            update(self._table("research_requirements"))
            .where(self._table("research_requirements").c.id == requirement["id"])
            .values(status=disposition)
        )
        return row

    def resolve_requirement(
        self, requirement_id, *, actor, disposition, reason, evidence_ids=()
    ):
        actor, reason = _actor(actor), _text(reason, "Research disposition reason")
        if disposition not in {"resolved", "contested"}:
            raise ValueError(
                "Resolution must be resolved or contested; waiver requires explicit QC action"
            )
        with self.engine.begin() as connection:
            requirement = self._row(
                connection, self._table("research_requirements"), requirement_id
            )
            self._scope(
                connection, requirement["case_id"], requirement["persona_id"], lock=True
            )
            row = self._resolve_requirement(
                connection, requirement, actor, disposition, reason, evidence_ids
            )
            self._bump(connection, requirement["persona_id"])
            return _json(row)

    def backfill_legacy(
        self,
        case_id,
        persona_id,
        *,
        actor,
        dry_run=True,
        limit=500,
        after_claim_id=None,
        materialize=True,
    ):
        """Checkpoint retained legacy claim revisions without promoting old reviews.

        Each claim revision owns a stable request key independent of page size or
        claim ordering. A later inserted UUID cannot cause old evidence to be
        imported twice merely because a batch boundary shifted. Existing source
        rows and review history are retained unchanged inside the native payload.
        """
        actor = _text(actor, "migration actor", 200)
        limit = min(max(int(limit), 1), 2000)
        records = []
        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            statement = select(persona_claims).where(
                persona_claims.c.persona_id == persona_id
            )
            if after_claim_id:
                statement = statement.where(persona_claims.c.id > after_claim_id)
            claims = [
                dict(row)
                for row in connection.execute(
                    statement.order_by(persona_claims.c.id).limit(limit)
                ).mappings()
            ]
            requests = self._table("requests")
            for claim in claims:
                claim["evidence"] = [
                    dict(row)
                    for row in connection.execute(
                        select(claim_evidence)
                        .where(claim_evidence.c.claim_id == claim["id"])
                        .order_by(claim_evidence.c.id)
                    ).mappings()
                ]
                claim["legacy_reviews"] = [
                    dict(row)
                    for row in connection.execute(
                        select(claim_reviews)
                        .where(claim_reviews.c.claim_id == claim["id"])
                        .order_by(claim_reviews.c.id)
                    ).mappings()
                ]
                claim = _json(claim)
                key = "legacy-claim:" + _digest([case_id, persona_id, claim])
                existing = (
                    connection.execute(
                        select(requests.c.id, requests.c.status).where(
                            requests.c.case_id == case_id,
                            requests.c.persona_id == persona_id,
                            requests.c.idempotency_key == key,
                        )
                    )
                    .mappings()
                    .first()
                )
                records.append((claim, key, dict(existing) if existing else None))
        report = {
            "case_id": case_id,
            "persona_id": persona_id,
            "dry_run": bool(dry_run),
            "claim_count": len(records),
            "pending_claim_count": sum(
                not item[2] or item[2]["status"] != "completed" for item in records
            ),
            "observation_count": sum(
                max(1, len(item[0]["evidence"])) for item in records
            ),
            "missing_provenance_count": sum(
                not item[0]["evidence"] for item in records
            ),
            "next_after_claim_id": claims[-1]["id"] if len(claims) == limit else None,
            "auto_finalized": False,
            "request_ids": [],
        }
        if dry_run or not records:
            return report
        from maigret.web.pipeline_evidence import iter_legacy_claim_observations
        from maigret.web.pipeline_consolidation import consolidate_observations

        for claim, key, existing in records:
            if existing and existing["status"] == "completed":
                report["request_ids"].append(existing["id"])
                continue
            request = self.create_request(
                case_id,
                persona_id,
                [{"type": "legacy_evidence", "value": claim["id"]}],
                {
                    "pipeline_id": PIPELINE_ID,
                    "tasks": [
                        {
                            "engine_id": "legacy_evidence_import",
                            "route_state": "active",
                            "task_id": key,
                        }
                    ],
                },
                actor=actor,
                idempotency_key=key,
            )
            task = request["tasks"][0]
            attempt = (
                task["attempts"][0]
                if task["attempts"]
                else self.start_attempt(task["id"], "legacy-import:" + actor)
            )
            observations = list(
                iter_legacy_claim_observations(
                    [claim],
                    case_id=case_id,
                    subject_id=persona_id,
                    request_id=request["id"],
                    task_id=task["id"],
                    attempt_id=attempt["id"],
                )
            )
            self.record_observations(
                attempt["id"],
                observations,
                outcome="candidate",
                worker_id=attempt["worker_id"],
            )
            report["request_ids"].append(request["id"])
        if materialize:
            self.upsert_groups(
                case_id,
                persona_id,
                consolidate_observations(self.iter_observations(case_id, persona_id)),
            )
        report["request_id"] = (
            report["request_ids"][0] if report["request_ids"] else None
        )
        return report

    def withdraw_final(
        self,
        case_id,
        persona_id,
        *,
        actor,
        permissions,
        reason,
        expected_version_id=None,
        expected_hash=None,
    ):
        actor, reason = self._qc_permission(actor, permissions), _text(
            reason, "Withdrawal reason"
        )
        with self.engine.begin() as connection:
            state = self._scope(connection, case_id, persona_id, lock=True)
            if not state.get("final_version_id"):
                raise ValueError("Persona has no final version to withdraw")
            version = self._row(
                connection, self._table("persona_versions"), state["final_version_id"]
            )
            if expected_version_id is not None and expected_version_id != version["id"]:
                raise ValueError("Final version changed; reload before withdrawal")
            if expected_hash is not None and expected_hash != version["content_hash"]:
                raise ValueError("Final version hash changed; reload before withdrawal")
            # An append-only grouping revision provides a retained human audit event.
            group_id = version["manifest"]["items"][0]["group_id"]
            connection.execute(
                insert(self._table("group_revisions")).values(
                    id=_id(),
                    case_id=case_id,
                    persona_id=persona_id,
                    group_id=group_id,
                    action="withdraw_final",
                    actor=actor,
                    reason=reason,
                    details={
                        "version_id": version["id"],
                        "content_hash": version["content_hash"],
                    },
                    created_at=_now(),
                )
            )
            connection.execute(
                update(self._table("persona_state"))
                .where(self._table("persona_state").c.persona_id == persona_id)
                .values(
                    final_status="withdrawn",
                    review_needed=True,
                    withdrawal_reason=reason,
                    withdrawn_by=actor,
                    updated_at=_now(),
                )
            )
            return {
                "version_id": version["id"],
                "status": "withdrawn",
                "reason": reason,
            }
