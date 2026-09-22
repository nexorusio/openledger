Warning: truncated output (original token count: 52727)
Total output lines: 4868

"""Durable P2 pipeline ledger and explicit Persona/QC state machine.

All writes are transactional and case scoped. Source observations and curated
versions are immutable database records. Operator judgement and QC are distinct
human actions; collection completion cannot finalize a Persona.
"""

from __future__ import annotations

import hashlib
import json
import math
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
from maigret.web.persona_schema import (
    FIELD_SECTIONS as SHORTLIST_PREDICATE_SECTIONS,
    PERSONA_SECTIONS,
    presentation_predicate,
    section_for,
)

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

SHORTLIST_SECTIONS = tuple(
    (section["key"], section["title"]) for section in PERSONA_SECTIONS
)

INPUT_EVIDENCE_ENGINE = "investigation_input"
INPUT_EVIDENCE_TASK_KEY = "system:submitted-inputs"
INPUT_CLAIM_PREDICATES = {
    "full_name": "full_name",
    "email": "email",
    "phone": "phone",
    "username": "username",
    "social_handle": "username",
    "organization": "organization",
}

INPUT_SECTIONS = {
    "full_name": "identity",
    "email": "contact",
    "phone": "contact",
    "username": "digital",
    "social_handle": "digital",
    "profile_url": "digital",
    "public_url": "digital",
    "organization": "affiliations",
    "official_website": "digital",
}


def _task_review_state(task):
    """Reduce technical lifecycle terms to one operator-facing coverage state."""
    outcome = str(task.get("outcome") or "").casefold()
    if outcome == "found":
        return "found"
    if outcome == "candidate":
        return "candidate"
    if outcome == "not_found":
        return "no_match"
    if outcome == "not_executed":
        return "not_searched"
    if outcome or task.get("status") == "completed":
        return "could_not_check"
    return "pending"


def _task_sections(task):
    """Map a collection task to the Persona sections whose coverage it affects."""
    engine = str(task.get("engine") or task.get("engine_id") or "").casefold()
    input_document = task.get("input") if isinstance(task.get("input"), dict) else {}
    input_type = str(
        task.get("input_type") or input_document.get("type") or ""
    ).casefold()
    if engine == INPUT_EVIDENCE_ENGINE:
        return set()
    sections = set()
    if any(value in engine for value in ("ai_cited", "case_fusion")):
        sections.update(key for key, _title in SHORTLIST_SECTIONS)
    if any(value in engine for value in ("icij", "offshore", "risk")):
        sections.update({"affiliations", "records"})
    if "github" in engine:
        # A GitHub profile can explicitly publish a name, email, location,
        # organization, website, and account identity.
        sections.update({"identity", "contact", "digital", "affiliations"})
    if "wikipedia" in engine:
        sections.update({"identity", "affiliations"})
    if any(
        value in engine
        for value in (
            "wikidata",
            "registry",
            "official_website",
            "business_context",
            "dns_context",
        )
    ):
        sections.add("affiliations")
    if "official_website" in engine:
        sections.add("contact")
    if any(
        value in engine
        for value in (
            "maigret",
            "profile_search",
            "user_scanner_username",
            "unfurl",
            "wayback",
        )
    ):
        sections.add("digital")
    if "user_scanner_email" in engine:
        sections.update({"contact", "digital"})
    if input_type in {"email", "phone"}:
        sections.add("contact")
    if input_type in {"username", "profile_url", "public_url"}:
        sections.add("digital")
    if input_type == "full_name":
        sections.add("identity")
    return sections


def _shortlist_section(kind, normalized):
    return section_for(kind, normalized)


def _reviewable_request_inputs(request):
    """Return human investigation anchors, excluding internal checkpoints."""
    actor = str(request.get("actor") or "").casefold()
    plan = request.get("plan") if isinstance(request.get("plan"), dict) else {}
    if actor.startswith(("system:", "worker:", "connector:")) or plan.get(
        "import_source"
    ):
        return []
    inputs = []
    for item in list(request.get("inputs") or []):
        if not isinstance(item, dict) or not str(item.get("value") or "").strip():
            continue
        input_type = str(item.get("type") or item.get("kind") or "").casefold()
        if input_type not in INPUT_SECTIONS:
            continue
        derivations = [
            row
            for row in list(item.get("derived_from") or [])
            if isinstance(row, dict)
        ]
        # A ranked spelling variant is an operator-selected search route, not
        # an asserted subject fact. Its positive source results remain
        # reviewable, but the alias itself must not be materialized as if the
        # investigator typed it as evidence.
        if derivations and all(
            row.get("type") == "ranked_alias" or row.get("context")
            for row in derivations
        ):
            continue
        normalized = dict(item)
        normalized["type"] = input_type
        inputs.append(normalized)
    return inputs


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
            legacy_converged_at=None,
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

    def mark_legacy_converged(self, case_id, persona_id):
        """Record the compatibility cutover after evidence and reviews converge.

        The older ``legacy_imported_at`` value proves only that evidence was
        copied. ``legacy_converged_at`` is deliberately separate so an older
        hybrid database cannot be mistaken for a completed review cutover.
        """
        with self.engine.begin() as connection:
            self._scope(connection, case_id, persona_id, lock=True)
            self._projection_state(connection, case_id, persona_id, create=True)
            table = self._table("projection_state")
            connection.execute(
                update(table)
                .where(table.c.persona_id == persona_id)
                .values(
                    legacy_imported_at=func.coalesce(
                        table.c.legacy_imported_at, _now()
                    ),
                    legacy_converged_at=_now(),
                    updated_at=_now(),
                )
            )

    def mark_legacy_imported(self, case_id, persona_id):
        """Compatibility alias for callers written before full convergence."""
        self.mark_legacy_converged(case_id, persona_id)

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

    def crawl_audit_for_job(self, job_id, *, observation_limit=5000):
        """Return bounded execution lineage for one investigation job.

        This is a read-only diagnostic projection.  It intentionally keeps the
        stored request/task/attempt lifecycle and source observations separate
        from Persona approval state.
        """
        bounded_limit = min(max(1, int(observation_limit)), 10000)
        requests = self._table("requests")
        observations = self._table("observations")
        with self.engine.connect() as connection:
            request_rows = list(
                connection.execute(
                    select(requests)
                    .where(requests.c.job_id == job_id)
                    .order_by(requests.c.created_at, requests.c.id)
                ).mappings()
            )
            request_ids = [row["id"] for row in request_rows]
            if not request_ids:
                return {
                    "requests": [],
                    "observations": [],
                    "observation_count": 0,
                    "observations_truncated": False,
                }
            observation_count = int(
                connection.scalar(
                    select(func.count())
                    .select_from(observations)
                    .where(observations.c.request_id.in_(request_ids))
                )
                or 0
            )
            observation_rows = list(
                connection.execute(
                    select(observations)
                    .where(observations.c.request_id.in_(request_ids))
                    .order_by(observations.c.created_at, observations.c.id)
                    .limit(bounded_limit)
                ).mappings()
            )
            return {
                "requests": [
                    self._request(connection, dict(row)) for row in request_rows
                ],
                "observations": [_json(dict(row)) for row in observation_rows],
                "observation_count": observation_count,
                "observations_truncated": observation_count > len(observation_rows),
            }

    def create_request_with_connection(self, connection, *args, **kwargs):
        return self.create_request(*args, connection=connection, **kwargs)

    def reconcile_submitted_inputs(self, case_id, persona_id):
        """Materialize submitted identifiers as reviewable, unverified evidence.

        Investigation inputs previously existed only inside request JSON. This
        idempotent bridge gives every supplied value an immutable ledger record
        and a normal account/claim group without treating it as independently
        corroborated or automatically approving it.
        """
        from maigret.web.pipeline_evidence import normalize_observation

        requests = self._table("requests")
        tasks = self._table("tasks")
        attempts = self._table("attempts")
        captured_inputs = 0
        captured_requests = 0
        with self.engine.begin() as connection:
            self._scope(connection, case_id, persona_id, lock=True)
            request_rows = list(
                connection.execute(
                    select(requests)
                    .where(
                        requests.c.case_id == case_id,
                        requests.c.persona_id == persona_id,
                    )
                    .order_by(requests.c.created_at, requests.c.id)
                ).mappings()
            )
            for request in request_rows:
                inputs = _reviewable_request_inputs(request)
                if not inputs:
                    continue
                capture_request_id = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        "openledger:submitted-inputs:" + request["id"],
                    )
                )
                existing = (
                    connection.execute(
                        select(requests.c.id).where(requests.c.id == capture_request_id)
                    )
                    .mappings()
                    .first()
                )
                if existing:
                    continue
                now = _now()
                observed_at = request["created_at"]
                if isinstance(observed_at, datetime) and observed_at.tzinfo is None:
                    # SQLite drops timezone metadata even though the value was
                    # written as UTC.  Restore the schema's declared timezone
                    # at this trusted database boundary.
                    observed_at = observed_at.replace(tzinfo=timezone.utc)
                capture_plan = {
                    "pipeline_id": PIPELINE_ID,
                    "request_id": capture_request_id,
                    "source_request_id": request["id"],
                    "system_input_capture": True,
                }
                connection.execute(
                    insert(requests).values(
                        id=capture_request_id,
                        case_id=case_id,
                        persona_id=persona_id,
                        pipeline_id=PIPELINE_ID,
                        job_id=None,
                        parent_request_id=None,
                        actor="system:input-reconciliation",
                        inputs=[
                            {"type": "source_request", "value": request["id"]}
                        ],
                        plan=capture_plan,
                        plan_hash=_digest(capture_plan),
                        idempotency_key="input-reconciliation:" + request["id"],
                        status="planned",
                        depth=0,
                        created_at=now,
                    )
                )
                task = dict(
                    id=_id(),
                    case_id=case_id,
                    persona_id=persona_id,
                    request_id=capture_request_id,
                    task_key=INPUT_EVIDENCE_TASK_KEY,
                    engine=INPUT_EVIDENCE_ENGINE,
                    platform=None,
                    input={"type": "submitted_inputs", "count": len(inputs)},
                    spec={
                        "task_id": INPUT_EVIDENCE_TASK_KEY,
                        "engine_id": INPUT_EVIDENCE_ENGINE,
                        "route_state": "active",
                        "retry_ceiling": 0,
                        "retention": {
                            "mode": "retained",
                            "final_eligible": True,
                        },
                        "system_input_capture": True,
                        "source_request_id": request["id"],
                    },
                    availability="active",
                    reason="Submitted values retained for analyst confirmation",
                    status="running",
                    outcome=None,
                    attempt_count=1,
                    retry_limit=0,
                    active_attempt_id=None,
                    updated_at=now,
                    created_at=now,
                )
                attempt = dict(
                    id=_id(),
                    case_id=case_id,
                    persona_id=persona_id,
                    task_id=task["id"],
                    number=1,
                    worker_id="system:input-reconciliation",
                    status="running",
                    outcome=None,
                    error=None,
                    finished_at=None,
                    created_at=now,
                )
                task["active_attempt_id"] = attempt["id"]
                connection.execute(insert(tasks).values(**task))
                connection.execute(insert(attempts).values(**attempt))
                observations = []
                for item in inputs:
                    input_type = str(item.get("type") or "").casefold()
                    value = str(item.get("value") or "").strip()
                    input_id = str(
                        item.get("input_id")
                        or "input:" + _digest([request["id"], input_type, value])
                    )
                    raw = {
                        "source_engine": INPUT_EVIDENCE_ENGINE,
                        "native_record_id": input_id,
                        "status": "candidate",
                        "observed_at": observed_at,
                        "locator": input_id,
                        "input_type": input_type,
                        "input_value": value,
                        "provenance_type": "investigator_supplied",
                        "input_provenance": list(item.get("provenance") or []),
                        # Query-planner derivation records can be structured
                        # objects. Keep them as input audit metadata; the
                        # observation-level derived_from field is reserved for
                        # immutable observation IDs.
                        "input_derivation": list(item.get("derived_from") or []),
                        "evidence_signals": {"investigator_supplied": True},
                        "retention": {
                            "mode": "retained",
                            "final_eligible": True,
                        },
                    }
                    if input_type in {"profile_url", "public_url"}:
                        raw.update(profile_url=value, source_url=value, claims=[])
                    else:
                        predicate = INPUT_CLAIM_PREDICATES.get(input_type)
                        raw["claims"] = (
                            [{"predicate": predicate, "value": value}]
                            if predicate
                            else []
                        )
                    observations.append(
                        normalize_observation(
                            raw,
                            case_id=case_id,
                            subject_id=persona_id,
                            request_id=capture_request_id,
                            task_id=task["id"],
                            attempt_id=attempt["id"],
                            engine=INPUT_EVIDENCE_ENGINE,
                            observed_at=observed_at,
                            engine_version="investigation-input-v1",
                            parser_version="pipeline-evidence-2",
                            retention_policy={
                                "mode": "retained",
                                "final_eligible": True,
                            },
                        )
                    )
                records = self._append_observations(
                    connection, attempt, task, observations
                )
                self._finish_attempt(
                    connection,
                    attempt,
                    task,
                    "candidate",
                    "Submitted evidence captured; independent corroboration remains pending",
                )
                captured_inputs += len(records)
                captured_requests += 1
        return {
            "case_id": case_id,
            "persona_id": persona_id,
            "captured_inputs": captured_inputs,
            "captured_requests": captured_requests,
        }

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
         …22727 tokens truncated…
                kept_count=kept_count,
                review_pending_count=review_pending_count,
                ai_ranked_group_count=ai_ranked_group_count,
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
        result["tasks"] = [
            task
            for task in result["tasks"]
            if task.get("engine") != INPUT_EVIDENCE_ENGINE
        ]
        coverage_labels = {
            "found": "Found",
            "candidate": "Candidate",
            "no_match": "No match",
            "could_not_check": "Could not check",
            "not_searched": "Not searched",
            "pending": "Pending",
        }
        for task in result["tasks"]:
            task["review_state"] = _task_review_state(task)
            task["review_state_label"] = coverage_labels[task["review_state"]]
            task_input = task["input"] if isinstance(task.get("input"), dict) else {}
            task["input_type"] = str(task_input.get("type") or "")
            task["input_value"] = str(
                task_input.get("value") or task_input.get("input_id") or "—"
            )
            latest_attempt = max(
                task.get("attempts") or [],
                key=lambda attempt: int(attempt.get("number") or 0),
                default={},
            )
            explanations = {
                "found": "The source returned at least one positive retained observation.",
                "candidate": "The source returned a possible match that requires analyst confirmation.",
                "no_match": "The source completed without a candidate. This is not proof of absence.",
                "could_not_check": "The source failed, timed out, was blocked, or returned incomplete output; no negative conclusion was drawn.",
                "not_searched": str(task.get("reason") or "This route was not eligible for the submitted input."),
                "pending": "This source has not reached a terminal state.",
            }
            task["review_explanation"] = str(
                latest_attempt.get("error")
                or task.get("reason")
                or explanations[task["review_state"]]
            )
        result["tasks_count"] = result["engine_task_count"]
        result.update(case_id=case_id, persona_id=persona_id)
        return _json(result)

    def ai_ranking_candidates(self, case_id, persona_id, *, limit=100):
        """Return a bounded non-conflicting set for one AI ranking pass."""
        limit = min(max(int(limit), 1), 100)
        groups, summaries = self._table("groups"), self._table("group_summaries")
        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            rows = connection.execute(
                select(
                    groups.c.id,
                    groups.c.kind,
                    summaries.c.normalized,
                    summaries.c.assessment,
                    summaries.c.observations,
                    summaries.c.observation_count,
                )
                .join(summaries, summaries.c.group_id == groups.c.id)
                .where(
                    groups.c.case_id == case_id,
                    groups.c.persona_id == persona_id,
                )
            ).mappings()
            candidates = []
            for row in rows:
                assessment = dict(row["assessment"] or {})
                conflicts = int(assessment.get("contradiction_count") or 0) + int(
                    assessment.get("group_conflict_count") or 0
                )
                if conflicts or int(row["observation_count"] or 0) < 1:
                    continue
                counts = dict(assessment.get("evidence_counts") or {})
                support = int(counts.get("support_origin_families") or 0)
                source_rows = []
                for observation in list(row["observations"] or [])[:3]:
                    if not isinstance(observation, dict):
                        continue
                    source_rows.append(
                        {
                            key: observation.get(key)
                            for key in (
                                "engine",
                                "status",
                                "source_name",
                                "source_url",
                                "reason",
                            )
                            if observation.get(key) not in (None, "")
                        }
                    )
                candidates.append(
                    {
                        "group_id": row["id"],
                        "kind": row["kind"],
                        "finding": row["normalized"],
                        "support_origin_families": support,
                        "evidence_status": assessment.get("evidence_status"),
                        "missing_evidence": list(assessment.get("missing_evidence") or [])[
                            :5
                        ],
                        "observation_count": int(row["observation_count"] or 0),
                        "source_examples": source_rows,
                        "_ranking_key": (
                            support,
                            int(row["observation_count"] or 0),
                            row["id"],
                        ),
                    }
                )
        candidates.sort(key=lambda item: item["_ranking_key"], reverse=True)
        for candidate in candidates:
            candidate.pop("_ranking_key", None)
        return _json(candidates[:limit])

    def apply_ai_rankings(self, case_id, persona_id, rankings, *, model):
        """Append model rankings to assessments without modifying source evidence."""
        if not isinstance(rankings, list) or len(rankings) > 100:
            raise ValueError("AI rankings must be a list of at most 100 records")
        normalized = {}
        for ranking in rankings:
            if not isinstance(ranking, dict):
                raise ValueError("AI ranking record must be an object")
            group_id = str(ranking.get("group_id") or "")
            priority = str(ranking.get("priority") or "").casefold()
            reason = str(ranking.get("reason") or "").strip()
            if (
                not group_id
                or group_id in normalized
                or priority not in {"high", "medium", "low"}
                or not isinstance(ranking.get("shortlisted"), bool)
                or not reason
            ):
                raise ValueError("AI ranking record is incomplete or duplicated")
            normalized[group_id] = {
                "schema_version": 1,
                "ranked_by": "openai",
                "model": str(model)[:100],
                "shortlisted": ranking["shortlisted"],
                "priority": priority,
                "reason": reason[:500],
            }
        changed = 0
        groups = self._table("groups")
        assessments = self._table("assessments")
        with self.engine.begin() as connection:
            self._scope(connection, case_id, persona_id, lock=True)
            for group_id, ranking in normalized.items():
                group = self._row(connection, groups, group_id)
                self._check_scope(group, case_id, persona_id)
                current = (
                    connection.execute(
                        select(assessments)
                        .where(assessments.c.group_id == group_id)
                        .order_by(
                            assessments.c.created_at.desc(), assessments.c.id.desc()
                        )
                        .limit(1)
                    )
                    .mappings()
                    .first()
                )
                if not current:
                    raise ValueError("AI ranking requires a materialized assessment")
                document = _json(current["document"])
                assessment = dict(document.get("assessment") or {})
                conflicts = int(assessment.get("contradiction_count") or 0) + int(
                    assessment.get("group_conflict_count") or 0
                )
                if conflicts:
                    raise ValueError("AI ranking cannot elevate conflicting evidence")
                assessment["ai_ranking"] = ranking
                document["assessment"] = assessment
                evidence_hash = _digest(document)
                if connection.scalar(
                    select(assessments.c.id).where(
                        assessments.c.group_id == group_id,
                        assessments.c.evidence_hash == evidence_hash,
                    )
                ):
                    continue
                connection.execute(
                    insert(assessments).values(
                        id=_id(),
                        case_id=case_id,
                        persona_id=persona_id,
                        group_id=group_id,
                        evidence_hash=evidence_hash,
                        document=document,
                        created_at=_now(),
                    )
                )
                self._refresh_group_summary(connection, group)
                changed += 1
            if changed:
                self._bump(connection, persona_id)
        return {"ranked": len(normalized), "changed": changed}

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
        from maigret.web.pipeline_evidence import (
            iter_legacy_claim_observations,
            legacy_claim_with_coordinates,
        )

        actor = _text(actor, "migration actor", 200)
        limit = min(max(int(limit), 1), 2000)
        records = []

        def coordinate_signature(claim, claim_id=None):
            document = legacy_claim_with_coordinates(claim)
            qualifiers = (
                document.get("qualifiers")
                if isinstance(document.get("qualifiers"), dict)
                else {}
            )
            try:
                latitude = float(qualifiers.get("latitude"))
                longitude = float(qualifiers.get("longitude"))
            except (TypeError, ValueError):
                return None
            identifier = str(claim_id or document.get("id") or "")
            return (identifier, latitude, longitude) if identifier else None

        def imported_coordinate_signature(claim, claim_id):
            qualifiers = (
                claim.get("qualifiers")
                if isinstance(claim.get("qualifiers"), dict)
                else {}
            )
            try:
                latitude = float(qualifiers.get("latitude"))
                longitude = float(qualifiers.get("longitude"))
            except (TypeError, ValueError):
                return None
            return (
                (str(claim_id), latitude, longitude)
                if (
                    claim_id
                    and math.isfinite(latitude)
                    and math.isfinite(longitude)
                    and -90 <= latitude <= 90
                    and -180 <= longitude <= 180
                )
                else None
            )

        with self.engine.connect() as connection:
            self._scope(connection, case_id, persona_id)
            observations_table = self._table("observations")
            existing_coordinate_signatures = set()
            for row in connection.execute(
                select(observations_table.c.payload).where(
                    observations_table.c.case_id == case_id,
                    observations_table.c.persona_id == persona_id,
                )
            ).mappings():
                document = row["payload"] if isinstance(row["payload"], dict) else {}
                native = (
                    document.get("payload")
                    if isinstance(document.get("payload"), dict)
                    else {}
                )
                legacy_claim_id = str(native.get("legacy_claim_id") or "")
                for imported_claim in document.get("claims") or []:
                    if not isinstance(imported_claim, dict):
                        continue
                    signature = imported_coordinate_signature(
                        imported_claim, claim_id=legacy_claim_id
                    )
                    if signature:
                        existing_coordinate_signatures.add(signature)
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
                signature = coordinate_signature(claim)
                repair_key = (
                    "legacy-coordinate-repair-v1:"
                    + _digest([case_id, persona_id, signature])
                    if (
                        existing
                        and existing["status"] == "completed"
                        and signature
                        and signature not in existing_coordinate_signatures
                    )
                    else None
                )
                repair_existing = (
                    connection.execute(
                        select(requests.c.id, requests.c.status).where(
                            requests.c.case_id == case_id,
                            requests.c.persona_id == persona_id,
                            requests.c.idempotency_key == repair_key,
                        )
                    )
                    .mappings()
                    .first()
                    if repair_key
                    else None
                )
                records.append(
                    {
                        "claim": claim,
                        "key": key,
                        "existing": dict(existing) if existing else None,
                        "coordinate_signature": signature,
                        "repair_key": repair_key,
                        "repair_existing": (
                            dict(repair_existing) if repair_existing else None
                        ),
                    }
                )
        report = {
            "case_id": case_id,
            "persona_id": persona_id,
            "dry_run": bool(dry_run),
            "claim_count": len(records),
            "pending_claim_count": sum(
                not item["existing"]
                or item["existing"]["status"] != "completed"
                for item in records
            ),
            "observation_count": sum(
                max(1, len(item["claim"]["evidence"])) for item in records
            ),
            "missing_provenance_count": sum(
                not item["claim"]["evidence"] for item in records
            ),
            "coordinate_claim_count": sum(
                item["coordinate_signature"] is not None for item in records
            ),
            "pending_coordinate_repair_count": sum(
                item["repair_key"] is not None for item in records
            ),
            "next_after_claim_id": claims[-1]["id"] if len(claims) == limit else None,
            "auto_finalized": False,
            "request_ids": [],
            "coordinate_repair_request_ids": [],
        }
        if dry_run or not records:
            return report
        from maigret.web.pipeline_consolidation import consolidate_observations

        for record in records:
            claim = record["claim"]
            existing = record["existing"]
            if existing and existing["status"] == "completed":
                report["request_ids"].append(existing["id"])
                repair_key = record["repair_key"]
                if not repair_key:
                    continue
                repair_existing = record["repair_existing"]
                if repair_existing and repair_existing["status"] == "completed":
                    raise ValueError(
                        "A completed legacy coordinate repair has no coordinate-bearing observation."
                    )
                request = self.create_request(
                    case_id,
                    persona_id,
                    [{"type": "legacy_coordinate", "value": claim["id"]}],
                    {
                        "pipeline_id": PIPELINE_ID,
                        "tasks": [
                            {
                                "engine_id": "legacy_coordinate_repair",
                                "route_state": "active",
                                "task_id": repair_key,
                            }
                        ],
                    },
                    actor=actor,
                    idempotency_key=repair_key,
                )
                task = request["tasks"][0]
                attempt = (
                    task["attempts"][0]
                    if task["attempts"]
                    else self.start_attempt(task["id"], "legacy-coordinate:" + actor)
                )
                observations = list(
                    iter_legacy_claim_observations(
                        [claim],
                        case_id=case_id,
                        subject_id=persona_id,
                        request_id=request["id"],
                        task_id=task["id"],
                        attempt_id=attempt["id"],
                        coordinate_repair=True,
                    )
                )
                self.record_observations(
                    attempt["id"],
                    observations,
                    outcome="candidate",
                    worker_id=attempt["worker_id"],
                )
                report["request_ids"].append(request["id"])
                report["coordinate_repair_request_ids"].append(request["id"])
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
                            "task_id": record["key"],
                        }
                    ],
                },
                actor=actor,
                idempotency_key=record["key"],
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

    def converge_legacy_reviews(self, case_id, persona_id, *, dry_run=True):
        """Project retained legacy review state into the P2 decision ledger.

        Evidence must already have been imported and consolidated.  Original
        claims, evidence and review rows are never changed.  One deterministic
        decision records the complete legacy history for each consolidated
        group.  Conflicting legacy states, or disagreement with an existing P2
        decision, become unresolved instead of being silently promoted.
        Pending-only groups remain undecided.
        """
        decision_map = {
            "approved": "include",
            "rejected": "reject",
            "uncertain": "unresolved",
            "pending": None,
        }

        def comparable(value):
            return "reject" if value in {"reject", "exclude"} else value

        groups = self._table("groups")
        observations = self._table("observations")
        memberships = self._table("group_observations")
        decisions = self._table("operator_decisions")
        with self.engine.begin() as connection:
            self._scope(connection, case_id, persona_id, lock=not dry_run)
            claims = [
                dict(row)
                for row in connection.execute(
                    select(persona_claims)
                    .where(persona_claims.c.persona_id == persona_id)
                    .order_by(persona_claims.c.id)
                ).mappings()
            ]
            reviews_by_claim = {claim["id"]: [] for claim in claims}
            if reviews_by_claim:
                for row in connection.execute(
                    select(claim_reviews)
                    .where(claim_reviews.c.claim_id.in_(reviews_by_claim))
                    .order_by(claim_reviews.c.created_at, claim_reviews.c.id)
                ).mappings():
                    reviews_by_claim[row["claim_id"]].append(_json(dict(row)))

            group_claims = {}
            membership_rows = connection.execute(
                select(
                    memberships.c.group_id,
                    observations.c.payload,
                )
                .join(observations, observations.c.id == memberships.c.observation_id)
                .join(groups, groups.c.id == memberships.c.group_id)
                .where(
                    groups.c.case_id == case_id,
                    groups.c.persona_id == persona_id,
                )
            ).mappings()
            known_claims = {claim["id"]: claim for claim in claims}
            for row in membership_rows:
                document = (
                    row["payload"] if isinstance(row["payload"], dict) else {}
                )
                native_payload = (
                    document.get("payload")
                    if isinstance(document.get("payload"), dict)
                    else {}
                )
                claim_id = str(
                    native_payload.get("legacy_claim_id")
                    or document.get("legacy_claim_id")
                    or ""
                )
                if claim_id in known_claims:
                    group_claims.setdefault(row["group_id"], set()).add(claim_id)

            existing_by_group = {}
            for row in connection.execute(
                select(decisions)
                .where(
                    decisions.c.case_id == case_id,
                    decisions.c.persona_id == persona_id,
                )
                .order_by(decisions.c.group_id, decisions.c.sequence)
            ).mappings():
                existing_by_group.setdefault(row["group_id"], []).append(dict(row))

            report = {
                "case_id": case_id,
                "persona_id": persona_id,
                "dry_run": bool(dry_run),
                "legacy_claim_count": len(claims),
                "mapped_claim_count": len(
                    {claim_id for values in group_claims.values() for claim_id in values}
                ),
                "group_count": len(group_claims),
                "decision_count": 0,
                "undecided_group_count": 0,
                "conflict_count": 0,
                "already_converged_count": 0,
                "conflicts": [],
                "auto_finalized": False,
                "qc_created": False,
            }
            planned = []
            for group_id in sorted(group_claims):
                claim_ids = sorted(group_claims[group_id])
                claim_documents = []
                desired = set()
                for claim_id in claim_ids:
                    claim = known_claims[claim_id]
                    target = decision_map[claim["review_status"]]
                    desired.add(
                        comparable(target) if target else "undecided"
                    )
                    claim_documents.append(
                        {
                            "legacy_claim_id": claim_id,
                            "current_status": claim["review_status"],
                            "reviewed_by": claim.get("reviewed_by"),
                            "reviewed_at": _json(claim.get("reviewed_at")),
                            "reviews": reviews_by_claim[claim_id],
                        }
                    )
                existing = existing_by_group.get(group_id, [])
                imported_fingerprints = {
                    str(
                        (row.get("details") or {})
                        .get("legacy_convergence", {})
                        .get("fingerprint", "")
                    )
                    for row in existing
                }
                # Imported checkpoints must never hide a later comparison with
                # an operator-authored P2 decision.  Scan the full append-only
                # ledger so replaying this importer cannot silently override a
                # human judgement that predates an earlier import.
                latest_human = next(
                    (
                        row
                        for row in reversed(existing)
                        if not (row.get("details") or {}).get(
                            "legacy_convergence"
                        )
                    ),
                    None,
                )
                conflict_reasons = []
                if len(desired) > 1:
                    conflict_reasons.append("legacy_states_disagree")
                target = next(iter(desired)) if len(desired) == 1 else None
                if target == "undecided":
                    target = None
                if (
                    target
                    and latest_human
                    and comparable(latest_human["decision"]) != target
                ):
                    conflict_reasons.append("legacy_and_p2_states_disagree")
                if target is None and not conflict_reasons:
                    report["undecided_group_count"] += 1
                    continue
                decision = "unresolved" if conflict_reasons else target
                convergence = {
                    "version": "p2-legacy-review-v1",
                    "group_id": group_id,
                    "decision": decision,
                    "conflicts": conflict_reasons,
                    "claims": claim_documents,
                }
                convergence["fingerprint"] = _digest(convergence)
                if convergence["fingerprint"] in imported_fingerprints:
                    report["already_converged_count"] += 1
                    continue
                if conflict_reasons:
                    report["conflict_count"] += 1
                    report["conflicts"].append(
                        {
                            "group_id": group_id,
                            "claim_ids": claim_ids,
                            "reasons": conflict_reasons,
                        }
                    )
                planned.append((group_id, decision, convergence))

            report["decision_count"] = len(planned)
            if dry_run:
                return _json(report)

            for group_id, decision, convergence in planned:
                sequence = (
                    connection.scalar(
                        select(func.max(decisions.c.sequence)).where(
                            decisions.c.group_id == group_id
                        )
                    )
                    or 0
                ) + 1
                connection.execute(
                    insert(decisions).values(
                        id=str(
                            uuid.uuid5(
                                uuid.NAMESPACE_URL,
                                "openledger:legacy-convergence:"
                                + convergence["fingerprint"],
                            )
                        ),
                        case_id=case_id,
                        persona_id=persona_id,
                        group_id=group_id,
                        sequence=sequence,
                        decision=decision,
                        actor="system:legacy-persona-convergence",
                        reason=(
                            "Legacy review states conflict; operator review is required."
                            if convergence["conflicts"]
                            else "Imported the current legacy review state with its complete audit history."
                        ),
                        details={"legacy_convergence": convergence},
                        created_at=_now(),
                    )
                )
            if planned:
                self._bump(connection, persona_id)
            return _json(report)

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
