"""Persistent case and investigation-job storage for OpenLedger."""

from __future__ import annotations

import hashlib
import math
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    event,
    insert,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Connection, Engine

metadata = MetaData()
json_document = JSON().with_variant(JSONB(), "postgresql")

cases = Table(
    "cases",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("title", String(500), nullable=False),
    Column("status", String(32), nullable=False, server_default="open"),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "status IN ('open', 'closed', 'archived')",
        name="ck_cases_status",
    ),
)

personas = Table(
    "personas",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "case_id",
        String(36),
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("display_name", String(500), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
Index("ix_personas_case_id", personas.c.case_id)

persona_claims = Table(
    "persona_claims",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "persona_id",
        String(36),
        ForeignKey("personas.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("field_name", String(64), nullable=False),
    Column("value", json_document, nullable=False),
    Column("display_value", Text, nullable=False),
    Column("normalized_value", Text, nullable=False),
    Column("confidence", Integer, nullable=False),
    Column("review_status", String(16), nullable=False, server_default="pending"),
    Column("source_engine", String(100), nullable=False),
    Column(
        "source_job_id",
        String(36),
        ForeignKey("investigation_jobs.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("fingerprint", String(64), nullable=False),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("reviewed_at", DateTime(timezone=True), nullable=True),
    Column("reviewed_by", String(200), nullable=True),
    Column("latitude", Float, nullable=True),
    Column("longitude", Float, nullable=True),
    CheckConstraint(
        "confidence >= 0 AND confidence <= 100",
        name="ck_persona_claims_confidence",
    ),
    CheckConstraint(
        "review_status IN ('pending', 'approved', 'rejected', 'uncertain')",
        name="ck_persona_claims_review_status",
    ),
    UniqueConstraint("persona_id", "fingerprint", name="uq_persona_claim_fingerprint"),
)
Index(
    "ix_persona_claims_persona_field",
    persona_claims.c.persona_id,
    persona_claims.c.field_name,
)
Index(
    "ix_persona_claims_relationship_projection",
    persona_claims.c.review_status,
    persona_claims.c.field_name,
    persona_claims.c.normalized_value,
)

claim_evidence = Table(
    "claim_evidence",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "claim_id",
        String(36),
        ForeignKey("persona_claims.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("evidence_type", String(64), nullable=False),
    Column("source_name", String(300), nullable=False),
    Column("source_url", Text, nullable=True),
    Column("details", json_document, nullable=False),
    Column("fingerprint", String(64), nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("claim_id", "fingerprint", name="uq_claim_evidence_fingerprint"),
)
Index("ix_claim_evidence_claim_id", claim_evidence.c.claim_id)

claim_reviews = Table(
    "claim_reviews",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "claim_id",
        String(36),
        ForeignKey("persona_claims.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("decision", String(16), nullable=False),
    Column("reviewer", String(200), nullable=False),
    Column("note", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "decision IN ('pending', 'approved', 'rejected', 'uncertain')",
        name="ck_claim_reviews_decision",
    ),
)
Index("ix_claim_reviews_claim_id", claim_reviews.c.claim_id)

investigation_jobs = Table(
    "investigation_jobs",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "case_id",
        String(36),
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("kind", String(32), nullable=False),
    Column("status", String(32), nullable=False),
    Column("usernames", json_document, nullable=False),
    Column("options", json_document, nullable=False),
    Column("progress", json_document, nullable=False),
    Column("result", json_document, nullable=True),
    Column("error", Text, nullable=True),
    Column("cancel_requested", Boolean, nullable=False, server_default="false"),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("worker_id", String(200), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("heartbeat_at", DateTime(timezone=True), nullable=True),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "status IN ('queued', 'running', 'cancel_requested', 'completed', "
        "'failed', 'cancelled', 'interrupted')",
        name="ck_investigation_jobs_status",
    ),
    CheckConstraint("attempts >= 0", name="ck_investigation_jobs_attempts"),
)
Index(
    "ix_investigation_jobs_status_created",
    investigation_jobs.c.status,
    investigation_jobs.c.created_at,
)
Index("ix_investigation_jobs_case_id", investigation_jobs.c.case_id)

investigation_events = Table(
    "investigation_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "job_id",
        String(36),
        ForeignKey("investigation_jobs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("event", json_document, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_investigation_events_job_id_id",
    investigation_events.c.job_id,
    investigation_events.c.id,
)


TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}
ACTIVE_STATUSES = {"queued", "running", "cancel_requested"}
WORKER_LOCK_KEY = 5714024849188199506


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def database_url_from_environment() -> str:
    """Build a database URL without a plaintext password environment variable."""
    explicit = os.getenv("DATABASE_URL", "").strip()
    if explicit:
        return explicit
    password_file = os.getenv("DATABASE_PASSWORD_FILE", "").strip()
    if not password_file:
        return ""
    with open(password_file, encoding="utf-8") as handle:
        password = handle.read().strip()
    if not password:
        raise RuntimeError("The database password file is empty")
    user = quote(os.getenv("DATABASE_USER", "openledger"), safe="")
    encoded_password = quote(password, safe="")
    host = os.getenv("DATABASE_HOST", "db")
    port = int(os.getenv("DATABASE_PORT", "5432"))
    name = quote(os.getenv("DATABASE_NAME", "openledger"), safe="")
    return f"postgresql+psycopg://{user}:{encoded_password}@{host}:{port}/{name}"


def _as_iso(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


class WorkerLock:
    """Own a worker connection and explicitly release its PostgreSQL session lock."""

    def __init__(
        self,
        connection: Connection,
        *,
        advisory_lock_key: Optional[int] = None,
    ):
        self._connection: Optional[Connection] = connection
        self._advisory_lock_key = advisory_lock_key

    def close(self) -> None:
        """Release the session lock before returning the connection to its pool."""
        connection = self._connection
        if connection is None:
            return
        self._connection = None
        try:
            if self._advisory_lock_key is not None:
                connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": self._advisory_lock_key},
                )
                connection.commit()
        finally:
            connection.close()


class CaseStore:
    """Small transactional repository shared by the web and worker processes."""

    def __init__(self, database_url: str, *, create_schema: bool = False):
        connect_args = (
            {"check_same_thread": False} if database_url.startswith("sqlite") else {}
        )
        self.engine: Engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args=connect_args,
        )
        if self.engine.dialect.name == "sqlite":

            @event.listens_for(self.engine, "connect")
            def enable_sqlite_foreign_keys(dbapi_connection, _connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        if create_schema:
            metadata.create_all(self.engine)

    def dispose(self) -> None:
        self.engine.dispose()

    def ping(self) -> bool:
        with self.engine.connect() as connection:
            connection.execute(select(1))
        return True

    def try_acquire_worker_lock(self):
        """Hold a session lock so only one production collector can run."""
        connection = self.engine.connect()
        if self.engine.dialect.name != "postgresql":
            return WorkerLock(connection)
        acquired = connection.execute(
            text("SELECT pg_try_advisory_lock(:lock_key)"),
            {"lock_key": WORKER_LOCK_KEY},
        ).scalar_one()
        connection.commit()
        if not acquired:
            connection.close()
            return None
        return WorkerLock(connection, advisory_lock_key=WORKER_LOCK_KEY)

    def create_investigation(
        self,
        usernames: Iterable[str],
        options: Dict[str, Any],
        *,
        kind: str = "live",
    ) -> str:
        normalized = [str(value).strip() for value in usernames if str(value).strip()]
        if not normalized:
            raise ValueError("At least one username is required")
        investigation_spec = options.get("investigation_spec")
        grouped = (
            isinstance(investigation_spec, dict)
            and investigation_spec.get("processing_mode") == "same_subject"
        )
        subject_label = (
            str(investigation_spec.get("subject_label") or "").strip()
            if isinstance(investigation_spec, dict)
            else ""
        )
        persona_names = [subject_label or normalized[0]] if grouped else normalized
        now = utcnow()
        case_id = str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        title = (subject_label if grouped and subject_label else ", ".join(normalized))[
            :500
        ]
        with self.engine.begin() as connection:
            connection.execute(
                insert(cases).values(
                    id=case_id,
                    title=title,
                    status="open",
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(personas),
                [
                    {
                        "id": str(uuid.uuid4()),
                        "case_id": case_id,
                        "display_name": username,
                        "created_at": now,
                    }
                    for username in persona_names
                ],
            )
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=case_id,
                    kind=kind,
                    status="queued",
                    usernames=normalized,
                    options=dict(options),
                    progress={"checked": 0, "total": None, "found": 0},
                    result=None,
                    error=None,
                    cancel_requested=False,
                    attempts=0,
                    created_at=now,
                    updated_at=now,
                )
            )
        self.append_event(job_id, {"type": "queued", "usernames": normalized})
        return job_id

    def repeat_persona_investigation(self, persona_id: str) -> str:
        """Queue a fresh collection for one existing persona in the same case."""
        now = utcnow()
        job_id = str(uuid.uuid4())
        with self.engine.begin() as connection:
            persona_statement = select(
                personas.c.case_id,
                personas.c.display_name,
            ).where(personas.c.id == persona_id)
            if self.engine.dialect.name == "postgresql":
                persona_statement = persona_statement.with_for_update()
            persona_row = connection.execute(persona_statement).mappings().first()
            if not persona_row:
                raise KeyError(persona_id)
            active_job = connection.scalar(
                select(investigation_jobs.c.id)
                .where(
                    investigation_jobs.c.case_id == persona_row["case_id"],
                    investigation_jobs.c.status.in_(ACTIVE_STATUSES),
                )
                .limit(1)
            )
            if active_job:
                raise ValueError("This case already has an active investigation")
            latest_job = (
                connection.execute(
                    select(
                        investigation_jobs.c.options,
                        investigation_jobs.c.usernames,
                    )
                    .where(investigation_jobs.c.case_id == persona_row["case_id"])
                    .order_by(investigation_jobs.c.created_at.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            latest_options: Dict[str, Any] = (
                dict(latest_job["options"] or {}) if latest_job else {}
            )
            latest_usernames = (
                list(latest_job["usernames"] or []) if latest_job else []
            )
            investigation_spec = latest_options.get("investigation_spec")
            grouped = (
                isinstance(investigation_spec, dict)
                and investigation_spec.get("processing_mode") == "same_subject"
            )
            username = str(persona_row["display_name"]).strip()
            usernames = (
                [str(value).strip() for value in latest_usernames]
                if grouped
                else [username]
            )
            usernames = [value for value in usernames if value]
            if not usernames:
                raise ValueError("No searchable account identifiers are available")
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=persona_row["case_id"],
                    kind="refresh",
                    status="queued",
                    usernames=usernames,
                    options=latest_options,
                    progress={"checked": 0, "total": None, "found": 0},
                    result=None,
                    error=None,
                    cancel_requested=False,
                    attempts=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                update(cases)
                .where(cases.c.id == persona_row["case_id"])
                .values(updated_at=now)
            )
        self.append_event(
            job_id,
            {"type": "queued", "usernames": usernames, "reason": "persona_refresh"},
        )
        return job_id

    def import_legacy_result(self, job_id: str, result: Dict[str, Any]) -> bool:
        """Index one existing file-backed terminal result without changing its files."""
        if self.get_job(job_id):
            return False
        status = str(result.get("status", "failed"))
        if status not in TERMINAL_STATUSES:
            raise ValueError("Only terminal legacy investigations can be imported")
        usernames = [
            str(value).strip()
            for value in result.get("usernames", [])
            if str(value).strip()
        ]
        now = utcnow()
        case_id = str(uuid.uuid4())
        with self.engine.begin() as connection:
            if self.engine.dialect.name == "postgresql":
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:job_id))"),
                    {"job_id": job_id},
                )
            if connection.scalar(
                select(investigation_jobs.c.id).where(investigation_jobs.c.id == job_id)
            ):
                return False
            connection.execute(
                insert(cases).values(
                    id=case_id,
                    title=(", ".join(usernames) or f"Imported investigation {job_id}")[
                        :500
                    ],
                    status="open",
                    created_at=now,
                    updated_at=now,
                )
            )
            if usernames:
                connection.execute(
                    insert(personas),
                    [
                        {
                            "id": str(uuid.uuid4()),
                            "case_id": case_id,
                            "display_name": username,
                            "created_at": now,
                        }
                        for username in usernames
                    ],
                )
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=case_id,
                    kind="legacy",
                    status=status,
                    usernames=usernames,
                    options={},
                    progress={
                        "checked": None,
                        "total": None,
                        "found": int(result.get("found_count", 0) or 0),
                    },
                    result=dict(result),
                    error=str(result.get("error")) if result.get("error") else None,
                    cancel_requested=status == "cancelled",
                    attempts=1,
                    created_at=now,
                    started_at=now,
                    heartbeat_at=now,
                    completed_at=now,
                    updated_at=now,
                )
            )
        self.append_event(job_id, {"type": "imported", "status": status})
        return True

    def claim_next(self, worker_id: str) -> Optional[Dict[str, Any]]:
        now = utcnow()
        with self.engine.begin() as connection:
            statement = (
                select(investigation_jobs)
                .where(investigation_jobs.c.status == "queued")
                .order_by(investigation_jobs.c.created_at)
                .limit(1)
            )
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update(skip_locked=True)
            row = connection.execute(statement).mappings().first()
            if not row:
                return None
            connection.execute(
                update(investigation_jobs)
                .where(
                    investigation_jobs.c.id == row["id"],
                    investigation_jobs.c.status == "queued",
                )
                .values(
                    status="running",
                    worker_id=worker_id,
                    attempts=investigation_jobs.c.attempts + 1,
                    started_at=row["started_at"] or now,
                    heartbeat_at=now,
                    updated_at=now,
                )
            )
        self.append_event(row["id"], {"type": "running"})
        return self.get_job(row["id"])

    def append_event(self, job_id: str, event: Dict[str, Any]) -> int:
        now = utcnow()
        progress_updates: Dict[str, Any] = {}
        with self.engine.begin() as connection:
            current = connection.execute(
                select(investigation_jobs.c.progress).where(
                    investigation_jobs.c.id == job_id
                )
            ).scalar_one_or_none()
            if current is None:
                raise KeyError(job_id)
            progress = dict(current or {})
            event_type = event.get("type")
            if event_type == "start":
                progress["total"] = event.get("total")
                progress["username"] = event.get("username")
            elif event_type == "progress":
                progress["checked"] = event.get("checked", progress.get("checked", 0))
                progress["total"] = event.get("total", progress.get("total"))
                progress["site"] = event.get("site")
            elif event_type == "found":
                progress["found"] = int(progress.get("found", 0)) + 1
            progress_updates["progress"] = progress
            progress_updates["heartbeat_at"] = now
            progress_updates["updated_at"] = now
            result = connection.execute(
                insert(investigation_events)
                .values(job_id=job_id, event=dict(event), created_at=now)
                .returning(investigation_events.c.id)
            )
            event_id = int(result.scalar_one())
            connection.execute(
                update(investigation_jobs)
                .where(investigation_jobs.c.id == job_id)
                .values(**progress_updates)
            )
        return event_id

    def get_events(self, job_id: str, after_id: int = 0, limit: int = 500):
        with self.engine.connect() as connection:
            rows = connection.execute(
                select(investigation_events)
                .where(
                    investigation_events.c.job_id == job_id,
                    investigation_events.c.id > max(0, int(after_id)),
                )
                .order_by(investigation_events.c.id)
                .limit(min(max(1, int(limit)), 1000))
            ).mappings()
            return [
                {
                    "id": row["id"],
                    "event": dict(row["event"]),
                    "created_at": _as_iso(row["created_at"]),
                }
                for row in rows
            ]

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(investigation_jobs).where(investigation_jobs.c.id == job_id)
                )
                .mappings()
                .first()
            )
        return self._serialize_job(row) if row else None

    def list_jobs(self, limit: int = 500):
        with self.engine.connect() as connection:
            rows = connection.execute(
                select(investigation_jobs)
                .order_by(investigation_jobs.c.created_at.desc())
                .limit(min(max(1, int(limit)), 2000))
            ).mappings()
            return [self._serialize_job(row) for row in rows]

    def list_cases(self, limit: int = 500):
        """List case summaries with their personas and latest job state."""
        with self.engine.connect() as connection:
            case_rows = list(
                connection.execute(
                    select(cases)
                    .order_by(cases.c.updated_at.desc())
                    .limit(min(max(1, int(limit)), 2000))
                ).mappings()
            )
            summaries = []
            for case_row in case_rows:
                persona_rows = list(
                    connection.execute(
                        select(personas.c.id, personas.c.display_name)
                        .where(personas.c.case_id == case_row["id"])
                        .order_by(personas.c.created_at)
                    ).mappings()
                )
                latest_job = (
                    connection.execute(
                        select(investigation_jobs)
                        .where(investigation_jobs.c.case_id == case_row["id"])
                        .order_by(investigation_jobs.c.created_at.desc())
                        .limit(1)
                    )
                    .mappings()
                    .first()
                )
                summaries.append(
                    {
                        "id": case_row["id"],
                        "title": case_row["title"],
                        "status": case_row["status"],
                        "created_at": _as_iso(case_row["created_at"]),
                        "updated_at": _as_iso(case_row["updated_at"]),
                        "personas": [dict(row) for row in persona_rows],
                        "latest_job": (
                            self._serialize_job(latest_job) if latest_job else None
                        ),
                    }
                )
        return summaries

    def get_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        with self.engine.connect() as connection:
            case_row = (
                connection.execute(select(cases).where(cases.c.id == case_id))
                .mappings()
                .first()
            )
            if not case_row:
                return None
            persona_rows = list(
                connection.execute(
                    select(personas)
                    .where(personas.c.case_id == case_id)
                    .order_by(personas.c.created_at)
                ).mappings()
            )
            job_rows = list(
                connection.execute(
                    select(investigation_jobs)
                    .where(investigation_jobs.c.case_id == case_id)
                    .order_by(investigation_jobs.c.created_at.desc())
                ).mappings()
            )
        return {
            "id": case_row["id"],
            "title": case_row["title"],
            "status": case_row["status"],
            "created_at": _as_iso(case_row["created_at"]),
            "updated_at": _as_iso(case_row["updated_at"]),
            "personas": [
                {
                    "id": row["id"],
                    "display_name": row["display_name"],
                    "created_at": _as_iso(row["created_at"]),
                }
                for row in persona_rows
            ],
            "jobs": [self._serialize_job(row) for row in job_rows],
        }

    def get_persona(self, persona_id: str) -> Optional[Dict[str, Any]]:
        """Load a persona, its evidence-backed claims, and review history."""
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(
                        personas,
                        cases.c.title.label("case_title"),
                        cases.c.status.label("case_status"),
                    )
                    .join(cases, cases.c.id == personas.c.case_id)
                    .where(personas.c.id == persona_id)
                )
                .mappings()
                .first()
            )
            if not row:
                return None
            claim_rows = list(
                connection.execute(
                    select(persona_claims)
                    .where(persona_claims.c.persona_id == persona_id)
                    .order_by(
                        persona_claims.c.field_name,
                        persona_claims.c.confidence.desc(),
                        persona_claims.c.created_at,
                    )
                ).mappings()
            )
            evidence_by_claim: Dict[str, list] = {}
            reviews_by_claim: Dict[str, list] = {}
            claim_ids = [claim_row["id"] for claim_row in claim_rows]
            if claim_ids:
                for evidence_row in connection.execute(
                    select(claim_evidence)
                    .where(claim_evidence.c.claim_id.in_(claim_ids))
                    .order_by(claim_evidence.c.observed_at.desc())
                ).mappings():
                    evidence_by_claim.setdefault(evidence_row["claim_id"], []).append(
                        evidence_row
                    )
                for review_row in connection.execute(
                    select(claim_reviews)
                    .where(claim_reviews.c.claim_id.in_(claim_ids))
                    .order_by(claim_reviews.c.created_at.desc())
                ).mappings():
                    reviews_by_claim.setdefault(review_row["claim_id"], []).append(
                        review_row
                    )
            serialized_claims = []
            for claim_row in claim_rows:
                serialized_claims.append(
                    self._serialize_claim(
                        claim_row,
                        evidence_by_claim.get(claim_row["id"], []),
                        reviews_by_claim.get(claim_row["id"], []),
                    )
                )
        return {
            "id": row["id"],
            "case_id": row["case_id"],
            "case_title": row["case_title"],
            "case_status": row["case_status"],
            "display_name": row["display_name"],
            "created_at": _as_iso(row["created_at"]),
            "claims": serialized_claims,
        }

    def get_claim(self, claim_id: str) -> Optional[Dict[str, Any]]:
        """Load the bounded claim fields needed before an analyst review."""
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(
                        persona_claims.c.id,
                        persona_claims.c.persona_id,
                        persona_claims.c.field_name,
                        persona_claims.c.display_value,
                        persona_claims.c.review_status,
                        persona_claims.c.latitude,
                        persona_claims.c.longitude,
                    ).where(persona_claims.c.id == claim_id)
                )
                .mappings()
                .first()
            )
        return dict(row) if row else None

    @staticmethod
    def _upsert_persona_candidates(
        connection: Connection,
        *,
        persona_id: str,
        job_id: str,
        candidates: Iterable[Dict[str, Any]],
        now: datetime,
    ) -> int:
        """Persist validated candidates without changing a human decision."""
        synchronized = 0
        for candidate in candidates:
            identity_match = persona_claims.c.fingerprint == candidate["fingerprint"]
            if (
                candidate.get("source_engine") == "openai_web_research"
                and candidate.get("field_name") == "social_account"
            ):
                identity_match = or_(
                    identity_match,
                    (
                        (persona_claims.c.field_name == "social_account")
                        & (persona_claims.c.display_value == candidate["display_value"])
                    ),
                )
            existing = (
                connection.execute(
                    select(persona_claims).where(
                        persona_claims.c.persona_id == persona_id,
                        identity_match,
                    )
                )
                .mappings()
                .first()
            )
            if existing:
                claim_id = existing["id"]
                confidence = int(existing["confidence"])
                if (
                    candidate.get("source_engine") != "openai_web_research"
                    or existing["review_status"] == "pending"
                ):
                    confidence = max(confidence, int(candidate["confidence"]))
                updated_values = {
                    "value": candidate["value"],
                    "display_value": candidate["display_value"],
                    "normalized_value": candidate["normalized_value"],
                    "confidence": confidence,
                    "source_job_id": job_id,
                    "last_seen_at": now,
                    "updated_at": now,
                }
                if (
                    existing["review_status"] == "pending"
                    and existing["latitude"] is None
                    and existing["longitude"] is None
                    and candidate.get("latitude") is not None
                    and candidate.get("longitude") is not None
                ):
                    updated_values.update(
                        latitude=candidate["latitude"],
                        longitude=candidate["longitude"],
                    )
                connection.execute(
                    update(persona_claims)
                    .where(persona_claims.c.id == claim_id)
                    .values(**updated_values)
                )
            else:
                claim_id = str(uuid.uuid4())
                connection.execute(
                    insert(persona_claims).values(
                        id=claim_id,
                        persona_id=persona_id,
                        field_name=candidate["field_name"],
                        value=candidate["value"],
                        display_value=candidate["display_value"],
                        normalized_value=candidate["normalized_value"],
                        confidence=candidate["confidence"],
                        review_status="pending",
                        source_engine=candidate["source_engine"],
                        source_job_id=job_id,
                        fingerprint=candidate["fingerprint"],
                        latitude=candidate.get("latitude"),
                        longitude=candidate.get("longitude"),
                        first_seen_at=now,
                        last_seen_at=now,
                        created_at=now,
                        updated_at=now,
                    )
                )
            for evidence in candidate["evidence"]:
                present = connection.scalar(
                    select(claim_evidence.c.id).where(
                        claim_evidence.c.claim_id == claim_id,
                        claim_evidence.c.fingerprint == evidence["fingerprint"],
                    )
                )
                if present:
                    continue
                connection.execute(
                    insert(claim_evidence).values(
                        id=str(uuid.uuid4()),
                        claim_id=claim_id,
                        evidence_type=evidence["evidence_type"],
                        source_name=evidence["source_name"],
                        source_url=evidence["source_url"] or None,
                        details=evidence["details"],
                        fingerprint=evidence["fingerprint"],
                        observed_at=now,
                    )
                )
            synchronized += 1
        return synchronized

    def sync_persona_claims(self, job_id: str, result: Dict[str, Any]) -> int:
        """Upsert deterministic claims while preserving every human decision."""
        from maigret.web.persona_intelligence import extract_persona_claims

        now = utcnow()
        synchronized = 0
        with self.engine.begin() as connection:
            job_row = (
                connection.execute(
                    select(
                        investigation_jobs.c.case_id,
                        investigation_jobs.c.options,
                    ).where(investigation_jobs.c.id == job_id)
                )
                .mappings()
                .first()
            )
            if not job_row:
                raise KeyError(job_id)
            case_id = job_row["case_id"]
            persona_rows = list(
                connection.execute(
                    select(personas.c.id, personas.c.display_name).where(
                        personas.c.case_id == case_id
                    )
                ).mappings()
            )
            personas_by_name = {
                str(row["display_name"]).strip().casefold(): row["id"]
                for row in persona_rows
            }
            investigation_spec = dict(job_row["options"] or {}).get(
                "investigation_spec"
            )
            grouped_persona_id = (
                persona_rows[0]["id"]
                if isinstance(investigation_spec, dict)
                and investigation_spec.get("processing_mode") == "same_subject"
                and len(persona_rows) == 1
                else None
            )
            for report in result.get("individual_reports") or []:
                username = str(report.get("username") or "").strip()
                persona_id = grouped_persona_id or personas_by_name.get(
                    username.casefold()
                )
                if not persona_id:
                    continue
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=persona_id,
                    job_id=job_id,
                    candidates=extract_persona_claims(report),
                    now=now,
                )
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(updated_at=now)
            )
        return synchronized

    def sync_ai_persona_claims(
        self,
        job_id: str,
        raw_proposals: Any,
        *,
        sources: Iterable[Dict[str, Any]],
        usernames: Iterable[str],
        model: str,
    ) -> Dict[str, Any]:
        """Validate and persist cited AI proposals as pending review records."""
        from maigret.web.persona_intelligence import extract_ai_persona_claims

        diagnostics: Dict[str, Any] = {}
        candidates = extract_ai_persona_claims(
            raw_proposals,
            sources=sources,
            usernames=usernames,
            model=model,
            diagnostics=diagnostics,
        )
        now = utcnow()
        synchronized = 0
        accepted_proposals = []
        with self.engine.begin() as connection:
            job_row = (
                connection.execute(
                    select(
                        investigation_jobs.c.case_id,
                        investigation_jobs.c.options,
                    ).where(investigation_jobs.c.id == job_id)
                )
                .mappings()
                .first()
            )
            if not job_row:
                raise KeyError(job_id)
            case_id = job_row["case_id"]
            persona_rows = list(
                connection.execute(
                    select(personas.c.id, personas.c.display_name).where(
                        personas.c.case_id == case_id
                    )
                ).mappings()
            )
            personas_by_name = {
                str(row["display_name"]).strip().casefold(): row["id"]
                for row in persona_rows
            }
            investigation_spec = dict(job_row["options"] or {}).get(
                "investigation_spec"
            )
            grouped_persona_id = (
                persona_rows[0]["id"]
                if isinstance(investigation_spec, dict)
                and investigation_spec.get("processing_mode") == "same_subject"
                and len(persona_rows) == 1
                else None
            )
            for candidate in candidates:
                persona_id = grouped_persona_id or personas_by_name.get(
                    candidate["username"].casefold()
                )
                if not persona_id:
                    continue
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=persona_id,
                    job_id=job_id,
                    candidates=[candidate],
                    now=now,
                )
                accepted_proposals.append(
                    {
                        "username": candidate["username"],
                        "field_name": candidate["field_name"],
                        "value": (
                            candidate["value"].get("url", "")
                            if isinstance(candidate["value"], dict)
                            else candidate["value"]
                        ),
                        "confidence": candidate["confidence"],
                        "source_url": candidate["evidence"][0]["source_url"],
                        "source_title": candidate["evidence"][0]["source_name"],
                        "reason": candidate["evidence"][0]["details"][
                            "proposal_reason"
                        ],
                    }
                )
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(updated_at=now)
            )
        return {
            "count": synchronized,
            "case_id": str(case_id),
            "proposals": accepted_proposals,
            "diagnostics": diagnostics,
        }

    def review_claim(
        self,
        claim_id: str,
        decision: str,
        reviewer: str,
        note: str = "",
        latitude: Optional[str] = None,
        longitude: Optional[str] = None,
    ) -> Optional[str]:
        """Record an auditable human decision and return the persona id."""
        if decision not in {"pending", "approved", "rejected", "uncertain"}:
            raise ValueError("Invalid claim review decision")
        reviewer = str(reviewer).strip()[:200]
        if not reviewer:
            raise ValueError("A reviewer is required")
        coordinates = self._validated_coordinates(latitude, longitude)
        now = utcnow()
        with self.engine.begin() as connection:
            claim = (
                connection.execute(
                    select(
                        persona_claims.c.persona_id,
                        persona_claims.c.field_name,
                    ).where(persona_claims.c.id == claim_id)
                )
                .mappings()
                .first()
            )
            if not claim:
                return None
            if coordinates and claim["field_name"] not in {
                "address",
                "current_location",
            }:
                raise ValueError(
                    "Coordinates can only be attached to a location record"
                )
            values = {
                "review_status": decision,
                "reviewed_at": now,
                "reviewed_by": reviewer,
                "updated_at": now,
            }
            if coordinates:
                values.update(
                    latitude=coordinates[0],
                    longitude=coordinates[1],
                )
            connection.execute(
                update(persona_claims)
                .where(persona_claims.c.id == claim_id)
                .values(**values)
            )
            connection.execute(
                insert(claim_reviews).values(
                    claim_id=claim_id,
                    decision=decision,
                    reviewer=reviewer,
                    note=str(note).strip()[:2000] or None,
                    created_at=now,
                )
            )
        return str(claim["persona_id"])

    @staticmethod
    def _validated_coordinates(
        latitude: Optional[str], longitude: Optional[str]
    ) -> Optional[tuple[float, float]]:
        """Validate analyst-supplied coordinates without external geocoding."""
        raw_latitude = str(latitude or "").strip()
        raw_longitude = str(longitude or "").strip()
        if not raw_latitude and not raw_longitude:
            return None
        if not raw_latitude or not raw_longitude:
            raise ValueError("Latitude and longitude must be provided together")
        try:
            parsed_latitude = float(raw_latitude)
            parsed_longitude = float(raw_longitude)
        except ValueError as error:
            raise ValueError("Latitude and longitude must be numbers") from error
        if not math.isfinite(parsed_latitude) or not math.isfinite(parsed_longitude):
            raise ValueError("Latitude and longitude must be finite numbers")
        if not -90 <= parsed_latitude <= 90:
            raise ValueError("Latitude must be between -90 and 90")
        if not -180 <= parsed_longitude <= 180:
            raise ValueError("Longitude must be between -180 and 180")
        return parsed_latitude, parsed_longitude

    def build_relationship_graph(self, case_id: Optional[str] = None) -> Dict[str, Any]:
        """Project approved, exact shared attributes across two or more personas."""
        relationship_fields = {
            "email",
            "phone",
            "address",
            "current_location",
            "social_account",
            "website",
            "occupation",
            "company",
            "company_ownership",
            "vehicle_ownership",
        }
        statement = (
            select(
                persona_claims.c.id.label("claim_id"),
                persona_claims.c.field_name,
                persona_claims.c.display_value,
                persona_claims.c.normalized_value,
                persona_claims.c.confidence,
                personas.c.id.label("persona_id"),
                personas.c.display_name.label("persona_name"),
                cases.c.id.label("case_id"),
                cases.c.title.label("case_title"),
            )
            .join(personas, personas.c.id == persona_claims.c.persona_id)
            .join(cases, cases.c.id == personas.c.case_id)
            .where(
                persona_claims.c.review_status == "approved",
                persona_claims.c.field_name.in_(relationship_fields),
            )
        )
        if case_id:
            statement = statement.where(cases.c.id == case_id)
        with self.engine.connect() as connection:
            rows = list(connection.execute(statement).mappings())
            evidence_by_claim: Dict[str, list] = {}
            claim_ids = [str(row["claim_id"]) for row in rows]
            if claim_ids:
                for evidence in connection.execute(
                    select(claim_evidence).where(
                        claim_evidence.c.claim_id.in_(claim_ids)
                    )
                ).mappings():
                    evidence_by_claim.setdefault(
                        str(evidence["claim_id"]), []
                    ).append(
                        {
                            "name": str(evidence["source_name"]),
                            "url": evidence["source_url"],
                            "type": str(evidence["evidence_type"]),
                        }
                    )

        shared: Dict[tuple[str, str], list] = {}
        for row in rows:
            key = (str(row["field_name"]), str(row["normalized_value"]))
            shared.setdefault(key, []).append(row)

        nodes: list[Dict[str, Any]] = []
        edges: list[Dict[str, Any]] = []
        persona_nodes: Dict[str, Dict[str, Any]] = {}
        field_counts: Dict[str, int] = {}
        for (field_name, normalized_value), candidates in shared.items():
            distinct_personas = {str(row["persona_id"]) for row in candidates}
            if len(distinct_personas) < 2:
                continue
            attribute_id = (
                f"attribute:{field_name}:"
                + hashlib.sha256(normalized_value.encode("utf-8")).hexdigest()[:20]
            )
            display_value = str(candidates[0]["display_value"])
            field_counts[field_name] = field_counts.get(field_name, 0) + 1
            nodes.append(
                {
                    "id": attribute_id,
                    "label": display_value,
                    "kind": "attribute",
                    "field_name": field_name,
                    "persona_count": len(distinct_personas),
                }
            )
            seen_personas = set()
            for row in candidates:
                persona_id = str(row["persona_id"])
                if persona_id in seen_personas:
                    continue
                seen_personas.add(persona_id)
                persona_nodes.setdefault(
                    persona_id,
                    {
                        "id": f"persona:{persona_id}",
                        "label": str(row["persona_name"]),
                        "kind": "persona",
                        "persona_id": persona_id,
                        "case_id": str(row["case_id"]),
                        "case_title": str(row["case_title"]),
                    },
                )
                edges.append(
                    {
                        "id": f"edge:{row['claim_id']}",
                        "from": f"persona:{persona_id}",
                        "to": attribute_id,
                        "label": field_name.replace("_", " "),
                        "field_name": field_name,
                        "confidence": int(row["confidence"]),
                        "claim_id": str(row["claim_id"]),
                        "sources": evidence_by_claim.get(
                            str(row["claim_id"]), []
                        )[:10],
                    }
                )
        nodes = list(persona_nodes.values()) + nodes
        return {
            "mode": "shared",
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "persona_count": len(persona_nodes),
                "shared_attribute_count": len(nodes) - len(persona_nodes),
                "connection_count": len(edges),
                "field_counts": field_counts,
            },
        }

    def build_persona_graph(self, persona_id: str) -> Dict[str, Any]:
        """Build a reviewable Persona-to-claim-to-source evidence graph."""
        persona = self.get_persona(persona_id)
        if not persona:
            raise KeyError(persona_id)
        nodes = [
            {
                "id": f"persona:{persona_id}",
                "label": persona["display_name"],
                "kind": "persona",
                "persona_id": persona_id,
                "case_id": persona["case_id"],
                "case_title": persona["case_title"],
            }
        ]
        edges = []
        seen_sources = set()
        field_counts: Dict[str, int] = {}
        graph_claims = sorted(
            persona["claims"],
            key=lambda claim: (
                claim["review_status"] != "approved",
                -int(claim["confidence"]),
                claim["field_name"],
            ),
        )
        graph_claims = [
            claim for claim in graph_claims if claim["review_status"] != "rejected"
        ]
        displayed_claims = graph_claims[:120]
        for claim in displayed_claims:
            claim_node = f"claim:{claim['id']}"
            field_counts[claim["field_name"]] = (
                field_counts.get(claim["field_name"], 0) + 1
            )
            nodes.append(
                {
                    "id": claim_node,
                    "label": claim["display_value"],
                    "kind": "claim",
                    "claim_id": claim["id"],
                    "field_name": claim["field_name"],
                    "confidence": claim["confidence"],
                    "review_status": claim["review_status"],
                }
            )
            edges.append(
                {
                    "id": f"persona-claim:{claim['id']}",
                    "from": f"persona:{persona_id}",
                    "to": claim_node,
                    "label": claim["field_name"].replace("_", " "),
                    "field_name": claim["field_name"],
                }
            )
            seen_claim_sources = set()
            for evidence in claim["evidence"]:
                source_key = evidence.get("source_url") or evidence["source_name"]
                source_id = "source:" + hashlib.sha256(
                    str(source_key).encode("utf-8")
                ).hexdigest()[:20]
                if source_id in seen_claim_sources:
                    continue
                seen_claim_sources.add(source_id)
                if source_id not in seen_sources:
                    nodes.append(
                        {
                            "id": source_id,
                            "label": evidence["source_name"],
                            "kind": "source",
                            "url": evidence.get("source_url"),
                            "evidence_type": evidence["evidence_type"],
                        }
                    )
                    seen_sources.add(source_id)
                edges.append(
                    {
                        "id": f"claim-source:{claim['id']}:{source_id}",
                        "from": claim_node,
                        "to": source_id,
                        "label": "supported by",
                        "field_name": claim["field_name"],
                    }
                )
        return {
            "mode": "persona",
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "persona_count": 1,
                "claim_count": len(displayed_claims),
                "source_count": len(seen_sources),
                "pending_count": sum(
                    claim["review_status"] in {"pending", "uncertain"}
                    for claim in displayed_claims
                ),
                "field_counts": field_counts,
                "truncated_count": max(0, len(graph_claims) - len(displayed_claims)),
            },
        }

    @staticmethod
    def _serialize_claim(claim_row, evidence_rows, review_rows) -> Dict[str, Any]:
        return {
            "id": claim_row["id"],
            "field_name": claim_row["field_name"],
            "value": claim_row["value"],
            "display_value": claim_row["display_value"],
            "confidence": int(claim_row["confidence"]),
            "review_status": claim_row["review_status"],
            "source_engine": claim_row["source_engine"],
            "source_job_id": claim_row["source_job_id"],
            "first_seen_at": _as_iso(claim_row["first_seen_at"]),
            "last_seen_at": _as_iso(claim_row["last_seen_at"]),
            "reviewed_at": _as_iso(claim_row["reviewed_at"]),
            "reviewed_by": claim_row["reviewed_by"],
            "normalized_value": claim_row["normalized_value"],
            "latitude": claim_row["latitude"],
            "longitude": claim_row["longitude"],
            "evidence": [
                {
                    "id": row["id"],
                    "evidence_type": row["evidence_type"],
                    "source_name": row["source_name"],
                    "source_url": row["source_url"],
                    "details": dict(row["details"] or {}),
                    "observed_at": _as_iso(row["observed_at"]),
                }
                for row in evidence_rows
            ],
            "reviews": [
                {
                    "decision": row["decision"],
                    "reviewer": row["reviewer"],
                    "note": row["note"],
                    "created_at": _as_iso(row["created_at"]),
                }
                for row in review_rows
            ],
        }

    def request_cancel(self, job_id: str) -> bool:
        now = utcnow()
        event_type = None
        with self.engine.begin() as connection:
            statement = select(
                investigation_jobs.c.status,
                investigation_jobs.c.usernames,
            ).where(investigation_jobs.c.id == job_id)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = connection.execute(statement).mappings().first()
            if not row or row["status"] not in {"queued", "running"}:
                return False
            if row["status"] == "queued":
                cancellation = {
                    "status": "cancelled",
                    "error": (
                        "The queued investigation was cancelled before it started."
                    ),
                    "usernames": list(row["usernames"] or []),
                    "session_folder": f"search_{job_id}",
                }
                connection.execute(
                    update(investigation_jobs)
                    .where(investigation_jobs.c.id == job_id)
                    .values(
                        status="cancelled",
                        result=cancellation,
                        error=cancellation["error"],
                        cancel_requested=True,
                        completed_at=now,
                        heartbeat_at=now,
                        updated_at=now,
                    )
                )
                event_type = "cancelled"
            else:
                connection.execute(
                    update(investigation_jobs)
                    .where(investigation_jobs.c.id == job_id)
                    .values(
                        status="cancel_requested",
                        cancel_requested=True,
                        updated_at=now,
                    )
                )
                event_type = "cancel_requested"
        self.append_event(job_id, {"type": event_type})
        return True

    def is_cancel_requested(self, job_id: str) -> bool:
        with self.engine.connect() as connection:
            value = connection.execute(
                select(investigation_jobs.c.cancel_requested).where(
                    investigation_jobs.c.id == job_id
                )
            ).scalar_one_or_none()
        return bool(value)

    def finish(self, job_id: str, result: Dict[str, Any]) -> None:
        status = str(result.get("status", "failed"))
        if status not in TERMINAL_STATUSES:
            status = "failed"
        now = utcnow()
        with self.engine.begin() as connection:
            connection.execute(
                update(investigation_jobs)
                .where(investigation_jobs.c.id == job_id)
                .values(
                    status=status,
                    result=dict(result),
                    error=str(result.get("error")) if result.get("error") else None,
                    completed_at=now,
                    heartbeat_at=now,
                    updated_at=now,
                )
            )

    def mark_stale_running(self, stale_after_seconds: int = 300) -> int:
        cutoff = utcnow() - timedelta(seconds=max(0, stale_after_seconds))
        now = utcnow()
        with self.engine.begin() as connection:
            result = connection.execute(
                update(investigation_jobs)
                .where(
                    investigation_jobs.c.status.in_(("running", "cancel_requested")),
                    or_(
                        investigation_jobs.c.heartbeat_at.is_(None),
                        investigation_jobs.c.heartbeat_at < cutoff,
                    ),
                )
                .values(
                    status="interrupted",
                    error="The worker stopped before this investigation completed.",
                    completed_at=now,
                    updated_at=now,
                )
            )
        return int(result.rowcount or 0)

    def delete_job(self, job_id: str) -> bool:
        with self.engine.begin() as connection:
            row = (
                connection.execute(
                    select(
                        investigation_jobs.c.case_id, investigation_jobs.c.status
                    ).where(investigation_jobs.c.id == job_id)
                )
                .mappings()
                .first()
            )
            if not row:
                return False
            if row["status"] not in TERMINAL_STATUSES:
                raise ValueError("Active investigations cannot be deleted")
            sibling_job = connection.scalar(
                select(investigation_jobs.c.id)
                .where(
                    investigation_jobs.c.case_id == row["case_id"],
                    investigation_jobs.c.id != job_id,
                )
                .limit(1)
            )
            if sibling_job:
                connection.execute(
                    delete(investigation_jobs).where(investigation_jobs.c.id == job_id)
                )
                connection.execute(
                    update(cases)
                    .where(cases.c.id == row["case_id"])
                    .values(updated_at=utcnow())
                )
            else:
                connection.execute(delete(cases).where(cases.c.id == row["case_id"]))
        return True

    @staticmethod
    def _serialize_job(row) -> Dict[str, Any]:
        result = dict(row.get("result") or {})
        payload = {
            "job_id": row["id"],
            "case_id": row["case_id"],
            "kind": row["kind"],
            "status": row["status"],
            "usernames": list(row["usernames"] or []),
            "options": dict(row["options"] or {}),
            "progress": dict(row["progress"] or {}),
            "cancel_requested": bool(row["cancel_requested"]),
            "attempts": int(row["attempts"] or 0),
            "started_at": _as_iso(row["started_at"] or row["created_at"]),
            "created_at": _as_iso(row["created_at"]),
            "heartbeat_at": _as_iso(row["heartbeat_at"]),
            "completed_at": _as_iso(row["completed_at"]),
            "error": row["error"],
            "session_folder": f"search_{row['id']}",
        }
        payload.update(result)
        payload["job_id"] = row["id"]
        payload["case_id"] = row["case_id"]
        payload["status"] = row["status"]
        payload["usernames"] = list(row["usernames"] or result.get("usernames") or [])
        payload["progress"] = dict(row["progress"] or {})
        payload["session_folder"] = result.get("session_folder", f"search_{row['id']}")
        payload["started_at"] = result.get("started_at") or payload["started_at"]
        return payload
