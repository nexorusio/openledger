"""Persistent case and investigation-job storage for OpenLedger."""

from __future__ import annotations

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
        now = utcnow()
        case_id = str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        title = ", ".join(normalized)[:500]
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
                    for username in normalized
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
            latest_options = connection.scalar(
                select(investigation_jobs.c.options)
                .where(investigation_jobs.c.case_id == persona_row["case_id"])
                .order_by(investigation_jobs.c.created_at.desc())
                .limit(1)
            )
            username = str(persona_row["display_name"]).strip()
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=persona_row["case_id"],
                    kind="refresh",
                    status="queued",
                    usernames=[username],
                    options=dict(latest_options or {}),
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
            {"type": "queued", "usernames": [username], "reason": "persona_refresh"},
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

    def sync_persona_claims(self, job_id: str, result: Dict[str, Any]) -> int:
        """Upsert deterministic claims while preserving every human decision."""
        from maigret.web.persona_intelligence import extract_persona_claims

        now = utcnow()
        synchronized = 0
        with self.engine.begin() as connection:
            case_id = connection.scalar(
                select(investigation_jobs.c.case_id).where(
                    investigation_jobs.c.id == job_id
                )
            )
            if not case_id:
                raise KeyError(job_id)
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
            for report in result.get("individual_reports") or []:
                username = str(report.get("username") or "").strip()
                persona_id = personas_by_name.get(username.casefold())
                if not persona_id:
                    continue
                for candidate in extract_persona_claims(report):
                    existing = (
                        connection.execute(
                            select(persona_claims).where(
                                persona_claims.c.persona_id == persona_id,
                                persona_claims.c.fingerprint
                                == candidate["fingerprint"],
                            )
                        )
                        .mappings()
                        .first()
                    )
                    if existing:
                        claim_id = existing["id"]
                        connection.execute(
                            update(persona_claims)
                            .where(persona_claims.c.id == claim_id)
                            .values(
                                value=candidate["value"],
                                display_value=candidate["display_value"],
                                normalized_value=candidate["normalized_value"],
                                confidence=max(
                                    int(existing["confidence"]),
                                    int(candidate["confidence"]),
                                ),
                                source_job_id=job_id,
                                last_seen_at=now,
                                updated_at=now,
                            )
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
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(updated_at=now)
            )
        return synchronized

    def review_claim(
        self,
        claim_id: str,
        decision: str,
        reviewer: str,
        note: str = "",
    ) -> Optional[str]:
        """Record an auditable human decision and return the persona id."""
        if decision not in {"pending", "approved", "rejected", "uncertain"}:
            raise ValueError("Invalid claim review decision")
        reviewer = str(reviewer).strip()[:200]
        if not reviewer:
            raise ValueError("A reviewer is required")
        now = utcnow()
        with self.engine.begin() as connection:
            persona_id = connection.scalar(
                select(persona_claims.c.persona_id).where(
                    persona_claims.c.id == claim_id
                )
            )
            if not persona_id:
                return None
            connection.execute(
                update(persona_claims)
                .where(persona_claims.c.id == claim_id)
                .values(
                    review_status=decision,
                    reviewed_at=now,
                    reviewed_by=reviewer,
                    updated_at=now,
                )
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
        return str(persona_id)

    def build_persona_graph(self, persona_id: str) -> Dict[str, Any]:
        """Build a compact evidence graph; rejected claims never become edges."""
        persona = self.get_persona(persona_id)
        if not persona:
            raise KeyError(persona_id)
        nodes = [
            {
                "id": f"persona:{persona_id}",
                "label": persona["display_name"],
                "kind": "persona",
            }
        ]
        edges = []
        seen_sources = set()
        graph_claims = sorted(
            persona["claims"],
            key=lambda claim: (
                claim["review_status"] != "approved",
                -int(claim["confidence"]),
                claim["field_name"],
            ),
        )[:24]
        for claim in graph_claims:
            if claim["review_status"] == "rejected":
                continue
            claim_node = f"claim:{claim['id']}"
            nodes.append(
                {
                    "id": claim_node,
                    "label": claim["display_value"],
                    "kind": claim["field_name"],
                    "confidence": claim["confidence"],
                    "review_status": claim["review_status"],
                }
            )
            edges.append(
                {
                    "source": f"persona:{persona_id}",
                    "target": claim_node,
                    "label": claim["field_name"].replace("_", " "),
                }
            )
            for evidence in claim["evidence"]:
                source_key = evidence.get("source_url") or evidence["source_name"]
                source_id = "source:" + str(source_key)
                if source_id not in seen_sources:
                    nodes.append(
                        {
                            "id": source_id,
                            "label": evidence["source_name"],
                            "kind": "source",
                            "url": evidence.get("source_url"),
                        }
                    )
                    seen_sources.add(source_id)
                edges.append(
                    {
                        "source": claim_node,
                        "target": source_id,
                        "label": "supported by",
                    }
                )
        return {"nodes": nodes, "edges": edges}

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
