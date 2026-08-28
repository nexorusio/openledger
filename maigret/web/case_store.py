"""Persistent case and investigation-job storage for OpenLedger."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote_plus

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
from sqlalchemy.engine import Engine

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
    """Build a database URL without requiring a plaintext password environment variable."""
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
    user = quote_plus(os.getenv("DATABASE_USER", "openledger"))
    encoded_password = quote_plus(password)
    host = os.getenv("DATABASE_HOST", "db")
    port = int(os.getenv("DATABASE_PORT", "5432"))
    name = quote_plus(os.getenv("DATABASE_NAME", "openledger"))
    return f"postgresql+psycopg://{user}:{encoded_password}@{host}:{port}/{name}"


def _as_iso(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


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
            return connection
        acquired = connection.execute(
            text("SELECT pg_try_advisory_lock(:lock_key)"),
            {"lock_key": WORKER_LOCK_KEY},
        ).scalar_one()
        connection.commit()
        if not acquired:
            connection.close()
            return None
        return connection

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
                    "error": "The queued investigation was cancelled before it started.",
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
