"""Persistent case and investigation-job storage for OpenLedger."""

from __future__ import annotations

import hashlib
import math
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote, urlsplit

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
    func,
    insert,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Connection, Engine

from maigret.web.external_evidence import (
    ExternalEvidenceValidationError,
    bounded_text,
    normalize_bounded_document,
    normalize_classification,
    normalize_external_evidence,
    normalize_policy_context,
    normalize_source_id,
    stable_fingerprint,
    validate_locator_authority,
)

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

case_chat_messages = Table(
    "case_chat_messages",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "case_id",
        String(36),
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "persona_id",
        String(36),
        ForeignKey("personas.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("role", String(16), nullable=False),
    Column("author", String(200), nullable=False),
    Column("content", Text, nullable=False),
    Column("research_enabled", Boolean, nullable=False, server_default="false"),
    Column("sources", json_document, nullable=False),
    Column("proposals", json_document, nullable=False),
    Column("model", String(100), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "role IN ('user', 'assistant')",
        name="ck_case_chat_messages_role",
    ),
)
Index(
    "ix_case_chat_messages_case_created",
    case_chat_messages.c.case_id,
    case_chat_messages.c.created_at,
)
Index("ix_case_chat_messages_persona", case_chat_messages.c.persona_id)

data_sources = Table(
    "data_sources",
    metadata,
    Column("id", String(100), primary_key=True),
    Column("name", String(200), nullable=False),
    Column("source_type", String(64), nullable=False),
    Column("authority", String(200), nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("enabled", Boolean, nullable=False, server_default="true"),
    Column("default_classification", String(64), nullable=False),
    Column("handling_defaults", json_document, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    CheckConstraint("schema_version > 0", name="ck_data_sources_schema_version"),
)

query_receipts = Table(
    "query_receipts",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "case_id",
        String(36),
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "source_id",
        String(100),
        ForeignKey("data_sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("requested_by", String(200), nullable=False),
    Column("purpose", Text, nullable=False),
    Column("query_fingerprint", String(64), nullable=False),
    Column("query_document", json_document, nullable=False),
    Column("policy_context", json_document, nullable=False),
    Column("status", String(16), nullable=False),
    Column("result_count", Integer, nullable=True),
    Column("error", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    CheckConstraint(
        "status IN ('queued', 'running', 'completed', 'failed', 'cancelled')",
        name="ck_query_receipts_status",
    ),
    CheckConstraint(
        "result_count IS NULL OR result_count >= 0",
        name="ck_query_receipts_result_count",
    ),
)
Index("ix_query_receipts_case_created", query_receipts.c.case_id, query_receipts.c.created_at)
Index("ix_query_receipts_source_status", query_receipts.c.source_id, query_receipts.c.status)

external_evidence_records = Table(
    "external_evidence_records",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "case_id",
        String(36),
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "source_id",
        String(100),
        ForeignKey("data_sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("source_record_id", String(500), nullable=False),
    Column("source_version", String(200), nullable=False),
    Column("record_type", String(100), nullable=False),
    Column("content_hash", String(71), nullable=False),
    Column("locator", json_document, nullable=False),
    Column("preview", Text, nullable=False),
    Column("attributes", json_document, nullable=False),
    Column("handling", json_document, nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False),
    Column("valid_from", DateTime(timezone=True), nullable=True),
    Column("valid_to", DateTime(timezone=True), nullable=True),
    UniqueConstraint(
        "case_id",
        "source_id",
        "source_record_id",
        "source_version",
        name="uq_external_evidence_source_version",
    ),
)
Index("ix_external_evidence_case_source", external_evidence_records.c.case_id, external_evidence_records.c.source_id)
Index("ix_external_evidence_content_hash", external_evidence_records.c.content_hash)

external_evidence_receipts = Table(
    "external_evidence_receipts",
    metadata,
    Column(
        "evidence_id",
        String(36),
        ForeignKey("external_evidence_records.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "query_receipt_id",
        String(36),
        ForeignKey("query_receipts.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("attached_by", String(200), nullable=False),
    Column("attached_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_external_evidence_receipts_receipt",
    external_evidence_receipts.c.query_receipt_id,
)

claim_observations = Table(
    "claim_observations",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "claim_id",
        String(36),
        ForeignKey("persona_claims.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("provenance_type", String(32), nullable=False),
    Column("provenance_id", String(500), nullable=False),
    Column(
        "job_id",
        String(36),
        ForeignKey("investigation_jobs.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column(
        "external_evidence_id",
        String(36),
        ForeignKey("external_evidence_records.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column(
        "chat_message_id",
        String(36),
        ForeignKey("case_chat_messages.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("source_engine", String(100), nullable=False),
    Column("source_record_id", String(500), nullable=True),
    Column("confidence", Integer, nullable=True),
    Column("native_status", String(100), nullable=False),
    Column("details", json_document, nullable=False),
    Column("fingerprint", String(64), nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "provenance_type IN ('investigation_job', 'external_evidence', "
        "'case_chat_message')",
        name="ck_claim_observations_provenance_type",
    ),
    CheckConstraint(
        "confidence IS NULL OR (confidence >= 0 AND confidence <= 100)",
        name="ck_claim_observations_confidence",
    ),
    UniqueConstraint("claim_id", "fingerprint", name="uq_claim_observation_fingerprint"),
)
Index("ix_claim_observations_claim_observed", claim_observations.c.claim_id, claim_observations.c.observed_at)
Index("ix_claim_observations_provenance", claim_observations.c.provenance_type, claim_observations.c.provenance_id)
Index("ix_claim_observations_chat_message", claim_observations.c.chat_message_id)


TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}
ACTIVE_STATUSES = {"queued", "running", "cancel_requested"}
WORKER_LOCK_KEY = 5714024849188199506


class ActiveInvestigationError(ValueError):
    """Raised when destructive case changes race an active investigation."""


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


def _bounded_chat_sources(value: Any) -> list[Dict[str, str]]:
    """Retain only bounded, public HTTP(S) citations on chat messages."""
    if not isinstance(value, list):
        return []
    sources: list[Dict[str, str]] = []
    seen = set()
    for item in value[:100]:
        if not isinstance(item, dict):
            continue
        candidate = str(item.get("url") or "").strip()[:2000]
        try:
            parsed = urlsplit(candidate)
        except ValueError:
            continue
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or candidate in seen
        ):
            continue
        seen.add(candidate)
        title = " ".join(str(item.get("title") or parsed.netloc).split())[:300]
        sources.append({"title": title or parsed.netloc, "url": candidate})
    return sources


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

    def create_affiliation_investigation(
        self,
        affiliation_name: str,
        *,
        source_claim_id: Optional[str] = None,
        jurisdiction: Any = None,
        enable_domain_context: bool = False,
        official_website: Any = None,
    ) -> str:
        from maigret.web.collector_adapters import (
            normalize_affiliation_name,
            normalize_legal_jurisdiction,
            normalize_official_website_url,
        )

        affiliation_name = normalize_affiliation_name(affiliation_name)
        legal_jurisdiction = normalize_legal_jurisdiction(jurisdiction)
        normalized_website = normalize_official_website_url(official_website)
        enable_domain_context = bool(enable_domain_context or normalized_website)
        source_claim_id = str(source_claim_id or "").strip() or None
        if source_claim_id and len(source_claim_id) > 100:
            raise ValueError("Invalid source claim identifier")
        now = utcnow()
        case_id, job_id = str(uuid.uuid4()), str(uuid.uuid4())
        specification = {
            "schema_version": 2,
            "investigation_type": "affiliation",
            "affiliation_name": affiliation_name,
            "source_claim_id": source_claim_id,
            "legal_jurisdiction": legal_jurisdiction,
            "enable_domain_context": enable_domain_context,
            "official_website": normalized_website,
        }
        case_title = f"Affiliation: {affiliation_name}"
        if legal_jurisdiction:
            case_title += f" · {legal_jurisdiction['code']}"
        with self.engine.begin() as connection:
            connection.execute(
                insert(cases).values(
                    id=case_id,
                    title=case_title[:500],
                    status="open",
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=case_id,
                    kind="affiliation",
                    status="queued",
                    usernames=[],
                    options={"investigation_spec": specification},
                    progress={
                        "checked": 0,
                        "total": (
                            4
                            if legal_jurisdiction
                            and legal_jurisdiction["code"] == "FR"
                            else 3 if legal_jurisdiction else 2
                        ) + (2 if enable_domain_context else 0),
                        "found": 0,
                    },
                    result=None,
                    error=None,
                    cancel_requested=False,
                    attempts=0,
                    created_at=now,
                    updated_at=now,
                )
            )
        self.append_event(
            job_id,
            {
                "type": "queued",
                "target_type": "affiliation",
                "affiliation": affiliation_name,
                "legal_jurisdiction": legal_jurisdiction,
                "domain_context_requested": enable_domain_context,
            },
        )
        return job_id

    def create_identity_enrichment(
        self,
        persona_id: str,
        source_claim_id: str,
        *,
        selected_wikipedia_page_id: Optional[str] = None,
    ) -> str:
        """Queue governed public-record checks for one approved full-name claim."""
        source_claim_id = str(source_claim_id or "").strip()
        selected_page_id = str(selected_wikipedia_page_id or "").strip() or None
        if selected_page_id and (
            not selected_page_id.isdigit() or len(selected_page_id) > 20
        ):
            raise ValueError("Select a valid Wikipedia biography")
        now, job_id = utcnow(), str(uuid.uuid4())
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
            claim = (
                connection.execute(
                    select(
                        persona_claims.c.display_value,
                        persona_claims.c.field_name,
                        persona_claims.c.review_status,
                    ).where(
                        persona_claims.c.id == source_claim_id,
                        persona_claims.c.persona_id == persona_id,
                    )
                )
                .mappings()
                .first()
            )
            if (
                not claim
                or claim["field_name"] != "full_name"
                or claim["review_status"] != "approved"
            ):
                raise ValueError(
                    "Public-record enrichment requires an approved full name"
                )
            if connection.scalar(
                select(investigation_jobs.c.id)
                .where(
                    investigation_jobs.c.case_id == persona_row["case_id"],
                    investigation_jobs.c.status.in_(ACTIVE_STATUSES),
                )
                .limit(1)
            ):
                raise ValueError("This case already has an active investigation")
            if selected_page_id:
                prior_rows = connection.execute(
                    select(investigation_jobs.c.result, investigation_jobs.c.options)
                    .where(
                        investigation_jobs.c.case_id == persona_row["case_id"],
                        investigation_jobs.c.kind == "identity_enrichment",
                        investigation_jobs.c.status == "completed",
                    )
                    .order_by(investigation_jobs.c.created_at.desc())
                    .limit(20)
                ).mappings()
                stored_candidate = False
                for prior in prior_rows:
                    prior_spec = dict(prior["options"] or {}).get(
                        "investigation_spec"
                    ) or {}
                    if str(prior_spec.get("persona_id") or "") != persona_id:
                        continue
                    candidates = list(
                        dict(prior["result"] or {}).get("wikipedia_candidates") or []
                    )[:5]
                    if any(
                        isinstance(candidate, dict)
                        and str(candidate.get("page_id") or "") == selected_page_id
                        for candidate in candidates
                    ):
                        stored_candidate = True
                        break
                if not stored_candidate:
                    raise ValueError(
                        "The selected Wikipedia page is not a stored candidate"
                    )
            confirmed_name = " ".join(str(claim["display_value"] or "").split())
            specification = {
                "schema_version": 1,
                "investigation_type": "identity_enrichment",
                "persona_id": persona_id,
                "source_claim_id": source_claim_id,
                "confirmed_name": confirmed_name[:300],
                "selected_wikipedia_page_id": selected_page_id,
            }
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=persona_row["case_id"],
                    kind="identity_enrichment",
                    status="queued",
                    usernames=[],
                    options={"investigation_spec": specification},
                    progress={"checked": 0, "total": 2, "found": 0},
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
            {
                "type": "queued",
                "target_type": "confirmed_person_name",
                "persona_id": persona_id,
            },
        )
        return job_id

    def select_affiliation_organization(
        self, case_id: str, candidate_key: str, reviewed_by: str
    ) -> Dict[str, Any]:
        """Persist one analyst-confirmed, source-neutral case organization."""
        candidate_key = bounded_text(
            candidate_key, "organization candidate", max_chars=500
        )
        reviewer = bounded_text(reviewed_by, "reviewer", max_chars=200)
        now = utcnow()
        selected_job_id = ""
        selection: Dict[str, Any] = {}
        selected_registry_observation = None
        with self.engine.begin() as connection:
            case_statement = select(cases.c.id).where(cases.c.id == case_id)
            if self.engine.dialect.name == "postgresql":
                case_statement = case_statement.with_for_update()
            if not connection.execute(case_statement).first():
                raise KeyError(case_id)
            prior_jobs = list(
                connection.execute(
                    select(
                        investigation_jobs.c.id,
                        investigation_jobs.c.result,
                        investigation_jobs.c.options,
                    )
                    .where(
                        investigation_jobs.c.case_id == case_id,
                        investigation_jobs.c.kind == "affiliation",
                        investigation_jobs.c.status == "completed",
                    )
                    .order_by(investigation_jobs.c.created_at.desc())
                    .limit(10)
                ).mappings()
            )
            selected_candidate = None
            selected_row = None
            for prior in prior_jobs:
                result = dict(prior["result"] or {})
                for candidate in list(
                    result.get("organization_resolution_candidates") or []
                )[:15]:
                    if (
                        isinstance(candidate, dict)
                        and candidate.get("candidate_key") == candidate_key
                    ):
                        selected_candidate = candidate
                        selected_row = prior
                        break
                if selected_candidate:
                    break
            if not selected_candidate or not selected_row:
                raise ValueError(
                    "The selected organization is not a stored candidate for this case"
                )
            if selected_candidate.get("selectable") is not True:
                raise ValueError(
                    "This candidate is not verified as an organization and cannot be selected"
                )
            normalized_candidate = normalize_bounded_document(
                selected_candidate, "organization candidate"
            )
            selection = {
                **normalized_candidate,
                "review_status": "approved",
                "reviewed_by": reviewer,
                "reviewed_at": _as_iso(now),
                "automatic_approval_allowed": False,
            }
            selected_job_id = str(selected_row["id"])
            result = dict(selected_row["result"] or {})
            result["selected_organization"] = selection
            for candidate in list(
                result.get("organization_resolution_candidates") or []
            )[:15]:
                if isinstance(candidate, dict):
                    candidate["selected"] = (
                        candidate.get("candidate_key") == candidate_key
                    )
            if selection.get("identity_scope") == "registered_legal_entity":
                registry_observations = list(
                    result.get("registry_observations") or []
                )[:5]
                for index, registry_observation in enumerate(
                    registry_observations
                ):
                    if (
                        not isinstance(registry_observation, dict)
                        or registry_observation.get("source_engine")
                        != selection.get("source_engine")
                    ):
                        continue
                    for entity in list(
                        registry_observation.get("candidates") or []
                    )[:5]:
                        if (
                            isinstance(entity, dict)
                            and str(entity.get("id") or "")
                            == str(selection.get("entity_id") or "")
                        ):
                            selected_registry_observation = {
                                **registry_observation,
                                "selected_entity": {
                                    **entity,
                                    "analyst_selected": True,
                                },
                            }
                            registry_observations[index] = (
                                selected_registry_observation
                            )
                            result["registry_observations"] = (
                                registry_observations
                            )
                            break
                    if selected_registry_observation:
                        break
            options = dict(selected_row["options"] or {})
            specification = dict(options.get("investigation_spec") or {})
            specification["selected_organization"] = selection
            options["investigation_spec"] = specification
            connection.execute(
                update(investigation_jobs)
                .where(investigation_jobs.c.id == selected_job_id)
                .values(result=result, options=options, updated_at=now)
            )
            title = f"Affiliation: {selection['label']}"
            legal_jurisdiction = specification.get("legal_jurisdiction")
            if isinstance(legal_jurisdiction, dict) and legal_jurisdiction.get(
                "code"
            ):
                title += f" · {legal_jurisdiction['code']}"
            connection.execute(
                update(cases)
                .where(cases.c.id == case_id)
                .values(title=title[:500], updated_at=now)
            )
        self.append_event(
            selected_job_id,
            {
                "type": "organization_selected",
                "candidate_key": selection["candidate_key"],
                "label": selection["label"],
                "source_engine": selection["source_engine"],
                "source_record_id": selection.get("source_record_id"),
                "identity_scope": selection["identity_scope"],
                "reviewed_by": reviewer,
            },
        )
        if selected_registry_observation:
            self.sync_affiliation_discovery(
                selected_job_id,
                {
                    "source_engine": "wikidata_affiliation",
                    "status": "not_found",
                    "organization": None,
                    "people": [],
                },
                registry_observations=[selected_registry_observation],
            )
        return selection

    def queue_affiliation_entity(self, case_id: str, entity_id: str) -> str:
        entity_id = str(entity_id or "").strip().upper()
        if not re.fullmatch(r"Q[1-9][0-9]{0,19}", entity_id):
            raise ValueError("Select a valid Wikidata organization")
        now, job_id = utcnow(), str(uuid.uuid4())
        with self.engine.begin() as connection:
            case_statement = select(cases.c.id).where(cases.c.id == case_id)
            if self.engine.dialect.name == "postgresql":
                case_statement = case_statement.with_for_update()
            if not connection.execute(case_statement).first():
                raise KeyError(case_id)
            if connection.scalar(
                select(investigation_jobs.c.id).where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.status.in_(ACTIVE_STATUSES),
                ).limit(1)
            ):
                raise ValueError("This case already has an active investigation")
            prior_jobs = connection.execute(
                select(investigation_jobs.c.result, investigation_jobs.c.options)
                .where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.kind == "affiliation",
                    investigation_jobs.c.status == "completed",
                )
                .order_by(investigation_jobs.c.created_at.desc())
                .limit(10)
            ).mappings()
            candidate = None
            affiliation_name = ""
            source_claim_id = None
            legal_jurisdiction = None
            enable_domain_context = False
            official_website = None
            selected_organization = None
            for prior in prior_jobs:
                spec = dict(prior["options"] or {}).get("investigation_spec") or {}
                affiliation_name = affiliation_name or str(spec.get("affiliation_name") or "")
                source_claim_id = source_claim_id or spec.get("source_claim_id")
                legal_jurisdiction = legal_jurisdiction or spec.get(
                    "legal_jurisdiction"
                )
                enable_domain_context = enable_domain_context or bool(
                    spec.get("enable_domain_context")
                )
                official_website = official_website or spec.get("official_website")
                selected_organization = selected_organization or spec.get(
                    "selected_organization"
                )
                for item in list(dict(prior["result"] or {}).get("organization_candidates") or [])[:5]:
                    if isinstance(item, dict) and str(item.get("id") or "").upper() == entity_id:
                        candidate = item
                        break
                if candidate:
                    break
            if not candidate or not affiliation_name:
                raise ValueError("The selected organization is not a stored candidate for this case")
            if candidate.get("organization_eligible") is not True:
                raise ValueError(
                    "The selected Wikidata item is not type-verified as an organization"
                )
            selected_label = " ".join(str(candidate.get("label") or affiliation_name).split())[:500]
            specification = {
                "schema_version": 2,
                "investigation_type": "affiliation",
                "affiliation_name": affiliation_name[:500],
                "source_claim_id": source_claim_id,
                "legal_jurisdiction": legal_jurisdiction,
                "enable_domain_context": enable_domain_context,
                "official_website": official_website,
                "wikidata_entity_id": entity_id,
                "selected_entity_label": selected_label,
                "selected_organization": selected_organization,
            }
            total_sources = (
                4
                if isinstance(legal_jurisdiction, dict)
                and legal_jurisdiction.get("code") == "FR"
                else 3 if legal_jurisdiction else 2
            )
            total_sources += 2 if enable_domain_context else 0
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id, case_id=case_id, kind="affiliation", status="queued",
                    usernames=[], options={"investigation_spec": specification},
                    progress={"checked": 1, "total": total_sources, "found": 0}, result=None,
                    error=None, cancel_requested=False, attempts=0,
                    created_at=now, updated_at=now,
                )
            )
            title = f"Affiliation: {selected_label}"
            if isinstance(legal_jurisdiction, dict) and legal_jurisdiction.get(
                "code"
            ):
                title += f" · {legal_jurisdiction['code']}"
            connection.execute(
                update(cases)
                .where(cases.c.id == case_id)
                .values(title=title[:500], updated_at=now)
            )
        self.append_event(job_id, {"type": "queued", "target_type": "wikidata_entity", "entity_id": entity_id})
        return job_id

    def queue_affiliation_context(
        self, case_id: str, *, official_website: Any = None
    ) -> str:
        """Rerun an affiliation case with an explicit domain-context opt-in."""
        from maigret.web.collector_adapters import normalize_official_website_url

        normalized_website = normalize_official_website_url(official_website)
        now, job_id = utcnow(), str(uuid.uuid4())
        with self.engine.begin() as connection:
            case_statement = select(cases.c.id).where(cases.c.id == case_id)
            if self.engine.dialect.name == "postgresql":
                case_statement = case_statement.with_for_update()
            if not connection.execute(case_statement).first():
                raise KeyError(case_id)
            if connection.scalar(
                select(investigation_jobs.c.id)
                .where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.status.in_(ACTIVE_STATUSES),
                )
                .limit(1)
            ):
                raise ValueError("This case already has an active investigation")
            prior = (
                connection.execute(
                    select(investigation_jobs.c.options)
                    .where(
                        investigation_jobs.c.case_id == case_id,
                        investigation_jobs.c.kind == "affiliation",
                    )
                    .order_by(investigation_jobs.c.created_at.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            if not prior:
                raise ValueError("This is not an affiliation case")
            prior_spec = dict(prior["options"] or {}).get("investigation_spec") or {}
            affiliation_name = " ".join(
                str(prior_spec.get("affiliation_name") or "").split()
            )
            if not affiliation_name:
                raise ValueError("The affiliation case has no reusable organization name")
            legal_jurisdiction = prior_spec.get("legal_jurisdiction")
            specification = {
                **prior_spec,
                "schema_version": 2,
                "investigation_type": "affiliation",
                "affiliation_name": affiliation_name[:500],
                "enable_domain_context": True,
                "official_website": (
                    normalized_website or prior_spec.get("official_website")
                ),
            }
            total_sources = (
                4
                if isinstance(legal_jurisdiction, dict)
                and legal_jurisdiction.get("code") == "FR"
                else 3 if legal_jurisdiction else 2
            ) + 2
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=case_id,
                    kind="affiliation",
                    status="queued",
                    usernames=[],
                    options={"investigation_spec": specification},
                    progress={"checked": 0, "total": total_sources, "found": 0},
                    result=None,
                    error=None,
                    cancel_requested=False,
                    attempts=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(updated_at=now)
            )
        self.append_event(
            job_id,
            {
                "type": "queued",
                "target_type": "organization_domain_context",
                "domain_context_requested": True,
            },
        )
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
                select(investigation_jobs, cases.c.title.label("case_title"))
                .join(cases, cases.c.id == investigation_jobs.c.case_id)
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

    def get_persona_identity_enrichment(
        self, persona_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return the latest durable identity-enrichment job for one Persona."""
        with self.engine.connect() as connection:
            persona_case_id = connection.scalar(
                select(personas.c.case_id).where(personas.c.id == persona_id)
            )
            if not persona_case_id:
                return None
            rows = connection.execute(
                select(investigation_jobs)
                .where(
                    investigation_jobs.c.case_id == persona_case_id,
                    investigation_jobs.c.kind == "identity_enrichment",
                )
                .order_by(investigation_jobs.c.created_at.desc())
                .limit(50)
            ).mappings()
            for row in rows:
                specification = dict(row["options"] or {}).get(
                    "investigation_spec"
                ) or {}
                if str(specification.get("persona_id") or "") == persona_id:
                    return self._serialize_job(row)
        return None

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

    def append_case_chat_message(
        self,
        case_id: str,
        *,
        role: str,
        author: str,
        content: str,
        persona_id: Optional[str] = None,
        research_enabled: bool = False,
        sources: Optional[list[Dict[str, Any]]] = None,
        proposals: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Append one durable case-scoped conversation message."""
        normalized_role = str(role or "").strip().casefold()
        if normalized_role not in {"user", "assistant"}:
            raise ValueError("Invalid chat message role")
        normalized_author = " ".join(str(author or "").split())[:200]
        if not normalized_author:
            raise ValueError("A chat message author is required")
        normalized_content = str(content or "").strip()
        content_limit = 12_000 if normalized_role == "user" else 50_000
        if not normalized_content:
            raise ValueError("A chat message cannot be empty")
        if len(normalized_content) > content_limit:
            raise ValueError(
                f"Chat message exceeds the {content_limit:,}-character limit"
            )
        normalized_model = " ".join(str(model or "").split())[:100] or None
        normalized_sources = _bounded_chat_sources(sources or [])
        normalized_proposals = normalize_bounded_document(
            proposals or {}, "proposals"
        )
        now = utcnow()
        message_id = str(uuid.uuid4())
        with self.engine.begin() as connection:
            if not connection.scalar(select(cases.c.id).where(cases.c.id == case_id)):
                raise KeyError(case_id)
            if persona_id:
                persona_case_id = connection.scalar(
                    select(personas.c.case_id).where(personas.c.id == persona_id)
                )
                if not persona_case_id:
                    raise KeyError(persona_id)
                if persona_case_id != case_id:
                    raise ValueError("Persona does not belong to this case")
            connection.execute(
                insert(case_chat_messages).values(
                    id=message_id,
                    case_id=case_id,
                    persona_id=persona_id,
                    role=normalized_role,
                    author=normalized_author,
                    content=normalized_content,
                    research_enabled=bool(research_enabled),
                    sources=normalized_sources,
                    proposals=normalized_proposals,
                    model=normalized_model,
                    created_at=now,
                )
            )
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(updated_at=now)
            )
        return {
            "id": message_id,
            "case_id": case_id,
            "persona_id": persona_id,
            "role": normalized_role,
            "author": normalized_author,
            "content": normalized_content,
            "research_enabled": bool(research_enabled),
            "sources": normalized_sources,
            "proposals": normalized_proposals,
            "model": normalized_model,
            "created_at": _as_iso(now),
        }

    def update_case_chat_message_proposals(
        self, message_id: str, proposals: Dict[str, Any]
    ) -> None:
        normalized = normalize_bounded_document(proposals or {}, "proposals")
        with self.engine.begin() as connection:
            updated = connection.execute(
                update(case_chat_messages)
                .where(
                    case_chat_messages.c.id == message_id,
                    case_chat_messages.c.role == "assistant",
                )
                .values(proposals=normalized)
            )
            if updated.rowcount != 1:
                raise KeyError(message_id)

    def list_case_chat_messages(
        self, case_id: str, *, limit: int = 200
    ) -> list[Dict[str, Any]]:
        bounded_limit = min(max(1, int(limit)), 500)
        with self.engine.connect() as connection:
            rows = list(
                connection.execute(
                    select(case_chat_messages)
                    .where(case_chat_messages.c.case_id == case_id)
                    .order_by(
                        case_chat_messages.c.created_at.desc(),
                        case_chat_messages.c.id.desc(),
                    )
                    .limit(bounded_limit)
                ).mappings()
            )
        rows.reverse()
        return [self._serialize_case_chat_message(row) for row in rows]

    def get_case_chat_context(
        self, case_id: str, *, claim_limit: int = 500
    ) -> Optional[Dict[str, Any]]:
        """Return bounded case evidence for a model prompt, with review labels."""
        bounded_limit = min(max(1, int(claim_limit)), 1000)
        with self.engine.connect() as connection:
            case_row = (
                connection.execute(
                    select(cases.c.id, cases.c.title, cases.c.status).where(
                        cases.c.id == case_id
                    )
                )
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
            persona_ids = [row["id"] for row in persona_rows]
            claim_rows = (
                list(
                    connection.execute(
                        select(persona_claims)
                        .where(persona_claims.c.persona_id.in_(persona_ids))
                        .order_by(
                            persona_claims.c.persona_id,
                            persona_claims.c.field_name,
                            persona_claims.c.confidence.desc(),
                        )
                        .limit(bounded_limit + 1)
                    ).mappings()
                )
                if persona_ids
                else []
            )
        claims_by_persona: Dict[str, list] = {}
        for claim in claim_rows[:bounded_limit]:
            claims_by_persona.setdefault(str(claim["persona_id"]), []).append(
                {
                    "id": str(claim["id"]),
                    "field_name": str(claim["field_name"]),
                    "display_value": str(claim["display_value"])[:4000],
                    "confidence": int(claim["confidence"]),
                    "review_status": str(claim["review_status"]),
                    "source_engine": str(claim["source_engine"]),
                    "last_seen_at": _as_iso(claim["last_seen_at"]),
                }
            )
        return {
            "id": str(case_row["id"]),
            "title": str(case_row["title"]),
            "status": str(case_row["status"]),
            "truncated_claim_count": max(0, len(claim_rows) - bounded_limit),
            "personas": [
                {
                    "id": str(row["id"]),
                    "display_name": str(row["display_name"]),
                    "claims": claims_by_persona.get(str(row["id"]), []),
                }
                for row in persona_rows
            ],
        }

    @staticmethod
    def _serialize_case_chat_message(row) -> Dict[str, Any]:
        return {
            "id": str(row["id"]),
            "case_id": str(row["case_id"]),
            "persona_id": str(row["persona_id"]) if row["persona_id"] else None,
            "role": str(row["role"]),
            "author": str(row["author"]),
            "content": str(row["content"]),
            "research_enabled": bool(row["research_enabled"]),
            "sources": list(row["sources"] or []),
            "proposals": dict(row["proposals"] or {}),
            "model": str(row["model"]) if row["model"] else None,
            "created_at": _as_iso(row["created_at"]),
        }

    def build_case_timeline(
        self,
        case_id: str,
        *,
        persona_id: Optional[str] = None,
        event_type: str = "all",
        order: str = "newest",
        limit: int = 300,
    ) -> Optional[Dict[str, Any]]:
        """Project existing case records into a bounded, read-only audit timeline."""
        event_type = str(event_type or "all").strip().casefold()
        if event_type not in {"all", "investigation", "evidence", "review"}:
            raise ValueError("Invalid timeline event type")
        order = str(order or "newest").strip().casefold()
        if order not in {"newest", "oldest"}:
            raise ValueError("Invalid timeline order")
        bounded_limit = min(max(1, int(limit)), 500)
        query_limit = bounded_limit + 1
        descending = order == "newest"
        timeline_events: list[Dict[str, Any]] = []

        with self.engine.connect() as connection:
            case_row = (
                connection.execute(
                    select(cases.c.id, cases.c.title).where(cases.c.id == case_id)
                )
                .mappings()
                .first()
            )
            if not case_row:
                return None

            selected_persona = None
            if persona_id:
                selected_persona = (
                    connection.execute(
                        select(personas.c.id, personas.c.display_name).where(
                            personas.c.id == persona_id,
                            personas.c.case_id == case_id,
                        )
                    )
                    .mappings()
                    .first()
                )
                if not selected_persona:
                    raise ValueError("Persona does not belong to this case")

            # Investigation events remain case-level. They are deliberately
            # excluded from a Persona-filtered view because a multi-subject job
            # cannot be attributed to one Persona without inference.
            if not persona_id and event_type in {"all", "investigation"}:
                latest_job_time = func.coalesce(
                    investigation_jobs.c.completed_at,
                    investigation_jobs.c.started_at,
                    investigation_jobs.c.created_at,
                )
                job_order = (
                    latest_job_time.desc()
                    if descending
                    else investigation_jobs.c.created_at.asc()
                )
                job_rows = list(
                    connection.execute(
                        select(
                            investigation_jobs.c.id,
                            investigation_jobs.c.kind,
                            investigation_jobs.c.status,
                            investigation_jobs.c.usernames,
                            investigation_jobs.c.created_at,
                            investigation_jobs.c.started_at,
                            investigation_jobs.c.completed_at,
                        )
                        .where(investigation_jobs.c.case_id == case_id)
                        .order_by(job_order, investigation_jobs.c.id)
                        .limit(query_limit)
                    ).mappings()
                )
                for row in job_rows:
                    start_time = row["started_at"] or row["created_at"]
                    start_kind = (
                        "investigation_started"
                        if row["started_at"]
                        else "investigation_queued"
                    )
                    timeline_events.append(
                        {
                            "id": f"job:{row['id']}:start",
                            "timestamp": _as_iso(start_time),
                            "sequence": 0,
                            "event_type": "investigation",
                            "kind": start_kind,
                            "title": (
                                "Investigation started"
                                if row["started_at"]
                                else "Investigation queued"
                            ),
                            "job_id": str(row["id"]),
                            "job_kind": str(row["kind"]),
                            "status": "running" if row["started_at"] else "queued",
                            "usernames": [
                                str(username)[:500]
                                for username in list(row["usernames"] or [])[:20]
                            ],
                            "persona": None,
                            "claim": None,
                        }
                    )
                    if row["completed_at"]:
                        status = str(row["status"])
                        timeline_events.append(
                            {
                                "id": f"job:{row['id']}:outcome",
                                "timestamp": _as_iso(row["completed_at"]),
                                "sequence": 3,
                                "event_type": "investigation",
                                "kind": f"investigation_{status}",
                                "title": f"Investigation {status.replace('_', ' ')}",
                                "job_id": str(row["id"]),
                                "job_kind": str(row["kind"]),
                                "status": status,
                                "usernames": [
                                    str(username)[:500]
                                    for username in list(row["usernames"] or [])[:20]
                                ],
                                "persona": None,
                                "claim": None,
                            }
                        )

            if event_type in {"all", "evidence"}:
                observation_statement = (
                    select(
                        claim_observations.c.id,
                        claim_observations.c.provenance_type,
                        claim_observations.c.provenance_id,
                        claim_observations.c.job_id,
                        claim_observations.c.external_evidence_id,
                        claim_observations.c.source_engine,
                        claim_observations.c.source_record_id,
                        claim_observations.c.confidence,
                        claim_observations.c.native_status,
                        claim_observations.c.details,
                        claim_observations.c.observed_at,
                        persona_claims.c.id.label("claim_id"),
                        persona_claims.c.field_name,
                        persona_claims.c.display_value,
                        persona_claims.c.review_status,
                        personas.c.id.label("persona_id"),
                        personas.c.display_name.label("persona_name"),
                    )
                    .select_from(
                        claim_observations.join(
                            persona_claims,
                            persona_claims.c.id == claim_observations.c.claim_id,
                        ).join(personas, personas.c.id == persona_claims.c.persona_id)
                    )
                    .where(personas.c.case_id == case_id)
                )
                if persona_id:
                    observation_statement = observation_statement.where(
                        personas.c.id == persona_id
                    )
                observation_order = (
                    claim_observations.c.observed_at.desc()
                    if descending
                    else claim_observations.c.observed_at.asc()
                )
                observation_rows = list(
                    connection.execute(
                        observation_statement.order_by(
                            observation_order, claim_observations.c.id
                        ).limit(query_limit)
                    ).mappings()
                )
                for row in observation_rows:
                    details = dict(row["details"] or {})
                    observation_details = details.get("observation")
                    if not isinstance(observation_details, dict):
                        observation_details = {}
                    raw_metadata = observation_details.get("account_metadata")
                    if not isinstance(raw_metadata, dict):
                        raw_metadata = {}
                    account_metadata = {
                        key: raw_metadata[key]
                        for key in (
                            "created_at",
                            "updated_at",
                            "latest_activity_at",
                            "is_verified",
                            "is_private",
                            "follower_count",
                            "following_count",
                        )
                        if key in raw_metadata
                        and isinstance(raw_metadata[key], (str, int, bool))
                    }
                    extractor = observation_details.get("extractor")
                    extractor = (
                        str(extractor)[:100]
                        if isinstance(extractor, (str, int))
                        else None
                    )
                    timeline_events.append(
                        {
                            "id": f"observation:{row['id']}",
                            "timestamp": _as_iso(row["observed_at"]),
                            "sequence": 1,
                            "event_type": "evidence",
                            "kind": "claim_observed",
                            "title": "Evidence observed",
                            "job_id": row["job_id"],
                            "status": str(row["native_status"]),
                            "source_engine": str(row["source_engine"]),
                            "source_record_id": row["source_record_id"],
                            "confidence": row["confidence"],
                            "provenance_type": str(row["provenance_type"]),
                            "provenance_id": str(row["provenance_id"]),
                            "external_evidence_id": row["external_evidence_id"],
                            "account_metadata": account_metadata,
                            "extractor": extractor,
                            "persona": {
                                "id": str(row["persona_id"]),
                                "display_name": str(row["persona_name"]),
                            },
                            "claim": {
                                "id": str(row["claim_id"]),
                                "field_name": str(row["field_name"]),
                                "display_value": str(row["display_value"]),
                                "review_status": str(row["review_status"]),
                            },
                        }
                    )

            if event_type in {"all", "review"}:
                review_statement = (
                    select(
                        claim_reviews.c.id,
                        claim_reviews.c.decision,
                        claim_reviews.c.reviewer,
                        claim_reviews.c.note,
                        claim_reviews.c.created_at,
                        persona_claims.c.id.label("claim_id"),
                        persona_claims.c.field_name,
                        persona_claims.c.display_value,
                        persona_claims.c.review_status,
                        personas.c.id.label("persona_id"),
                        personas.c.display_name.label("persona_name"),
                    )
                    .select_from(
                        claim_reviews.join(
                            persona_claims,
                            persona_claims.c.id == claim_reviews.c.claim_id,
                        ).join(personas, personas.c.id == persona_claims.c.persona_id)
                    )
                    .where(personas.c.case_id == case_id)
                )
                if persona_id:
                    review_statement = review_statement.where(
                        personas.c.id == persona_id
                    )
                review_order = (
                    claim_reviews.c.created_at.desc()
                    if descending
                    else claim_reviews.c.created_at.asc()
                )
                review_rows = list(
                    connection.execute(
                        review_statement.order_by(
                            review_order, claim_reviews.c.id
                        ).limit(query_limit)
                    ).mappings()
                )
                for row in review_rows:
                    decision = str(row["decision"])
                    timeline_events.append(
                        {
                            "id": f"review:{row['id']}",
                            "timestamp": _as_iso(row["created_at"]),
                            "sequence": 2,
                            "event_type": "review",
                            "kind": "claim_reviewed",
                            "title": f"Claim marked {decision}",
                            "decision": decision,
                            "reviewer": str(row["reviewer"]),
                            "note": row["note"],
                            "persona": {
                                "id": str(row["persona_id"]),
                                "display_name": str(row["persona_name"]),
                            },
                            "claim": {
                                "id": str(row["claim_id"]),
                                "field_name": str(row["field_name"]),
                                "display_value": str(row["display_value"]),
                                "review_status": str(row["review_status"]),
                            },
                        }
                    )

        timeline_events.sort(
            key=lambda item: (
                item["timestamp"] or "",
                int(item["sequence"]),
                item["id"],
            ),
            reverse=descending,
        )
        truncated = len(timeline_events) > bounded_limit
        timeline_events = timeline_events[:bounded_limit]
        for item in timeline_events:
            item.pop("sequence", None)
        return {
            "case_id": str(case_row["id"]),
            "case_title": str(case_row["title"]),
            "selected_persona": (
                dict(selected_persona) if selected_persona is not None else None
            ),
            "event_type": event_type,
            "order": order,
            "events": timeline_events,
            "stats": {
                "displayed_count": len(timeline_events),
                "investigation_count": sum(
                    item["event_type"] == "investigation"
                    for item in timeline_events
                ),
                "evidence_count": sum(
                    item["event_type"] == "evidence" for item in timeline_events
                ),
                "review_count": sum(
                    item["event_type"] == "review" for item in timeline_events
                ),
                "truncated": truncated,
                "limit": bounded_limit,
            },
        }

    def register_data_source(
        self,
        source_id: str,
        *,
        name: str,
        source_type: str,
        authority: str,
        schema_version: int = 1,
        default_classification: str,
        handling_defaults: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register immutable source identity and non-secret handling metadata."""
        normalized_id = normalize_source_id(source_id)
        normalized = {
            "name": bounded_text(name, "name", max_chars=200),
            "source_type": bounded_text(
                source_type, "source_type", max_chars=64
            ).casefold(),
            "authority": bounded_text(authority, "authority", max_chars=200),
            "schema_version": int(schema_version),
            "default_classification": normalize_classification(
                default_classification,
                "default_classification",
            ),
            "handling_defaults": normalize_bounded_document(
                handling_defaults or {}, "handling_defaults"
            ),
        }
        if normalized["schema_version"] <= 0:
            raise ExternalEvidenceValidationError("schema_version must be positive")
        now = utcnow()
        with self.engine.begin() as connection:
            existing = (
                connection.execute(
                    select(data_sources).where(data_sources.c.id == normalized_id)
                )
                .mappings()
                .first()
            )
            if existing:
                for key, expected in normalized.items():
                    actual = (
                        dict(existing[key] or {})
                        if key == "handling_defaults"
                        else existing[key]
                    )
                    if actual != expected:
                        raise ExternalEvidenceValidationError(
                            "Registered data-source identity is immutable"
                        )
                return normalized_id
            connection.execute(
                insert(data_sources).values(
                    id=normalized_id,
                    enabled=True,
                    created_at=now,
                    updated_at=now,
                    **normalized,
                )
            )
        return normalized_id

    def set_data_source_enabled(self, source_id: str, enabled: bool) -> None:
        normalized_id = normalize_source_id(source_id)
        with self.engine.begin() as connection:
            result = connection.execute(
                update(data_sources)
                .where(data_sources.c.id == normalized_id)
                .values(enabled=bool(enabled), updated_at=utcnow())
            )
            if result.rowcount != 1:
                raise KeyError(normalized_id)

    def create_query_receipt(
        self,
        case_id: str,
        source_id: str,
        *,
        requested_by: str,
        purpose: str,
        query_document: Dict[str, Any],
        policy_context: Dict[str, Any],
    ) -> str:
        """Record who queried which source, for what case and declared purpose."""
        normalized_source_id = normalize_source_id(source_id)
        actor = bounded_text(requested_by, "requested_by", max_chars=200)
        declared_purpose = bounded_text(purpose, "purpose", max_chars=2_000)
        query = normalize_bounded_document(query_document, "query_document")
        policy = normalize_policy_context(
            policy_context,
            requested_by=actor,
            purpose=declared_purpose,
        )
        receipt_id = str(uuid.uuid4())
        now = utcnow()
        with self.engine.begin() as connection:
            if not connection.scalar(select(cases.c.id).where(cases.c.id == case_id)):
                raise KeyError(case_id)
            source = (
                connection.execute(
                    select(data_sources).where(data_sources.c.id == normalized_source_id)
                )
                .mappings()
                .first()
            )
            if not source:
                raise KeyError(normalized_source_id)
            if not source["enabled"]:
                raise ExternalEvidenceValidationError("Data source is disabled")
            if policy["authority"] != source["authority"]:
                raise ExternalEvidenceValidationError(
                    "policy_context.authority does not match the registered source"
                )
            if (
                policy["classification_ceiling"]
                != source["default_classification"]
            ):
                raise ExternalEvidenceValidationError(
                    "policy_context.classification_ceiling is not authorized "
                    "for the registered source"
                )
            connection.execute(
                insert(query_receipts).values(
                    id=receipt_id,
                    case_id=case_id,
                    source_id=normalized_source_id,
                    requested_by=actor,
                    purpose=declared_purpose,
                    query_fingerprint=stable_fingerprint(query),
                    query_document=query,
                    policy_context=policy,
                    status="queued",
                    result_count=None,
                    error=None,
                    created_at=now,
                    completed_at=None,
                )
            )
        return receipt_id

    def complete_query_receipt(self, receipt_id: str, result_count: int) -> None:
        count = int(result_count)
        if count < 0:
            raise ExternalEvidenceValidationError("result_count must not be negative")
        with self.engine.begin() as connection:
            result = connection.execute(
                update(query_receipts)
                .where(
                    query_receipts.c.id == receipt_id,
                    query_receipts.c.status.in_(("queued", "running")),
                )
                .values(
                    status="completed",
                    result_count=count,
                    completed_at=utcnow(),
                )
            )
            if result.rowcount != 1:
                raise KeyError(receipt_id)

    def fail_query_receipt(self, receipt_id: str, error: str) -> None:
        bounded_error = bounded_text(error, "error", max_chars=4_000)
        with self.engine.begin() as connection:
            result = connection.execute(
                update(query_receipts)
                .where(
                    query_receipts.c.id == receipt_id,
                    query_receipts.c.status.in_(("queued", "running")),
                )
                .values(
                    status="failed",
                    error=bounded_error,
                    completed_at=utcnow(),
                )
            )
            if result.rowcount != 1:
                raise KeyError(receipt_id)

    def attach_external_evidence(
        self,
        case_id: str,
        receipt_id: str,
        payload: Dict[str, Any],
        *,
        attached_by: str,
    ) -> str:
        """Attach a validated immutable source version to one case query receipt."""
        evidence = normalize_external_evidence(payload)
        actor = bounded_text(attached_by, "attached_by", max_chars=200)
        now = utcnow()
        with self.engine.begin() as connection:
            receipt = (
                connection.execute(
                    select(query_receipts)
                    .where(query_receipts.c.id == receipt_id)
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if not receipt:
                raise KeyError(receipt_id)
            if receipt["case_id"] != case_id:
                raise ExternalEvidenceValidationError(
                    "Query receipt belongs to a different case"
                )
            if receipt["status"] != "completed":
                raise ExternalEvidenceValidationError(
                    "External evidence requires a completed query receipt"
                )
            if receipt["source_id"] != evidence["source_id"]:
                raise ExternalEvidenceValidationError(
                    "Evidence source does not match the query receipt"
                )
            source = (
                connection.execute(
                    select(data_sources).where(
                        data_sources.c.id == evidence["source_id"]
                    )
                )
                .mappings()
                .one()
            )
            if not source["enabled"]:
                raise ExternalEvidenceValidationError("Data source is disabled")
            if evidence["handling"]["authority"] != source["authority"]:
                raise ExternalEvidenceValidationError(
                    "Evidence authority does not match the registered source"
                )
            validate_locator_authority(evidence["locator"], source["authority"])
            classification_ceiling = receipt["policy_context"][
                "classification_ceiling"
            ]
            if evidence["handling"]["classification"] != classification_ceiling:
                raise ExternalEvidenceValidationError(
                    "Evidence classification does not match the authorized ceiling"
                )
            stored_evidence = {
                key: value
                for key, value in evidence.items()
                if key != "schema_version"
            }
            existing = (
                connection.execute(
                    select(external_evidence_records).where(
                        external_evidence_records.c.case_id == case_id,
                        external_evidence_records.c.source_id == evidence["source_id"],
                        external_evidence_records.c.source_record_id
                        == evidence["source_record_id"],
                        external_evidence_records.c.source_version
                        == evidence["source_version"],
                    )
                )
                .mappings()
                .first()
            )
            if existing:
                timestamp_fields = {"observed_at", "valid_from", "valid_to"}
                existing_version = {
                    key: (
                        _as_iso(existing[key])
                        if key in timestamp_fields
                        else existing[key]
                    )
                    for key in stored_evidence
                }
                incoming_version = {
                    key: _as_iso(value) if key in timestamp_fields else value
                    for key, value in stored_evidence.items()
                }
                if stable_fingerprint(existing_version) != stable_fingerprint(
                    incoming_version
                ):
                    raise ExternalEvidenceValidationError(
                        "External source versions are immutable"
                    )
                evidence_id = str(existing["id"])
            else:
                evidence_id = str(uuid.uuid4())
                connection.execute(
                    insert(external_evidence_records).values(
                        id=evidence_id,
                        case_id=case_id,
                        **stored_evidence,
                    )
                )
            linked = connection.scalar(
                select(external_evidence_receipts.c.evidence_id).where(
                    external_evidence_receipts.c.evidence_id == evidence_id,
                    external_evidence_receipts.c.query_receipt_id == receipt_id,
                )
            )
            if not linked:
                result_count = receipt["result_count"]
                if result_count is None:
                    raise ExternalEvidenceValidationError(
                        "Completed query receipt must declare a result count"
                    )
                attached_count = connection.scalar(
                    select(func.count())
                    .select_from(external_evidence_receipts)
                    .where(
                        external_evidence_receipts.c.query_receipt_id == receipt_id
                    )
                )
                if int(attached_count or 0) >= int(result_count):
                    raise ExternalEvidenceValidationError(
                        "Query receipt result count does not allow another evidence record"
                    )
                connection.execute(
                    insert(external_evidence_receipts).values(
                        evidence_id=evidence_id,
                        query_receipt_id=receipt_id,
                        attached_by=actor,
                        attached_at=now,
                    )
                )
        return evidence_id

    def get_external_evidence(
        self, case_id: str, evidence_id: str
    ) -> Optional[Dict[str, Any]]:
        """Read external evidence only through its owning case boundary."""
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(external_evidence_records).where(
                        external_evidence_records.c.id == evidence_id,
                        external_evidence_records.c.case_id == case_id,
                    )
                )
                .mappings()
                .first()
            )
        if not row:
            return None
        serialized = dict(row)
        for field in ("observed_at", "valid_from", "valid_to"):
            serialized[field] = _as_iso(row[field])
        with self.engine.connect() as connection:
            receipt_rows = list(
                connection.execute(
                    select(external_evidence_receipts)
                    .where(
                        external_evidence_receipts.c.evidence_id == evidence_id
                    )
                    .order_by(external_evidence_receipts.c.attached_at)
                ).mappings()
            )
        serialized["query_receipts"] = [
            {
                **dict(receipt_row),
                "attached_at": _as_iso(receipt_row["attached_at"]),
            }
            for receipt_row in receipt_rows
        ]
        return serialized

    def record_claim_observation(
        self,
        claim_id: str,
        *,
        source_engine: str,
        native_status: str,
        job_id: Optional[str] = None,
        external_evidence_id: Optional[str] = None,
        chat_message_id: Optional[str] = None,
        source_record_id: Optional[str] = None,
        confidence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Append idempotent provenance without overwriting claim history."""
        provenance_values = (job_id, external_evidence_id, chat_message_id)
        if sum(value is not None for value in provenance_values) != 1:
            raise ExternalEvidenceValidationError(
                "Provide exactly one provenance record"
            )
        with self.engine.begin() as connection:
            claim_case_id = connection.scalar(
                select(personas.c.case_id)
                .select_from(
                    persona_claims.join(
                        personas, personas.c.id == persona_claims.c.persona_id
                    )
                )
                .where(persona_claims.c.id == claim_id)
            )
            if not claim_case_id:
                raise KeyError(claim_id)
            provenance_id = str(job_id or external_evidence_id or chat_message_id)
            if job_id:
                provenance_case_id = connection.scalar(
                    select(investigation_jobs.c.case_id).where(
                        investigation_jobs.c.id == job_id
                    )
                )
                provenance_type = "investigation_job"
            elif external_evidence_id:
                provenance_case_id = connection.scalar(
                    select(external_evidence_records.c.case_id).where(
                        external_evidence_records.c.id == external_evidence_id
                    )
                )
                provenance_type = "external_evidence"
            else:
                provenance_case_id = connection.scalar(
                    select(case_chat_messages.c.case_id).where(
                        case_chat_messages.c.id == chat_message_id
                    )
                )
                provenance_type = "case_chat_message"
            if not provenance_case_id:
                raise KeyError(provenance_id)
            if provenance_case_id != claim_case_id:
                raise ExternalEvidenceValidationError(
                    "Claim and provenance belong to different cases"
                )
            return self._record_claim_observation_with_connection(
                connection,
                claim_id=claim_id,
                provenance_type=provenance_type,
                provenance_id=provenance_id,
                job_id=job_id,
                external_evidence_id=external_evidence_id,
                chat_message_id=chat_message_id,
                source_engine=source_engine,
                source_record_id=source_record_id,
                confidence=confidence,
                native_status=native_status,
                details=details or {},
                now=utcnow(),
            )

    def get_claim_lineage(self, claim_id: str) -> list[Dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = list(
                connection.execute(
                    select(claim_observations)
                    .where(claim_observations.c.claim_id == claim_id)
                    .order_by(claim_observations.c.observed_at)
                ).mappings()
            )
        return [
            {**dict(row), "observed_at": _as_iso(row["observed_at"])}
            for row in rows
        ]

    @staticmethod
    def _record_claim_observation_with_connection(
        connection: Connection,
        *,
        claim_id: str,
        provenance_type: str,
        provenance_id: str,
        job_id: Optional[str],
        external_evidence_id: Optional[str],
        chat_message_id: Optional[str],
        source_engine: str,
        source_record_id: Optional[str],
        confidence: Optional[int],
        native_status: str,
        details: Dict[str, Any],
        now: datetime,
    ) -> str:
        engine = bounded_text(source_engine, "source_engine", max_chars=100)
        status = bounded_text(native_status, "native_status", max_chars=100)
        record_id = (
            bounded_text(source_record_id, "source_record_id", max_chars=500)
            if source_record_id is not None
            else None
        )
        normalized_details = normalize_bounded_document(details, "details")
        normalized_confidence = None if confidence is None else int(confidence)
        if normalized_confidence is not None and not 0 <= normalized_confidence <= 100:
            raise ExternalEvidenceValidationError(
                "confidence must be between 0 and 100"
            )
        fingerprint = stable_fingerprint(
            {
                "provenance_type": provenance_type,
                "provenance_id": provenance_id,
                "source_engine": engine,
                "source_record_id": record_id,
                "confidence": normalized_confidence,
                "native_status": status,
                "details": normalized_details,
            }
        )
        existing_id = connection.scalar(
            select(claim_observations.c.id).where(
                claim_observations.c.claim_id == claim_id,
                claim_observations.c.fingerprint == fingerprint,
            )
        )
        if existing_id:
            return str(existing_id)
        observation_id = str(uuid.uuid4())
        connection.execute(
            insert(claim_observations).values(
                id=observation_id,
                claim_id=claim_id,
                provenance_type=provenance_type,
                provenance_id=provenance_id,
                job_id=job_id,
                external_evidence_id=external_evidence_id,
                chat_message_id=chat_message_id,
                source_engine=engine,
                source_record_id=record_id,
                confidence=normalized_confidence,
                native_status=status,
                details=normalized_details,
                fingerprint=fingerprint,
                observed_at=now,
            )
        )
        return observation_id

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
        job_id: Optional[str],
        provenance_type: str = "investigation_job",
        provenance_id: Optional[str] = None,
        chat_message_id: Optional[str] = None,
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
                    candidate.get("source_engine")
                    not in {
                        "openai_web_research",
                        "openai_case_chat_research",
                        "case_chat_user_statement",
                    }
                    or existing["review_status"] == "pending"
                ):
                    confidence = max(confidence, int(candidate["confidence"]))
                updated_values = {
                    "value": candidate["value"],
                    "display_value": candidate["display_value"],
                    "normalized_value": candidate["normalized_value"],
                    "confidence": confidence,
                    "last_seen_at": now,
                    "updated_at": now,
                }
                if job_id is not None:
                    updated_values["source_job_id"] = job_id
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
            observation_details = {
                "claim_fingerprint": candidate["fingerprint"],
                "evidence_fingerprints": [
                    evidence["fingerprint"] for evidence in candidate["evidence"]
                ],
            }
            if isinstance(candidate.get("observation_details"), dict):
                observation_details["observation"] = dict(
                    candidate["observation_details"]
                )
            CaseStore._record_claim_observation_with_connection(
                connection,
                claim_id=claim_id,
                provenance_type=provenance_type,
                provenance_id=str(provenance_id or job_id or chat_message_id),
                job_id=job_id,
                external_evidence_id=None,
                chat_message_id=chat_message_id,
                source_engine=candidate["source_engine"],
                source_record_id=candidate.get("source_record_id"),
                confidence=candidate.get("confidence"),
                native_status=candidate.get("native_status", "observed"),
                details=observation_details,
                now=now,
            )
            synchronized += 1
        return synchronized

    def sync_affiliation_discovery(
        self,
        job_id: str,
        observation: Dict[str, Any],
        *,
        registry_observations: Optional[list[Dict[str, Any]]] = None,
        website_observations: Optional[list[Dict[str, Any]]] = None,
    ) -> Dict[str, int]:
        from maigret.web.collector_adapters import (
            OFFICIAL_WEBSITE_ENGINE,
            REGISTRY_SOURCE_ENGINES,
            WIKIDATA_ENGINE,
            extract_official_website_affiliated_people,
            extract_registry_affiliated_people,
            extract_wikidata_affiliation_people,
        )

        people = extract_wikidata_affiliation_people(observation)
        registry_observations = list(registry_observations or [])[:5]
        for registry_observation in registry_observations:
            people.extend(
                extract_registry_affiliated_people(registry_observation)
            )
        website_observations = list(website_observations or [])[:2]
        for website_observation in website_observations:
            people.extend(
                extract_official_website_affiliated_people(website_observation)
            )
        if not people:
            return {"personas": 0, "claims": 0}

        organization = observation.get("organization")
        organization_label = (
            " ".join(str(organization.get("label") or "").split())[:500]
            if isinstance(organization, dict)
            else ""
        )
        if not organization_label:
            for registry_observation in registry_observations:
                selected_entity = registry_observation.get("selected_entity")
                if not isinstance(selected_entity, dict):
                    continue
                organization_label = " ".join(
                    str(selected_entity.get("legal_name") or "").split()
                )[:500]
                if organization_label:
                    break
        if not organization_label:
            for website_observation in website_observations:
                website_organization = website_observation.get("organization")
                if not isinstance(website_organization, dict):
                    continue
                organization_label = " ".join(
                    str(website_organization.get("name") or "").split()
                )[:500]
                if organization_label:
                    break
        if not organization_label:
            raise ValueError("Affiliation discovery is missing its organization")

        now = utcnow()
        synchronized = inserted_personas = 0
        with self.engine.begin() as connection:
            statement = select(
                investigation_jobs.c.case_id,
                investigation_jobs.c.kind,
                investigation_jobs.c.options,
            ).where(investigation_jobs.c.id == job_id)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            job = connection.execute(statement).mappings().first()
            if not job:
                raise KeyError(job_id)
            if job["kind"] != "affiliation":
                raise ValueError("Only affiliation jobs can synchronize this evidence")
            case_id = str(job["case_id"])

            personas_by_id = {}
            personas_by_registry_record = {}
            personas_by_public_record = {}
            personas_by_public_name = {}
            rows = connection.execute(
                select(personas.c.id, persona_claims.c.value)
                .join(persona_claims, persona_claims.c.persona_id == personas.c.id)
                .where(
                    personas.c.case_id == case_id,
                    persona_claims.c.field_name == "platform_identifier",
                    persona_claims.c.source_engine == WIKIDATA_ENGINE,
                )
            ).mappings()
            for row in rows:
                value = row["value"]
                if (
                    isinstance(value, dict)
                    and value.get("identifier_type") == "wikidata_item_id"
                ):
                    personas_by_id[
                        str(value.get("identifier") or "").upper()
                    ] = str(row["id"])
            for row in connection.execute(
                select(personas.c.id, claim_observations.c.source_record_id)
                .join(persona_claims, persona_claims.c.persona_id == personas.c.id)
                .join(
                    claim_observations,
                    claim_observations.c.claim_id == persona_claims.c.id,
                )
                .where(
                    personas.c.case_id == case_id,
                    persona_claims.c.field_name == "full_name",
                    claim_observations.c.source_engine.in_(
                        REGISTRY_SOURCE_ENGINES
                    ),
                    claim_observations.c.source_record_id.is_not(None),
                )
            ).mappings():
                personas_by_registry_record[
                    str(row["source_record_id"])
                ] = str(row["id"])
            for row in connection.execute(
                select(
                    personas.c.id,
                    persona_claims.c.value,
                    claim_observations.c.source_record_id,
                )
                .join(persona_claims, persona_claims.c.persona_id == personas.c.id)
                .join(
                    claim_observations,
                    claim_observations.c.claim_id == persona_claims.c.id,
                )
                .where(
                    personas.c.case_id == case_id,
                    persona_claims.c.field_name == "full_name",
                    claim_observations.c.source_engine == OFFICIAL_WEBSITE_ENGINE,
                    claim_observations.c.source_record_id.is_not(None),
                )
            ).mappings():
                persona_id = str(row["id"])
                source_record_id = str(row["source_record_id"])
                personas_by_public_record[source_record_id] = persona_id
                identity = " ".join(str(row["value"] or "").split()).casefold()
                if identity:
                    personas_by_public_name.setdefault(identity, persona_id)

            for person in people:
                wikidata_id = str(person.get("wikidata_id") or "").upper()
                claims = list(person.get("claims") or [])
                registry_record_id = next(
                    (
                        str(candidate.get("source_record_id"))
                        for candidate in claims
                        if candidate.get("source_engine")
                        in REGISTRY_SOURCE_ENGINES
                        and candidate.get("field_name") == "full_name"
                        and candidate.get("source_record_id")
                    ),
                    "",
                )
                public_record_ids = [
                    str(candidate.get("source_record_id"))
                    for candidate in claims
                    if candidate.get("source_engine") == OFFICIAL_WEBSITE_ENGINE
                    and candidate.get("field_name") == "full_name"
                    and candidate.get("source_record_id")
                ]
                public_name_identity = " ".join(
                    str(person.get("display_name") or "").split()
                ).casefold()
                persona_id = (
                    personas_by_id.get(wikidata_id)
                    if wikidata_id
                    else personas_by_registry_record.get(registry_record_id)
                )
                if not persona_id:
                    persona_id = next(
                        (
                            personas_by_public_record.get(source_record_id)
                            for source_record_id in public_record_ids
                            if personas_by_public_record.get(source_record_id)
                        ),
                        None,
                    )
                if not persona_id and public_record_ids and public_name_identity:
                    persona_id = personas_by_public_name.get(public_name_identity)
                if not persona_id:
                    persona_id = str(uuid.uuid4())
                    connection.execute(
                        insert(personas).values(
                            id=persona_id,
                            case_id=case_id,
                            display_name=person["display_name"],
                            created_at=now,
                        )
                    )
                    if wikidata_id:
                        personas_by_id[wikidata_id] = persona_id
                    elif registry_record_id:
                        personas_by_registry_record[
                            registry_record_id
                        ] = persona_id
                    elif public_record_ids:
                        for source_record_id in public_record_ids:
                            personas_by_public_record[source_record_id] = persona_id
                        if public_name_identity:
                            personas_by_public_name[
                                public_name_identity
                            ] = persona_id
                    inserted_personas += 1
                for source_record_id in public_record_ids:
                    personas_by_public_record[source_record_id] = persona_id
                if public_record_ids and public_name_identity:
                    personas_by_public_name[public_name_identity] = persona_id
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=persona_id,
                    job_id=job_id,
                    candidates=claims,
                    now=now,
                )

            specification = dict(job["options"] or {}).get(
                "investigation_spec"
            ) or {}
            legal_jurisdiction = specification.get("legal_jurisdiction")
            title = f"Affiliation: {organization_label}"
            if isinstance(legal_jurisdiction, dict) and legal_jurisdiction.get(
                "code"
            ):
                title += f" · {legal_jurisdiction['code']}"
            connection.execute(
                update(cases)
                .where(cases.c.id == case_id)
                .values(title=title[:500], updated_at=now)
            )
        return {"personas": inserted_personas, "claims": synchronized}

    def sync_identity_enrichment(
        self,
        job_id: str,
        wikipedia_observation: Dict[str, Any],
        icij_observation: Dict[str, Any],
    ) -> Dict[str, int]:
        """Persist public-record findings as pending, provenance-linked claims."""
        from maigret.web.collector_adapters import (
            extract_icij_offshore_claims,
            extract_wikipedia_person_claims,
        )

        wikipedia_claims = extract_wikipedia_person_claims(wikipedia_observation)
        offshore_claims = extract_icij_offshore_claims(icij_observation)
        now = utcnow()
        with self.engine.begin() as connection:
            statement = select(
                investigation_jobs.c.case_id,
                investigation_jobs.c.kind,
                investigation_jobs.c.options,
            ).where(investigation_jobs.c.id == job_id)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            job = connection.execute(statement).mappings().first()
            if not job:
                raise KeyError(job_id)
            if job["kind"] != "identity_enrichment":
                raise ValueError(
                    "Only identity-enrichment jobs can synchronize this evidence"
                )
            specification = dict(job["options"] or {}).get(
                "investigation_spec"
            ) or {}
            persona_id = str(specification.get("persona_id") or "")
            persona_case_id = connection.scalar(
                select(personas.c.case_id).where(personas.c.id == persona_id)
            )
            if not persona_case_id or persona_case_id != job["case_id"]:
                raise ValueError("Identity-enrichment Persona does not belong to its case")
            wikipedia_count = self._upsert_persona_candidates(
                connection,
                persona_id=persona_id,
                job_id=job_id,
                candidates=wikipedia_claims,
                now=now,
            )
            offshore_count = self._upsert_persona_candidates(
                connection,
                persona_id=persona_id,
                job_id=job_id,
                candidates=offshore_claims,
                now=now,
            )
            if wikipedia_count or offshore_count:
                connection.execute(
                    update(cases)
                    .where(cases.c.id == job["case_id"])
                    .values(updated_at=now)
                )
        return {"wikipedia_claims": wikipedia_count, "offshore_alerts": offshore_count}

    def sync_persona_claims(self, job_id: str, result: Dict[str, Any]) -> int:
        """Upsert deterministic claims while preserving every human decision."""
        from maigret.web.collector_adapters import (
            extract_github_profile_claims,
            extract_profile_url_evidence_claims,
            extract_user_scanner_claims,
        )
        from maigret.web.persona_intelligence import (
            extract_investigation_identifier_claims,
            extract_persona_claims,
        )

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
            if grouped_persona_id:
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=grouped_persona_id,
                    job_id=job_id,
                    candidates=extract_investigation_identifier_claims(
                        investigation_spec
                    ),
                    now=now,
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
            collector_observations = [
                observation
                for observation in result.get("collector_observations") or []
                if isinstance(observation, dict)
            ]
            if grouped_persona_id:
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=grouped_persona_id,
                    job_id=job_id,
                    candidates=extract_user_scanner_claims(
                        collector_observations
                    ),
                    now=now,
                )
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=grouped_persona_id,
                    job_id=job_id,
                    candidates=extract_github_profile_claims(
                        collector_observations
                    ),
                    now=now,
                )
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=grouped_persona_id,
                    job_id=job_id,
                    candidates=extract_profile_url_evidence_claims(
                        collector_observations
                    ),
                    now=now,
                )
            else:
                observations_by_username: Dict[str, list] = {}
                for observation in collector_observations:
                    username_key = str(
                        observation.get("subject_value") or ""
                    ).strip().casefold()
                    if username_key:
                        observations_by_username.setdefault(username_key, []).append(
                            observation
                        )
                for username_key, observations in observations_by_username.items():
                    persona_id = personas_by_name.get(username_key)
                    if not persona_id:
                        continue
                    synchronized += self._upsert_persona_candidates(
                        connection,
                        persona_id=persona_id,
                        job_id=job_id,
                        candidates=extract_github_profile_claims(observations),
                        now=now,
                    )
                    synchronized += self._upsert_persona_candidates(
                        connection,
                        persona_id=persona_id,
                        job_id=job_id,
                        candidates=extract_profile_url_evidence_claims(observations),
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

    def sync_case_chat_persona_claims(
        self,
        case_id: str,
        persona_id: str,
        candidates: Iterable[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Persist validated chat findings as pending, provenance-linked claims."""
        now = utcnow()
        synchronized = 0
        accepted = []
        with self.engine.begin() as connection:
            persona_case_id = connection.scalar(
                select(personas.c.case_id).where(personas.c.id == persona_id)
            )
            if not persona_case_id:
                raise KeyError(persona_id)
            if persona_case_id != case_id:
                raise ValueError("Persona does not belong to this case")
            for candidate in list(candidates)[:100]:
                message_id = str(candidate.get("provenance_message_id") or "")
                message_case_id = connection.scalar(
                    select(case_chat_messages.c.case_id).where(
                        case_chat_messages.c.id == message_id
                    )
                )
                if not message_case_id:
                    raise KeyError(message_id)
                if message_case_id != case_id:
                    raise ValueError("Chat message does not belong to this case")
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=persona_id,
                    job_id=None,
                    provenance_type="case_chat_message",
                    provenance_id=message_id,
                    chat_message_id=message_id,
                    candidates=[candidate],
                    now=now,
                )
                accepted.append(
                    {
                        "field_name": str(candidate.get("field_name") or "")[:64],
                        "display_value": str(
                            candidate.get("display_value") or ""
                        )[:4000],
                        "confidence": int(candidate.get("confidence") or 0),
                        "evidence_basis": str(
                            candidate.get("evidence_basis") or ""
                        )[:32],
                    }
                )
            if synchronized:
                connection.execute(
                    update(cases).where(cases.c.id == case_id).values(updated_at=now)
                )
        return {
            "count": synchronized,
            "case_id": case_id,
            "persona_id": persona_id,
            "proposals": accepted,
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

    def delete_job(
        self, job_id: str, *, confirmation_name: Optional[str] = None
    ) -> bool:
        with self.engine.begin() as connection:
            statement = (
                select(
                    investigation_jobs.c.case_id,
                    investigation_jobs.c.status,
                    cases.c.title.label("case_title"),
                )
                .join(cases, cases.c.id == investigation_jobs.c.case_id)
                .where(investigation_jobs.c.id == job_id)
            )
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (
                connection.execute(statement)
                .mappings()
                .first()
            )
            if not row:
                return False
            if row["status"] not in TERMINAL_STATUSES:
                raise ValueError("Active investigations cannot be deleted")
            if confirmation_name is not None and confirmation_name != row["case_title"]:
                raise ValueError("Case name confirmation does not match")
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

    def delete_case(
        self, case_id: str, *, confirmation_name: Optional[str] = None
    ) -> bool:
        """Delete a case atomically once none of its investigations are active."""
        with self.engine.begin() as connection:
            statement = select(cases.c.id, cases.c.title).where(cases.c.id == case_id)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            stored_case = connection.execute(statement).mappings().first()
            if not stored_case:
                return False
            if (
                confirmation_name is not None
                and confirmation_name != stored_case["title"]
            ):
                raise ValueError("Case name confirmation does not match")
            active_job = connection.scalar(
                select(investigation_jobs.c.id)
                .where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.status.in_(ACTIVE_STATUSES),
                )
                .limit(1)
            )
            if active_job:
                raise ActiveInvestigationError(
                    "Cases with active investigations cannot be deleted"
                )
            connection.execute(delete(cases).where(cases.c.id == case_id))
        return True

    @staticmethod
    def _serialize_job(row) -> Dict[str, Any]:
        result = dict(row.get("result") or {})
        payload = {
            "job_id": row["id"],
            "case_id": row["case_id"],
            "case_title": row.get("case_title"),
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
