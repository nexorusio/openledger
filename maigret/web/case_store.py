"""Persistent case and investigation-job storage for OpenLedger."""

from __future__ import annotations

import hashlib
import hmac
import json
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
from maigret.web.execution_budget import execution_budget_spec_from_options
from maigret.web.profile_discovery_policy import (
    PROFILE_DISCOVERY_JOB_KINDS,
    ProfileDiscoveryPolicyError,
    govern_profile_discovery_options,
)
from maigret.web.profile_reliability import PROFILE_RELIABILITY_VERSION
from maigret.web.profile_search_facebook import parse_facebook_profile_url
from maigret.web.profile_search_instagram import parse_instagram_profile_url
from maigret.web.profile_search_threads import parse_threads_profile_url
from maigret.web.profile_search_tiktok import parse_tiktok_profile_url
from maigret.web.profile_search_x import parse_x_profile_url

metadata = MetaData()
json_document = JSON().with_variant(JSONB(), "postgresql")
LEGACY_PROFILE_CLAIM_ENGINES = frozenset(
    {
        "github_public_profile",
        "openledger_profile_discovery",
        "unfurl_url_analysis",
        "wayback_cdx",
    }
)
LEGACY_UNTRIAGED_SOURCE_PREFIX = "legacy_untriaged:"
RELIABILITY_MIGRATION_REVIEWER = "openledger-reliability-migration"
LEGACY_EVIDENCE_MARKER = "_openledger_reliability"


def _legacy_untriaged_source(
    review_status: str,
    source_engine: str,
) -> str:
    return (
        f"{LEGACY_UNTRIAGED_SOURCE_PREFIX}{review_status}:{source_engine}"
    )[:100]


def _legacy_untriaged_source_details(source_engine: Any) -> Optional[tuple[str, str]]:
    value = str(source_engine or "")
    if not value.startswith(LEGACY_UNTRIAGED_SOURCE_PREFIX):
        return None
    status, separator, original_engine = value.removeprefix(
        LEGACY_UNTRIAGED_SOURCE_PREFIX
    ).partition(":")
    if (
        not separator
        or status not in {"pending", "approved", "rejected", "uncertain"}
        or not original_engine
    ):
        return None
    return status, original_engine


def _is_retired_legacy_evidence(details: Any) -> bool:
    if not isinstance(details, dict):
        return False
    marker = details.get(LEGACY_EVIDENCE_MARKER)
    return isinstance(marker, dict) and marker.get("status") == "legacy_untriaged"


cases = Table(
    "cases",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("title", String(500), nullable=False),
    Column("status", String(32), nullable=False, server_default="open"),
    Column("case_type", String(32), nullable=False, server_default="standalone"),
    Column("purpose", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "status IN ('open', 'closed', 'archived')",
        name="ck_cases_status",
    ),
    CheckConstraint(
        "case_type IN ('standalone', 'combined')",
        name="ck_cases_case_type",
    ),
)

combined_case_members = Table(
    "combined_case_members",
    metadata,
    Column(
        "combined_case_id",
        String(36),
        ForeignKey("cases.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "source_case_id",
        String(36),
        ForeignKey("cases.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column("position", Integer, nullable=False),
    Column("added_by", String(200), nullable=False),
    Column("added_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "combined_case_id <> source_case_id",
        name="ck_combined_case_members_distinct",
    ),
    CheckConstraint("position >= 0", name="ck_combined_case_members_position"),
)
Index(
    "ix_combined_case_members_source",
    combined_case_members.c.source_case_id,
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


def _active_claim_evidence_clause():
    marker_status = claim_evidence.c.details[LEGACY_EVIDENCE_MARKER][
        "status"
    ].as_string()
    return or_(
        marker_status.is_(None),
        marker_status != "legacy_untriaged",
    )

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
    Column("cancel_requested_at", DateTime(timezone=True), nullable=True),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("worker_id", String(200), nullable=True),
    Column("budget_seconds", Integer, nullable=True),
    Column("budget_policy_version", String(64), nullable=True),
    Column("deadline_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("heartbeat_at", DateTime(timezone=True), nullable=True),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "status IN ('queued', 'running', 'cancel_requested', 'completed', "
        "'failed', 'cancelled', 'interrupted', 'budget_exhausted')",
        name="ck_investigation_jobs_status",
    ),
    CheckConstraint("attempts >= 0", name="ck_investigation_jobs_attempts"),
    CheckConstraint(
        "budget_seconds IS NULL OR budget_seconds > 0",
        name="ck_investigation_jobs_budget_seconds",
    ),
)
Index(
    "ix_investigation_jobs_status_created",
    investigation_jobs.c.status,
    investigation_jobs.c.created_at,
)
Index("ix_investigation_jobs_case_id", investigation_jobs.c.case_id)

combined_analysis_runs = Table(
    "combined_analysis_runs",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "combined_case_id",
        String(36),
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "job_id",
        String(36),
        ForeignKey("investigation_jobs.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    ),
    Column("snapshot_sha256", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("model", String(100), nullable=True),
    Column("web_search_enabled", Boolean, nullable=False, server_default="false"),
    Column("executive_summary", Text, nullable=True),
    Column("key_findings", json_document, nullable=False),
    Column("contradictions", json_document, nullable=False),
    Column("information_gaps", json_document, nullable=False),
    Column("next_steps", json_document, nullable=False),
    Column("sources", json_document, nullable=False),
    Column("error", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    CheckConstraint(
        "status IN ('processing', 'completed', 'unavailable', 'failed', 'cancelled')",
        name="ck_combined_analysis_runs_status",
    ),
)
Index(
    "ix_combined_analysis_runs_case_created",
    combined_analysis_runs.c.combined_case_id,
    combined_analysis_runs.c.created_at,
)

combined_relationship_proposals = Table(
    "combined_relationship_proposals",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "analysis_run_id",
        String(36),
        ForeignKey("combined_analysis_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "chat_message_id",
        String(36),
        ForeignKey("case_chat_messages.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("title", String(500), nullable=False),
    Column("relationship_type", String(64), nullable=False),
    Column("subject_ref", String(100), nullable=False),
    Column("subject_entity", json_document, nullable=False),
    Column("object_ref", String(100), nullable=False),
    Column("object_entity", json_document, nullable=False),
    Column("explanation", Text, nullable=False),
    Column("confidence", Integer, nullable=False),
    Column("evidence", json_document, nullable=False),
    Column("contradictory_evidence", json_document, nullable=False),
    Column("limitations", json_document, nullable=False),
    Column("review_status", String(16), nullable=False, server_default="pending"),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("reviewed_at", DateTime(timezone=True), nullable=True),
    Column("reviewed_by", String(200), nullable=True),
    CheckConstraint(
        "confidence >= 0 AND confidence <= 85",
        name="ck_combined_relationship_proposals_confidence",
    ),
    CheckConstraint(
        "review_status IN ('pending', 'approved', 'rejected', 'uncertain')",
        name="ck_combined_relationship_proposals_review_status",
    ),
)
Index(
    "ix_combined_relationship_proposals_run_status",
    combined_relationship_proposals.c.analysis_run_id,
    combined_relationship_proposals.c.review_status,
)
Index(
    "ix_combined_relationship_proposals_chat_message",
    combined_relationship_proposals.c.chat_message_id,
)

combined_relationship_reviews = Table(
    "combined_relationship_reviews",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "proposal_id",
        String(36),
        ForeignKey("combined_relationship_proposals.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("decision", String(16), nullable=False),
    Column("reviewer", String(200), nullable=False),
    Column("note", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "decision IN ('approved', 'rejected', 'uncertain')",
        name="ck_combined_relationship_reviews_decision",
    ),
)
Index(
    "ix_combined_relationship_reviews_proposal",
    combined_relationship_reviews.c.proposal_id,
    combined_relationship_reviews.c.created_at,
)

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

profile_search_audits = Table(
    "profile_search_audits",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "job_id",
        String(36),
        ForeignKey("investigation_jobs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("status", String(16), nullable=False),
    Column("stopped", Boolean, nullable=False, server_default="false"),
    Column("orchestration_version", Integer, nullable=False),
    Column("planned_query_count", Integer, nullable=False),
    Column("executed_query_count", Integer, nullable=False),
    Column("error_count", Integer, nullable=False),
    Column("candidate_count", Integer, nullable=False),
    Column("document_sha256", String(64), nullable=False),
    Column("document", json_document, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "status IN ('completed', 'partial', 'failed', 'stopped')",
        name="ck_profile_search_audits_status",
    ),
    CheckConstraint(
        "planned_query_count >= 0 AND executed_query_count >= 0 "
        "AND executed_query_count <= planned_query_count",
        name="ck_profile_search_audits_query_counts",
    ),
    CheckConstraint(
        "error_count >= 0 AND error_count <= executed_query_count",
        name="ck_profile_search_audits_error_count",
    ),
    CheckConstraint(
        "candidate_count >= 0 "
        "AND candidate_count <= planned_query_count * 10",
        name="ck_profile_search_audits_candidate_count",
    ),
    CheckConstraint(
        "orchestration_version > 0",
        name="ck_profile_search_audits_orchestration_version",
    ),
    UniqueConstraint(
        "job_id",
        "document_sha256",
        name="uq_profile_search_audits_job_document",
    ),
)
Index(
    "ix_profile_search_audits_job_created",
    profile_search_audits.c.job_id,
    profile_search_audits.c.created_at,
)

profile_search_candidate_reviews = Table(
    "profile_search_candidate_reviews",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "audit_id",
        String(36),
        ForeignKey("profile_search_audits.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("candidate_id", String(100), nullable=False),
    Column(
        "persona_id",
        String(36),
        ForeignKey("personas.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "claim_id",
        String(36),
        ForeignKey("persona_claims.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("decision", String(16), nullable=False),
    Column("reviewer", String(200), nullable=False),
    Column("note", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "decision IN ('proposed', 'rejected', 'uncertain')",
        name="ck_profile_search_candidate_reviews_decision",
    ),
)
Index(
    "ix_profile_search_candidate_reviews_lookup",
    profile_search_candidate_reviews.c.audit_id,
    profile_search_candidate_reviews.c.candidate_id,
    profile_search_candidate_reviews.c.persona_id,
    profile_search_candidate_reviews.c.created_at,
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
Index(
    "ix_query_receipts_case_created",
    query_receipts.c.case_id,
    query_receipts.c.created_at,
)
Index(
    "ix_query_receipts_source_status",
    query_receipts.c.source_id,
    query_receipts.c.status,
)

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
Index(
    "ix_external_evidence_case_source",
    external_evidence_records.c.case_id,
    external_evidence_records.c.source_id,
)
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
Index(
    "ix_claim_observations_claim_observed",
    claim_observations.c.claim_id,
    claim_observations.c.observed_at,
)
Index(
    "ix_claim_observations_provenance",
    claim_observations.c.provenance_type,
    claim_observations.c.provenance_id,
)
Index("ix_claim_observations_chat_message", claim_observations.c.chat_message_id)


TERMINAL_STATUSES = {
    "completed",
    "failed",
    "cancelled",
    "interrupted",
    "budget_exhausted",
}
ACTIVE_STATUSES = {"queued", "running", "cancel_requested"}
WORKER_LOCK_KEY = 5714024849188199506
WORKER_HEARTBEAT_SECONDS = 5
WORKER_STALE_AFTER_SECONDS = 30
MAX_COMBINED_SOURCE_CASES = 10
MAX_COMBINED_APPROVED_CLAIMS = 10_000
MAX_COMBINED_EVIDENCE_REFERENCES = 50_000
MAX_COMBINED_RELATIONSHIP_EDGES = 20_000
MAX_COMBINED_AI_CLAIMS = 500
MAX_COMBINED_AI_PROPOSALS = 100
MAX_PROFILE_SEARCH_AUDIT_BYTES = 12_000_000
MAX_PROFILE_SEARCH_AUDITS_PER_JOB = 10
MAX_PROFILE_SEARCH_UI_CANDIDATES = 100
MAX_PROFILE_SEARCH_UI_REVIEWS = 500
PROFILE_SEARCH_PENDING_CLAIM_CONFIDENCE = 50
PROFILE_SEARCH_PLATFORM_PARSERS = {
    "facebook": parse_facebook_profile_url,
    "instagram": parse_instagram_profile_url,
    "threads": parse_threads_profile_url,
    "tiktok": parse_tiktok_profile_url,
    "x": parse_x_profile_url,
}
PROFILE_SEARCH_CANDIDATE_ID_PATTERN = re.compile(
    r"^profile-search:[0-9a-f]{64}$"
)
RELATIONSHIP_FIELDS = {
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


def _bounded_round_robin_case_records(
    records: Iterable[Dict[str, Any]], case_ids: Iterable[str], limit: int
) -> list[Dict[str, Any]]:
    """Select a deterministic, balanced record projection across source cases."""
    ordered_case_ids = [str(case_id) for case_id in case_ids]
    buckets = {case_id: [] for case_id in ordered_case_ids}
    for record in records:
        case_id = str(record.get("case_id") or "")
        if case_id in buckets:
            buckets[case_id].append(record)
    selected = []
    position = 0
    bounded_limit = max(0, int(limit))
    while len(selected) < bounded_limit:
        appended = False
        for case_id in ordered_case_ids:
            bucket = buckets[case_id]
            if position >= len(bucket):
                continue
            selected.append(bucket[position])
            appended = True
            if len(selected) >= bounded_limit:
                break
        if not appended:
            break
        position += 1
    return selected


def _persona_candidate_identity_match(candidate: Dict[str, Any]):
    """Build the exact cross-source identity predicate for one claim."""
    identity_match = (
        persona_claims.c.fingerprint == candidate["fingerprint"]
    )
    if (
        candidate.get("source_engine")
        not in {"openai_web_research", "native_profile_search_review"}
        or candidate.get("field_name") != "social_account"
    ):
        return identity_match
    identity_match = or_(
        identity_match,
        (
            (persona_claims.c.field_name == "social_account")
            & (persona_claims.c.display_value == candidate["display_value"])
        ),
    )
    return identity_match


def _profile_search_candidate_alias_identity(candidate: Dict[str, Any]):
    """Derive a supported account identity only from its validated URL."""
    if (
        candidate.get("source_engine")
        not in {"openai_web_research", "native_profile_search_review"}
        or candidate.get("field_name") != "social_account"
    ):
        return None
    value = candidate.get("value")
    candidate_url = (
        value.get("url") if isinstance(value, dict) else None
    ) or candidate.get("display_value")
    if not candidate_url:
        return None
    for parser in PROFILE_SEARCH_PLATFORM_PARSERS.values():
        profile_reference = parser(str(candidate_url))
        if profile_reference is None:
            continue
        return profile_reference.handle.casefold(), parser
    return None


def _persona_candidate_claim_with_connection(
    connection: Connection,
    persona_id: str,
    candidate: Dict[str, Any],
):
    """Find an exact or URL-verified alias match for one candidate."""
    exact = (
        connection.execute(
            select(persona_claims)
            .where(
                persona_claims.c.persona_id == persona_id,
                _persona_candidate_identity_match(candidate),
            )
            .order_by(
                persona_claims.c.created_at.asc(),
                persona_claims.c.id.asc(),
            )
        )
        .mappings()
        .first()
    )
    if exact is not None:
        return exact
    alias_identity = _profile_search_candidate_alias_identity(candidate)
    if alias_identity is None:
        return None
    candidate_handle, parser = alias_identity
    possible_matches = connection.execute(
        select(persona_claims)
        .where(
            persona_claims.c.persona_id == persona_id,
            persona_claims.c.field_name == "social_account",
        )
        .order_by(
            persona_claims.c.created_at.asc(),
            persona_claims.c.id.asc(),
        )
    ).mappings()
    for possible_match in possible_matches:
        stored_value = possible_match["value"]
        stored_url = (
            stored_value.get("url")
            if isinstance(stored_value, dict)
            else None
        ) or possible_match["display_value"]
        profile_reference = parser(str(stored_url or ""))
        if (
            profile_reference is not None
            and profile_reference.handle.casefold() == candidate_handle
        ):
            return possible_match
    return None


class ActiveInvestigationError(ValueError):
    """Raised when destructive case changes race an active investigation."""


class ReferencedCaseError(ValueError):
    """Raised when a source case is still retained by a combined case."""

    def __init__(self, references: Iterable[Dict[str, str]]):
        self.references = [dict(item) for item in references]
        super().__init__("Source case is referenced by a combined investigation")


class StaleCombinedSnapshotError(ValueError):
    """Raised when chat output no longer matches the active combined snapshot."""


# Register additive P2 tables with the shared metadata, without a store import cycle.
from maigret.web.pipeline_schema import register_pipeline_schema
from maigret.web.pipeline_projection_schema import register_projection_schema

register_pipeline_schema(metadata)
from maigret.web.pipeline_connector_schema import register_connector_ingestion_schema

register_connector_ingestion_schema(metadata)

register_projection_schema(metadata)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _heartbeat_expired(value: Optional[datetime], *, now: datetime) -> bool:
    if value is None:
        return True
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value < now - timedelta(seconds=WORKER_STALE_AFTER_SECONDS)


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


def _profile_search_document_sha256(document: Dict[str, Any]) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _profile_search_audit_document(result: Any) -> tuple[Dict[str, Any], str]:
    """Validate and fingerprint one bounded, review-safe search result."""
    from maigret.web.profile_search_orchestrator import (
        PROFILE_SEARCH_TERMINAL_ERROR_CODES,
        ProfileSearchDiscoveryResult,
    )
    from maigret.web.profile_search_backend import ProfileSearchRun
    from maigret.web.profile_search_contract import (
        MAX_PROFILE_SEARCH_RESULTS,
        ProfileSearchQuery,
    )
    from maigret.web.profile_search_planner import MAX_PROFILE_SEARCH_QUERIES
    from maigret.web.profile_search_ranking import (
        RankedProfileSearchCandidate,
    )

    if not isinstance(result, ProfileSearchDiscoveryResult):
        raise ValueError("A profile-search discovery result is required")
    if not all(
        isinstance(items, tuple)
        for items in (result.queries, result.runs, result.candidates)
    ):
        raise ValueError("Profile-search audit collections must be immutable")
    if not all(isinstance(query, ProfileSearchQuery) for query in result.queries):
        raise ValueError("Profile-search audit contains an invalid query")
    if not all(isinstance(run, ProfileSearchRun) for run in result.runs):
        raise ValueError("Profile-search audit contains an invalid run")
    if not all(
        isinstance(candidate, RankedProfileSearchCandidate)
        for candidate in result.candidates
    ):
        raise ValueError("Profile-search audit contains an invalid candidate")
    document = result.as_dict()
    if result.status not in {"completed", "partial", "failed", "stopped"}:
        raise ValueError("Invalid profile-search audit status")
    if result.stopped != (result.status == "stopped"):
        raise ValueError("Profile-search stop status is inconsistent")
    if not 0 <= result.planned_query_count <= MAX_PROFILE_SEARCH_QUERIES:
        raise ValueError("Profile-search audit exceeds the query limit")
    if not 0 <= result.executed_query_count <= result.planned_query_count:
        raise ValueError("Profile-search audit query counts are inconsistent")
    if not 0 <= result.error_count <= result.executed_query_count:
        raise ValueError("Profile-search audit error count is inconsistent")
    planned_by_id = {query.query_id: query for query in result.queries}
    if len(planned_by_id) != result.planned_query_count:
        raise ValueError("Profile-search audit contains duplicate queries")
    if any(
        planned_by_id.get(run.query.query_id) != run.query
        for run in result.runs
    ):
        raise ValueError("Profile-search audit run is not in its query plan")
    if len(result.candidates) > (
        result.planned_query_count * MAX_PROFILE_SEARCH_RESULTS
    ):
        raise ValueError("Profile-search audit exceeds the candidate limit")
    if result.status == "completed" and (
        result.error_count or result.skipped_query_count
    ):
        raise ValueError("Completed profile-search audit is inconsistent")
    terminal_failure = bool(
        result.runs
        and result.runs[-1].error is not None
        and result.runs[-1].error.code
        in PROFILE_SEARCH_TERMINAL_ERROR_CODES
    )
    if result.status == "partial" and not (
        0 < result.error_count < result.executed_query_count
        and (
            result.skipped_query_count == 0
            or terminal_failure
        )
    ):
        raise ValueError("Partial profile-search audit is inconsistent")
    if result.status == "failed":
        if not (
            result.executed_query_count > 0
            and result.error_count == result.executed_query_count
            and (
                result.skipped_query_count == 0
                or terminal_failure
            )
        ):
            raise ValueError("Failed profile-search audit is inconsistent")
    if result.status == "stopped" and result.skipped_query_count <= 0:
        raise ValueError("Stopped profile-search audit is inconsistent")
    for candidate in result.candidates:
        serialized = candidate.as_dict()
        if (
            serialized.get("account_status") != "candidate"
            or serialized.get("identity_status") != "unverified"
            or serialized.get("review_status") != "pending"
            or serialized.get("score_scope")
            != "discovery_review_priority"
        ):
            raise ValueError("Profile-search candidate is not review safe")
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > MAX_PROFILE_SEARCH_AUDIT_BYTES:
        raise ValueError("Profile-search audit document is too large")
    return document, hashlib.sha256(encoded).hexdigest()


def _serialize_profile_search_audit(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row["id"]),
        "job_id": str(row["job_id"]),
        "status": str(row["status"]),
        "stopped": bool(row["stopped"]),
        "orchestration_version": int(row["orchestration_version"]),
        "planned_query_count": int(row["planned_query_count"]),
        "executed_query_count": int(row["executed_query_count"]),
        "error_count": int(row["error_count"]),
        "candidate_count": int(row["candidate_count"]),
        "document_sha256": str(row["document_sha256"]),
        "document": dict(row["document"]),
        "created_at": _as_iso(row["created_at"]),
    }


def _profile_search_candidate_from_document(
    document: Any, candidate_id: str
) -> Dict[str, Any]:
    """Resolve one immutable unverified candidate from a stored audit."""
    if not isinstance(document, dict):
        raise ValueError("Profile-search audit document is invalid")
    matches = [
        candidate
        for candidate in list(document.get("candidates") or [])
        if isinstance(candidate, dict)
        and candidate.get("candidate_id") == candidate_id
    ]
    if len(matches) != 1:
        raise KeyError(candidate_id)
    candidate = dict(matches[0])
    if (
        candidate.get("account_status") != "candidate"
        or candidate.get("identity_status") != "unverified"
        or candidate.get("review_status") != "pending"
        or candidate.get("score_scope") != "discovery_review_priority"
    ):
        raise ValueError("Profile-search candidate is not review safe")
    platform = str(candidate.get("platform") or "").strip().casefold()
    handle = str(candidate.get("handle") or "").strip().casefold()
    profile_url = str(candidate.get("profile_url") or "").strip()
    try:
        parsed = urlsplit(profile_url)
        port = parsed.port
    except ValueError as error:
        raise ValueError("Profile-search candidate URL is invalid") from error
    if (
        platform not in {"facebook", "instagram", "threads", "tiktok", "x"}
        or not handle
        or len(handle) > 128
        or parsed.scheme.casefold() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
    ):
        raise ValueError("Profile-search candidate identity is invalid")
    expected_id = "profile-search:" + hashlib.sha256(
        f"{platform}\0{handle}".encode("utf-8")
    ).hexdigest()
    if not hmac.compare_digest(candidate_id, expected_id):
        raise ValueError("Profile-search candidate identity is inconsistent")
    return candidate


def _bounded_chat_sources(value: Any) -> list[Dict[str, str]]:
    """Retain only bounded, public HTTP(S) citations on chat messages."""
    if not isinstance(value, list):
        return []
    sources: list[Dict[str, str]] = []
    seen = set()
    for item in value[:100]:
        if not isinstance(item, dict):
            continue
        candidate = str(item.get("url") or "")
        if len(candidate) > 2000 or candidate != candidate.strip():
            continue
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

    @staticmethod
    def _job_persona_bindings(investigation_spec, persona_rows):
        """Resolve queued ownership by ID, with a fallback for historical jobs."""
        specification = (
            investigation_spec if isinstance(investigation_spec, dict) else {}
        )
        known_ids = {row["id"] for row in persona_rows}
        target_id = str(specification.get("target_persona_id") or "")
        grouped_id = target_id if target_id in known_ids else None
        if "persona_bindings" in specification:
            by_username: Dict[str, list[str]] = {}
            for binding in specification.get("persona_bindings") or []:
                if (
                    not isinstance(binding, dict)
                    or binding.get("persona_id") not in known_ids
                ):
                    continue
                for username in binding.get("usernames") or []:
                    key = str(username).strip().casefold()
                    owners = by_username.setdefault(key, [])
                    if binding["persona_id"] not in owners:
                        owners.append(binding["persona_id"])
            return grouped_id, by_username
        if (
            not target_id
            and specification.get("processing_mode") == "same_subject"
            and len(persona_rows) == 1
        ):
            grouped_id = persona_rows[0]["id"]
        return grouped_id, {
            str(row["display_name"]).strip().casefold(): [row["id"]]
            for row in persona_rows
        }

    @staticmethod
    def _persona_input_specification(investigation_spec, persona_id):
        """Limit supplied context to its entered subject, even for shared handles."""
        if not isinstance(investigation_spec, dict):
            return {}
        for binding in investigation_spec.get("persona_bindings") or []:
            if binding.get("persona_id") == persona_id and "identifiers" in binding:
                return dict(
                    investigation_spec,
                    processing_mode="same_subject",
                    identifiers=binding["identifiers"],
                )
        return investigation_spec

    @staticmethod
    def _profile_source_persona_ids(investigation_spec, persona_ids, source_url):
        """Respect explicit profile ownership when distinct subjects share a handle."""
        if (
            not isinstance(investigation_spec, dict)
            or investigation_spec.get("processing_mode") != "independent"
            or not source_url
        ):
            return persona_ids

        def url_key(value):
            from maigret.web.persona_intelligence import (
                supported_profile_identity_key,
            )

            profile_identity = supported_profile_identity_key(value)
            if profile_identity is not None:
                return ("supported_profile", *profile_identity)
            try:
                parsed = urlsplit(str(value or ""))
            except ValueError:
                return None
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                return None
            return (
                "exact_url",
                parsed.scheme.casefold(),
                parsed.netloc.casefold(),
                parsed.path.rstrip("/") or "/",
                parsed.query,
            )

        key = url_key(source_url)
        bindings = [
            binding
            for binding in investigation_spec.get("persona_bindings") or []
            if isinstance(binding, dict)
        ]
        explicit_profile_bindings = [
            binding
            for binding in bindings
            if any(
                isinstance(identifier, dict)
                and identifier.get("type") == "profile_url"
                for identifier in binding.get("identifiers") or []
            )
        ]
        owners = {
            binding.get("persona_id")
            for binding in explicit_profile_bindings
            if any(
                identifier.get("type") == "profile_url"
                and key is not None
                and url_key(identifier.get("value")) == key
                for identifier in binding.get("identifiers") or []
            )
        }
        if not explicit_profile_bindings:
            # Username-only independent investigations have no source URL
            # ownership to apply; keep their seed-based routing intact.
            return persona_ids
        # Once explicit profile inputs exist, an unmatched URL is ambiguous and
        # must not be broadcast to every subject in the case.
        return [persona_id for persona_id in persona_ids if persona_id in owners]

    def create_investigation(
        self,
        usernames: Iterable[str],
        options: Dict[str, Any],
        *,
        kind: str = "live",
    ) -> str:
        from maigret.web.persona_intelligence import extract_supplied_profile_claims

        normalized = [str(value).strip() for value in usernames if str(value).strip()]
        investigation_spec = options.get("investigation_spec")
        identifiers = (
            list(investigation_spec.get("identifiers") or [])
            if isinstance(investigation_spec, dict) else []
        )
        if not normalized and not identifiers:
            raise ValueError("At least one investigation identifier is required")
        grouped = (
            isinstance(investigation_spec, dict)
            and investigation_spec.get("processing_mode") == "same_subject"
        )
        subject_label = (
            str(investigation_spec.get("subject_label") or "").strip()
            if isinstance(investigation_spec, dict)
            else ""
        )
        group_specs = (
            [
                {
                    "label": subject_label or (normalized[0] if normalized else str(identifiers[0].get("value") or "Subject")),
                    "usernames": normalized,
                    "identifiers": investigation_spec.get("identifiers", []),
                }
            ]
            if grouped
            else (
                investigation_spec.get("subject_groups")
                if isinstance(investigation_spec, dict)
                else None
            )
        )
        if group_specs is None:
            group_specs = [
                {"label": username, "usernames": [username]} for username in normalized
            ]
        if not isinstance(group_specs, list) or not group_specs:
            raise ValueError("At least one subject group is required")
        target_keys = {username.casefold() for username in normalized}
        covered_keys = set()
        new_personas = []
        persona_bindings = []
        for group in group_specs:
            label = str(group.get("label") or "").strip()[:500]
            usernames_for_persona = [
                str(value).strip() for value in group.get("usernames", [])
            ]
            group_keys = {value.casefold() for value in usernames_for_persona}
            if not label or not group_keys.issubset(target_keys):
                raise ValueError("Invalid subject group")
            covered_keys.update(group_keys)
            persona_id = str(uuid.uuid4())
            new_personas.append({"id": persona_id, "display_name": label})
            persona_bindings.append(
                {
                    "persona_id": persona_id,
                    "subject_label": label,
                    "usernames": usernames_for_persona,
                    **(
                        {"identifiers": group["identifiers"]}
                        if "identifiers" in group
                        else {}
                    ),
                }
            )
        if covered_keys != target_keys:
            raise ValueError("Every search target needs a subject group")
        now = utcnow()
        case_id = str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        stored_options = (
            govern_profile_discovery_options(options)
            if kind in PROFILE_DISCOVERY_JOB_KINDS
            else dict(options)
        )
        # Capture ownership once, when the new Personas are created. Labels may
        # later change after review; existing jobs and Personas are not rewritten.
        specification = (
            dict(investigation_spec) if isinstance(investigation_spec, dict) else {}
        )
        specification["persona_bindings"] = persona_bindings
        specification["pipeline_id"] = "p2-e2e-v1"
        if grouped:
            specification["target_persona_id"] = new_personas[0]["id"]
        else:
            specification.pop("target_persona_id", None)
        stored_options["investigation_spec"] = specification
        budget = (
            execution_budget_spec_from_options(stored_options)
            if kind in PROFILE_DISCOVERY_JOB_KINDS
            else None
        )
        if budget is not None:
            stored_options["execution_mode"] = budget["mode"]
            stored_options["all_sites"] = budget["mode"] == "exhaustive"
            stored_options["execution_budget"] = dict(budget)
        title = ", ".join(persona["display_name"] for persona in new_personas)[:500]
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
                        "id": persona["id"],
                        "case_id": case_id,
                        "display_name": persona["display_name"],
                        "created_at": now,
                    }
                    for persona in new_personas
                ],
            )
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=case_id,
                    kind=kind,
                    status="queued",
                    usernames=normalized,
                    options=stored_options,
                    progress={"checked": 0, "total": None, "found": 0},
                    result=None,
                    error=None,
                    cancel_requested=False,
                    attempts=0,
                    budget_seconds=(
                        int(budget["total_seconds"]) if budget is not None else None
                    ),
                    budget_policy_version=(
                        str(budget["policy_version"]) if budget is not None else None
                    ),
                    deadline_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            for binding in persona_bindings:
                self._upsert_persona_candidates(
                    connection,
                    persona_id=binding["persona_id"],
                    job_id=job_id,
                    candidates=extract_supplied_profile_claims(
                        self._persona_input_specification(
                            specification, binding["persona_id"]
                        ),
                        usernames=binding["usernames"],
                    ),
                    now=now,
                )
            from maigret.web.pipeline_enqueue import enqueue_primary_requests
            enqueue_primary_requests(self, connection, job_id=job_id, case_id=case_id,
                                     options=stored_options, bindings=persona_bindings)
        self.append_event(job_id, {"type": "queued", "usernames": normalized})
        return job_id

    @staticmethod
    def _normalize_combined_source_case_ids(
        source_case_ids: Iterable[str],
    ) -> list[str]:
        normalized: list[str] = []
        seen = set()
        for value in source_case_ids:
            case_id = str(value or "").strip()
            if not case_id or case_id in seen:
                continue
            if len(case_id) > 36:
                raise ValueError("Invalid source case identifier")
            seen.add(case_id)
            normalized.append(case_id)
        if len(normalized) < 2:
            raise ValueError("Select at least two source cases")
        if len(normalized) > MAX_COMBINED_SOURCE_CASES:
            raise ValueError(
                f"Select no more than {MAX_COMBINED_SOURCE_CASES} source cases"
            )
        return normalized

    @staticmethod
    def _case_fusion_job_values(
        *,
        job_id: str,
        case_id: str,
        source_case_ids: list[str],
        purpose: str,
        requested_by: str,
        now: datetime,
    ) -> Dict[str, Any]:
        return {
            "id": job_id,
            "case_id": case_id,
            "kind": "case_fusion",
            "status": "queued",
            "usernames": [],
            "options": {
                "investigation_spec": {
                    "schema_version": 1,
                    "investigation_type": "case_fusion",
                    "source_case_ids": source_case_ids,
                    "purpose": purpose,
                    "requested_by": requested_by,
                    "evidence_scope": "approved_only",
                }
            },
            "progress": {
                "checked": 0,
                "total": len(source_case_ids),
                "found": 0,
            },
            "result": None,
            "error": None,
            "cancel_requested": False,
            "attempts": 0,
            "created_at": now,
            "updated_at": now,
        }

    def create_combined_investigation(
        self,
        source_case_ids: Iterable[str],
        *,
        title: str,
        purpose: str,
        created_by: str,
    ) -> str:
        """Create a durable combined case without copying source evidence."""
        normalized_ids = self._normalize_combined_source_case_ids(source_case_ids)
        normalized_title = bounded_text(title, "combined case title", max_chars=500)
        normalized_purpose = bounded_text(
            purpose, "investigation purpose", max_chars=2000
        )
        actor = bounded_text(created_by, "creator", max_chars=200)
        now = utcnow()
        case_id, job_id = str(uuid.uuid4()), str(uuid.uuid4())
        with self.engine.begin() as connection:
            source_rows = list(
                connection.execute(
                    select(cases.c.id, cases.c.case_type).where(
                        cases.c.id.in_(normalized_ids)
                    )
                ).mappings()
            )
            found = {str(row["id"]): row for row in source_rows}
            missing = [case_id for case_id in normalized_ids if case_id not in found]
            if missing:
                raise KeyError(missing[0])
            if any(
                found[case_id]["case_type"] != "standalone"
                for case_id in normalized_ids
            ):
                raise ValueError(
                    "Combined investigations cannot be nested inside another combined investigation"
                )
            connection.execute(
                insert(cases).values(
                    id=case_id,
                    title=normalized_title,
                    status="open",
                    case_type="combined",
                    purpose=normalized_purpose,
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(combined_case_members),
                [
                    {
                        "combined_case_id": case_id,
                        "source_case_id": source_case_id,
                        "position": position,
                        "added_by": actor,
                        "added_at": now,
                    }
                    for position, source_case_id in enumerate(normalized_ids)
                ],
            )
            connection.execute(
                insert(investigation_jobs).values(
                    **self._case_fusion_job_values(
                        job_id=job_id,
                        case_id=case_id,
                        source_case_ids=normalized_ids,
                        purpose=normalized_purpose,
                        requested_by=actor,
                        now=now,
                    )
                )
            )
        self.append_event(
            job_id,
            {"type": "queued", "source_case_count": len(normalized_ids)},
        )
        return job_id

    def queue_combined_investigation_refresh(
        self, case_id: str, *, requested_by: str
    ) -> str:
        """Queue a new immutable snapshot for an existing combined case."""
        actor = bounded_text(requested_by, "requester", max_chars=200)
        now = utcnow()
        job_id = str(uuid.uuid4())
        with self.engine.begin() as connection:
            case_statement = select(
                cases.c.id, cases.c.case_type, cases.c.purpose
            ).where(cases.c.id == case_id)
            if self.engine.dialect.name == "postgresql":
                case_statement = case_statement.with_for_update()
            case_row = connection.execute(case_statement).mappings().first()
            if not case_row:
                raise KeyError(case_id)
            if case_row["case_type"] != "combined":
                raise ValueError("Only combined investigations can be refreshed")
            if connection.scalar(
                select(investigation_jobs.c.id)
                .where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.status.in_(ACTIVE_STATUSES),
                )
                .limit(1)
            ):
                raise ActiveInvestigationError(
                    "Wait for the active combined investigation before refreshing"
                )
            source_case_ids = list(
                connection.scalars(
                    select(combined_case_members.c.source_case_id)
                    .where(combined_case_members.c.combined_case_id == case_id)
                    .order_by(combined_case_members.c.position)
                )
            )
            source_case_ids = self._normalize_combined_source_case_ids(source_case_ids)
            connection.execute(
                insert(investigation_jobs).values(
                    **self._case_fusion_job_values(
                        job_id=job_id,
                        case_id=case_id,
                        source_case_ids=source_case_ids,
                        purpose=str(case_row["purpose"] or ""),
                        requested_by=actor,
                        now=now,
                    )
                )
            )
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(updated_at=now)
            )
        self.append_event(
            job_id,
            {
                "type": "queued",
                "source_case_count": len(source_case_ids),
                "refresh": True,
            },
        )
        return job_id

    def create_affiliation_investigation(
        self,
        affiliation_name: str,
        *,
        source_claim_id: Optional[str] = None,
        source_claim_field: Optional[str] = None,
        target_basis: Optional[str] = None,
        jurisdiction: Any = None,
        enable_domain_context: bool = False,
        enable_public_web_research: bool = False,
        enable_google_places_search: bool = False,
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
        source_claim_field = str(source_claim_field or "").strip() or None
        if source_claim_field not in {None, "company", "occupation"}:
            raise ValueError("Invalid source claim field")
        target_basis = str(target_basis or "").strip() or None
        if target_basis not in {
            None,
            "approved_affiliation_claim",
            "analyst_confirmed_role_organization",
        }:
            raise ValueError("Invalid organization target basis")
        now = utcnow()
        case_id, job_id = str(uuid.uuid4()), str(uuid.uuid4())
        specification = {
            "schema_version": 2,
            "investigation_type": "affiliation",
            "affiliation_name": affiliation_name,
            "source_claim_id": source_claim_id,
            "source_claim_field": source_claim_field,
            "target_basis": target_basis,
            "legal_jurisdiction": legal_jurisdiction,
            "enable_domain_context": enable_domain_context,
            "enable_public_web_research": bool(enable_public_web_research),
            "enable_google_places_search": bool(enable_google_places_search),
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
                        )
                        + (2 if enable_domain_context else 0)
                        + (1 if enable_public_web_research else 0)
                        + (1 if enable_google_places_search else 0),
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
                "source_claim_id": source_claim_id,
                "source_claim_field": source_claim_field,
                "target_basis": target_basis,
                "legal_jurisdiction": legal_jurisdiction,
                "domain_context_requested": enable_domain_context,
                "public_web_research_requested": bool(
                    enable_public_web_research
                ),
                "google_places_search_requested": bool(
                    enable_google_places_search
                ),
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
            if connection.scalar(
                select(investigation_jobs.c.id)
                .where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.kind == "affiliation",
                    investigation_jobs.c.status.in_(ACTIVE_STATUSES),
                )
                .limit(1)
            ):
                raise ValueError(
                    "Wait for the current affiliation investigation before "
                    "confirming an organization"
                )
            selected_row = (
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
                    .limit(1)
                )
                .mappings()
                .first()
            )
            selected_candidate = None
            if selected_row:
                result = dict(selected_row["result"] or {})
                for candidate in list(
                    result.get("organization_resolution_candidates") or []
                )[:15]:
                    if (
                        isinstance(candidate, dict)
                        and candidate.get("candidate_key") == candidate_key
                    ):
                        selected_candidate = candidate
                        break
            if not selected_candidate or not selected_row:
                raise ValueError(
                    "The selected organization is not a candidate from the current "
                    "completed affiliation investigation"
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

    def queue_affiliation_entity(
        self,
        case_id: str,
        entity_id: str,
        *,
        enable_public_web_research: bool = False,
        enable_google_places_search: bool = False,
    ) -> str:
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
            source_claim_field = None
            target_basis = None
            legal_jurisdiction = None
            enable_domain_context = False
            enable_public_web_research = bool(enable_public_web_research)
            enable_google_places_search = bool(enable_google_places_search)
            official_website = None
            selected_organization = None
            for prior in prior_jobs:
                spec = dict(prior["options"] or {}).get("investigation_spec") or {}
                affiliation_name = affiliation_name or str(spec.get("affiliation_name") or "")
                source_claim_id = source_claim_id or spec.get("source_claim_id")
                source_claim_field = source_claim_field or spec.get(
                    "source_claim_field"
                )
                target_basis = target_basis or spec.get("target_basis")
                legal_jurisdiction = legal_jurisdiction or spec.get(
                    "legal_jurisdiction"
                )
                enable_domain_context = enable_domain_context or bool(
                    spec.get("enable_domain_context")
                )
                enable_public_web_research = enable_public_web_research or bool(
                    spec.get("enable_public_web_research")
                )
                enable_google_places_search = enable_google_places_search or bool(
                    spec.get("enable_google_places_search")
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
                "source_claim_field": source_claim_field,
                "target_basis": target_basis,
                "legal_jurisdiction": legal_jurisdiction,
                "enable_domain_context": enable_domain_context,
                "enable_public_web_research": enable_public_web_research,
                "enable_google_places_search": enable_google_places_search,
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
            total_sources += 1 if enable_public_web_research else 0
            total_sources += 1 if enable_google_places_search else 0
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
        self,
        case_id: str,
        *,
        official_website: Any = None,
        enable_public_web_research: bool = False,
        enable_google_places_search: bool = False,
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
                "enable_public_web_research": bool(
                    enable_public_web_research
                    or prior_spec.get("enable_public_web_research")
                ),
                "enable_google_places_search": bool(
                    enable_google_places_search
                    or prior_spec.get("enable_google_places_search")
                ),
                "official_website": (
                    normalized_website or prior_spec.get("official_website")
                ),
            }
            total_sources = (
                4
                if isinstance(legal_jurisdiction, dict)
                and legal_jurisdiction.get("code") == "FR"
                else 3 if legal_jurisdiction else 2
            ) + 2 + (
                1 if specification["enable_public_web_research"] else 0
            ) + (1 if specification["enable_google_places_search"] else 0)
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

    def repeat_persona_investigation(
        self,
        persona_id: str,
        usernames: Optional[Iterable[str]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> str:
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
            previous_jobs = connection.execute(
                select(investigation_jobs.c.options, investigation_jobs.c.usernames)
                .where(investigation_jobs.c.case_id == persona_row["case_id"])
                .order_by(investigation_jobs.c.created_at.desc())
            ).mappings()
            latest_job = None
            for previous_job in previous_jobs:
                previous_spec = (
                    dict(previous_job["options"] or {}).get("investigation_spec") or {}
                )
                previous_target = previous_spec.get("target_persona_id")
                if previous_target and previous_target != persona_id:
                    continue
                if "persona_bindings" in previous_spec and not any(
                    binding.get("persona_id") == persona_id
                    for binding in previous_spec.get("persona_bindings") or []
                ):
                    continue
                latest_job = previous_job
                break
            explicit_plan = usernames is not None or options is not None
            if explicit_plan and (usernames is None or options is None):
                raise ValueError(
                    "Persona reruns require both search targets and options"
                )
            latest_options: Dict[str, Any] = (
                dict(latest_job["options"] or {}) if latest_job else {}
            )
            latest_usernames = list(latest_job["usernames"] or []) if latest_job else []
            queued_options = dict(options or {}) if explicit_plan else latest_options
            investigation_spec = queued_options.get("investigation_spec")
            grouped = (
                isinstance(investigation_spec, dict)
                and investigation_spec.get("processing_mode") == "same_subject"
            )
            display_name = str(persona_row["display_name"]).strip()
            binding = next(
                (
                    item
                    for item in (investigation_spec or {}).get("persona_bindings", [])
                    if item.get("persona_id") == persona_id
                ),
                None,
            )
            queued_usernames = (
                [str(value).strip() for value in list(usernames or [])]
                if explicit_plan
                else (
                    list(binding["usernames"])
                    if binding is not None
                    else (
                        [str(value).strip() for value in latest_usernames]
                        if grouped
                        else [display_name]
                    )
                )
            )
            queued_usernames = [value for value in queued_usernames if value]
            specification = (
                dict(investigation_spec) if isinstance(investigation_spec, dict) else {}
            )
            if not queued_usernames and not specification.get("identifiers"):
                raise ValueError("No investigation identifiers are available")
            specification["pipeline_id"] = "p2-e2e-v1"
            if not explicit_plan and not grouped:
                target_keys = {value.casefold() for value in queued_usernames}
                source_label = str(
                    (binding or {}).get("subject_label") or display_name
                ).casefold()
                specification["search_targets"] = [
                    target
                    for target in specification.get("search_targets", [])
                    if str(target.get("value") or "").casefold() in target_keys
                ]
                profile_urls = {
                    target.get("source_value")
                    for target in specification["search_targets"]
                    if target.get("source_type") == "profile_url"
                }
                specification["identifiers"] = [
                    identifier
                    for identifier in specification.get("identifiers", [])
                    if (
                        identifier.get("type") in {"username", "social_handle"}
                        and str(identifier.get("value") or "").casefold() in target_keys
                    )
                    or (
                        identifier.get("type") == "full_name"
                        and str(identifier.get("value") or "").casefold()
                        == source_label
                    )
                    or (
                        identifier.get("type") == "profile_url"
                        and identifier.get("value") in profile_urls
                    )
                ]
                if binding is not None and "identifiers" in binding:
                    specification["identifiers"] = binding["identifiers"]
                    if len(binding["identifiers"]) == 1:
                        origin = binding["identifiers"][0]
                        source_type = (
                            "ranked_alias"
                            if origin.get("type") == "full_name"
                            else origin.get("type")
                        )
                        specification["search_targets"] = [
                            dict(
                                target,
                                source_type=source_type,
                                source_value=origin["value"],
                            )
                            for target in specification["search_targets"]
                        ]
            specification.update(
                processing_mode="same_subject",
                subject_label=display_name,
                target_persona_id=persona_id,
                subject_groups=[{"label": display_name, "usernames": queued_usernames}],
                persona_bindings=[
                    {
                        "persona_id": persona_id,
                        "subject_label": display_name,
                        "usernames": queued_usernames,
                        "identifiers": specification.get("identifiers", []),
                    }
                ],
            )
            queued_options["investigation_spec"] = specification
            queued_options = govern_profile_discovery_options(queued_options)
            budget = execution_budget_spec_from_options(queued_options)
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=persona_row["case_id"],
                    kind="refresh",
                    status="queued",
                    usernames=queued_usernames,
                    options=queued_options,
                    progress={"checked": 0, "total": None, "found": 0},
                    result=None,
                    error=None,
                    cancel_requested=False,
                    attempts=0,
                    budget_seconds=int(budget["total_seconds"]),
                    budget_policy_version=str(budget["policy_version"]),
                    deadline_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                update(cases)
                .where(cases.c.id == persona_row["case_id"])
                .values(updated_at=now)
            )
            from maigret.web.pipeline_enqueue import enqueue_primary_requests
            enqueue_primary_requests(self, connection, job_id=job_id,
                                     case_id=persona_row["case_id"], options=queued_options,
                                     bindings=specification['persona_bindings'])
        self.append_event(
            job_id,
            {
                "type": "queued",
                "usernames": queued_usernames,
                "reason": "persona_refresh",
                "target_persona_id": persona_id,
            },
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
        return self.claim_next_matching(worker_id)

    def claim_next_matching(
        self,
        worker_id: str,
        *,
        include_kinds: Optional[Iterable[str]] = None,
        exclude_kinds: Optional[Iterable[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Claim the oldest queued job accepted by one worker execution lane."""
        included = {
            str(kind).strip()[:32]
            for kind in (include_kinds or [])
            if str(kind).strip()
        }
        excluded = {
            str(kind).strip()[:32]
            for kind in (exclude_kinds or [])
            if str(kind).strip()
        }
        if included and excluded:
            raise ValueError("Choose either included or excluded job kinds")
        now = utcnow()
        with self.engine.begin() as connection:
            statement = (
                select(investigation_jobs)
                .where(investigation_jobs.c.status == "queued")
                .order_by(investigation_jobs.c.created_at)
                .limit(1)
            )
            if included:
                statement = statement.where(investigation_jobs.c.kind.in_(included))
            elif excluded:
                statement = statement.where(investigation_jobs.c.kind.not_in(excluded))
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update(skip_locked=True)
            row = connection.execute(statement).mappings().first()
            if not row:
                return None
            started_at = row["started_at"] or now
            budget_seconds = row["budget_seconds"]
            budget_policy_version = row["budget_policy_version"]
            deadline_at = row["deadline_at"]
            budget_mode = None
            claimed_options = dict(row["options"] or {})
            if str(row["kind"]) in PROFILE_DISCOVERY_JOB_KINDS:
                try:
                    claimed_options = govern_profile_discovery_options(
                        claimed_options
                    )
                except ProfileDiscoveryPolicyError as error:
                    public_error = str(error)[:1000]
                    connection.execute(
                        update(investigation_jobs)
                        .where(
                            investigation_jobs.c.id == row["id"],
                            investigation_jobs.c.status == "queued",
                        )
                        .values(
                            status="failed",
                            result={
                                "status": "failed",
                                "error": public_error,
                                "usernames": list(row["usernames"] or []),
                                "session_folder": f"search_{row['id']}",
                            },
                            error=public_error,
                            completed_at=now,
                            updated_at=now,
                        )
                    )
                    return None
                budget = execution_budget_spec_from_options(claimed_options)
                budget_mode = str(budget["mode"])
                budget_seconds = int(budget["total_seconds"])
                budget_policy_version = str(budget["policy_version"])
                deadline_at = started_at + timedelta(seconds=budget_seconds)
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
                    started_at=started_at,
                    heartbeat_at=now,
                    options=claimed_options,
                    budget_seconds=budget_seconds,
                    budget_policy_version=budget_policy_version,
                    deadline_at=deadline_at,
                    updated_at=now,
                )
            )
        running_event: Dict[str, Any] = {"type": "running"}
        if budget_seconds is not None:
            running_event["execution_budget"] = {
                "policy_version": budget_policy_version,
                "mode": budget_mode,
                "total_seconds": budget_seconds,
                "deadline_at": _as_iso(deadline_at),
            }
        self.append_event(row["id"], running_event)
        claimed = self.get_job(row["id"])
        if claimed is not None:
            # The worker lease token is returned only to the claiming process;
            # ordinary job reads and public API responses do not expose it.
            claimed["worker_id"] = worker_id
        return claimed

    def append_event(
        self,
        job_id: str,
        event: Dict[str, Any],
        *,
        runtime_guard: bool = False,
        worker_id: Optional[str] = None,
    ) -> int:
        now = utcnow()
        progress_updates: Dict[str, Any] = {}
        with self.engine.begin() as connection:
            statement = select(
                investigation_jobs.c.status,
                investigation_jobs.c.progress,
                investigation_jobs.c.worker_id,
                investigation_jobs.c.heartbeat_at,
            ).where(investigation_jobs.c.id == job_id)
            if runtime_guard and self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = connection.execute(statement).mappings().first()
            if row is None:
                raise KeyError(job_id)
            event_type = event.get("type")
            if runtime_guard:
                status = str(row["status"])
                if worker_id is not None and row["worker_id"] != worker_id:
                    return 0
                if worker_id is not None and _heartbeat_expired(
                    row["heartbeat_at"], now=now
                ):
                    return 0
                if status in TERMINAL_STATUSES and event_type != "done":
                    return 0
                if status in TERMINAL_STATUSES and event_type == "done":
                    latest_event = connection.execute(
                        select(investigation_events.c.event)
                        .where(investigation_events.c.job_id == job_id)
                        .order_by(investigation_events.c.id.desc())
                        .limit(1)
                    ).scalar_one_or_none()
                    if (
                        isinstance(latest_event, dict)
                        and latest_event.get("type") == "done"
                    ):
                        return 0
                if status == "cancel_requested" and event_type not in {
                    "stopped",
                    "done",
                }:
                    return 0
            progress = dict(row["progress"] or {})
            if event_type == "start":
                progress["total"] = event.get("total")
                progress["username"] = event.get("username")
            elif event_type == "progress":
                progress["checked"] = event.get("checked", progress.get("checked", 0))
                progress["total"] = event.get("total", progress.get("total"))
                progress["site"] = event.get("site")
                if event.get("activity"):
                    progress["activity"] = event.get("activity")
            elif event_type in {"phase", "heartbeat"}:
                if event.get("phase"):
                    progress["phase"] = str(event["phase"])[:64]
                if event.get("message"):
                    progress["message"] = str(event["message"])[:500]
                try:
                    progress["elapsed_seconds"] = max(
                        0, int(event.get("elapsed_seconds", 0))
                    )
                except (TypeError, ValueError):
                    pass
            elif event_type == "found":
                progress["found"] = int(progress.get("found", 0)) + 1
            progress_updates["progress"] = progress
            # Queueing and other control-plane events are not worker activity.
            # Only an owner-guarded runtime event may renew the worker lease.
            if runtime_guard:
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

    def publish_case_fusion_snapshot(
        self,
        job_id: str,
        snapshot_result: Dict[str, Any],
        analysis_context: Dict[str, Any],
        *,
        worker_id: Optional[str] = None,
    ) -> Optional[str]:
        """Commit a snapshot and its follow-on AI job in one transaction.

        Returning ``None`` means a concurrent stop request won before publication.
        The source snapshot is otherwise terminal and independently usable before
        the queued AI phase starts.
        """
        snapshot_payload = dict(snapshot_result or {})
        snapshot_payload.pop("analysis_context", None)
        snapshot = snapshot_payload.get("snapshot")
        if not isinstance(snapshot, dict):
            raise ValueError("A combined snapshot is required")
        snapshot_sha = str(snapshot.get("sha256") or "").strip().casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", snapshot_sha):
            raise ValueError("A valid combined snapshot SHA-256 is required")
        context_payload = dict(analysis_context or {})
        if not hmac.compare_digest(
            str(context_payload.get("snapshot_sha256") or "").casefold(),
            snapshot_sha,
        ):
            raise ValueError("AI context does not match the combined snapshot")

        now = utcnow()
        ai_job_id = str(uuid.uuid4())
        with self.engine.begin() as connection:
            statement = select(
                investigation_jobs.c.case_id,
                investigation_jobs.c.kind,
                investigation_jobs.c.status,
                investigation_jobs.c.cancel_requested,
                investigation_jobs.c.progress,
                investigation_jobs.c.worker_id,
                investigation_jobs.c.heartbeat_at,
            ).where(investigation_jobs.c.id == job_id)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = connection.execute(statement).mappings().first()
            if not row:
                raise KeyError(job_id)
            if row["kind"] != "case_fusion":
                raise ValueError("This job is not a combined-case snapshot")
            if worker_id is not None and row["worker_id"] != worker_id:
                return None
            if worker_id is not None and _heartbeat_expired(
                row["heartbeat_at"], now=now
            ):
                return None
            if row["status"] == "cancel_requested" or row["cancel_requested"]:
                return None
            if row["status"] != "running":
                raise ValueError("The combined-case snapshot is not running")

            ai_options = {
                "investigation_spec": {
                    "schema_version": 1,
                    "investigation_type": "case_fusion_ai",
                    "snapshot_job_id": job_id,
                    "snapshot_sha256": snapshot_sha,
                    "analysis_context": context_payload,
                }
            }
            connection.execute(
                insert(investigation_jobs).values(
                    id=ai_job_id,
                    case_id=str(row["case_id"]),
                    kind="case_fusion_ai",
                    status="queued",
                    usernames=[],
                    options=ai_options,
                    progress={
                        "phase": "queued",
                        "message": "AI synthesis is queued.",
                        "elapsed_seconds": 0,
                    },
                    result=None,
                    error=None,
                    cancel_requested=False,
                    attempts=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            result_payload = {
                "status": "completed",
                "kind": "case_fusion",
                **snapshot_payload,
                "ai_job_id": ai_job_id,
            }
            connection.execute(
                update(investigation_jobs)
                .where(
                    investigation_jobs.c.id == job_id,
                    investigation_jobs.c.status == "running",
                    *(
                        (investigation_jobs.c.worker_id == worker_id,)
                        if worker_id is not None
                        else ()
                    ),
                )
                .values(
                    status="completed",
                    result=result_payload,
                    error=None,
                    completed_at=now,
                    heartbeat_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(investigation_events),
                [
                    {
                        "job_id": job_id,
                        "event": {
                            "type": "snapshot_published",
                            "status": "completed",
                            "redirect": f"/cases/{row['case_id']}",
                            "ai_job_id": ai_job_id,
                        },
                        "created_at": now,
                    },
                    {
                        "job_id": job_id,
                        "event": {
                            "type": "done",
                            "status": "completed",
                            "redirect": f"/cases/{row['case_id']}",
                        },
                        "created_at": now,
                    },
                    {
                        "job_id": ai_job_id,
                        "event": {
                            "type": "queued",
                            "phase": "queued",
                            "message": "AI synthesis is queued.",
                            "elapsed_seconds": 0,
                            "snapshot_job_id": job_id,
                        },
                        "created_at": now,
                    },
                ],
            )
        return ai_job_id

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

    def record_profile_search_result(
        self,
        job_id: str,
        result: Any,
        *,
        worker_id: Optional[str] = None,
    ) -> Optional[str]:
        """Append an idempotent audit snapshot without publishing a claim."""
        document, document_sha256 = _profile_search_audit_document(result)
        now = utcnow()
        audit_id = str(uuid.uuid4())
        with self.engine.begin() as connection:
            statement = select(
                investigation_jobs.c.kind,
                investigation_jobs.c.status,
                investigation_jobs.c.worker_id,
                investigation_jobs.c.heartbeat_at,
            ).where(investigation_jobs.c.id == job_id)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            job = connection.execute(statement).mappings().first()
            if job is None:
                raise KeyError(job_id)
            if job["kind"] not in PROFILE_DISCOVERY_JOB_KINDS:
                raise ValueError(
                    "Profile-search audits require a profile-discovery job"
                )
            if job["status"] not in ACTIVE_STATUSES:
                return None
            if (job["worker_id"] is not None or worker_id is not None) and (
                job["worker_id"] != worker_id
                or _heartbeat_expired(job["heartbeat_at"], now=now)
            ):
                return None
            if job["status"] == "cancel_requested" and not result.stopped:
                return None
            existing = connection.scalar(
                select(profile_search_audits.c.id).where(
                    profile_search_audits.c.job_id == job_id,
                    profile_search_audits.c.document_sha256
                    == document_sha256,
                )
            )
            if existing is not None:
                return str(existing)
            audit_count = int(
                connection.scalar(
                    select(func.count())
                    .select_from(profile_search_audits)
                    .where(profile_search_audits.c.job_id == job_id)
                )
                or 0
            )
            if audit_count >= MAX_PROFILE_SEARCH_AUDITS_PER_JOB:
                raise ValueError(
                    "Profile-search audit history reached its storage limit"
                )
            connection.execute(
                insert(profile_search_audits).values(
                    id=audit_id,
                    job_id=job_id,
                    status=result.status,
                    stopped=result.stopped,
                    orchestration_version=result.orchestration_version,
                    planned_query_count=result.planned_query_count,
                    executed_query_count=result.executed_query_count,
                    error_count=result.error_count,
                    candidate_count=len(result.candidates),
                    document_sha256=document_sha256,
                    document=document,
                    created_at=now,
                )
            )
            connection.execute(
                insert(investigation_events).values(
                    job_id=job_id,
                    event={
                        "type": "profile_search_audit",
                        "audit_id": audit_id,
                        "status": result.status,
                        "planned_query_count": result.planned_query_count,
                        "executed_query_count": result.executed_query_count,
                        "error_count": result.error_count,
                        "candidate_count": len(result.candidates),
                        "document_sha256": document_sha256,
                    },
                    created_at=now,
                )
            )
            updates = {"updated_at": now}
            if worker_id is not None:
                updates["heartbeat_at"] = now
            connection.execute(
                update(investigation_jobs)
                .where(investigation_jobs.c.id == job_id)
                .values(**updates)
            )
        return audit_id

    def list_profile_search_audits(
        self, job_id: str, *, limit: int = MAX_PROFILE_SEARCH_AUDITS_PER_JOB
    ) -> list[Dict[str, Any]]:
        """Return newest-first immutable search snapshots for one job."""
        bounded_limit = min(
            max(1, int(limit)), MAX_PROFILE_SEARCH_AUDITS_PER_JOB
        )
        with self.engine.connect() as connection:
            rows = connection.execute(
                select(profile_search_audits)
                .where(profile_search_audits.c.job_id == job_id)
                .order_by(
                    profile_search_audits.c.created_at.desc(),
                    profile_search_audits.c.id.desc(),
                )
                .limit(bounded_limit)
            ).mappings()
            return [_serialize_profile_search_audit(row) for row in rows]

    def get_profile_search_audit(
        self, job_id: str, audit_id: str
    ) -> Optional[Dict[str, Any]]:
        """Read one job-scoped immutable search snapshot."""
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(profile_search_audits).where(
                        profile_search_audits.c.job_id == job_id,
                        profile_search_audits.c.id == audit_id,
                    )
                )
                .mappings()
                .first()
            )
        return _serialize_profile_search_audit(row) if row else None

    def get_case_profile_search_discovery(
        self, case_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return the latest bounded candidate set with current review overlays."""
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(profile_search_audits)
                    .join(
                        investigation_jobs,
                        investigation_jobs.c.id
                        == profile_search_audits.c.job_id,
                    )
                    .where(investigation_jobs.c.case_id == case_id)
                    .order_by(
                        profile_search_audits.c.created_at.desc(),
                        profile_search_audits.c.id.desc(),
                    )
                    .limit(1)
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            audit = _serialize_profile_search_audit(row)
            if not hmac.compare_digest(
                _profile_search_document_sha256(audit["document"]),
                audit["document_sha256"],
            ):
                raise ValueError("Profile-search audit integrity check failed")
            raw_candidates = list(
                audit["document"].get("candidates") or []
            )
            candidates = [
                dict(candidate)
                for candidate in raw_candidates[:MAX_PROFILE_SEARCH_UI_CANDIDATES]
                if isinstance(candidate, dict)
            ]
            candidate_ids = [
                str(candidate.get("candidate_id") or "")
                for candidate in candidates
                if str(candidate.get("candidate_id") or "")
            ]
            review_rows = (
                list(
                    connection.execute(
                        select(
                            profile_search_candidate_reviews,
                            personas.c.display_name.label("persona_name"),
                            persona_claims.c.review_status.label(
                                "claim_review_status"
                            ),
                        )
                        .select_from(profile_search_candidate_reviews)
                        .join(
                            profile_search_audits,
                            profile_search_audits.c.id
                            == profile_search_candidate_reviews.c.audit_id,
                        )
                        .join(
                            investigation_jobs,
                            investigation_jobs.c.id
                            == profile_search_audits.c.job_id,
                        )
                        .join(
                            personas,
                            personas.c.id
                            == profile_search_candidate_reviews.c.persona_id,
                        )
                        .outerjoin(
                            persona_claims,
                            persona_claims.c.id
                            == profile_search_candidate_reviews.c.claim_id,
                        )
                        .where(
                            investigation_jobs.c.case_id == case_id,
                            profile_search_candidate_reviews.c.candidate_id.in_(
                                candidate_ids
                            ),
                        )
                        .order_by(
                            profile_search_candidate_reviews.c.created_at.desc(),
                            profile_search_candidate_reviews.c.id.desc(),
                        )
                        .limit(MAX_PROFILE_SEARCH_UI_REVIEWS)
                    ).mappings()
                )
                if candidate_ids
                else []
            )

        latest_reviews: Dict[tuple[str, str], Dict[str, Any]] = {}
        for review in review_rows:
            key = (str(review["candidate_id"]), str(review["persona_id"]))
            if key in latest_reviews:
                continue
            latest_reviews[key] = {
                "id": int(review["id"]),
                "audit_id": str(review["audit_id"]),
                "candidate_id": str(review["candidate_id"]),
                "persona_id": str(review["persona_id"]),
                "persona_name": str(review["persona_name"]),
                "claim_id": (
                    str(review["claim_id"]) if review["claim_id"] else None
                ),
                "claim_review_status": (
                    str(review["claim_review_status"])
                    if review["claim_review_status"]
                    else None
                ),
                "decision": str(review["decision"]),
                "reviewer": str(review["reviewer"]),
                "note": str(review["note"] or ""),
                "created_at": _as_iso(review["created_at"]),
            }
        for candidate in candidates:
            candidate_id = str(candidate.get("candidate_id") or "")
            candidate["reviews"] = [
                review
                for (review_candidate_id, _persona_id), review
                in latest_reviews.items()
                if review_candidate_id == candidate_id
            ]
            candidate["anchor_id"] = (
                "profile-candidate-" + candidate_id.rsplit(":", 1)[-1]
            )
        return {
            "audit_id": audit["id"],
            "job_id": audit["job_id"],
            "status": audit["status"],
            "created_at": audit["created_at"],
            "document_sha256": audit["document_sha256"],
            "candidate_count": audit["candidate_count"],
            "displayed_candidate_count": len(candidates),
            "truncated_candidate_count": max(
                0, audit["candidate_count"] - len(candidates)
            ),
            "planned_query_count": audit["planned_query_count"],
            "executed_query_count": audit["executed_query_count"],
            "error_count": audit["error_count"],
            "candidates": candidates,
        }

    def review_profile_search_candidate(
        self,
        case_id: str,
        audit_id: str,
        candidate_id: str,
        persona_id: str,
        decision: str,
        reviewer: str,
        note: str = "",
    ) -> Dict[str, Any]:
        """Record a candidate decision; proposals enter Persona review pending."""
        candidate_id = str(candidate_id or "").strip().casefold()
        if not PROFILE_SEARCH_CANDIDATE_ID_PATTERN.fullmatch(candidate_id):
            raise ValueError("Invalid profile-search candidate identifier")
        decision = str(decision or "").strip().casefold()
        if decision not in {"proposed", "rejected", "uncertain"}:
            raise ValueError("Choose propose, reject, or uncertain")
        reviewer = " ".join(str(reviewer or "").split())[:200]
        if not reviewer:
            raise ValueError("A reviewer is required")
        note = str(note or "").strip()[:2000] or None
        now = utcnow()
        with self.engine.begin() as connection:
            statement = (
                select(
                    profile_search_audits.c.id,
                    profile_search_audits.c.job_id,
                    profile_search_audits.c.document,
                    profile_search_audits.c.document_sha256,
                )
                .join(
                    investigation_jobs,
                    investigation_jobs.c.id == profile_search_audits.c.job_id,
                )
                .where(
                    profile_search_audits.c.id == audit_id,
                    investigation_jobs.c.case_id == case_id,
                )
            )
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            audit = connection.execute(statement).mappings().first()
            if audit is None:
                raise KeyError(audit_id)
            persona = (
                connection.execute(
                    select(personas.c.id, personas.c.display_name).where(
                        personas.c.id == persona_id,
                        personas.c.case_id == case_id,
                    )
                )
                .mappings()
                .first()
            )
            if persona is None:
                raise ValueError("The selected Persona does not belong to this case")
            document = dict(audit["document"] or {})
            if not hmac.compare_digest(
                _profile_search_document_sha256(document),
                str(audit["document_sha256"]),
            ):
                raise ValueError("Profile-search audit integrity check failed")
            candidate = _profile_search_candidate_from_document(
                document, candidate_id
            )
            claim_id = None
            claim_review_status = None
            if decision == "proposed":
                from maigret.web.persona_intelligence import (
                    claim_fingerprint,
                    evidence_fingerprint,
                )

                platform = str(candidate["platform"])
                profile_url = str(candidate["profile_url"])
                value = {
                    "platform": platform,
                    "url": profile_url,
                    "username": str(candidate["handle"]),
                }
                fingerprint = claim_fingerprint("social_account", value)
                evidence = {
                    "evidence_type": "native_profile_search_candidate",
                    "source_name": (
                        f"Native profile search · {platform.title()}"
                    )[:300],
                    "source_url": profile_url,
                    "details": {
                        "audit_id": str(audit["id"]),
                        "audit_sha256": str(audit["document_sha256"]),
                        "candidate_id": candidate_id,
                        "score_scope": "discovery_review_priority",
                        "discovery_score": int(
                            candidate.get("discovery_score") or 0
                        ),
                        "review_priority": str(
                            candidate.get("review_priority") or "low"
                        ),
                        "candidate_identity_unverified": True,
                        "human_review_required": True,
                        "proposed_by": reviewer,
                    },
                }
                evidence["fingerprint"] = evidence_fingerprint(evidence)
                self._upsert_persona_candidates(
                    connection,
                    persona_id=str(persona["id"]),
                    job_id=str(audit["job_id"]),
                    candidates=[
                        {
                            "field_name": "social_account",
                            "value": value,
                            "display_value": profile_url,
                            "normalized_value": json.dumps(
                                value,
                                sort_keys=True,
                                ensure_ascii=False,
                            )[:4000],
                            "confidence": (
                                PROFILE_SEARCH_PENDING_CLAIM_CONFIDENCE
                            ),
                            "fingerprint": fingerprint,
                            "source_engine": "native_profile_search_review",
                            "source_record_id": candidate_id,
                            "native_status": "candidate_proposed",
                            "evidence": [evidence],
                            "observation_details": {
                                "audit_id": str(audit["id"]),
                                "candidate_identity_unverified": True,
                                "human_review_required": True,
                            },
                        }
                    ],
                    now=now,
                )
                claim = _persona_candidate_claim_with_connection(
                    connection,
                    persona_id,
                    {
                        "field_name": "social_account",
                        "value": value,
                        "display_value": profile_url,
                        "fingerprint": fingerprint,
                        "source_engine": "native_profile_search_review",
                    },
                )
                if claim is None:
                    raise RuntimeError("Persona review proposal was not retained")
                claim_id = str(claim["id"])
                claim_review_status = str(claim["review_status"])
            review_id = connection.execute(
                insert(profile_search_candidate_reviews).values(
                    audit_id=str(audit["id"]),
                    candidate_id=candidate_id,
                    persona_id=str(persona["id"]),
                    claim_id=claim_id,
                    decision=decision,
                    reviewer=reviewer,
                    note=note,
                    created_at=now,
                )
            ).inserted_primary_key[0]
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(updated_at=now)
            )
        return {
            "id": int(review_id),
            "audit_id": str(audit_id),
            "candidate_id": candidate_id,
            "persona_id": str(persona_id),
            "persona_name": str(persona["display_name"]),
            "claim_id": claim_id,
            "claim_review_status": claim_review_status,
            "decision": decision,
            "reviewer": reviewer,
            "note": note or "",
            "created_at": _as_iso(now),
        }

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

    @staticmethod
    def _snapshot_source_versions(job_rows) -> Dict[str, str]:
        for job_row in job_rows:
            if (
                str(job_row["kind"]) != "case_fusion"
                or str(job_row["status"]) != "completed"
            ):
                continue
            result = dict(job_row["result"] or {})
            snapshot = result.get("snapshot")
            if not isinstance(snapshot, dict):
                return {}
            return {
                str(item.get("id")): str(item.get("updated_at") or "")
                for item in list(snapshot.get("source_cases") or [])
                if isinstance(item, dict) and item.get("id")
            }
        return {}

    def _combined_members_with_connection(
        self,
        connection: Connection,
        combined_case_id: str,
        *,
        snapshot_versions: Optional[Dict[str, str]] = None,
    ) -> list[Dict[str, Any]]:
        rows = list(
            connection.execute(
                select(
                    combined_case_members.c.source_case_id,
                    combined_case_members.c.position,
                    combined_case_members.c.added_by,
                    combined_case_members.c.added_at,
                    cases.c.title,
                    cases.c.status,
                    cases.c.case_type,
                    cases.c.updated_at,
                )
                .join(cases, cases.c.id == combined_case_members.c.source_case_id)
                .where(combined_case_members.c.combined_case_id == combined_case_id)
                .order_by(combined_case_members.c.position)
            ).mappings()
        )
        versions = snapshot_versions or {}
        members = []
        for row in rows:
            source_case_id = str(row["source_case_id"])
            current_updated_at = _as_iso(row["updated_at"])
            snapshot_updated_at = versions.get(source_case_id)
            persona_count = int(
                connection.scalar(
                    select(func.count())
                    .select_from(personas)
                    .where(personas.c.case_id == source_case_id)
                )
                or 0
            )
            latest_job_row = (
                connection.execute(
                    select(investigation_jobs)
                    .where(investigation_jobs.c.case_id == source_case_id)
                    .order_by(investigation_jobs.c.created_at.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            members.append(
                {
                    "id": source_case_id,
                    "title": str(row["title"]),
                    "status": str(row["status"]),
                    "case_type": str(row["case_type"]),
                    "updated_at": current_updated_at,
                    "snapshot_updated_at": snapshot_updated_at,
                    "changed_since_snapshot": bool(
                        snapshot_updated_at
                        and snapshot_updated_at != current_updated_at
                    ),
                    "persona_count": persona_count,
                    "latest_job": (
                        self._serialize_job(latest_job_row) if latest_job_row else None
                    ),
                    "position": int(row["position"]),
                    "added_by": str(row["added_by"]),
                    "added_at": _as_iso(row["added_at"]),
                }
            )
        return members

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
                        "case_type": case_row["case_type"],
                        "purpose": case_row["purpose"],
                        "created_at": _as_iso(case_row["created_at"]),
                        "updated_at": _as_iso(case_row["updated_at"]),
                        "personas": [dict(row) for row in persona_rows],
                        "latest_job": (
                            self._serialize_job(latest_job) if latest_job else None
                        ),
                        "source_case_count": (
                            int(
                                connection.scalar(
                                    select(func.count())
                                    .select_from(combined_case_members)
                                    .where(
                                        combined_case_members.c.combined_case_id
                                        == case_row["id"]
                                    )
                                )
                                or 0
                            )
                            if case_row["case_type"] == "combined"
                            else 0
                        ),
                        "member_persona_count": (
                            int(
                                connection.scalar(
                                    select(func.count())
                                    .select_from(personas)
                                    .join(
                                        combined_case_members,
                                        combined_case_members.c.source_case_id
                                        == personas.c.case_id,
                                    )
                                    .where(
                                        combined_case_members.c.combined_case_id
                                        == case_row["id"]
                                    )
                                )
                                or 0
                            )
                            if case_row["case_type"] == "combined"
                            else len(persona_rows)
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
            snapshot_versions = self._snapshot_source_versions(job_rows)
            members = (
                self._combined_members_with_connection(
                    connection,
                    str(case_row["id"]),
                    snapshot_versions=snapshot_versions,
                )
                if case_row["case_type"] == "combined"
                else []
            )
            analysis_runs = (
                self._combined_analysis_runs_with_connection(
                    connection, str(case_row["id"])
                )
                if case_row["case_type"] == "combined"
                else []
            )
        return {
            "id": case_row["id"],
            "title": case_row["title"],
            "status": case_row["status"],
            "case_type": case_row["case_type"],
            "purpose": case_row["purpose"],
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
            "source_cases": members,
            "analysis_runs": analysis_runs,
            "source_changed_count": sum(
                bool(item["changed_since_snapshot"]) for item in members
            ),
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
                        .where(
                            persona_claims.c.persona_id.in_(persona_ids),
                            ~persona_claims.c.source_engine.like(
                                f"{LEGACY_UNTRIAGED_SOURCE_PREFIX}%"
                            ),
                        )
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
        """Append idempotent provenance without overwriting audit history."""
        provenance_values = (job_id, external_evidence_id, chat_message_id)
        if sum(value is not None for value in provenance_values) != 1:
            raise ExternalEvidenceValidationError(
                "Provide exactly one provenance record"
            )
        with self.engine.begin() as connection:
            claim = (
                connection.execute(
                    select(
                        personas.c.case_id,
                        persona_claims.c.source_engine,
                    )
                    .select_from(
                        persona_claims.join(
                            personas,
                            personas.c.id == persona_claims.c.persona_id,
                        )
                    )
                    .where(persona_claims.c.id == claim_id)
                )
                .mappings()
                .first()
            )
            if not claim:
                raise KeyError(claim_id)
            claim_case_id = claim["case_id"]
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
            now = utcnow()
            observation_id = self._record_claim_observation_with_connection(
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
                now=now,
            )
            existing_legacy = _legacy_untriaged_source_details(
                claim["source_engine"]
            )
            normalized_engine = bounded_text(
                source_engine, "source_engine", max_chars=100
            )
            independent = (
                normalized_engine not in LEGACY_PROFILE_CLAIM_ENGINES
                and normalized_engine != RELIABILITY_MIGRATION_REVIEWER
                and not normalized_engine.startswith(
                    LEGACY_UNTRIAGED_SOURCE_PREFIX
                )
            )
            if existing_legacy and independent:
                values: Dict[str, Any] = {
                    "review_status": "pending",
                    "reviewed_at": None,
                    "reviewed_by": None,
                    "source_engine": normalized_engine,
                    "source_job_id": job_id,
                    "last_seen_at": now,
                    "updated_at": now,
                }
                if confidence is not None:
                    values["confidence"] = int(confidence)
                reactivated = connection.execute(
                    update(persona_claims)
                    .where(
                        persona_claims.c.id == claim_id,
                        persona_claims.c.source_engine == claim["source_engine"],
                    )
                    .values(**values)
                )
                if reactivated.rowcount == 1:
                    self._retire_claim_evidence_with_connection(
                        connection,
                        claim_id,
                        now=now,
                    )
                    connection.execute(
                        update(cases)
                        .where(cases.c.id == claim_case_id)
                        .values(updated_at=now)
                    )
            return observation_id

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

    def _get_persona_with_connection(
        self, connection: Connection, persona_id: str
    ) -> Optional[Dict[str, Any]]:
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
                    persona_claims.c.id,
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
                .order_by(
                    claim_evidence.c.observed_at.desc(),
                    claim_evidence.c.id,
                )
            ).mappings():
                evidence_by_claim.setdefault(evidence_row["claim_id"], []).append(
                    evidence_row
                )
            for review_row in connection.execute(
                select(claim_reviews)
                .where(claim_reviews.c.claim_id.in_(claim_ids))
                .order_by(
                    claim_reviews.c.created_at.desc(),
                    claim_reviews.c.id.desc(),
                )
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

    def get_persona(self, persona_id: str) -> Optional[Dict[str, Any]]:
        """Load a persona, its evidence-backed claims, and review history."""
        with self.engine.connect() as connection:
            return self._get_persona_with_connection(connection, persona_id)

    def list_approved_persona_social_accounts(
        self, persona_id: str, *, limit: int = 8
    ) -> list[Dict[str, Any]]:
        """Load a bounded set of approved social claims for discovery seeds."""
        bounded_limit = min(max(1, int(limit)), 50)
        with self.engine.connect() as connection:
            rows = list(
                connection.execute(
                    select(
                        persona_claims.c.field_name,
                        persona_claims.c.value,
                        persona_claims.c.review_status,
                    )
                    .where(
                        persona_claims.c.persona_id == persona_id,
                        persona_claims.c.field_name == "social_account",
                        persona_claims.c.review_status == "approved",
                    )
                    .order_by(
                        persona_claims.c.confidence.desc(),
                        persona_claims.c.created_at,
                        persona_claims.c.id,
                    )
                    .limit(bounded_limit)
                ).mappings()
            )
        return [dict(row) for row in rows]

    def get_persona_export_snapshot(
        self, persona_id: str
    ) -> tuple[Optional[Dict[str, Any]], datetime]:
        """Load an export and timestamp from one consistent database snapshot."""
        dialect = self.engine.dialect.name
        connection = self.engine.connect()
        if dialect == "postgresql":
            connection = connection.execution_options(isolation_level="REPEATABLE READ")
        elif dialect != "sqlite":
            connection = connection.execution_options(isolation_level="SERIALIZABLE")
        with connection:
            if dialect == "sqlite":
                # Reserve the writer briefly so the timestamp cannot precede a
                # decision that becomes visible partway through the read.
                connection.exec_driver_sql("BEGIN IMMEDIATE")
                generated_at = utcnow()
            else:
                connection.begin()
                if dialect == "postgresql":
                    connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                    generated_at = connection.scalar(
                        select(func.statement_timestamp())
                    )
                else:
                    generated_at = utcnow()
            try:
                persona = self._get_persona_with_connection(connection, persona_id)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        if not isinstance(generated_at, datetime):
            raise RuntimeError("Database did not provide an export snapshot timestamp")
        if generated_at.tzinfo is None:
            generated_at = generated_at.replace(tzinfo=timezone.utc)
        else:
            generated_at = generated_at.astimezone(timezone.utc)
        return persona, generated_at

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
    def _retire_claim_evidence_with_connection(
        connection: Connection,
        claim_id: str,
        *,
        now: datetime,
    ) -> int:
        """Keep legacy evidence as audit history without presenting it as support."""
        retired = 0
        rows = list(
            connection.execute(
                select(
                    claim_evidence.c.id,
                    claim_evidence.c.details,
                ).where(claim_evidence.c.claim_id == claim_id)
            ).mappings()
        )
        for row in rows:
            details = dict(row["details"] or {})
            if _is_retired_legacy_evidence(details):
                continue
            details[LEGACY_EVIDENCE_MARKER] = {
                "status": "legacy_untriaged",
                "retired_at": _as_iso(now),
                "reason": "claim_reactivated_from_new_provenance",
            }
            connection.execute(
                update(claim_evidence)
                .where(claim_evidence.c.id == row["id"])
                .values(details=details)
            )
            retired += 1
        return retired

    @staticmethod
    def _restore_claim_evidence_fingerprints_with_connection(
        connection: Connection,
        claim_id: str,
        fingerprints: Iterable[str],
    ) -> int:
        """Restore only evidence explicitly linked to the surviving observation."""
        normalized = {
            str(fingerprint)
            for fingerprint in fingerprints
            if fingerprint is not None and str(fingerprint).strip()
        }
        if not normalized:
            return 0
        rows = list(
            connection.execute(
                select(
                    claim_evidence.c.id,
                    claim_evidence.c.details,
                ).where(
                    claim_evidence.c.claim_id == claim_id,
                    claim_evidence.c.fingerprint.in_(normalized),
                )
            ).mappings()
        )
        restored = 0
        for row in rows:
            details = dict(row["details"] or {})
            if not _is_retired_legacy_evidence(details):
                continue
            details.pop(LEGACY_EVIDENCE_MARKER, None)
            connection.execute(
                update(claim_evidence)
                .where(claim_evidence.c.id == row["id"])
                .values(details=details)
            )
            restored += 1
        return restored

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
        allow_legacy_reactivation: bool = False,
    ) -> int:
        """Persist validated candidates without changing a human decision."""
        synchronized = 0
        for candidate in candidates:
            reactivate_legacy = False
            existing = _persona_candidate_claim_with_connection(
                connection,
                persona_id,
                candidate,
            )
            if existing:
                claim_id = existing["id"]
                legacy_source = _legacy_untriaged_source_details(
                    existing["source_engine"]
                )
                candidate_engine = str(candidate.get("source_engine") or "")
                reactivate_from_profile = bool(
                    legacy_source
                    and allow_legacy_reactivation
                    and candidate_engine in LEGACY_PROFILE_CLAIM_ENGINES
                )
                reactivate_from_independent = bool(
                    legacy_source
                    and candidate_engine not in LEGACY_PROFILE_CLAIM_ENGINES
                    and candidate_engine != RELIABILITY_MIGRATION_REVIEWER
                    and not candidate_engine.startswith(
                        LEGACY_UNTRIAGED_SOURCE_PREFIX
                    )
                )
                reactivate_legacy = (
                    reactivate_from_profile or reactivate_from_independent
                )
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
                if legacy_source and reactivate_from_profile:
                    restored_status, _original_engine = legacy_source
                    updated_values.update(
                        review_status=restored_status,
                        source_engine=candidate["source_engine"],
                    )
                    if restored_status == "pending":
                        updated_values.update(
                            reviewed_at=None,
                            reviewed_by=None,
                        )
                    else:
                        latest_human_review = (
                            connection.execute(
                                select(claim_reviews)
                                .where(
                                    claim_reviews.c.claim_id == claim_id,
                                    claim_reviews.c.reviewer
                                    != RELIABILITY_MIGRATION_REVIEWER,
                                )
                                .order_by(
                                    claim_reviews.c.created_at.desc(),
                                    claim_reviews.c.id.desc(),
                                )
                                .limit(1)
                            )
                            .mappings()
                            .first()
                        )
                        if latest_human_review:
                            updated_values.update(
                                reviewed_at=latest_human_review["created_at"],
                                reviewed_by=latest_human_review["reviewer"],
                            )
                        else:
                            # A status without a matching human review is not
                            # safe to revive as a curated decision.
                            updated_values.update(
                                review_status="pending",
                                reviewed_at=None,
                                reviewed_by=None,
                            )
                elif legacy_source and reactivate_from_independent:
                    updated_values.update(
                        confidence=int(candidate["confidence"]),
                        review_status="pending",
                        reviewed_at=None,
                        reviewed_by=None,
                        source_engine=candidate_engine,
                        source_job_id=job_id,
                    )
                if job_id is not None and (
                    not legacy_source or reactivate_legacy
                ):
                    updated_values["source_job_id"] = job_id
                if (
                    (
                        existing["review_status"] == "pending"
                        or updated_values.get("review_status") == "pending"
                    )
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
                if reactivate_legacy:
                    CaseStore._retire_claim_evidence_with_connection(
                        connection,
                        claim_id,
                        now=now,
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
                present = (
                    connection.execute(
                        select(
                            claim_evidence.c.id,
                            claim_evidence.c.details,
                        ).where(
                            claim_evidence.c.claim_id == claim_id,
                            claim_evidence.c.fingerprint
                            == evidence["fingerprint"],
                        )
                    )
                    .mappings()
                    .first()
                )
                if present:
                    if _is_retired_legacy_evidence(present["details"]):
                        connection.execute(
                            update(claim_evidence)
                            .where(claim_evidence.c.id == present["id"])
                            .values(
                                evidence_type=evidence["evidence_type"],
                                source_name=evidence["source_name"],
                                source_url=evidence["source_url"] or None,
                                details=evidence["details"],
                                observed_at=now,
                            )
                        )
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
            extract_user_scanner_username_claims,
        )
        from maigret.web.persona_intelligence import (
            extract_investigation_identifier_claims,
            extract_persona_claims,
            extract_supplied_profile_claims,
        )

        now = utcnow()
        synchronized = 0
        allow_legacy_reactivation = (
            result.get("profile_reliability_version") == PROFILE_RELIABILITY_VERSION
        )
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
            investigation_spec = dict(job_row["options"] or {}).get(
                "investigation_spec"
            )
            grouped_persona_id, personas_by_username = self._job_persona_bindings(
                investigation_spec, persona_rows
            )
            inputs_by_persona: Dict[str, list[str]] = {}
            for username, persona_ids in personas_by_username.items():
                for persona_id in persona_ids:
                    inputs_by_persona.setdefault(persona_id, []).append(username)
            if grouped_persona_id:
                inputs_by_persona = {grouped_persona_id: list(personas_by_username)}
            for persona_id, input_usernames in inputs_by_persona.items():
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=persona_id,
                    job_id=job_id,
                    candidates=extract_supplied_profile_claims(
                        self._persona_input_specification(
                            investigation_spec, persona_id
                        ),
                        usernames=input_usernames,
                    ),
                    now=now,
                    allow_legacy_reactivation=allow_legacy_reactivation,
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
                    allow_legacy_reactivation=allow_legacy_reactivation,
                )
            for report in result.get("individual_reports") or []:
                username = str(report.get("username") or "").strip()
                persona_ids = (
                    [grouped_persona_id]
                    if grouped_persona_id
                    else personas_by_username.get(username.casefold(), [])
                )
                for persona_id in persona_ids:
                    scoped_report = dict(
                        report,
                        claimed_profiles=[
                            profile
                            for profile in report.get("claimed_profiles") or []
                            if isinstance(profile, dict)
                            and persona_id
                            in self._profile_source_persona_ids(
                                investigation_spec, persona_ids, profile.get("url")
                            )
                        ],
                    )
                    synchronized += self._upsert_persona_candidates(
                        connection,
                        persona_id=persona_id,
                        job_id=job_id,
                        candidates=extract_persona_claims(scoped_report),
                        now=now,
                        allow_legacy_reactivation=allow_legacy_reactivation,
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
                    candidates=extract_user_scanner_claims(collector_observations),
                    now=now,
                    allow_legacy_reactivation=allow_legacy_reactivation,
                )
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=grouped_persona_id,
                    job_id=job_id,
                    candidates=extract_user_scanner_username_claims(
                        collector_observations
                    ),
                    now=now,
                    allow_legacy_reactivation=allow_legacy_reactivation,
                )
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=grouped_persona_id,
                    job_id=job_id,
                    candidates=extract_github_profile_claims(collector_observations),
                    now=now,
                    allow_legacy_reactivation=allow_legacy_reactivation,
                )
                synchronized += self._upsert_persona_candidates(
                    connection,
                    persona_id=grouped_persona_id,
                    job_id=job_id,
                    candidates=extract_profile_url_evidence_claims(
                        collector_observations
                    ),
                    now=now,
                    allow_legacy_reactivation=allow_legacy_reactivation,
                )
            else:
                observations_by_username: Dict[str, list] = {}
                for observation in collector_observations:
                    username_key = (
                        str(
                            (
                                observation.get("seed_username")
                                or observation.get("subject_value")
                                or ""
                            )
                            if observation.get("source_engine")
                            == "user_scanner_username"
                            else observation.get("subject_value") or ""
                        )
                        .strip()
                        .casefold()
                    )
                    if username_key:
                        observations_by_username.setdefault(username_key, []).append(
                            observation
                        )
                for username_key, observations in observations_by_username.items():
                    for persona_id in personas_by_username.get(username_key, []):
                        scoped_observations = [
                            observation
                            for observation in observations
                            if persona_id
                            in self._profile_source_persona_ids(
                                investigation_spec,
                                personas_by_username.get(username_key, []),
                                (
                                    observation.get("extra")
                                    if isinstance(observation.get("extra"), dict)
                                    else {}
                                ).get("queried_profile_url")
                                or observation.get("source_url"),
                            )
                        ]
                        synchronized += self._upsert_persona_candidates(
                            connection,
                            persona_id=persona_id,
                            job_id=job_id,
                            candidates=extract_user_scanner_username_claims(
                                scoped_observations
                            ),
                            now=now,
                            allow_legacy_reactivation=allow_legacy_reactivation,
                        )
                        synchronized += self._upsert_persona_candidates(
                            connection,
                            persona_id=persona_id,
                            job_id=job_id,
                            candidates=extract_github_profile_claims(
                                scoped_observations
                            ),
                            now=now,
                            allow_legacy_reactivation=allow_legacy_reactivation,
                        )
                        synchronized += self._upsert_persona_candidates(
                            connection,
                            persona_id=persona_id,
                            job_id=job_id,
                            candidates=extract_profile_url_evidence_claims(
                                scoped_observations
                            ),
                            now=now,
                            allow_legacy_reactivation=allow_legacy_reactivation,
                        )
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(updated_at=now)
            )
        return synchronized

    @staticmethod
    def _retire_profile_claim_rows(
        connection: Connection,
        claim_rows: Iterable[Dict[str, Any]],
        *,
        current_reliability_version: int,
        source_job_becoming_unavailable: bool = False,
    ) -> int:
        now = utcnow()
        retired = 0
        affected_personas = set()
        for claim in claim_rows:
            previous_status = str(claim["review_status"])
            previous_engine = str(claim["source_engine"])
            next_status = (
                "rejected" if previous_status == "rejected" else "uncertain"
            )
            values: Dict[str, Any] = {
                "review_status": next_status,
                "source_engine": _legacy_untriaged_source(
                    previous_status,
                    previous_engine,
                ),
                "updated_at": now,
            }
            if previous_status != "rejected":
                values.update(
                    reviewed_at=now,
                    reviewed_by=RELIABILITY_MIGRATION_REVIEWER,
                )
            source_job_id = claim["source_job_id"]
            source_job_match = (
                persona_claims.c.source_job_id.is_(None)
                if source_job_id is None
                else persona_claims.c.source_job_id == source_job_id
            )
            updated = connection.execute(
                update(persona_claims)
                .where(
                    persona_claims.c.id == claim["id"],
                    source_job_match,
                    persona_claims.c.source_engine == previous_engine,
                )
                .values(**values)
            )
            if updated.rowcount != 1:
                continue
            source_job_orphaned = (
                source_job_id is None or source_job_becoming_unavailable
            )
            if previous_status != "rejected":
                provenance_note = (
                    "Source job is no longer available, so its reliability "
                    "version cannot be verified."
                    if source_job_orphaned
                    else "Source investigation predates profile reliability triage."
                )
                connection.execute(
                    insert(claim_reviews).values(
                        claim_id=claim["id"],
                        decision="uncertain",
                        reviewer=RELIABILITY_MIGRATION_REVIEWER,
                        note=(
                            f"{provenance_note} Previous status was "
                            f"{previous_status}. "
                            "Rerun required before this claim becomes active."
                        ),
                        created_at=now,
                    )
                )
            provenance_id = (
                str(source_job_id)
                if source_job_id is not None
                else connection.scalar(
                    select(claim_observations.c.provenance_id)
                    .where(
                        claim_observations.c.claim_id == claim["id"],
                        claim_observations.c.provenance_type
                        == "investigation_job",
                        claim_observations.c.source_engine == previous_engine,
                    )
                    .order_by(claim_observations.c.observed_at.desc())
                    .limit(1)
                )
                or f"orphaned-claim:{claim['id']}"
            )
            CaseStore._record_claim_observation_with_connection(
                connection,
                claim_id=claim["id"],
                provenance_type="investigation_job",
                provenance_id=str(provenance_id),
                job_id=(str(source_job_id) if source_job_id is not None else None),
                external_evidence_id=None,
                chat_message_id=None,
                source_engine=RELIABILITY_MIGRATION_REVIEWER,
                source_record_id=None,
                confidence=None,
                native_status="legacy_untriaged",
                details={
                    "profile_reliability_version": current_reliability_version,
                    "previous_review_status": previous_status,
                    "previous_source_engine": previous_engine,
                    "rerun_required": True,
                    "source_job_orphaned": source_job_orphaned,
                },
                now=now,
            )
            affected_personas.add(str(claim["persona_id"]))
            retired += 1
        if affected_personas:
            case_ids = select(personas.c.case_id).where(
                personas.c.id.in_(affected_personas)
            )
            connection.execute(
                update(cases)
                .where(cases.c.id.in_(case_ids))
                .values(updated_at=now)
            )
        return retired

    def retire_pretriage_profile_claims(
        self,
        *,
        current_reliability_version: int,
        job_id: Optional[str] = None,
    ) -> int:
        """Retire claims from version-0 or unverifiable profile jobs."""
        with self.engine.begin() as connection:
            job_statement = select(
                investigation_jobs.c.id,
                investigation_jobs.c.result,
            ).where(
                investigation_jobs.c.status == "completed",
                investigation_jobs.c.kind.not_in(
                    {"affiliation", "case_fusion", "identity_enrichment"}
                ),
            )
            if job_id is not None:
                job_statement = job_statement.where(
                    investigation_jobs.c.id == job_id
                )
            legacy_job_ids = []
            for job_row in connection.execute(job_statement).mappings():
                result = dict(job_row["result"] or {})
                version = result.get("profile_reliability_version")
                if version is None or version == 0:
                    legacy_job_ids.append(str(job_row["id"]))
            source_job_conditions = [
                persona_claims.c.source_job_id.is_(None)
            ]
            if legacy_job_ids:
                source_job_conditions.append(
                    persona_claims.c.source_job_id.in_(legacy_job_ids)
                )
            claim_rows = list(
                connection.execute(
                    select(
                        persona_claims.c.id,
                        persona_claims.c.persona_id,
                        persona_claims.c.source_job_id,
                        persona_claims.c.source_engine,
                        persona_claims.c.review_status,
                    ).where(
                        persona_claims.c.source_engine.in_(
                            LEGACY_PROFILE_CLAIM_ENGINES
                        ),
                        or_(*source_job_conditions),
                    )
                ).mappings()
            )
            retire_claim_rows = (
                self._repoint_profile_claims_with_surviving_lineage(
                    connection,
                    claim_rows,
                )
            )
            return self._retire_profile_claim_rows(
                connection,
                retire_claim_rows,
                current_reliability_version=current_reliability_version,
            )

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
            investigation_spec = dict(job_row["options"] or {}).get(
                "investigation_spec"
            )
            grouped_persona_id, personas_by_username = self._job_persona_bindings(
                investigation_spec, persona_rows
            )
            for candidate in candidates:
                persona_ids = (
                    [grouped_persona_id]
                    if grouped_persona_id
                    else personas_by_username.get(candidate["username"].casefold(), [])
                )
                persona_ids = self._profile_source_persona_ids(
                    investigation_spec,
                    persona_ids,
                    candidate["evidence"][0].get("source_url"),
                )
                if not persona_ids:
                    continue
                for persona_id in persona_ids:
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
                        persona_claims.c.source_engine,
                        personas.c.case_id,
                    )
                    .select_from(
                        persona_claims.join(
                            personas,
                            personas.c.id == persona_claims.c.persona_id,
                        )
                    )
                    .where(persona_claims.c.id == claim_id)
                )
                .mappings()
                .first()
            )
            if not claim:
                return None
            legacy_source = _legacy_untriaged_source_details(
                claim["source_engine"]
            )
            if legacy_source and decision == "approved":
                raise ValueError(
                    "Rerun the source investigation before approving this "
                    "reliability-unverified claim"
                )
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
            if legacy_source:
                _previous_status, original_engine = legacy_source
                values["source_engine"] = _legacy_untriaged_source(
                    decision,
                    original_engine,
                )
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
            connection.execute(
                update(cases)
                .where(cases.c.id == claim["case_id"])
                .values(updated_at=now)
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

    @staticmethod
    def _relationship_sources(evidence_rows) -> list[Dict[str, Any]]:
        return [
            {
                "id": str(row["id"]),
                "name": str(row["source_name"]),
                "url": row["source_url"],
                "type": str(row["evidence_type"]),
                "observed_at": _as_iso(row["observed_at"]),
            }
            for row in evidence_rows
        ]

    def _build_case_fusion_snapshot_with_connection(
        self,
        connection: Connection,
        job_id: str,
        generated_at: datetime,
    ) -> Dict[str, Any]:
        job_row = (
            connection.execute(
                select(
                    investigation_jobs.c.id,
                    investigation_jobs.c.case_id,
                    investigation_jobs.c.kind,
                    cases.c.case_type,
                    cases.c.purpose,
                )
                .join(cases, cases.c.id == investigation_jobs.c.case_id)
                .where(investigation_jobs.c.id == job_id)
            )
            .mappings()
            .first()
        )
        if not job_row:
            raise KeyError(job_id)
        if job_row["kind"] != "case_fusion" or job_row["case_type"] != "combined":
            raise ValueError("This job is not a combined-case investigation")

        member_rows = list(
            connection.execute(
                select(
                    combined_case_members.c.source_case_id,
                    combined_case_members.c.position,
                    cases.c.title,
                    cases.c.status,
                    cases.c.updated_at,
                )
                .join(cases, cases.c.id == combined_case_members.c.source_case_id)
                .where(combined_case_members.c.combined_case_id == job_row["case_id"])
                .order_by(combined_case_members.c.position)
            ).mappings()
        )
        source_case_ids = [str(row["source_case_id"]) for row in member_rows]
        self._normalize_combined_source_case_ids(source_case_ids)
        source_cases = [
            {
                "id": str(row["source_case_id"]),
                "title": str(row["title"]),
                "status": str(row["status"]),
                "updated_at": _as_iso(row["updated_at"]),
            }
            for row in member_rows
        ]
        source_titles = {item["id"]: item["title"] for item in source_cases}

        persona_rows = list(
            connection.execute(
                select(personas)
                .where(personas.c.case_id.in_(source_case_ids))
                .order_by(personas.c.case_id, personas.c.created_at, personas.c.id)
            ).mappings()
        )
        persona_ids = [str(row["id"]) for row in persona_rows]
        persona_by_id = {str(row["id"]): row for row in persona_rows}
        claim_rows = (
            list(
                connection.execute(
                    select(persona_claims)
                    .where(
                        persona_claims.c.persona_id.in_(persona_ids),
                        persona_claims.c.review_status == "approved",
                    )
                    .order_by(
                        persona_claims.c.persona_id,
                        persona_claims.c.field_name,
                        persona_claims.c.normalized_value,
                        persona_claims.c.id,
                    )
                    .limit(MAX_COMBINED_APPROVED_CLAIMS + 1)
                ).mappings()
            )
            if persona_ids
            else []
        )
        if len(claim_rows) > MAX_COMBINED_APPROVED_CLAIMS:
            raise ValueError(
                "The selected cases contain too many approved records for one "
                "combined investigation; select a smaller case group"
            )
        claim_ids = [str(row["id"]) for row in claim_rows]
        evidence_by_claim: Dict[str, list] = {}
        latest_review_by_claim: Dict[str, Any] = {}
        if claim_ids:
            evidence_rows = list(
                connection.execute(
                    select(claim_evidence)
                    .where(
                        claim_evidence.c.claim_id.in_(claim_ids),
                        _active_claim_evidence_clause(),
                    )
                    .order_by(
                        claim_evidence.c.claim_id,
                        claim_evidence.c.observed_at,
                        claim_evidence.c.id,
                    )
                    .limit(MAX_COMBINED_EVIDENCE_REFERENCES + 1)
                ).mappings()
            )
            if len(evidence_rows) > MAX_COMBINED_EVIDENCE_REFERENCES:
                raise ValueError(
                    "The selected cases contain too many evidence references for "
                    "one combined investigation; select a smaller case group"
                )
            for evidence in evidence_rows:
                evidence_by_claim.setdefault(str(evidence["claim_id"]), []).append(
                    evidence
                )
            for review in connection.execute(
                select(claim_reviews)
                .where(claim_reviews.c.claim_id.in_(claim_ids))
                .order_by(
                    claim_reviews.c.claim_id,
                    claim_reviews.c.created_at.desc(),
                    claim_reviews.c.id.desc(),
                )
            ).mappings():
                latest_review_by_claim.setdefault(str(review["claim_id"]), review)

        latest_affiliation_jobs: Dict[str, Any] = {}
        for affiliation_job in connection.execute(
            select(investigation_jobs)
            .where(
                investigation_jobs.c.case_id.in_(source_case_ids),
                investigation_jobs.c.kind == "affiliation",
                investigation_jobs.c.status == "completed",
            )
            .order_by(
                investigation_jobs.c.case_id,
                investigation_jobs.c.created_at.desc(),
                investigation_jobs.c.id.desc(),
            )
        ).mappings():
            latest_affiliation_jobs.setdefault(
                str(affiliation_job["case_id"]), affiliation_job
            )

        organizations = []
        organizations_by_name: Dict[str, list] = {}
        for source_case_id in source_case_ids:
            affiliation_job = latest_affiliation_jobs.get(source_case_id)
            if not affiliation_job:
                continue
            result = dict(affiliation_job["result"] or {})
            selected = result.get("selected_organization")
            if not (
                isinstance(selected, dict)
                and selected.get("review_status") == "approved"
                and str(selected.get("label") or "").strip()
            ):
                continue
            label = " ".join(str(selected["label"]).split())[:500]
            normalized_label = label.casefold()
            organization = {
                "case_id": source_case_id,
                "case_title": source_titles[source_case_id],
                "label": label,
                "normalized_label": normalized_label,
                "candidate_key": str(selected.get("candidate_key") or "")[:500],
                "source_engine": str(selected.get("source_engine") or "")[:100],
                "source_name": str(selected.get("source_name") or "")[:300],
                "source_url": selected.get("source_url"),
                "identity_scope": str(selected.get("identity_scope") or "")[:100],
                "reviewed_by": str(selected.get("reviewed_by") or "")[:200],
                "reviewed_at": str(selected.get("reviewed_at") or ""),
            }
            organizations.append(organization)
            organizations_by_name.setdefault(normalized_label, []).append(organization)

        graph_rows = []
        manifest_claims = []
        analysis_claims = []
        for claim in claim_rows:
            claim_id = str(claim["id"])
            persona = persona_by_id[str(claim["persona_id"])]
            case_id = str(persona["case_id"])
            evidence_rows = evidence_by_claim.get(claim_id, [])
            review = latest_review_by_claim.get(claim_id)
            manifest_claims.append(
                {
                    "id": claim_id,
                    "case_id": case_id,
                    "persona_id": str(persona["id"]),
                    "field_name": str(claim["field_name"]),
                    "fingerprint": str(claim["fingerprint"]),
                    "review_id": int(review["id"]) if review else None,
                    "evidence_ids": [str(item["id"]) for item in evidence_rows],
                }
            )
            analysis_claims.append(
                {
                    "reference_id": f"claim:{claim_id}",
                    "claim_id": claim_id,
                    "case_id": case_id,
                    "case_title": source_titles[case_id],
                    "entity_ref": f"persona:{persona['id']}",
                    "persona_id": str(persona["id"]),
                    "persona_name": str(persona["display_name"]),
                    "field_name": str(claim["field_name"]),
                    "display_value": str(claim["display_value"])[:4000],
                    "confidence": int(claim["confidence"]),
                    "last_seen_at": _as_iso(claim["last_seen_at"]),
                    "sources": self._relationship_sources(evidence_rows)[:10],
                }
            )
            if claim["field_name"] in RELATIONSHIP_FIELDS:
                graph_rows.append(
                    {
                        "claim_id": claim_id,
                        "field_name": str(claim["field_name"]),
                        "display_value": str(claim["display_value"]),
                        "normalized_value": str(claim["normalized_value"]),
                        "confidence": int(claim["confidence"]),
                        "persona_id": str(persona["id"]),
                        "persona_name": str(persona["display_name"]),
                        "case_id": case_id,
                        "case_title": source_titles[case_id],
                        "sources": self._relationship_sources(evidence_rows)[:10],
                    }
                )

        shared: Dict[tuple[str, str], list] = {}
        for row in graph_rows:
            key = (row["field_name"], row["normalized_value"])
            shared.setdefault(key, []).append(row)

        nodes: list[Dict[str, Any]] = []
        edges: list[Dict[str, Any]] = []
        persona_nodes: Dict[str, Dict[str, Any]] = {}
        organization_nodes: Dict[str, Dict[str, Any]] = {}
        field_counts: Dict[str, int] = {}

        def append_edge(edge: Dict[str, Any]) -> None:
            if len(edges) >= MAX_COMBINED_RELATIONSHIP_EDGES:
                raise ValueError(
                    "The selected cases produce too many exact relationship paths "
                    "for one combined investigation; select a smaller case group"
                )
            edges.append(edge)

        for (field_name, normalized_value), candidates in shared.items():
            distinct_case_ids = {row["case_id"] for row in candidates}
            matched_organizations = (
                organizations_by_name.get(normalized_value, [])
                if field_name == "company"
                else []
            )
            external_organizations = [
                organization
                for organization in matched_organizations
                if any(row["case_id"] != organization["case_id"] for row in candidates)
            ]
            if external_organizations:
                field_counts[field_name] = field_counts.get(field_name, 0) + 1
                for organization in external_organizations:
                    organization_id = f"organization:{organization['case_id']}"
                    organization_nodes.setdefault(
                        organization_id,
                        {
                            "id": organization_id,
                            "label": organization["label"],
                            "kind": "organization",
                            "case_id": organization["case_id"],
                            "case_title": organization["case_title"],
                            "source_name": organization["source_name"],
                            "source_url": organization["source_url"],
                            "identity_scope": organization["identity_scope"],
                        },
                    )
                    for row in candidates:
                        if row["case_id"] == organization["case_id"]:
                            continue
                        persona_id = row["persona_id"]
                        persona_nodes.setdefault(
                            persona_id,
                            {
                                "id": f"persona:{persona_id}",
                                "label": row["persona_name"],
                                "kind": "persona",
                                "persona_id": persona_id,
                                "case_id": row["case_id"],
                                "case_title": row["case_title"],
                            },
                        )
                        sources = list(row["sources"])
                        if organization["source_name"] or organization["source_url"]:
                            sources.append(
                                {
                                    "name": organization["source_name"]
                                    or "Confirmed organization record",
                                    "url": organization["source_url"],
                                    "type": "analyst_confirmed_organization",
                                }
                            )
                        append_edge(
                            {
                                "id": (
                                    f"edge:{row['claim_id']}:"
                                    f"organization:{organization['case_id']}"
                                ),
                                "from": f"persona:{persona_id}",
                                "to": organization_id,
                                "label": "approved affiliation matches confirmed organization",
                                "field_name": field_name,
                                "confidence": row["confidence"],
                                "claim_id": row["claim_id"],
                                "sources": sources[:11],
                                "relationship_rule": (
                                    "Exact approved affiliation and analyst-confirmed "
                                    "organization name"
                                ),
                            }
                        )
                continue
            if len(distinct_case_ids) < 2:
                continue
            attribute_id = (
                f"attribute:{field_name}:"
                + hashlib.sha256(normalized_value.encode("utf-8")).hexdigest()[:20]
            )
            distinct_personas = {row["persona_id"] for row in candidates}
            field_counts[field_name] = field_counts.get(field_name, 0) + 1
            nodes.append(
                {
                    "id": attribute_id,
                    "label": candidates[0]["display_value"],
                    "kind": "attribute",
                    "field_name": field_name,
                    "persona_count": len(distinct_personas),
                    "case_count": len(distinct_case_ids),
                }
            )
            seen_personas = set()
            for row in candidates:
                persona_id = row["persona_id"]
                if persona_id in seen_personas:
                    continue
                seen_personas.add(persona_id)
                persona_nodes.setdefault(
                    persona_id,
                    {
                        "id": f"persona:{persona_id}",
                        "label": row["persona_name"],
                        "kind": "persona",
                        "persona_id": persona_id,
                        "case_id": row["case_id"],
                        "case_title": row["case_title"],
                    },
                )
                append_edge(
                    {
                        "id": f"edge:{row['claim_id']}",
                        "from": f"persona:{persona_id}",
                        "to": attribute_id,
                        "label": field_name.replace("_", " "),
                        "field_name": field_name,
                        "confidence": row["confidence"],
                        "claim_id": row["claim_id"],
                        "sources": row["sources"],
                        "relationship_rule": (
                            "Exact normalized value across approved claims in "
                            "different source cases"
                        ),
                    }
                )

        nodes = list(persona_nodes.values()) + list(organization_nodes.values()) + nodes
        graph = {
            "mode": "shared",
            "scope": "combined_case_snapshot",
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "persona_count": len(persona_nodes),
                "organization_count": len(organization_nodes),
                "shared_attribute_count": len(
                    [node for node in nodes if node.get("kind") == "attribute"]
                ),
                "connection_count": len(edges),
                "field_counts": field_counts,
            },
        }
        manifest = {
            "schema_version": 1,
            "source_cases": source_cases,
            "approved_claims": manifest_claims,
            "approved_organizations": organizations,
        }
        snapshot_sha256 = hashlib.sha256(
            json.dumps(
                manifest,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        analysis_claims.sort(
            key=lambda item: (
                item["field_name"] not in RELATIONSHIP_FIELDS,
                item["persona_id"],
                item["field_name"],
                item["claim_id"],
            )
        )
        bounded_analysis_claims = _bounded_round_robin_case_records(
            analysis_claims, source_case_ids, MAX_COMBINED_AI_CLAIMS
        )
        entities = [
            {
                "reference_id": f"case:{item['id']}",
                "entity_type": "case",
                "entity_id": item["id"],
                "label": item["title"],
                "case_id": item["id"],
                "case_title": item["title"],
            }
            for item in source_cases
        ]
        entities.extend(
            {
                "reference_id": f"persona:{row['id']}",
                "entity_type": "persona",
                "entity_id": str(row["id"]),
                "label": str(row["display_name"]),
                "case_id": str(row["case_id"]),
                "case_title": source_titles[str(row["case_id"])],
            }
            for row in persona_rows
        )
        entities.extend(
            {
                "reference_id": f"organization:{item['case_id']}",
                "entity_type": "organization",
                "entity_id": f"organization:{item['case_id']}",
                "label": item["label"],
                "case_id": item["case_id"],
                "case_title": item["case_title"],
            }
            for item in organizations
        )
        return {
            "snapshot": {
                **manifest,
                "generated_at": _as_iso(generated_at),
                "sha256": snapshot_sha256,
            },
            "relationship_graph": graph,
            "analysis_context": {
                "purpose": str(job_row["purpose"] or "")[:4000],
                "snapshot_sha256": snapshot_sha256,
                "source_cases": source_cases,
                "entities": entities,
                "approved_claims": bounded_analysis_claims,
                "approved_organizations": [
                    {
                        "reference_id": (
                            f"organization-evidence:{item['case_id']}"
                        ),
                        "case_id": item["case_id"],
                        "case_title": item["case_title"],
                        "entity_ref": f"organization:{item['case_id']}",
                        "label": item["label"],
                        "source_name": item["source_name"],
                        "source_url": item["source_url"],
                        "reviewed_by": item["reviewed_by"],
                        "reviewed_at": item["reviewed_at"],
                    }
                    for item in organizations
                ],
                "truncated_claim_count": max(
                    0, len(analysis_claims) - len(bounded_analysis_claims)
                ),
            },
            "source_case_count": len(source_cases),
            "approved_claim_count": len(manifest_claims),
            "approved_organization_count": len(organizations),
            "shared_attribute_count": graph["stats"]["shared_attribute_count"],
            "connection_count": graph["stats"]["connection_count"],
        }

    def build_case_fusion_snapshot(self, job_id: str) -> Dict[str, Any]:
        """Capture approved source evidence and exact links in one DB snapshot."""
        dialect = self.engine.dialect.name
        connection = self.engine.connect()
        if dialect == "postgresql":
            connection = connection.execution_options(isolation_level="REPEATABLE READ")
        elif dialect != "sqlite":
            connection = connection.execution_options(isolation_level="SERIALIZABLE")
        with connection:
            if dialect == "sqlite":
                connection.exec_driver_sql("BEGIN IMMEDIATE")
                generated_at = utcnow()
            else:
                connection.begin()
                if dialect == "postgresql":
                    connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                    generated_at = connection.scalar(select(func.statement_timestamp()))
                else:
                    generated_at = utcnow()
            try:
                if not isinstance(generated_at, datetime):
                    raise RuntimeError(
                        "Database did not provide a combined snapshot timestamp"
                    )
                if generated_at.tzinfo is None:
                    generated_at = generated_at.replace(tzinfo=timezone.utc)
                else:
                    generated_at = generated_at.astimezone(timezone.utc)
                snapshot = self._build_case_fusion_snapshot_with_connection(
                    connection, job_id, generated_at
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return snapshot

    @staticmethod
    def _serialize_combined_proposal(row, reviews=()) -> Dict[str, Any]:
        return {
            "id": str(row["id"]),
            "analysis_run_id": str(row["analysis_run_id"]),
            "chat_message_id": (
                str(row["chat_message_id"]) if row["chat_message_id"] else None
            ),
            "title": str(row["title"]),
            "relationship_type": str(row["relationship_type"]),
            "subject_ref": str(row["subject_ref"]),
            "subject_entity": dict(row["subject_entity"] or {}),
            "object_ref": str(row["object_ref"]),
            "object_entity": dict(row["object_entity"] or {}),
            "explanation": str(row["explanation"]),
            "confidence": int(row["confidence"]),
            "evidence": list(row["evidence"] or []),
            "contradictory_evidence": list(row["contradictory_evidence"] or []),
            "limitations": list(row["limitations"] or []),
            "review_status": str(row["review_status"]),
            "created_at": _as_iso(row["created_at"]),
            "reviewed_at": _as_iso(row["reviewed_at"]),
            "reviewed_by": str(row["reviewed_by"]) if row["reviewed_by"] else None,
            "reviews": [
                {
                    "decision": str(review["decision"]),
                    "reviewer": str(review["reviewer"]),
                    "note": str(review["note"]) if review["note"] else None,
                    "created_at": _as_iso(review["created_at"]),
                }
                for review in reviews
            ],
        }

    def _combined_analysis_runs_with_connection(
        self, connection: Connection, combined_case_id: str
    ) -> list[Dict[str, Any]]:
        run_rows = list(
            connection.execute(
                select(combined_analysis_runs)
                .where(combined_analysis_runs.c.combined_case_id == combined_case_id)
                .order_by(
                    combined_analysis_runs.c.created_at.desc(),
                    combined_analysis_runs.c.id.desc(),
                )
            ).mappings()
        )
        if not run_rows:
            return []
        run_ids = [str(row["id"]) for row in run_rows]
        proposal_rows = list(
            connection.execute(
                select(combined_relationship_proposals)
                .where(combined_relationship_proposals.c.analysis_run_id.in_(run_ids))
                .order_by(
                    combined_relationship_proposals.c.created_at,
                    combined_relationship_proposals.c.id,
                )
            ).mappings()
        )
        proposal_ids = [str(row["id"]) for row in proposal_rows]
        reviews_by_proposal: Dict[str, list] = {}
        if proposal_ids:
            for review in connection.execute(
                select(combined_relationship_reviews)
                .where(
                    combined_relationship_reviews.c.proposal_id.in_(proposal_ids)
                )
                .order_by(
                    combined_relationship_reviews.c.created_at,
                    combined_relationship_reviews.c.id,
                )
            ).mappings():
                reviews_by_proposal.setdefault(str(review["proposal_id"]), []).append(
                    review
                )
        proposals_by_run: Dict[str, list] = {}
        for proposal in proposal_rows:
            proposal_id = str(proposal["id"])
            proposals_by_run.setdefault(str(proposal["analysis_run_id"]), []).append(
                self._serialize_combined_proposal(
                    proposal, reviews_by_proposal.get(proposal_id, [])
                )
            )
        return [
            {
                "id": str(row["id"]),
                "combined_case_id": str(row["combined_case_id"]),
                "job_id": str(row["job_id"]),
                "snapshot_sha256": str(row["snapshot_sha256"]),
                "status": str(row["status"]),
                "model": str(row["model"]) if row["model"] else None,
                "web_search_enabled": bool(row["web_search_enabled"]),
                "executive_summary": str(row["executive_summary"] or ""),
                "key_findings": list(row["key_findings"] or []),
                "contradictions": list(row["contradictions"] or []),
                "information_gaps": list(row["information_gaps"] or []),
                "next_steps": list(row["next_steps"] or []),
                "sources": list(row["sources"] or []),
                "error": str(row["error"]) if row["error"] else None,
                "created_at": _as_iso(row["created_at"]),
                "completed_at": _as_iso(row["completed_at"]),
                "proposals": proposals_by_run.get(str(row["id"]), []),
            }
            for row in run_rows
        ]

    def start_combined_analysis_run(
        self,
        job_id: str,
        snapshot_sha256: str,
        *,
        model: Optional[str],
        web_search_enabled: bool,
    ) -> str:
        """Create the durable AI run bound to one immutable fusion snapshot."""
        snapshot_sha = str(snapshot_sha256 or "").strip().casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", snapshot_sha):
            raise ValueError("A valid combined snapshot SHA-256 is required")
        now = utcnow()
        with self.engine.begin() as connection:
            existing = connection.scalar(
                select(combined_analysis_runs.c.id).where(
                    combined_analysis_runs.c.job_id == job_id
                )
            )
            if existing:
                return str(existing)
            job_row = (
                connection.execute(
                    select(
                        investigation_jobs.c.case_id,
                        investigation_jobs.c.kind,
                        cases.c.case_type,
                    )
                    .join(cases, cases.c.id == investigation_jobs.c.case_id)
                    .where(investigation_jobs.c.id == job_id)
                )
                .mappings()
                .first()
            )
            if not job_row:
                raise KeyError(job_id)
            if job_row["kind"] != "case_fusion" or job_row["case_type"] != "combined":
                raise ValueError("This job is not a combined-case investigation")
            run_id = str(uuid.uuid4())
            connection.execute(
                insert(combined_analysis_runs).values(
                    id=run_id,
                    combined_case_id=str(job_row["case_id"]),
                    job_id=job_id,
                    snapshot_sha256=snapshot_sha,
                    status="processing",
                    model=str(model)[:100] if model else None,
                    web_search_enabled=bool(web_search_enabled),
                    executive_summary=None,
                    key_findings=[],
                    contradictions=[],
                    information_gaps=[],
                    next_steps=[],
                    sources=[],
                    error=None,
                    created_at=now,
                    completed_at=None,
                )
            )
        return run_id

    def complete_combined_analysis_run(
        self, run_id: str, insights: Dict[str, Any], *, connection=None
    ) -> int:
        """Persist validated insight output and pending relationship proposals."""
        now = utcnow()
        proposals = list(insights.get("proposals") or [])[:MAX_COMBINED_AI_PROPOSALS]
        from contextlib import nullcontext
        with (self.engine.begin() if connection is None else nullcontext(connection)) as connection:
            run_row = (
                connection.execute(
                    select(combined_analysis_runs.c.status).where(
                        combined_analysis_runs.c.id == run_id
                    )
                )
                .mappings()
                .first()
            )
            if not run_row:
                raise KeyError(run_id)
            if run_row["status"] != "processing":
                raise ValueError("This combined analysis run is already complete")
            for proposal in proposals:
                connection.execute(
                    insert(combined_relationship_proposals).values(
                        id=str(uuid.uuid4()),
                        analysis_run_id=run_id,
                        chat_message_id=None,
                        title=str(proposal["title"]),
                        relationship_type=str(proposal["relationship_type"]),
                        subject_ref=str(proposal["subject_ref"]),
                        subject_entity=dict(proposal["subject_entity"]),
                        object_ref=str(proposal["object_ref"]),
                        object_entity=dict(proposal["object_entity"]),
                        explanation=str(proposal["explanation"]),
                        confidence=int(proposal["confidence"]),
                        evidence=list(proposal.get("evidence") or []),
                        contradictory_evidence=list(
                            proposal.get("contradictory_evidence") or []
                        ),
                        limitations=list(proposal.get("limitations") or []),
                        review_status="pending",
                        created_at=now,
                        reviewed_at=None,
                        reviewed_by=None,
                    )
                )
            connection.execute(
                update(combined_analysis_runs)
                .where(combined_analysis_runs.c.id == run_id)
                .values(
                    status="completed",
                    executive_summary=str(insights.get("executive_summary") or ""),
                    key_findings=list(insights.get("key_findings") or []),
                    contradictions=list(insights.get("contradictions") or []),
                    information_gaps=list(insights.get("information_gaps") or []),
                    next_steps=list(insights.get("next_steps") or []),
                    sources=list(insights.get("sources") or []),
                    error=None,
                    completed_at=now,
                )
            )
        return len(proposals)

    def append_combined_relationship_proposals(
        self,
        combined_case_id: str,
        analysis_run_id: str,
        chat_message_id: str,
        proposals: Iterable[Dict[str, Any]],
    ) -> list[str]:
        """Append chat hypotheses only while their snapshot remains current."""
        candidates = list(proposals or [])[:MAX_COMBINED_AI_PROPOSALS]
        if not candidates:
            return []
        now = utcnow()
        dialect = self.engine.dialect.name
        connection = self.engine.connect()
        if dialect != "sqlite":
            connection = connection.execution_options(isolation_level="SERIALIZABLE")
        with connection:
            if dialect == "sqlite":
                connection.exec_driver_sql("BEGIN IMMEDIATE")
            else:
                connection.begin()
            try:
                member_ids = list(
                    connection.scalars(
                        select(combined_case_members.c.source_case_id).where(
                            combined_case_members.c.combined_case_id == combined_case_id
                        )
                    )
                )
                case_ids = sorted({combined_case_id, *map(str, member_ids)})
                case_lock = select(cases.c.id).where(cases.c.id.in_(case_ids))
                if dialect == "postgresql":
                    case_lock = case_lock.with_for_update()
                locked_case_ids = {str(item) for item in connection.scalars(case_lock)}
                if set(case_ids) != locked_case_ids:
                    raise StaleCombinedSnapshotError(
                        "The combined investigation source set changed"
                    )
                inserted_ids = self._append_current_combined_relationship_proposals(
                    connection,
                    combined_case_id,
                    analysis_run_id,
                    chat_message_id,
                    candidates,
                    now,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return inserted_ids

    def _append_current_combined_relationship_proposals(
        self,
        connection: Connection,
        combined_case_id: str,
        analysis_run_id: str,
        chat_message_id: str,
        candidates: list[Dict[str, Any]],
        now: datetime,
    ) -> list[str]:
        """Validate current snapshot state and insert within one transaction."""
        run_row = (
            connection.execute(
                select(
                    combined_analysis_runs.c.combined_case_id,
                    combined_analysis_runs.c.job_id,
                    combined_analysis_runs.c.snapshot_sha256,
                    combined_analysis_runs.c.status,
                ).where(combined_analysis_runs.c.id == analysis_run_id)
            )
            .mappings()
            .first()
        )
        if not run_row:
            raise KeyError(analysis_run_id)
        if (
            str(run_row["combined_case_id"]) != combined_case_id
            or str(run_row["status"]) != "completed"
        ):
            raise ValueError(
                "Relationship proposals require the completed analysis for this investigation"
            )
        current_job = (
            connection.execute(
                select(investigation_jobs.c.id, investigation_jobs.c.status)
                .where(
                    investigation_jobs.c.case_id == combined_case_id,
                    investigation_jobs.c.kind == "case_fusion",
                )
                .order_by(
                    investigation_jobs.c.created_at.desc(),
                    investigation_jobs.c.id.desc(),
                )
                .limit(1)
            )
            .mappings()
            .first()
        )
        if (
            not current_job
            or str(current_job["id"]) != str(run_row["job_id"])
            or str(current_job["status"]) != "completed"
        ):
            raise StaleCombinedSnapshotError(
                "A newer combined investigation snapshot is active"
            )
        current_job_id = str(current_job["id"])
        rebuilt = self._build_case_fusion_snapshot_with_connection(
            connection,
            current_job_id,
            now,
        )
        rebuilt_sha = str((rebuilt.get("snapshot") or {}).get("sha256") or "")
        if rebuilt_sha != str(run_row["snapshot_sha256"]):
            raise StaleCombinedSnapshotError(
                "The source cases changed after the AI request started"
            )
        message_row = (
            connection.execute(
                select(
                    case_chat_messages.c.case_id,
                    case_chat_messages.c.role,
                ).where(case_chat_messages.c.id == chat_message_id)
            )
            .mappings()
            .first()
        )
        if not message_row:
            raise KeyError(chat_message_id)
        if (
            str(message_row["case_id"]) != combined_case_id
            or str(message_row["role"]) != "assistant"
        ):
            raise ValueError(
                "Relationship proposals require an assistant message from this investigation"
            )
        existing_rows = list(
            connection.execute(
                select(
                    combined_relationship_proposals.c.relationship_type,
                    combined_relationship_proposals.c.subject_ref,
                    combined_relationship_proposals.c.object_ref,
                    combined_relationship_proposals.c.title,
                ).where(
                    combined_relationship_proposals.c.analysis_run_id
                    == analysis_run_id
                )
            ).mappings()
        )
        remaining = max(0, MAX_COMBINED_AI_PROPOSALS - len(existing_rows))
        fingerprints = {
            (
                str(row["relationship_type"]),
                tuple(sorted((str(row["subject_ref"]), str(row["object_ref"])))),
                str(row["title"]).strip().casefold(),
            )
            for row in existing_rows
        }
        inserted_ids: list[str] = []
        for proposal in candidates:
            if len(inserted_ids) >= remaining:
                break
            fingerprint = (
                str(proposal["relationship_type"]),
                tuple(
                    sorted(
                        (
                            str(proposal["subject_ref"]),
                            str(proposal["object_ref"]),
                        )
                    )
                ),
                str(proposal["title"]).strip().casefold(),
            )
            if fingerprint in fingerprints:
                continue
            fingerprints.add(fingerprint)
            proposal_id = str(uuid.uuid4())
            connection.execute(
                insert(combined_relationship_proposals).values(
                    id=proposal_id,
                    analysis_run_id=analysis_run_id,
                    chat_message_id=chat_message_id,
                    title=str(proposal["title"]),
                    relationship_type=str(proposal["relationship_type"]),
                    subject_ref=str(proposal["subject_ref"]),
                    subject_entity=dict(proposal["subject_entity"]),
                    object_ref=str(proposal["object_ref"]),
                    object_entity=dict(proposal["object_entity"]),
                    explanation=str(proposal["explanation"]),
                    confidence=int(proposal["confidence"]),
                    evidence=list(proposal.get("evidence") or []),
                    contradictory_evidence=list(
                        proposal.get("contradictory_evidence") or []
                    ),
                    limitations=list(proposal.get("limitations") or []),
                    review_status="pending",
                    created_at=now,
                    reviewed_at=None,
                    reviewed_by=None,
                )
            )
            inserted_ids.append(proposal_id)
        if inserted_ids:
            connection.execute(
                update(cases)
                .where(cases.c.id == combined_case_id)
                .values(updated_at=now)
            )
        return inserted_ids

    def stop_combined_analysis_run(
        self, run_id: str, *, status: str, error: str
    ) -> None:
        """Finish an unavailable or failed AI run without failing its snapshot."""
        if status not in {"unavailable", "failed", "cancelled"}:
            raise ValueError("Invalid combined analysis terminal status")
        now = utcnow()
        with self.engine.begin() as connection:
            result = connection.execute(
                update(combined_analysis_runs)
                .where(
                    combined_analysis_runs.c.id == run_id,
                    combined_analysis_runs.c.status == "processing",
                )
                .values(status=status, error=str(error)[:2000], completed_at=now)
            )
            if result.rowcount != 1:
                raise KeyError(run_id)

    def review_combined_relationship_proposal(
        self,
        combined_case_id: str,
        proposal_id: str,
        decision: str,
        reviewer: str,
        note: str = "",
    ) -> None:
        """Record an analyst decision without changing any source-case evidence."""
        if decision not in {"approved", "rejected", "uncertain"}:
            raise ValueError("Choose approved, rejected, or uncertain")
        normalized_reviewer = " ".join(str(reviewer or "").split())[:200]
        if not normalized_reviewer:
            raise ValueError("A reviewer is required")
        normalized_note = str(note or "").strip()[:2000] or None
        now = utcnow()
        with self.engine.begin() as connection:
            statement = (
                select(combined_relationship_proposals.c.id)
                .join(
                    combined_analysis_runs,
                    combined_analysis_runs.c.id
                    == combined_relationship_proposals.c.analysis_run_id,
                )
                .where(
                    combined_relationship_proposals.c.id == proposal_id,
                    combined_analysis_runs.c.combined_case_id == combined_case_id,
                )
            )
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            if connection.scalar(statement) is None:
                raise KeyError(proposal_id)
            connection.execute(
                update(combined_relationship_proposals)
                .where(combined_relationship_proposals.c.id == proposal_id)
                .values(
                    review_status=decision,
                    reviewed_at=now,
                    reviewed_by=normalized_reviewer,
                )
            )
            connection.execute(
                insert(combined_relationship_reviews).values(
                    proposal_id=proposal_id,
                    decision=decision,
                    reviewer=normalized_reviewer,
                    note=normalized_note,
                    created_at=now,
                )
            )
            connection.execute(
                update(cases)
                .where(cases.c.id == combined_case_id)
                .values(updated_at=now)
            )

    def build_relationship_graph(self, case_id: Optional[str] = None) -> Dict[str, Any]:
        """Project approved, exact shared attributes across two or more personas."""
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
                persona_claims.c.field_name.in_(RELATIONSHIP_FIELDS),
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
                        claim_evidence.c.claim_id.in_(claim_ids),
                        _active_claim_evidence_clause(),
                    )
                ).mappings():
                    evidence_by_claim.setdefault(str(evidence["claim_id"]), []).append(
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
                        "sources": evidence_by_claim.get(str(row["claim_id"]), [])[:10],
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
            claim
            for claim in graph_claims
            if claim["review_status"] != "rejected"
            and claim.get("reliability_status") != "legacy_untriaged"
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
        legacy_untriaged = _legacy_untriaged_source_details(
            claim_row["source_engine"]
        )
        serialized_evidence = [
            {
                "id": row["id"],
                "evidence_type": row["evidence_type"],
                "source_name": row["source_name"],
                "source_url": row["source_url"],
                "details": dict(row["details"] or {}),
                "observed_at": _as_iso(row["observed_at"]),
            }
            for row in evidence_rows
        ]
        active_evidence = [
            row
            for row in serialized_evidence
            if not _is_retired_legacy_evidence(row["details"])
        ]
        retired_evidence = [
            row
            for row in serialized_evidence
            if _is_retired_legacy_evidence(row["details"])
        ]
        return {
            "id": claim_row["id"],
            "field_name": claim_row["field_name"],
            "value": claim_row["value"],
            "display_value": claim_row["display_value"],
            "confidence": int(claim_row["confidence"]),
            "review_status": claim_row["review_status"],
            "source_engine": claim_row["source_engine"],
            "reliability_status": (
                "legacy_untriaged" if legacy_untriaged else "current"
            ),
            "source_job_id": claim_row["source_job_id"],
            "first_seen_at": _as_iso(claim_row["first_seen_at"]),
            "last_seen_at": _as_iso(claim_row["last_seen_at"]),
            "reviewed_at": _as_iso(claim_row["reviewed_at"]),
            "reviewed_by": claim_row["reviewed_by"],
            "normalized_value": claim_row["normalized_value"],
            "latitude": claim_row["latitude"],
            "longitude": claim_row["longitude"],
            "evidence": active_evidence,
            "retired_evidence": retired_evidence,
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
                investigation_jobs.c.cancel_requested,
                investigation_jobs.c.cancel_requested_at,
            ).where(investigation_jobs.c.id == job_id)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = connection.execute(statement).mappings().first()
            if not row:
                return False
            if row["status"] == "cancel_requested" or (
                bool(row["cancel_requested"])
                and row["status"] in TERMINAL_STATUSES
            ):
                return True
            if row["status"] not in {"queued", "running"}:
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
                        cancel_requested_at=now,
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
                        cancel_requested_at=now,
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

    def heartbeat(self, job_id: str, worker_id: str) -> bool:
        """Renew an active job lease only for the worker that claimed it."""
        now = utcnow()
        cutoff = now - timedelta(seconds=WORKER_STALE_AFTER_SECONDS)
        with self.engine.begin() as connection:
            result = connection.execute(
                update(investigation_jobs)
                .where(
                    investigation_jobs.c.id == job_id,
                    investigation_jobs.c.worker_id == worker_id,
                    investigation_jobs.c.status.in_(("running", "cancel_requested")),
                    investigation_jobs.c.heartbeat_at.is_not(None),
                    investigation_jobs.c.heartbeat_at >= cutoff,
                )
                .values(heartbeat_at=now, updated_at=now)
            )
        return bool(result.rowcount)

    def finish(
        self,
        job_id: str,
        result: Dict[str, Any],
        *,
        worker_id: Optional[str] = None,
    ) -> bool:
        """Publish a terminal result once, optionally enforcing worker ownership."""
        status = str(result.get("status", "failed"))
        if status not in TERMINAL_STATUSES:
            status = "failed"
        now = utcnow()
        with self.engine.begin() as connection:
            conditions = [
                investigation_jobs.c.id == job_id,
                investigation_jobs.c.status.in_(("running", "cancel_requested")),
            ]
            if worker_id is not None:
                conditions.extend(
                    (
                        investigation_jobs.c.worker_id == worker_id,
                        investigation_jobs.c.heartbeat_at.is_not(None),
                        investigation_jobs.c.heartbeat_at
                        >= now - timedelta(seconds=WORKER_STALE_AFTER_SECONDS),
                    )
                )
            updated = connection.execute(
                update(investigation_jobs)
                .where(*conditions)
                .values(
                    status=status,
                    result=dict(result),
                    error=str(result.get("error")) if result.get("error") else None,
                    completed_at=now,
                    heartbeat_at=now,
                    updated_at=now,
                )
            )
        return bool(updated.rowcount)

    def mark_stale_running(
        self, stale_after_seconds: int = WORKER_STALE_AFTER_SECONDS
    ) -> int:
        cutoff = utcnow() - timedelta(seconds=max(0, stale_after_seconds))
        now = utcnow()
        with self.engine.begin() as connection:
            stale_rows = list(
                connection.execute(
                    select(investigation_jobs).where(
                        investigation_jobs.c.status.in_(
                            ("running", "cancel_requested")
                        ),
                        or_(
                            investigation_jobs.c.heartbeat_at.is_(None),
                            investigation_jobs.c.heartbeat_at < cutoff,
                        ),
                    ).order_by(investigation_jobs.c.id).with_for_update()
                ).mappings()
            )
            from maigret.web.pipeline_store import PipelineStore
            pipeline = PipelineStore(self)
            for row in stale_rows:
                pipeline.reconcile_interrupted_job(
                    connection, row,
                    reason="Worker lease expired; evidence retained. Resume this query within its original budgets or run new research in the same case.",
                )
            result = connection.execute(
                update(investigation_jobs)
                .where(
                    investigation_jobs.c.id.in_([row["id"] for row in stale_rows]),
                )
                .values(
                    status="interrupted",
                    error="The worker stopped before this investigation completed.",
                    worker_id=None,
                    completed_at=now,
                    updated_at=now,
                )
            )
            snapshot_job_ids = {
                str(row["id"])
                for row in stale_rows
                if str(row["kind"]) == "case_fusion"
            }
            for row in stale_rows:
                if str(row["kind"]) != "case_fusion_ai":
                    continue
                specification = dict(row["options"] or {}).get("investigation_spec")
                if isinstance(specification, dict):
                    snapshot_job_id = str(
                        specification.get("snapshot_job_id") or ""
                    ).strip()
                    if snapshot_job_id:
                        snapshot_job_ids.add(snapshot_job_id)
            if snapshot_job_ids:
                connection.execute(
                    update(combined_analysis_runs)
                    .where(
                        combined_analysis_runs.c.job_id.in_(snapshot_job_ids),
                        combined_analysis_runs.c.status == "processing",
                    )
                    .values(
                        status="failed",
                        error=(
                            "The worker stopped during AI synthesis. The immutable "
                            "approved-evidence snapshot remains available."
                        ),
                        completed_at=now,
                    )
                )
        return int(result.rowcount or 0)

    @staticmethod
    def _combined_case_references_with_connection(
        connection: Connection, source_case_id: str
    ) -> list[Dict[str, str]]:
        parent_cases = cases.alias("parent_cases")
        rows = connection.execute(
            select(parent_cases.c.id, parent_cases.c.title)
            .select_from(
                combined_case_members.join(
                    parent_cases,
                    parent_cases.c.id == combined_case_members.c.combined_case_id,
                )
            )
            .where(combined_case_members.c.source_case_id == source_case_id)
            .order_by(parent_cases.c.created_at, parent_cases.c.id)
        ).mappings()
        return [
            {"id": str(row["id"]), "title": str(row["title"])} for row in rows
        ]

    @staticmethod
    def _repoint_profile_claims_with_surviving_lineage(
        connection: Connection,
        claim_rows: Iterable[Dict[str, Any]],
        *,
        excluded_job_ids: Iterable[str] = (),
    ) -> list[Dict[str, Any]]:
        """Repoint claims with valid lineage and return unsupported claims."""
        claims = list(claim_rows)
        claim_ids = [str(claim["id"]) for claim in claims]
        if not claim_ids:
            return []
        excluded_jobs = {str(job_id) for job_id in excluded_job_ids}
        observations = list(
            connection.execute(
                select(
                    claim_observations.c.id,
                    claim_observations.c.claim_id,
                    claim_observations.c.provenance_type,
                    claim_observations.c.job_id,
                    claim_observations.c.external_evidence_id,
                    claim_observations.c.chat_message_id,
                    claim_observations.c.source_engine,
                    claim_observations.c.details,
                    claim_observations.c.observed_at,
                )
                .where(
                    claim_observations.c.claim_id.in_(claim_ids),
                    claim_observations.c.source_engine
                    != RELIABILITY_MIGRATION_REVIEWER,
                    ~claim_observations.c.source_engine.like(
                        f"{LEGACY_UNTRIAGED_SOURCE_PREFIX}%"
                    ),
                )
                .order_by(
                    claim_observations.c.observed_at.desc(),
                    claim_observations.c.id.desc(),
                )
            ).mappings()
        )
        job_ids = {
            str(observation["job_id"])
            for observation in observations
            if observation["job_id"] is not None
            and str(observation["job_id"]) not in excluded_jobs
        }
        job_rows = (
            connection.execute(
                select(
                    investigation_jobs.c.id,
                    investigation_jobs.c.status,
                    investigation_jobs.c.result,
                ).where(investigation_jobs.c.id.in_(job_ids))
            ).mappings()
            if job_ids
            else []
        )
        jobs_by_id = {str(row["id"]): dict(row) for row in job_rows}
        chat_message_ids = {
            str(observation["chat_message_id"])
            for observation in observations
            if observation["chat_message_id"] is not None
        }
        surviving_chat_ids = (
            set(
                connection.scalars(
                    select(case_chat_messages.c.id).where(
                        case_chat_messages.c.id.in_(chat_message_ids)
                    )
                )
            )
            if chat_message_ids
            else set()
        )
        evidence_ids = {
            str(observation["external_evidence_id"])
            for observation in observations
            if observation["external_evidence_id"] is not None
        }
        surviving_evidence_ids = (
            set(
                connection.scalars(
                    select(external_evidence_records.c.id).where(
                        external_evidence_records.c.id.in_(evidence_ids)
                    )
                )
            )
            if evidence_ids
            else set()
        )
        surviving_by_claim: Dict[str, Dict[str, Any]] = {}
        surviving_evidence_by_claim: Dict[str, set[str]] = {}
        for observation in observations:
            claim_id = str(observation["claim_id"])
            provenance_type = str(observation["provenance_type"])
            source_engine = str(observation["source_engine"])
            job = jobs_by_id.get(str(observation["job_id"] or ""))
            valid = False
            if source_engine in LEGACY_PROFILE_CLAIM_ENGINES:
                result = dict(job["result"] or {}) if job else {}
                valid = bool(
                    job
                    and job["status"] == "completed"
                    and result.get("profile_reliability_version")
                    == PROFILE_RELIABILITY_VERSION
                )
            elif provenance_type == "investigation_job":
                valid = bool(job and job["status"] == "completed")
            elif provenance_type == "case_chat_message":
                valid = observation["chat_message_id"] in surviving_chat_ids
            elif provenance_type == "external_evidence":
                valid = (
                    observation["external_evidence_id"]
                    in surviving_evidence_ids
                )
            if valid:
                surviving_by_claim.setdefault(claim_id, dict(observation))
                observation_details = dict(observation["details"] or {})
                evidence_fingerprints = observation_details.get(
                    "evidence_fingerprints"
                )
                if isinstance(evidence_fingerprints, list):
                    surviving_evidence_by_claim.setdefault(
                        claim_id,
                        set(),
                    ).update(
                        str(fingerprint)
                        for fingerprint in evidence_fingerprints
                        if fingerprint is not None
                        and str(fingerprint).strip()
                    )

        retire_rows = []
        now = utcnow()
        for claim in claims:
            survivor = surviving_by_claim.get(str(claim["id"]))
            if not survivor:
                retire_rows.append(claim)
                continue
            source_job_match = (
                persona_claims.c.source_job_id.is_(None)
                if claim["source_job_id"] is None
                else persona_claims.c.source_job_id == claim["source_job_id"]
            )
            repointed = connection.execute(
                update(persona_claims)
                .where(
                    persona_claims.c.id == claim["id"],
                    source_job_match,
                    persona_claims.c.source_engine == claim["source_engine"],
                )
                .values(
                    source_job_id=survivor["job_id"],
                    source_engine=survivor["source_engine"],
                    updated_at=now,
                )
            )
            if repointed.rowcount == 1:
                CaseStore._retire_claim_evidence_with_connection(
                    connection,
                    str(claim["id"]),
                    now=now,
                )
                CaseStore._restore_claim_evidence_fingerprints_with_connection(
                    connection,
                    str(claim["id"]),
                    surviving_evidence_by_claim.get(str(claim["id"]), set()),
                )
        return retire_rows

    @staticmethod
    def _assert_pipeline_lineage_retained_with_connection(connection, case_id):
        """Give old deletion routes a reviewable error before any retirement writes."""
        for table_name in ("pipeline_requests", "pipeline_persona_versions"):
            table = metadata.tables[table_name]
            if connection.execute(
                select(table.c.id).where(table.c.case_id == case_id).limit(1)
            ).first():
                raise ValueError(
                    "This case has retained P2 pipeline evidence or review history. "
                    "Archive the case or withdraw its final Persona version; ordinary "
                    "deletion cannot erase the existing research lineage."
                )

    def delete_job(
        self, job_id: str, *, confirmation_name: Optional[str] = None
    ) -> bool:
        with self.engine.begin() as connection:
            statement = (
                select(
                    investigation_jobs.c.case_id,
                    investigation_jobs.c.status,
                    cases.c.title.label("case_title"),
                    cases.c.case_type,
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
            self._assert_pipeline_lineage_retained_with_connection(connection, row["case_id"])
            sibling_job = connection.scalar(
                select(investigation_jobs.c.id)
                .where(
                    investigation_jobs.c.case_id == row["case_id"],
                    investigation_jobs.c.id != job_id,
                )
                .limit(1)
            )
            if sibling_job:
                claim_rows = list(
                    connection.execute(
                        select(
                            persona_claims.c.id,
                            persona_claims.c.persona_id,
                            persona_claims.c.source_job_id,
                            persona_claims.c.source_engine,
                            persona_claims.c.review_status,
                        ).where(
                            persona_claims.c.source_job_id == job_id,
                            persona_claims.c.source_engine.in_(
                                LEGACY_PROFILE_CLAIM_ENGINES
                            ),
                        )
                    ).mappings()
                )
                retire_claim_rows = (
                    self._repoint_profile_claims_with_surviving_lineage(
                        connection,
                        claim_rows,
                        excluded_job_ids={job_id},
                    )
                )
                self._retire_profile_claim_rows(
                    connection,
                    retire_claim_rows,
                    current_reliability_version=PROFILE_RELIABILITY_VERSION,
                    source_job_becoming_unavailable=True,
                )
                connection.execute(
                    delete(investigation_jobs).where(investigation_jobs.c.id == job_id)
                )
                connection.execute(
                    update(cases)
                    .where(cases.c.id == row["case_id"])
                    .values(updated_at=utcnow())
                )
            else:
                if row["case_type"] == "standalone":
                    references = self._combined_case_references_with_connection(
                        connection, str(row["case_id"])
                    )
                    if references:
                        raise ReferencedCaseError(references)
                connection.execute(delete(cases).where(cases.c.id == row["case_id"]))
        return True

    def delete_case(
        self, case_id: str, *, confirmation_name: Optional[str] = None
    ) -> bool:
        """Delete a case atomically once none of its investigations are active."""
        with self.engine.begin() as connection:
            statement = select(cases.c.id, cases.c.title, cases.c.case_type).where(
                cases.c.id == case_id
            )
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
            self._assert_pipeline_lineage_retained_with_connection(connection, case_id)
            if stored_case["case_type"] == "standalone":
                references = self._combined_case_references_with_connection(
                    connection, case_id
                )
                if references:
                    raise ReferencedCaseError(references)
            connection.execute(delete(cases).where(cases.c.id == case_id))
        return True

    def archive_case(self, case_id: str, *, confirmation_name: Optional[str] = None) -> bool:
        """Hide a completed case from active work without erasing its lineage."""
        with self.engine.begin() as connection:
            statement = select(cases.c.id, cases.c.title).where(cases.c.id == case_id)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            stored_case = connection.execute(statement).mappings().first()
            if not stored_case:
                return False
            if confirmation_name is not None and confirmation_name != stored_case["title"]:
                raise ValueError("Case name confirmation does not match")
            active_job = connection.scalar(
                select(investigation_jobs.c.id).where(
                    investigation_jobs.c.case_id == case_id,
                    investigation_jobs.c.status.in_(ACTIVE_STATUSES),
                ).limit(1)
            )
            if active_job:
                raise ActiveInvestigationError("Cases with active investigations cannot be archived")
            connection.execute(
                update(cases).where(cases.c.id == case_id).values(
                    status="archived", updated_at=datetime.now(timezone.utc)
                )
            )
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
            "cancel_requested_at": _as_iso(row.get("cancel_requested_at")),
            "attempts": int(row["attempts"] or 0),
            "budget_seconds": (
                int(row["budget_seconds"])
                if row.get("budget_seconds") is not None
                else None
            ),
            "budget_policy_version": row.get("budget_policy_version"),
            "deadline_at": _as_iso(row.get("deadline_at")),
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
        payload["budget_seconds"] = (
            int(row["budget_seconds"])
            if row.get("budget_seconds") is not None
            else None
        )
        payload["budget_policy_version"] = row.get("budget_policy_version")
        payload["deadline_at"] = _as_iso(row.get("deadline_at"))
        payload["usernames"] = list(row["usernames"] or result.get("usernames") or [])
        payload["progress"] = dict(row["progress"] or {})
        payload["session_folder"] = result.get("session_folder", f"search_{row['id']}")
        payload["started_at"] = result.get("started_at") or payload["started_at"]
        return payload
