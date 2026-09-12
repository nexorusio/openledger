"""Additive P2 end-to-end pipeline schema; no application imports or legacy fallback.

Call register_pipeline_schema with CaseStore's metadata. All historical facts and
submitted manifests are append-only. Mutable execution and publication pointers
are deliberately separate from those facts.
"""

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    DDL,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    event,
)
from sqlalchemy.dialects.postgresql import JSONB

PIPELINE_ID = "p2-e2e-v1"
SCHEMA_REVISION = "e2e2b8d0a502"
PREFIX = "pipeline_"
IMMUTABLE_TABLES = (
    "pipeline_observations",
    "pipeline_groups",
    "pipeline_group_observations",
    "pipeline_operator_decisions",
    "pipeline_persona_versions",
    "pipeline_version_evidence",
    "pipeline_assessments",
    "pipeline_qc_decisions",
    "pipeline_requirement_resolutions",
    "pipeline_group_revisions",
)


def register_pipeline_schema(metadata):
    if "pipeline_requests" in metadata.tables:
        return {
            name: table
            for name, table in metadata.tables.items()
            if name.startswith(PREFIX)
        }
    document = JSON().with_variant(JSONB(), "postgresql")

    def identity():
        return Column("id", String(36), primary_key=True)

    def scope():
        return [
            Column(
                "case_id",
                String(36),
                ForeignKey("cases.id", ondelete="RESTRICT"),
                nullable=False,
            ),
            Column(
                "persona_id",
                String(36),
                ForeignKey("personas.id", ondelete="RESTRICT"),
                nullable=False,
            ),
        ]

    def timestamp():
        return Column("created_at", DateTime(timezone=True), nullable=False)

    def scoped_fk(table, field):
        return ForeignKeyConstraint(
            [field, "case_id", "persona_id"],
            [f"{table}.id", f"{table}.case_id", f"{table}.persona_id"],
            ondelete="RESTRICT",
        )

    requests = Table(
        "pipeline_requests",
        metadata,
        identity(),
        *scope(),
        Column("pipeline_id", String(32), nullable=False),
        Column(
            "job_id",
            String(36),
            ForeignKey("investigation_jobs.id", ondelete="RESTRICT"),
        ),
        Column("parent_request_id", String(36)),
        Column("actor", String(200), nullable=False),
        Column("inputs", document, nullable=False),
        Column("plan", document, nullable=False),
        Column("plan_hash", String(64), nullable=False),
        Column("idempotency_key", String(200), nullable=False),
        Column("status", String(24), nullable=False),
        Column("depth", Integer, nullable=False, server_default="0"),
        timestamp(),
        UniqueConstraint(
            "id", "case_id", "persona_id", name="uq_pipeline_request_scope"
        ),
        UniqueConstraint(
            "case_id",
            "persona_id",
            "idempotency_key",
            name="uq_pipeline_request_replay",
        ),
        scoped_fk("pipeline_requests", "parent_request_id"),
        CheckConstraint("pipeline_id = 'p2-e2e-v1'", name="ck_pipeline_id"),
        CheckConstraint("depth >= 0 AND depth <= 8", name="ck_pipeline_request_depth"),
    )
    tasks = Table(
        "pipeline_tasks",
        metadata,
        identity(),
        *scope(),
        Column("request_id", String(36), nullable=False),
        Column("task_key", String(200), nullable=False),
        Column("engine", String(100), nullable=False),
        Column("platform", String(100)),
        Column("input", document, nullable=False),
        Column("spec", document, nullable=False),
        Column("availability", String(24), nullable=False),
        Column("reason", Text),
        Column("status", String(24), nullable=False),
        Column("outcome", String(32)),
        Column("attempt_count", Integer, nullable=False, server_default="0"),
        Column("retry_limit", Integer, nullable=False, server_default="0"),
        Column("active_attempt_id", String(36)),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        timestamp(),
        scoped_fk("pipeline_requests", "request_id"),
        UniqueConstraint("request_id", "task_key", name="uq_pipeline_task_key"),
        UniqueConstraint("id", "case_id", "persona_id", name="uq_pipeline_task_scope"),
        CheckConstraint(
            "status IN ('planned','running','completed','cancelled','not_executed')",
            name="ck_pipeline_task_status",
        ),
    )
    attempts = Table(
        "pipeline_attempts",
        metadata,
        identity(),
        *scope(),
        Column("task_id", String(36), nullable=False),
        Column("number", Integer, nullable=False),
        Column("worker_id", String(200), nullable=False),
        Column("status", String(24), nullable=False),
        Column("outcome", String(32)),
        Column("error", Text),
        Column("finished_at", DateTime(timezone=True)),
        timestamp(),
        scoped_fk("pipeline_tasks", "task_id"),
        UniqueConstraint("task_id", "number", name="uq_pipeline_attempt_number"),
        UniqueConstraint(
            "id", "case_id", "persona_id", name="uq_pipeline_attempt_scope"
        ),
    )
    observations = Table(
        "pipeline_observations",
        metadata,
        identity(),
        *scope(),
        Column("request_id", String(36), nullable=False),
        Column("task_id", String(36), nullable=False),
        Column("attempt_id", String(36), nullable=False),
        Column("observation_key", String(200), nullable=False),
        Column("engine", String(100), nullable=False),
        Column("outcome", String(32), nullable=False),
        Column("source_url", Text),
        Column("canonical_url", Text),
        Column("origin_family", Text),
        Column("content_hash", String(64), nullable=False),
        Column("retained", Boolean, nullable=False),
        Column(
            "original_observation_id",
            String(36),
            ForeignKey("pipeline_observations.id", ondelete="RESTRICT"),
        ),
        Column("artifact_ref", Text),
        Column("payload", document, nullable=False),
        timestamp(),
        scoped_fk("pipeline_requests", "request_id"),
        scoped_fk("pipeline_tasks", "task_id"),
        scoped_fk("pipeline_attempts", "attempt_id"),
        UniqueConstraint(
            "attempt_id", "observation_key", name="uq_pipeline_observation_replay"
        ),
        UniqueConstraint(
            "id", "case_id", "persona_id", name="uq_pipeline_observation_scope"
        ),
    )
    groups = Table(
        "pipeline_groups",
        metadata,
        identity(),
        *scope(),
        Column("kind", String(16), nullable=False),
        Column("canonical_key", String(64), nullable=False),
        Column("normalized", document, nullable=False),
        timestamp(),
        UniqueConstraint(
            "case_id",
            "persona_id",
            "kind",
            "canonical_key",
            name="uq_pipeline_group_identity",
        ),
        UniqueConstraint("id", "case_id", "persona_id", name="uq_pipeline_group_scope"),
        CheckConstraint("kind IN ('account','claim')", name="ck_pipeline_group_kind"),
    )
    memberships = Table(
        "pipeline_group_observations",
        metadata,
        *scope(),
        Column("group_id", String(36), primary_key=True),
        Column("observation_id", String(36), primary_key=True),
        timestamp(),
        scoped_fk("pipeline_groups", "group_id"),
        scoped_fk("pipeline_observations", "observation_id"),
    )
    assessments = Table(
        "pipeline_assessments",
        metadata,
        identity(),
        *scope(),
        Column("group_id", String(36), nullable=False),
        Column("evidence_hash", String(64), nullable=False),
        Column("document", document, nullable=False),
        timestamp(),
        scoped_fk("pipeline_groups", "group_id"),
        UniqueConstraint(
            "group_id", "evidence_hash", name="uq_pipeline_assessment_snapshot"
        ),
    )
    revisions = Table(
        "pipeline_group_revisions",
        metadata,
        identity(),
        *scope(),
        Column("group_id", String(36), nullable=False),
        Column("action", String(32), nullable=False),
        Column("actor", String(200), nullable=False),
        Column("reason", Text, nullable=False),
        Column("details", document, nullable=False),
        timestamp(),
        scoped_fk("pipeline_groups", "group_id"),
    )
    decisions = Table(
        "pipeline_operator_decisions",
        metadata,
        identity(),
        *scope(),
        Column("group_id", String(36), nullable=False),
        Column("sequence", Integer, nullable=False),
        Column("decision", String(16), nullable=False),
        Column("actor", String(200), nullable=False),
        Column("reason", Text, nullable=False),
        Column("details", document, nullable=False),
        timestamp(),
        scoped_fk("pipeline_groups", "group_id"),
        UniqueConstraint("group_id", "sequence", name="uq_pipeline_decision_sequence"),
        CheckConstraint(
            "decision IN ('include','exclude','reject','unresolved')",
            name="ck_pipeline_operator_decision",
        ),
    )
    versions = Table(
        "pipeline_persona_versions",
        metadata,
        identity(),
        *scope(),
        Column("sequence", Integer, nullable=False),
        Column("parent_version_id", String(36)),
        Column("actor", String(200), nullable=False),
        Column("workspace_revision", Integer, nullable=False),
        Column("content_hash", String(64), nullable=False),
        Column("manifest", document, nullable=False),
        timestamp(),
        scoped_fk("pipeline_persona_versions", "parent_version_id"),
        UniqueConstraint("persona_id", "sequence", name="uq_pipeline_version_sequence"),
        UniqueConstraint(
            "id", "case_id", "persona_id", name="uq_pipeline_version_scope"
        ),
    )
    evidence = Table(
        "pipeline_version_evidence",
        metadata,
        *scope(),
        Column("version_id", String(36), primary_key=True),
        Column("observation_id", String(36), primary_key=True),
        scoped_fk("pipeline_persona_versions", "version_id"),
        scoped_fk("pipeline_observations", "observation_id"),
    )
    qc = Table(
        "pipeline_qc_decisions",
        metadata,
        identity(),
        *scope(),
        Column("version_id", String(36), nullable=False),
        Column("decision", String(24), nullable=False),
        Column("actor", String(200), nullable=False),
        Column("expected_hash", String(64), nullable=False),
        Column("findings", document, nullable=False),
        Column("waivers", document, nullable=False),
        timestamp(),
        scoped_fk("pipeline_persona_versions", "version_id"),
        UniqueConstraint("version_id", name="uq_pipeline_qc_version"),
        UniqueConstraint("id", "case_id", "persona_id", name="uq_pipeline_qc_scope"),
        CheckConstraint(
            "decision IN ('approved','changes_required')",
            name="ck_pipeline_qc_decision",
        ),
    )
    requirements = Table(
        "pipeline_research_requirements",
        metadata,
        identity(),
        *scope(),
        Column("version_id", String(36), nullable=False),
        Column("qc_id", String(36), nullable=False),
        Column("fingerprint", String(64), nullable=False),
        Column("spec", document, nullable=False),
        Column("status", String(24), nullable=False),
        timestamp(),
        scoped_fk("pipeline_persona_versions", "version_id"),
        scoped_fk("pipeline_qc_decisions", "qc_id"),
        UniqueConstraint(
            "qc_id", "fingerprint", name="uq_pipeline_requirement_duplicate"
        ),
        UniqueConstraint(
            "id", "case_id", "persona_id", name="uq_pipeline_requirement_scope"
        ),
    )
    links = Table(
        "pipeline_requirement_requests",
        metadata,
        *scope(),
        Column("requirement_id", String(36), primary_key=True),
        Column("request_id", String(36), primary_key=True),
        scoped_fk("pipeline_research_requirements", "requirement_id"),
        scoped_fk("pipeline_requests", "request_id"),
    )
    resolutions = Table(
        "pipeline_requirement_resolutions",
        metadata,
        identity(),
        *scope(),
        Column("requirement_id", String(36), nullable=False),
        Column("disposition", String(24), nullable=False),
        Column("actor", String(200), nullable=False),
        Column("reason", Text, nullable=False),
        Column("evidence_ids", document, nullable=False),
        timestamp(),
        scoped_fk("pipeline_research_requirements", "requirement_id"),
    )
    state = Table(
        "pipeline_persona_state",
        metadata,
        Column(
            "persona_id",
            String(36),
            ForeignKey("personas.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        Column(
            "case_id",
            String(36),
            ForeignKey("cases.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        Column("revision", Integer, nullable=False),
        Column("final_version_id", String(36)),
        Column("final_status", String(24)),
        Column("review_needed", Boolean, nullable=False),
        Column("withdrawal_reason", Text),
        Column("withdrawn_by", String(200)),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        scoped_fk("pipeline_persona_versions", "final_version_id"),
    )
    tables = [
        requests,
        tasks,
        attempts,
        observations,
        groups,
        memberships,
        assessments,
        revisions,
        decisions,
        versions,
        evidence,
        qc,
        requirements,
        links,
        resolutions,
        state,
    ]
    for table in tables:
        if "created_at" in table.c and "persona_id" in table.c:
            Index(
                f"ix_{table.name}_persona_time", table.c.persona_id, table.c.created_at
            )
    Index("ix_pipeline_tasks_request", tasks.c.request_id, tasks.c.status)
    Index("ix_pipeline_observations_attempt", observations.c.attempt_id)
    Index("ix_pipeline_memberships_observation", memberships.c.observation_id)
    # Database-enforced immutability protects history from old mutation routes.
    for name in IMMUTABLE_TABLES:
        table = metadata.tables[name]
        for operation in ("UPDATE", "DELETE"):
            event.listen(
                table,
                "after_create",
                DDL(
                    f"CREATE TRIGGER {name}_{operation.lower()}_guard BEFORE {operation} ON {name} "
                    f"BEGIN SELECT RAISE(ABORT, 'immutable pipeline record'); END"
                ).execute_if(dialect="sqlite"),
            )
        event.listen(
            table,
            "after_create",
            DDL(
                f"CREATE OR REPLACE FUNCTION {name}_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'immutable pipeline record'; END; $$"
            ).execute_if(dialect="postgresql"),
        )
        event.listen(
            table,
            "after_create",
            DDL(
                f"CREATE TRIGGER {name}_immutable_guard BEFORE UPDATE OR DELETE ON {name} "
                f"FOR EACH ROW EXECUTE FUNCTION {name}_immutable()"
            ).execute_if(dialect="postgresql"),
        )
        event.listen(
            table,
            "after_drop",
            DDL(f"DROP FUNCTION IF EXISTS {name}_immutable()").execute_if(
                dialect="postgresql"
            ),
        )
    from maigret.web.pipeline_runtime_schema import register_runtime_schema

    extra = register_runtime_schema(metadata)
    return {**{table.name: table for table in tables}, **extra}
