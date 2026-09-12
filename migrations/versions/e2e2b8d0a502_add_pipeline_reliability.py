"""Add durable provider budgets, connector receipts/checkpoints and read projections.

Revision ID: e2e2b8d0a502
Revises: e2e1a7c9d401

Schema definitions are frozen here. No live application schema is imported.
The upgrade is additive; all historical evidence, QC and manifests are retained.
A populated pipeline must keep this schema during application rollback.
"""

from datetime import datetime, timezone
from alembic import op
from sqlalchemy import MetaData, Column, String, Table, DDL, event, inspect, select, func, insert

revision = "e2e2b8d0a502"
down_revision = "e2e1a7c9d401"
branch_labels = None
depends_on = None

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    CheckConstraint,
)


def register_runtime_schema(metadata):
    if "pipeline_request_budgets" not in metadata.tables:
        Table(
            "pipeline_request_budgets",
            metadata,
            Column(
                "request_id",
                String(36),
                ForeignKey("pipeline_requests.id", ondelete="RESTRICT"),
                primary_key=True,
            ),
            Column("max_requests", Integer, nullable=False),
            Column("consumed", Integer, nullable=False, server_default="0"),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            CheckConstraint(
                "consumed >= 0 AND consumed <= max_requests",
                name="ck_pipeline_request_budget",
            ),
        )
        Table(
            "pipeline_provider_state",
            metadata,
            Column("provider", String(200), primary_key=True),
            Column("consecutive_failures", Integer, nullable=False, server_default="0"),
            Column("cooldown_until", DateTime(timezone=True)),
            Column("probe_until", DateTime(timezone=True)),
            Column("probe_token", String(36)),
            Column("last_outcome", String(32)),
            Column("updated_at", DateTime(timezone=True), nullable=False),
        )
    return {
        name: metadata.tables[name]
        for name in ("pipeline_request_budgets", "pipeline_provider_state")
    }


from sqlalchemy import (
    JSON,
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
    UniqueConstraint,
    event,
)
from sqlalchemy.dialects.postgresql import JSONB

IMMUTABLE_CONNECTOR_TABLES = (
    "pipeline_connector_pages",
    "pipeline_connector_record_versions",
)


def register_connector_ingestion_schema(metadata):
    if "pipeline_connector_checkpoints" in metadata.tables:
        return {
            name: table
            for name, table in metadata.tables.items()
            if name.startswith("pipeline_connector_")
        }
    document = JSON().with_variant(JSONB(), "postgresql")

    def scope():
        return [
            Column("case_id", String(36), nullable=False),
            Column("persona_id", String(36), nullable=False),
        ]

    def scoped_fk(table, field):
        return ForeignKeyConstraint(
            [field, "case_id", "persona_id"],
            [f"{table}.id", f"{table}.case_id", f"{table}.persona_id"],
            ondelete="RESTRICT",
        )

    checkpoints = Table(
        "pipeline_connector_checkpoints",
        metadata,
        Column("task_id", String(36), primary_key=True),
        *scope(),
        Column("attempt_id", String(36), nullable=False),
        Column("cursor", document),
        Column("watermark", document),
        Column("completeness", String(16), nullable=False),
        Column("page_count", Integer, nullable=False),
        Column("record_count", Integer, nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        scoped_fk("pipeline_tasks", "task_id"),
        scoped_fk("pipeline_attempts", "attempt_id"),
        CheckConstraint(
            "completeness IN ('complete','partial','truncated','unknown')",
            name="ck_connector_checkpoint_completeness",
        ),
        CheckConstraint(
            "page_count >= 0 AND record_count >= 0",
            name="ck_connector_checkpoint_counts",
        ),
    )
    pages = Table(
        "pipeline_connector_pages",
        metadata,
        Column("id", String(36), primary_key=True),
        *scope(),
        Column("task_id", String(36), nullable=False),
        Column("attempt_id", String(36), nullable=False),
        Column("page_key", String(64), nullable=False),
        Column("content_hash", String(64), nullable=False),
        Column("observation_ids", document, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        scoped_fk("pipeline_tasks", "task_id"),
        scoped_fk("pipeline_attempts", "attempt_id"),
        UniqueConstraint("task_id", "page_key", name="uq_connector_page_replay"),
    )
    versions = Table(
        "pipeline_connector_record_versions",
        metadata,
        Column("id", String(36), primary_key=True),
        *scope(),
        Column("connector_id", String(100), nullable=False),
        Column("record_id", String(200), nullable=False),
        Column("source_version", String(200), nullable=False),
        Column("supersedes_version", String(200)),
        Column("operation", String(16), nullable=False),
        Column("content_hash", String(64), nullable=False),
        Column("observation_id", String(36), nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        scoped_fk("pipeline_observations", "observation_id"),
        UniqueConstraint(
            "case_id",
            "persona_id",
            "connector_id",
            "record_id",
            "source_version",
            name="uq_connector_source_version",
        ),
        UniqueConstraint(
            "id",
            "case_id",
            "persona_id",
            "connector_id",
            "record_id",
            name="uq_connector_version_identity",
        ),
        CheckConstraint(
            "operation IN ('upsert','withdraw')", name="ck_connector_record_operation"
        ),
    )
    heads = Table(
        "pipeline_connector_record_heads",
        metadata,
        Column("case_id", String(36), primary_key=True),
        Column("persona_id", String(36), primary_key=True),
        Column("connector_id", String(100), primary_key=True),
        Column("record_id", String(200), primary_key=True),
        Column("version_id", String(36), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        ForeignKeyConstraint(
            ["version_id", "case_id", "persona_id", "connector_id", "record_id"],
            [
                "pipeline_connector_record_versions." + name
                for name in ("id", "case_id", "persona_id", "connector_id", "record_id")
            ],
            ondelete="RESTRICT",
        ),
    )
    receipts = Table(
        "pipeline_connector_receipts",
        metadata,
        Column("id", String(36), primary_key=True),
        *scope(),
        Column("connector_id", String(100), nullable=False),
        Column("idempotency_key", String(200), nullable=False),
        Column("content_hash", String(64), nullable=False),
        Column("payload", document, nullable=False),
        Column(
            "job_id",
            String(36),
            ForeignKey("investigation_jobs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        Column("request_id", String(36), nullable=False),
        Column("status", String(16), nullable=False),
        Column("error_code", String(100)),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        scoped_fk("pipeline_requests", "request_id"),
        UniqueConstraint(
            "connector_id",
            "case_id",
            "persona_id",
            "idempotency_key",
            name="uq_connector_receipt_replay",
        ),
        CheckConstraint(
            "status IN ('queued','running','completed','failed')",
            name="ck_connector_receipt_status",
        ),
    )
    Index("ix_connector_receipts_job", receipts.c.job_id)
    Index("ix_connector_version_observation", versions.c.observation_id)
    receipt_facts = (
        "id",
        "case_id",
        "persona_id",
        "connector_id",
        "idempotency_key",
        "content_hash",
        "payload",
        "job_id",
        "request_id",
        "created_at",
    )
    changed_sqlite = " OR ".join(
        f"NEW.{name} IS NOT OLD.{name}" for name in receipt_facts
    )
    event.listen(
        receipts,
        "after_create",
        DDL(
            "CREATE TRIGGER pipeline_connector_receipt_facts_guard BEFORE UPDATE ON pipeline_connector_receipts "
            f"WHEN {changed_sqlite} BEGIN SELECT RAISE(ABORT, 'immutable connector receipt'); END"
        ).execute_if(dialect="sqlite"),
    )
    event.listen(
        receipts,
        "after_create",
        DDL(
            "CREATE TRIGGER pipeline_connector_receipt_delete_guard BEFORE DELETE ON pipeline_connector_receipts "
            "BEGIN SELECT RAISE(ABORT, 'immutable connector receipt'); END"
        ).execute_if(dialect="sqlite"),
    )
    changed_postgres = " OR ".join(
        f"NEW.{name} IS DISTINCT FROM OLD.{name}" for name in receipt_facts
    )
    event.listen(
        receipts,
        "after_create",
        DDL(
            "CREATE OR REPLACE FUNCTION pipeline_connector_receipt_facts_guard() RETURNS trigger LANGUAGE plpgsql AS $$ "
            "BEGIN IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'immutable connector receipt'; END IF; "
            f"IF {changed_postgres} THEN RAISE EXCEPTION 'immutable connector receipt'; END IF; RETURN NEW; END; $$"
        ).execute_if(dialect="postgresql"),
    )
    event.listen(
        receipts,
        "after_create",
        DDL(
            "CREATE TRIGGER pipeline_connector_receipt_facts_guard BEFORE UPDATE OR DELETE ON pipeline_connector_receipts "
            "FOR EACH ROW EXECUTE FUNCTION pipeline_connector_receipt_facts_guard()"
        ).execute_if(dialect="postgresql"),
    )
    event.listen(
        receipts,
        "after_drop",
        DDL(
            "DROP FUNCTION IF EXISTS pipeline_connector_receipt_facts_guard()"
        ).execute_if(dialect="postgresql"),
    )
    for name in IMMUTABLE_CONNECTOR_TABLES:
        table = metadata.tables[name]
        for operation in ("UPDATE", "DELETE"):
            event.listen(
                table,
                "after_create",
                DDL(
                    f"CREATE TRIGGER {name}_{operation.lower()}_guard BEFORE {operation} ON {name} "
                    "BEGIN SELECT RAISE(ABORT, 'immutable connector record'); END"
                ).execute_if(dialect="sqlite"),
            )
        event.listen(
            table,
            "after_create",
            DDL(
                f"CREATE OR REPLACE FUNCTION {name}_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'immutable connector record'; END; $$"
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
    return {
        table.name: table for table in (checkpoints, pages, versions, heads, receipts)
    }


from sqlalchemy import JSON, Column, DateTime, ForeignKey, ForeignKeyConstraint, Integer, String, Table
from sqlalchemy.dialects.postgresql import JSONB


def register_projection_schema(metadata):
    if "pipeline_projection_state" in metadata.tables:
        return {name: metadata.tables[name] for name in (
            "pipeline_projection_state", "pipeline_group_summaries"
        )}
    document = JSON().with_variant(JSONB(), "postgresql")
    state = Table(
        "pipeline_projection_state", metadata,
        Column("persona_id", String(36), ForeignKey("personas.id", ondelete="RESTRICT"), primary_key=True),
        Column("case_id", String(36), ForeignKey("cases.id", ondelete="RESTRICT"), nullable=False),
        Column("evidence_revision", Integer, nullable=False, server_default="0"),
        Column("projected_revision", Integer, nullable=False, server_default="0"),
        Column("legacy_imported_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    summaries = Table(
        "pipeline_group_summaries", metadata,
        Column("group_id", String(36), primary_key=True),
        Column("case_id", String(36), nullable=False),
        Column("persona_id", String(36), nullable=False),
        Column("normalized", document, nullable=False),
        Column("assessment", document),
        Column("assessment_id", String(36)),
        Column("observations", document, nullable=False),
        Column("observation_count", Integer, nullable=False),
        Column("revision_count", Integer, nullable=False),
        Column("latest_grouping_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        ForeignKeyConstraint(
            ["group_id", "case_id", "persona_id"],
            ["pipeline_groups.id", "pipeline_groups.case_id", "pipeline_groups.persona_id"],
            ondelete="RESTRICT",
        ),
    )
    return {table.name: table for table in (state, summaries)}


def _metadata():
    metadata = MetaData()
    for name in ("cases", "personas", "investigation_jobs", "pipeline_requests",
                 "pipeline_tasks", "pipeline_attempts", "pipeline_observations", "pipeline_groups"):
        Table(name, metadata, Column("id", String(36), primary_key=True),
              Column("case_id", String(36)), Column("persona_id", String(36)))
    existing = set(metadata.tables)
    register_runtime_schema(metadata)
    register_connector_ingestion_schema(metadata)
    register_projection_schema(metadata)
    tables = [table for table in metadata.sorted_tables if table.name not in existing]
    return metadata, tables


def upgrade():
    connection = op.get_bind()
    metadata, tables = _metadata()
    for table in tables:
        table.create(connection, checkfirst=False)
    # Existing evidence is visibly stale until explicitly rebuilt. No HTTP read
    # writes a summary, and no pre-existing curated manifest is regenerated.
    observations = metadata.tables["pipeline_observations"]
    state = metadata.tables["pipeline_projection_state"]
    scopes = connection.execute(select(observations.c.case_id, observations.c.persona_id).distinct())
    now = datetime.now(timezone.utc)
    for scope in scopes:
        connection.execute(insert(state).values(
            case_id=scope.case_id, persona_id=scope.persona_id,
            evidence_revision=1, projected_revision=0, updated_at=now,
        ))


def downgrade():
    connection = op.get_bind()
    # Preserve every accepted receipt/checkpoint and every old evidence/version
    # record; rollback is a reviewed compatible application image, never deletion.
    for name in inspect(connection).get_table_names():
        if name.startswith("pipeline_"):
            table = Table(name, MetaData(), autoload_with=connection)
            if connection.scalar(select(func.count()).select_from(table)):
                raise RuntimeError("Pipeline records exist; preserve expanded schema during code rollback")
    _, tables = _metadata()
    for table in reversed(tables):
        table.drop(connection)
