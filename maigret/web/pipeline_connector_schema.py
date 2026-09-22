"""Additive connector delivery ledger; registered by the reviewed migration."""

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
            "WHEN NOT EXISTS (SELECT 1 FROM pipeline_case_purge_authorizations "
            "WHERE case_id = OLD.case_id) "
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
            "BEGIN IF TG_OP = 'DELETE' AND EXISTS "
            "(SELECT 1 FROM pipeline_case_purge_authorizations WHERE case_id = OLD.case_id) "
            "THEN RETURN OLD; END IF; "
            "IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'immutable connector receipt'; END IF; "
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
        event.listen(
            table,
            "after_create",
            DDL(
                f"CREATE TRIGGER {name}_update_guard BEFORE UPDATE ON {name} "
                "BEGIN SELECT RAISE(ABORT, 'immutable connector record'); END"
            ).execute_if(dialect="sqlite"),
        )
        event.listen(
            table,
            "after_create",
            DDL(
                f"CREATE TRIGGER {name}_delete_guard BEFORE DELETE ON {name} "
                "WHEN NOT EXISTS (SELECT 1 FROM pipeline_case_purge_authorizations "
                "WHERE case_id = OLD.case_id) "
                "BEGIN SELECT RAISE(ABORT, 'immutable connector record'); END"
            ).execute_if(dialect="sqlite"),
        )
        event.listen(
            table,
            "after_create",
            DDL(
                f"CREATE OR REPLACE FUNCTION {name}_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN IF TG_OP = 'DELETE' AND EXISTS "
                "(SELECT 1 FROM pipeline_case_purge_authorizations WHERE case_id = OLD.case_id) "
                "THEN RETURN OLD; END IF; "
                "RAISE EXCEPTION 'immutable connector record'; END; $$"
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
