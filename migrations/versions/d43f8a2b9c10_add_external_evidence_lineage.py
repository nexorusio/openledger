"""add governed external evidence and claim lineage

Revision ID: d43f8a2b9c10
Revises: c18e7f42a9bd
Create Date: 2026-08-30 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "d43f8a2b9c10"
down_revision: Union[str, Sequence[str], None] = "c18e7f42a9bd"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

json_document = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "data_sources",
        sa.Column("id", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("authority", sa.String(length=200), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("default_classification", sa.String(length=64), nullable=False),
        sa.Column("handling_defaults", json_document, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "schema_version > 0", name="ck_data_sources_schema_version"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "query_receipts",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("case_id", sa.String(length=36), nullable=False),
        sa.Column("source_id", sa.String(length=100), nullable=False),
        sa.Column("requested_by", sa.String(length=200), nullable=False),
        sa.Column("purpose", sa.Text(), nullable=False),
        sa.Column("query_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("query_document", json_document, nullable=False),
        sa.Column("policy_context", json_document, nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("result_count", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "result_count IS NULL OR result_count >= 0",
            name="ck_query_receipts_result_count",
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed', 'cancelled')",
            name="ck_query_receipts_status",
        ),
        sa.ForeignKeyConstraint(["case_id"], ["cases.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_id"], ["data_sources.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_query_receipts_case_created",
        "query_receipts",
        ["case_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_query_receipts_source_status",
        "query_receipts",
        ["source_id", "status"],
        unique=False,
    )
    op.create_table(
        "external_evidence_records",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("case_id", sa.String(length=36), nullable=False),
        sa.Column("source_id", sa.String(length=100), nullable=False),
        sa.Column("source_record_id", sa.String(length=500), nullable=False),
        sa.Column("source_version", sa.String(length=200), nullable=False),
        sa.Column("record_type", sa.String(length=100), nullable=False),
        sa.Column("content_hash", sa.String(length=71), nullable=False),
        sa.Column("locator", json_document, nullable=False),
        sa.Column("preview", sa.Text(), nullable=False),
        sa.Column("attributes", json_document, nullable=False),
        sa.Column("handling", json_document, nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["case_id"], ["cases.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_id"], ["data_sources.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "case_id",
            "source_id",
            "source_record_id",
            "source_version",
            name="uq_external_evidence_source_version",
        ),
    )
    op.create_index(
        "ix_external_evidence_case_source",
        "external_evidence_records",
        ["case_id", "source_id"],
        unique=False,
    )
    op.create_index(
        "ix_external_evidence_content_hash",
        "external_evidence_records",
        ["content_hash"],
        unique=False,
    )
    op.create_table(
        "external_evidence_receipts",
        sa.Column("evidence_id", sa.String(length=36), nullable=False),
        sa.Column("query_receipt_id", sa.String(length=36), nullable=False),
        sa.Column("attached_by", sa.String(length=200), nullable=False),
        sa.Column("attached_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["evidence_id"], ["external_evidence_records.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["query_receipt_id"], ["query_receipts.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("evidence_id", "query_receipt_id"),
    )
    op.create_index(
        "ix_external_evidence_receipts_receipt",
        "external_evidence_receipts",
        ["query_receipt_id"],
        unique=False,
    )
    op.create_table(
        "claim_observations",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("claim_id", sa.String(length=36), nullable=False),
        sa.Column("provenance_type", sa.String(length=32), nullable=False),
        sa.Column("provenance_id", sa.String(length=500), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=True),
        sa.Column("external_evidence_id", sa.String(length=36), nullable=True),
        sa.Column("source_engine", sa.String(length=100), nullable=False),
        sa.Column("source_record_id", sa.String(length=500), nullable=True),
        sa.Column("confidence", sa.Integer(), nullable=True),
        sa.Column("native_status", sa.String(length=100), nullable=False),
        sa.Column("details", json_document, nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 100)",
            name="ck_claim_observations_confidence",
        ),
        sa.CheckConstraint(
            "provenance_type IN ('investigation_job', 'external_evidence')",
            name="ck_claim_observations_provenance_type",
        ),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["persona_claims.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["external_evidence_id"],
            ["external_evidence_records.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["investigation_jobs.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "claim_id", "fingerprint", name="uq_claim_observation_fingerprint"
        ),
    )
    op.create_index(
        "ix_claim_observations_claim_observed",
        "claim_observations",
        ["claim_id", "observed_at"],
        unique=False,
    )
    op.create_index(
        "ix_claim_observations_provenance",
        "claim_observations",
        ["provenance_type", "provenance_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_claim_observations_provenance", table_name="claim_observations"
    )
    op.drop_index(
        "ix_claim_observations_claim_observed", table_name="claim_observations"
    )
    op.drop_table("claim_observations")
    op.drop_index(
        "ix_external_evidence_receipts_receipt",
        table_name="external_evidence_receipts",
    )
    op.drop_table("external_evidence_receipts")
    op.drop_index(
        "ix_external_evidence_content_hash", table_name="external_evidence_records"
    )
    op.drop_index(
        "ix_external_evidence_case_source", table_name="external_evidence_records"
    )
    op.drop_table("external_evidence_records")
    op.drop_index(
        "ix_query_receipts_source_status", table_name="query_receipts"
    )
    op.drop_index("ix_query_receipts_case_created", table_name="query_receipts")
    op.drop_table("query_receipts")
    op.drop_table("data_sources")
