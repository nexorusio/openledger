"""add governed combined-case AI insights

Revision ID: b91e2f6a4c8d
Revises: f04c2a8d1b73
Create Date: 2026-09-03 09:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "b91e2f6a4c8d"
down_revision: Union[str, Sequence[str], None] = "f04c2a8d1b73"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

json_document = sa.JSON().with_variant(
    postgresql.JSONB(astext_type=sa.Text()), "postgresql"
)


def upgrade() -> None:
    op.create_table(
        "combined_analysis_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("combined_case_id", sa.String(length=36), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("snapshot_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("model", sa.String(length=100), nullable=True),
        sa.Column(
            "web_search_enabled",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.Column("executive_summary", sa.Text(), nullable=True),
        sa.Column("key_findings", json_document, nullable=False),
        sa.Column("contradictions", json_document, nullable=False),
        sa.Column("information_gaps", json_document, nullable=False),
        sa.Column("next_steps", json_document, nullable=False),
        sa.Column("sources", json_document, nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('processing', 'completed', 'unavailable', 'failed', "
            "'cancelled')",
            name="ck_combined_analysis_runs_status",
        ),
        sa.ForeignKeyConstraint(["combined_case_id"], ["cases.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["job_id"], ["investigation_jobs.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id"),
    )
    op.create_index(
        "ix_combined_analysis_runs_case_created",
        "combined_analysis_runs",
        ["combined_case_id", "created_at"],
        unique=False,
    )
    op.create_table(
        "combined_relationship_proposals",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("analysis_run_id", sa.String(length=36), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("relationship_type", sa.String(length=64), nullable=False),
        sa.Column("subject_ref", sa.String(length=100), nullable=False),
        sa.Column("subject_entity", json_document, nullable=False),
        sa.Column("object_ref", sa.String(length=100), nullable=False),
        sa.Column("object_entity", json_document, nullable=False),
        sa.Column("explanation", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Integer(), nullable=False),
        sa.Column("evidence", json_document, nullable=False),
        sa.Column("contradictory_evidence", json_document, nullable=False),
        sa.Column("limitations", json_document, nullable=False),
        sa.Column(
            "review_status",
            sa.String(length=16),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviewed_by", sa.String(length=200), nullable=True),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 85",
            name="ck_combined_relationship_proposals_confidence",
        ),
        sa.CheckConstraint(
            "review_status IN ('pending', 'approved', 'rejected', 'uncertain')",
            name="ck_combined_relationship_proposals_review_status",
        ),
        sa.ForeignKeyConstraint(
            ["analysis_run_id"], ["combined_analysis_runs.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_combined_relationship_proposals_run_status",
        "combined_relationship_proposals",
        ["analysis_run_id", "review_status"],
        unique=False,
    )
    op.create_table(
        "combined_relationship_reviews",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("proposal_id", sa.String(length=36), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("reviewer", sa.String(length=200), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "decision IN ('approved', 'rejected', 'uncertain')",
            name="ck_combined_relationship_reviews_decision",
        ),
        sa.ForeignKeyConstraint(
            ["proposal_id"],
            ["combined_relationship_proposals.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_combined_relationship_reviews_proposal",
        "combined_relationship_reviews",
        ["proposal_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_combined_relationship_reviews_proposal",
        table_name="combined_relationship_reviews",
    )
    op.drop_table("combined_relationship_reviews")
    op.drop_index(
        "ix_combined_relationship_proposals_run_status",
        table_name="combined_relationship_proposals",
    )
    op.drop_table("combined_relationship_proposals")
    op.drop_index(
        "ix_combined_analysis_runs_case_created",
        table_name="combined_analysis_runs",
    )
    op.drop_table("combined_analysis_runs")
