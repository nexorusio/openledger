"""add persona claims, evidence, and review history

Revision ID: 7f0f3a91c2de
Revises: 33a669de9c7c
Create Date: 2026-08-28 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "7f0f3a91c2de"
down_revision: Union[str, Sequence[str], None] = "33a669de9c7c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

json_document = sa.JSON().with_variant(
    postgresql.JSONB(astext_type=sa.Text()), "postgresql"
)


def upgrade() -> None:
    op.create_table(
        "persona_claims",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("persona_id", sa.String(length=36), nullable=False),
        sa.Column("field_name", sa.String(length=64), nullable=False),
        sa.Column("value", json_document, nullable=False),
        sa.Column("display_value", sa.Text(), nullable=False),
        sa.Column("normalized_value", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Integer(), nullable=False),
        sa.Column(
            "review_status",
            sa.String(length=16),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("source_engine", sa.String(length=100), nullable=False),
        sa.Column("source_job_id", sa.String(length=36), nullable=True),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviewed_by", sa.String(length=200), nullable=True),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 100",
            name="ck_persona_claims_confidence",
        ),
        sa.CheckConstraint(
            "review_status IN ('pending', 'approved', 'rejected', 'uncertain')",
            name="ck_persona_claims_review_status",
        ),
        sa.ForeignKeyConstraint(["persona_id"], ["personas.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_job_id"], ["investigation_jobs.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "persona_id", "fingerprint", name="uq_persona_claim_fingerprint"
        ),
    )
    op.create_index(
        "ix_persona_claims_persona_field",
        "persona_claims",
        ["persona_id", "field_name"],
        unique=False,
    )
    op.create_table(
        "claim_evidence",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("claim_id", sa.String(length=36), nullable=False),
        sa.Column("evidence_type", sa.String(length=64), nullable=False),
        sa.Column("source_name", sa.String(length=300), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("details", json_document, nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["persona_claims.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "claim_id", "fingerprint", name="uq_claim_evidence_fingerprint"
        ),
    )
    op.create_index(
        "ix_claim_evidence_claim_id", "claim_evidence", ["claim_id"], unique=False
    )
    op.create_table(
        "claim_reviews",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("claim_id", sa.String(length=36), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("reviewer", sa.String(length=200), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "decision IN ('pending', 'approved', 'rejected', 'uncertain')",
            name="ck_claim_reviews_decision",
        ),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["persona_claims.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_claim_reviews_claim_id", "claim_reviews", ["claim_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_claim_reviews_claim_id", table_name="claim_reviews")
    op.drop_table("claim_reviews")
    op.drop_index("ix_claim_evidence_claim_id", table_name="claim_evidence")
    op.drop_table("claim_evidence")
    op.drop_index("ix_persona_claims_persona_field", table_name="persona_claims")
    op.drop_table("persona_claims")
