"""add profile-search candidate review decisions

Revision ID: b3e9d7c4a610
Revises: 8c4f2a1d9e70
Create Date: 2026-09-08 14:45:00.000000
"""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b3e9d7c4a610"
down_revision: Union[str, Sequence[str], None] = "8c4f2a1d9e70"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "profile_search_candidate_reviews",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("audit_id", sa.String(length=36), nullable=False),
        sa.Column("candidate_id", sa.String(length=100), nullable=False),
        sa.Column("persona_id", sa.String(length=36), nullable=False),
        sa.Column("claim_id", sa.String(length=36), nullable=True),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("reviewer", sa.String(length=200), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "decision IN ('proposed', 'rejected', 'uncertain')",
            name="ck_profile_search_candidate_reviews_decision",
        ),
        sa.ForeignKeyConstraint(
            ["audit_id"], ["profile_search_audits.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["persona_claims.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["persona_id"], ["personas.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_profile_search_candidate_reviews_lookup",
        "profile_search_candidate_reviews",
        ["audit_id", "candidate_id", "persona_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_profile_search_candidate_reviews_lookup",
        table_name="profile_search_candidate_reviews",
    )
    op.drop_table("profile_search_candidate_reviews")
