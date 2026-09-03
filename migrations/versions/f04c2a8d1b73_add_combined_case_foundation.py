"""add combined case foundation

Revision ID: f04c2a8d1b73
Revises: a62f1d7e4b90
Create Date: 2026-09-03 04:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "f04c2a8d1b73"
down_revision: Union[str, Sequence[str], None] = "a62f1d7e4b90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "cases",
        sa.Column(
            "case_type",
            sa.String(length=32),
            server_default="standalone",
            nullable=False,
        ),
    )
    op.add_column("cases", sa.Column("purpose", sa.Text(), nullable=True))
    op.create_check_constraint(
        "ck_cases_case_type",
        "cases",
        "case_type IN ('standalone', 'combined')",
    )
    op.create_table(
        "combined_case_members",
        sa.Column("combined_case_id", sa.String(length=36), nullable=False),
        sa.Column("source_case_id", sa.String(length=36), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("added_by", sa.String(length=200), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "combined_case_id <> source_case_id",
            name="ck_combined_case_members_distinct",
        ),
        sa.CheckConstraint("position >= 0", name="ck_combined_case_members_position"),
        sa.ForeignKeyConstraint(["combined_case_id"], ["cases.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_case_id"], ["cases.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("combined_case_id", "source_case_id"),
    )
    op.create_index(
        "ix_combined_case_members_source",
        "combined_case_members",
        ["source_case_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_combined_case_members_source", table_name="combined_case_members")
    op.drop_table("combined_case_members")
    op.drop_constraint("ck_cases_case_type", "cases", type_="check")
    op.drop_column("cases", "purpose")
    op.drop_column("cases", "case_type")
