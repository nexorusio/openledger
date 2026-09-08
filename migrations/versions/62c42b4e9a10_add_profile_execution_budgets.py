"""add governed profile-discovery execution budgets

Revision ID: 62c42b4e9a10
Revises: c47a1e9d5b20
Create Date: 2026-09-07 09:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "62c42b4e9a10"
down_revision: Union[str, Sequence[str], None] = "c47a1e9d5b20"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "investigation_jobs",
        sa.Column("budget_seconds", sa.Integer(), nullable=True),
    )
    op.add_column(
        "investigation_jobs",
        sa.Column("budget_policy_version", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "investigation_jobs",
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_investigation_jobs_budget_seconds",
        "investigation_jobs",
        "budget_seconds IS NULL OR budget_seconds > 0",
    )
    op.drop_constraint(
        "ck_investigation_jobs_status",
        "investigation_jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_investigation_jobs_status",
        "investigation_jobs",
        "status IN ('queued', 'running', 'cancel_requested', 'completed', "
        "'failed', 'cancelled', 'interrupted', 'budget_exhausted')",
    )


def downgrade() -> None:
    op.execute(
        "UPDATE investigation_jobs SET status = 'failed' "
        "WHERE status = 'budget_exhausted'"
    )
    op.drop_constraint(
        "ck_investigation_jobs_status",
        "investigation_jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_investigation_jobs_status",
        "investigation_jobs",
        "status IN ('queued', 'running', 'cancel_requested', 'completed', "
        "'failed', 'cancelled', 'interrupted')",
    )
    op.drop_constraint(
        "ck_investigation_jobs_budget_seconds",
        "investigation_jobs",
        type_="check",
    )
    op.drop_column("investigation_jobs", "deadline_at")
    op.drop_column("investigation_jobs", "budget_policy_version")
    op.drop_column("investigation_jobs", "budget_seconds")
