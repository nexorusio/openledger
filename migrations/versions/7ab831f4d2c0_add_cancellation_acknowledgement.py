"""add durable cancellation acknowledgement

Revision ID: 7ab831f4d2c0
Revises: 62c42b4e9a10
Create Date: 2026-09-07 10:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "7ab831f4d2c0"
down_revision: Union[str, Sequence[str], None] = "62c42b4e9a10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "investigation_jobs",
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE investigation_jobs SET cancel_requested_at = updated_at "
        "WHERE cancel_requested = true AND cancel_requested_at IS NULL"
    )


def downgrade() -> None:
    op.drop_column("investigation_jobs", "cancel_requested_at")
