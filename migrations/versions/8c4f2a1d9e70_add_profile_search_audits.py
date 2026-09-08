"""add append-only profile-search audit snapshots

Revision ID: 8c4f2a1d9e70
Revises: 7ab831f4d2c0
Create Date: 2026-09-08 13:00:00.000000
"""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "8c4f2a1d9e70"
down_revision: Union[str, Sequence[str], None] = "7ab831f4d2c0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

json_document = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "profile_search_audits",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "stopped", sa.Boolean(), server_default="false", nullable=False
        ),
        sa.Column("orchestration_version", sa.Integer(), nullable=False),
        sa.Column("planned_query_count", sa.Integer(), nullable=False),
        sa.Column("executed_query_count", sa.Integer(), nullable=False),
        sa.Column("error_count", sa.Integer(), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        sa.Column("document_sha256", sa.String(length=64), nullable=False),
        sa.Column("document", json_document, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('completed', 'partial', 'failed', 'stopped')",
            name="ck_profile_search_audits_status",
        ),
        sa.CheckConstraint(
            "planned_query_count >= 0 AND executed_query_count >= 0 "
            "AND executed_query_count <= planned_query_count",
            name="ck_profile_search_audits_query_counts",
        ),
        sa.CheckConstraint(
            "error_count >= 0 AND error_count <= executed_query_count",
            name="ck_profile_search_audits_error_count",
        ),
        sa.CheckConstraint(
            "candidate_count >= 0 "
            "AND candidate_count <= planned_query_count * 10",
            name="ck_profile_search_audits_candidate_count",
        ),
        sa.CheckConstraint(
            "orchestration_version > 0",
            name="ck_profile_search_audits_orchestration_version",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["investigation_jobs.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "job_id",
            "document_sha256",
            name="uq_profile_search_audits_job_document",
        ),
    )
    op.create_index(
        "ix_profile_search_audits_job_created",
        "profile_search_audits",
        ["job_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_profile_search_audits_job_created",
        table_name="profile_search_audits",
    )
    op.drop_table("profile_search_audits")
