"""add durable case chat and claim provenance

Revision ID: a62f1d7e4b90
Revises: e91b7a4c2d6f
Create Date: 2026-08-31 03:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "a62f1d7e4b90"
down_revision: Union[str, Sequence[str], None] = "e91b7a4c2d6f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

json_document = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "case_chat_messages",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("case_id", sa.String(length=36), nullable=False),
        sa.Column("persona_id", sa.String(length=36), nullable=True),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("author", sa.String(length=200), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column(
            "research_enabled", sa.Boolean(), server_default="false", nullable=False
        ),
        sa.Column("sources", json_document, nullable=False),
        sa.Column("proposals", json_document, nullable=False),
        sa.Column("model", sa.String(length=100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "role IN ('user', 'assistant')",
            name="ck_case_chat_messages_role",
        ),
        sa.ForeignKeyConstraint(["case_id"], ["cases.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["persona_id"], ["personas.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_case_chat_messages_case_created",
        "case_chat_messages",
        ["case_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_case_chat_messages_persona",
        "case_chat_messages",
        ["persona_id"],
        unique=False,
    )
    op.add_column(
        "claim_observations",
        sa.Column("chat_message_id", sa.String(length=36), nullable=True),
    )
    op.create_foreign_key(
        "fk_claim_observations_chat_message_id",
        "claim_observations",
        "case_chat_messages",
        ["chat_message_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.drop_constraint(
        "ck_claim_observations_provenance_type",
        "claim_observations",
        type_="check",
    )
    op.create_check_constraint(
        "ck_claim_observations_provenance_type",
        "claim_observations",
        "provenance_type IN ('investigation_job', 'external_evidence', "
        "'case_chat_message')",
    )
    op.create_index(
        "ix_claim_observations_chat_message",
        "claim_observations",
        ["chat_message_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_claim_observations_chat_message", table_name="claim_observations"
    )
    op.drop_constraint(
        "ck_claim_observations_provenance_type",
        "claim_observations",
        type_="check",
    )
    # Chat messages and their observations do not exist in the prior schema.
    # Remove those lineage rows before restoring the narrower provenance check.
    op.execute(
        sa.text(
            "DELETE FROM claim_observations "
            "WHERE provenance_type = 'case_chat_message'"
        )
    )
    op.create_check_constraint(
        "ck_claim_observations_provenance_type",
        "claim_observations",
        "provenance_type IN ('investigation_job', 'external_evidence')",
    )
    op.drop_constraint(
        "fk_claim_observations_chat_message_id",
        "claim_observations",
        type_="foreignkey",
    )
    op.drop_column("claim_observations", "chat_message_id")
    op.drop_index(
        "ix_case_chat_messages_persona", table_name="case_chat_messages"
    )
    op.drop_index(
        "ix_case_chat_messages_case_created", table_name="case_chat_messages"
    )
    op.drop_table("case_chat_messages")
