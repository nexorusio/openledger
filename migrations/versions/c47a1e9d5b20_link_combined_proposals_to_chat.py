"""link combined relationship proposals to durable chat

Revision ID: c47a1e9d5b20
Revises: b91e2f6a4c8d
Create Date: 2026-09-03 07:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "c47a1e9d5b20"
down_revision: Union[str, Sequence[str], None] = "b91e2f6a4c8d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "combined_relationship_proposals",
        sa.Column("chat_message_id", sa.String(length=36), nullable=True),
    )
    op.create_foreign_key(
        "fk_combined_relationship_proposals_chat_message_id",
        "combined_relationship_proposals",
        "case_chat_messages",
        ["chat_message_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_combined_relationship_proposals_chat_message",
        "combined_relationship_proposals",
        ["chat_message_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_combined_relationship_proposals_chat_message",
        table_name="combined_relationship_proposals",
    )
    op.drop_constraint(
        "fk_combined_relationship_proposals_chat_message_id",
        "combined_relationship_proposals",
        type_="foreignkey",
    )
    op.drop_column("combined_relationship_proposals", "chat_message_id")
