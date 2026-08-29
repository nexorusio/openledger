"""add reviewed claim coordinates

Revision ID: c18e7f42a9bd
Revises: 7f0f3a91c2de
Create Date: 2026-08-28 15:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "c18e7f42a9bd"
down_revision: Union[str, Sequence[str], None] = "7f0f3a91c2de"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("persona_claims", sa.Column("latitude", sa.Float(), nullable=True))
    op.add_column("persona_claims", sa.Column("longitude", sa.Float(), nullable=True))
    op.create_index(
        "ix_persona_claims_relationship_projection",
        "persona_claims",
        ["review_status", "field_name", "normalized_value"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_persona_claims_relationship_projection",
        table_name="persona_claims",
    )
    op.drop_column("persona_claims", "longitude")
    op.drop_column("persona_claims", "latitude")
