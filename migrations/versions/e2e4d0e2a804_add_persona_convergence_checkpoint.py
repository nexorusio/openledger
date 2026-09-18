"""Add a distinct complete Persona convergence checkpoint.

Revision ID: e2e4d0e2a804
Revises: e2e3c9d1f703

Existing ``legacy_imported_at`` values record evidence import only and are not
promoted.  Every existing Persona therefore remains unconverged until the
explicit legacy-to-P2 conversion completes its evidence and review ledgers.
"""

from alembic import op
import sqlalchemy as sa


revision = "e2e4d0e2a804"
down_revision = "e2e3c9d1f703"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "pipeline_projection_state",
        sa.Column("legacy_converged_at", sa.DateTime(timezone=True)),
    )


def downgrade():
    raise RuntimeError(
        "Persona convergence state is additive and cannot be downgraded safely"
    )
