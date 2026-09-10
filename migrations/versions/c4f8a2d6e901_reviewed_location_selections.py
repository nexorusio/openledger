"""Add reviewed affiliation sites and append-only coordinate decisions.

Revision ID: c4f8a2d6e901
Revises: b3e9d7c4a610
"""
# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = 'c4f8a2d6e901'
down_revision = 'b3e9d7c4a610'
branch_labels = None
depends_on = None


def upgrade():
    document = sa.JSON().with_variant(JSONB(), 'postgresql')
    op.create_table('affiliation_sites',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('case_id', sa.String(36), sa.ForeignKey('cases.id', ondelete='CASCADE'), nullable=False),
        sa.Column('persona_id', sa.String(36), sa.ForeignKey('personas.id', ondelete='CASCADE'), nullable=False),
        sa.Column('origin_claim_id', sa.String(36), sa.ForeignKey('persona_claims.id', ondelete='CASCADE'), nullable=False),
        sa.Column('source_job_id', sa.String(36), sa.ForeignKey('investigation_jobs.id', ondelete='SET NULL')),
        sa.Column('observation_key', sa.String(64), nullable=False),
        sa.Column('evidence', document, nullable=False),
        sa.Column('candidates', document, nullable=False),
        sa.Column('review_status', sa.String(16), nullable=False),
        sa.Column('revision', sa.Integer, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('case_id', 'origin_claim_id', 'observation_key', name='uq_affiliation_site_observation'),
        sa.CheckConstraint("review_status IN ('pending', 'approved', 'rejected', 'uncertain')", name='ck_affiliation_site_review'))
    op.create_table('location_selections',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('claim_id', sa.String(36), sa.ForeignKey('persona_claims.id', ondelete='CASCADE')),
        sa.Column('site_id', sa.String(36), sa.ForeignKey('affiliation_sites.id', ondelete='CASCADE')),
        sa.Column('decision', sa.String(16), nullable=False),
        sa.Column('reviewer', sa.String(200), nullable=False),
        sa.Column('reason', sa.Text, nullable=False),
        sa.Column('snapshot', document, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('(claim_id IS NULL) <> (site_id IS NULL)', name='ck_location_selection_subject'),
        sa.CheckConstraint("decision IN ('pending', 'approved', 'rejected', 'uncertain')", name='ck_location_selection_review'))
    # Legacy coordinates/evidence/approvals remain untouched. Their origin is
    # unknown until a new, independently auditable human selection is made.


def downgrade():
    # Never silently erase human coordinate/site review history on code rollback.
    # The previous application ignores these additive tables safely.
    raise RuntimeError('Use code rollback while retaining location review history; destructive schema downgrade requires a separately approved export/removal plan')
