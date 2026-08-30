"""backfill retained claim provenance into observations

Revision ID: e91b7a4c2d6f
Revises: d43f8a2b9c10
Create Date: 2026-08-30 15:00:00.000000
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "e91b7a4c2d6f"
down_revision: Union[str, Sequence[str], None] = "d43f8a2b9c10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

json_document = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
_BACKFILL_NAMESPACE = uuid.UUID("21f664a5-5ee8-423f-aa85-aa3380f3a481")

persona_claims = sa.table(
    "persona_claims",
    sa.column("id", sa.String(length=36)),
    sa.column("source_job_id", sa.String(length=36)),
    sa.column("source_engine", sa.String(length=100)),
    sa.column("confidence", sa.Integer()),
    sa.column("fingerprint", sa.String(length=64)),
    sa.column("last_seen_at", sa.DateTime(timezone=True)),
)

claim_observations = sa.table(
    "claim_observations",
    sa.column("id", sa.String(length=36)),
    sa.column("claim_id", sa.String(length=36)),
    sa.column("provenance_type", sa.String(length=32)),
    sa.column("provenance_id", sa.String(length=500)),
    sa.column("job_id", sa.String(length=36)),
    sa.column("external_evidence_id", sa.String(length=36)),
    sa.column("source_engine", sa.String(length=100)),
    sa.column("source_record_id", sa.String(length=500)),
    sa.column("confidence", sa.Integer()),
    sa.column("native_status", sa.String(length=100)),
    sa.column("details", json_document),
    sa.column("fingerprint", sa.String(length=64)),
    sa.column("observed_at", sa.DateTime(timezone=True)),
)


def _stable_fingerprint(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _backfill_claim_observations(connection: sa.Connection) -> None:
    retained_claims = list(
        connection.execute(
            sa.select(persona_claims).where(
                persona_claims.c.source_job_id.is_not(None)
            )
        ).mappings()
    )
    existing_provenance = set(
        connection.execute(
            sa.select(
                claim_observations.c.claim_id,
                claim_observations.c.provenance_id,
            ).where(
                claim_observations.c.provenance_type == "investigation_job"
            )
        ).tuples()
    )
    for claim in retained_claims:
        provenance_key = (claim["id"], claim["source_job_id"])
        if provenance_key in existing_provenance:
            continue
        details = {
            "backfilled": True,
            "claim_fingerprint": claim["fingerprint"],
        }
        fingerprint = _stable_fingerprint(
            {
                "provenance_type": "investigation_job",
                "provenance_id": claim["source_job_id"],
                "source_engine": claim["source_engine"],
                "source_record_id": None,
                "confidence": claim["confidence"],
                "native_status": "historical_claim",
                "details": details,
            }
        )
        observation_id = str(
            uuid.uuid5(
                _BACKFILL_NAMESPACE,
                f"{claim['id']}:{fingerprint}",
            )
        )
        connection.execute(
            sa.insert(claim_observations).values(
                id=observation_id,
                claim_id=claim["id"],
                provenance_type="investigation_job",
                provenance_id=claim["source_job_id"],
                job_id=claim["source_job_id"],
                external_evidence_id=None,
                source_engine=claim["source_engine"],
                source_record_id=None,
                confidence=claim["confidence"],
                native_status="historical_claim",
                details=details,
                fingerprint=fingerprint,
                observed_at=claim["last_seen_at"],
            )
        )
        existing_provenance.add(provenance_key)


def upgrade() -> None:
    _backfill_claim_observations(op.get_bind())


def downgrade() -> None:
    # The prior schema already supports observations. Retaining append-only
    # provenance is safer than deleting lineage during a code rollback.
    pass
