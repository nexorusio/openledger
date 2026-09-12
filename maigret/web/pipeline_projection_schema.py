"""Mutable read projections; evidence and curated manifests remain immutable."""

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import JSONB


def register_projection_schema(metadata):
    if "pipeline_projection_state" in metadata.tables:
        return {
            name: metadata.tables[name]
            for name in ("pipeline_projection_state", "pipeline_group_summaries")
        }
    document = JSON().with_variant(JSONB(), "postgresql")
    state = Table(
        "pipeline_projection_state",
        metadata,
        Column(
            "persona_id",
            String(36),
            ForeignKey("personas.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        Column(
            "case_id",
            String(36),
            ForeignKey("cases.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        Column("evidence_revision", Integer, nullable=False, server_default="0"),
        Column("projected_revision", Integer, nullable=False, server_default="0"),
        Column("legacy_imported_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    summaries = Table(
        "pipeline_group_summaries",
        metadata,
        Column("group_id", String(36), primary_key=True),
        Column("case_id", String(36), nullable=False),
        Column("persona_id", String(36), nullable=False),
        Column("normalized", document, nullable=False),
        Column("assessment", document),
        Column("assessment_id", String(36)),
        Column("observations", document, nullable=False),
        Column("observation_count", Integer, nullable=False),
        Column("revision_count", Integer, nullable=False),
        Column("latest_grouping_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        ForeignKeyConstraint(
            ["group_id", "case_id", "persona_id"],
            [
                "pipeline_groups.id",
                "pipeline_groups.case_id",
                "pipeline_groups.persona_id",
            ],
            ondelete="RESTRICT",
        ),
    )
    return {table.name: table for table in (state, summaries)}
