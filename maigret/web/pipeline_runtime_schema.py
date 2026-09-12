"""Durable outbound budgets and provider health shared by attempt processes."""

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    CheckConstraint,
)


def register_runtime_schema(metadata):
    if "pipeline_request_budgets" not in metadata.tables:
        Table(
            "pipeline_request_budgets",
            metadata,
            Column(
                "request_id",
                String(36),
                ForeignKey("pipeline_requests.id", ondelete="RESTRICT"),
                primary_key=True,
            ),
            Column("max_requests", Integer, nullable=False),
            Column("consumed", Integer, nullable=False, server_default="0"),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            CheckConstraint(
                "consumed >= 0 AND consumed <= max_requests",
                name="ck_pipeline_request_budget",
            ),
        )
        Table(
            "pipeline_provider_state",
            metadata,
            Column("provider", String(200), primary_key=True),
            Column("consecutive_failures", Integer, nullable=False, server_default="0"),
            Column("cooldown_until", DateTime(timezone=True)),
            Column("probe_until", DateTime(timezone=True)),
            Column("probe_token", String(36)),
            Column("last_outcome", String(32)),
            Column("updated_at", DateTime(timezone=True), nullable=False),
        )
    return {
        name: metadata.tables[name]
        for name in ("pipeline_request_budgets", "pipeline_provider_state")
    }
