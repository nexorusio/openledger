"""Convergence checkpoint migration preserves older import-only state."""

import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
    insert,
    select,
)

from maigret.web.case_store import utcnow


ROOT = Path(__file__).resolve().parents[1]


def _load_migration():
    path = (
        ROOT
        / "migrations/versions/e2e4d0e2a804_add_persona_convergence_checkpoint.py"
    )
    spec = importlib.util.spec_from_file_location("persona_convergence_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_old_evidence_import_marker_does_not_imply_complete_convergence(
    tmp_path, monkeypatch
):
    url = f"sqlite:///{tmp_path / 'persona-convergence-migration.db'}"
    engine = create_engine(url)
    metadata = MetaData()
    state = Table(
        "pipeline_projection_state",
        metadata,
        Column("persona_id", String(36), primary_key=True),
        Column("case_id", String(36), nullable=False),
        Column("evidence_revision", Integer, nullable=False),
        Column("projected_revision", Integer, nullable=False),
        Column("legacy_imported_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    metadata.create_all(engine)
    imported_at = utcnow()
    with engine.begin() as connection:
        connection.execute(
            insert(state).values(
                persona_id="persona-1",
                case_id="case-1",
                evidence_revision=1,
                projected_revision=1,
                legacy_imported_at=imported_at,
                updated_at=imported_at,
            )
        )
        migration = _load_migration()
        monkeypatch.setattr(
            migration,
            "op",
            Operations(MigrationContext.configure(connection)),
        )
        migration.upgrade()
    try:
        columns = {
            column["name"]
            for column in inspect(engine).get_columns("pipeline_projection_state")
        }
        assert "legacy_converged_at" in columns
        migrated = Table(
            "pipeline_projection_state", MetaData(), autoload_with=engine
        )
        with engine.connect() as connection:
            row = connection.execute(select(migrated)).mappings().one()
        assert row["legacy_imported_at"] is not None
        assert row["legacy_converged_at"] is None
    finally:
        engine.dispose()
