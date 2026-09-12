"""Real additive migration DDL, history preservation and rollback protection."""
import importlib.util
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, MetaData, select, Table, update
from sqlalchemy.exc import DBAPIError

ROOT = Path(__file__).resolve().parents[1]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


old = load("migrations/versions/e2e1a7c9d401_add_p2_end_to_end_pipeline.py", "frozen_e2e1")
new = load("migrations/versions/e2e2b8d0a502_add_pipeline_reliability.py", "frozen_e2e2")
fixture = load("deploy/recovery-fixture.py", "recovery_fixture")


@pytest.fixture
def database(tmp_path, monkeypatch):
    from maigret.web.case_store import metadata
    engine = create_engine(f"sqlite:///{tmp_path}/reliability.db")
    metadata.create_all(engine, tables=[table for table in metadata.sorted_tables if not table.name.startswith("pipeline_")])
    yield engine
    engine.dispose()


def test_frozen_historical_migration_does_not_create_future_tables(database, monkeypatch):
    import maigret.web.pipeline_schema as live
    monkeypatch.setattr(live, "register_pipeline_schema", lambda _: pytest.fail("Historical migration called live schema"))
    with database.begin() as connection:
        monkeypatch.setattr(old.op, "get_bind", lambda: connection)
        old.upgrade()
        names = {name for name in inspect(connection).get_table_names() if name.startswith("pipeline_")}
        assert len(names) == 16
        assert not names & {"pipeline_provider_state", "pipeline_connector_receipts", "pipeline_group_summaries"}
        old.downgrade()
        assert not any(name.startswith("pipeline_") for name in inspect(connection).get_table_names())


def test_reliability_migration_preserves_evidence_and_refuses_populated_downgrade(database, monkeypatch):
    with database.begin() as connection:
        monkeypatch.setattr(old.op, "get_bind", lambda: connection)
        old.upgrade()
        ids = fixture.seed_historical(connection)
        historical_names = inspect(connection).get_table_names()
        before = fixture.snapshot(connection, historical_names)
        monkeypatch.setattr(new.op, "get_bind", lambda: connection)
        new.upgrade()
        assert fixture.snapshot(connection, historical_names) == before
        state = Table("pipeline_projection_state", MetaData(), autoload_with=connection)
        dirty = connection.execute(select(state)).mappings().one()
        assert dirty["persona_id"] == ids["persona"]
        assert dirty["evidence_revision"] == 1 and dirty["projected_revision"] == 0
        fixture.seed_reliability(connection, ids)
        expected = fixture.snapshot(connection)
        with pytest.raises(RuntimeError, match="preserve expanded schema"):
            new.downgrade()
        assert fixture.snapshot(connection) == expected
    for name in ("pipeline_observations", "pipeline_persona_versions", "pipeline_connector_pages", "pipeline_connector_record_versions", "pipeline_connector_receipts"):
        with database.begin() as connection:
            immutable = Table(name, MetaData(), autoload_with=connection)
            with pytest.raises(DBAPIError, match="immutable (pipeline|connector)"):
                connection.execute(update(immutable).values(content_hash="0" * 64))


def test_empty_reliability_migration_can_downgrade_without_touching_historical_schema(database, monkeypatch):
    with database.begin() as connection:
        monkeypatch.setattr(old.op, "get_bind", lambda: connection)
        old.upgrade()
        names = set(inspect(connection).get_table_names())
        monkeypatch.setattr(new.op, "get_bind", lambda: connection)
        new.upgrade()
        assert len(set(inspect(connection).get_table_names()) - names) == 9
        new.downgrade()
        assert set(inspect(connection).get_table_names()) == names
