#!/usr/bin/env python3
"""Dump/restore populated, disposable PostgreSQL databases and refuse data rollback.

Only databases created by this invocation are written or deleted. No existing
application database, production volume, provider endpoint or image is changed.
PostgreSQL 17 client tools come from the already-present CI service image.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from sqlalchemy import create_engine, text, update
from sqlalchemy.exc import DBAPIError

ROOT = Path(__file__).resolve().parents[1]
TARGET = "e2e2b8d0a502"


def load_fixture():
    spec = importlib.util.spec_from_file_location("recovery_fixture", ROOT / "deploy/recovery-fixture.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def migrate(url, operation, target, *, check=True):
    result = subprocess.run(
        [sys.executable, "-m", "alembic", operation, target], cwd=ROOT,
        env=dict(os.environ, DATABASE_URL=url), capture_output=True, text=True, timeout=120,
    )
    if check and result.returncode:
        raise RuntimeError("Disposable migration rehearsal failed: " + result.stderr[-3000:])
    return result


def main():
    url = os.environ["OPENLEDGER_TEST_POSTGRES_URL"]
    administrator = create_engine(url, isolation_level="AUTOCOMMIT")
    suffix = uuid.uuid4().hex[:12]
    names = ["pipeline_recovery_source_" + suffix, "pipeline_recovery_restore_" + suffix]
    created, engines = [], []
    fixture = load_fixture()
    def client(database, command, *args, input=None):
        address = administrator.url
        if not address.host or address.host not in {"127.0.0.1", "localhost"}:
            raise ValueError("Recovery CI requires the explicit loopback disposable PostgreSQL service")
        result = subprocess.run(
            ["docker", "run", "--rm", "--pull", "never", "-i", "--network", "host",
             "--env", "PGPASSWORD", "postgres:17-alpine", command,
             "--host", address.host, "--port", str(address.port or 5432),
             "--username", address.username, "--dbname", database, *args],
            input=input, capture_output=True,
            env=dict(os.environ, PGPASSWORD=address.password or ""), timeout=120,
        )
        if result.returncode:
            raise RuntimeError("Disposable PostgreSQL client failed: " + result.stderr.decode(errors="replace")[-2000:])
        return result.stdout
    try:
        for name in names:
            with administrator.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE "{name}"')
            created.append(name)
            engines.append(create_engine(administrator.url.set(database=name)))
        source, restored = engines
        source_url = source.url.render_as_string(hide_password=False)
        migrate(source_url, "upgrade", "e2e1a7c9d401")
        with source.begin() as connection:
            ids = fixture.seed_historical(connection)
            historical_names = list(fixture.snapshot(connection)["counts"])
            historical_names.remove("alembic_version")
            historical = fixture.snapshot(connection, historical_names)
        migrate(source_url, "upgrade", TARGET)
        with source.begin() as connection:
            assert fixture.snapshot(connection, historical_names) == historical
            fixture.seed_reliability(connection, ids)
            before = fixture.snapshot(connection)
        backup = client(names[0], "pg_dump", "--format=custom", "--no-owner", "--no-acl")
        assert backup.startswith(b"PGDMP")
        client(names[1], "pg_restore", "--exit-on-error", "--no-owner", "--no-acl", input=backup)
        with restored.connect() as connection:
            after = fixture.snapshot(connection)
            assert after == before, "Restored evidence/receipts/checkpoints/versions differ"
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == TARGET
        restored_url = restored.url.render_as_string(hide_password=False)
        refused = migrate(restored_url, "downgrade", "e2e1a7c9d401", check=False)
        assert refused.returncode != 0 and "preserve expanded schema" in refused.stderr
        with restored.connect() as connection:
            assert fixture.snapshot(connection) == before, "Refused rollback changed evidence"
        protected = ["pipeline_observations", "pipeline_persona_versions", "pipeline_connector_pages", "pipeline_connector_record_versions", "pipeline_connector_receipts"]
        for name in protected:
            try:
                with restored.begin() as connection:
                    connection.execute(update(fixture.table(connection, name)).values(content_hash="0" * 64))
            except DBAPIError as error:
                assert "immutable pipeline record" in str(error) or "immutable connector" in str(error)
            else:
                raise AssertionError("Restored immutability guard missing: " + name)
        record = ROOT / "runtime/ci/backup-restore-rehearsal.json"
        record.parent.mkdir(parents=True, exist_ok=True)
        record.write_text(json.dumps({
            "schema_revision": TARGET, "synthetic_fixture": True,
            "backup_bytes": len(backup), "before": before, "restored": after,
            "historical_upgrade_preserved": True, "populated_downgrade_refused": True,
            "restored_immutable_tables_verified": protected,
            "production_touched": False,
        }, indent=2) + "\n")
        print("Disposable PostgreSQL backup restored with identical rows; populated downgrade refused; evidence guards retained.")
    finally:
        for engine in engines:
            engine.dispose()
        for name in created:
            with administrator.connect() as connection:
                connection.exec_driver_sql(f'DROP DATABASE "{name}" WITH (FORCE)')
        administrator.dispose()


if __name__ == "__main__":
    main()
