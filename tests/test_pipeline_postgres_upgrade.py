"""An actual legacy PostgreSQL upgrade; no metadata.create_all shortcut."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest
from sqlalchemy import MetaData, Table, create_engine, inspect, insert, select, text

ROOT = Path(__file__).resolve().parents[1]
POSTGRES_URL = os.getenv("OPENLEDGER_TEST_POSTGRES_URL", "")
if os.getenv("OPENLEDGER_REQUIRE_POSTGRES") == "true" and not POSTGRES_URL:
    raise RuntimeError("Required PostgreSQL acceptance cannot be skipped")
pytestmark = pytest.mark.skipif(
    not POSTGRES_URL, reason="Explicit disposable PostgreSQL URL is not configured"
)


def migrate(url, target, *, check=True):
    return subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", target],
        cwd=ROOT,
        env=dict(os.environ, DATABASE_URL=url),
        text=True,
        capture_output=True,
        check=check,
    )


def snapshot(engine, names):
    documents = {}
    with engine.connect() as connection:
        for name in names:
            table = Table(name, MetaData(), autoload_with=connection)
            rows = [dict(row) for row in connection.execute(select(table)).mappings()]
            documents[name] = sorted(
                json.dumps(row, sort_keys=True, default=str) for row in rows
            )
    return hashlib.sha256(json.dumps(documents, sort_keys=True).encode()).hexdigest()


def test_postgres_upgrades_existing_legacy_case_and_preserves_backfill_evidence():
    from maigret.web.case_store import (
        CaseStore,
        cases,
        personas,
        investigation_jobs,
        persona_claims,
        claim_evidence,
        claim_reviews,
        utcnow,
    )
    from maigret.web.pipeline_store import PipelineStore

    administrator = create_engine(POSTGRES_URL, isolation_level="AUTOCOMMIT")
    name = "openledger_pipeline_upgrade_" + uuid.uuid4().hex
    store = None
    with administrator.connect() as connection:
        connection.exec_driver_sql(f'CREATE DATABASE "{name}"')
    url = administrator.url.set(database=name).render_as_string(hide_password=False)
    try:
        migrate(url, "b3e9d7c4a610")
        store = CaseStore(url)  # create_schema is deliberately false
        assert not inspect(store.engine).has_table("pipeline_requests")
        # Seed the pre-pipeline shape directly. The new application correctly
        # refuses to enqueue its workflow before the additive migration exists.
        case_id, persona_id, job_id, claim_id = [str(uuid.uuid4()) for _ in range(4)]
        now = utcnow()
        with store.engine.begin() as connection:
            connection.execute(
                insert(cases).values(
                    id=case_id,
                    title="Synthetic legacy case",
                    status="open",
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(personas).values(
                    id=persona_id,
                    case_id=case_id,
                    display_name="Synthetic Upgrade Person",
                    created_at=now,
                )
            )
            connection.execute(
                insert(investigation_jobs).values(
                    id=job_id,
                    case_id=case_id,
                    kind="profile",
                    status="completed",
                    usernames=["synthetic-legacy-upgrade"],
                    options={},
                    progress={},
                    result={},
                    cancel_requested=False,
                    attempts=1,
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(persona_claims).values(
                    id=claim_id,
                    persona_id=persona_id,
                    field_name="full_name",
                    value="Synthetic Upgrade Person",
                    display_value="Synthetic Upgrade Person",
                    normalized_value="synthetic upgrade person",
                    confidence=80,
                    review_status="approved",
                    source_engine="synthetic_public_document",
                    source_job_id=job_id,
                    fingerprint="a" * 64,
                    first_seen_at=now,
                    last_seen_at=now,
                    created_at=now,
                    updated_at=now,
                    reviewed_at=now,
                    reviewed_by="legacy-operator",
                )
            )
            connection.execute(
                insert(claim_evidence).values(
                    id=str(uuid.uuid4()),
                    claim_id=claim_id,
                    evidence_type="public_document",
                    source_name="Synthetic source",
                    source_url="https://example.org/synthetic-upgrade",
                    details={"fixture": True},
                    fingerprint="b" * 64,
                    observed_at=now,
                )
            )
            connection.execute(
                insert(claim_reviews).values(
                    claim_id=claim_id,
                    decision="approved",
                    reviewer="legacy-operator",
                    note="Preserve the existing review",
                    created_at=now,
                )
            )
        legacy_tables = [
            name
            for name in inspect(store.engine).get_table_names()
            if name != "alembic_version"
        ]
        before = snapshot(store.engine, legacy_tables)
        migrate(url, "e2e2b8d0a502")
        assert snapshot(store.engine, legacy_tables) == before
        with store.engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalars().all() == ["e2e2b8d0a502"]
        pipeline = PipelineStore(store)
        dry = pipeline.backfill_legacy(case_id, persona_id, actor="migration-reviewer")
        assert dry["dry_run"] is True and dry["claim_count"] > 0
        first = pipeline.backfill_legacy(
            case_id, persona_id, actor="migration-reviewer", dry_run=False
        )
        evidence_before = list(pipeline.iter_observations(case_id, persona_id))
        assert evidence_before
        second = pipeline.backfill_legacy(
            case_id, persona_id, actor="migration-reviewer", dry_run=False
        )
        assert first["request_id"] == second["request_id"]
        assert list(pipeline.iter_observations(case_id, persona_id)) == evidence_before
        assert pipeline.get_final_version(case_id, persona_id) is None
        refused = subprocess.run(
            [sys.executable, "-m", "alembic", "downgrade", "b3e9d7c4a610"],
            cwd=ROOT,
            env=dict(os.environ, DATABASE_URL=url),
            text=True,
            capture_output=True,
        )
        assert refused.returncode != 0 and "Pipeline records exist" in refused.stderr
        assert list(pipeline.iter_observations(case_id, persona_id)) == evidence_before
    finally:
        if store is not None:
            store.dispose()
        with administrator.connect() as connection:
            connection.exec_driver_sql(f'DROP DATABASE "{name}" WITH (FORCE)')
        administrator.dispose()
