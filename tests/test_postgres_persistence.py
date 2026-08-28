"""Integration checks that run only against an explicitly supplied PostgreSQL URL."""

import os

import pytest
from sqlalchemy import text

from maigret.web.case_store import CaseStore

POSTGRES_URL = os.getenv("OPENLEDGER_TEST_POSTGRES_URL", "")
pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="OPENLEDGER_TEST_POSTGRES_URL is not configured",
)


@pytest.fixture
def postgres_store():
    store = CaseStore(POSTGRES_URL)
    with store.engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE cases RESTART IDENTITY CASCADE"))
    yield store
    with store.engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE cases RESTART IDENTITY CASCADE"))
    store.dispose()


def test_migrated_postgres_schema_runs_durable_job_lifecycle(postgres_store):
    job_id = postgres_store.create_investigation(
        ["alice"],
        {"top_sites": 500, "proxy_configured": False},
    )
    claimed = postgres_store.claim_next("worker:integration")
    assert claimed["job_id"] == job_id
    assert claimed["status"] == "running"

    postgres_store.append_event(
        job_id,
        {"type": "progress", "checked": 12, "total": 500},
    )
    postgres_store.finish(
        job_id,
        {
            "status": "completed",
            "session_folder": f"search_{job_id}",
            "usernames": ["alice"],
            "individual_reports": [],
            "graph_file": f"search_{job_id}/graph.html",
            "found_count": 2,
        },
    )

    completed = postgres_store.get_job(job_id)
    assert completed["status"] == "completed"
    assert completed["progress"]["checked"] == 12
    assert completed["found_count"] == 2


def test_postgres_allows_only_one_investigation_worker_lock(postgres_store):
    competing_store = CaseStore(POSTGRES_URL)
    first_lock = postgres_store.try_acquire_worker_lock()
    try:
        assert first_lock is not None
        assert competing_store.try_acquire_worker_lock() is None
    finally:
        first_lock.close()

    replacement_lock = competing_store.try_acquire_worker_lock()
    assert replacement_lock is not None
    replacement_lock.close()
    competing_store.dispose()
