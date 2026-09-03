"""Integration checks that run only against an explicitly supplied PostgreSQL URL."""

import os

import pytest
from sqlalchemy import text

from maigret.web.case_store import CaseStore
from maigret.web.external_evidence import ExternalEvidenceValidationError

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


def test_postgres_persona_export_uses_a_timestamped_consistent_snapshot(
    postgres_store,
):
    job_id = postgres_store.create_investigation(["alice"], {})
    job = postgres_store.get_job(job_id)
    persona_id = postgres_store.get_case(job["case_id"])["personas"][0]["id"]

    persona, generated_at = postgres_store.get_persona_export_snapshot(persona_id)

    assert persona["id"] == persona_id
    assert generated_at.tzinfo is not None


def test_postgres_combined_case_uses_repeatable_read_snapshot(postgres_store):
    source_case_ids = []
    for username in ("alice", "bob"):
        source_job_id = postgres_store.create_investigation([username], {})
        source_job = postgres_store.claim_next(f"worker:{username}")
        postgres_store.finish(
            source_job_id,
            {
                "status": "completed",
                "usernames": [username],
                "individual_reports": [],
            },
        )
        source_case_ids.append(source_job["case_id"])
    fusion_job_id = postgres_store.create_combined_investigation(
        source_case_ids,
        title="PostgreSQL combined snapshot",
        purpose="Exercise the production transaction path.",
        created_by="analyst",
    )
    postgres_store.claim_next("worker:fusion")

    snapshot = postgres_store.build_case_fusion_snapshot(fusion_job_id)

    assert snapshot["source_case_count"] == 2
    assert len(snapshot["snapshot"]["sha256"]) == 64
    assert snapshot["snapshot"]["generated_at"].endswith("+00:00")


def test_postgres_allows_only_one_investigation_worker_lock(postgres_store):
    competing_store = CaseStore(POSTGRES_URL)
    first_lock = postgres_store.try_acquire_worker_lock()
    try:
        assert first_lock is not None
        assert competing_store.try_acquire_worker_lock() is None
    finally:
        first_lock.close()
        first_lock.close()

    replacement_lock = competing_store.try_acquire_worker_lock()
    assert replacement_lock is not None
    replacement_lock.close()
    competing_store.dispose()


def test_postgres_case_delete_requires_terminal_jobs_and_cascades(postgres_store):
    job_id = postgres_store.create_investigation(["alice"], {})
    case_id = postgres_store.get_job(job_id)["case_id"]
    with pytest.raises(ValueError, match="active investigations"):
        postgres_store.delete_case(case_id)

    job = postgres_store.claim_next("worker:integration")
    postgres_store.finish(
        job_id,
        {"status": "cancelled", "usernames": job["usernames"]},
    )
    assert postgres_store.delete_case(case_id) is True
    assert postgres_store.get_case(case_id) is None
    assert postgres_store.get_job(job_id) is None


def test_postgres_enforces_external_evidence_receipt_and_immutability_guards(
    postgres_store,
):
    job_id = postgres_store.create_investigation(["alice"], {})
    case_id = postgres_store.get_job(job_id)["case_id"]
    postgres_store.register_data_source(
        "client.datamart",
        name="Client governed datamart",
        source_type="datamart",
        authority="client-alpha",
        default_classification="restricted",
    )
    receipt_id = postgres_store.create_query_receipt(
        case_id,
        "client.datamart",
        requested_by="analyst-7",
        purpose="Corroborate the assigned case",
        query_document={"record_ids": ["record-42"]},
        policy_context={
            "principal_id": "analyst-7",
            "purpose": "Corroborate the assigned case",
            "authority": "client-alpha",
            "classification_ceiling": "restricted",
        },
    )
    postgres_store.complete_query_receipt(receipt_id, 1)
    envelope = {
        "schema_version": 1,
        "source_id": "client.datamart",
        "source_record_id": "record-42",
        "source_version": "v1",
        "record_type": "identity.observation",
        "content_hash": f"sha256:{'b' * 64}",
        "observed_at": "2026-08-30T12:00:00Z",
        "handling": {
            "classification": "restricted",
            "authority": "client-alpha",
        },
        "locator": {"uri": "datamart://client-alpha/record-42/v1"},
        "attributes": {"source_table": "identity_observations"},
        "preview": "Redacted preview",
    }

    evidence_id = postgres_store.attach_external_evidence(
        case_id,
        receipt_id,
        envelope,
        attached_by="analyst-7",
    )
    assert evidence_id == postgres_store.attach_external_evidence(
        case_id,
        receipt_id,
        envelope,
        attached_by="analyst-7",
    )
    with pytest.raises(ExternalEvidenceValidationError, match="immutable"):
        postgres_store.attach_external_evidence(
            case_id,
            receipt_id,
            {**envelope, "preview": "Conflicting preview"},
            attached_by="analyst-7",
        )
    with pytest.raises(ExternalEvidenceValidationError, match="locator authority"):
        postgres_store.attach_external_evidence(
            case_id,
            receipt_id,
            {
                **envelope,
                "source_record_id": "record-43",
                "locator": {"uri": "datamart://another-client/record-43/v1"},
            },
            attached_by="analyst-7",
        )
    with pytest.raises(ExternalEvidenceValidationError, match="result count"):
        postgres_store.attach_external_evidence(
            case_id,
            receipt_id,
            {
                **envelope,
                "source_record_id": "record-43",
                "locator": {"uri": "datamart://client-alpha/record-43/v1"},
            },
            attached_by="analyst-7",
        )
