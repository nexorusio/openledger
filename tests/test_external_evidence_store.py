import pytest
from sqlalchemy import delete

from maigret.web.case_store import CaseStore, claim_observations
from maigret.web.external_evidence import ExternalEvidenceValidationError
from migrations.versions.e91b7a4c2d6f_backfill_claim_observations import (
    _backfill_claim_observations,
)


@pytest.fixture
def store(tmp_path):
    instance = CaseStore(
        f"sqlite:///{tmp_path / 'external-evidence.db'}",
        create_schema=True,
    )
    yield instance
    instance.dispose()


def envelope(**overrides):
    payload = {
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
            "policy_tags": ["case-use-only"],
        },
        "locator": {"uri": "datamart://client-alpha/record-42/v1"},
        "attributes": {"source_table": "identity_observations"},
        "preview": "Redacted preview",
    }
    payload.update(overrides)
    return payload


def create_case_with_claim(store):
    job_id = store.create_investigation(["alice"], {})
    store.claim_next("worker:test")
    result = {
        "status": "completed",
        "usernames": ["alice"],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": "https://example.test/alice",
                        "confidence": "strong",
                        "evidence": {},
                    }
                ],
            }
        ],
    }
    store.finish(job_id, result)
    store.sync_persona_claims(job_id, result)
    job = store.get_job(job_id)
    persona = store.get_persona(store.get_case(job["case_id"])["personas"][0]["id"])
    return job_id, job["case_id"], persona["claims"][0]["id"]


def register_source(store):
    return store.register_data_source(
        "client.datamart",
        name="Client governed datamart",
        source_type="datamart",
        authority="client-alpha",
        default_classification="restricted",
        handling_defaults={"policy_tags": ["case-use-only"]},
    )


def create_completed_receipt(store, case_id, *, result_count=1):
    receipt_id = store.create_query_receipt(
        case_id,
        "client.datamart",
        requested_by="analyst-7",
        purpose="Corroborate identity claims in the assigned case",
        query_document={"record_ids": ["record-42"]},
        policy_context={
            "principal_id": "analyst-7",
            "purpose": "Corroborate identity claims in the assigned case",
            "authority": "client-alpha",
            "classification_ceiling": "restricted",
        },
    )
    store.complete_query_receipt(receipt_id, result_count)
    return receipt_id


def test_external_evidence_is_case_scoped_idempotent_and_immutable(store):
    _, case_id, _ = create_case_with_claim(store)
    register_source(store)
    receipt_id = create_completed_receipt(store, case_id)

    evidence_id = store.attach_external_evidence(
        case_id, receipt_id, envelope(), attached_by="analyst-7"
    )
    assert evidence_id == store.attach_external_evidence(
        case_id,
        receipt_id,
        envelope(),
        attached_by="analyst-7",
    )
    second_receipt_id = create_completed_receipt(store, case_id)
    assert evidence_id == store.attach_external_evidence(
        case_id,
        second_receipt_id,
        envelope(),
        attached_by="analyst-8",
    )
    stored = store.get_external_evidence(case_id, evidence_id)
    assert stored["source_version"] == "v1"
    assert {
        receipt["query_receipt_id"] for receipt in stored["query_receipts"]
    } == {receipt_id, second_receipt_id}
    assert store.get_external_evidence("different-case", evidence_id) is None

    with pytest.raises(ExternalEvidenceValidationError, match="immutable"):
        store.attach_external_evidence(
            case_id,
            receipt_id,
            envelope(content_hash=f"sha256:{'c' * 64}"),
            attached_by="analyst-7",
        )
    for conflicting_fields in (
        {"observed_at": "2026-08-30T12:00:01Z"},
        {"validity": {"from": "2026-01-01T00:00:00Z"}},
        {
            "handling": {
                "classification": "restricted",
                "authority": "client-alpha",
                "policy_tags": ["different-policy"],
            }
        },
        {"attributes": {"source_table": "different_table"}},
        {"preview": "A different retained preview"},
    ):
        with pytest.raises(ExternalEvidenceValidationError, match="immutable"):
            store.attach_external_evidence(
                case_id,
                receipt_id,
                envelope(**conflicting_fields),
                attached_by="analyst-7",
            )
    with pytest.raises(ExternalEvidenceValidationError, match="classification"):
        store.attach_external_evidence(
            case_id,
            receipt_id,
            envelope(
                source_record_id="record-43",
                handling={
                    "classification": "secret",
                    "authority": "client-alpha",
                },
            ),
            attached_by="analyst-7",
        )
    with pytest.raises(ExternalEvidenceValidationError, match="locator authority"):
        store.attach_external_evidence(
            case_id,
            receipt_id,
            envelope(
                source_record_id="record-43",
                locator={"uri": "datamart://another-client/record-43/v1"},
            ),
            attached_by="analyst-7",
        )


def test_query_receipt_result_count_bounds_distinct_evidence_links(store):
    _, case_id, _ = create_case_with_claim(store)
    register_source(store)

    empty_receipt_id = create_completed_receipt(store, case_id, result_count=0)
    with pytest.raises(ExternalEvidenceValidationError, match="result count"):
        store.attach_external_evidence(
            case_id,
            empty_receipt_id,
            envelope(),
            attached_by="analyst-7",
        )

    receipt_id = create_completed_receipt(store, case_id, result_count=1)
    evidence_id = store.attach_external_evidence(
        case_id,
        receipt_id,
        envelope(),
        attached_by="analyst-7",
    )
    assert evidence_id == store.attach_external_evidence(
        case_id,
        receipt_id,
        envelope(),
        attached_by="analyst-7",
    )
    with pytest.raises(ExternalEvidenceValidationError, match="result count"):
        store.attach_external_evidence(
            case_id,
            receipt_id,
            envelope(
                source_record_id="record-43",
                locator={"uri": "datamart://client-alpha/record-43/v1"},
            ),
            attached_by="analyst-7",
        )


def test_query_receipt_requires_matching_identity_purpose_and_authority(store):
    _, case_id, _ = create_case_with_claim(store)
    register_source(store)
    base_context = {
        "principal_id": "analyst-7",
        "purpose": "Assigned investigation",
        "authority": "client-alpha",
        "classification_ceiling": "restricted",
    }

    for field, wrong_value in (
        ("principal_id", "another-analyst"),
        ("purpose", "Unrelated purpose"),
        ("authority", "another-client"),
        ("classification_ceiling", "secret"),
    ):
        context = {**base_context, field: wrong_value}
        with pytest.raises(ExternalEvidenceValidationError):
            store.create_query_receipt(
                case_id,
                "client.datamart",
                requested_by="analyst-7",
                purpose="Assigned investigation",
                query_document={"record_ids": ["record-42"]},
                policy_context=context,
            )


def test_claim_lineage_retains_each_engine_and_external_observation(store):
    job_id, case_id, claim_id = create_case_with_claim(store)
    initial = store.get_claim_lineage(claim_id)
    assert len(initial) == 1
    assert initial[0]["provenance_type"] == "investigation_job"
    assert initial[0]["job_id"] == job_id

    register_source(store)
    receipt_id = create_completed_receipt(store, case_id)
    evidence_id = store.attach_external_evidence(
        case_id, receipt_id, envelope(), attached_by="analyst-7"
    )
    observation_id = store.record_claim_observation(
        claim_id,
        external_evidence_id=evidence_id,
        source_engine="client_datamart",
        source_record_id="record-42",
        confidence=88,
        native_status="corroborated",
        details={"matching_fields": ["username"]},
    )
    duplicate_id = store.record_claim_observation(
        claim_id,
        external_evidence_id=evidence_id,
        source_engine="client_datamart",
        source_record_id="record-42",
        confidence=88,
        native_status="corroborated",
        details={"matching_fields": ["username"]},
    )

    assert duplicate_id == observation_id
    lineage = store.get_claim_lineage(claim_id)
    assert [item["provenance_type"] for item in lineage] == [
        "investigation_job",
        "external_evidence",
    ]


def test_cross_case_receipts_and_claim_provenance_are_rejected(store):
    _, first_case_id, first_claim_id = create_case_with_claim(store)
    second_job_id, second_case_id, _ = create_case_with_claim(store)
    register_source(store)
    receipt_id = create_completed_receipt(store, first_case_id)

    with pytest.raises(ExternalEvidenceValidationError, match="different case"):
        store.attach_external_evidence(
            second_case_id, receipt_id, envelope(), attached_by="analyst-7"
        )
    with pytest.raises(ExternalEvidenceValidationError, match="different cases"):
        store.record_claim_observation(
            first_claim_id,
            job_id=second_job_id,
            source_engine="maigret",
            native_status="observed",
        )


def test_lineage_migration_backfills_retained_claim_provenance_idempotently(store):
    retained_claims = [create_case_with_claim(store) for _ in range(3)]
    claim_ids = [claim_id for _, _, claim_id in retained_claims]
    with store.engine.begin() as connection:
        connection.execute(
            delete(claim_observations).where(
                claim_observations.c.claim_id.in_(claim_ids)
            )
        )
        _backfill_claim_observations(connection, batch_size=2)
        _backfill_claim_observations(connection, batch_size=2)

    for job_id, _, claim_id in retained_claims:
        lineage = store.get_claim_lineage(claim_id)
        assert len(lineage) == 1
        assert lineage[0]["provenance_type"] == "investigation_job"
        assert lineage[0]["provenance_id"] == job_id
        assert lineage[0]["job_id"] == job_id
        assert lineage[0]["source_engine"] == "openledger_profile_discovery"
        assert lineage[0]["native_status"] == "historical_claim"
        assert lineage[0]["details"]["backfilled"] is True
