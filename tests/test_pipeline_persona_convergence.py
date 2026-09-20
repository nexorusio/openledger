"""Legacy-to-P2 convergence invariants on SQLite and PostgreSQL."""

import os
import uuid

import pytest
from sqlalchemy import func, insert, select

from maigret.web.case_store import (
    CaseStore,
    claim_evidence,
    claim_reviews,
    persona_claims,
    utcnow,
)
from maigret.web.persona_presenter import legacy_persona_projection
from maigret.web.persona_schema import presentation_predicate, section_for
from maigret.web.pipeline_ingestion import (
    bootstrap_legacy_workspace,
    converge_legacy_persona,
)
from maigret.web.pipeline_store import PipelineStore


@pytest.fixture(params=("sqlite", "postgres"))
def convergence_store(request, tmp_path):
    postgres_url = os.getenv("OPENLEDGER_TEST_POSTGRES_URL")
    if request.param == "postgres" and not postgres_url:
        pytest.skip("OPENLEDGER_TEST_POSTGRES_URL is not configured")
    url = postgres_url or f"sqlite:///{tmp_path}/persona-convergence.db"
    store = CaseStore(url, create_schema=True)
    yield store
    store.dispose()


def _scope(store):
    job_id = store.create_investigation(["synthetic-" + uuid.uuid4().hex[:10]], {})
    case_id = store.get_job(job_id)["case_id"]
    persona_id = store.get_case(case_id)["personas"][0]["id"]
    return case_id, persona_id


def _legacy_claim(
    connection,
    persona_id,
    *,
    field_name,
    value,
    status,
    reviews=None,
):
    claim_id = str(uuid.uuid4())
    now = utcnow()
    connection.execute(
        insert(persona_claims).values(
            id=claim_id,
            persona_id=persona_id,
            field_name=field_name,
            value=value,
            display_value=value,
            normalized_value=value.casefold(),
            confidence=80,
            review_status=status,
            source_engine="synthetic_public_document",
            source_job_id=None,
            fingerprint=uuid.uuid4().hex * 2,
            first_seen_at=now,
            last_seen_at=now,
            created_at=now,
            updated_at=now,
            reviewed_at=now if status != "pending" else None,
            reviewed_by="legacy-analyst" if status != "pending" else None,
        )
    )
    evidence_id = str(uuid.uuid4())
    connection.execute(
        insert(claim_evidence).values(
            id=evidence_id,
            claim_id=claim_id,
            evidence_type="public_document",
            source_name="Synthetic retained source",
            source_url="https://example.test/evidence/" + evidence_id,
            details={"fixture": True},
            fingerprint=uuid.uuid4().hex * 2,
            observed_at=now,
        )
    )
    for decision in reviews or [status]:
        connection.execute(
            insert(claim_reviews).values(
                claim_id=claim_id,
                decision=decision,
                reviewer="legacy-analyst",
                note="Retained " + decision + " review",
                created_at=now,
            )
        )
    return claim_id, evidence_id


def _snapshot(connection, table, persona_id):
    statement = select(table)
    if "persona_id" in table.c:
        statement = statement.where(table.c.persona_id == persona_id)
    elif table is claim_evidence:
        statement = statement.join(
            persona_claims, persona_claims.c.id == claim_evidence.c.claim_id
        ).where(persona_claims.c.persona_id == persona_id)
    elif table is claim_reviews:
        statement = statement.join(
            persona_claims, persona_claims.c.id == claim_reviews.c.claim_id
        ).where(persona_claims.c.persona_id == persona_id)
    return [dict(row) for row in connection.execute(statement).mappings()]


def test_convergence_is_complete_idempotent_and_never_finalizes(convergence_store):
    store = convergence_store
    pipeline = PipelineStore(store)
    case_id, persona_id = _scope(store)
    with store.engine.begin() as connection:
        approved_id, _ = _legacy_claim(
            connection,
            persona_id,
            field_name="occupation",
            value="Researcher",
            status="approved",
            reviews=["pending", "uncertain", "approved"],
        )
        _legacy_claim(
            connection,
            persona_id,
            field_name="company",
            value="Rejected Company",
            status="rejected",
        )
        _legacy_claim(
            connection,
            persona_id,
            field_name="email",
            value="uncertain@example.test",
            status="uncertain",
        )
        _legacy_claim(
            connection,
            persona_id,
            field_name="phone",
            value="+620000000",
            status="pending",
            reviews=["pending"],
        )
        _legacy_claim(
            connection,
            persona_id,
            field_name="full_name",
            value="Conflicting Person",
            status="approved",
        )
        _legacy_claim(
            connection,
            persona_id,
            field_name="full_name",
            value="Conflicting Person",
            status="rejected",
        )
        _legacy_claim(
            connection,
            persona_id,
            field_name="company",
            value="Partially Reviewed Company",
            status="approved",
        )
        _legacy_claim(
            connection,
            persona_id,
            field_name="company",
            value="Partially Reviewed Company",
            status="pending",
            reviews=["pending"],
        )
    with store.engine.connect() as connection:
        before = {
            table.name: _snapshot(connection, table, persona_id)
            for table in (persona_claims, claim_evidence, claim_reviews)
        }

    first = converge_legacy_persona(store, case_id, persona_id, dry_run=False)
    assert first["auto_finalized"] is False
    assert first["qc_created"] is False
    assert first["decisions"]["conflict_count"] == 2

    workspace = pipeline.get_workspace(case_id, persona_id, limit=100)
    decisions = {
        str(group["normalized"].get("value")): group.get("latest_decision")
        for group in workspace["shortlist"]
    }
    assert decisions["Researcher"] == "include"
    assert decisions["Rejected Company"] == "reject"
    assert decisions["uncertain@example.test"] == "unresolved"
    assert decisions["+620000000"] is None
    assert decisions["Conflicting Person"] == "unresolved"
    assert decisions["Partially Reviewed Company"] == "unresolved"

    observations = pipeline.list_observations(case_id, persona_id, limit=500)
    imported_claim_ids = {
        (row["payload"].get("payload") or {}).get("legacy_claim_id")
        for row in observations
        if (row["payload"].get("payload") or {}).get("legacy_claim_id")
    }
    assert len(imported_claim_ids) == 8
    assert approved_id in imported_claim_ids

    operator_decisions = pipeline._table("operator_decisions")
    versions = pipeline._table("persona_versions")
    qc_decisions = pipeline._table("qc_decisions")
    with store.engine.connect() as connection:
        imported = [
            dict(row)
            for row in connection.execute(
                select(operator_decisions).where(
                    operator_decisions.c.persona_id == persona_id
                )
            ).mappings()
        ]
        approved_document = next(
            claim
            for row in imported
            for claim in row["details"]["legacy_convergence"]["claims"]
            if claim["legacy_claim_id"] == approved_id
        )
        assert [review["decision"] for review in approved_document["reviews"]] == [
            "pending",
            "uncertain",
            "approved",
        ]
        assert (
            connection.scalar(
                select(func.count())
                .select_from(versions)
                .where(versions.c.persona_id == persona_id)
            )
            == 0
        )
        assert (
            connection.scalar(
                select(func.count())
                .select_from(qc_decisions)
                .where(qc_decisions.c.persona_id == persona_id)
            )
            == 0
        )
        decision_count = len(imported)

    second = converge_legacy_persona(store, case_id, persona_id, dry_run=False)
    assert second["decisions"]["decision_count"] == 0
    assert second["decisions"]["already_converged_count"] == decision_count
    with store.engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(operator_decisions)
                .where(operator_decisions.c.persona_id == persona_id)
            )
            == decision_count
        )
        after = {
            table.name: _snapshot(connection, table, persona_id)
            for table in (persona_claims, claim_evidence, claim_reviews)
        }
    assert after == before


def test_convergence_flags_disagreement_with_existing_p2_decision(
    convergence_store,
):
    store = convergence_store
    pipeline = PipelineStore(store)
    case_id, persona_id = _scope(store)
    with store.engine.begin() as connection:
        _legacy_claim(
            connection,
            persona_id,
            field_name="occupation",
            value="Public researcher",
            status="approved",
        )
    bootstrap_legacy_workspace(store, case_id, persona_id)
    group = next(
        group
        for group in pipeline.get_workspace(case_id, persona_id, limit=100)["groups"]
        if group["normalized"].get("value") == "Public researcher"
    )
    pipeline.decide(
        case_id,
        persona_id,
        group["id"],
        "reject",
        actor="p2-analyst",
        reason="Existing P2 review disagrees with legacy state.",
    )

    first = pipeline.converge_legacy_reviews(case_id, persona_id, dry_run=False)
    assert first["conflict_count"] == 1
    assert first["conflicts"][0]["reasons"] == ["legacy_and_p2_states_disagree"]
    assert (
        pipeline.get_group(case_id, persona_id, group["id"])["latest_decision"][
            "decision"
        ]
        == "unresolved"
    )

    # The previous imported checkpoint must not hide the human P2 decision.
    replay = pipeline.converge_legacy_reviews(case_id, persona_id, dry_run=False)
    assert replay["decision_count"] == 0
    assert replay["already_converged_count"] == 1
    assert pipeline.get_final_version(case_id, persona_id) is None


def test_public_event_affiliation_uses_public_exposure_without_storage_rewrite():
    normalized = {
        "predicate": "affiliation",
        "value": "PhD Defence Jati Pratomo | Remote Sensing-based Slum Mapping",
    }
    assert presentation_predicate("claim", normalized) == "event_appearance"
    assert section_for("claim", normalized) == "public_exposure"
    persona = {
        "display_name": "Jati Pratomo",
        "claims": [
            {
                "id": "legacy-claim",
                "field_name": "affiliation",
                "value": normalized["value"],
                "display_value": normalized["value"],
                "review_status": "approved",
                "reviewed_by": "analyst",
                "reviews": [],
                "evidence": [],
                "latitude": None,
                "longitude": None,
            }
        ],
    }
    projection = legacy_persona_projection(persona)
    item = projection["items"][0]
    assert item["section"] == "public_exposure"
    assert item["field_key"] == "event_appearance"
    assert item["legacy_claim"]["field_name"] == "affiliation"


@pytest.mark.parametrize(
    ("predicate", "section"),
    (
        ("public_profile_description", "identity"),
        ("public_fact", "public_exposure"),
        ("unrecognized_public_predicate", "public_exposure"),
        ("sanctions_record", "records"),
    ),
)
def test_only_explicit_risk_predicates_enter_risk_records(predicate, section):
    assert section_for("claim", {"predicate": predicate, "value": "Value"}) == section
