"""Real persistence tests for legacy recovery and direct operator evidence."""

import pytest
from sqlalchemy import func, select

from maigret.web.case_store import CaseStore
from maigret.web.pipeline_ingestion import (
    bootstrap_legacy_workspace,
    ingest_legacy_claim_updates,
    submit_manual_evidence,
)
from maigret.web.pipeline_store import PipelineStore


@pytest.fixture
def legacy_result():
    return {
        "status": "completed",
        "usernames": ["synthetic-alice"],
        "found_count": 1,
        "individual_reports": [
            {
                "username": "synthetic-alice",
                "claimed_profiles": [
                    {
                        "site_name": "Example",
                        "url": "https://example.test/alice",
                        "confidence": "strong",
                        "evidence": {"fullname": "Alice Example"},
                    }
                ],
            }
        ],
        "collector_observations": [
            {
                "source_engine": "user_scanner_username",
                "site_name": "TikTok",
                "status": "blocked",
                "subject_type": "username",
                "subject_value": "synthetic-alice",
                "source_url": "https://www.tiktok.com/@synthetic-alice",
            }
        ],
        "source_coverage": {"checked": 2, "found": 1, "blocked": 1},
    }


@pytest.fixture
def context(tmp_path, legacy_result):
    store = CaseStore(f"sqlite:///{tmp_path}/ingestion.db", create_schema=True)
    job_id = "retained-legacy-synthetic-alice"
    result = legacy_result
    assert store.import_legacy_result(job_id, result)
    store.sync_persona_claims(job_id, result)
    case_id = store.get_job(job_id)["case_id"]
    persona_id = store.get_case(case_id)["personas"][0]["id"]
    yield store, PipelineStore(store), case_id, persona_id, job_id
    store.dispose()


def counts(pipeline):
    with pipeline.engine.connect() as connection:
        return {
            name: connection.scalar(
                select(func.count()).select_from(pipeline._table(name))
            )
            for name in (
                "requests",
                "observations",
                "groups",
                "operator_decisions",
                "persona_versions",
                "qc_decisions",
            )
        }


def manual(context, **overrides):
    store, _, case_id, persona_id, _ = context
    args = dict(
        actor="analyst-1",
        claim={
            "predicate": "occupation",
            "value": "Researcher",
            "qualifiers": {"organization": "Example University"},
            "valid_from": "2024",
        },
        source_url="https://university.example/staff/alice",
        reason="The public staff page states this role.",
    )
    args.update(overrides)
    return submit_manual_evidence(store, case_id, persona_id, **args)


def test_manual_submission_is_pending_source_linked_and_idempotent(context):
    _, pipeline, case_id, persona_id, _ = context
    first = manual(context)
    before = counts(pipeline)
    second = manual(context)
    assert first == second
    assert counts(pipeline) == before
    assert len(first["group_ids"]) == 1
    group = pipeline.get_group(case_id, persona_id, first["group_ids"][0])
    assert group["latest_decision"] is None
    assert group["normalized"]["qualifiers"] == {"organization": "Example University"}
    assert group["normalized"]["valid_from"] == "2024"
    assert not first["auto_finalized"] and not first["auto_included"]
    assert (
        before["operator_decisions"]
        == before["persona_versions"]
        == before["qc_decisions"]
        == 0
    )
    assert group["observations"][0]["payload"]["evidence_signals"] == {}


def test_manual_adopts_ungrouped_evidence_without_copying_origin(context):
    store, pipeline, case_id, persona_id, _ = context
    bootstrap_legacy_workspace(store, case_id, persona_id)
    rows = pipeline.list_observations(case_id, persona_id, limit=500)
    source = next(row for row in rows if row["payload"]["engine"] == "maigret_report")
    result = manual(
        context,
        claim={
            "predicate": "public_profile_description",
            "value": "Alice Example",
            "observation_ids": [source["id"]],
        },
        source_url=None,
        reason="The retained profile record supports this literal description.",
    )
    group = pipeline.get_group(case_id, persona_id, result["group_ids"][0])
    assert {row["id"] for row in group["observations"]} == {
        source["id"],
        result["observation_id"],
    }
    assert group["normalized"]["independent_origin_count"] == 1
    proposal = next(
        row for row in group["observations"] if row["id"] == result["observation_id"]
    )
    assert proposal["payload"]["derived_from"] == [source["payload"]["id"]]
    assert (
        source["payload"]
        == next(
            row
            for row in pipeline.list_observations(case_id, persona_id, limit=500)
            if row["id"] == source["id"]
        )["payload"]
    )


def test_manual_reference_accepts_normalized_ids_and_rejects_foreign(context):
    store, pipeline, case_id, persona_id, _ = context
    first = manual(context)
    second = manual(
        context,
        claim={
            "predicate": "role",
            "value": "Researcher",
            "observation_ids": [first["normalized_observation_id"]],
        },
        source_url=None,
    )
    assert second["group_ids"]
    before = counts(pipeline)
    with pytest.raises(ValueError, match="does not belong"):
        manual(
            context,
            claim={
                "predicate": "role",
                "value": "Researcher",
                "observation_ids": ["foreign-observation"],
            },
            source_url=None,
        )
    assert counts(pipeline) == before


def test_multiple_cited_sources_keep_two_origins_and_no_operator_third(context):
    _, pipeline, case_id, persona_id, _ = context
    one = manual(context)
    two = manual(
        context,
        source_url="https://registry.example/alice",
        reason="Independent public registry entry.",
    )
    combined = manual(
        context,
        claim={
            "predicate": "reviewed_occupation",
            "value": "Researcher",
            "observation_ids": [one["observation_id"], two["observation_id"]],
        },
        source_url=None,
    )
    group = pipeline.get_group(case_id, persona_id, combined["group_ids"][0])
    assert group["normalized"]["independent_origin_count"] == 2
    assert len(combined["observation_ids"]) == 2
    assert len(group["observations"]) == 4


@pytest.mark.parametrize(
    "updates",
    [
        {"actor": "ai:model"},
        {
            "claim": {
                "predicate": "occupation",
                "value": "Researcher",
                "probability": 0.98,
            }
        },
        {"claim": {"predicate": "occupation"}},
        {"source_url": "http://127.0.0.1/private"},
        {"source_url": None},
        {
            "claim": {
                "predicate": "occupation",
                "value": "Researcher",
                "account_key": "foreign-account",
            }
        },
    ],
)
def test_invalid_manual_proposal_has_no_writes(context, updates):
    before = counts(context[1])
    with pytest.raises((ValueError, PermissionError)):
        manual(context, **updates)
    assert counts(context[1]) == before


def test_bootstrap_preserves_claims_reports_failures_and_never_finalizes(context):
    store, pipeline, case_id, persona_id, job_id = context
    old_claims = store.get_persona(persona_id)["claims"]
    report = bootstrap_legacy_workspace(store, case_id, persona_id)
    assert report["claim_count"] == len(old_claims)
    assert report["job_count"] == 1
    assert report["request_ids"]
    docs = list(pipeline.iter_observations(case_id, persona_id))
    assert any(doc["status"] == "blocked" for doc in docs)
    assert any(doc["payload"].get("legacy_job_id") == job_id for doc in docs)
    imported_claim_ids = {doc["payload"].get("legacy_claim_id") for doc in docs}
    assert {claim["id"] for claim in old_claims}.issubset(imported_claim_ids)
    assert store.get_persona(persona_id)["claims"] == old_claims
    assert pipeline.get_final_version(case_id, persona_id) is None
    assert counts(pipeline)["operator_decisions"] == 0
    groups = pipeline.get_workspace(case_id, persona_id)["groups"]
    account_claims = [
        group
        for group in groups
        if group["kind"] == "claim"
        and group["normalized"].get("predicate") == "social_account"
    ]
    assert len(account_claims) == 1
    before = counts(pipeline)
    bootstrap_legacy_workspace(store, case_id, persona_id)
    assert counts(pipeline) == before


def test_attaching_a_plan_does_not_hide_historical_source_records(context):
    store, pipeline, case_id, persona_id, job_id = context
    pipeline.create_request(
        case_id,
        persona_id,
        [{"type": "username", "value": "synthetic-alice"}],
        {"pipeline_id": "p2-e2e-v1", "tasks": []},
        actor="case-operator",
        job_id=job_id,
    )
    report = bootstrap_legacy_workspace(store, case_id, persona_id)
    assert report["job_count"] == 1
    docs = list(pipeline.iter_observations(case_id, persona_id))
    assert any(doc["engine"] == "maigret_report" for doc in docs)
    assert any(doc["status"] == "blocked" for doc in docs)
    assert pipeline.get_final_version(case_id, persona_id) is None


def test_primary_request_preserves_initial_supplied_claims_without_promotion(tmp_path):
    store = CaseStore(f"sqlite:///{tmp_path}/primary-claims.db", create_schema=True)
    try:
        job_id = store.create_investigation(
            ["synthetic-alice"],
            {
                "investigation_spec": {
                    "processing_mode": "same_subject",
                    "subject_label": "Synthetic Alice",
                    "identifiers": [
                        {"type": "username", "value": "synthetic-alice"},
                        {
                            "type": "profile_url",
                            "value": "https://github.com/synthetic-alice",
                        },
                    ],
                }
            },
        )
        pipeline = PipelineStore(store)
        case_id = store.get_job(job_id)["case_id"]
        persona_id = store.get_case(case_id)["personas"][0]["id"]
        # This is a genuine new submission; the request exists before collection.
        assert pipeline.requests_for_job(job_id)
        initial = store.get_persona(persona_id)["claims"]
        assert len(initial) == 1
        report = bootstrap_legacy_workspace(store, case_id, persona_id)
        assert report["claim_count"] == 1
        assert report["job_count"] == 0
        docs = list(pipeline.iter_observations(case_id, persona_id))
        assert {doc["payload"].get("legacy_claim_id") for doc in docs} == {
            initial[0]["id"]
        }
        assert all(doc["payload"]["legacy_review_status"] == "pending" for doc in docs)
        assert pipeline.get_workspace(case_id, persona_id)["groups"]
        assert counts(pipeline)["operator_decisions"] == 0
        assert pipeline.get_final_version(case_id, persona_id) is None
    finally:
        store.dispose()


def test_native_pipeline_projection_is_not_imported_as_another_source(
    tmp_path, legacy_result
):
    store = CaseStore(f"sqlite:///{tmp_path}/native-projection.db", create_schema=True)
    try:
        job_id = store.create_investigation(["synthetic-alice"], {})
        pipeline = PipelineStore(store)
        assert pipeline.requests_for_job(job_id)
        store.claim_next("worker:fixture")
        store.finish(job_id, {**legacy_result, "pipeline_id": "p2-e2e-v1"})
        case_id = store.get_job(job_id)["case_id"]
        persona_id = store.get_case(case_id)["personas"][0]["id"]
        report = bootstrap_legacy_workspace(store, case_id, persona_id)
        assert report["job_count"] == 0
        assert not list(pipeline.iter_observations(case_id, persona_id))
    finally:
        store.dispose()


def test_bootstrap_new_chat_proposal_and_history_are_discovered(context):
    store, pipeline, case_id, persona_id, _ = context
    bootstrap_legacy_workspace(store, case_id, persona_id)
    message_id = store.append_case_chat_message(
        case_id,
        role="assistant",
        author="assistant",
        content="A proposal with insufficient independent evidence.",
        persona_id=persona_id,
        sources=[{"url": "https://example.test/alice", "title": "Profile"}],
        model="fixture",
    )
    message_id = message_id["id"]
    report = bootstrap_legacy_workspace(store, case_id, persona_id)
    assert report["chat_count"] == 1
    doc = next(
        item
        for item in pipeline.iter_observations(case_id, persona_id)
        if item["payload"].get("legacy_chat_message_id") == message_id
    )
    assert doc["engine"] == "case_chat"
    assert doc["origin_family_id"] is None
    assert doc["claims"] == []
    assert (
        doc["payload"]["message"]["sources"][0]["url"] == "https://example.test/alice"
    )
    before = counts(pipeline)
    bootstrap_legacy_workspace(store, case_id, persona_id)
    assert counts(pipeline) == before


def test_bootstrap_later_cited_ai_claim_keeps_pending_original_lineage(context):
    store, pipeline, case_id, persona_id, job_id = context
    bootstrap_legacy_workspace(store, case_id, persona_id)
    synced = store.sync_ai_persona_claims(
        job_id,
        [
            {
                "username": "synthetic-alice",
                "field_name": "summary",
                "value": "Alice is a researcher.",
                "source_url": "https://research.example/alice",
                "confidence": 80,
                "reason": "Source states this biography.",
            }
        ],
        sources=[
            {"url": "https://research.example/alice", "title": "Research profile"}
        ],
        usernames=["synthetic-alice"],
        model="fixture",
    )
    updated = ingest_legacy_claim_updates(store, case_id, persona_id)
    assert updated["pending_claim_count"] == 1
    old_claim = next(
        claim
        for claim in store.get_persona(persona_id)["claims"]
        if claim["field_name"] == "summary"
    )
    imported = next(
        doc
        for doc in pipeline.iter_observations(case_id, persona_id)
        if doc["payload"].get("legacy_claim_id") == old_claim["id"]
    )
    assert imported["payload"]["legacy_review_status"] == "pending"
    assert (
        imported["payload"]["claims"][0]["source_engine"] == old_claim["source_engine"]
    )
    assert not imported["evidence_signals"]
    assert counts(pipeline)["operator_decisions"] == 0


def test_failed_import_resumes_checkpoint_without_duplicating_sources(
    context, monkeypatch
):
    store, pipeline, case_id, persona_id, _ = context
    original = PipelineStore.record_observations
    failed = False

    def interrupt_once(self, attempt_id, observations, **kwargs):
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("Simulated process interruption")
        return original(self, attempt_id, observations, **kwargs)

    monkeypatch.setattr(PipelineStore, "record_observations", interrupt_once)
    with pytest.raises(RuntimeError, match="interruption"):
        bootstrap_legacy_workspace(store, case_id, persona_id)
    bootstrap_legacy_workspace(store, case_id, persona_id)
    before = counts(pipeline)
    bootstrap_legacy_workspace(store, case_id, persona_id)
    assert counts(pipeline) == before
    assert before["observations"] > 0


def test_bootstrap_scope_rejects_foreign_persona_before_writes(context):
    before = counts(context[1])
    with pytest.raises(KeyError):
        bootstrap_legacy_workspace(context[0], context[2], "foreign-persona")
    assert counts(context[1]) == before
