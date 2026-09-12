"""Combined-case adapters preserve source scope and await pipeline publication."""

import asyncio
import copy
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, JSON, MetaData, String, Table, insert

from maigret.web import pipeline_case_fusion as fusion
from maigret.web.case_store import CaseStore
from maigret.web.pipeline_contract import ENGINE_REGISTRY, PIPELINE_ID, canonical_digest


def source_version(case_id="left", persona_id="person-left", value="Example Ltd"):
    manifest = {
        "case_id": case_id,
        "persona_id": persona_id,
        "items": [
            {
                "group_id": "claim-" + persona_id,
                "kind": "claim",
                "normalized": {"predicate": "company", "value": value},
                "decision": {"id": "decision-" + persona_id, "decision": "include"},
                "evidence": [
                    {
                        "id": "evidence-" + persona_id,
                        "engine": "official_website_public_content",
                        "source_url": "https://example.org/about",
                        "payload": {"retained": "source bytes"},
                    }
                ],
            }
        ],
        "exclusions": [
            {"group_id": "rejected-" + persona_id, "normalized": {"value": "Rejected"}}
        ],
        "scope": "Public company affiliation",
        "limitations": ["As of source date"],
    }
    return {
        "id": "version-" + persona_id,
        "case_id": case_id,
        "persona_id": persona_id,
        "persona_name": persona_id,
        "sequence": 2,
        "manifest": manifest,
        "content_hash": canonical_digest(manifest).removeprefix("sha256:"),
    }


def source_snapshot():
    return {
        "snapshot": {
            "source_cases": [
                {"id": "left", "title": "Left"},
                {"id": "right", "title": "Right"},
            ],
            "claims": [{"claim_id": "legacy-claim", "evidence": ["legacy-source"]}],
            "generated_at": "2026-09-11T00:00:00+00:00",
            "sha256": "0" * 64,
        },
        "relationship_graph": {
            "nodes": [],
            "edges": [],
            "stats": {"connection_count": 0},
        },
        "analysis_context": {
            "source_cases": [{"id": "left"}, {"id": "right"}],
            "entities": [],
            "approved_claims": [],
            "approved_organizations": [],
        },
    }


def snapshot_job():
    return {
        "job_id": "snapshot-job",
        "kind": "case_fusion",
        "case_id": "combined",
        "options": {
            "investigation_spec": {
                "source_case_ids": ["left", "right"],
                "evidence_scope": "approved_only",
            }
        },
    }


def prepare(versions=None, raw=None):
    original = raw or source_snapshot()
    store = SimpleNamespace(build_case_fusion_snapshot=lambda _: original)
    return fusion.prepare_case_fusion_snapshot(
        store,
        snapshot_job(),
        pipeline=object(),
        version_reader=lambda *_: (
            versions if versions is not None else [source_version()]
        ),
    )


def test_snapshot_retains_exact_manifests_and_does_not_merge_equal_source_claims():
    versions = [source_version(), source_version("right", "person-right")]
    original = source_snapshot()
    original_copy = copy.deepcopy(original)
    prepared = prepare(versions, original)
    result = prepared["snapshot_result"]
    manifest = result["snapshot"]
    assert original == original_copy
    assert manifest["pipeline_id"] == PIPELINE_ID
    assert manifest["claims"] == original["snapshot"]["claims"]
    assert [row["manifest"] for row in manifest["pipeline_versions"]] == [
        row["manifest"] for row in versions
    ]
    assert len(manifest["pipeline_claims"]) == 2
    assert len({row["reference_id"] for row in manifest["pipeline_claims"]}) == 2
    assert all(
        row["normalized"]["value"] == "Example Ltd"
        for row in manifest["pipeline_claims"]
    )
    assert all(
        row["normalized"]["value"] != "Rejected" for row in manifest["pipeline_claims"]
    )
    assert all(
        edge["identity_scope"] == "source_version_fact"
        for edge in result["relationship_graph"]["edges"]
    )
    assert all(
        "from" in edge and "to" in edge
        for edge in result["relationship_graph"]["edges"]
    )
    assert all(row["independence"] == "derivative" for row in prepared["observations"])
    assert (
        prepared["observations"][0]["payload"]["source_item"]
        == versions[0]["manifest"]["items"][0]
    )
    assert prepared["analysis_context"]["snapshot_sha256"] == manifest["sha256"]


def test_snapshot_digest_binds_exact_source_lineage_but_not_capture_time():
    first = prepare()
    later = source_snapshot()
    later["snapshot"]["generated_at"] = "2026-09-12T00:00:00+00:00"
    assert (
        prepare(raw=later)["snapshot_result"]["snapshot"]["sha256"]
        == first["snapshot_result"]["snapshot"]["sha256"]
    )
    changed = source_version(value="Changed Ltd")
    assert (
        prepare([changed])["snapshot_result"]["snapshot"]["sha256"]
        != first["snapshot_result"]["snapshot"]["sha256"]
    )


def test_snapshot_refuses_out_of_scope_or_corrupted_final_version():
    with pytest.raises(ValueError, match="outside"):
        prepare([source_version("unselected")])
    corrupt = source_version()
    corrupt["manifest"]["items"][0]["normalized"]["value"] = "Altered"
    with pytest.raises(ValueError, match="digest"):
        prepare([corrupt])


def test_database_capture_selects_only_final_source_versions_inside_snapshot_transaction(
    tmp_path, monkeypatch
):
    store = CaseStore(f"sqlite:///{tmp_path / 'fusion.db'}", create_schema=True)
    try:
        selected = []
        people = []
        for username in ("left", "right", "unselected"):
            job_id = store.create_investigation([username], {})
            store.claim_next("worker:" + username)
            store.finish(
                job_id,
                {
                    "status": "completed",
                    "individual_reports": [],
                    "usernames": [username],
                },
            )
            case_id = store.get_job(job_id)["case_id"]
            selected.append(case_id)
            people.append(store.get_case(case_id)["personas"][0]["id"])
        schema = MetaData()
        versions = Table(
            "test_final_versions",
            schema,
            Column("id", String, primary_key=True),
            Column("case_id", String),
            Column("persona_id", String),
            Column("sequence", Integer),
            Column("manifest", JSON),
            Column("content_hash", String),
        )
        states = Table(
            "test_final_states",
            schema,
            Column("persona_id", String, primary_key=True),
            Column("final_version_id", String),
            Column("final_status", String),
        )
        schema.create_all(store.engine)
        with store.engine.begin() as connection:
            for index, (case_id, persona_id) in enumerate(zip(selected, people)):
                version = source_version(case_id, persona_id)
                version.pop("persona_name")
                connection.execute(insert(versions).values(**version))
                connection.execute(
                    insert(states).values(
                        persona_id=persona_id,
                        final_version_id=version["id"],
                        final_status="withdrawn" if index == 1 else "final",
                    )
                )
            draft = source_version(selected[0], people[0])
            draft.pop("persona_name")
            draft.update(id="unapproved-draft", sequence=3)
            connection.execute(insert(versions).values(**draft))
        job_id = store.create_combined_investigation(
            selected[:2],
            title="Combined",
            purpose="Compare reviewed evidence",
            created_by="analyst",
        )
        pipeline = SimpleNamespace(
            _table=lambda name: {"persona_versions": versions, "persona_state": states}[
                name
            ]
        )
        original_reader = fusion.read_final_source_versions
        reads = []

        def read(*args, connection=None, **kwargs):
            assert connection is not None and connection.in_transaction()
            reads.append(connection)
            return original_reader(*args, connection=connection, **kwargs)

        monkeypatch.setattr(fusion, "read_final_source_versions", read)
        prepared = fusion.prepare_case_fusion_snapshot(
            store, store.get_job(job_id), pipeline=pipeline
        )
        captured = prepared["snapshot_result"]["snapshot"]["pipeline_versions"]
        assert len(reads) == 1
        assert [row["version_id"] for row in captured] == ["version-" + people[0]]
        assert captured[0]["manifest"]["items"][0]["evidence"][0]["payload"] == {
            "retained": "source bytes"
        }
    finally:
        store.dispose()


class Context:
    def __init__(self, *, stop_after=None):
        self.job = snapshot_job()
        self.pipeline = object()
        self.store = SimpleNamespace()
        self.observations = []
        self.stop_after = stop_after
        self.context = {}

    def cancelled(self):
        return self.stop_after is not None and len(self.observations) >= self.stop_after

    def emit_observations(self, rows):
        self.observations.extend(copy.deepcopy(rows))


def test_snapshot_adapter_commits_marker_and_leaves_publication_to_coordinator(
    monkeypatch,
):
    context = Context()
    prepared = prepare()
    monkeypatch.setattr(
        fusion, "prepare_case_fusion_snapshot", lambda *args, **kwargs: prepared
    )
    result = asyncio.run(fusion.collect_case_fusion_snapshot({}, context))
    assert result["outcome"] == "candidate"
    assert len(context.observations) == 2
    marker = context.observations[-1]
    assert marker["source_record_id"] == "snapshot-publication"
    assert marker["payload"]["publication"] == context.pending_publication
    assert marker["payload"]["publication_digest"] == canonical_digest(
        context.pending_publication
    )
    assert not hasattr(context.store, "publish_case_fusion_snapshot")


def test_snapshot_cancel_preserves_committed_claims_without_publication(monkeypatch):
    context = Context(stop_after=1)
    prepared = prepare([source_version(), source_version("right", "person-right")])
    monkeypatch.setattr(
        fusion, "prepare_case_fusion_snapshot", lambda *args, **kwargs: prepared
    )
    assert (
        asyncio.run(fusion.collect_case_fusion_snapshot({}, context))["outcome"]
        == "cancelled"
    )
    assert len(context.observations) == 1
    assert not hasattr(context, "pending_publication")


def test_snapshot_cancel_after_last_source_still_does_not_emit_publication(monkeypatch):
    context = Context(stop_after=1)
    prepared = prepare()
    monkeypatch.setattr(
        fusion, "prepare_case_fusion_snapshot", lambda *args, **kwargs: prepared
    )
    assert (
        asyncio.run(fusion.collect_case_fusion_snapshot({}, context))["outcome"]
        == "cancelled"
    )
    assert len(context.observations) == 1
    assert not hasattr(context, "pending_publication")


def synthesis_context():
    context = Context()
    context.job = {
        "job_id": "synthesis-job",
        "kind": "case_fusion_ai",
        "case_id": "combined",
    }
    prepared = prepare([source_version(), source_version("right", "person-right")])
    snapshot = prepared["snapshot_result"]
    digest = snapshot["snapshot"]["sha256"]
    context.context = {
        "snapshot_job_id": "snapshot-job",
        "snapshot_sha256": digest,
        "validated_snapshot_reference": True,
        "analysis_context": prepared["analysis_context"],
    }
    context.stopped = []
    context.store = SimpleNamespace(
        get_job=lambda _: {
            "job_id": "snapshot-job",
            "kind": "case_fusion",
            "case_id": "combined",
            "status": "completed",
            **snapshot,
        },
        start_combined_analysis_run=lambda *args, **kwargs: "analysis-run",
        stop_combined_analysis_run=lambda *args, **kwargs: context.stopped.append(
            (args, kwargs)
        ),
    )
    return context


def app(key="key"):
    return SimpleNamespace(
        get_openai_api_key=lambda: key,
        load_settings=lambda: {},
        DEFAULT_SETTINGS={"openai_model": "test-model"},
        ai_endpoint_options=lambda: {},
    )


async def research(**kwargs):
    assert all(
        "confidence" not in claim for claim in kwargs["case_context"]["approved_claims"]
    )
    return {
        "analysis": "Supported comparison",
        "sources": [],
        "web_search_completed": True,
    }


async def insights(**kwargs):
    context = kwargs["case_context"]
    claims = context["approved_claims"]
    return {
        "executive_summary": "Human review required",
        "proposals": [
            {
                "subject_ref": claims[0]["entity_ref"],
                "object_ref": claims[1]["entity_ref"],
                "relationship_type": "affiliation",
                "title": "Shared company claim",
                "explanation": "Source versions independently include this affiliation; identity remains distinct.",
                "confidence": 60,
                "evidence_reference_ids": [row["reference_id"] for row in claims],
            }
        ],
    }


def collect(context, *, research_call=research, insights_call=insights, key="key"):
    return asyncio.run(
        fusion.collect_case_fusion_synthesis(
            {"timeout_seconds": 420},
            context,
            app_module=app(key),
            research_call=research_call,
            insights_call=insights_call,
        )
    )


def test_synthesis_uses_async_primitives_and_retains_exact_pending_payload():
    context = synthesis_context()
    result = collect(context)
    assert result["outcome"] == "candidate"
    assert result["proposal_count"] == 1
    assert not context.stopped
    assert context.pending_publication["kind"] == "case_fusion_ai"
    assert context.pending_publication["run_id"] == "analysis-run"
    marker = context.observations[-1]
    assert marker["source_record_id"] == "synthesis-publication"
    assert marker["payload"]["publication"] == context.pending_publication
    assert not hasattr(context.store, "complete_combined_analysis_run")
    assert context.observations[0]["payload"]["source_version_refs"]
    assert ENGINE_REGISTRY["case_fusion_synthesis"].retry_ceiling == 0


def test_synthesis_rejects_invalid_snapshot_before_model_or_analysis_run():
    context = synthesis_context()
    context.context["snapshot_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="exact published"):
        collect(context)
    assert context.observations == []
    assert collect(synthesis_context(), key=None)["outcome"] == "not_executed"


def test_synthesis_failure_retains_research_without_publishing():
    context = synthesis_context()

    async def fail(**kwargs):
        raise RuntimeError("Provider interrupted")

    with pytest.raises(RuntimeError, match="Provider interrupted"):
        collect(context, insights_call=fail)
    assert len(context.observations) == 1
    assert context.stopped[0][1]["status"] == "failed"
    assert not hasattr(context, "pending_publication")


def test_synthesis_cancel_retains_research_without_publishing():
    context = synthesis_context()
    context.stop_after = 1
    with pytest.raises(asyncio.CancelledError):
        collect(context)
    assert len(context.observations) == 1
    assert context.stopped[0][1]["status"] == "cancelled"
    assert not hasattr(context, "pending_publication")


def test_publication_recovery_is_bound_to_request_completed_attempt_and_digest():
    context = synthesis_context()
    collect(context)
    marker = context.observations[-1]
    normalized = {
        "request_id": "request",
        "attempt_id": "attempt",
        "engine": "case_fusion_synthesis",
        "payload": marker,
    }
    assert (
        fusion.recover_pending_publication(
            [normalized],
            kind="case_fusion_ai",
            request_id="request",
            completed_attempt_ids=["attempt"],
        )
        == context.pending_publication
    )
    assert (
        fusion.recover_pending_publication(
            [normalized],
            kind="case_fusion_ai",
            request_id="other",
            completed_attempt_ids=["attempt"],
        )
        is None
    )
    assert (
        fusion.recover_pending_publication(
            [normalized],
            kind="case_fusion_ai",
            request_id="request",
            completed_attempt_ids=[],
        )
        is None
    )
    normalized["payload"]["payload"]["publication"]["insights"][
        "executive_summary"
    ] = "Altered"
    with pytest.raises(ValueError, match="digest"):
        fusion.recover_pending_publication(
            [normalized],
            kind="case_fusion_ai",
            request_id="request",
            completed_attempt_ids=["attempt"],
        )
