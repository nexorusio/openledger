"""Reproductions of real QC, retention and lineage review failures; no network."""

import json

import pytest

from maigret.web.pipeline_consolidation import consolidate_observations
from maigret.web.pipeline_evidence import (
    NORMALIZER_VERSION,
    ObservationContractError,
    iter_result_observations,
    normalize_observation,
    resolve_retention,
)
from maigret.web.pipeline_routes import version_graph, version_projection
from tests.test_pipeline_consolidation import SCOPE, observation
from tests.test_pipeline_store import approve, pair, query, curated  # noqa: F401
from tests.test_pipeline_routes import journey  # noqa: F401


def _materialize(pair, raws):
    pipeline = pair[1]
    request = query(pair)
    attempt = pipeline.start_attempt(request["tasks"][0]["id"], "worker:test")
    scope = dict(
        case_id=request["case_id"],
        subject_id=request["persona_id"],
        request_id=request["id"],
        task_id=request["tasks"][0]["id"],
        attempt_id=attempt["id"],
        observed_at=attempt["created_at"],
        engine="fixture",
    )
    docs = [normalize_observation(raw, **scope) for raw in raws]
    stored = pipeline.record_observations(
        attempt["id"], docs, outcome="found", worker_id="worker:test"
    )["observations"]
    groups = pipeline.upsert_groups(
        request["case_id"],
        request["persona_id"],
        consolidate_observations(docs),
        projection_revision=pipeline.projection_revision(
            request["case_id"], request["persona_id"]
        ),
    )
    for group in groups:
        pipeline.decide(
            request["case_id"],
            request["persona_id"],
            group["id"],
            "include",
            actor="operator",
            reason="Fixture sources identify this subject",
        )
    return request, stored, groups


def _facts(predicate="birth_date", values=("1980-01-01", "1990-01-01")):
    return [
        dict(
            id=str(i),
            status="found",
            source_url=f"https://evidence.example/{i}",
            account={"platform": platform, "url": url},
            claims=[{"predicate": predicate, "value": value}],
        )
        for i, (platform, url, value) in enumerate(
            zip(
                ("github", "instagram"),
                ("https://github.com/fixture", "https://www.instagram.com/fixture/"),
                values,
            )
        )
    ]


def _version(pair, request):
    return pair[1].create_version(
        request["case_id"],
        request["persona_id"],
        actor="operator",
        scope="Verify source assertions",
    )


def test_cross_account_birth_dates_block_qc_until_operator_resolves(pair):
    request, _, groups = _materialize(pair, _facts())
    version = _version(pair, request)
    with pytest.raises(ValueError, match="incompatible subject facts") as error:
        approve(pair[1], version)
    conflict = next(
        f
        for f in error.value.findings
        if f.get("kind") == "contradictory_attributed_subject_values"
    )
    assert len(conflict["group_ids"]) == 2
    assert not any(
        g["normalized"].get("conflicts") for g in groups
    )  # Collection hypotheses remain distinct.
    wrong = next(
        g
        for g in groups
        if g["kind"] == "claim" and g["normalized"]["value"] == "1990-01-01"
    )
    pair[1].decide(
        request["case_id"],
        request["persona_id"],
        wrong["id"],
        "exclude",
        actor="operator",
        reason="Source refers to another date",
    )
    assert approve(pair[1], _version(pair, request))["status"] == "approved"


def test_date_aliases_cannot_bypass_subject_consistency(pair):
    facts = _facts()
    facts[1]["claims"][0]["predicate"] = "date_of_birth"
    request, _, _ = _materialize(pair, facts)
    with pytest.raises(ValueError, match="incompatible subject facts"):
        approve(pair[1], _version(pair, request))


@pytest.mark.parametrize("values", [("1980", "1980-01-01"), ("1980-01", "1980-01-11")])
def test_compatible_date_precision_is_not_a_conflict(pair, values):
    request, _, _ = _materialize(pair, _facts(values=values))
    assert approve(pair[1], _version(pair, request))["status"] == "approved"


def test_temporal_affiliations_are_not_assumed_single_valued(pair):
    facts = _facts("affiliation", ("Organization A", "Organization B"))
    facts[0]["claims"][0].update(valid_from="2015-01-01", valid_to="2020-01-01")
    facts[1]["claims"][0].update(valid_from="2021-01-01", valid_to="2026-01-01")
    request, _, _ = _materialize(pair, facts)
    assert approve(pair[1], _version(pair, request))["status"] == "approved"


@pytest.mark.parametrize(
    "mode", ["metadata_only", "transient", "live_only", "prohibited"]
)
def test_restricted_payload_never_reaches_ledger_or_export(pair, mode):
    doc = observation(
        retention={"mode": mode, "final_eligible": False},
        content="FORBIDDEN_DETAIL",
        artifact_ref="FORBIDDEN_ARTIFACT",
        source_url="https://restricted.example/FORBIDDEN_LOCATOR",
        content_hash="FORBIDDEN_HASH",
        claims=[{"predicate": "occupation", "value": "FORBIDDEN_CLAIM"}],
    )
    assert doc["retention"]["mode"] == mode
    assert not doc["retention"]["final_eligible"]
    assert doc["account"] is None and not doc["claims"]
    assert "FORBIDDEN" not in json.dumps(doc)
    request = query(pair)
    attempt = pair[1].start_attempt(request["tasks"][0]["id"], "worker:test")
    # Exercise a direct ledger caller that bypasses normalize_observation.
    stored = pair[1].record_observations(
        attempt["id"],
        [
            {
                "id": "restricted-source",
                "status": "found",
                "source_url": "https://restricted.example/FORBIDDEN_URL",
                "content": "FORBIDDEN_NATIVE",
                "payload": {"secret": "FORBIDDEN_NESTED"},
                "retention": {"mode": mode},
                "claims": [{"predicate": "occupation", "value": "FORBIDDEN_CLAIM"}],
            }
        ],
        outcome="found",
        worker_id="worker:test",
    )["observations"][0]
    assert not stored["retained"] and "FORBIDDEN" not in json.dumps(stored)
    groups = pair[1].upsert_groups(
        request["case_id"],
        request["persona_id"],
        {
            "claims": [
                {
                    "id": "fabricated",
                    "predicate": "occupation",
                    "value": "Engineer",
                    "observation_ids": [stored["id"]],
                }
            ]
        },
        projection_revision=pair[1].projection_revision(
            request["case_id"], request["persona_id"]
        ),
    )
    pair[1].decide(
        request["case_id"],
        request["persona_id"],
        groups[0]["id"],
        "include",
        actor="operator",
        reason="Fixture attempts to bypass retention",
    )
    version = _version(pair, request)
    with pytest.raises(ValueError, match="lacks retained"):
        approve(pair[1], version)
    assert "FORBIDDEN" not in json.dumps(version_projection(version))


def test_retention_policy_intersection_and_unknown_modes():
    assert (
        resolve_retention("prohibited", {"mode": "retained"}, has_locator=True)["mode"]
        == "prohibited"
    )
    assert (
        resolve_retention("bounded_source_evidence", {"mode": "live_only"})["mode"]
        == "live_only"
    )
    assert not resolve_retention({"final_eligible": False}, has_locator=True)[
        "final_eligible"
    ]
    with pytest.raises(ObservationContractError, match="Unknown retention"):
        normalize_observation(
            {"status": "found", "retention": {"mode": "maybe"}}, **SCOPE
        )


@pytest.mark.parametrize(
    "field",
    [
        "place_id",
        "source_record_id",
        "legacy",
        "id",
        "engine_version",
        "parser_version",
        "observed_at",
    ],
)
def test_restricted_metadata_cannot_hide_nested_payload(pair, field):
    raw = {
        "id": "restricted-metadata",
        "status": "found",
        "retention": {"mode": "metadata_only"},
        field: {"secret": "FORBIDDEN_NESTED_PAYLOAD"},
    }
    try:
        normalized = normalize_observation(raw, **SCOPE)
    except ObservationContractError:
        pass
    else:
        assert "FORBIDDEN" not in json.dumps(normalized)
    request = query(pair)
    attempt = pair[1].start_attempt(request["tasks"][0]["id"], "worker:test")
    try:
        pair[1].append_observations(attempt["id"], [raw], worker_id="worker:test")
    except ObservationContractError:
        pass
    assert "FORBIDDEN" not in json.dumps(
        pair[1].list_observations(request["case_id"], request["persona_id"])
    )


def test_task_retention_and_versions_flow_through_result_stream():
    docs = list(
        iter_result_observations(
            {
                "source_observations": [
                    {
                        "status": "found",
                        "source_url": "https://example.com/secret",
                        "content": "FORBIDDEN",
                    }
                ]
            },
            **SCOPE,
            retention_policy="transient_display_only",
            engine_version="adapter-3",
            parser_version="decoder-2",
        )
    )
    assert docs[0]["retention"]["mode"] == "transient"
    assert (
        docs[0]["engine_version"] == "adapter-3"
        and docs[0]["parser_version"] == "decoder-2"
    )
    assert docs[0]["normalizer_version"] == NORMALIZER_VERSION
    assert "FORBIDDEN" not in json.dumps(docs)


def test_final_ineligible_retained_observation_cannot_approve(pair):
    facts = _facts("occupation", ("Engineer", "Engineer"))[:1]
    facts[0]["retention"] = {"mode": "retained", "final_eligible": False}
    request, _, _ = _materialize(pair, facts)
    with pytest.raises(ValueError, match="lacks retained"):
        approve(pair[1], _version(pair, request))


def test_explicit_shared_origin_takes_precedence_and_account_url_is_separate():
    docs = [
        observation(
            "source-" + str(i),
            source_url=f"https://mirror.example/{i}",
            origin_family_id="upstream:dataset-row-1",
            account={"platform": "github", "url": "https://github.com/alice"},
        )
        for i in range(2)
    ]
    assert {d["origin_family_id"] for d in docs} == {"upstream:dataset-row-1"}
    assert all(
        d["account"]["canonical_url"] == "https://github.com/alice" for d in docs
    )
    assert (
        consolidate_observations(docs)["accounts"][0]["independent_origin_count"] == 1
    )


def test_final_graph_distinguishes_provenance_absence_failures_and_contradictions(pair):
    facts = _facts("occupation", ("Engineer", "Engineer"))[:1]
    for status, role in (
        ("not_found", "supports"),
        ("timeout", "supports"),
        ("found", "contradicts"),
    ):
        facts.append(
            {
                **facts[0],
                "id": status + role,
                "status": status,
                "evidence_role": role,
                "claims": [],
            }
        )
    request, stored, _ = _materialize(pair, facts)
    version = _version(pair, request)
    with pytest.raises(ValueError, match="contradictory observations"):
        approve(pair[1], version)
    graph = version_graph(version_projection(version))
    for row in stored:
        roles = {
            e["kind"] for e in graph["edges"] if e.get("observation_id") == row["id"]
        }
        assert "provenance" in roles
        if row["outcome"] == "not_found":
            assert "absence_observation" in roles and "supports" not in roles
        elif row["outcome"] == "timeout":
            assert "collection_outcome" in roles and "supports" not in roles
        elif row["payload"].get("payload", {}).get("evidence_role") == "contradicts":
            assert "contradicts" in roles and "supports" not in roles


def test_contradictory_assertion_cannot_be_the_only_qc_support(pair):
    facts = _facts("occupation", ("Engineer", "Engineer"))[:1]
    facts[0]["evidence_role"] = "contradicts"
    request, _, _ = _materialize(pair, facts)
    with pytest.raises(ValueError, match="lacks retained"):
        approve(pair[1], _version(pair, request))


def test_connector_actor_cannot_curate_or_qc(pair):
    request, _, group, version = curated(pair)
    with pytest.raises(PermissionError, match="human actor"):
        pair[1].decide(
            request["case_id"],
            request["persona_id"],
            group["id"],
            "include",
            actor="connector:feed",
            reason="Machine attempts curation",
        )
    with pytest.raises(PermissionError, match="human actor"):
        pair[1].qc(
            version["id"],
            "approved",
            actor="connector:feed",
            permissions=["persona:qc"],
            expected_hash=version["content_hash"],
        )


def test_operator_can_resolve_and_reopen_contradictory_evidence_without_erasing_it(
    pair,
):
    facts = _facts("occupation", ("Engineer", "Engineer"))[:1]
    facts.append({**facts[0], "id": "refutation", "evidence_role": "contradicts"})
    request, stored, groups = _materialize(pair, facts)
    wrong = next(
        row
        for row in stored
        if row["payload"]["payload"].get("evidence_role") == "contradicts"
    )
    with pytest.raises(ValueError, match="contradictory observations"):
        approve(pair[1], _version(pair, request))
    for group in groups:
        decision = pair[1].decide(
            request["case_id"],
            request["persona_id"],
            group["id"],
            "include",
            actor="operator",
            reason="Record review of conflicting original source",
            evidence_dispositions=[
                {
                    "observation_id": wrong["id"],
                    "disposition": "exclude_from_support",
                    "reason": "Retraction concerns a different person; cited subject binding is erroneous",
                }
            ],
        )
        assert (
            decision["details"]["evidence_dispositions"][0]["reviewed_content_hash"]
            == wrong["content_hash"]
        )
    final = approve(pair[1], _version(pair, request))
    assert all(item["probability"] is None for item in final["manifest"]["items"])
    assert any(
        e["id"] == wrong["id"] and e["payload"] == wrong["payload"]
        for item in final["manifest"]["items"]
        for e in item["evidence"]
    )
    graph = version_graph(version_projection(final))
    assert {
        edge["kind"]
        for edge in graph["edges"]
        if edge.get("observation_id") == wrong["id"]
    } == {"provenance", "excluded_evidence"}
    import shutil
    import subprocess
    from maigret.web.pipeline_pdf import generate_pipeline_pdf

    converter = shutil.which("pdftotext")
    if converter:
        rendered = generate_pipeline_pdf(version_projection(final))
        extracted = subprocess.run(
            [converter, "-", "-"], input=rendered, capture_output=True, check=True
        ).stdout.decode()
        assert "excluded evidence" in extracted
        assert "Use of this observation in each curated group" in extracted
        assert "Retraction concerns a different person" in extracted
    pair[1].decide(
        request["case_id"],
        request["persona_id"],
        groups[0]["id"],
        "include",
        actor="operator",
        reason="Reopen prior binding analysis",
        evidence_dispositions=[
            {
                "observation_id": wrong["id"],
                "disposition": "restore_support",
                "reason": "New source questions the exclusion; research needed",
            }
        ],
    )
    with pytest.raises(ValueError, match="contradictory observations"):
        approve(pair[1], _version(pair, request))


def test_disposition_requires_group_member_and_specific_reason(pair):
    request, _, group, _ = curated(pair)
    with pytest.raises(ValueError, match="outside the reviewed group"):
        pair[1].decide(
            request["case_id"],
            request["persona_id"],
            group["id"],
            "include",
            actor="operator",
            reason="Attempt cross-group evidence exclusion",
            evidence_dispositions=[
                {
                    "observation_id": "missing",
                    "disposition": "exclude_from_support",
                    "reason": "Not this group's evidence",
                }
            ],
        )


def test_evidence_disposition_is_available_in_operator_form_and_json(journey):
    path = f"/cases/{journey['case_id']}/pipeline/{journey['persona_id']}/groups/{journey['group_id']}"
    page = journey["client"].get(path)
    assert page.status_code == 200 and b"evidence_observation_id" in page.data
    response = journey["client"].post(
        path + "/decision",
        headers={"X-OpenLedger-CSRF": "test-csrf"},
        json={
            "csrf_token": "test-csrf",
            "decision": "include",
            "reason": "Review original source",
            "evidence_dispositions": [
                {
                    "observation_id": journey["observation_id"],
                    "disposition": "exclude_from_support",
                    "reason": "Source did not establish this subject binding",
                }
            ],
        },
    )
    assert response.status_code == 201
    assert journey["client"].get(path).status_code == 200
    group = journey["pipeline"].get_group(
        journey["case_id"], journey["persona_id"], journey["group_id"]
    )
    assert (
        group["latest_decision"]["details"]["evidence_dispositions"][0][
            "observation_id"
        ]
        == journey["observation_id"]
    )
    response = journey["client"].post(
        path + "/decision",
        data={
            "csrf_token": "test-csrf",
            "decision": "include",
            "reason": "Reconsider source after research",
            "evidence_observation_id": journey["observation_id"],
            "evidence_disposition": "restore_support",
            "evidence_reason": "Newly reviewed original record resolves the prior ambiguity",
        },
    )
    assert response.status_code == 303
    group = journey["pipeline"].get_group(
        journey["case_id"], journey["persona_id"], journey["group_id"]
    )
    assert not group["latest_decision"]["details"].get("evidence_dispositions")


def test_generic_refutation_cannot_inflate_assessment_support_features():
    from tests.test_pipeline_assessment import (
        assess,
        observation as assessment_observation,
    )

    row = assessment_observation()
    row["evidence_role"] = "contradicts"
    result = assess([row])
    assert result["features"]["support_origin_families"] == 0
    assert result["contradictions"][0]["assertion_role"] == "contradicts"
    assert result["probability"]["value"] is None
    assert (
        result["probability"]["reason"] == "unresolved_contradictory_source_assertion"
    )
