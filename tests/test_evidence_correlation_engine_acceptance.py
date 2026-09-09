# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Black-box acceptance for P3b correlation and Persona reuse."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from pathlib import Path

import pytest

from maigret.web.case_store import CaseStore
from maigret.web.evidence_correlation import correlate_evidence
from maigret.web.evidence_correlation_contract import (
    EVIDENCE_CORRELATION_SCHEMA_VERSION,
    CorrelationContractError,
    normalize_evidence_observation,
    normalize_evidence_relationship,
)
from maigret.web.evidence_correlation_profile_search import (
    profile_search_audit_observations,
)
from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_contract import (
    ProfileSearchError,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
)
from maigret.web.profile_search_orchestrator import ProfileSearchOrchestrator

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "evidence_correlation"
EXPECTED_OUTCOMES = {
    "observed",
    "absent",
    "private",
    "blocked",
    "rate_limited",
    "parser_error",
    "provider_error",
    "indeterminate",
}
PROHIBITED_APPROVAL_FIELDS = {
    "approval",
    "approval_status",
    "approved",
    "approved_at",
    "approved_by",
    "auto_approved",
    "automatic_approval",
    "review_status",
    "reviewed_at",
    "reviewed_by",
}


def _fixture(name):
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _observation_with(base, **updates):
    payload = copy.deepcopy(base)
    payload.update(updates)
    return payload


def _relationship(left, right, kind):
    left = normalize_evidence_observation(left)
    right = normalize_evidence_observation(right)
    return normalize_evidence_relationship(
        {
            "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
            "case_id": left["case_id"],
            "left_case_id": left["case_id"],
            "right_case_id": right["case_id"],
            "left_observation_id": left["observation_id"],
            "right_observation_id": right["observation_id"],
            "relationship_kind": kind,
            "basis": f"Explicit {kind} relationship retained for review.",
        }
    )


def _assert_no_approval_fields(value):
    if isinstance(value, dict):
        assert PROHIBITED_APPROVAL_FIELDS.isdisjoint(value)
        for item in value.values():
            _assert_no_approval_fields(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _assert_no_approval_fields(item)


def _document_sha256(document):
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _run_discovery(client, *, platforms=("instagram",)):
    return asyncio.run(
        ProfileSearchOrchestrator(client).discover(
            {
                "identifiers": [],
                "search_targets": [
                    {"value": "alice_example", "source_type": "username"}
                ],
            },
            platforms=platforms,
        )
    )


class _ObservedProfileClient:
    def __init__(
        self,
        *,
        source_url="https://instagram.com/alice_example/?ref=search",
        retrieved_at="2026-09-09T10:00:00Z",
        request_id="request-1",
    ):
        self.source_url = source_url
        self.retrieved_at = retrieved_at
        self.request_id = request_id

    async def search(self, query):
        provenance = ProfileSearchProvenance.for_query(
            query,
            provider="searxng",
            retrieved_at=self.retrieved_at,
            provider_request_id=self.request_id,
        )
        return ProfileSearchRun(
            query=query,
            provenance=provenance,
            evidence=(
                ProfileSearchEvidence(
                    result_rank=1,
                    source_url=self.source_url,
                    title="Alice Example on Instagram",
                    snippet="Public profile search result.",
                ),
            ),
        )


class _OutcomeProfileClient:
    def __init__(self, error_code=None):
        self.error_code = error_code

    async def search(self, query):
        if self.error_code is not None:
            return ProfileSearchRun(
                query=query,
                provenance=None,
                evidence=(),
                error=ProfileSearchError(
                    query_id=query.query_id,
                    provider="searxng",
                    code=self.error_code,
                    message="Bounded provider diagnostic.",
                    retryable=self.error_code
                    in {"rate_limited", "network_error", "timeout"},
                    occurred_at="2026-09-09T10:00:00Z",
                    http_status=429 if self.error_code == "rate_limited" else None,
                ),
            )
        return ProfileSearchRun(
            query=query,
            provenance=ProfileSearchProvenance.for_query(
                query,
                provider="searxng",
                retrieved_at="2026-09-09T10:00:00Z",
                provider_request_id="empty-request",
            ),
            evidence=(),
        )


@pytest.fixture
def store(tmp_path):
    instance = CaseStore(
        f"sqlite:///{tmp_path / 'evidence-correlation.db'}",
        create_schema=True,
    )
    yield instance
    instance.dispose()


def test_aliases_cluster_deterministically_without_cross_platform_merging():
    observations = _fixture("observations.json")
    inputs = list(
        reversed(
            observations["instagram_variants"]
            + observations["x_variants"]
            + observations["same_handle_different_platforms"]
        )
    )

    result = correlate_evidence(inputs)

    assert result["schema_version"] == EVIDENCE_CORRELATION_SCHEMA_VERSION
    assert result["case_id"] == "case:alpha"
    assert [item["cluster_id"] for item in result["clusters"]] == sorted(
        item["cluster_id"] for item in result["clusters"]
    )
    identities = {
        (
            cluster["canonical_profile_identity"]["platform"],
            cluster["canonical_profile_identity"]["handle"],
        )
        for cluster in result["clusters"]
    }
    assert identities == {
        ("instagram", "alice.example"),
        ("instagram", "alice_example"),
        ("x", "alice_example"),
    }
    assert len(result["clusters"]) == 3
    assert [item["relationship_id"] for item in result["relationships"]] == sorted(
        item["relationship_id"] for item in result["relationships"]
    )


def test_independent_support_is_deduplicated_but_separately_attributable():
    payloads = _fixture("observations.json")["cross_source_claim"]

    result = correlate_evidence(payloads)

    assert len(result["clusters"]) == 1
    cluster = result["clusters"][0]
    assert cluster["case_id"] == "case:alpha"
    assert cluster["claim_type"] == "profile"
    assert cluster["canonical_value"] == ("https://www.instagram.com/alice_example/")
    assert cluster["canonical_profile_identity"] == {
        "platform": "instagram",
        "handle": "alice_example",
        "canonical_url": "https://www.instagram.com/alice_example/",
    }
    assert len(cluster["observations"]) == 4
    assert [item["observation_id"] for item in cluster["observations"]] == sorted(
        item["observation_id"] for item in cluster["observations"]
    )
    assert {item["source_id"] for item in cluster["observations"]} == {
        "native.profile-search",
        "maigret",
        "user-scanner",
        "ai.cited-web",
    }
    assert cluster["independent_observed_source_count"] == 4
    assert cluster["outcome_counts"] == {
        outcome: 4 if outcome == "observed" else 0
        for outcome in sorted(EXPECTED_OUTCOMES)
    }
    assert cluster["confidence"]["scope"] == "correlation"
    assert 0 <= cluster["confidence"]["score"] <= 100
    assert cluster["confidence"]["basis"]
    for original in payloads:
        retained = next(
            item
            for item in cluster["observations"]
            if item["source_record_id"] == original["source_record_id"]
        )
        assert retained["source_snapshot_sha256"] == original["source_snapshot_sha256"]
        assert retained["source_snapshot_ref"] == original["source_snapshot_ref"]
        assert retained["citations"] == original["citations"]
    _assert_no_approval_fields(result)


def test_exact_reruns_are_idempotent_and_changed_context_is_retained_once():
    reruns = _fixture("observations.json")["reruns"]
    original = reruns["original"]
    changed_context = reruns["changed_retrieval_context"]

    baseline = correlate_evidence([original])
    exact_rerun = correlate_evidence([original, copy.deepcopy(original)])
    context_rerun = correlate_evidence(
        [original, copy.deepcopy(original), changed_context]
    )

    assert exact_rerun == baseline
    baseline_cluster = baseline["clusters"][0]
    rerun_cluster = context_rerun["clusters"][0]
    assert rerun_cluster["cluster_id"] == baseline_cluster["cluster_id"]
    assert len(rerun_cluster["observations"]) == 1
    assert len(rerun_cluster["retrieval_contexts"]) == 2
    assert {item["retrieved_at"] for item in rerun_cluster["retrieval_contexts"]} == {
        "2026-09-09T04:00:00Z",
        "2026-09-10T04:00:00Z",
    }
    assert {
        item["originating_query"] for item in rerun_cluster["retrieval_contexts"]
    } == {"first query", "a different approved query"}
    assert rerun_cluster["outcome_counts"] == baseline_cluster["outcome_counts"]
    assert (
        rerun_cluster["independent_observed_source_count"]
        == baseline_cluster["independent_observed_source_count"]
    )
    assert rerun_cluster["confidence"] == baseline_cluster["confidence"]
    assert context_rerun["relationships"] == baseline["relationships"]


def test_changed_snapshot_is_new_evidence_but_not_a_duplicate_claim():
    reruns = _fixture("observations.json")["reruns"]
    baseline = correlate_evidence([reruns["original"]])["clusters"][0]

    result = correlate_evidence([reruns["original"], reruns["changed_snapshot"]])

    assert len(result["clusters"]) == 1
    cluster = result["clusters"][0]
    assert cluster["cluster_id"] == baseline["cluster_id"]
    assert len(cluster["observations"]) == 2
    assert (
        len({item["source_snapshot_sha256"] for item in cluster["observations"]}) == 2
    )
    assert cluster["independent_observed_source_count"] == 1
    assert cluster["confidence"] == baseline["confidence"]


def test_repeated_source_snapshot_does_not_inflate_support_or_confidence():
    original = _fixture("observations.json")["reruns"]["original"]
    duplicate_snapshot = _observation_with(
        original,
        source_id="mirror.native.profile-search",
        source_record_id="a-second-record-for-the-same-snapshot",
    )
    baseline = correlate_evidence([original])["clusters"][0]

    result = correlate_evidence([original, duplicate_snapshot])

    cluster = result["clusters"][0]
    assert len(cluster["observations"]) == 2
    assert cluster["independent_observed_source_count"] == 1
    assert cluster["confidence"] == baseline["confidence"]
    assert any(
        item["relationship_kind"] == "duplicate" for item in result["relationships"]
    )


def test_explicit_conflict_remains_visible_and_lowers_only_correlation_confidence():
    first, second = _fixture("observations.json")["cross_source_claim"][:2]
    supporting = correlate_evidence(
        [first, second], relationships=[_relationship(first, second, "supporting")]
    )
    conflicting = correlate_evidence(
        [first, second], relationships=[_relationship(first, second, "conflicting")]
    )

    support_cluster = supporting["clusters"][0]
    conflict_cluster = conflicting["clusters"][0]
    assert any(
        item["relationship_kind"] == "conflicting"
        for item in conflicting["relationships"]
    )
    assert len(conflict_cluster["observations"]) == 2
    assert (
        conflict_cluster["confidence"]["score"] < support_cluster["confidence"]["score"]
    )
    assert any(
        "conflict" in item.casefold()
        for item in conflict_cluster["confidence"]["basis"]
    )
    _assert_no_approval_fields(conflicting)


def test_unrelated_relationship_is_retained_without_merging_clusters():
    instagram, x_account = _fixture("observations.json")[
        "same_handle_different_platforms"
    ]

    result = correlate_evidence(
        [instagram, x_account],
        relationships=[_relationship(instagram, x_account, "unrelated")],
    )

    assert len(result["clusters"]) == 2
    assert result["relationships"][0]["relationship_kind"] == "unrelated"


def test_all_eight_outcomes_remain_distinct_and_errors_never_become_absent():
    base = _fixture("observations.json")["reruns"]["original"]
    payloads = []
    for index, scenario in enumerate(_fixture("outcomes.json"), start=1):
        outcome = scenario["outcome"]
        payloads.append(
            _observation_with(
                base,
                source_id=f"fixture.{outcome.replace('_', '.')}",
                source_record_id=f"outcome-{outcome}",
                source_snapshot_sha256=f"sha256:{index:064x}",
                **scenario,
            )
        )

    result = correlate_evidence(payloads)

    assert len(result["clusters"]) == 1
    cluster = result["clusters"][0]
    assert {item["outcome"] for item in cluster["observations"]} == (EXPECTED_OUTCOMES)
    assert cluster["outcome_counts"] == {
        outcome: 1 for outcome in sorted(EXPECTED_OUTCOMES)
    }
    for ambiguous in {
        "private",
        "blocked",
        "rate_limited",
        "parser_error",
        "provider_error",
        "indeterminate",
    }:
        retained = next(
            item for item in cluster["observations"] if item["outcome"] == ambiguous
        )
        assert retained["outcome"] != "absent"


def test_case_boundary_rejects_mixed_observations_and_cross_case_relationships():
    original = _fixture("observations.json")["reruns"]["original"]
    other_case = _observation_with(original, case_id="case:other")

    with pytest.raises((CorrelationContractError, ValueError), match="case"):
        correlate_evidence([original, other_case])

    cross_case_relationship = {
        "schema_version": EVIDENCE_CORRELATION_SCHEMA_VERSION,
        "case_id": original["case_id"],
        "left_case_id": original["case_id"],
        "right_case_id": other_case["case_id"],
        "left_observation_id": normalize_evidence_observation(original)[
            "observation_id"
        ],
        "right_observation_id": normalize_evidence_observation(other_case)[
            "observation_id"
        ],
        "relationship_kind": "unrelated",
        "basis": "Different cases must never be correlated.",
    }
    with pytest.raises((CorrelationContractError, ValueError), match="case"):
        correlate_evidence(
            [original],
            relationships=[cross_case_relationship],
        )


def test_profile_search_adapter_preserves_observed_lineage_and_integrity():
    result = _run_discovery(_ObservedProfileClient())
    document = result.as_dict()
    document_sha256 = _document_sha256(document)

    observations = profile_search_audit_observations(
        case_id="case:adapter",
        audit_id="audit-observed-1",
        document_sha256=document_sha256,
        document=document,
        retrieved_at="2026-09-09T10:00:01Z",
    )

    assert isinstance(observations, tuple)
    assert len(observations) == 1
    observation = observations[0]
    assert observation["outcome"] == "observed"
    assert observation["case_id"] == "case:adapter"
    assert observation["canonical_profile_identity"] == {
        "platform": "instagram",
        "handle": "alice_example",
        "canonical_url": "https://www.instagram.com/alice_example/",
    }
    assert observation["originating_query"] == document["queries"][0]["query_text"]
    assert (
        observation["originating_query_fingerprint"]
        == document["queries"][0]["query_fingerprint"]
    )
    assert observation["retrieved_at"] == "2026-09-09T10:00:00Z"
    assert observation["source_snapshot_sha256"].startswith("sha256:")
    assert len(observation["source_snapshot_sha256"]) == 71
    assert observation["source_snapshot_ref"] == (
        "evidence://openledger/profile-search-audit/audit-observed-1/records/"
        + observation["source_record_id"]
        + "/snapshots/"
        + observation["source_snapshot_sha256"].removeprefix("sha256:")
    )
    assert observation["citations"] == [
        {
            "url": "https://www.instagram.com/alice_example/",
            "title": "Alice Example on Instagram",
        }
    ]
    assert normalize_evidence_observation(observation) == observation
    _assert_no_approval_fields(observations)

    tampered = copy.deepcopy(document)
    tampered["candidates"][0]["handle"] = "mallory"
    with pytest.raises(ValueError, match="integrity|hash|sha256"):
        profile_search_audit_observations(
            case_id="case:adapter",
            audit_id="audit-observed-1",
            document_sha256=document_sha256,
            document=tampered,
            retrieved_at="2026-09-09T10:00:01Z",
        )


@pytest.mark.parametrize(
    ("error_code", "expected_outcome"),
    [
        (None, "absent"),
        ("private", "private"),
        ("blocked", "blocked"),
        ("circuit_open", "blocked"),
        ("rate_limited", "rate_limited"),
        ("invalid_response", "parser_error"),
        ("malformed_response", "parser_error"),
        ("provider_error", "provider_error"),
        ("credential_rejected", "provider_error"),
        ("oversized_response", "provider_error"),
        ("network_error", "indeterminate"),
        ("timeout", "indeterminate"),
        ("request_failed", "indeterminate"),
    ],
)
def test_profile_search_adapter_never_converts_ambiguous_failure_to_absent(
    error_code, expected_outcome
):
    result = _run_discovery(_OutcomeProfileClient(error_code))
    document = result.as_dict()

    observations = profile_search_audit_observations(
        case_id="case:adapter-outcomes",
        audit_id=f"audit-{expected_outcome}",
        document_sha256=_document_sha256(document),
        document=document,
        retrieved_at="2026-09-09T10:00:01Z",
    )

    assert len(observations) == 1
    assert observations[0]["outcome"] == expected_outcome
    if error_code is None:
        assert observations[0]["claim_type"] == "profile_search_query"
        native_result = " ".join(
            (
                observations[0]["native_outcome"],
                observations[0]["native_status"],
            )
        ).casefold()
        assert "result" in native_result
        assert any(
            marker in native_result for marker in {"absent", "empty", "no", "zero"}
        )
    else:
        assert observations[0]["outcome"] != "absent"
    assert normalize_evidence_observation(observations[0]) == observations[0]


def test_case_store_exposes_correlation_without_automatic_claim(store):
    job_id = store.create_investigation(["alice_example"], {})
    job = store.claim_next("worker:correlation-acceptance")
    result = _run_discovery(_ObservedProfileClient())
    audit_id = store.record_profile_search_result(
        job_id,
        result,
        worker_id=job["worker_id"],
    )

    discovery = store.get_case_profile_search_discovery(job["case_id"])

    assert discovery["audit_id"] == audit_id
    assert discovery["correlation"]["case_id"] == job["case_id"]
    assert len(discovery["correlation"]["clusters"]) == 1
    assert discovery["correlation"]["clusters"][0]["outcome_counts"] == {
        outcome: 1 if outcome == "observed" else 0
        for outcome in sorted(EXPECTED_OUTCOMES)
    }
    persona = store.get_case(job["case_id"])["personas"][0]
    assert store.get_persona(persona["id"])["claims"] == []
    _assert_no_approval_fields(discovery["correlation"])


def test_case_store_reads_legacy_audit_with_credential_like_tracking_key(store):
    job_id = store.create_investigation(["alice_example"], {})
    job = store.claim_next("worker:legacy-audit-compatibility")
    result = _run_discovery(
        _ObservedProfileClient(
            source_url=("https://x.com/alice_example?token=public-tracking-value")
        ),
        platforms=("x",),
    )
    audit_id = store.record_profile_search_result(
        job_id,
        result,
        worker_id=job["worker_id"],
    )

    discovery = store.get_case_profile_search_discovery(job["case_id"])

    assert discovery["audit_id"] == audit_id
    observation = discovery["correlation"]["clusters"][0]["observations"][0]
    assert observation["claim_value"].endswith("?token=public-tracking-value")
    assert observation["citations"] == [
        {
            "url": "https://x.com/alice_example",
            "title": "Alice Example on Instagram",
        }
    ]
    assert observation["source_snapshot_ref"].startswith(
        f"evidence://openledger/profile-search-audit/{audit_id}/records/"
    )


def test_case_store_reuses_claim_retains_lineage_and_preserves_human_approval(store):
    job_id = store.create_investigation(["alice_example"], {})
    job = store.claim_next("worker:correlation-persona")
    case = store.get_case(job["case_id"])
    persona_id = case["personas"][0]["id"]
    profile_url = "https://www.instagram.com/alice_example/"
    maigret_result = {
        "status": "completed",
        "usernames": ["alice_example"],
        "individual_reports": [
            {
                "username": "alice_example",
                "claimed_profiles": [
                    {
                        "site_name": "Instagram",
                        "url": profile_url,
                        "confidence": "strong",
                        "evidence": {},
                    }
                ],
            }
        ],
    }
    store.sync_persona_claims(job_id, maigret_result)
    before_proposal = store.get_persona(persona_id)
    assert len(before_proposal["claims"]) == 1
    assert len(before_proposal["claims"][0]["evidence"]) == 1

    search_result = _run_discovery(_ObservedProfileClient())
    audit_id = store.record_profile_search_result(
        job_id,
        search_result,
        worker_id=job["worker_id"],
    )
    candidate_id = search_result.candidates[0].candidate.candidate_id
    first_review = store.review_profile_search_candidate(
        job["case_id"],
        audit_id,
        candidate_id,
        persona_id,
        "proposed",
        "analyst.one",
        "Canonical Instagram account matches the retained Maigret evidence.",
    )
    after_proposal = store.get_persona(persona_id)
    assert len(after_proposal["claims"]) == 1
    claim = after_proposal["claims"][0]
    assert claim["id"] == before_proposal["claims"][0]["id"]
    assert len(claim["evidence"]) == 2
    assert {item["evidence_type"] for item in claim["evidence"]} == {
        "observed_profile",
        "native_profile_search_candidate",
    }
    assert any(
        item["details"].get("audit_id") == audit_id for item in claim["evidence"]
    )
    lineage = store.get_claim_lineage(claim["id"])
    assert len(lineage) == 2
    assert {item["source_engine"] for item in lineage} == {
        "openledger_profile_discovery",
        "native_profile_search_review",
    }

    store.review_claim(claim["id"], "approved", "senior.analyst")
    stable_evidence_ids = {item["id"] for item in claim["evidence"]}
    repeated_review = store.review_profile_search_candidate(
        job["case_id"],
        audit_id,
        candidate_id,
        persona_id,
        "proposed",
        "analyst.two",
        "Exact rerun of the already retained candidate.",
    )

    final_case = store.get_case(job["case_id"])
    final_persona = store.get_persona(persona_id)
    assert len(final_case["personas"]) == 1
    assert len(final_persona["claims"]) == 1
    final_claim = final_persona["claims"][0]
    assert final_claim["id"] == claim["id"] == first_review["claim_id"]
    assert repeated_review["claim_id"] == claim["id"]
    assert final_claim["review_status"] == "approved"
    assert final_claim["reviewed_by"] == "senior.analyst"
    assert len(final_claim["evidence"]) == 2
    assert {item["id"] for item in final_claim["evidence"]} == stable_evidence_ids
    assert len(store.get_claim_lineage(final_claim["id"])) == 2
