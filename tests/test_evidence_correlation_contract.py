# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from maigret.web.evidence_correlation_contract import (
    EVIDENCE_CORRELATION_OUTCOMES,
    EVIDENCE_CORRELATION_SCHEMA_VERSION,
    EVIDENCE_RELATIONSHIP_KINDS,
    EVIDENCE_OUTCOMES,
    EVIDENCE_RELATIONSHIPS,
    CorrelationContractError,
    canonical_profile_identity,
    evidence_cluster_id,
    evidence_observation_id,
    evidence_relationship_id,
    normalize_evidence_observation,
    normalize_evidence_relationship,
)


def observation(**overrides):
    payload = {
        "schema_version": 1,
        "case_id": "case-123",
        "claim_type": "profile",
        "claim_value": "https://twitter.com/Alice_Example/status/12345",
        "source_id": "search.example",
        "source_version": "2026.09",
        "source_record_id": "result-42",
        "outcome": "observed",
        "native_outcome": "found",
        "native_status": "200",
        "citations": [
            {
                "url": "https://evidence.example.org/result/42",
                "title": "Public search result",
            }
        ],
        "retrieved_at": "2026-09-09T08:30:00+07:00",
        "originating_query": 'site:x.com "Alice Example"',
        "originating_query_fingerprint": f"sha256:{'a' * 64}",
        "source_snapshot_sha256": f"sha256:{'b' * 64}",
        "source_snapshot_ref": "evidence://openledger/profile-search-audit/audit-42",
    }
    payload.update(overrides)
    return payload


def relationship(left, right, **overrides):
    payload = {
        "schema_version": 1,
        "case_id": "case-123",
        "left_observation_id": left,
        "left_case_id": "case-123",
        "right_observation_id": right,
        "right_case_id": "case-123",
        "relationship_kind": "supporting",
        "basis": "Independent sources report the same public profile.",
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    ("url", "identity"),
    [
        ("https://www.facebook.com/Alice.Example", "facebook:alice.example"),
        ("https://instagram.com/Alice.Example/", "instagram:alice.example"),
        ("https://threads.net/@Alice.Example", "threads:alice.example"),
        ("https://www.tiktok.com/@Alice_Example", "tiktok:alice_example"),
        ("https://x.com/Alice_Example", "x:alice_example"),
        ("https://twitter.com/Alice_Example/status/12345", "x:alice_example"),
    ],
)
def test_supported_profile_parsers_produce_platform_scoped_identity(url, identity):
    normalized = canonical_profile_identity(url)

    assert normalized is not None
    assert f"{normalized['platform']}:{normalized['handle']}" == identity
    assert normalized["canonical_url"].startswith("https://")


@pytest.mark.parametrize(
    "value",
    [
        None,
        42,
        "",
        "alice_example",
        "http://x.com/alice_example",
        "https://example.org/alice_example",
        "https://x.com/home",
    ],
)
def test_canonical_profile_identity_returns_none_for_unsupported_values(value):
    assert canonical_profile_identity(value) is None


def test_normalization_preserves_complete_lineage_without_approval_state():
    normalized = normalize_evidence_observation(
        observation(
            citations=[
                {"url": "https://z.example.org/a", "title": "Z result"},
                {"url": "https://a.example.org/a", "title": "A result"},
            ],
            retrieved_at=datetime(2026, 9, 9, 1, 30, tzinfo=timezone.utc),
            confidence={
                "scope": "review_priority",
                "score": 72,
                "basis": ["Second signal", "First signal"],
            },
        )
    )

    assert normalized["schema_version"] == 1
    assert normalized["canonical_profile_identity"] == {
        "platform": "x",
        "handle": "alice_example",
        "canonical_url": "https://x.com/alice_example",
    }
    assert normalized["source_id"] == "search.example"
    assert normalized["source_version"] == "2026.09"
    assert normalized["source_record_id"] == "result-42"
    assert normalized["native_outcome"] == "found"
    assert normalized["native_status"] == "200"
    assert normalized["retrieved_at"] == "2026-09-09T01:30:00Z"
    assert normalized["originating_query_fingerprint"].endswith("a" * 64)
    assert normalized["source_snapshot_sha256"].endswith("b" * 64)
    assert normalized["source_snapshot_ref"].startswith("evidence://openledger/")
    assert [item["title"] for item in normalized["citations"]] == [
        "A result",
        "Z result",
    ]
    assert normalized["confidence"] == {
        "scope": "review_priority",
        "score": 72,
        "basis": ["First signal", "Second signal"],
    }
    assert not any("approv" in key for key in normalized)


@pytest.mark.parametrize("outcome", sorted(EVIDENCE_CORRELATION_OUTCOMES))
def test_all_outcomes_remain_distinct(outcome):
    normalized = normalize_evidence_observation(
        observation(
            outcome=outcome,
            native_outcome=f"native-{outcome}",
            native_status=f"status-{outcome}",
        )
    )

    assert normalized["outcome"] == outcome
    assert normalized["native_outcome"] == f"native-{outcome}"
    assert normalized["native_status"] == f"status-{outcome}"


def test_rerun_context_is_preserved_but_does_not_inflate_stable_ids():
    first = normalize_evidence_observation(observation())
    rerun = normalize_evidence_observation(
        observation(
            retrieved_at="2026-09-10T12:30:00Z",
            originating_query="Alice Example social profile",
            originating_query_fingerprint=f"sha256:{'c' * 64}",
        )
    )

    assert first["observation_id"] == rerun["observation_id"]
    assert first["cluster_id"] == rerun["cluster_id"]
    assert first["retrieved_at"] != rerun["retrieved_at"]
    assert first["originating_query"] != rerun["originating_query"]
    assert (
        first["originating_query_fingerprint"] != rerun["originating_query_fingerprint"]
    )


def test_snapshot_change_creates_new_observation_in_the_same_cluster():
    first = observation()
    changed = observation(source_snapshot_sha256=f"sha256:{'c' * 64}")

    assert evidence_observation_id(first) != evidence_observation_id(changed)
    assert evidence_cluster_id(first) == evidence_cluster_id(changed)


def test_independent_sources_share_profile_cluster_but_retain_observation_ids():
    first = observation()
    corroborating = observation(
        claim_value="https://x.com/alice_example",
        source_id="maigret",
        source_record_id="maigret-99",
        source_snapshot_sha256=f"sha256:{'c' * 64}",
    )

    assert evidence_cluster_id(first) == evidence_cluster_id(corroborating)
    assert evidence_observation_id(first) != evidence_observation_id(corroborating)


def test_platform_aliases_cluster_but_different_platforms_do_not():
    x_current = observation(claim_value="https://x.com/Alice_Example")
    x_legacy = observation(claim_value="https://twitter.com/alice_example")
    instagram = observation(claim_value="https://instagram.com/alice_example/")

    assert evidence_cluster_id(x_current) == evidence_cluster_id(x_legacy)
    assert evidence_observation_id(x_current) == evidence_observation_id(x_legacy)
    assert evidence_cluster_id(x_current) != evidence_cluster_id(instagram)


def test_profile_url_does_not_merge_across_different_claim_types():
    profile = observation(claim_type="profile")
    website = observation(claim_type="website")

    assert evidence_cluster_id(profile) != evidence_cluster_id(website)
    assert evidence_observation_id(profile) != evidence_observation_id(website)


def test_profile_claim_fails_closed_when_value_is_not_a_supported_profile():
    with pytest.raises(CorrelationContractError, match="supported public profile"):
        normalize_evidence_observation(
            observation(claim_value="https://example.org/alice_example")
        )


def test_clusters_are_case_scoped_and_generic_claims_are_normalized():
    first = observation(claim_type="full_name", claim_value=" Alice   Example ")
    same = observation(claim_type="full_name", claim_value="alice example")
    other_case = observation(
        case_id="case-456", claim_type="full_name", claim_value="alice example"
    )

    assert evidence_cluster_id(first) == evidence_cluster_id(same)
    assert evidence_cluster_id(first) != evidence_cluster_id(other_case)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"schema_version": 2}, "Unsupported evidence-correlation schema"),
        ({"case_id": "case id"}, "Invalid case_id"),
        ({"claim_type": "Profile Name"}, "Invalid claim_type"),
        ({"claim_value": "x" * 2001}, "claim_value is too large"),
        ({"outcome": "not_found"}, "Unsupported evidence outcome"),
        ({"retrieved_at": "2026-09-09T01:30:00"}, "include a timezone"),
        (
            {"originating_query_fingerprint": "sha256:not-a-hash"},
            "originating_query_fingerprint",
        ),
        ({"source_snapshot_sha256": "b" * 64}, "source_snapshot_sha256"),
        ({"source_snapshot_ref": "file:///tmp/audit"}, "source_snapshot_ref"),
        (
            {"source_snapshot_ref": "evidence://user:password@openledger/audit"},
            "credential-free",
        ),
        ({"approval_status": "approved"}, "unsupported fields"),
        ({"api_key": "secret"}, "credential field"),
    ],
)
def test_observation_rejects_malformed_or_unsafe_fields(overrides, message):
    with pytest.raises(CorrelationContractError, match=message):
        normalize_evidence_observation(observation(**overrides))


@pytest.mark.parametrize(
    "field_name",
    [
        "case_id",
        "claim_type",
        "claim_value",
        "source_id",
        "source_version",
        "source_record_id",
        "outcome",
        "native_outcome",
        "native_status",
        "citations",
        "retrieved_at",
        "originating_query",
        "originating_query_fingerprint",
        "source_snapshot_sha256",
        "source_snapshot_ref",
    ],
)
def test_observation_rejects_missing_lineage(field_name):
    payload = observation()
    payload.pop(field_name)

    with pytest.raises(CorrelationContractError):
        normalize_evidence_observation(payload)


@pytest.mark.parametrize(
    "url",
    [
        "http://evidence.example.org/result",
        "https://user:password@evidence.example.org/result",
        "https://localhost/result",
        "https://127.0.0.1/result",
        "https://service.internal/result",
        "https://example.test/result",
        "https://evidence.example.org/result?api_key=secret",
    ],
)
def test_citations_reject_non_public_or_credential_bearing_urls(url):
    with pytest.raises(CorrelationContractError, match=r"citations\[0\].url"):
        normalize_evidence_observation(
            observation(citations=[{"url": url, "title": "Result"}])
        )


def test_citations_and_confidence_are_bounded_and_deterministic():
    duplicate_url = "https://evidence.example.org/result"
    normalized = normalize_evidence_observation(
        observation(
            citations=[
                {"url": duplicate_url, "title": "Result"},
                {"url": duplicate_url, "title": "Result"},
            ],
            confidence={
                "scope": "correlation",
                "score": 50,
                "basis": ["Signal", "Signal"],
            },
        )
    )
    assert normalized["citations"] == [{"url": duplicate_url, "title": "Result"}]
    assert normalized["confidence"]["basis"] == ["Signal"]

    normalized_scheme = normalize_evidence_observation(
        observation(
            citations=[
                {"url": "HTTPS://evidence.example.org/result", "title": "Result"}
            ],
            source_snapshot_ref="EVIDENCE://openledger/audit/42",
        )
    )
    assert normalized_scheme["citations"][0]["url"].startswith("https://")
    assert normalized_scheme["source_snapshot_ref"].startswith("evidence://")

    with pytest.raises(CorrelationContractError, match="too many items"):
        normalize_evidence_observation(
            observation(
                citations=[
                    {
                        "url": f"https://evidence.example.org/{index}",
                        "title": str(index),
                    }
                    for index in range(33)
                ]
            )
        )
    with pytest.raises(CorrelationContractError, match="same title"):
        normalize_evidence_observation(
            observation(
                citations=[
                    {"url": duplicate_url, "title": "First"},
                    {"url": duplicate_url, "title": "Second"},
                ]
            )
        )


@pytest.mark.parametrize(
    ("confidence", "message"),
    [
        ({"scope": "approval", "score": 50, "basis": ["Signal"]}, "scope"),
        (
            {"scope": "correlation", "score": True, "basis": ["Signal"]},
            "score",
        ),
        ({"scope": "correlation", "score": 101, "basis": ["Signal"]}, "score"),
        ({"scope": "correlation", "score": 50, "basis": []}, "basis"),
        (
            {
                "scope": "correlation",
                "score": 50,
                "basis": ["Signal"],
                "approval": "approved",
            },
            "confidence must contain only",
        ),
    ],
)
def test_confidence_is_explainable_bounded_and_not_approval(confidence, message):
    with pytest.raises(CorrelationContractError, match=message):
        normalize_evidence_observation(observation(confidence=confidence))


def test_supplied_derived_observation_identity_must_match():
    normalized = normalize_evidence_observation(observation())
    assert normalize_evidence_observation(normalized) == normalized

    for field_name in ("observation_id", "cluster_id"):
        inconsistent = copy.deepcopy(normalized)
        inconsistent[field_name] = inconsistent[field_name][:-1] + "0"
        if inconsistent[field_name] == normalized[field_name]:
            inconsistent[field_name] = inconsistent[field_name][:-1] + "1"
        with pytest.raises(CorrelationContractError, match="inconsistent"):
            normalize_evidence_observation(inconsistent)


@pytest.mark.parametrize("kind", sorted(EVIDENCE_RELATIONSHIP_KINDS))
def test_relationship_kinds_are_symmetric_and_stable(kind):
    left = evidence_observation_id(observation())
    right = evidence_observation_id(
        observation(
            source_id="maigret",
            source_record_id="result-99",
            source_snapshot_sha256=f"sha256:{'c' * 64}",
        )
    )
    forward = relationship(left, right, relationship_kind=kind)
    reverse = relationship(right, left, relationship_kind=kind)

    normalized = normalize_evidence_relationship(forward)
    assert evidence_relationship_id(forward) == evidence_relationship_id(reverse)
    assert normalized["left_observation_id"] < normalized["right_observation_id"]
    assert normalized["left_case_id"] == normalized["case_id"]
    assert normalized["right_case_id"] == normalized["case_id"]
    assert normalize_evidence_relationship(normalized) == normalized


def test_relationship_identity_is_stable_when_explanation_is_refined():
    left = f"evidence-observation:{'a' * 64}"
    right = f"evidence-observation:{'b' * 64}"
    first = relationship(left, right, basis="Initial bounded explanation.")
    refined = relationship(left, right, basis="Refined bounded explanation.")

    assert evidence_relationship_id(first) == evidence_relationship_id(refined)


@pytest.mark.parametrize("endpoint", ["left_case_id", "right_case_id"])
def test_relationship_rejects_cross_case_endpoints(endpoint):
    payload = relationship(
        f"evidence-observation:{'a' * 64}",
        f"evidence-observation:{'b' * 64}",
    )
    payload[endpoint] = "case-456"

    with pytest.raises(CorrelationContractError, match="belong to case_id"):
        normalize_evidence_relationship(payload)


def test_relationship_rejects_self_reference_unknown_kind_and_approval_fields():
    identifier = f"evidence-observation:{'a' * 64}"
    with pytest.raises(CorrelationContractError, match="self-reference"):
        normalize_evidence_relationship(relationship(identifier, identifier))
    with pytest.raises(CorrelationContractError, match="Unsupported"):
        normalize_evidence_relationship(
            relationship(
                identifier,
                f"evidence-observation:{'b' * 64}",
                relationship_kind="similar",
            )
        )
    with pytest.raises(CorrelationContractError, match="unsupported fields"):
        normalize_evidence_relationship(
            relationship(
                identifier,
                f"evidence-observation:{'b' * 64}",
                review_status="approved",
            )
        )


def test_relationship_rejects_malformed_id_unbounded_basis_and_derived_mismatch():
    left = f"evidence-observation:{'a' * 64}"
    right = f"evidence-observation:{'b' * 64}"
    with pytest.raises(CorrelationContractError, match="left_observation_id"):
        normalize_evidence_relationship(relationship("observation-a", right))
    with pytest.raises(CorrelationContractError, match="basis is too large"):
        normalize_evidence_relationship(relationship(left, right, basis="x" * 1001))
    with pytest.raises(
        CorrelationContractError, match="relationship_id is inconsistent"
    ):
        normalize_evidence_relationship(
            relationship(
                left,
                right,
                relationship_id=f"evidence-relationship:{'c' * 64}",
            )
        )


def test_json_schema_parity_with_runtime_contract():
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "schemas"
        / "evidence-correlation-envelope.v1.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    definitions = schema["$defs"]
    observation_schema = definitions["observation"]
    relationship_schema = definitions["relationship"]

    assert schema["$schema"].endswith("draft/2020-12/schema")
    assert EVIDENCE_OUTCOMES is EVIDENCE_CORRELATION_OUTCOMES
    assert EVIDENCE_RELATIONSHIPS is EVIDENCE_RELATIONSHIP_KINDS
    assert observation_schema["properties"]["schema_version"]["const"] == (
        EVIDENCE_CORRELATION_SCHEMA_VERSION
    )
    assert set(observation_schema["properties"]["outcome"]["enum"]) == (
        EVIDENCE_CORRELATION_OUTCOMES
    )
    assert (
        set(relationship_schema["properties"]["relationship_kind"]["enum"])
        == EVIDENCE_RELATIONSHIP_KINDS
    )
    assert set(observation_schema["required"]) == (
        set(normalize_evidence_observation(observation())) - {"confidence"}
    )
    left = f"evidence-observation:{'a' * 64}"
    right = f"evidence-observation:{'b' * 64}"
    assert set(relationship_schema["required"]) == set(
        normalize_evidence_relationship(relationship(left, right))
    )
    assert "approval" not in json.dumps(schema).casefold()
