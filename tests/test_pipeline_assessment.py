from copy import deepcopy

import pytest

from maigret.web.pipeline_assessment import SIGNAL_METHODS, assess_group

NOW = "2026-09-11T12:00:00+00:00"
GROUP = {
    "id": "account:1",
    "kind": "account",
    "case_id": "case:1",
    "subject_id": "subject:1",
}


def observation(
    identifier="obs:1", *, origin="origin:1", signal="source_link_match", status="found"
):
    url = "https://example.org/public/subject"
    return {
        "id": identifier,
        "case_id": "case:1",
        "subject_id": "subject:1",
        "status": status,
        "source_url": url,
        "observed_at": "2026-09-10T12:00:00+00:00",
        "origin_family_id": origin,
        "dependence": {"status": "known_origin" if origin else "unknown"},
        "retention": {"mode": "retained", "final_eligible": True},
        "evidence_signals": {
            signal: {
                "matched": True,
                "evidence_ref": url,
                "subject_id": "subject:1",
                "hypothesis_key": "account:1",
                "method": SIGNAL_METHODS[signal],
            }
        },
        "payload": {},
    }


def assess(rows, group=None, **kwargs):
    return assess_group(group or GROUP, rows, as_of=NOW, **kwargs)


def test_one_origin_copied_one_hundred_times_never_raises_feature_weight():
    once = assess([observation()])
    copied = assess([observation(f"obs:{i}") for i in range(100)])
    assert copied["features"] == once["features"]
    assert copied["evidence_counts"]["observations"] == 100
    assert copied["evidence_counts"]["support_origin_families"] == 1
    assert len(copied["origin_families"][0]["observation_ids"]) == 100
    assert copied["evidence_digest"] != once["evidence_digest"]


def test_independent_origin_changes_support_with_full_lineage():
    result = assess([observation(), observation("obs:2", origin="origin:2")])
    assert result["features"]["support_origin_families"] == 2
    assert result["probability"]["value"] is None
    assert result["probability"]["reason"] == "not_calibrated"


def test_consolidated_copy_lineage_overrides_different_raw_origin_ids():
    rows = [
        observation("obs:1", origin="origin:1"),
        observation("obs:2", origin="origin:copy"),
    ]
    result = assess(
        rows,
        group={
            **GROUP,
            "origin_by_observation": {"obs:1": "origin:1", "obs:2": "origin:1"},
        },
    )
    assert result["features"]["support_origin_families"] == 1
    assert result["origin_families"][0]["observation_ids"] == ["obs:1", "obs:2"]
    assert rows[1]["origin_family_id"] == "origin:copy"


@pytest.mark.parametrize(
    "status", ["blocked", "timeout", "error", "cancelled", "not_found"]
)
def test_source_failure_is_visible_and_never_a_negative_identity_feature(status):
    initial = assess([observation()])
    unavailable = observation("obs:failed", status=status)
    unavailable["observed_at"] = "2020-01-01T00:00:00Z"
    result = assess([observation(), unavailable])
    assert result["features"] == initial["features"]
    assert result["outcomes"][status] == 1
    assert result["warnings"]


def test_found_account_and_same_handle_do_not_prove_attribution():
    row = observation(signal="stable_id_match")
    row["payload"]["username"] = "matching_handle"
    row["evidence_signals"]["stable_id_match"]["method"] = "handle_match"
    result = assess([row])
    assert result["features"]["support_origin_families"] == 0
    assert result["evidence_status"] == "needs_evidence"
    assert result["probability"]["reason"] == "insufficient_source_backed_evidence"


@pytest.mark.parametrize(
    "signal",
    [
        True,
        {"matched": True},
        {
            "matched": True,
            "method": "llm",
            "evidence_ref": "https://example.org/public/subject",
        },
        {
            "matched": True,
            "method": "explicit_profile_link",
            "evidence_ref": "unrelated",
        },
    ],
)
def test_signal_without_source_backed_reference_is_not_scored(signal):
    row = observation()
    row["evidence_signals"]["source_link_match"] = signal
    assert assess([row])["features"]["source_link_match"] == 0


def test_source_signal_for_another_subject_or_qualified_hypothesis_is_not_reused():
    row = observation()
    row["evidence_signals"]["source_link_match"][
        "hypothesis_key"
    ] = "account:someone-else"
    assert assess([row])["features"]["source_link_match"] == 0
    row["evidence_signals"]["source_link_match"]["hypothesis_key"] = "account:1"
    row["evidence_signals"]["source_link_match"]["subject_id"] = "another-person"
    assert assess([row])["features"]["source_link_match"] == 0


def test_unknown_independence_is_not_independent_corroboration():
    result = assess([observation(f"obs:{i}", origin=None) for i in range(20)])
    assert result["features"]["support_origin_families"] == 0
    assert result["features"]["unknown_dependence"] == 1
    assert result["evidence_counts"]["unknown_origin_observations"] == 20


def test_stale_archived_assertion_does_not_become_fresh_when_fetched_again():
    row = observation()
    row["published_at"] = "2010-01-01T00:00:00Z"
    result = assess([row])
    assert result["features"]["stale_origin_families"] == 1
    assert result["probability"]["reason"] == "stale_or_undated_evidence"


def test_timezone_unknown_is_undated_and_no_numeric_probability_is_allowed():
    row = observation()
    row["observed_at"] = "2026-09-10T12:00:00"
    assert assess([row])["probability"]["reason"] == "stale_or_undated_evidence"


def test_metadata_only_provider_details_do_not_support_final_facts_or_probabilities():
    row = observation()
    row["retention"] = {"mode": "metadata_only", "final_eligible": False}
    result = assess([row])
    assert result["evidence_counts"]["retainable_observations"] == 0
    assert result["features"]["support_origin_families"] == 0
    assert result["operator_review_available"]


def test_qualified_subject_claim_event_requires_explicit_joint_support():
    claim = {
        **GROUP,
        "kind": "claim",
        "predicate": "affiliation",
        "value": "Organization",
    }
    result = assess([observation()], group=claim)
    assert result["probability"]["reason"] == "joint_subject_claim_event_unsupported"
    supported = assess([observation(signal="qualified_claim_support")], group=claim)
    assert supported["event"] == "claim_correctness"
    assert supported["probability"]["reason"] == "not_calibrated"


def test_contradictions_are_retained_and_never_resolved_by_assessment():
    result = assess(
        [
            observation(),
            observation("obs:negative", origin="origin:2", signal="contradiction"),
        ],
        group={**GROUP, "conflicts": ["group:other"]},
    )
    assert result["evidence_status"] == "conflicting"
    assert result["features"]["contradiction_origin_families"] == 1
    assert result["group_conflicts"] == ["group:other"]
    assert result["contradictions"][0]["observation_id"] == "obs:negative"
    assert result["operator_review_available"] is True


def test_evidence_digest_rejects_stale_assessment_without_mutating_evidence():
    rows = [observation()]
    before = deepcopy(rows)
    first = assess(rows)
    result = assess(
        rows + [observation("obs:new")],
        expected_evidence_digest=first["evidence_digest"],
    )
    assert result["probability"]["reason"] == "stale_evidence_digest"
    assert rows == before


def test_replay_and_order_do_not_change_evidence_snapshot():
    rows = [observation("obs:2"), observation("obs:1")]
    first = assess(rows)
    second = assess(list(reversed(rows)) + [rows[0]])
    assert first == second


@pytest.mark.parametrize("field", ["case_id", "subject_id"])
def test_foreign_scope_is_rejected(field):
    row = observation()
    row[field] = "foreign"
    with pytest.raises(ValueError, match="cross-"):
        assess([row])


def test_same_observation_id_cannot_hide_revised_payload():
    row = observation()
    revised = {**row, "payload": {"changed": True}}
    with pytest.raises(ValueError, match="conflicting observation"):
        assess([row, revised])


def test_empty_evidence_never_blocks_operator():
    result = assess([])
    assert result["operator_review_available"] is True
    assert result["probability"]["value"] is None
    assert result["missing_evidence"]
