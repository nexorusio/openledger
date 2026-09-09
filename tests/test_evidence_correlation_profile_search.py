# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import copy
import hashlib
import json

import pytest

from maigret.web.evidence_correlation import correlate_evidence
from maigret.web.evidence_correlation_profile_search import (
    profile_search_audit_observations,
)


def _query(index=1):
    return {
        "schema_version": 1,
        "query_id": f"query-{index}",
        "platform": "x",
        "query_text": f"site:x.com alice_{index}",
        "seed_kind": "username",
        "seed_value": f"alice_{index}",
        "seed_score": 50,
        "seed_reason": "Approved username seed.",
        "max_results": 5,
        "query_fingerprint": f"sha256:{index:064x}",
    }


def _success_run(query, evidence=None):
    return {
        "query": query,
        "provenance": {
            "query_id": query["query_id"],
            "query_fingerprint": query["query_fingerprint"],
            "provider": "searxng",
            "provider_request_id": "request-1",
            "retrieved_at": "2026-09-09T10:00:00Z",
        },
        "evidence": list(evidence or []),
        "error": None,
    }


def _failed_run(query, code):
    return {
        "query": query,
        "provenance": None,
        "evidence": [],
        "error": {
            "query_id": query["query_id"],
            "provider": "searxng",
            "code": code,
            "message": "Bounded provider diagnostic.",
            "retryable": True,
            "occurred_at": "2026-09-09T10:00:00Z",
            "http_status": 429 if code == "rate_limited" else None,
        },
    }


def _document(queries, runs):
    return {
        "orchestration_version": 1,
        "status": "completed",
        "stopped": False,
        "planned_query_count": len(queries),
        "executed_query_count": len(runs),
        "skipped_query_count": len(queries) - len(runs),
        "error_count": sum(run["error"] is not None for run in runs),
        "candidate_count": 0,
        "queries": queries,
        "runs": runs,
        "candidates": [],
    }


def _sha256(document):
    payload = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _adapt(document, **overrides):
    arguments = {
        "case_id": "case-adapter",
        "audit_id": "audit-adapter",
        "document_sha256": _sha256(document),
        "document": document,
        "retrieved_at": "2026-09-09T10:00:01Z",
    }
    arguments.update(overrides)
    return profile_search_audit_observations(**arguments)


def test_supported_profile_result_preserves_lineage_and_canonical_identity():
    query = _query()
    document = _document(
        [query],
        [
            _success_run(
                query,
                [
                    {
                        "result_rank": 1,
                        "source_url": "https://twitter.com/Alice_1/status/12345",
                        "title": "Alice on X",
                        "snippet": "Public profile result.",
                    }
                ],
            )
        ],
    )

    observation = _adapt(document)[0]

    assert observation["outcome"] == "observed"
    assert observation["canonical_profile_identity"] == {
        "platform": "x",
        "handle": "alice_1",
        "canonical_url": "https://x.com/alice_1",
    }
    assert observation["source_id"] == "searxng"
    assert observation["originating_query"] == query["query_text"]
    assert observation["originating_query_fingerprint"] == (query["query_fingerprint"])
    assert observation["retrieved_at"] == "2026-09-09T10:00:00Z"
    assert observation["source_snapshot_sha256"].startswith("sha256:")
    assert len(observation["source_snapshot_sha256"]) == 71


def test_success_without_supported_profile_is_query_scoped_absence():
    query = _query()
    document = _document(
        [query],
        [
            _success_run(
                query,
                [
                    {
                        "result_rank": 1,
                        "source_url": "https://public.example.org/article",
                        "title": "Article, not a profile",
                        "snippet": "No supported platform profile.",
                    }
                ],
            )
        ],
    )

    observation = _adapt(document)[0]

    assert observation["claim_type"] == "profile_search_query"
    assert observation["outcome"] == "absent"
    assert observation["native_outcome"] == "no_returned_profile_result"
    assert "no_supported_profile_result" in observation["native_status"]


@pytest.mark.parametrize(
    ("code", "outcome"),
    [
        ("private", "private"),
        ("blocked", "blocked"),
        ("circuit_open", "blocked"),
        ("rate_limited", "rate_limited"),
        ("invalid_response", "parser_error"),
        ("malformed_response", "parser_error"),
        ("provider_error", "provider_error"),
        ("credential_rejected", "provider_error"),
        ("oversized_response", "provider_error"),
        ("timeout", "indeterminate"),
        ("network_error", "indeterminate"),
        ("new_ambiguous_failure", "indeterminate"),
    ],
)
def test_failure_taxonomy_never_converts_errors_to_absence(code, outcome):
    query = _query()
    document = _document([query], [_failed_run(query, code)])

    observation = _adapt(document)[0]

    assert observation["outcome"] == outcome
    assert observation["outcome"] != "absent"
    assert observation["native_outcome"] == code
    assert observation["retrieved_at"] == "2026-09-09T10:00:01Z"


def test_output_order_and_source_record_ids_are_stable():
    first = _query(1)
    second = _query(2)
    document = _document(
        [first, second],
        [_failed_run(first, "timeout"), _failed_run(second, "blocked")],
    )

    baseline = _adapt(document)
    repeated = _adapt(copy.deepcopy(document))

    assert repeated == baseline
    assert [item["observation_id"] for item in baseline] == sorted(
        item["observation_id"] for item in baseline
    )
    assert len({item["source_record_id"] for item in baseline}) == 2


def test_independent_providers_get_distinct_source_snapshots_and_support():
    first = _query(1)
    second = _query(2)
    result = {
        "result_rank": 1,
        "source_url": "https://x.com/alice_example",
        "title": "Alice Example on X",
        "snippet": "Public profile result.",
    }
    first_run = _success_run(first, [result])
    second_run = _success_run(second, [result])
    second_run["provenance"]["provider"] = "brave"
    document = _document([first, second], [first_run, second_run])

    observations = _adapt(document)
    cluster = correlate_evidence(observations)["clusters"][0]

    assert len({item["source_snapshot_sha256"] for item in observations}) == 2
    assert cluster["independent_observed_source_count"] == 2
    assert cluster["confidence"]["score"] == 55


def test_rejects_tampering_and_invalid_hashes():
    query = _query()
    document = _document([query], [_success_run(query)])
    tampered = copy.deepcopy(document)
    tampered["queries"][0]["query_text"] = "different query"

    with pytest.raises(ValueError, match="integrity"):
        _adapt(tampered, document_sha256=_sha256(document))
    with pytest.raises(ValueError, match="sha256"):
        _adapt(document, document_sha256="not-a-hash")


def test_rejects_duplicate_queries_and_runs():
    query = _query()
    second_query = _query(2)
    duplicate_queries = _document([query, copy.deepcopy(query)], [_success_run(query)])
    duplicate_runs = _document(
        [query, second_query], [_success_run(query), _success_run(query)]
    )

    with pytest.raises(ValueError, match="duplicate query"):
        _adapt(duplicate_queries)
    with pytest.raises(ValueError, match="duplicate run"):
        _adapt(duplicate_runs)


def test_rejects_query_and_provenance_mismatches():
    query = _query()
    mismatched_run = _success_run(query)
    mismatched_run["query"] = {**query, "query_text": "different"}
    document = _document([query], [mismatched_run])

    with pytest.raises(ValueError, match="does not match its query"):
        _adapt(document)

    mismatched_provenance = _success_run(query)
    mismatched_provenance["provenance"]["query_fingerprint"] = f"sha256:{9:064x}"
    document = _document([query], [mismatched_provenance])
    with pytest.raises(ValueError, match="provenance"):
        _adapt(document)


def test_rejects_query_and_result_bounds():
    queries = [_query(index) for index in range(1, 27)]
    oversized_queries = _document(queries, [])
    query = _query()
    oversized_results = _document(
        [query],
        [
            _success_run(
                query,
                [
                    {
                        "result_rank": index,
                        "source_url": f"https://x.com/alice_{index}",
                        "title": "Profile",
                        "snippet": "",
                    }
                    for index in range(1, 12)
                ],
            )
        ],
    )

    with pytest.raises(ValueError, match="queries exceeds"):
        _adapt(oversized_queries)
    with pytest.raises(ValueError, match="evidence exceeds"):
        _adapt(oversized_results)
