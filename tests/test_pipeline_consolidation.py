"""P2 A07-A11/A25: source retention, collisions, replay and dense reconciliation."""

import copy
import json
import time
import tracemalloc

import pytest

from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web.pipeline_consolidation import (
    account_identity,
    canonical_account,
    consolidate_observations,
    qualified_claim_identity,
)
from maigret.web.pipeline_evidence import (
    ObservationContractError,
    canonical_source_url,
    iter_legacy_claim_observations,
    iter_result_observations,
    normalize_observation,
    normalize_status,
)

SCOPE = dict(
    case_id="case-a",
    subject_id="subject-a",
    request_id="request-a",
    task_id="task-a",
    attempt_id="attempt-a",
    observed_at="2026-09-11T10:00:00Z",
)


def observation(engine="engine-a", **overrides):
    raw = {
        "source_engine": engine,
        "source_record_id": "record-a",
        "status": "found",
        "source_url": "https://www.instagram.com/alice/",
        "account": {
            "platform": "instagram",
            "profile_url": "https://www.instagram.com/alice/",
        },
        "claims": [{"predicate": "date_of_birth", "value": "1990-01-01"}],
    }
    raw.update(overrides)
    return normalize_observation(raw, **SCOPE)


def test_three_engines_one_account_and_claim_all_lineages():
    docs = [observation(engine) for engine in ("a", "b", "c")]
    result = consolidate_observations(iter(docs))
    assert len(result["accounts"]) == len(result["claims"]) == 1
    assert result["observation_count"] == 3
    for group in result["accounts"] + result["claims"]:
        assert group["observation_count"] == 3
        assert set(group["observation_ids"]) == {doc["id"] for doc in docs}
        assert group["independent_origin_count"] == 1


def test_mirrors_snippets_models_do_not_manufacture_independent_support():
    docs = [observation("original")]
    for number in range(100):
        docs.append(
            observation(
                "copy" + str(number),
                source_url="https://mirror.example/" + str(number),
                original_url="https://www.instagram.com/alice/",
                evidence_type="cached_copy",
            )
        )
    docs.append(
        observation("registry", source_url="https://registry.example/records/alice")
    )
    result = consolidate_observations(docs)
    assert result["claims"][0]["observation_count"] == 102
    assert result["claims"][0]["independent_origin_count"] == 2
    assert result["accounts"][0]["independent_origin_count"] == 2


def test_known_content_copies_union_roots_and_unknown_derivation_abstains():
    docs = [
        observation("a", content_hash="sha256:" + "a" * 64),
        observation(
            "b",
            source_url="https://mirror.example/alice",
            content_hash="sha256:" + "a" * 64,
        ),
        observation("openai_web_research", source_url="https://model.example/result"),
    ]
    group = consolidate_observations(docs)["claims"][0]
    assert group["independent_origin_count"] == 1
    assert group["unknown_origin_count"] == 1


def test_explicit_derivation_resolves_in_reverse_order_without_weight_gain():
    parent = observation("a")
    child = observation(
        "ai_research",
        source_url="https://model.example/result",
        derived_from=[parent["id"]],
    )
    grandchild = observation(
        "case_chat",
        source_url="https://model.example/result2",
        derived_from=[child["id"]],
    )
    group = consolidate_observations([grandchild, child, parent])["claims"][0]
    assert group["independent_origin_count"] == 1
    assert group["unknown_origin_count"] == 0


def test_handle_across_platforms_is_never_same_account():
    instagram = observation("a", claims=[])
    tiktok = observation(
        "b",
        account={"platform": "tiktok", "profile_url": "https://www.tiktok.com/@alice"},
        claims=[],
    )
    result = consolidate_observations([instagram, tiktok])
    assert {item["platform"] for item in result["accounts"]} == {"instagram", "tiktok"}


def test_stable_id_rename_retains_url_and_handle_history():
    old = observation(
        "a",
        account={
            "platform": "github",
            "stable_id": "123",
            "profile_url": "https://github.com/old-name",
        },
        claims=[],
    )
    new = observation(
        "b",
        account={
            "platform": "github",
            "stable_id": "123",
            "profile_url": "https://github.com/new-name",
        },
        claims=[],
    )
    result = consolidate_observations([old, new])
    assert len(result["accounts"]) == 1
    assert result["accounts"][0]["handles"] == ["new-name", "old-name"]
    assert result["accounts"][0]["profile_urls"] == [
        "https://github.com/new-name",
        "https://github.com/old-name",
    ]


def test_handle_reuse_and_missing_stable_id_remain_reviewable():
    url = "https://www.instagram.com/alice/"
    docs = [
        observation(
            "a",
            account={"platform": "instagram", "stable_id": "100", "profile_url": url},
        ),
        observation(
            "b",
            account={"platform": "instagram", "stable_id": "200", "profile_url": url},
        ),
        observation("c"),
    ]
    result = consolidate_observations(docs)
    assert len(result["accounts"]) == 3
    assert result["conflicts"][0]["kind"] == "profile_url_stable_id_collision"
    assert all(item["conflicts"] for item in result["accounts"])
    assert len(result["conflicts"][0]["group_ids"]) == 3


def test_url_only_to_stable_id_needs_explicit_continuity_decision():
    one = observation()
    two = observation(
        "b",
        account={
            "platform": "instagram",
            "stable_id": "100",
            "profile_url": "https://www.instagram.com/alice/",
        },
    )
    result = consolidate_observations([one, two])
    assert len(result["accounts"]) == 2
    assert result["conflicts"][0]["kind"] == "unresolved_account_continuity"


def test_history_and_role_organization_qualifiers_do_not_collapse():
    values = [
        {
            "predicate": "affiliation",
            "value": "Organization A",
            "role": "director",
            "valid_from": "2020",
            "valid_to": "2021",
        },
        {
            "predicate": "affiliation",
            "value": "Organization A",
            "role": "director",
            "valid_from": "2022",
        },
        {
            "predicate": "affiliation",
            "value": "Organization A",
            "role": "consultant",
            "valid_from": "2022",
        },
        {
            "predicate": "affiliation",
            "value": "Organization B",
            "role": "consultant",
            "valid_from": "2022",
        },
    ]
    result = consolidate_observations([observation(claims=values)])
    assert len(result["claims"]) == 4
    assert not result["conflicts"]


def test_contradictory_single_value_claims_preserved():
    result = consolidate_observations(
        [
            observation(
                claims=[
                    {"predicate": "date_of_birth", "value": "1990-01-01"},
                    {"predicate": "date_of_birth", "value": "1992-01-01"},
                ]
            )
        ]
    )
    assert len(result["claims"]) == 2
    assert result["conflicts"][0]["kind"] == "contradictory_qualified_values"


def test_replay_idempotent_later_attempt_retained_and_mutation_rejected():
    first = observation()
    replay = observation()
    later = normalize_observation(
        first["payload"],
        **{**SCOPE, "attempt_id": "attempt-b", "observed_at": "2026-09-12T10:00:00Z"},
    )
    result = consolidate_observations([first, replay, later])
    assert result["observation_count"] == 2
    assert result["accounts"][0]["observed_times"] == [
        "2026-09-11T10:00:00Z",
        "2026-09-12T10:00:00Z",
    ]
    changed = copy.deepcopy(first)
    changed["status"] = "not_found"
    with pytest.raises(ObservationContractError, match="changed evidence"):
        consolidate_observations([first, changed])


@pytest.mark.parametrize(
    "native, expected",
    [
        ("Registered", "found"),
        ("Available", "not_found"),
        ("candidate", "candidate"),
        ("Unknown", "inconclusive"),
        ("rate_limited", "blocked"),
        ("timed_out", "timeout"),
        ("failed", "error"),
        ("stopped", "cancelled"),
        ("skipped", "not_executed"),
        ("Illegal", "not_executed"),
        ("novel-native-status", "inconclusive"),
    ],
)
def test_native_outcomes_preserved(native, expected):
    doc = observation(status=native, claims=[])
    assert doc["status"] == expected
    assert doc["native_status"] == native


def test_blocked_and_timeout_diagnostics_are_never_negative():
    assert (
        normalize_status("Unknown", error={"code": "provider_rate_limited"})[0]
        == "blocked"
    )
    assert (
        normalize_status("Unknown", error={"code": "request_timeout"})[0] == "timeout"
    )


def test_google_live_fields_and_hashes_never_enter_durable_observation():
    doc = observation(
        "google_places_live_details",
        place_id="place-123",
        formatted_address="Restricted live address",
        phone="1234",
        content_hash="sha256:" + "a" * 64,
        content="live raw data",
    )
    serialized = json.dumps(doc)
    assert doc["retention"]["mode"] == "metadata_only"
    assert not doc["retention"]["final_eligible"]
    assert doc["payload"]["place_id"] == "place-123"
    assert not doc["claims"] and doc["account"] is None
    for value in (
        "Restricted live address",
        "1234",
        "live raw data",
        "instagram.com",
        "a" * 64,
    ):
        assert value not in serialized


def test_case_scope_cannot_cross_and_reuse_keeps_source_lineage():
    first = observation(original_evidence_id="shared-evidence")
    second = normalize_observation(
        first["payload"], **{**SCOPE, "case_id": "case-b", "subject_id": "subject-b"}
    )
    result = consolidate_observations([first, second])
    assert len(result["accounts"]) == len(result["claims"]) == 2
    assert len(result["case_accounts"]) == 2
    assert (
        first["original_evidence_id"]
        == second["original_evidence_id"]
        == "shared-evidence"
    )
    with pytest.raises(ObservationContractError, match="scope"):
        normalize_observation({"case_id": "foreign-case"}, **SCOPE)
    with pytest.raises(ObservationContractError, match="subject"):
        qualified_claim_identity(
            {"subject_id": "foreign", "predicate": "name", "value": "Alice"},
            case_id="case-a",
            subject_id="subject-a",
        )


def test_case_account_consolidation_does_not_merge_subject_attribution():
    first = observation()
    second = normalize_observation(
        first["payload"], **{**SCOPE, "subject_id": "subject-b"}
    )
    result = consolidate_observations([first, second])
    assert len(result["accounts"]) == 2
    assert len(result["case_accounts"]) == 1
    physical = result["case_accounts"][0]
    assert physical["subject_ids"] == ["subject-a", "subject-b"]
    assert len(physical["account_hypothesis_ids"]) == 2
    assert len(physical["observation_ids"]) == 2
    assert physical["identity_status"] == "unverified"


def test_legacy_social_account_descriptor_merges_with_report_hypothesis():
    report = normalize_observation(
        {
            "source_engine": "maigret_report",
            "site_name": "Instagram",
            "source_url": "https://www.instagram.com/alice/",
            "status": "claimed",
            "username": "alice",
        },
        **SCOPE,
    )
    legacy = next(
        iter_legacy_claim_observations(
            [
                {
                    "id": "legacy-social",
                    "persona_id": "subject-a",
                    "source_engine": "maigret",
                    "field_name": "social_account",
                    "value": {
                        "platform": "Instagram",
                        "username": "alice",
                        "url": "https://instagram.com/alice",
                    },
                    "evidence": [
                        {
                            "id": "evidence-legacy",
                            "source_name": "Instagram",
                            "source_url": "https://www.instagram.com/alice/",
                        }
                    ],
                }
            ],
            **SCOPE,
        )
    )
    result = consolidate_observations([legacy, report])
    assert len(result["accounts"]) == 1
    assert len(result["claims"]) == 1
    assert result["claims"][0]["observation_count"] == 2


def test_missing_time_is_explicit_legacy_not_fabricated():
    with pytest.raises(ObservationContractError, match="time"):
        normalize_observation({"status": "found"}, **{**SCOPE, "observed_at": None})
    doc = normalize_observation(
        {"status": "found"}, **{**SCOPE, "observed_at": None}, legacy=True
    )
    assert doc["observed_at"] is None and doc["provenance_incomplete"]


def test_canonical_profiles_reject_posts_roots_and_platform_collision():
    assert (
        canonical_account(
            {
                "platform": "instagram",
                "profile_url": "https://www.instagram.com/p/post-id/",
            }
        )
        is None
    )
    assert (
        canonical_account(
            {"platform": "example", "profile_url": "https://example.test/"}
        )
        is None
    )
    assert canonical_account({"platform": "github", "handle": "alice"}) is None
    with pytest.raises(ObservationContractError, match="platform"):
        canonical_account(
            {"platform": "tiktok", "profile_url": "https://www.instagram.com/alice/"}
        )
    with pytest.raises(ObservationContractError, match="stable ID"):
        canonical_account(
            {
                "platform": "facebook",
                "stable_id": "456",
                "profile_url": "https://www.facebook.com/profile.php?id=123",
            }
        )


def test_unknown_url_semantics_and_input_values_not_guessed():
    assert (
        canonical_source_url(
            "https://EXAMPLE.test/User/Alice?q=One&utm_source=track#section"
        )
        == "https://example.test/User/Alice?q=One"
    )
    upper = canonical_account(
        {"platform": "example", "profile_url": "https://example.test/Alice"}
    )
    lower = canonical_account(
        {"platform": "example", "profile_url": "https://example.test/alice"}
    )
    assert upper["canonical_url"] != lower["canonical_url"]
    assert canonical_source_url("https://secret:password@example.test/Alice") is None
    assert canonical_source_url("http://127.0.0.1/private") is None
    a = qualified_claim_identity(
        {"predicate": "email", "value": "Alice+tag@example.test"},
        case_id="c",
        subject_id="s",
    )
    b = qualified_claim_identity(
        {"predicate": "email", "value": "alice@example.test"},
        case_id="c",
        subject_id="s",
    )
    assert a["key"] != b["key"]


def test_profile_aliases_and_adapter_origin_ids_share_one_family():
    one = observation("a", source_url="https://instagram.com/Alice?utm_source=search")
    two = observation(
        "b",
        source_url="https://www.instagram.com/alice/",
        original_url="https://instagram.com/alice",
        source_origin_family="different-adapter-hash",
        independence="derivative",
    )
    group = consolidate_observations([one, two])["accounts"][0]
    assert group["independent_origin_count"] == 1
    assert len(set(group["origin_by_observation"].values())) == 1


def test_disputed_native_account_identity_is_preserved_for_direct_review():
    doc = observation(
        account={
            "platform": "facebook",
            "stable_id": "456",
            "profile_url": "https://www.facebook.com/profile.php?id=123",
        }
    )
    assert doc["account"] is None
    assert doc["normalization_findings"][0]["code"] == "account_identity_conflict"
    assert doc["payload"]["account"]["stable_id"] == "456"
    assert doc["native_status"] == "found"


def test_account_claim_dictionary_variants_share_group():
    one = observation(
        "a",
        claims=[
            {
                "predicate": "social_account",
                "value": {
                    "platform": "Instagram",
                    "username": "ALICE",
                    "url": "https://instagram.com/alice",
                },
            }
        ],
    )
    two = observation(
        "b",
        claims=[
            {
                "field_name": "social_account",
                "value": {
                    "platform": "instagram",
                    "username": "alice",
                    "url": "https://www.instagram.com/alice/",
                },
            }
        ],
    )
    assert len(consolidate_observations([one, two])["claims"]) == 1


def test_nonaccount_public_biography_is_a_source_and_claim_not_account():
    raw = {
        "source_engine": "wikipedia_public_biography",
        "status": "observed",
        "site_name": "Wikipedia",
        "page": {
            "page_id": "123",
            "title": "Alice Example",
            "url": "https://en.wikipedia.org/wiki/Alice_Example",
            "extract": "Alice Example is an author.",
        },
    }
    doc = normalize_observation(raw, **SCOPE)
    assert doc["account"] is None
    assert doc["canonical_url"] == "https://en.wikipedia.org/wiki/Alice_Example"
    assert doc["retention"]["final_eligible"]
    assert any(claim["field_name"] == "summary" for claim in doc["claims"])


def test_maigret_native_reports_and_email_registration_bridge():
    found = MaigretCheckResult(
        "alice",
        "Instagram",
        "https://www.instagram.com/alice/",
        MaigretCheckStatus.CLAIMED,
    )
    blocked = MaigretCheckResult(
        "alice",
        "TikTok",
        "https://www.tiktok.com/@alice",
        MaigretCheckStatus.UNKNOWN,
        error="captcha blocked",
    )
    result = {
        "general_results": [
            (
                "alice",
                "username",
                {"Instagram": {"status": found}, "TikTok": {"status": blocked}},
            )
        ],
        "individual_reports": [
            {
                "username": "alice",
                "claimed_profiles": [
                    {
                        "site_name": "Instagram",
                        "url": "https://www.instagram.com/alice/",
                        "evidence": {"fullname": "Alice Example"},
                    }
                ],
            }
        ],
        "collector_observations": [
            {
                "source_engine": "user_scanner_email",
                "subject_type": "email",
                "subject_value": "alice@example.test",
                "status": "Registered",
                "site_name": "Gravatar",
                "source_url": "https://gravatar.com",
                "extra": {},
            }
        ],
    }
    docs = list(iter_result_observations(result, **SCOPE))
    assert len(docs) == 4
    assert {doc["status"] for doc in docs} == {"found", "blocked"}
    email = next(doc for doc in docs if doc["engine"] == "user_scanner_email")
    assert email["account"] is None
    assert email["claims"][0]["field_name"] == "account_registration"
    assert any(
        claim.get("predicate") == "full_name" for doc in docs for claim in doc["claims"]
    )


def test_native_search_audit_accounts_for_every_planned_query():
    queries = [
        {"query_id": "q1", "platform": "instagram"},
        {"query_id": "q2", "platform": "tiktok"},
        {"query_id": "q3", "platform": "x"},
    ]
    audit = {
        "id": "audit-1",
        "created_at": "2026-09-11T10:00:00Z",
        "document": {
            "queries": queries,
            "runs": [
                {
                    "query": queries[0],
                    "evidence": [
                        {
                            "source_url": "https://www.instagram.com/alice/",
                            "snippet": "Alice",
                        }
                    ],
                },
                {
                    "query": queries[1],
                    "evidence": [],
                    "error": {"code": "provider_rate_limited"},
                },
            ],
            "candidates": [],
        },
    }
    docs = list(iter_result_observations({"profile_search_audits": [audit]}, **SCOPE))
    assert len(docs) == 3
    assert {doc["status"] for doc in docs} == {"candidate", "blocked", "not_executed"}
    assert len({doc["native_record_id"] for doc in docs}) == 3
    assert docs[0]["dependence"]["origin_url"] == "https://www.instagram.com/alice/"


def test_legacy_backfill_retains_reviews_evidence_id_and_replay():
    claim = {
        "id": "claim-a",
        "persona_id": "subject-a",
        "source_engine": "organization_website",
        "field_name": "affiliation",
        "value": "Organization A",
        "review_status": "rejected",
        "evidence": [
            {
                "id": "evidence-a",
                "source_url": "https://organization.example/staff/alice",
            }
        ],
    }
    docs = list(iter_legacy_claim_observations([claim], **SCOPE))
    assert docs[0]["payload"]["legacy_review_status"] == "rejected"
    assert docs[0]["original_evidence_id"] == "evidence-a"
    assert docs[0]["legacy"]
    assert docs == list(iter_legacy_claim_observations([claim], **SCOPE))
    assert len(consolidate_observations(docs + docs)["claims"]) == 1


def test_dense_50k_observation_reconciliation_is_bounded():
    """Engineering integrity fixture, not probability/source-accuracy validation."""
    base = observation(claims=[])
    started = time.monotonic()
    tracemalloc.start()

    def rows():
        # 50,000 returned records: 10% replay deliveries and 10% failures.
        for index in range(50_000):
            number = index - 1 if index % 10 == 9 else index
            doc = {
                **base,
                "id": "dense:" + str(number),
                "native_record_id": str(number),
                "status": (
                    ("blocked", "timeout", "error")[number % 3]
                    if number % 10 == 0
                    else "found"
                ),
            }
            yield doc

    result = consolidate_observations(rows())
    elapsed = time.monotonic() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert result["observation_count"] == 45_000
    assert result["outcome_counts"]["found"] == 40_000
    assert sum(result["outcome_counts"].values()) == 45_000
    assert len(result["accounts"][0]["observation_ids"]) == 45_000
    assert result["accounts"][0]["independent_origin_count"] == 1
    # Gross quadratic regressions are disallowed; production API SLOs have a
    # separate staging gate because this shared test host is not production.
    assert elapsed < 60
    assert peak < 180 * 1024 * 1024
