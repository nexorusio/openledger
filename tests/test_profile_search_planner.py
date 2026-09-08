# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import pytest

from maigret.web.profile_search_planner import (
    MAX_PROFILE_SEARCH_QUERIES,
    ProfileSearchPlanningError,
    plan_profile_search_queries,
)


def _plan():
    return {
        "identifiers": [
            {"type": "full_name", "value": "Alice Example"},
            {"type": "email", "value": "alice@example.test"},
            {"type": "phone", "value": "+628123456789"},
        ],
        "search_targets": [
            {
                "value": "alice.primary",
                "source_type": "username",
                "source_value": "alice.primary",
            },
            {
                "value": "alice_example",
                "source_type": "ranked_alias",
                "source_value": "Alice Example",
                "alias_score": 96,
                "alias_reason": "First and last name",
            },
            {
                "value": "examplealice",
                "source_type": "ranked_alias",
                "source_value": "Alice Example",
                "alias_score": 82,
                "alias_reason": "Reversed last and first name",
            },
        ],
        "include_terms": ["Jakarta Selatan"],
        "exclude_terms": ["fan page"],
    }


def test_planner_builds_stable_site_scoped_queries_with_ranked_seed_context():
    first = plan_profile_search_queries(
        _plan(), platforms=["instagram", "x"], max_results=4
    )
    second = plan_profile_search_queries(
        _plan(), platforms=["instagram", "x"], max_results=4
    )

    assert [query.as_dict() for query in first] == [
        query.as_dict() for query in second
    ]
    assert [(query.platform, query.query_text) for query in first[:2]] == [
        ("instagram", 'site:instagram.com "alice.primary"'),
        ("x", 'site:x.com "alice.primary"'),
    ]
    assert first[0].seed_kind == "username"
    assert first[0].seed_score == 100
    alias_query = next(
        query for query in first if query.seed_value == "alice_example"
    )
    assert alias_query.seed_kind == "alias"
    assert alias_query.seed_score == 96
    assert alias_query.seed_reason == "First and last name"
    assert any(query.seed_kind == "full_name" for query in first)
    assert all(query.max_results == 4 for query in first)


def test_planner_never_uses_email_phone_or_legacy_free_form_terms():
    serialized = " ".join(
        query.query_text for query in plan_profile_search_queries(_plan())
    )

    assert "alice@example.test" not in serialized
    assert "+628123456789" not in serialized
    assert "Jakarta Selatan" not in serialized
    assert "fan page" not in serialized


def test_approved_existing_account_is_prioritized_and_pending_is_ignored():
    evidence = [
        {
            "field_name": "social_account",
            "review_status": "pending",
            "value": {"username": "unreviewed_handle"},
        },
        {
            "field_name": "social_account",
            "review_status": "approved",
            "value": {"username": "confirmed_handle"},
        },
        {
            "field_name": "location",
            "review_status": "approved",
            "value": {"username": "wrong_field"},
        },
    ]

    queries = plan_profile_search_queries(
        _plan(), existing_evidence=evidence, platforms=["tiktok"]
    )

    assert queries[0].query_text == 'site:tiktok.com "confirmed_handle"'
    assert queries[0].seed_kind == "confirmed_username"
    assert "unreviewed_handle" not in {query.seed_value for query in queries}
    assert "wrong_field" not in {query.seed_value for query in queries}


def test_full_name_reserves_one_bounded_seed_slot_after_stronger_handles():
    plan = _plan()
    plan["search_targets"].extend(
        {
            "value": f"alias{index}",
            "source_type": "ranked_alias",
            "alias_score": 90 - index,
            "alias_reason": "Ranked alias",
        }
        for index in range(10)
    )

    queries = plan_profile_search_queries(plan, platforms=["facebook"])

    assert len(queries) == 5
    assert sum(query.seed_kind == "full_name" for query in queries) == 1


def test_query_cap_is_global_and_maintains_early_platform_coverage():
    queries = plan_profile_search_queries(
        _plan(),
        platforms=["facebook", "instagram", "threads", "tiktok", "x"],
        max_queries=7,
    )

    assert len(queries) == 7
    assert {query.platform for query in queries[:5]} == {
        "facebook",
        "instagram",
        "threads",
        "tiktok",
        "x",
    }


def test_default_plan_is_capped_and_query_ids_do_not_expose_seed_values():
    queries = plan_profile_search_queries(_plan())

    assert len(queries) <= MAX_PROFILE_SEARCH_QUERIES
    assert all("alice" not in query.query_id for query in queries)
    assert len({query.query_id for query in queries}) == len(queries)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"platforms": ["linkedin"]}, "supported profile-search platforms"),
        ({"platforms": []}, "at least one"),
        ({"max_queries": 26}, "max_queries must be between"),
        ({"max_results": 11}, "max_results must be between"),
    ],
)
def test_planner_rejects_unsupported_or_unbounded_scope(kwargs, message):
    with pytest.raises(ProfileSearchPlanningError, match=message):
        plan_profile_search_queries(_plan(), **kwargs)


def test_query_phrase_neutralizes_quote_and_backslash_expansion():
    plan = {
        "identifiers": [],
        "search_targets": [
            {
                "value": 'alice" OR site:example.test \\name',
                "source_type": "username",
            }
        ],
    }

    queries = plan_profile_search_queries(plan, platforms=["instagram"])

    assert queries[0].query_text == (
        'site:instagram.com "alice OR site:example.test name"'
    )
