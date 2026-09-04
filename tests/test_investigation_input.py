import pytest

from maigret.web.investigation_input import (
    InvestigationInputError,
    build_investigation_plan,
    generate_username_variants,
    normalize_profile_url,
    public_ai_context,
    search_usernames,
)
from maigret.web.username_aliases import rank_username_aliases


def test_full_name_stays_intact_and_generates_bounded_account_candidates():
    plan = build_investigation_plan(
        {
            "identifier_type": ["full_name"],
            "identifier_value": ["Jati Pratomo"],
            "processing_mode": "same_subject",
            "generate_name_variants": "on",
        }
    )

    assert plan["identifiers"] == [{"type": "full_name", "value": "Jati Pratomo"}]
    assert search_usernames(plan)[:4] == [
        "jatipratomo",
        "jati.pratomo",
        "jati_pratomo",
        "jati-pratomo",
    ]
    assert "jati" not in search_usernames(plan)
    assert "pratomo" not in search_usernames(plan)
    assert len(search_usernames(plan)) <= 16


def test_identifiers_are_typed_and_sensitive_context_is_not_scanned():
    plan = build_investigation_plan(
        {
            "identifier_type": [
                "social_handle",
                "email",
                "phone",
                "full_name",
            ],
            "identifier_value": [
                "@jatipratomo",
                "Jati@Example.com",
                "+62 812-3456-7890",
                "Jati Pratomo",
            ],
            "processing_mode": "same_subject",
            "tags": ["social", "id", "SOCIAL"],
            "excluded_tags": ["gaming"],
            "include_terms": "urban planning, Jakarta Selatan",
            "exclude_terms": "fan page\nfootball club",
        }
    )

    assert search_usernames(plan) == ["jatipratomo"]
    assert plan["identifiers"] == [
        {"type": "social_handle", "value": "jatipratomo"},
        {"type": "email", "value": "jati@example.com"},
        {"type": "phone", "value": "+6281234567890"},
        {"type": "full_name", "value": "Jati Pratomo"},
    ]
    assert plan["include_terms"] == ["urban planning", "Jakarta Selatan"]
    assert plan["exclude_terms"] == ["fan page", "football club"]
    assert plan["tags"] == ["social", "id"]
    assert plan["excluded_tags"] == ["gaming"]


def test_case_source_filter_cannot_include_and_exclude_the_same_tag():
    with pytest.raises(InvestigationInputError, match="both included and excluded"):
        build_investigation_plan(
            {
                "identifier_type": ["username"],
                "identifier_value": ["johndoe"],
                "tags": ["social"],
                "excluded_tags": ["SOCIAL"],
            }
        )


def test_profile_url_extracts_handle_without_fetching():
    plan = build_investigation_plan(
        {
            "identifier_type": ["profile_url"],
            "identifier_value": ["https://www.instagram.com/jatipratomo/"],
        },
        profile_url_resolver=lambda url: {},
    )

    assert search_usernames(plan) == ["jatipratomo"]
    assert plan["identifiers"][0]["value"] == ("https://www.instagram.com/jatipratomo/")


def test_profile_url_rejects_embedded_credentials():
    with pytest.raises(InvestigationInputError, match="credentials"):
        normalize_profile_url("https://operator:secret@example.com/profile")


def test_context_only_submission_requires_a_searchable_account_candidate():
    with pytest.raises(InvestigationInputError, match="retained as context"):
        build_investigation_plan(
            {
                "identifier_type": ["email", "phone"],
                "identifier_value": ["jati@example.com", "+628123456789"],
            }
        )


def test_user_scanner_email_collection_is_explicit_and_requires_grouping():
    plan = build_investigation_plan(
        {
            "identifier_type": ["username", "email"],
            "identifier_value": ["alice", "alice@example.test"],
            "processing_mode": "same_subject",
            "enable_user_scanner_email": "on",
        }
    )

    assert plan["enable_user_scanner_email"] is True

    with pytest.raises(InvestigationInputError, match="One subject"):
        build_investigation_plan(
            {
                "identifier_type": ["username", "email"],
                "identifier_value": ["alice", "alice@example.test"],
                "processing_mode": "independent",
                "enable_user_scanner_email": "on",
            }
        )

    with pytest.raises(InvestigationInputError, match="one email"):
        build_investigation_plan(
            {
                "identifier_type": ["username", "email", "email"],
                "identifier_value": [
                    "alice",
                    "alice@example.test",
                    "alias@example.test",
                ],
                "processing_mode": "same_subject",
                "enable_user_scanner_email": "on",
            }
        )


def test_github_profile_enrichment_is_explicit_and_available_in_both_modes():
    base = {
        "identifier_type": ["username"],
        "identifier_value": ["alice"],
    }

    assert build_investigation_plan(base)["enable_github_profile_enrichment"] is False
    assert (
        build_investigation_plan({**base, "enable_github_profile_enrichment": "on"})[
            "enable_github_profile_enrichment"
        ]
        is True
    )
    assert (
        build_investigation_plan(
            {
                **base,
                "processing_mode": "independent",
                "enable_github_profile_enrichment": "on",
            }
        )["enable_github_profile_enrichment"]
        is True
    )


def test_archived_url_evidence_requires_explicit_opt_in():
    base = {
        "identifier_type": ["username"],
        "identifier_value": ["alice"],
    }

    assert build_investigation_plan(base)["enable_archived_url_evidence"] is False
    assert (
        build_investigation_plan({**base, "enable_archived_url_evidence": "on"})[
            "enable_archived_url_evidence"
        ]
        is True
    )


def test_ai_context_requires_explicit_consent():
    base = {
        "identifier_type": ["username", "full_name"],
        "identifier_value": ["jatipratomo", "Jati Pratomo"],
        "include_terms": "Jakarta",
        "exclude_terms": "fan page",
    }
    withheld = build_investigation_plan(base)
    approved = build_investigation_plan({**base, "allow_ai_context": "on"})

    assert public_ai_context(withheld) == {}
    assert public_ai_context(approved) == {
        "subject_label": "Jati Pratomo",
        "identifiers": [
            {"type": "username", "value": "jatipratomo"},
            {"type": "full_name", "value": "Jati Pratomo"},
        ],
        "include_terms": ["Jakarta"],
        "exclude_terms": ["fan page"],
    }


def test_variant_generation_avoids_unbounded_permutations():
    variants = generate_username_variants("Jati Budi Santoso Pratomo")

    assert len(variants) <= 16
    assert "jatibudisantosopratomo" in variants
    assert "jatipratomo" in variants
    assert "pratomojati" in variants


def test_ranked_aliases_are_explainable_transliterated_and_context_bounded():
    aliases = rank_username_aliases(
        ["José María Núñez"],
        nicknames=["Pepe"],
        contextual_numbers=["84"],
    )

    assert aliases[0] == {
        "value": "josemarianunez",
        "score": 100,
        "reason": "Full name in natural order",
        "selected": True,
    }
    assert any(alias["value"] == "pepenunez" for alias in aliases)
    assert any(alias["value"] == "josenunez84" for alias in aliases)
    assert not any(alias["value"].endswith("00") for alias in aliases)
    assert len(aliases) <= 24
    assert aliases == sorted(
        aliases, key=lambda item: (-item["score"], item["value"].casefold())
    )


@pytest.mark.parametrize(
    ("full_name", "expected_alias"),
    [
        ("Иван Иванов", "иваниванов"),
        ("王小明", "王小明"),
        ("محمد علي", "محمدعلي"),
        ("José 王", "jose王"),
        ("Søren Kierkegaard", "sørenkierkegaard"),
        ("李John Smith", "李johnsmith"),
    ],
)
def test_ranked_aliases_preserve_non_latin_name_tokens(full_name, expected_alias):
    plan = build_investigation_plan(
        {
            "identifier_type": ["full_name"],
            "identifier_value": [full_name],
            "generate_name_variants": "on",
        }
    )

    assert expected_alias in search_usernames(plan)


def test_analyst_can_edit_and_deselect_ranked_aliases():
    plan = build_investigation_plan(
        {
            "identifier_type": ["full_name"],
            "identifier_value": ["John Doe"],
            "generate_name_variants": "on",
            "alias_candidates_present": "1",
            "alias_candidate": ["johndoe", "johnny.d"],
            "selected_alias": ["johnny.d"],
        }
    )

    assert search_usernames(plan) == ["johnny.d"]
    assert plan["alias_candidates"] == [
        {
            "value": "johndoe",
            "score": 100,
            "reason": "Full name in natural order",
            "selected": False,
        },
        {
            "value": "johnny.d",
            "score": 70,
            "reason": "Analyst-edited alias candidate",
            "selected": True,
        },
    ]


def test_context_numbers_and_username_platform_policy_require_explicit_values():
    plan = build_investigation_plan(
        {
            "identifier_type": ["username", "full_name"],
            "identifier_value": ["johndoe", "John Doe"],
            "generate_name_variants": "on",
            "alias_context_numbers": "84, 1990",
            "enable_user_scanner_username": "on",
            "user_scanner_platforms_present": "1",
            "user_scanner_platform": ["instagram", "x"],
        }
    )

    assert plan["alias_context_numbers"] == ["84", "1990"]
    assert plan["user_scanner_username_platforms"] == ["instagram", "x"]
    assert plan["allow_user_scanner_vxtwitter"] is False

    approved = build_investigation_plan(
        {
            **{
                "identifier_type": ["username"],
                "identifier_value": ["johndoe"],
                "enable_user_scanner_username": "on",
                "user_scanner_platforms_present": "1",
                "user_scanner_platform": ["x"],
            },
            "allow_user_scanner_vxtwitter": "on",
        }
    )
    assert approved["allow_user_scanner_vxtwitter"] is True

    with pytest.raises(InvestigationInputError, match="1 to 6 digits"):
        build_investigation_plan(
            {
                "identifier_type": ["username"],
                "identifier_value": ["johndoe"],
                "alias_context_numbers": "00-99",
            }
        )


def test_alias_ranking_can_learn_a_separator_from_confirmed_profile_input():
    plan = build_investigation_plan(
        {
            "identifier_type": ["full_name", "profile_url"],
            "identifier_value": [
                "John Doe",
                "https://instagram.com/known_handle",
            ],
            "generate_name_variants": "on",
        },
        profile_url_resolver=lambda _url: {"known_handle": "username"},
    )

    learned = next(
        candidate
        for candidate in plan["alias_candidates"]
        if candidate["value"] == "john_doe"
    )
    assert learned["score"] == 100
    assert "learned from a confirmed profile" in learned["reason"]


def test_username_verification_rejects_more_than_sixteen_total_targets():
    with pytest.raises(InvestigationInputError, match="no more than 16 total"):
        build_investigation_plan(
            {
                "identifier_type": ["username"] * 16 + ["full_name"],
                "identifier_value": [f"exact{index}" for index in range(16)]
                + ["John Doe"],
                "generate_name_variants": "on",
                "alias_candidates_present": "1",
                "alias_candidate": ["johndoe"],
                "selected_alias": ["johndoe"],
                "enable_user_scanner_username": "on",
                "user_scanner_platforms_present": "1",
                "user_scanner_platform": ["instagram"],
            }
        )


def test_username_verification_cap_counts_the_deduplicated_target_union():
    exact_usernames = ["johndoe"] + [f"exact{index}" for index in range(14)]
    submitted = build_investigation_plan(
        {
            "identifier_type": ["username"] * len(exact_usernames) + ["full_name"],
            "identifier_value": exact_usernames + ["John Doe"],
            "generate_name_variants": "on",
            "alias_candidates_present": "1",
            "alias_candidate": ["johndoe", "john.doe"],
            "selected_alias": ["johndoe", "john.doe"],
            "enable_user_scanner_username": "on",
            "user_scanner_platforms_present": "1",
            "user_scanner_platform": ["instagram"],
        }
    )

    assert len(search_usernames(submitted)) == 16
    assert search_usernames(submitted).count("johndoe") == 1

    generated = build_investigation_plan(
        {
            "identifier_type": ["username", "full_name"],
            "identifier_value": ["johnmichaeldoe", "John Michael Doe"],
            "generate_name_variants": "on",
            "alias_nicknames": "Johnny, JD",
            "enable_user_scanner_username": "on",
            "user_scanner_platforms_present": "1",
            "user_scanner_platform": ["instagram"],
        }
    )

    assert len(search_usernames(generated)) == 16
    assert search_usernames(generated).count("johnmichaeldoe") == 1
