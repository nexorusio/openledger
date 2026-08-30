import pytest

from maigret.web.investigation_input import (
    InvestigationInputError,
    build_investigation_plan,
    generate_username_variants,
    normalize_profile_url,
    public_ai_context,
    search_usernames,
)


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
