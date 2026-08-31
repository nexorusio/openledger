from maigret.web.persona_intelligence import (
    extract_ai_persona_claims,
    extract_case_chat_persona_claims,
    extract_persona_claims,
    group_claims,
)


def test_extraction_ignores_unsupported_sensitive_inferences_and_unsafe_urls():
    report = {
        "username": "alice",
        "claimed_profiles": [
            {
                "site_name": "Unsafe",
                "url": "javascript:alert(1)",
                "confidence": "strong",
                "evidence": {"fullname": "Not accepted without a source"},
            },
            {
                "site_name": "Public source",
                "url": "https://example.test/alice",
                "confidence": "moderate",
                "evidence": {
                    "fullname": "Alice Example",
                    "financial_profile": "wealthy",
                    "criminal_record": "none",
                },
            },
        ],
    }
    claims = extract_persona_claims(report)
    fields = {claim["field_name"] for claim in claims}
    assert fields == {"social_account", "full_name"}
    assert all(
        evidence["source_url"].startswith("https://")
        for claim in claims
        for evidence in claim["evidence"]
    )


def test_grouped_form_keeps_requested_empty_categories_visible():
    groups = group_claims([])
    fields = {
        field["key"]: field["claims"] for group in groups for field in group["fields"]
    }
    assert fields["full_name"] == []
    assert fields["social_account"] == []
    assert fields["platform_identifier"] == []
    assert fields["linked_profile_lead"] == []
    assert fields["company_ownership"] == []
    assert fields["financial_profile"] == []
    assert fields["vehicle_ownership"] == []
    assert fields["criminal_record"] == []


def test_socid_account_intelligence_is_bounded_and_keeps_metadata_as_evidence():
    report = {
        "username": "alice",
        "claimed_profiles": [
            {
                "site_name": "Example Social",
                "url": "https://social.example.test/alice",
                "confidence": "strong",
                "evidence": {
                    "_extractor": "example_profile",
                    "uid": 123456,
                    "tiktok_id": "account-9001",
                    "country_id": "must-not-be-promoted",
                    "created_at": "2020-01-02 03:04:05",
                    "is_verified": "False",
                    "is_private": "True",
                    "follower_count": "1,234",
                    "following_count": "1.2K",
                    "website": "https://alice.example.test",
                    "links": (
                        "['https://social.example.test/alice/', "
                        "'https://alice.example.test', "
                        "'https://linked.example.test/alice', "
                        "'https://linked.example.test/alice', "
                        "'https://127.0.0.1/private', "
                        "'https://user:secret@host.example.test/alice', "
                        "'javascript:alert(1)']"
                    ),
                },
            }
        ],
    }

    claims = extract_persona_claims(report)
    by_field = {}
    for claim in claims:
        by_field.setdefault(claim["field_name"], []).append(claim)

    assert set(by_field) == {
        "social_account",
        "platform_identifier",
        "linked_profile_lead",
        "website",
    }
    account = by_field["social_account"][0]
    assert account["evidence"][0]["details"] == {
        "investigated_username": "alice",
        "account_metadata": {
            "created_at": "2020-01-02 03:04:05",
            "is_verified": False,
            "is_private": True,
            "follower_count": 1234,
        },
        "extractor": "example_profile",
    }
    assert account["observation_details"] == {
        "account_metadata": {
            "created_at": "2020-01-02 03:04:05",
            "is_verified": False,
            "is_private": True,
            "follower_count": 1234,
        },
        "extractor": "example_profile",
    }
    identifiers = by_field["platform_identifier"]
    assert {
        (claim["value"]["identifier_type"], claim["value"]["identifier"])
        for claim in identifiers
    } == {("uid", "123456"), ("tiktok_id", "account-9001")}
    assert all(claim["confidence"] == 80 for claim in identifiers)
    assert by_field["linked_profile_lead"][0]["value"] == (
        "https://linked.example.test/alice"
    )
    assert by_field["linked_profile_lead"][0]["confidence"] == 50
    assert by_field["website"][0]["value"] == "https://alice.example.test"


def test_platform_identifier_fingerprint_survives_username_and_url_change():
    first = extract_persona_claims(
        {
            "username": "old-name",
            "claimed_profiles": [
                {
                    "site_name": "Example Social",
                    "url": "https://social.example.test/old-name",
                    "confidence": "moderate",
                    "evidence": {"uid": "stable-123"},
                }
            ],
        }
    )
    renamed = extract_persona_claims(
        {
            "username": "new-name",
            "claimed_profiles": [
                {
                    "site_name": "example social",
                    "url": "https://social.example.test/new-name",
                    "confidence": "moderate",
                    "evidence": {"uid": "stable-123"},
                }
            ],
        }
    )

    first_identifier = next(
        claim for claim in first if claim["field_name"] == "platform_identifier"
    )
    renamed_identifier = next(
        claim for claim in renamed if claim["field_name"] == "platform_identifier"
    )
    assert first_identifier["fingerprint"] == renamed_identifier["fingerprint"]
    assert first_identifier["value"]["platform"] == "Example Social"
    assert renamed_identifier["value"]["platform"] == "example social"


def test_ai_proposals_require_known_user_cited_url_and_public_field():
    sources = [{"title": "Official biography", "url": "https://example.test/biography"}]
    raw = [
        {
            "username": "Alice",
            "field_name": "full_name",
            "value": "Alice Example",
            "confidence": 99,
            "source_url": "https://example.test/biography",
            "source_title": "Untrusted model title",
            "reason": "The official biography identifies Alice Example.",
        },
        {
            "username": "alice",
            "field_name": "criminal_record",
            "value": "None",
            "confidence": 80,
            "source_url": "https://example.test/biography",
            "source_title": "Official biography",
            "reason": "Sensitive field is never accepted.",
        },
        {
            "username": "alice",
            "field_name": "company",
            "value": "Example Ltd",
            "confidence": 80,
            "source_url": "https://invented.test/source",
            "source_title": "Invented",
            "reason": "The URL was not returned by web research.",
        },
        {
            "username": "mallory",
            "field_name": "occupation",
            "value": "Engineer",
            "confidence": 80,
            "source_url": "https://example.test/biography",
            "source_title": "Official biography",
            "reason": "The subject was not investigated.",
        },
    ]

    claims = extract_ai_persona_claims(
        raw,
        sources=sources,
        usernames=["alice"],
        model="gpt-5.6-terra",
    )

    assert len(claims) == 1
    claim = claims[0]
    assert claim["field_name"] == "full_name"
    assert claim["confidence"] == 85
    assert claim["source_engine"] == "openai_web_research"
    assert claim["evidence"][0]["source_name"] == "Official biography"
    assert claim["evidence"][0]["evidence_type"] == "cited_public_web"
    assert claim["evidence"][0]["details"]["human_review_required"] is True


def test_ai_public_account_proposal_keeps_structured_account_value():
    claims = extract_ai_persona_claims(
        [
            {
                "username": "alice",
                "field_name": "social_account",
                "value": "https://social.example/alice",
                "confidence": 75,
                "source_url": "https://social.example/alice",
                "source_title": "Official profile",
                "reason": "The profile identifies the same public figure.",
            }
        ],
        sources=[{"title": "Official profile", "url": "https://social.example/alice"}],
        usernames=["alice"],
        model="gpt-5.6-terra",
    )

    assert claims[0]["value"] == {
        "platform": "social.example",
        "url": "https://social.example/alice",
        "username": "alice",
    }


def test_ai_location_map_center_is_reviewable_and_invalid_coordinates_are_explained():
    diagnostics = {}
    claims = extract_ai_persona_claims(
        [
            {
                "username": "alice",
                "field_name": "current_location",
                "value": "Jakarta, Indonesia",
                "confidence": 76,
                "source_url": "https://example.test/biography",
                "source_title": "Biography",
                "reason": "The biography explicitly names Jakarta.",
                "latitude": -6.1754,
                "longitude": 106.8272,
                "coordinate_precision": "city",
            },
            {
                "username": "alice",
                "field_name": "occupation",
                "value": "Engineer",
                "confidence": 70,
                "source_url": "https://example.test/biography",
                "source_title": "Biography",
                "reason": "The biography names the occupation.",
                "latitude": -6.2,
                "longitude": 106.8,
                "coordinate_precision": "city",
            },
        ],
        sources=[
            {"title": "Biography", "url": "https://example.test/biography"}
        ],
        usernames=["alice"],
        model="gpt-5.6-terra",
        diagnostics=diagnostics,
    )

    assert len(claims) == 1
    assert claims[0]["latitude"] == -6.1754
    assert claims[0]["longitude"] == 106.8272
    assert claims[0]["evidence"][0]["details"]["coordinate_precision"] == "city"
    assert claims[0]["evidence"][0]["details"]["proposed_latitude"] == -6.1754
    assert diagnostics == {
        "received": 2,
        "accepted": 1,
        "rejected": {"invalid_coordinate_proposal": 1},
    }


def test_case_chat_keeps_user_statements_and_cited_research_separate():
    diagnostics = {}
    claims = extract_case_chat_persona_claims(
        [
            {
                "field_name": "company",
                "value": "Acme Labs",
                "confidence": 90,
                "evidence_basis": "user_statement",
                "source_url": None,
                "source_title": None,
                "reason": "The analyst explicitly supplied the employer.",
                "latitude": None,
                "longitude": None,
                "coordinate_precision": None,
            },
            {
                "field_name": "occupation",
                "value": "Research engineer",
                "confidence": 79,
                "evidence_basis": "public_web",
                "source_url": "https://example.test/alice",
                "source_title": "Official biography",
                "reason": "The official biography states this occupation.",
                "latitude": None,
                "longitude": None,
                "coordinate_precision": None,
            },
            {
                "field_name": "summary",
                "value": "Alice is likely influential.",
                "confidence": 45,
                "evidence_basis": "user_statement",
                "source_url": None,
                "source_title": None,
                "reason": "This is an inference rather than supplied evidence.",
                "latitude": None,
                "longitude": None,
                "coordinate_precision": None,
            },
        ],
        sources=[
            {
                "title": "Official biography",
                "url": "https://example.test/alice",
            }
        ],
        target_persona="alice",
        model="test-model",
        user_message="Alice works at Acme Labs.",
        user_message_id="user-message",
        assistant_message_id="assistant-message",
        provided_by="field.analyst",
        diagnostics=diagnostics,
    )

    assert {claim["field_name"] for claim in claims} == {"company", "occupation"}
    user_claim = next(
        claim for claim in claims if claim["evidence_basis"] == "user_statement"
    )
    web_claim = next(
        claim for claim in claims if claim["evidence_basis"] == "public_web"
    )
    assert user_claim["confidence"] == 50
    assert user_claim["provenance_message_id"] == "user-message"
    assert user_claim["evidence"][0]["source_url"] == ""
    assert web_claim["confidence"] == 79
    assert web_claim["provenance_message_id"] == "assistant-message"
    assert web_claim["evidence"][0]["source_url"] == "https://example.test/alice"
    assert diagnostics["rejected"]["unsupported_user_statement_field"] == 1


def test_case_chat_rejects_model_values_not_explicitly_in_user_message():
    diagnostics = {}
    claims = extract_case_chat_persona_claims(
        [
            {
                "field_name": "company",
                "value": "Invented Corporation",
                "confidence": 50,
                "evidence_basis": "user_statement",
                "source_url": None,
                "source_title": None,
                "reason": "The model guessed this company.",
                "latitude": None,
                "longitude": None,
                "coordinate_precision": None,
            }
        ],
        sources=[],
        target_persona="alice",
        model="test-model",
        user_message="Please evaluate Alice.",
        user_message_id="user-message",
        assistant_message_id="assistant-message",
        provided_by="field.analyst",
        diagnostics=diagnostics,
    )

    assert claims == []
    assert diagnostics["rejected"]["not_explicitly_user_provided"] == 1
