from maigret.web.persona_intelligence import extract_persona_claims, group_claims


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
    assert fields["company_ownership"] == []
    assert fields["financial_profile"] == []
    assert fields["vehicle_ownership"] == []
    assert fields["criminal_record"] == []
