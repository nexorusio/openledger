from maigret.web.collector_adapters import (
    USER_SCANNER_ENGINE,
    extract_user_scanner_claims,
    normalize_user_scanner_results,
    user_scanner_email_targets,
)


def test_user_scanner_email_targets_require_opt_in_and_one_subject_binding():
    plan = {
        "processing_mode": "same_subject",
        "enable_user_scanner_email": True,
        "identifiers": [
            {"type": "username", "value": "alice"},
            {"type": "email", "value": "Alice@Example.test"},
        ],
    }

    assert user_scanner_email_targets(plan) == ["alice@example.test"]
    assert user_scanner_email_targets({**plan, "enable_user_scanner_email": False}) == []
    assert user_scanner_email_targets({**plan, "processing_mode": "independent"}) == []


def test_user_scanner_results_are_bounded_and_normalized():
    observations = normalize_user_scanner_results(
        "alice@example.test",
        [
            {
                "status": "Registered",
                "site_name": "Gravatar",
                "category": "social",
                "url": "https://gravatar.com",
                "extra": {"username": "alice", "nested": {"ignored": True}},
                "media": {"avatar": "https://example.test/avatar.png"},
            },
            {
                "status": "Skipped",
                "site_name": "Medium",
                "reason": "Notifies the target",
            },
        ],
    )

    assert len(observations) == 2
    assert observations[0]["source_engine"] == USER_SCANNER_ENGINE
    assert observations[0]["subject_value"] == "alice@example.test"
    assert observations[0]["extra"] == {"username": "alice"}
    assert observations[0]["media"]["avatar"].startswith("https://")
    assert observations[1]["status"] == "Skipped"


def test_only_positive_registrations_become_pending_claim_candidates():
    observations = normalize_user_scanner_results(
        "alice@example.test",
        [
            {
                "status": "Registered",
                "site_name": "Gravatar",
                "url": "https://gravatar.com",
                "extra": {"username": "alice"},
            },
            {
                "status": "Not Registered",
                "site_name": "Example",
                "url": "https://example.test",
            },
        ],
    )

    candidates = extract_user_scanner_claims(observations)

    assert len(candidates) == 1
    assert candidates[0]["field_name"] == "account_registration"
    assert candidates[0]["source_engine"] == USER_SCANNER_ENGINE
    assert candidates[0]["native_status"] == "registered"
    assert candidates[0]["source_record_id"].startswith(
        f"{USER_SCANNER_ENGINE}:"
    )
    assert candidates[0]["confidence"] == 55
    assert candidates[0]["evidence"][0]["evidence_type"] == "email_registration_probe"
