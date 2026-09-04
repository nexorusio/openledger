import json
import random
import re

import pytest

from maigret.sites import MaigretSite
from maigret.web.profile_reliability import (
    DetectorHealthRegistryError,
    classify_profile_detection,
    detector_health_for_site,
    empty_detector_health_registry,
    evolve_detector_health_registry,
    load_detector_health_registry,
    serialize_detector_health_registry,
)
from utils.detector_health_canary import (
    build_probe_plan,
    evaluate_probe_results,
    transport_aware_status,
)


def _classify(**overrides):
    values = {
        "username": "alice",
        "site_name": "Example",
        "url": "https://example.test/alice",
        "evidence": {},
        "check_type": "message",
        "health_state": "healthy",
        "status_context": "",
        "status_error": "",
    }
    values.update(overrides)
    return classify_profile_detection(**values)


def test_profile_specific_content_supports_account_not_subject_identity():
    decision = _classify(
        evidence={"fullname": "Alice Example", "description": "Researcher"}
    )

    assert decision["classification"] == "supported"
    assert decision["detection_confidence"] == "strong"
    assert decision["identity_status"] == "unverified"


def test_status_only_claimed_result_is_a_candidate_not_a_finding():
    decision = _classify(check_type="status_code")

    assert decision["classification"] == "candidate"
    assert decision["detection_confidence"] == "weak"
    assert "without profile-specific" in decision["reason"]


def test_reflected_input_username_is_not_profile_specific_by_itself():
    decision = _classify(evidence={"username": "@alice"})

    assert decision["classification"] == "candidate"
    assert decision["signals"] == []


@pytest.mark.parametrize(
    ("overrides", "reason_fragment"),
    [
        ({"health_state": "quarantined"}, "quarantined"),
        ({"status_context": "Captcha detected"}, "block, challenge"),
        ({"url": "javascript:alert(1)"}, "safe public profile URL"),
        (
            {"evidence": {"description": "Log in or create an account"}},
            "generic or missing-page shell",
        ),
    ],
)
def test_unreliable_detector_responses_are_suppressed(overrides, reason_fragment):
    decision = _classify(**overrides)

    assert decision["classification"] == "suppressed"
    assert reason_fragment in decision["reason"]


def test_degraded_detector_never_auto_promotes_rich_profile_content():
    decision = _classify(
        health_state="degraded",
        evidence={"fullname": "Alice Example", "uid": "12345"},
    )

    assert decision["classification"] == "candidate"
    assert decision["health_state"] == "degraded"


def test_registry_validation_and_case_insensitive_lookup(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-09-04T00:00:00+00:00",
                "sites": {"SoundCloud": {"state": "quarantined"}},
            }
        ),
        encoding="utf-8",
    )

    registry = load_detector_health_registry(path)

    assert detector_health_for_site(registry, "soundcloud") == "quarantined"
    assert detector_health_for_site(registry, "Unknown") == "untested"


def test_invalid_registry_is_rejected_as_one_document(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "sites": {"Example": {"state": "definitely-fine"}},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(DetectorHealthRegistryError):
        load_detector_health_registry(path)


def test_two_failures_quarantine_and_two_successes_recover():
    registry = empty_detector_health_registry()
    registry = evolve_detector_health_registry(
        registry,
        {"Example": {"outcome": "fail", "reason": "soft 404"}},
        checked_at="2026-09-01T00:00:00+00:00",
    )
    assert detector_health_for_site(registry, "Example") == "degraded"

    registry = evolve_detector_health_registry(
        registry,
        {"Example": {"outcome": "fail", "reason": "soft 404"}},
        checked_at="2026-09-02T00:00:00+00:00",
    )
    assert detector_health_for_site(registry, "Example") == "quarantined"

    registry = evolve_detector_health_registry(
        registry,
        {"Example": {"outcome": "pass", "reason": "clean"}},
        checked_at="2026-09-03T00:00:00+00:00",
    )
    assert detector_health_for_site(registry, "Example") == "quarantined"

    registry = evolve_detector_health_registry(
        registry,
        {"Example": {"outcome": "pass", "reason": "clean"}},
        checked_at="2026-09-04T00:00:00+00:00",
    )
    assert detector_health_for_site(registry, "Example") == "healthy"
    serialized = serialize_detector_health_registry(registry)
    assert serialized["sites"]["Example"]["consecutive_successes"] == 2


def test_unknown_canary_degrades_without_false_positive_accumulation():
    registry = evolve_detector_health_registry(
        empty_detector_health_registry(),
        {"Example": {"outcome": "unknown", "reason": "rate limited"}},
        checked_at="2026-09-04T00:00:00+00:00",
    )
    entry = serialize_detector_health_registry(registry)["sites"]["Example"]

    assert entry["state"] == "degraded"
    assert entry["consecutive_failures"] == 0


def test_unknown_canary_never_lifts_an_existing_quarantine():
    registry = evolve_detector_health_registry(
        empty_detector_health_registry(),
        {"Example": {"outcome": "fail", "reason": "soft 404"}},
    )
    registry = evolve_detector_health_registry(
        registry,
        {"Example": {"outcome": "fail", "reason": "soft 404"}},
    )
    registry = evolve_detector_health_registry(
        registry,
        {"Example": {"outcome": "unknown", "reason": "rate limited"}},
    )
    entry = serialize_detector_health_registry(registry)["sites"]["Example"]

    assert entry["state"] == "quarantined"
    assert entry["consecutive_successes"] == 0


def test_neutral_title_does_not_mask_explicit_login_shell_metadata():
    decision = _classify(
        evidence={
            "name": "Instagram",
            "description": "Log in or create an account",
        }
    )

    assert decision["classification"] == "suppressed"
    assert "generic or missing-page shell" in decision["reason"]


def test_canary_plan_uses_existing_missing_and_high_entropy_handles():
    site = MaigretSite(
        "Example",
        {
            "urlMain": "https://example.test/",
            "url": "https://example.test/{username}",
            "regexCheck": "^[a-z0-9]{3,15}$",
            "usernameClaimed": "knownuser",
            "usernameUnclaimed": "missinguser",
        },
    )

    plan = build_probe_plan(site, samples=3, rng=random.Random(7))

    assert [probe["kind"] for probe in plan[:2]] == [
        "declared_existing",
        "declared_missing",
    ]
    random_probes = plan[2:]
    assert len(random_probes) == 3
    assert all(re.fullmatch(site.regex_check, probe["username"]) for probe in random_probes)


def test_canary_evaluation_distinguishes_contradiction_from_unknown():
    failed = evaluate_probe_results(
        [
            {"kind": "declared_existing", "expected": "claimed", "actual": "claimed"},
            {"kind": "declared_missing", "expected": "available", "actual": "claimed"},
            {"kind": "high_entropy_missing", "expected": "available", "actual": "available"},
        ]
    )
    unknown = evaluate_probe_results(
        [
            {"kind": "declared_existing", "expected": "claimed", "actual": "unknown"},
            {"kind": "declared_missing", "expected": "available", "actual": "available"},
            {"kind": "high_entropy_missing", "expected": "available", "actual": "available"},
        ]
    )

    assert failed["outcome"] == "fail"
    assert unknown["outcome"] == "unknown"


@pytest.mark.parametrize("http_status", [403, 429, 503, 999])
def test_canary_treats_anti_bot_transport_as_unknown(http_status):
    assert (
        transport_aware_status("available", http_status=http_status)
        == "unknown"
    )


def test_canary_keeps_normal_missing_response_available():
    assert transport_aware_status("available", http_status=404) == "available"
