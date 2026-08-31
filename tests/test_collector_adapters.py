import json

import pytest

from maigret.result import MaigretCheckResult, MaigretCheckStatus
from maigret.web.collector_adapters import (
    GITHUB_API_BASE_URL,
    GITHUB_API_VERSION,
    GITHUB_ENGINE,
    USER_SCANNER_ENGINE,
    extract_github_profile_claims,
    extract_user_scanner_claims,
    github_profile_targets,
    normalize_github_public_profile,
    normalize_user_scanner_results,
    run_github_public_profile,
    user_scanner_email_targets,
)


def _claimed_github(username="alice", url="https://github.com/alice"):
    result = MaigretCheckResult(
        username=username,
        site_name="GitHub",
        site_url_user=url,
        status=MaigretCheckStatus.CLAIMED,
    )
    return (username, "username", {"GitHub": {"status": result, "url_user": url}})


def _github_profile(**overrides):
    profile = {
        "login": "alice",
        "id": 12345,
        "type": "User",
        "html_url": "https://github.com/alice",
        "name": "Alice Example",
        "company": "Example Cooperative",
        "location": "Jakarta",
        "bio": "Public-interest technologist",
        "blog": "https://alice.example",
        "twitter_username": "alice_public",
        "avatar_url": "https://avatars.githubusercontent.com/u/12345?v=4",
        "created_at": "2012-01-02T03:04:05Z",
        "updated_at": "2026-08-30T10:00:00Z",
        "followers": 12,
        "following": 3,
        "public_repos": 8,
        "public_gists": 1,
    }
    profile.update(overrides)
    return profile


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
    assert (
        user_scanner_email_targets({**plan, "enable_user_scanner_email": False}) == []
    )
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
    assert candidates[0]["source_record_id"].startswith(f"{USER_SCANNER_ENGINE}:")
    assert candidates[0]["confidence"] == 55
    assert candidates[0]["evidence"][0]["evidence_type"] == "email_registration_probe"


def test_github_targets_require_opt_in_and_a_native_claimed_exact_profile():
    results = [
        _claimed_github(),
        _claimed_github("mallory", "https://evil.example/mallory"),
    ]

    assert (
        github_profile_targets(
            [_claimed_github("mallory", "https://github.com:invalid/mallory")],
            {"enable_github_profile_enrichment": True},
        )
        == []
    )

    assert github_profile_targets(results, {}) == []
    assert github_profile_targets(
        results, {"enable_github_profile_enrichment": True}
    ) == [
        {
            "investigated_username": "alice",
            "github_login": "alice",
            "profile_url": "https://github.com/alice",
        }
    ]


def test_github_public_profile_maps_facts_and_keeps_activity_as_metadata():
    target = {
        "investigated_username": "alice",
        "github_login": "alice",
        "profile_url": "https://github.com/alice",
    }
    observation = normalize_github_public_profile(target, _github_profile())
    candidates = extract_github_profile_claims([observation])

    assert observation["source_engine"] == GITHUB_ENGINE
    assert observation["source_record_id"] == "github-user:12345"
    assert observation["extra"]["followers"] == 12
    assert {candidate["field_name"] for candidate in candidates} == {
        "social_account",
        "platform_identifier",
        "full_name",
        "company",
        "current_location",
        "summary",
        "website",
        "photograph",
        "linked_profile_lead",
    }
    assert not {
        "created_at",
        "updated_at",
        "followers",
        "following",
        "public_repos",
        "public_gists",
    }.intersection(candidate["field_name"] for candidate in candidates)
    assert all(candidate["native_status"] == "observed" for candidate in candidates)
    assert all(
        candidate["source_record_id"] == "github-user:12345" for candidate in candidates
    )
    identifier = next(
        candidate
        for candidate in candidates
        if candidate["field_name"] == "platform_identifier"
    )
    assert identifier["value"]["identifier_type"] == "github_id"
    assert identifier["evidence"][0]["details"]["human_review_required"] is True


def test_github_organization_observation_never_becomes_a_persona_claim():
    target = {
        "investigated_username": "openai",
        "github_login": "openai",
        "profile_url": "https://github.com/openai",
    }
    observation = normalize_github_public_profile(
        target,
        _github_profile(
            login="openai",
            id=14957082,
            type="Organization",
            html_url="https://github.com/openai",
        ),
    )

    assert observation["status"] == "unsupported_account_type"
    assert extract_github_profile_claims([observation]) == []


class _FakeContent:
    def __init__(self, body):
        self.body = body

    async def read(self, _limit):
        return self.body


class _FakeResponse:
    def __init__(self, *, status, body=b"{}", headers=None):
        self.status = status
        self.headers = headers or {}
        self.content = _FakeContent(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _FakeSession:
    def __init__(self, response, calls, **options):
        self.response = response
        self.calls = calls
        self.calls.append(("session", options))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def get(self, url, **options):
        self.calls.append(("get", {"url": url, **options}))
        return self.response


@pytest.mark.asyncio
async def test_github_request_uses_fixed_origin_version_and_no_redirects():
    calls = []
    response = _FakeResponse(
        status=200,
        body=json.dumps(_github_profile()).encode(),
        headers={"X-RateLimit-Remaining": "59", "X-RateLimit-Reset": "1780000000"},
    )
    target = {
        "investigated_username": "alice",
        "github_login": "alice",
        "profile_url": "https://github.com/alice",
    }

    observation = await run_github_public_profile(
        target,
        session_factory=lambda **options: _FakeSession(response, calls, **options),
    )

    session_options = calls[0][1]
    request_options = calls[1][1]
    assert session_options["headers"]["X-GitHub-Api-Version"] == GITHUB_API_VERSION
    assert "Authorization" not in session_options["headers"]
    assert request_options == {
        "url": f"{GITHUB_API_BASE_URL}/users/alice",
        "allow_redirects": False,
    }
    assert observation["extra"]["rate_limit_remaining"] == "59"


@pytest.mark.asyncio
async def test_github_rate_limit_becomes_a_bounded_diagnostic():
    calls = []
    response = _FakeResponse(
        status=403,
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1780000000"},
    )
    target = {
        "investigated_username": "alice",
        "github_login": "alice",
        "profile_url": "https://github.com/alice",
    }

    observation = await run_github_public_profile(
        target,
        session_factory=lambda **options: _FakeSession(response, calls, **options),
    )

    assert observation["status"] == "rate_limited"
    assert observation["extra"]["rate_limit_remaining"] == "0"
    assert extract_github_profile_claims([observation]) == []
