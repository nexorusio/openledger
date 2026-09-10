"""P3R retention validation for normalized profile checkpoint evidence."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from copy import deepcopy

import pytest

from maigret.web.collector_adapters import (
    normalize_github_public_profile,
    normalize_unfurl_url_analysis,
    normalize_user_scanner_results,
    normalize_user_scanner_username_results,
    normalize_wayback_capture_index,
)
from maigret.web.profile_checkpoint import validate_profile_observation


def _github():
    return normalize_github_public_profile(
        {
            "investigated_username": "alice",
            "github_login": "alice",
            "profile_url": "https://github.com/alice",
        },
        {
            "id": 1,
            "login": "alice",
            "type": "User",
            "html_url": "https://github.com/alice",
            "bio": "Public profile",
            "avatar_url": "https://avatars.githubusercontent.com/u/1",
        },
    )


def _unfurl():
    return normalize_unfurl_url_analysis(
        {
            "investigated_username": "alice",
            "site_name": "Example",
            "profile_url": "https://example.com/alice",
        },
        {
            "schema_version": 1,
            "engine": "dfir-unfurl",
            "version": "20260405",
            "remote_lookups": False,
            "nodes": [
                {
                    "id": 1,
                    "data_type": "url.query",
                    "key": "token",
                    "value": "token=not-retained",
                    "parent_id": None,
                }
            ],
        },
    )


def _wayback():
    return normalize_wayback_capture_index(
        {
            "investigated_username": "alice",
            "site_name": "Example",
            "profile_url": "https://example.com/alice",
        },
        [
            ["timestamp", "original", "statuscode", "mimetype", "digest"],
            [
                "20260101000000",
                "https://example.com/alice",
                "200",
                "text/html",
                "abc123",
            ],
        ],
    )


def _scanner_email():
    return normalize_user_scanner_results(
        "alice@example.test",
        [
            {
                "status": "Registered",
                "site_name": "Gravatar",
                "category": "social",
                "url": "https://gravatar.com",
                "extra": {"username": "alice"},
                "media": {"avatar": "https://example.test/avatar.png"},
            }
        ],
    )[0]


def _scanner_username():
    return normalize_user_scanner_username_results(
        [
            {
                "status": "Found",
                "username": "alice",
                "site_name": "Instagram",
                "url": "https://instagram.com/alice",
                "extra": {"confidence": "likely", "scan_stage": "cross_scan"},
            }
        ]
    )[0]


@pytest.mark.parametrize(
    "observation_factory",
    [_github, _unfurl, _wayback, _scanner_email, _scanner_username],
    ids=["github", "unfurl", "wayback", "scanner-email", "scanner-username"],
)
def test_checkpoint_validator_accepts_each_normalized_profile_adapter(
    observation_factory,
):
    observation = observation_factory()

    assert validate_profile_observation(observation) == observation


def test_checkpoint_validator_rejects_nested_credential_field():
    observation = _scanner_email()
    observation["extra"]["authorization"] = "Bearer retained-secret"

    with pytest.raises(ValueError, match="credential"):
        validate_profile_observation(observation)


def test_checkpoint_validator_rejects_oversized_unfurl_nodes():
    observation = _unfurl()
    node = observation["extra"]["nodes"][0]
    observation["extra"]["nodes"] = [deepcopy(node) for _ in range(81)]
    observation["extra"]["node_count"] = 81

    with pytest.raises(ValueError, match="node count"):
        validate_profile_observation(observation)


def test_checkpoint_validator_rejects_oversized_wayback_captures():
    observation = _wayback()
    capture = observation["extra"]["captures"][0]
    observation["extra"]["captures"] = [deepcopy(capture) for _ in range(11)]
    observation["extra"]["sampled_capture_count"] = 11

    with pytest.raises(ValueError, match="capture count"):
        validate_profile_observation(observation)
