# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

import json
import logging
from datetime import datetime, timezone

import pytest

from maigret.web.profile_search_backend import (
    BRAVE_SEARCH_API_URL,
    ProfileSearchClient,
    ProfileSearchConfigurationError,
    load_profile_search_config,
    read_profile_search_api_key,
)
from maigret.web.profile_search_contract import ProfileSearchQuery


class _Content:
    def __init__(self, payload):
        self.payload = payload

    async def read(self, limit):
        return self.payload[:limit]


class _Response:
    def __init__(self, status, payload, headers=None):
        self.status = status
        self.headers = headers or {}
        self.content = _Content(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _Session:
    def __init__(self, response, capture):
        self.response = response
        self.capture = capture

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def get(self, url, **kwargs):
        self.capture["url"] = url
        self.capture["request"] = kwargs
        return self.response


def _query(max_results=5):
    return ProfileSearchQuery(
        query_id="profile-query:1",
        platform="instagram",
        query_text='site:instagram.com "alice_example"',
        seed_kind="alias",
        seed_value="alice_example",
        max_results=max_results,
    )


def _config(tmp_path, **overrides):
    key_file = tmp_path / "brave_search_api_key"
    key_file.write_text("server-only-test-key", encoding="utf-8")
    key_file.chmod(0o600)
    values = {
        "OPENLEDGER_PROFILE_SEARCH_PROVIDER": "brave",
        "OPENLEDGER_PROFILE_SEARCH_API_KEY_FILE": str(key_file),
        "OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS": "12",
        "OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS": "3",
    }
    values.update(overrides)
    return load_profile_search_config(values), key_file


def test_search_is_disabled_by_default_without_reading_a_key():
    config = load_profile_search_config({})

    assert config.enabled is False
    assert config.provider == "disabled"
    with pytest.raises(ProfileSearchConfigurationError, match="disabled"):
        read_profile_search_api_key(config)


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("OPENLEDGER_PROFILE_SEARCH_PROVIDER", "unknown", "unsupported"),
        ("OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS", "0", "between 1 and 30"),
        ("OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS", "11", "between 1 and 10"),
    ],
)
def test_configuration_rejects_unsupported_or_unbounded_values(
    name, value, message
):
    with pytest.raises(ProfileSearchConfigurationError, match=message):
        load_profile_search_config({name: value})


def test_key_loader_requires_an_owner_only_regular_file(tmp_path):
    config, key_file = _config(tmp_path)
    key_file.chmod(0o640)

    with pytest.raises(ProfileSearchConfigurationError, match="owner-only"):
        read_profile_search_api_key(config)


@pytest.mark.asyncio
async def test_brave_client_returns_bounded_provider_neutral_evidence(
    tmp_path,
):
    config, _ = _config(tmp_path)
    capture = {}
    payload = json.dumps(
        {
            "web": {
                "results": [
                    {
                        "url": "https://www.instagram.com/alice_example/",
                        "title": "Alice Example",
                        "description": "Public Instagram profile.",
                    },
                    {
                        "url": "https://www.instagram.com/alice_example/",
                        "title": "Duplicate",
                        "description": "Duplicate result.",
                    },
                    {
                        "url": "http://localhost/private",
                        "title": "Unsafe",
                        "description": "Must be discarded.",
                    },
                    {
                        "url": "https://www.instagram.com/alice_work/",
                        "title": "Alice Work",
                        "description": "Another public candidate.",
                    },
                ]
            }
        }
    ).encode("utf-8")
    response = _Response(
        200,
        payload,
        {"Content-Length": str(len(payload)), "X-Request-Id": "request-42"},
    )

    def session_factory(**kwargs):
        capture["session"] = kwargs
        return _Session(response, capture)

    client = ProfileSearchClient(
        config,
        session_factory=session_factory,
        clock=lambda: datetime(2026, 9, 8, 9, 0, tzinfo=timezone.utc),
    )
    result = await client.search(_query(max_results=5))

    assert result.error is None
    assert [item.source_url for item in result.evidence] == [
        "https://www.instagram.com/alice_example/"
    ]
    assert result.provenance.provider == "brave"
    assert result.provenance.provider_request_id == "request-42"
    assert capture["url"] == BRAVE_SEARCH_API_URL
    assert capture["request"]["params"] == {
        "q": 'site:instagram.com "alice_example"',
        "count": 3,
        "safesearch": "strict",
        "spellcheck": "0",
        "text_decorations": "false",
    }
    assert capture["request"]["allow_redirects"] is False
    assert capture["session"]["headers"]["X-Subscription-Token"] == (
        "server-only-test-key"
    )
    serialized = json.dumps(result.as_dict())
    assert "server-only-test-key" not in serialized


@pytest.mark.asyncio
async def test_provider_error_is_bounded_and_logs_no_query_or_key(
    tmp_path, caplog
):
    config, _ = _config(tmp_path)
    capture = {}
    response = _Response(429, b'{"private":"server-only-test-key"}')

    def session_factory(**kwargs):
        capture["session"] = kwargs
        return _Session(response, capture)

    client = ProfileSearchClient(
        config,
        session_factory=session_factory,
        clock=lambda: datetime(2026, 9, 8, 9, 0, tzinfo=timezone.utc),
    )
    with caplog.at_level(logging.WARNING, logger="openledger.profile_search"):
        result = await client.search(_query())

    assert result.provenance is None
    assert result.evidence == ()
    assert result.error.code == "rate_limited"
    assert result.error.http_status == 429
    assert result.error.retryable is True
    assert "alice_example" not in caplog.text
    assert "server-only-test-key" not in caplog.text


@pytest.mark.asyncio
async def test_oversized_response_is_rejected_without_parsing(tmp_path):
    config, _ = _config(tmp_path)
    capture = {}
    response = _Response(
        200,
        b"{}",
        {"Content-Length": "1000001"},
    )

    def session_factory(**kwargs):
        return _Session(response, capture)

    result = await ProfileSearchClient(
        config,
        session_factory=session_factory,
        clock=lambda: datetime(2026, 9, 8, 9, 0, tzinfo=timezone.utc),
    ).search(_query())

    assert result.error.code == "oversized_response"
    assert result.error.retryable is False
