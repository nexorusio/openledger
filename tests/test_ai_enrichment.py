import asyncio

import pytest

from maigret import ai
from maigret.ai import (
    AIEnrichmentContractError,
    _check_response,
    _parse_responses_analysis,
    _parse_structured_response,
    get_ai_evidence_proposals,
    get_enriched_ai_analysis,
    validate_ai_api_base_url,
)


def test_remote_ai_error_body_is_not_exposed():
    class ErrorResponse:
        status = 500

        async def read(self):
            return b"upstream secret and internal stack trace"

    with pytest.raises(RuntimeError, match=r"OpenAI API error \(HTTP 500\)") as exc:
        asyncio.run(_check_response(ErrorResponse()))

    assert "upstream secret" not in str(exc.value)


def test_ai_api_endpoint_defaults_to_the_fixed_openai_origin():
    assert (
        validate_ai_api_base_url("https://api.openai.com/v1/")
        == "https://api.openai.com/v1"
    )


def test_custom_ai_endpoint_requires_server_authorization():
    with pytest.raises(ValueError, match="explicit server authorization"):
        validate_ai_api_base_url("https://gateway.example.test/openai/v1")

    assert (
        validate_ai_api_base_url(
            "https://gateway.example.test/openai/v1",
            allow_custom_endpoint=True,
        )
        == "https://gateway.example.test/openai/v1"
    )


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://user:password@api.openai.com/v1",
        "https://api.openai.com/v1?target=http://127.0.0.1",
        "https://api.openai.com/v1#fragment",
        "http://gateway.example.test/v1",
        "file:///etc/passwd",
    ],
)
def test_ai_api_endpoint_rejects_unsafe_urls(endpoint):
    with pytest.raises(ValueError):
        validate_ai_api_base_url(endpoint, allow_custom_endpoint=True)


def test_private_ai_endpoint_requires_separate_authorization():
    with pytest.raises(ValueError, match="special-purpose"):
        validate_ai_api_base_url(
            "https://169.254.169.254/latest/meta-data",
            allow_custom_endpoint=True,
        )

    assert (
        validate_ai_api_base_url(
            "http://127.0.0.1:11434/v1",
            allow_custom_endpoint=True,
        )
        == "http://127.0.0.1:11434/v1"
    )
    assert (
        validate_ai_api_base_url(
            "https://10.20.30.40/v1",
            allow_custom_endpoint=True,
            allow_private_endpoint=True,
        )
        == "https://10.20.30.40/v1"
    )


def test_responses_analysis_preserves_deduplicated_safe_citations():
    payload = {
        "output": [
            {"type": "web_search_call", "status": "completed"},
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": "Maigret evidence supports the identified subject.",
                        "annotations": [
                            {
                                "type": "url_citation",
                                "url": "https://example.com/profile",
                                "title": "Official Maigret profile",
                            },
                            {
                                "type": "url_citation",
                                "url": "https://example.com/profile",
                                "title": "Duplicate",
                            },
                            {
                                "type": "url_citation",
                                "url": "javascript:alert(1)",
                                "title": "Unsafe",
                            },
                        ],
                    }
                ],
            },
        ]
    }

    result = _parse_responses_analysis(payload)

    assert result["analysis"].startswith("OpenLedger evidence supports")
    assert result["sources"] == [
        {"title": "Official OpenLedger profile", "url": "https://example.com/profile"}
    ]
    assert result["web_search_completed"] is True


def test_required_web_enrichment_rejects_uncited_model_only_prose():
    model_only = {
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": "Assessment."}],
            }
        ]
    }
    searched_without_citations = {
        "output": [
            {"type": "web_search_call", "status": "completed"},
            {
                "type": "message",
                "content": [{"type": "output_text", "text": "Assessment."}],
            },
        ]
    }

    with pytest.raises(AIEnrichmentContractError, match="did not perform"):
        _parse_responses_analysis(model_only, require_web_search=True)
    with pytest.raises(AIEnrichmentContractError, match="no cited public sources"):
        _parse_responses_analysis(
            searched_without_citations,
            require_web_search=True,
        )


def test_enriched_analysis_request_requires_web_search_when_enabled(monkeypatch):
    captured = {}

    class FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def json(self):
            return {
                "output": [
                    {"type": "web_search_call", "status": "completed"},
                    {
                        "type": "message",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "Cited assessment.",
                                "annotations": [
                                    {
                                        "type": "url_citation",
                                        "url": "https://example.test/source",
                                        "title": "Source",
                                    }
                                ],
                            }
                        ],
                    },
                ]
            }

    class FakeSession:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def post(self, url, *, json, headers):
            captured.update(url=url, payload=json, headers=headers)
            return FakeResponse()

    monkeypatch.setattr(ai.aiohttp, "ClientSession", FakeSession)
    result = asyncio.run(
        get_enriched_ai_analysis(
            api_key="test-key",
            investigation_evidence="evidence",
            model="gpt-5.6-terra",
            web_search_enabled=True,
        )
    )

    assert result["sources"] == [
        {"title": "Source", "url": "https://example.test/source"}
    ]
    assert captured["payload"]["tools"] == [{"type": "web_search"}]
    assert captured["payload"]["tool_choice"] == "required"


def test_structured_proposal_response_requires_json_object_with_list():
    payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": '{"proposals":[{"username":"alice"}]}',
                    }
                ],
            }
        ]
    }

    assert _parse_structured_response(payload) == [{"username": "alice"}]

    payload["output"][0]["content"][0]["text"] = "not json"
    with pytest.raises(RuntimeError, match="invalid evidence proposal JSON"):
        _parse_structured_response(payload)


def test_structured_proposal_response_handles_refusal():
    payload = {
        "output": [
            {
                "type": "message",
                "content": [{"type": "refusal", "refusal": "Cannot assist"}],
            }
        ]
    }
    with pytest.raises(RuntimeError, match="declined"):
        _parse_structured_response(payload)


def test_evidence_proposal_request_uses_strict_schema_without_web_search(monkeypatch):
    captured = {}

    class FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def json(self):
            return {
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": '{"proposals":[]}'},
                        ],
                    }
                ]
            }

    class FakeSession:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def post(self, url, *, json, headers):
            captured.update(url=url, payload=json, headers=headers)
            return FakeResponse()

    monkeypatch.setattr(ai.aiohttp, "ClientSession", FakeSession)
    proposals = asyncio.run(
        get_ai_evidence_proposals(
            api_key="test-key",
            investigation_evidence="evidence",
            analysis="assessment",
            sources=[{"title": "Source", "url": "https://example.test/source"}],
            model="gpt-5.6-terra",
        )
    )

    assert proposals == []
    assert captured["url"] == "https://api.openai.com/v1/responses"
    assert captured["payload"]["text"]["format"]["type"] == "json_schema"
    assert captured["payload"]["text"]["format"]["strict"] is True
    proposal_schema = captured["payload"]["text"]["format"]["schema"]["properties"]["proposals"]["items"]
    assert {"latitude", "longitude", "coordinate_precision"}.issubset(
        proposal_schema["required"]
    )
    assert "tools" not in captured["payload"]
