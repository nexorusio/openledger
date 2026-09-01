import asyncio

import pytest

from maigret import ai
from maigret.ai import (
    AIEnrichmentContractError,
    _check_response,
    _parse_responses_analysis,
    _parse_structured_response,
    get_ai_evidence_proposals,
    get_case_chat_claim_proposals,
    get_case_chat_response,
    get_enriched_ai_analysis,
    get_organization_context_proposals,
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


def test_case_chat_request_sends_bounded_case_memory_and_requires_cited_research(
    monkeypatch,
):
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
                                "text": "Case answer with cited corroboration.",
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
        get_case_chat_response(
            api_key="test-key",
            case_context={"id": "case-1", "personas": []},
            conversation=[
                {"role": "user", "author": "analyst", "content": "Earlier question"}
            ],
            user_message="Research this subject",
            model="gpt-5.6-terra",
            web_search_enabled=True,
        )
    )

    assert result["sources"][0]["url"] == "https://example.test/source"
    assert captured["url"] == "https://api.openai.com/v1/responses"
    assert captured["payload"]["tools"] == [{"type": "web_search"}]
    assert captured["payload"]["tool_choice"] == "required"
    assert "Earlier question" in captured["payload"]["input"]
    assert "Research this subject" in captured["payload"]["input"]


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
    assert {"email", "phone", "address"}.issubset(
        proposal_schema["properties"]["field_name"]["enum"]
    )
    normalized_instructions = " ".join(
        captured["payload"]["instructions"].split()
    )
    assert "explicitly published" in normalized_instructions
    assert "educational institution" in normalized_instructions
    assert "separate occupation and company proposals" in normalized_instructions
    assert "do not infer one from a role title" in normalized_instructions
    assert "tools" not in captured["payload"]


def test_case_chat_proposal_request_supports_user_and_web_evidence_without_browsing(
    monkeypatch,
):
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
                            {"type": "output_text", "text": '{"proposals":[]}'}
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
        get_case_chat_claim_proposals(
            api_key="test-key",
            target_persona="alice",
            user_message="Alice works at Acme Labs.",
            assistant_answer="This remains unverified.",
            sources=[],
            model="gpt-5.6-terra",
        )
    )

    assert proposals == []
    proposal_schema = captured["payload"]["text"]["format"]["schema"]
    evidence_basis = proposal_schema["properties"]["proposals"]["items"][
        "properties"
    ]["evidence_basis"]
    assert set(evidence_basis["enum"]) == {"user_statement", "public_web"}
    normalized_instructions = " ".join(
        captured["payload"]["instructions"].split()
    )
    assert "return two separate proposals" in normalized_instructions
    assert "do not infer one from a role title" in normalized_instructions
    assert "tools" not in captured["payload"]


def test_organization_context_proposals_use_citation_bound_strict_schema(monkeypatch):
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
                            {"type": "output_text", "text": '{"proposals":[]}'}
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
        get_organization_context_proposals(
            api_key="test-key",
            organization_name="Unistellar",
            legal_jurisdiction={"code": "ID"},
            official_website={"url": "https://www.unistellar.co/"},
            research_answer="A cited company profile publishes an address.",
            sources=[
                {
                    "title": "Unistellar | LinkedIn",
                    "url": "https://www.linkedin.com/company/unistellar/",
                }
            ],
            model="gpt-5.6-terra",
        )
    )

    assert proposals == []
    assert captured["url"] == "https://api.openai.com/v1/responses"
    assert "tools" not in captured["payload"]
    response_format = captured["payload"]["text"]["format"]
    assert response_format["name"] == "openledger_organization_context_proposals"
    assert response_format["strict"] is True
    item_schema = response_format["schema"]["properties"]["proposals"]["items"]
    assert {"company_profile", "business_address", "headquarters"}.issubset(
        item_schema["properties"]["observation_type"]["enum"]
    )
    assert {"professional_profile", "map_listing"}.issubset(
        item_schema["properties"]["source_role"]["enum"]
    )
    assert "must not be described" in " ".join(
        captured["payload"]["instructions"].split()
    )
