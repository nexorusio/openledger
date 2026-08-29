import asyncio

import pytest

from maigret import ai
from maigret.ai import (
    _parse_responses_analysis,
    _parse_structured_response,
    get_ai_evidence_proposals,
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
