import asyncio
import json

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
    get_combined_case_chat_response,
    get_combined_investigation_insights,
    get_enriched_ai_analysis,
    get_organization_context_proposals,
    get_pipeline_group_rankings,
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


def test_pipeline_ranking_sends_bounded_complete_candidate_records(monkeypatch):
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
                            {
                                "type": "output_text",
                                "text": json.dumps(
                                    {
                                        "rankings": [
                                            {
                                                "group_id": "group-1",
                                                "shortlisted": True,
                                                "priority": "high",
                                                "reason": "Two retained sources support review.",
                                            }
                                        ]
                                    }
                                ),
                            }
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
    rankings = asyncio.run(
        get_pipeline_group_rankings(
            api_key="test-key",
            subject_label="Synthetic subject",
            groups=[
                {
                    "group_id": "group-1",
                    "kind": "account",
                    "finding": {"platform": "github", "handle": "synthetic"},
                    "evidence_status": "source_supported",
                    "support_origin_families": 2,
                    "observation_count": 3,
                    "missing_evidence": [],
                    "source_examples": [
                        {
                            "source_name": "Public profile",
                            "source_url": "https://example.test/synthetic",
                        }
                    ],
                }
            ],
            model="test-model",
        )
    )

    assert rankings[0]["group_id"] == "group-1"
    structured_input = json.loads(captured["payload"]["input"])
    assert structured_input["candidate_findings"][0]["finding"] == (
        '{"platform": "github", "handle": "synthetic"}'
    )
    assert captured["payload"]["text"]["format"]["name"] == (
        "openledger_pipeline_group_rankings"
    )
    assert "tools" not in captured["payload"]


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
                            {
                                "type": "url_citation",
                                "url": "https://user:secret@example.com/private",
                                "title": "Credential-bearing URL",
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


def test_required_web_enrichment_rejects_oversized_citation_without_altering_it():
    oversized = "https://example.test/" + "x" * 2100
    payload = {"output": [
        {"type": "web_search_call", "status": "completed"},
        {"type": "message", "content": [{"type": "output_text", "text": "An answer.",
            "annotations": [{"type": "url_citation", "url": oversized}]}]},
    ]}
    assert _parse_responses_analysis(payload)["sources"] == []
    with pytest.raises(AIEnrichmentContractError, match="no cited public sources"):
        _parse_responses_analysis(payload, require_web_search=True)


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
                {"role": "user", "author": "analyst", "content": "Earlier question"},
                {
                    "role": "assistant",
                    "content": "A regional URL was cited.",
                    "sources": [
                        {
                            "url": "https://id.linkedin.com/in/alice-example?ref=search",
                            "title": "Public profile",
                        }
                    ],
                    "proposals": {
                        "url_evidence": [
                            {
                                "supplied_url": "https://www.linkedin.com/in/alice-example/",
                                "citation_status": "profile_url_cited",
                                "direct_access": "not_verified",
                                "identity_status": "unverified",
                            }
                        ]
                    },
                },
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
    context = json.loads(captured["payload"]["input"])
    prior = context["conversation_history"][-1]
    assert (
        prior["sources"][0]["url"]
        == "https://id.linkedin.com/in/alice-example?ref=search"
    )
    assert prior["url_evidence"][0]["identity_status"] == "unverified"
    assert prior["url_evidence"][0]["direct_access"] == "not_verified"
    assert "failed\ndirect retrieval" in captured["payload"]["instructions"]
    assert (
        "regional LinkedIn URLs only when actually returned by search"
        in captured["payload"]["instructions"]
    )


@pytest.mark.parametrize("repetitions", [1, 8])
def test_case_chat_memory_retains_exact_provenance_before_shortening_text(
    monkeypatch, repetitions
):
    captured = {}
    supplied = [
        f"https://{host}/in/alice-example?ref=" + "s" * 1800
        for host in ("www.linkedin.com", "uk.linkedin.com", "ca.linkedin.com")
    ]
    cited = "https://id.linkedin.com/in/alice-example?ref=" + "c" * 1800
    content = "The cited public excerpt has limited evidence. " * 100
    prior = {
        "role": "assistant",
        "content": content,
        "sources": [{"url": cited, "title": "Actual public citation"}],
        "proposals": {
            "url_evidence": [
                {
                    "supplied_url": url,
                    "citation_urls": [cited],
                    "citation_count": 1,
                    "citation_status": "profile_url_cited",
                    "direct_access": "not_verified",
                    "identity_status": "unverified",
                }
                for url in supplied
            ]
        },
    }

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
                            {
                                "type": "output_text",
                                "text": "The earlier source remains unverified.",
                            }
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

        def post(self, _url, *, json, headers):
            captured.update(json)
            return FakeResponse()

    monkeypatch.setattr(ai.aiohttp, "ClientSession", FakeSession)
    asyncio.run(
        get_case_chat_response(
            api_key="fixture",
            case_context={},
            conversation=[prior] * repetitions,
            user_message="What source did you cite?",
            web_search_enabled=False,
        )
    )
    history = json.loads(captured["input"])["conversation_history"]
    assert history
    assert len(json.dumps(history, ensure_ascii=False)) <= 30_000
    newest = history[-1]
    assert len(json.dumps(newest, ensure_ascii=False)) <= 10_000
    assert newest["content_truncated"] is True
    assert content.startswith(newest["content"])
    assert len(newest["content"]) < 4000
    assert newest["sources"] == prior["sources"]
    assert newest["source_count"] == 1
    assert [record["supplied_url"] for record in newest["url_evidence"]] == supplied
    assert all(
        record["citation_status"] == "profile_url_cited"
        for record in newest["url_evidence"]
    )
    assert all(record["citation_count"] == 1 for record in newest["url_evidence"])
    assert all(
        record["direct_access"] == "not_verified" for record in newest["url_evidence"]
    )
    assert all(
        record["identity_status"] == "unverified" for record in newest["url_evidence"]
    )
    assert all(
        url == cited
        for record in newest["url_evidence"]
        for url in record["citation_urls"]
    )
    # Reducing a request must not mutate the durable message passed by the caller.
    assert all(
        record["citation_urls"] == [cited]
        for record in prior["proposals"]["url_evidence"]
    )


def test_chat_history_does_not_create_a_different_url_by_truncating_sources():
    cited = "https://example.test/actual"
    entry = ai._bounded_chat_history_entry(
        {
            "role": "assistant",
            "content": "An earlier answer.",
            "sources": [{"url": "https://example.test/" + "x" * 2000}, {"url": cited}],
        },
        maximum_chars=10_000,
    )
    assert entry["sources"] == [{"url": cited, "title": ""}]


def test_combined_case_chat_distinguishes_snapshot_review_and_inference(monkeypatch):
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
                            {
                                "type": "output_text",
                                "text": "The approved hypothesis remains limited by the publication evidence.",
                                "annotations": [],
                            }
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
    result = asyncio.run(
        get_combined_case_chat_response(
            api_key="test-key",
            case_context={
                "scope": "combined_investigation",
                "snapshot_current": True,
                "latest_ai_assessment": {
                    "executive_summary": "A possible link.",
                    "proposals": [
                        {
                            "title": f"Hypothesis {index}",
                            "explanation": "x" * 6_000,
                            "sources": [
                                {
                                    "url": (
                                        "https://example.test/path?"
                                        "evidence=1&view=full"
                                    )
                                }
                            ],
                        }
                        for index in range(100)
                    ],
                },
            },
            conversation=[
                {
                    "role": "user",
                    "author": "analyst",
                    "content": "Why is this not proof of coordination?",
                }
            ],
            user_message="What evidence would change the conclusion?",
            model="gpt-5.6-sol",
            web_search_enabled=False,
        )
    )

    assert "publication evidence" in result["analysis"]
    assert "combined_investigation_json" in captured["payload"]["input"]
    assert "Why is this not proof" in captured["payload"]["input"]
    assert "What evidence would change" in captured["payload"]["input"]
    structured_input = json.loads(captured["payload"]["input"])
    bounded_context_json = structured_input["combined_investigation_json"]
    bounded_context = json.loads(bounded_context_json)
    assert len(bounded_context_json) <= 90_000
    assert bounded_context["context_truncated"] is True
    assert bounded_context["latest_ai_assessment"]["executive_summary"] == (
        "A possible link."
    )
    assert (
        bounded_context["latest_ai_assessment"]["proposals"][0]["sources"][0]["url"]
        == "https://example.test/path?evidence=1&view=full"
    )
    normalized_instructions = " ".join(captured["payload"]["instructions"].split())
    assert "approved AI relationship is still an analyst-approved hypothesis" in (
        normalized_instructions
    )
    assert "private or residential details" in normalized_instructions
    assert "tools" not in captured["payload"]


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
    assert {
        "company_profile",
        "business_address",
        "headquarters",
        "public_contact",
    }.issubset(
        item_schema["properties"]["observation_type"]["enum"]
    )
    assert {"professional_profile", "map_listing"}.issubset(
        item_schema["properties"]["source_role"]["enum"]
    )
    assert "must not be described" in " ".join(
        captured["payload"]["instructions"].split()
    )
    assert "associated with a named employee or officer" in " ".join(
        captured["payload"]["instructions"].split()
    )
    structured_input = json.loads(captured["payload"]["input"])
    assert structured_input["citation_catalogue"][0]["source_scope"] == (
        "organization"
    )


def test_combined_insight_extraction_is_strict_and_cannot_browse(monkeypatch):
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
                            {
                                "type": "output_text",
                                "text": json.dumps(
                                    {
                                        "executive_summary": "No link established.",
                                        "key_findings": [],
                                        "contradictions": [],
                                        "information_gaps": ["Need ownership records."],
                                        "next_steps": ["Check the public registry."],
                                        "proposals": [],
                                    }
                                ),
                            }
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
    output = asyncio.run(
        get_combined_investigation_insights(
            api_key="test-key",
            case_context={
                "purpose": "Find a publication connection.",
                "snapshot_sha256": "a" * 64,
                "source_cases": [],
                "entities": [],
                "approved_claims": [],
                "approved_organizations": [],
            },
            research_answer="No cited relationship was established.",
            sources=[
                {
                    "title": "Public registry",
                    "url": "https://registry.example/entity",
                }
            ],
            model="gpt-5.6-terra",
        )
    )

    assert output["executive_summary"] == "No link established."
    assert captured["url"] == "https://api.openai.com/v1/responses"
    assert "tools" not in captured["payload"]
    response_format = captured["payload"]["text"]["format"]
    assert response_format["strict"] is True
    assert response_format["name"] == "openledger_combined_investigation_insights"
    structured_input = json.loads(captured["payload"]["input"])
    assert structured_input["web_citation_catalogue"] == [
        {
            "reference_id": "web:1",
            "title": "Public registry",
            "url": "https://registry.example/entity",
        }
    ]
    normalized_instructions = " ".join(
        captured["payload"]["instructions"].split()
    )
    assert "two different source cases" in normalized_instructions
    assert "pending analyst hypotheses" in normalized_instructions
    assert "never transform them into organization facts" in normalized_instructions
