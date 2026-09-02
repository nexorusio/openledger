"""Maigret AI Analysis Module

Provides AI-powered analysis of search results using OpenAI-compatible APIs.
"""

import asyncio
import ipaddress
import json
import os
import re
import sys
import threading
from urllib.parse import urlsplit, urlunsplit

import aiohttp

DEFAULT_AI_API_BASE_URL = "https://api.openai.com/v1"
_LOOPBACK_AI_HOSTS = {"localhost", "127.0.0.1", "::1"}

AI_EVIDENCE_FIELDS = (
    "summary",
    "full_name",
    "email",
    "phone",
    "address",
    "current_location",
    "occupation",
    "company",
    "social_account",
    "website",
    "photograph",
)

AI_EVIDENCE_SCHEMA = {
    "type": "object",
    "properties": {
        "proposals": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "username": {"type": "string"},
                    "field_name": {
                        "type": "string",
                        "enum": list(AI_EVIDENCE_FIELDS),
                    },
                    "value": {"type": "string"},
                    "confidence": {"type": "integer"},
                    "source_url": {"type": "string"},
                    "source_title": {"type": "string"},
                    "reason": {"type": "string"},
                    "latitude": {"type": ["number", "null"]},
                    "longitude": {"type": ["number", "null"]},
                    "coordinate_precision": {
                        "type": ["string", "null"],
                        "enum": ["city", "region", None],
                    },
                },
                "required": [
                    "username",
                    "field_name",
                    "value",
                    "confidence",
                    "source_url",
                    "source_title",
                    "reason",
                    "latitude",
                    "longitude",
                    "coordinate_precision",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["proposals"],
    "additionalProperties": False,
}

CASE_CHAT_PROPOSAL_SCHEMA = {
    "type": "object",
    "properties": {
        "proposals": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "field_name": {
                        "type": "string",
                        "enum": list(AI_EVIDENCE_FIELDS),
                    },
                    "value": {"type": "string"},
                    "confidence": {"type": "integer"},
                    "evidence_basis": {
                        "type": "string",
                        "enum": ["user_statement", "public_web"],
                    },
                    "source_url": {"type": ["string", "null"]},
                    "source_title": {"type": ["string", "null"]},
                    "reason": {"type": "string"},
                    "latitude": {"type": ["number", "null"]},
                    "longitude": {"type": ["number", "null"]},
                    "coordinate_precision": {
                        "type": ["string", "null"],
                        "enum": ["city", "region", None],
                    },
                },
                "required": [
                    "field_name",
                    "value",
                    "confidence",
                    "evidence_basis",
                    "source_url",
                    "source_title",
                    "reason",
                    "latitude",
                    "longitude",
                    "coordinate_precision",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["proposals"],
    "additionalProperties": False,
}

ORGANIZATION_CONTEXT_PROPOSAL_SCHEMA = {
    "type": "object",
    "properties": {
        "proposals": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "observation_type": {
                        "type": "string",
                        "enum": [
                            "organization_identity",
                            "company_profile",
                            "business_address",
                            "headquarters",
                            "business_activity",
                            "public_contact",
                        ],
                    },
                    "value": {"type": "string"},
                    "source_url": {"type": "string"},
                    "source_title": {"type": "string"},
                    "source_role": {
                        "type": "string",
                        "enum": [
                            "official_organization",
                            "legal_registry",
                            "professional_profile",
                            "map_listing",
                            "public_directory",
                            "news_or_institutional",
                            "other_public_source",
                        ],
                    },
                    "identity_match_basis": {
                        "type": "string",
                        "enum": [
                            "exact_name_and_official_website",
                            "exact_name_and_location",
                            "exact_name_only",
                            "ambiguous",
                        ],
                    },
                    "reason": {"type": "string"},
                    "confidence": {"type": "integer"},
                    "latitude": {"type": ["number", "null"]},
                    "longitude": {"type": ["number", "null"]},
                },
                "required": [
                    "observation_type",
                    "value",
                    "source_url",
                    "source_title",
                    "source_role",
                    "identity_match_basis",
                    "reason",
                    "confidence",
                    "latitude",
                    "longitude",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["proposals"],
    "additionalProperties": False,
}


class AIEnrichmentContractError(RuntimeError):
    """The enrichment response did not contain the required cited research."""


def validate_ai_api_base_url(
    api_base_url: str,
    *,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
) -> str:
    """Return a canonical, explicitly authorized OpenAI-compatible API base.

    The web application defaults to OpenAI's fixed HTTPS origin. Alternative
    providers require an explicit server-side opt-in; private-network providers
    require a second opt-in. This prevents an accidentally attacker-controlled
    value from turning the API key and investigation evidence into an SSRF or
    secret-exfiltration primitive while retaining deliberate local deployments.
    """
    if not isinstance(api_base_url, str) or not api_base_url.strip():
        raise ValueError("AI API base URL is required")
    candidate = api_base_url.strip().rstrip("/")
    parsed = urlsplit(candidate)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("AI API base URL must be an HTTP(S) origin without credentials")

    hostname = parsed.hostname.casefold().rstrip(".")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("AI API base URL contains an invalid port") from exc

    canonical_netloc = hostname
    if ":" in hostname and not hostname.startswith("["):
        canonical_netloc = f"[{hostname}]"
    if port is not None:
        canonical_netloc = f"{canonical_netloc}:{port}"
    canonical = urlunsplit(
        (parsed.scheme.casefold(), canonical_netloc, parsed.path.rstrip("/"), "", "")
    )

    if canonical == DEFAULT_AI_API_BASE_URL:
        return canonical
    if not allow_custom_endpoint:
        raise ValueError(
            "Custom AI API endpoints require explicit server authorization"
        )

    is_loopback = hostname in _LOOPBACK_AI_HOSTS
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address is not None:
        is_loopback = address.is_loopback
        if not address.is_global and not is_loopback and not allow_private_endpoint:
            raise ValueError(
                "Private or special-purpose AI API addresses require explicit authorization"
            )

    if parsed.scheme != "https" and not is_loopback:
        raise ValueError("Custom AI API endpoints must use HTTPS")
    return canonical


def _ai_api_url(
    api_base_url: str,
    resource: str,
    *,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
) -> str:
    base = validate_ai_api_base_url(
        api_base_url,
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    return f"{base}/{resource.lstrip('/')}"


def _openledger_label(text: str) -> str:
    """Prevent the internal engine name from leaking into branded output."""
    return re.sub(r"\bmaigret\b", "OpenLedger", text, flags=re.IGNORECASE)


def load_ai_prompt() -> str:
    """Load the AI system prompt from the resources directory."""
    maigret_path = os.path.dirname(os.path.realpath(__file__))
    prompt_path = os.path.join(maigret_path, "resources", "ai_prompt.txt")
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()


def resolve_api_key(settings) -> str | None:
    """Resolve OpenAI API key from settings or environment variable.

    Priority: settings.openai_api_key > OPENAI_API_KEY env var.
    """
    key = getattr(settings, "openai_api_key", None)
    if key:
        return key
    return os.environ.get("OPENAI_API_KEY")


class _Spinner:
    """Simple animated spinner for terminal output."""

    FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def __init__(self, text=""):
        self.text = text
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()

    def _spin(self):
        i = 0
        while not self._stop.is_set():
            frame = self.FRAMES[i % len(self.FRAMES)]
            sys.stderr.write(f"\r{frame} {self.text}")
            sys.stderr.flush()
            i += 1
            self._stop.wait(0.08)

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join()
        sys.stderr.write("\r\033[2K")
        sys.stderr.flush()


async def print_streaming(text: str, delay: float = 0.04):
    """Print text word by word with a delay, simulating streaming LLM output."""
    words = text.split(" ")
    for i, word in enumerate(words):
        if i > 0:
            sys.stdout.write(" ")
        sys.stdout.write(word)
        sys.stdout.flush()
        await asyncio.sleep(delay)
    sys.stdout.write("\n")
    sys.stdout.flush()


async def _check_response(resp):
    """Raise descriptive errors for non-success HTTP responses."""
    if resp.status == 401:
        raise RuntimeError("Invalid OpenAI API key (HTTP 401)")
    if resp.status == 429:
        raise RuntimeError("OpenAI API rate limit exceeded (HTTP 429)")
    if resp.status != 200:
        # Consume the response for connection reuse, but do not propagate a
        # remote error body into browser messages, persisted jobs, or logs.
        await resp.read()
        raise RuntimeError(f"OpenAI API error (HTTP {resp.status})")


async def _stream_response(resp, spinner, first_token):
    """Stream tokens from resp, display them, and return (first_token, full_analysis)."""
    full_response = []
    async for line in resp.content:
        decoded = line.decode("utf-8").strip()
        if not decoded or not decoded.startswith("data: "):
            continue
        data_str = decoded[len("data: ") :]
        if data_str == "[DONE]":
            break
        try:
            chunk = json.loads(data_str)
        except json.JSONDecodeError:
            continue
        delta = chunk.get("choices", [{}])[0].get("delta", {})
        content = delta.get("content", "")
        if not content:
            continue
        if first_token:
            spinner.stop()
            print()
            first_token = False
        sys.stdout.write(content)
        sys.stdout.flush()
        full_response.append(content)
    return first_token, "".join(full_response)


async def get_ai_analysis(
    api_key: str,
    markdown_report: str,
    model: str = "gpt-4o",
    api_base_url: str = DEFAULT_AI_API_BASE_URL,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
) -> str:
    """Send the markdown report to an OpenAI-compatible API and return the analysis.

    Uses streaming to display tokens as they arrive.
    Raises on HTTP errors with descriptive messages.
    """
    system_prompt = load_ai_prompt()

    url = _ai_api_url(
        api_base_url,
        "chat/completions",
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload: dict[str, object] = {
        "model": model,
        "stream": True,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": markdown_report},
        ],
    }

    spinner = _Spinner("Analysing the data with AI...")
    spinner.start()
    first_token = True

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                await _check_response(resp)
                first_token, analysis = await _stream_response(
                    resp, spinner, first_token
                )
    except Exception:
        spinner.stop()
        raise

    if first_token:
        # No tokens received — stop spinner anyway
        spinner.stop()

    print()
    return analysis


async def get_ai_analysis_text(
    api_key: str,
    markdown_report: str,
    model: str = "gpt-4o",
    api_base_url: str = DEFAULT_AI_API_BASE_URL,
    timeout_seconds: int = 180,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
) -> str:
    """Return an AI analysis without writing model output to server logs.

    This is the web-safe counterpart to get_ai_analysis. The CLI function
    intentionally streams tokens to stdout; a web request must not do that
    because investigation results may contain sensitive personal data.
    """
    system_prompt = load_ai_prompt()
    url = _ai_api_url(
        api_base_url,
        "chat/completions",
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": markdown_report},
        ],
    }
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            await _check_response(resp)
            try:
                response_data = await resp.json()
            except (aiohttp.ContentTypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "OpenAI API returned an invalid JSON response"
                ) from exc

    try:
        analysis = response_data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("OpenAI API response did not contain an analysis") from exc

    if not isinstance(analysis, str) or not analysis.strip():
        raise RuntimeError("OpenAI API returned an empty analysis")
    return analysis.strip()


def _parse_responses_analysis(response_data, *, require_web_search=False):
    """Extract assistant text and citations, enforcing requested web grounding."""
    text_parts = []
    sources = []
    seen_urls = set()
    web_search_completed = any(
        isinstance(item, dict)
        and item.get("type") == "web_search_call"
        and item.get("status") == "completed"
        for item in response_data.get("output", [])
    )
    for item in response_data.get("output", []):
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict) or content.get("type") != "output_text":
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                text_parts.append(_openledger_label(text.strip()))
            for annotation in content.get("annotations", []):
                if (
                    not isinstance(annotation, dict)
                    or annotation.get("type") != "url_citation"
                ):
                    continue
                url = annotation.get("url", "")
                parsed = urlsplit(url) if isinstance(url, str) else None
                if (
                    not parsed
                    or parsed.scheme not in {"http", "https"}
                    or not parsed.netloc
                ):
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                title = annotation.get("title")
                sources.append(
                    {
                        "title": (
                            _openledger_label(title.strip())
                            if isinstance(title, str) and title.strip()
                            else parsed.netloc
                        ),
                        "url": url,
                    }
                )
    analysis = "\n\n".join(text_parts).strip()
    if not analysis:
        raise RuntimeError("OpenAI API response did not contain an analysis")
    if require_web_search and not web_search_completed:
        raise AIEnrichmentContractError(
            "OpenAI API did not perform the required cited public-web search"
        )
    if require_web_search and not sources:
        raise AIEnrichmentContractError(
            "OpenAI API completed web search but returned no cited public sources"
        )
    return {
        "analysis": analysis,
        "sources": sources,
        "web_search_completed": web_search_completed,
    }


def _parse_structured_response(response_data):
    """Parse one strict Responses API JSON output without trusting its shape."""
    text_parts = []
    for item in response_data.get("output", []):
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            if content.get("type") == "refusal":
                raise RuntimeError("OpenAI declined to create evidence proposals")
            if content.get("type") != "output_text":
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                text_parts.append(text.strip())
    if not text_parts:
        raise RuntimeError("OpenAI API response did not contain evidence proposals")
    try:
        payload = json.loads("\n".join(text_parts))
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "OpenAI API returned invalid evidence proposal JSON"
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("proposals"), list):
        raise RuntimeError("OpenAI API returned an invalid evidence proposal payload")
    return payload["proposals"]


async def get_enriched_ai_analysis(
    api_key: str,
    investigation_evidence: str,
    model: str = "gpt-5.4",
    api_base_url: str = DEFAULT_AI_API_BASE_URL,
    timeout_seconds: int = 240,
    web_search_enabled: bool = True,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
):
    """Analyze extracted evidence, optionally enriching it with cited web search.

    The Responses API is used so web-derived claims retain source annotations.
    No investigation content or model output is written to server logs here.
    """
    url = _ai_api_url(
        api_base_url,
        "responses",
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload: dict[str, object] = {
        "model": model,
        "instructions": load_ai_prompt(),
        "input": investigation_evidence,
    }
    if web_search_enabled:
        payload["tools"] = [{"type": "web_search"}]
        # Merely listing the tool leaves selection on auto. OpenLedger promises
        # cited enrichment here, so an uncited model-only answer is not valid.
        payload["tool_choice"] = "required"

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            await _check_response(resp)
            try:
                response_data = await resp.json()
            except (aiohttp.ContentTypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "OpenAI API returned an invalid JSON response"
                ) from exc
    return _parse_responses_analysis(
        response_data,
        require_web_search=web_search_enabled,
    )


async def get_case_chat_response(
    api_key: str,
    *,
    case_context,
    conversation,
    user_message: str,
    model: str = "gpt-5.4",
    api_base_url: str = DEFAULT_AI_API_BASE_URL,
    timeout_seconds: int = 240,
    web_search_enabled: bool = False,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
):
    """Answer one case-scoped analyst request with optional cited web research."""
    url = _ai_api_url(
        api_base_url,
        "responses",
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    instructions = """You are the OpenLedger case assistant. Answer the analyst's
current request using the supplied case record and bounded conversation history.
Treat approved claims as reviewed case facts; label pending and uncertain claims
explicitly; treat rejected claims only as audit context. User statements are
unverified until reviewed. Separate stored case evidence, cited public-web
information, and analytical inference. Never present an inference as a stored
fact. When web search is available, cite the sources returned by the tool and
prefer official, institutional, and reputable public sources. Do not infer
sensitive traits, private addresses, criminality, or interpersonal relationships
from weak signals. Do not claim to have modified a Persona; a separate
server-controlled review workflow handles proposed updates. Be concise, neutral,
and explicit about uncertainty. Use the product name OpenLedger only."""
    bounded_conversation = []
    conversation_budget = 30_000
    for item in reversed(list(conversation or [])[-30:]):
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().casefold()
        if role not in {"user", "assistant"}:
            continue
        content = str(item.get("content") or "")[: min(4_000, conversation_budget)]
        if not content:
            continue
        bounded_conversation.insert(
            0,
            {
                "role": role,
                "author": str(item.get("author") or "")[:200],
                "content": content,
            },
        )
        conversation_budget -= len(content)
        if conversation_budget <= 0:
            break
    serialized_case_context = json.dumps(
        case_context, ensure_ascii=False, separators=(",", ":")
    )
    if len(serialized_case_context) > 70_000:
        serialized_case_context = (
            serialized_case_context[:70_000]
            + "\n[Case context truncated at the server safety limit.]"
        )
    structured_input = json.dumps(
        {
            "case_record_json": serialized_case_context,
            "conversation_history": bounded_conversation,
            "current_analyst_request": str(user_message)[:12_000],
        },
        ensure_ascii=False,
    )
    payload: dict[str, object] = {
        "model": model,
        "instructions": instructions,
        "input": structured_input,
    }
    if web_search_enabled:
        payload["tools"] = [{"type": "web_search"}]
        payload["tool_choice"] = "required"

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            await _check_response(resp)
            try:
                response_data = await resp.json()
            except (aiohttp.ContentTypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "OpenAI API returned an invalid JSON response"
                ) from exc
    return _parse_responses_analysis(
        response_data,
        require_web_search=web_search_enabled,
    )


async def get_case_chat_claim_proposals(
    api_key: str,
    *,
    target_persona: str,
    user_message: str,
    assistant_answer: str,
    sources,
    model: str = "gpt-5.4",
    api_base_url: str = DEFAULT_AI_API_BASE_URL,
    timeout_seconds: int = 180,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
):
    """Extract narrow, reviewable Persona proposals from one chat turn."""
    source_catalog = [
        {
            "title": str(source.get("title", ""))[:300],
            "url": str(source.get("url", ""))[:2000],
            "source_scope": str(source.get("source_scope", "organization"))[:32],
        }
        for source in list(sources or [])[:100]
        if isinstance(source, dict)
    ]
    url = _ai_api_url(
        api_base_url,
        "responses",
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    instructions = """Extract only reviewable public-biographical proposals for
the named OpenLedger Persona. A user_statement proposal must be explicitly
asserted in the analyst message; never convert the assistant's inference into a
user statement. Cap its confidence at 50, use null for source and coordinate
fields, and omit ambiguity. A public_web proposal must be explicitly supported
by the assistant answer and one exact URL from the citation catalogue; cap
confidence at 85. Extrapolations, predictions, and hypotheses must never become
Persona proposals. Email and phone values must be exact and explicitly supplied
by the analyst or explicitly published by the cited source. Address proposals
are allowed only with public_web evidence and must be explicitly published
institutional or business contact addresses; never propose or infer a private
residence. Use the company field for any explicit
affiliation, including an employer, educational institution, association, or
organization. When a cited source explicitly states a role or occupation at a
named organization, return two separate proposals: occupation for the role and
company for the exact organization name. Do not leave the organization only
embedded inside the occupation value, and do not infer one from a role title.
Never propose finances, vehicles, criminal records, sensitive
traits, or relationships. For a cited
coarse current location only, an approximate city or region map center may be
included. These records always require human review. Return an empty list when
nothing qualifies."""
    structured_input = json.dumps(
        {
            "target_persona": str(target_persona)[:500],
            "analyst_message": str(user_message)[:12_000],
            "assistant_answer": str(assistant_answer)[:30_000],
            "citation_catalogue": source_catalog,
        },
        ensure_ascii=False,
    )
    payload = {
        "model": model,
        "instructions": instructions,
        "input": structured_input,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "openledger_case_chat_persona_proposals",
                "strict": True,
                "schema": CASE_CHAT_PROPOSAL_SCHEMA,
            }
        },
    }
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            await _check_response(resp)
            try:
                response_data = await resp.json()
            except (aiohttp.ContentTypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "OpenAI API returned an invalid JSON response"
                ) from exc
    return _parse_structured_response(response_data)


async def get_organization_context_proposals(
    api_key: str,
    *,
    organization_name: str,
    legal_jurisdiction,
    official_website,
    research_answer: str,
    sources,
    model: str = "gpt-5.4",
    api_base_url: str = DEFAULT_AI_API_BASE_URL,
    timeout_seconds: int = 180,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
):
    """Extract bounded organization observations from one cited research turn."""
    source_catalog = [
        {
            "title": str(source.get("title", ""))[:300],
            "url": str(source.get("url", ""))[:2000],
            "source_scope": str(source.get("source_scope", "organization"))[:32],
        }
        for source in list(sources or [])[:100]
        if isinstance(source, dict)
    ]
    if not source_catalog:
        return []
    url = _ai_api_url(
        api_base_url,
        "responses",
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    instructions = """Extract only reviewable public organization observations
explicitly supported by the research answer and one exact URL from the supplied
citation catalogue. Never invent or repair a URL. Professional-network company
pages and public map/business listings may be retained as cited third-party
observations, but they are not legal-registry evidence and must not be described
as content fetched directly by OpenLedger. A headquarters observation is allowed
only when the cited source explicitly labels the location as headquarters. A map
or directory address without that label is only a business_address. Do not infer
an address from coordinates, infrastructure, a map viewport, or a nearby place.
Omit ambiguous identity matches, private or residential addresses, sensitive
traits, and unsupported conclusions. Retain an exact phone or email explicitly
published by a cited source as public_contact, including when it is associated
with a named employee or officer. State that association in the reason and do
not present the contact as an organization, identity, affiliation, or ownership
fact. Do not infer or reconstruct a contact value.
Treat a citation catalogued with source_scope public_contact only as provenance
for a public_contact observation; it cannot support another observation type.
Use exact_name_only cautiously and keep its confidence at or below 60; every
other confidence must remain at or below 85. Latitude and longitude are allowed
only when the cited source explicitly provides both for a business location.
Return an empty list when no observation qualifies. All output remains pending
human review and must never be presented as a canonical organization fact."""
    structured_input = json.dumps(
        {
            "organization_name": str(organization_name)[:500],
            "legal_jurisdiction": legal_jurisdiction,
            "official_website": official_website,
            "research_answer": str(research_answer)[:30_000],
            "citation_catalogue": source_catalog,
        },
        ensure_ascii=False,
    )
    payload = {
        "model": model,
        "instructions": instructions,
        "input": structured_input,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "openledger_organization_context_proposals",
                "strict": True,
                "schema": ORGANIZATION_CONTEXT_PROPOSAL_SCHEMA,
            }
        },
    }
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            await _check_response(resp)
            try:
                response_data = await resp.json()
            except (aiohttp.ContentTypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "OpenAI API returned an invalid JSON response"
                ) from exc
    return _parse_structured_response(response_data)


async def get_ai_evidence_proposals(
    api_key: str,
    investigation_evidence: str,
    analysis: str,
    sources,
    model: str = "gpt-5.4",
    api_base_url: str = DEFAULT_AI_API_BASE_URL,
    timeout_seconds: int = 180,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
):
    """Convert a cited assessment into schema-constrained evidence proposals.

    This second pass cannot browse. It may reference only the citation catalogue
    returned by the preceding web-enabled assessment; the server validates that
    constraint again before a proposal is persisted.
    """
    source_catalog = [
        {
            "title": str(source.get("title", ""))[:300],
            "url": str(source.get("url", ""))[:2000],
        }
        for source in list(sources)[:100]
        if isinstance(source, dict)
    ]
    if not source_catalog:
        return []

    url = _ai_api_url(
        api_base_url,
        "responses",
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    instructions = """You extract reviewable public-biographical evidence proposals for OpenLedger.

Return only facts explicitly supported by the supplied assessment and citation catalogue. Every
proposal must use a source_url exactly as written in that catalogue and must identify exactly one
investigated username. Omit uncertain values instead of guessing. Do not infer or propose private
or residential addresses. Email and phone values must be exact and explicitly published by the
cited source. Address values must be explicitly published institutional or business contact
addresses. Use the company field for any explicit affiliation, including an employer, educational
institution, association, or organization. When a cited source explicitly states a role or
occupation at a named organization, return separate occupation and company proposals; do not leave
the organization only embedded in the occupation value, and do not infer one from a role title.
Never propose finances, vehicles, criminal records,
sensitive traits, or interpersonal relationships. A summary must be a concise public-biographical description, not a
speculative biography. Confidence measures source support, never identity certainty alone. Keep it
at or below 85. For a coarse current_location only, latitude and longitude may contain an
approximate city or region map center when that place is explicitly supported; set
coordinate_precision accordingly. They must never represent a person's precise position. Use null
for all coordinate fields otherwise. These are analyst-review proposals and must never be described
as verified facts."""
    structured_input = json.dumps(
        {
            "investigation_evidence": investigation_evidence[:100_000],
            "assessment": analysis[:30_000],
            "citation_catalogue": source_catalog,
        },
        ensure_ascii=False,
    )
    payload = {
        "model": model,
        "instructions": instructions,
        "input": structured_input,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "openledger_persona_evidence",
                "strict": True,
                "schema": AI_EVIDENCE_SCHEMA,
            }
        },
    }
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            await _check_response(resp)
            try:
                response_data = await resp.json()
            except (aiohttp.ContentTypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "OpenAI API returned an invalid JSON response"
                ) from exc
    return _parse_structured_response(response_data)


async def validate_openai_connection(
    api_key: str,
    model: str,
    api_base_url: str = DEFAULT_AI_API_BASE_URL,
    timeout_seconds: int = 20,
    allow_custom_endpoint: bool = False,
    allow_private_endpoint: bool = False,
) -> str:
    """Verify a server-side OpenAI key and model without generating content."""
    url = _ai_api_url(
        api_base_url,
        f"models/{model}",
        allow_custom_endpoint=allow_custom_endpoint,
        allow_private_endpoint=allow_private_endpoint,
    )
    headers = {"Authorization": f"Bearer {api_key}"}
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=headers) as resp:
            await _check_response(resp)
            try:
                response_data = await resp.json()
            except (aiohttp.ContentTypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "OpenAI API returned an invalid model response"
                ) from exc

    model_id = response_data.get("id")
    if not isinstance(model_id, str) or not model_id:
        raise RuntimeError("OpenAI API did not confirm the requested model")
    return model_id
