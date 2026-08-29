"""Maigret AI Analysis Module

Provides AI-powered analysis of search results using OpenAI-compatible APIs.
"""

import asyncio
import json
import os
import re
import sys
import threading
from urllib.parse import urlsplit

import aiohttp

AI_EVIDENCE_FIELDS = (
    "summary",
    "full_name",
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
                },
                "required": [
                    "username",
                    "field_name",
                    "value",
                    "confidence",
                    "source_url",
                    "source_title",
                    "reason",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["proposals"],
    "additionalProperties": False,
}


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
        body = await resp.text()
        raise RuntimeError(f"OpenAI API error (HTTP {resp.status}): {body[:500]}")


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
    api_base_url: str = "https://api.openai.com/v1",
) -> str:
    """Send the markdown report to an OpenAI-compatible API and return the analysis.

    Uses streaming to display tokens as they arrive.
    Raises on HTTP errors with descriptive messages.
    """
    system_prompt = load_ai_prompt()

    url = f"{api_base_url.rstrip('/')}/chat/completions"
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
    api_base_url: str = "https://api.openai.com/v1",
    timeout_seconds: int = 180,
) -> str:
    """Return an AI analysis without writing model output to server logs.

    This is the web-safe counterpart to get_ai_analysis. The CLI function
    intentionally streams tokens to stdout; a web request must not do that
    because investigation results may contain sensitive personal data.
    """
    system_prompt = load_ai_prompt()
    url = f"{api_base_url.rstrip('/')}/chat/completions"
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


def _parse_responses_analysis(response_data):
    """Extract assistant text and deduplicated web citations from Responses."""
    text_parts = []
    sources = []
    seen_urls = set()
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
    return {"analysis": analysis, "sources": sources}


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
    api_base_url: str = "https://api.openai.com/v1",
    timeout_seconds: int = 240,
    web_search_enabled: bool = True,
):
    """Analyze extracted evidence, optionally enriching it with cited web search.

    The Responses API is used so web-derived claims retain source annotations.
    No investigation content or model output is written to server logs here.
    """
    url = f"{api_base_url.rstrip('/')}/responses"
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
    return _parse_responses_analysis(response_data)


async def get_ai_evidence_proposals(
    api_key: str,
    investigation_evidence: str,
    analysis: str,
    sources,
    model: str = "gpt-5.4",
    api_base_url: str = "https://api.openai.com/v1",
    timeout_seconds: int = 180,
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

    url = f"{api_base_url.rstrip('/')}/responses"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    instructions = """You extract reviewable public-biographical evidence proposals for OpenLedger.

Return only facts explicitly supported by the supplied assessment and citation catalogue. Every
proposal must use a source_url exactly as written in that catalogue and must identify exactly one
investigated username. Omit uncertain values instead of guessing. Do not infer or propose private
addresses, email addresses, phone numbers, finances, vehicles, criminal records, sensitive traits,
or interpersonal relationships. A summary must be a concise public-professional description, not a
speculative biography. Confidence measures source support, never identity certainty alone. Keep it
at or below 85. These are analyst-review proposals and must never be described as verified facts."""
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
    api_base_url: str = "https://api.openai.com/v1",
    timeout_seconds: int = 20,
) -> str:
    """Verify a server-side OpenAI key and model without generating content."""
    url = f"{api_base_url.rstrip('/')}/models/{model}"
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
