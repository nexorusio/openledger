"""Bounded provider foundation for public-profile web search."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import json
import logging
import os
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import aiohttp

from maigret.web.profile_search_contract import (
    MAX_PROFILE_SEARCH_RESULTS,
    ProfileSearchContractError,
    ProfileSearchError,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)

PROFILE_SEARCH_DISABLED_PROVIDER = "disabled"
PROFILE_SEARCH_SEARXNG_PROVIDER = "searxng"
PROFILE_SEARCH_PROVIDERS = frozenset(
    {
        PROFILE_SEARCH_DISABLED_PROVIDER,
        PROFILE_SEARCH_SEARXNG_PROVIDER,
        "brave",
    }
)
BRAVE_SEARCH_API_URL = "https://api.search.brave.com/res/v1/web/search"
SEARXNG_SEARCH_URL = "http://searxng:8080/search"
DEFAULT_PROFILE_SEARCH_KEY_FILE = "/app/runtime/secrets/brave_search_api_key"
DEFAULT_PROFILE_SEARCH_TIMEOUT_SECONDS = 10
DEFAULT_PROFILE_SEARCH_MAX_RESULTS = 5
MAX_PROFILE_SEARCH_RESPONSE_BYTES = 1_000_000
MAX_PROFILE_SEARCH_KEY_BYTES = 1_024

logger = logging.getLogger("openledger.profile_search")


class ProfileSearchConfigurationError(RuntimeError):
    """Raised when an administrator has not safely configured search."""


class _ProviderFailure(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        http_status: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.public_message = message
        self.retryable = retryable
        self.http_status = http_status


def _integer_setting(
    value: Any,
    field_name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ProfileSearchConfigurationError(
            f"{field_name} must be an integer"
        ) from exc
    if not minimum <= parsed <= maximum:
        raise ProfileSearchConfigurationError(
            f"{field_name} must be between {minimum} and {maximum}"
        )
    return parsed


@dataclass(frozen=True)
class ProfileSearchConfig:
    """Server-owned search settings; credentials never enter this object."""

    provider: str
    api_key_file: str
    timeout_seconds: int
    max_results: int

    @property
    def enabled(self) -> bool:
        return self.provider != PROFILE_SEARCH_DISABLED_PROVIDER


def load_profile_search_config(
    environ: Optional[Mapping[str, str]] = None,
) -> ProfileSearchConfig:
    """Load fail-closed settings without opening the credential file."""
    environment = os.environ if environ is None else environ
    provider = (
        str(
            environment.get(
                "OPENLEDGER_PROFILE_SEARCH_PROVIDER",
                PROFILE_SEARCH_DISABLED_PROVIDER,
            )
        )
        .strip()
        .casefold()
    )
    if provider not in PROFILE_SEARCH_PROVIDERS:
        raise ProfileSearchConfigurationError(
            "OPENLEDGER_PROFILE_SEARCH_PROVIDER is unsupported"
        )
    key_file = str(
        environment.get(
            "OPENLEDGER_PROFILE_SEARCH_API_KEY_FILE",
            DEFAULT_PROFILE_SEARCH_KEY_FILE,
        )
    ).strip()
    if not key_file or "\x00" in key_file or len(key_file) > 1_000:
        raise ProfileSearchConfigurationError(
            "OPENLEDGER_PROFILE_SEARCH_API_KEY_FILE is invalid"
        )
    return ProfileSearchConfig(
        provider=provider,
        api_key_file=key_file,
        timeout_seconds=_integer_setting(
            environment.get(
                "OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS",
                DEFAULT_PROFILE_SEARCH_TIMEOUT_SECONDS,
            ),
            "OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS",
            minimum=1,
            maximum=30,
        ),
        max_results=_integer_setting(
            environment.get(
                "OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS",
                DEFAULT_PROFILE_SEARCH_MAX_RESULTS,
            ),
            "OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS",
            minimum=1,
            maximum=MAX_PROFILE_SEARCH_RESULTS,
        ),
    )


def read_profile_search_api_key(config: ProfileSearchConfig) -> str:
    """Read an owner-only key at request time, outside the config object."""
    if not config.enabled:
        raise ProfileSearchConfigurationError("Profile search is disabled")
    if config.provider != "brave":
        raise ProfileSearchConfigurationError(
            "Configured profile-search provider does not use an API key"
        )
    path = Path(config.api_key_file)
    try:
        file_stat = path.stat()
        if not stat.S_ISREG(file_stat.st_mode):
            raise ProfileSearchConfigurationError(
                "Profile-search API key path must be a regular file"
            )
        if stat.S_IMODE(file_stat.st_mode) & 0o077:
            raise ProfileSearchConfigurationError(
                "Profile-search API key file must use owner-only permissions"
            )
        payload = path.read_bytes()
    except ProfileSearchConfigurationError:
        raise
    except OSError as exc:
        raise ProfileSearchConfigurationError(
            "Profile-search API key file is unavailable"
        ) from exc
    if len(payload) > MAX_PROFILE_SEARCH_KEY_BYTES:
        raise ProfileSearchConfigurationError(
            "Profile-search API key file is oversized"
        )
    try:
        api_key = payload.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ProfileSearchConfigurationError(
            "Profile-search API key file is invalid"
        ) from exc
    if not 8 <= len(api_key) <= 512 or any(
        ord(character) < 33 or ord(character) > 126 for character in api_key
    ):
        raise ProfileSearchConfigurationError(
            "Profile-search API key is invalid"
        )
    return api_key


@dataclass(frozen=True)
class ProfileSearchRun:
    """One provider execution with evidence or a bounded public error."""

    query: ProfileSearchQuery
    provenance: Optional[ProfileSearchProvenance]
    evidence: Tuple[ProfileSearchEvidence, ...]
    error: Optional[ProfileSearchError] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query.as_dict(),
            "provenance": (
                self.provenance.as_dict() if self.provenance else None
            ),
            "evidence": [item.as_dict() for item in self.evidence],
            "error": self.error.as_dict() if self.error else None,
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _header_value(headers: Any, name: str, *, max_chars: int = 200) -> str:
    value = str(getattr(headers, "get", lambda *_: "")(name, "") or "").strip()
    if len(value) > max_chars or any(
        ord(character) < 32 for character in value
    ):
        return ""
    return value


async def _bounded_response_body(response: Any) -> bytes:
    content_length = _header_value(
        response.headers, "Content-Length", max_chars=20
    )
    if content_length:
        try:
            if int(content_length) > MAX_PROFILE_SEARCH_RESPONSE_BYTES:
                raise _ProviderFailure(
                    "oversized_response",
                    "Search provider returned an oversized response.",
                    retryable=False,
                    http_status=int(response.status),
                )
        except ValueError:
            pass
    body = await response.content.read(MAX_PROFILE_SEARCH_RESPONSE_BYTES + 1)
    if len(body) > MAX_PROFILE_SEARCH_RESPONSE_BYTES:
        raise _ProviderFailure(
            "oversized_response",
            "Search provider returned an oversized response.",
            retryable=False,
            http_status=int(response.status),
        )
    return body


def _brave_evidence(
    payload: Any, *, limit: int
) -> Tuple[ProfileSearchEvidence, ...]:
    if not isinstance(payload, dict):
        raise _ProviderFailure(
            "invalid_response",
            "Search provider returned an invalid response.",
            retryable=False,
        )
    web = payload.get("web")
    raw_results = web.get("results") if isinstance(web, dict) else []
    if not isinstance(raw_results, list):
        raise _ProviderFailure(
            "invalid_response",
            "Search provider returned an invalid response.",
            retryable=False,
        )
    evidence = []
    seen_urls = set()
    for rank, raw_result in enumerate(raw_results[:limit], start=1):
        if not isinstance(raw_result, dict):
            continue
        url = str(raw_result.get("url") or "").strip()
        if url.casefold() in seen_urls:
            continue
        try:
            item = ProfileSearchEvidence(
                result_rank=rank,
                source_url=url,
                title=str(raw_result.get("title") or ""),
                snippet=str(raw_result.get("description") or ""),
            )
        except ProfileSearchContractError:
            continue
        seen_urls.add(url.casefold())
        evidence.append(item)
    return tuple(evidence)


def _searxng_evidence(payload: Any, *, limit: int) -> Tuple[ProfileSearchEvidence, ...]:
    if not isinstance(payload, dict):
        raise _ProviderFailure(
            "invalid_response",
            "Search provider returned an invalid response.",
            retryable=False,
        )
    raw_results = payload.get("results", [])
    if not isinstance(raw_results, list):
        raise _ProviderFailure(
            "invalid_response",
            "Search provider returned an invalid response.",
            retryable=False,
        )
    evidence = []
    seen_urls = set()
    for rank, raw_result in enumerate(raw_results[:100], start=1):
        if len(evidence) >= limit:
            break
        if not isinstance(raw_result, dict):
            continue
        url = str(raw_result.get("url") or "").strip()
        if url.casefold() in seen_urls:
            continue
        try:
            item = ProfileSearchEvidence(
                result_rank=rank,
                source_url=url,
                title=str(raw_result.get("title") or ""),
                snippet=str(raw_result.get("content") or ""),
            )
        except ProfileSearchContractError:
            continue
        seen_urls.add(url.casefold())
        evidence.append(item)
    return tuple(evidence)


class ProfileSearchClient:
    """Search without logging queries, results, or secrets."""

    def __init__(
        self,
        config: ProfileSearchConfig,
        *,
        session_factory: Optional[Callable[..., Any]] = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.config = config
        self._session_factory = session_factory or aiohttp.ClientSession
        self._clock = clock

    async def search(self, query: ProfileSearchQuery) -> ProfileSearchRun:
        if not self.config.enabled:
            raise ProfileSearchConfigurationError("Profile search is disabled")
        try:
            if self.config.provider == "brave":
                return await self._search_brave(query)
            if self.config.provider == PROFILE_SEARCH_SEARXNG_PROVIDER:
                return await self._search_searxng(query)
            raise ProfileSearchConfigurationError(
                "Configured profile-search provider is unavailable"
            )
        except _ProviderFailure as exc:
            occurred_at = self._clock()
            logger.warning(
                "Profile search failed provider=%s query_id=%s code=%s "
                "error_type=%s",
                self.config.provider,
                query.query_id,
                exc.code,
                type(exc).__name__,
            )
            return ProfileSearchRun(
                query=query,
                provenance=None,
                evidence=(),
                error=ProfileSearchError(
                    query_id=query.query_id,
                    provider=self.config.provider,
                    code=exc.code,
                    message=exc.public_message,
                    retryable=exc.retryable,
                    occurred_at=occurred_at,
                    http_status=exc.http_status,
                ),
            )
        except (aiohttp.ClientError, TimeoutError) as exc:
            occurred_at = self._clock()
            logger.warning(
                "Profile search failed provider=%s query_id=%s "
                "code=request_failed error_type=%s",
                self.config.provider,
                query.query_id,
                type(exc).__name__,
            )
            return ProfileSearchRun(
                query=query,
                provenance=None,
                evidence=(),
                error=ProfileSearchError(
                    query_id=query.query_id,
                    provider=self.config.provider,
                    code="request_failed",
                    message="Search provider request failed.",
                    retryable=True,
                    occurred_at=occurred_at,
                ),
            )

    async def _search_brave(
        self, query: ProfileSearchQuery
    ) -> ProfileSearchRun:
        api_key = read_profile_search_api_key(self.config)
        result_limit = min(query.max_results, self.config.max_results)
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "User-Agent": "OpenLedger-Profile-Discovery/1.0",
            "X-Subscription-Token": api_key,
        }
        params = {
            "q": query.query_text,
            "count": result_limit,
            "safesearch": "strict",
            "spellcheck": "0",
            "text_decorations": "false",
        }
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        async with self._session_factory(
            timeout=timeout, headers=headers
        ) as session:
            async with session.get(
                BRAVE_SEARCH_API_URL,
                params=params,
                allow_redirects=False,
            ) as response:
                status = int(response.status)
                if status in {401, 403}:
                    raise _ProviderFailure(
                        "credential_rejected",
                        "Search provider rejected its server credential.",
                        retryable=False,
                        http_status=status,
                    )
                if status == 429:
                    raise _ProviderFailure(
                        "rate_limited",
                        "Search provider rate limit was reached.",
                        retryable=True,
                        http_status=status,
                    )
                if status >= 500:
                    raise _ProviderFailure(
                        "provider_unavailable",
                        "Search provider is temporarily unavailable.",
                        retryable=True,
                        http_status=status,
                    )
                if status != 200:
                    raise _ProviderFailure(
                        "provider_error",
                        "Search provider returned an unexpected response.",
                        retryable=False,
                        http_status=status,
                    )
                body = await _bounded_response_body(response)
                request_id = _header_value(response.headers, "X-Request-Id")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _ProviderFailure(
                "invalid_response",
                "Search provider returned invalid JSON.",
                retryable=False,
                http_status=200,
            ) from exc
        retrieved_at = self._clock()
        provenance = ProfileSearchProvenance.for_query(
            query,
            provider=self.config.provider,
            provider_request_id=request_id,
            retrieved_at=retrieved_at,
        )
        return ProfileSearchRun(
            query=query,
            provenance=provenance,
            evidence=_brave_evidence(payload, limit=result_limit),
        )

    async def _search_searxng(self, query: ProfileSearchQuery) -> ProfileSearchRun:
        result_limit = min(query.max_results, self.config.max_results)
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "User-Agent": "OpenLedger-Profile-Discovery/1.0",
        }
        params = {
            "q": query.query_text,
            "format": "json",
            "safesearch": "2",
            "language": "all",
            "categories": "general",
        }
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        async with self._session_factory(timeout=timeout, headers=headers) as session:
            async with session.get(
                SEARXNG_SEARCH_URL,
                params=params,
                allow_redirects=False,
            ) as response:
                status = int(response.status)
                if status == 429:
                    raise _ProviderFailure(
                        "rate_limited",
                        "Search provider rate limit was reached.",
                        retryable=True,
                        http_status=status,
                    )
                if status >= 500:
                    raise _ProviderFailure(
                        "provider_unavailable",
                        "Search provider is temporarily unavailable.",
                        retryable=True,
                        http_status=status,
                    )
                if status != 200:
                    raise _ProviderFailure(
                        "provider_error",
                        "Search provider returned an unexpected response.",
                        retryable=False,
                        http_status=status,
                    )
                body = await _bounded_response_body(response)
                request_id = _header_value(response.headers, "X-Request-Id")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _ProviderFailure(
                "invalid_response",
                "Search provider returned invalid JSON.",
                retryable=False,
                http_status=200,
            ) from exc
        retrieved_at = self._clock()
        provenance = ProfileSearchProvenance.for_query(
            query,
            provider=self.config.provider,
            provider_request_id=request_id,
            retrieved_at=retrieved_at,
        )
        return ProfileSearchRun(
            query=query,
            provenance=provenance,
            evidence=_searxng_evidence(payload, limit=result_limit),
        )
