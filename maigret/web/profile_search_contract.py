"""Provider-neutral contracts for bounded public-profile search discovery."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import hashlib
import ipaddress
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

PROFILE_SEARCH_SCHEMA_VERSION = 1
PROFILE_SEARCH_PLATFORMS = frozenset(
    {"facebook", "instagram", "threads", "tiktok", "x"}
)
PROFILE_SEARCH_SEED_KINDS = frozenset(
    {
        "alias",
        "confirmed_username",
        "full_name",
        "profile_url",
        "social_handle",
        "username",
    }
)
MAX_PROFILE_SEARCH_RESULTS = 10

_IDENTIFIER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,99}$")
_ERROR_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class ProfileSearchContractError(ValueError):
    """Raised when provider-neutral search data violates the contract."""


def _bounded_text(
    value: Any,
    field_name: str,
    *,
    max_chars: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise ProfileSearchContractError(f"{field_name} must be text")
    candidate = value.strip()
    if not candidate and not allow_empty:
        raise ProfileSearchContractError(f"{field_name} is required")
    if len(candidate) > max_chars:
        raise ProfileSearchContractError(f"{field_name} is too large")
    if any(
        ord(character) < 32 and character not in "\n\r\t"
        for character in candidate
    ):
        raise ProfileSearchContractError(
            f"{field_name} contains prohibited control characters"
        )
    return candidate


def _identifier(value: Any, field_name: str) -> str:
    candidate = _bounded_text(value, field_name, max_chars=100).casefold()
    if not _IDENTIFIER_PATTERN.fullmatch(candidate):
        raise ProfileSearchContractError(f"Invalid {field_name}")
    return candidate


def _timestamp(value: Any, field_name: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        candidate = _bounded_text(value, field_name, max_chars=40)
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ProfileSearchContractError(f"Invalid {field_name}") from exc
    else:
        raise ProfileSearchContractError(
            f"{field_name} must be an ISO timestamp"
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProfileSearchContractError(
            f"{field_name} must include a timezone"
        )
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _public_https_url(value: Any, field_name: str) -> str:
    candidate = _bounded_text(value, field_name, max_chars=2_000)
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as exc:
        raise ProfileSearchContractError(f"Invalid {field_name}") from exc
    if (
        parsed.scheme.casefold() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
    ):
        raise ProfileSearchContractError(
            f"{field_name} must be a public HTTPS URL"
        )
    hostname = parsed.hostname.casefold().rstrip(".")
    if hostname == "localhost" or hostname.endswith(
        (".localhost", ".local", ".internal", ".lan")
    ):
        raise ProfileSearchContractError(
            f"{field_name} must be a public HTTPS URL"
        )
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address is not None and not address.is_global:
        raise ProfileSearchContractError(
            f"{field_name} must be a public HTTPS URL"
        )
    return candidate


@dataclass(frozen=True)
class ProfileSearchQuery:
    """One bounded, platform-scoped query derived from an approved seed."""

    query_id: str
    platform: str
    query_text: str
    seed_kind: str
    seed_value: str
    seed_score: int = 0
    seed_reason: str = ""
    max_results: int = MAX_PROFILE_SEARCH_RESULTS
    schema_version: int = field(
        default=PROFILE_SEARCH_SCHEMA_VERSION, init=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "query_id", _identifier(self.query_id, "query_id")
        )
        platform = _identifier(self.platform, "platform")
        if platform not in PROFILE_SEARCH_PLATFORMS:
            raise ProfileSearchContractError(
                "Unsupported profile-search platform"
            )
        object.__setattr__(self, "platform", platform)
        object.__setattr__(
            self,
            "query_text",
            _bounded_text(self.query_text, "query_text", max_chars=500),
        )
        seed_kind = _identifier(self.seed_kind, "seed_kind")
        if seed_kind not in PROFILE_SEARCH_SEED_KINDS:
            raise ProfileSearchContractError(
                "Unsupported profile-search seed kind"
            )
        object.__setattr__(self, "seed_kind", seed_kind)
        object.__setattr__(
            self,
            "seed_value",
            _bounded_text(self.seed_value, "seed_value", max_chars=500),
        )
        if (
            isinstance(self.seed_score, bool)
            or not isinstance(self.seed_score, int)
            or not 0 <= self.seed_score <= 100
        ):
            raise ProfileSearchContractError(
                "seed_score must be between 0 and 100"
            )
        object.__setattr__(
            self,
            "seed_reason",
            _bounded_text(
                self.seed_reason,
                "seed_reason",
                max_chars=240,
                allow_empty=True,
            ),
        )
        if (
            isinstance(self.max_results, bool)
            or not isinstance(self.max_results, int)
            or not 1 <= self.max_results <= MAX_PROFILE_SEARCH_RESULTS
        ):
            raise ProfileSearchContractError(
                "max_results must be between 1 and "
                f"{MAX_PROFILE_SEARCH_RESULTS}"
            )

    @property
    def fingerprint(self) -> str:
        material = "\0".join(
            (
                self.platform,
                self.query_text,
                self.seed_kind,
                self.seed_value,
                str(self.seed_score),
                self.seed_reason,
                str(self.max_results),
            )
        )
        return f"sha256:{hashlib.sha256(material.encode('utf-8')).hexdigest()}"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "query_id": self.query_id,
            "platform": self.platform,
            "query_text": self.query_text,
            "seed_kind": self.seed_kind,
            "seed_value": self.seed_value,
            "seed_score": self.seed_score,
            "seed_reason": self.seed_reason,
            "max_results": self.max_results,
            "query_fingerprint": self.fingerprint,
        }


@dataclass(frozen=True)
class ProfileSearchProvenance:
    """Provider lineage for one query execution and its returned records."""

    query_id: str
    query_fingerprint: str
    provider: str
    retrieved_at: str | datetime
    provider_request_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "query_id", _identifier(self.query_id, "query_id")
        )
        fingerprint = _bounded_text(
            self.query_fingerprint, "query_fingerprint", max_chars=71
        ).casefold()
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", fingerprint):
            raise ProfileSearchContractError(
                "query_fingerprint must be sha256:<64 lowercase hex>"
            )
        object.__setattr__(self, "query_fingerprint", fingerprint)
        object.__setattr__(
            self, "provider", _identifier(self.provider, "provider")
        )
        object.__setattr__(
            self, "retrieved_at", _timestamp(self.retrieved_at, "retrieved_at")
        )
        object.__setattr__(
            self,
            "provider_request_id",
            _bounded_text(
                self.provider_request_id,
                "provider_request_id",
                max_chars=200,
                allow_empty=True,
            ),
        )

    @classmethod
    def for_query(
        cls,
        query: ProfileSearchQuery,
        *,
        provider: str,
        retrieved_at: str | datetime,
        provider_request_id: str = "",
    ) -> "ProfileSearchProvenance":
        return cls(
            query_id=query.query_id,
            query_fingerprint=query.fingerprint,
            provider=provider,
            retrieved_at=retrieved_at,
            provider_request_id=provider_request_id,
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "query_id": self.query_id,
            "query_fingerprint": self.query_fingerprint,
            "provider": self.provider,
            "provider_request_id": self.provider_request_id,
            "retrieved_at": self.retrieved_at,
        }


@dataclass(frozen=True)
class ProfileSearchEvidence:
    """A bounded copy of the provider result used to derive a candidate."""

    result_rank: int
    source_url: str
    title: str = ""
    snippet: str = ""

    def __post_init__(self) -> None:
        if (
            isinstance(self.result_rank, bool)
            or not isinstance(self.result_rank, int)
            or not 1 <= self.result_rank <= 100
        ):
            raise ProfileSearchContractError(
                "result_rank must be between 1 and 100"
            )
        object.__setattr__(
            self,
            "source_url",
            _public_https_url(self.source_url, "source_url"),
        )
        object.__setattr__(
            self,
            "title",
            _bounded_text(
                self.title, "title", max_chars=500, allow_empty=True
            ),
        )
        object.__setattr__(
            self,
            "snippet",
            _bounded_text(
                self.snippet, "snippet", max_chars=2_000, allow_empty=True
            ),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "result_rank": self.result_rank,
            "source_url": self.source_url,
            "title": self.title,
            "snippet": self.snippet,
        }


@dataclass(frozen=True)
class ProfileSearchCandidate:
    """An unverified profile candidate; never an identity determination."""

    candidate_id: str
    query_id: str
    platform: str
    profile_url: str
    handle: str
    evidence: ProfileSearchEvidence
    provenance: ProfileSearchProvenance
    account_status: str = field(default="candidate", init=False)
    identity_status: str = field(default="unverified", init=False)
    review_status: str = field(default="pending", init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "candidate_id",
            _identifier(self.candidate_id, "candidate_id"),
        )
        query_id = _identifier(self.query_id, "query_id")
        if query_id != self.provenance.query_id:
            raise ProfileSearchContractError(
                "Candidate query_id does not match its provenance"
            )
        object.__setattr__(self, "query_id", query_id)
        platform = _identifier(self.platform, "platform")
        if platform not in PROFILE_SEARCH_PLATFORMS:
            raise ProfileSearchContractError(
                "Unsupported profile-search platform"
            )
        object.__setattr__(self, "platform", platform)
        profile_url = _public_https_url(self.profile_url, "profile_url")
        object.__setattr__(self, "profile_url", profile_url)
        object.__setattr__(
            self,
            "handle",
            _bounded_text(
                self.handle, "handle", max_chars=128, allow_empty=True
            ),
        )

    @classmethod
    def from_result(
        cls,
        query: ProfileSearchQuery,
        *,
        profile_url: str,
        handle: str,
        evidence: ProfileSearchEvidence,
        provenance: ProfileSearchProvenance,
    ) -> "ProfileSearchCandidate":
        if provenance.query_id != query.query_id:
            raise ProfileSearchContractError(
                "Query does not match the result provenance"
            )
        if provenance.query_fingerprint != query.fingerprint:
            raise ProfileSearchContractError(
                "Query fingerprint does not match the result provenance"
            )
        normalized_handle = handle.strip().casefold()
        identity = normalized_handle or profile_url.casefold()
        material = "\0".join((query.platform, identity))
        candidate_id = (
            "profile-search:"
            + hashlib.sha256(material.encode("utf-8")).hexdigest()
        )
        return cls(
            candidate_id=candidate_id,
            query_id=query.query_id,
            platform=query.platform,
            profile_url=profile_url,
            handle=handle,
            evidence=evidence,
            provenance=provenance,
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "query_id": self.query_id,
            "platform": self.platform,
            "profile_url": self.profile_url,
            "handle": self.handle,
            "account_status": self.account_status,
            "identity_status": self.identity_status,
            "review_status": self.review_status,
            "evidence": self.evidence.as_dict(),
            "provenance": self.provenance.as_dict(),
        }


@dataclass(frozen=True)
class ProfileSearchError:
    """A bounded public diagnostic for one failed provider request."""

    query_id: str
    provider: str
    code: str
    message: str
    retryable: bool
    occurred_at: str | datetime
    http_status: Optional[int] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "query_id", _identifier(self.query_id, "query_id")
        )
        object.__setattr__(
            self, "provider", _identifier(self.provider, "provider")
        )
        code = _bounded_text(self.code, "code", max_chars=64).casefold()
        if not _ERROR_CODE_PATTERN.fullmatch(code):
            raise ProfileSearchContractError(
                "Invalid profile-search error code"
            )
        object.__setattr__(self, "code", code)
        object.__setattr__(
            self,
            "message",
            _bounded_text(self.message, "message", max_chars=1_000),
        )
        if not isinstance(self.retryable, bool):
            raise ProfileSearchContractError("retryable must be a boolean")
        object.__setattr__(
            self, "occurred_at", _timestamp(self.occurred_at, "occurred_at")
        )
        if self.http_status is not None and (
            isinstance(self.http_status, bool)
            or not isinstance(self.http_status, int)
            or not 100 <= self.http_status <= 599
        ):
            raise ProfileSearchContractError(
                "http_status must be between 100 and 599"
            )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "query_id": self.query_id,
            "provider": self.provider,
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "occurred_at": self.occurred_at,
            "http_status": self.http_status,
        }
