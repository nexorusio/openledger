"""TikTok URL filtering and candidate extraction for profile search."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple
from urllib.parse import quote, unquote, urlsplit

from maigret.web.profile_search_contract import (
    ProfileSearchCandidate,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)


TIKTOK_PROFILE_HOSTS = frozenset(
    {
        "m.tiktok.com",
        "tiktok.com",
        "www.tiktok.com",
    }
)

_TIKTOK_HANDLE_PATTERN = re.compile(
    r"^[A-Za-z0-9_][A-Za-z0-9._]{0,22}[A-Za-z0-9_]$"
)
_TIKTOK_CONTENT_ID_PATTERN = re.compile(r"^[0-9]{5,32}$")


@dataclass(frozen=True)
class TikTokProfileReference:
    canonical_url: str
    handle: str
    reference_kind: str


def _path_segments(path: str) -> Optional[Tuple[str, ...]]:
    segments = []
    for raw_segment in path.split("/"):
        if not raw_segment:
            continue
        segment = unquote(raw_segment).strip()
        if (
            not segment
            or len(segment) > 120
            or "/" in segment
            or "\\" in segment
            or any(ord(character) < 32 for character in segment)
        ):
            return None
        segments.append(segment)
    return tuple(segments)


def _valid_handle(value: str) -> str:
    if not value.startswith("@") or value.count("@") != 1:
        return ""
    handle = value[1:].strip()
    if (
        not _TIKTOK_HANDLE_PATTERN.fullmatch(handle)
        or ".." in handle
        or handle.endswith(".")
    ):
        return ""
    return handle.casefold()


def parse_tiktok_profile_url(
    value: str,
) -> Optional[TikTokProfileReference]:
    """Return a TikTok profile reference from a durable account-linked URL."""
    try:
        parsed = urlsplit(str(value or "").strip())
        port = parsed.port
    except ValueError:
        return None
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if (
        parsed.scheme.casefold() != "https"
        or hostname not in TIKTOK_PROFILE_HOSTS
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    segments = _path_segments(parsed.path)
    if not segments:
        return None
    handle = _valid_handle(segments[0])
    if not handle:
        return None

    if len(segments) == 1:
        reference_kind = "profile"
    elif len(segments) == 2 and segments[1].casefold() == "live":
        reference_kind = "profile_live"
    elif (
        len(segments) == 3
        and segments[1].casefold() in {"photo", "video"}
        and _TIKTOK_CONTENT_ID_PATTERN.fullmatch(segments[2])
    ):
        reference_kind = f"profile_{segments[1].casefold()}"
    else:
        return None
    return TikTokProfileReference(
        canonical_url=(
            f"https://www.tiktok.com/@{quote(handle, safe='._')}"
        ),
        handle=handle,
        reference_kind=reference_kind,
    )


def tiktok_candidates_from_evidence(
    query: ProfileSearchQuery,
    evidence: Sequence[ProfileSearchEvidence],
    provenance: ProfileSearchProvenance,
) -> Tuple[ProfileSearchCandidate, ...]:
    """Convert only TikTok account-linked results into pending candidates."""
    if query.platform != "tiktok":
        raise ValueError("TikTok adapter requires a TikTok query")
    candidates = []
    seen = set()
    for item in evidence[: query.max_results]:
        reference = parse_tiktok_profile_url(item.source_url)
        if reference is None or reference.canonical_url.casefold() in seen:
            continue
        candidate = ProfileSearchCandidate.from_result(
            query,
            profile_url=reference.canonical_url,
            handle=reference.handle,
            evidence=item,
            provenance=provenance,
        )
        seen.add(reference.canonical_url.casefold())
        candidates.append(candidate)
    return tuple(candidates)
