"""X/Twitter URL filtering and candidate extraction for profile search."""

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


X_PROFILE_HOSTS = frozenset(
    {
        "m.twitter.com",
        "mobile.twitter.com",
        "twitter.com",
        "www.twitter.com",
        "x.com",
        "www.x.com",
    }
)

_X_RESERVED_ROOTS = frozenset(
    {
        "about",
        "account",
        "compose",
        "download",
        "explore",
        "hashtag",
        "help",
        "home",
        "i",
        "intent",
        "jobs",
        "login",
        "logout",
        "messages",
        "notifications",
        "privacy",
        "search",
        "settings",
        "share",
        "signup",
        "tos",
    }
)
_X_PROFILE_TABS = frozenset(
    {
        "articles",
        "followers",
        "following",
        "highlights",
        "likes",
        "media",
        "verified_followers",
        "with_replies",
    }
)
_X_HANDLE_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,15}$")
_X_STATUS_ID_PATTERN = re.compile(r"^[0-9]{5,32}$")
_X_MEDIA_INDEX_PATTERN = re.compile(r"^[1-4]$")


@dataclass(frozen=True)
class XProfileReference:
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
    handle = value.strip()
    if (
        not _X_HANDLE_PATTERN.fullmatch(handle)
        or handle.casefold() in _X_RESERVED_ROOTS
    ):
        return ""
    return handle.casefold()


def _status_path_kind(segments: Tuple[str, ...]) -> str:
    if (
        len(segments) < 3
        or segments[1].casefold() not in {"status", "statuses"}
        or not _X_STATUS_ID_PATTERN.fullmatch(segments[2])
    ):
        return ""
    if len(segments) == 3:
        return "profile_status"
    if (
        len(segments) == 5
        and segments[3].casefold() in {"photo", "video"}
        and _X_MEDIA_INDEX_PATTERN.fullmatch(segments[4])
    ):
        return "profile_status_media"
    return ""


def parse_x_profile_url(value: str) -> Optional[XProfileReference]:
    """Return an X profile from x.com or a legacy twitter.com URL."""
    try:
        parsed = urlsplit(str(value or "").strip())
        port = parsed.port
    except ValueError:
        return None
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if (
        parsed.scheme.casefold() != "https"
        or hostname not in X_PROFILE_HOSTS
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
    elif len(segments) == 2 and segments[1].casefold() in _X_PROFILE_TABS:
        reference_kind = "profile_tab"
    else:
        reference_kind = _status_path_kind(segments)
        if not reference_kind:
            return None
    return XProfileReference(
        canonical_url=f"https://x.com/{quote(handle, safe='_')}",
        handle=handle,
        reference_kind=reference_kind,
    )


def x_candidates_from_evidence(
    query: ProfileSearchQuery,
    evidence: Sequence[ProfileSearchEvidence],
    provenance: ProfileSearchProvenance,
) -> Tuple[ProfileSearchCandidate, ...]:
    """Convert X/Twitter account-linked results to pending candidates."""
    if query.platform != "x":
        raise ValueError("X adapter requires an X query")
    candidates = []
    seen = set()
    for item in evidence[: query.max_results]:
        reference = parse_x_profile_url(item.source_url)
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
