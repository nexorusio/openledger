"""Instagram URL filtering and candidate extraction for profile search."""

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


INSTAGRAM_PROFILE_HOSTS = frozenset(
    {
        "instagram.com",
        "m.instagram.com",
        "www.instagram.com",
    }
)

_INSTAGRAM_RESERVED_ROOTS = frozenset(
    {
        "about",
        "accounts",
        "api",
        "challenge",
        "developer",
        "developers",
        "direct",
        "directory",
        "emails",
        "explore",
        "graphql",
        "legal",
        "oauth",
        "p",
        "privacy",
        "reel",
        "reels",
        "share",
        "stories",
        "terms",
        "tv",
        "web",
    }
)
_INSTAGRAM_PROFILE_TABS = frozenset(
    {"channel", "followers", "following", "reels", "tagged"}
)
_INSTAGRAM_HANDLE_PATTERN = re.compile(
    r"^[A-Za-z0-9_](?:[A-Za-z0-9._]{0,29})$"
)


@dataclass(frozen=True)
class InstagramProfileReference:
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
            or len(segment) > 100
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
        not _INSTAGRAM_HANDLE_PATTERN.fullmatch(handle)
        or handle.casefold() in _INSTAGRAM_RESERVED_ROOTS
        or ".." in handle
        or handle.endswith(".")
    ):
        return ""
    return handle.casefold()


def parse_instagram_profile_url(
    value: str,
) -> Optional[InstagramProfileReference]:
    """Return an Instagram profile reference, excluding content routes."""
    try:
        parsed = urlsplit(str(value or "").strip())
        port = parsed.port
    except ValueError:
        return None
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if (
        parsed.scheme.casefold() != "https"
        or hostname not in INSTAGRAM_PROFILE_HOSTS
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    segments = _path_segments(parsed.path)
    if not segments:
        return None

    root = segments[0].casefold()
    if root == "_u":
        if len(segments) != 2:
            return None
        handle = _valid_handle(segments[1])
        reference_kind = "app_profile_link"
    else:
        if root in _INSTAGRAM_RESERVED_ROOTS:
            return None
        if len(segments) > 2:
            return None
        if len(segments) == 2 and (
            segments[1].casefold() not in _INSTAGRAM_PROFILE_TABS
        ):
            return None
        handle = _valid_handle(segments[0])
        reference_kind = (
            "profile_tab" if len(segments) == 2 else "profile"
        )
    if not handle:
        return None
    return InstagramProfileReference(
        canonical_url=(
            f"https://www.instagram.com/{quote(handle, safe='._')}/"
        ),
        handle=handle,
        reference_kind=reference_kind,
    )


def instagram_candidates_from_evidence(
    query: ProfileSearchQuery,
    evidence: Sequence[ProfileSearchEvidence],
    provenance: ProfileSearchProvenance,
) -> Tuple[ProfileSearchCandidate, ...]:
    """Convert only Instagram profile-like results into pending candidates."""
    if query.platform != "instagram":
        raise ValueError("Instagram adapter requires an Instagram query")
    candidates = []
    seen = set()
    for item in evidence[: query.max_results]:
        reference = parse_instagram_profile_url(item.source_url)
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
