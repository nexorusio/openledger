"""Facebook URL filtering and candidate extraction for profile search."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote, unquote, urlsplit

from maigret.web.profile_search_contract import (
    ProfileSearchCandidate,
    ProfileSearchEvidence,
    ProfileSearchProvenance,
    ProfileSearchQuery,
)


FACEBOOK_PROFILE_HOSTS = frozenset(
    {
        "facebook.com",
        "m.facebook.com",
        "mbasic.facebook.com",
        "web.facebook.com",
        "www.facebook.com",
    }
)

_FACEBOOK_RESERVED_ROOTS = frozenset(
    {
        "about",
        "ads",
        "app",
        "apps",
        "business",
        "checkpoint",
        "community",
        "developers",
        "dialog",
        "events",
        "gaming",
        "groups",
        "hashtag",
        "help",
        "home.php",
        "legal",
        "live",
        "login",
        "logout",
        "marketplace",
        "messages",
        "photo.php",
        "photos",
        "plugins",
        "policies",
        "privacy",
        "public",
        "recover",
        "reel",
        "reels",
        "search",
        "share",
        "sharer",
        "story.php",
        "watch",
    }
)
_FACEBOOK_HANDLE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,99}$")
_FACEBOOK_NUMERIC_ID_PATTERN = re.compile(r"^[0-9]{1,32}$")
_DOMAIN_LIKE_HANDLE_PATTERN = re.compile(r"\.(?:com|net|org)$", re.IGNORECASE)


@dataclass(frozen=True)
class FacebookProfileReference:
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
            or len(segment) > 200
            or "/" in segment
            or "\\" in segment
            or any(ord(character) < 32 for character in segment)
        ):
            return None
        segments.append(segment)
    return tuple(segments)


def _valid_handle(value: str, *, allow_domain_like: bool = False) -> str:
    handle = value.lstrip("@").strip()
    if (
        not _FACEBOOK_HANDLE_PATTERN.fullmatch(handle)
        or handle.casefold() in _FACEBOOK_RESERVED_ROOTS
        or ".." in handle
        or handle.endswith(".")
        or (
            not allow_domain_like
            and _DOMAIN_LIKE_HANDLE_PATTERN.search(handle) is not None
        )
    ):
        return ""
    return handle.casefold()


def parse_facebook_profile_url(
    value: str,
) -> Optional[FacebookProfileReference]:
    """Return a canonical public profile/page reference or reject the URL."""
    try:
        parsed = urlsplit(str(value or "").strip())
        port = parsed.port
    except ValueError:
        return None
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if (
        parsed.scheme.casefold() != "https"
        or hostname not in FACEBOOK_PROFILE_HOSTS
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    segments = _path_segments(parsed.path)
    if not segments:
        return None
    root = segments[0].casefold()

    if root == "profile.php":
        query = parse_qs(parsed.query, keep_blank_values=True)
        identifiers = query.get("id") or []
        if len(identifiers) != 1:
            return None
        identifier = identifiers[0]
        if not _FACEBOOK_NUMERIC_ID_PATTERN.fullmatch(identifier):
            return None
        return FacebookProfileReference(
            canonical_url=(
                f"https://www.facebook.com/profile.php?id={identifier}"
            ),
            handle=identifier,
            reference_kind="numeric_profile",
        )

    if root in {"pages", "people"}:
        if len(segments) < 3:
            return None
        identifier = segments[2]
        if not _FACEBOOK_NUMERIC_ID_PATTERN.fullmatch(identifier):
            return None
        slug = _valid_handle(segments[1], allow_domain_like=True)
        if not slug:
            return None
        return FacebookProfileReference(
            canonical_url=(
                f"https://www.facebook.com/{root}/"
                f"{quote(slug, safe='._-')}/{identifier}"
            ),
            handle=identifier,
            reference_kind=(
                "legacy_page" if root == "pages" else "people_profile"
            ),
        )

    if root == "pg":
        if len(segments) < 2:
            return None
        handle = _valid_handle(segments[1])
    elif root in _FACEBOOK_RESERVED_ROOTS or root.endswith(".php"):
        return None
    else:
        handle = _valid_handle(segments[0])
    if not handle:
        return None
    return FacebookProfileReference(
        canonical_url=f"https://www.facebook.com/{quote(handle, safe='._-')}",
        handle=handle,
        reference_kind="vanity_profile_or_page",
    )


def facebook_candidates_from_evidence(
    query: ProfileSearchQuery,
    evidence: Sequence[ProfileSearchEvidence],
    provenance: ProfileSearchProvenance,
) -> Tuple[ProfileSearchCandidate, ...]:
    """Convert only Facebook profile-like results into pending candidates."""
    if query.platform != "facebook":
        raise ValueError("Facebook adapter requires a Facebook query")
    candidates = []
    seen = set()
    for item in evidence[: query.max_results]:
        reference = parse_facebook_profile_url(item.source_url)
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
