"""Deterministic query planning for bounded major-platform discovery."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from itertools import islice
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from maigret.web.profile_search_contract import (
    MAX_PROFILE_SEARCH_RESULTS,
    PROFILE_SEARCH_PLATFORMS,
    ProfileSearchQuery,
)


MAX_PROFILE_SEARCH_QUERIES = 25
MAX_PROFILE_SEARCH_SEEDS = 5
MAX_EXISTING_PROFILE_SEEDS = 8

PLATFORM_SEARCH_DOMAINS = {
    "facebook": "facebook.com",
    "instagram": "instagram.com",
    "threads": "threads.com",
    "tiktok": "tiktok.com",
    "x": "x.com",
}

_SEED_PRIORITY = {
    "confirmed_username": 0,
    "profile_url": 1,
    "social_handle": 2,
    "username": 2,
    "alias": 3,
    "full_name": 4,
}
_WHITESPACE_PATTERN = re.compile(r"\s+")


class ProfileSearchPlanningError(ValueError):
    """Raised when a caller attempts an unsafe or unbounded search plan."""


@dataclass(frozen=True)
class _Seed:
    value: str
    kind: str
    score: int
    reason: str


def _text(value: Any, *, max_chars: int) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    collapsed = _WHITESPACE_PATTERN.sub(" ", normalized).strip()
    if len(collapsed) > max_chars:
        collapsed = collapsed[:max_chars].rstrip()
    if any(ord(character) < 32 for character in collapsed):
        return ""
    return collapsed


def _search_phrase(value: str) -> str:
    # Exact phrases prevent a name or alias from becoming a broad token spray.
    return _text(value.replace('"', " ").replace("\\", " "), max_chars=300)


def _seed_key(seed: _Seed) -> str:
    return unicodedata.normalize("NFKC", seed.value).casefold()


def _add_seed(seeds: Dict[str, _Seed], seed: _Seed) -> None:
    value = _search_phrase(seed.value)
    if not value:
        return
    normalized = _Seed(
        value=value,
        kind=seed.kind,
        score=max(0, min(int(seed.score), 100)),
        reason=_text(seed.reason, max_chars=240),
    )
    key = _seed_key(normalized)
    previous = seeds.get(key)
    if previous is None or (
        _SEED_PRIORITY[normalized.kind],
        -normalized.score,
    ) < (_SEED_PRIORITY[previous.kind], -previous.score):
        seeds[key] = normalized


def _approved_existing_profile_seeds(
    existing_evidence: Iterable[Any],
) -> List[_Seed]:
    seeds = []
    for raw_claim in islice(existing_evidence, MAX_EXISTING_PROFILE_SEEDS):
        if not isinstance(raw_claim, dict):
            continue
        review_status = str(
            raw_claim.get("review_status")
            or raw_claim.get("decision")
            or ""
        ).casefold()
        if review_status != "approved":
            continue
        if str(raw_claim.get("field_name") or "") != "social_account":
            continue
        value = raw_claim.get("value")
        if not isinstance(value, dict):
            continue
        username = _text(
            value.get("username") or value.get("handle"), max_chars=128
        ).lstrip("@")
        if username:
            seeds.append(
                _Seed(
                    value=username,
                    kind="confirmed_username",
                    score=100,
                    reason="Username from an analyst-approved social account",
                )
            )
    return seeds


def _plan_seeds(
    investigation_plan: Any,
    *,
    existing_evidence: Iterable[Any],
) -> List[_Seed]:
    if not isinstance(investigation_plan, dict):
        raise ProfileSearchPlanningError(
            "Profile search requires an investigation plan"
        )
    seeds: Dict[str, _Seed] = {}
    for seed in _approved_existing_profile_seeds(existing_evidence):
        _add_seed(seeds, seed)

    raw_targets = investigation_plan.get("search_targets") or []
    for raw_target in list(raw_targets)[:24]:
        if not isinstance(raw_target, dict):
            continue
        value = _text(raw_target.get("value"), max_chars=128).lstrip("@")
        source_type = str(raw_target.get("source_type") or "").casefold()
        if source_type not in {
            "profile_url",
            "ranked_alias",
            "social_handle",
            "username",
        }:
            continue
        if source_type == "ranked_alias":
            kind = "alias"
            try:
                score = int(raw_target.get("alias_score", 0))
            except (TypeError, ValueError):
                continue
            reason = str(
                raw_target.get("alias_reason") or "Analyst-selected alias"
            )
        elif source_type == "profile_url":
            kind = "profile_url"
            score = 100
            reason = "Handle extracted from an analyst-supplied profile URL"
        else:
            kind = source_type
            score = 100
            reason = "Analyst-supplied account identifier"
        if value:
            _add_seed(
                seeds,
                _Seed(value=value, kind=kind, score=score, reason=reason),
            )

    full_names = []
    for identifier in list(investigation_plan.get("identifiers") or [])[:24]:
        if not isinstance(identifier, dict):
            continue
        if str(identifier.get("type") or "") != "full_name":
            continue
        name = _text(identifier.get("value"), max_chars=300)
        if name and name.casefold() not in {
            item.casefold() for item in full_names
        }:
            full_names.append(name)
    ordered = sorted(
        seeds.values(),
        key=lambda item: (
            _SEED_PRIORITY[item.kind],
            -item.score,
            item.value.casefold(),
        ),
    )
    if full_names:
        # Reserve one of the five search slots for the strongest exact name.
        ordered = ordered[: MAX_PROFILE_SEARCH_SEEDS - 1]
        reserved_seeds = {_seed_key(item): item for item in ordered}
        _add_seed(
            reserved_seeds,
            _Seed(
                value=full_names[0],
                kind="full_name",
                score=70,
                reason="Analyst-supplied full name",
            ),
        )
        ordered = sorted(
            reserved_seeds.values(),
            key=lambda item: (
                _SEED_PRIORITY[item.kind],
                -item.score,
                item.value.casefold(),
            ),
        )
    return ordered[:MAX_PROFILE_SEARCH_SEEDS]


def _platforms(values: Optional[Sequence[Any]]) -> Tuple[str, ...]:
    requested = sorted(PROFILE_SEARCH_PLATFORMS) if values is None else values
    normalized = []
    for raw_value in requested:
        platform = str(raw_value or "").strip().casefold()
        if platform not in PROFILE_SEARCH_PLATFORMS:
            raise ProfileSearchPlanningError(
                "Select only supported profile-search platforms"
            )
        if platform not in normalized:
            normalized.append(platform)
    if not normalized:
        raise ProfileSearchPlanningError(
            "Select at least one profile-search platform"
        )
    return tuple(normalized)


def _query_id(platform: str, seed: _Seed) -> str:
    material = "\0".join((platform, seed.kind, seed.value.casefold()))
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
    return f"profile-query:{digest}"


def plan_profile_search_queries(
    investigation_plan: Any,
    *,
    existing_evidence: Iterable[Any] = (),
    platforms: Optional[Sequence[Any]] = None,
    max_queries: int = MAX_PROFILE_SEARCH_QUERIES,
    max_results: int = MAX_PROFILE_SEARCH_RESULTS,
) -> List[ProfileSearchQuery]:
    """Build a fair, stable query set without email or phone pivots."""
    if (
        isinstance(max_queries, bool)
        or not isinstance(max_queries, int)
        or not 1 <= max_queries <= MAX_PROFILE_SEARCH_QUERIES
    ):
        raise ProfileSearchPlanningError(
            f"max_queries must be between 1 and {MAX_PROFILE_SEARCH_QUERIES}"
        )
    if (
        isinstance(max_results, bool)
        or not isinstance(max_results, int)
        or not 1 <= max_results <= MAX_PROFILE_SEARCH_RESULTS
    ):
        raise ProfileSearchPlanningError(
            f"max_results must be between 1 and {MAX_PROFILE_SEARCH_RESULTS}"
        )
    selected_platforms = _platforms(platforms)
    seeds = _plan_seeds(
        investigation_plan,
        existing_evidence=existing_evidence,
    )
    queries = []
    # Seed-first ordering gives every selected platform equal early coverage.
    for seed in seeds:
        for platform in selected_platforms:
            domain = PLATFORM_SEARCH_DOMAINS[platform]
            queries.append(
                ProfileSearchQuery(
                    query_id=_query_id(platform, seed),
                    platform=platform,
                    query_text=f'site:{domain} "{seed.value}"',
                    seed_kind=seed.kind,
                    seed_value=seed.value,
                    seed_score=seed.score,
                    seed_reason=seed.reason,
                    max_results=max_results,
                )
            )
            if len(queries) >= max_queries:
                return queries
    return queries
