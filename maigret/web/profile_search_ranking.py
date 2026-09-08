"""Explainable review-priority ranking for profile-search candidates."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Tuple

from maigret.web.profile_search_candidates import (
    ProfileSearchCandidateGroup,
)
from maigret.web.profile_search_contract import ProfileSearchQuery
from maigret.web.profile_search_facebook import parse_facebook_profile_url
from maigret.web.profile_search_instagram import parse_instagram_profile_url
from maigret.web.profile_search_threads import parse_threads_profile_url
from maigret.web.profile_search_tiktok import parse_tiktok_profile_url
from maigret.web.profile_search_x import parse_x_profile_url


PROFILE_SEARCH_RANKING_MODEL_VERSION = 1

_SEED_MATCH_POINTS = {
    "confirmed_username": 60,
    "profile_url": 58,
    "social_handle": 55,
    "username": 55,
    "alias": 45,
    "full_name": 30,
}
_PROFILE_URL_PARSERS = (
    parse_facebook_profile_url,
    parse_instagram_profile_url,
    parse_threads_profile_url,
    parse_tiktok_profile_url,
    parse_x_profile_url,
)


@dataclass(frozen=True)
class ProfileSearchRankingSignal:
    """One bounded, human-readable contribution to review priority."""

    code: str
    points: int
    detail: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "points": self.points,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class RankedProfileSearchCandidate:
    """A candidate ordered for review without asserting identity."""

    candidate: ProfileSearchCandidateGroup
    discovery_score: int
    review_priority: str
    ranking_signals: Tuple[ProfileSearchRankingSignal, ...]
    ranking_model_version: int = field(
        default=PROFILE_SEARCH_RANKING_MODEL_VERSION,
        init=False,
    )
    score_scope: str = field(
        default="discovery_review_priority",
        init=False,
    )

    def as_dict(self) -> Dict[str, Any]:
        result = self.candidate.as_dict()
        result.update(
            {
                "ranking_model_version": self.ranking_model_version,
                "discovery_score": self.discovery_score,
                "score_scope": self.score_scope,
                "review_priority": self.review_priority,
                "ranking_signals": [
                    signal.as_dict() for signal in self.ranking_signals
                ],
            }
        )
        return result


def _normalized(value: str) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).strip().casefold()


def _identifier(value: str) -> str:
    return _normalized(value).lstrip("@").strip("/ ")


def _compact(value: str) -> str:
    return "".join(
        character for character in _identifier(value) if character.isalnum()
    )


def _profile_url_seed(value: str) -> str:
    """Extract an account identifier from a known profile-shaped URL seed."""
    for parser in _PROFILE_URL_PARSERS:
        reference = parser(value)
        if reference is not None:
            return _identifier(reference.handle)
    return ""


def _seed_identifier(query: ProfileSearchQuery) -> str:
    if query.seed_kind == "profile_url":
        return _profile_url_seed(query.seed_value)
    return _identifier(query.seed_value)


def _match_mode(handle: str, query: ProfileSearchQuery) -> str:
    seed = _seed_identifier(query)
    normalized_handle = _identifier(handle)
    if not seed or not normalized_handle:
        return ""
    if normalized_handle == seed:
        return "exact"
    if _compact(normalized_handle) == _compact(seed):
        return "normalized"
    return ""


def _query_index(
    queries: Iterable[ProfileSearchQuery],
) -> Mapping[str, ProfileSearchQuery]:
    indexed: Dict[str, ProfileSearchQuery] = {}
    for query in queries:
        current = indexed.get(query.query_id)
        if current is not None and current != query:
            raise ValueError("Conflicting profile-search query definitions")
        indexed[query.query_id] = query
    return indexed


def _queries_for_candidate(
    candidate: ProfileSearchCandidateGroup,
    query_by_id: Mapping[str, ProfileSearchQuery],
) -> Tuple[ProfileSearchQuery, ...]:
    if not candidate.observations:
        raise ValueError("Cannot rank a candidate without observations")
    relevant: Dict[str, ProfileSearchQuery] = {}
    profile_urls = {
        candidate.profile_url.casefold(),
        *(value.casefold() for value in candidate.alternate_profile_urls),
    }
    for observation in candidate.observations:
        query = query_by_id.get(observation.query_id)
        if query is None:
            raise ValueError("Candidate observation has no query definition")
        if (
            observation.candidate_id != candidate.candidate_id
            or _identifier(observation.handle)
            != _identifier(candidate.handle)
            or observation.profile_url.casefold() not in profile_urls
        ):
            raise ValueError(
                "Candidate group contains conflicting account identity"
            )
        if (
            query.platform != candidate.platform
            or observation.platform != candidate.platform
            or observation.provenance.query_fingerprint != query.fingerprint
        ):
            raise ValueError(
                "Candidate observation has conflicting query lineage"
            )
        relevant[query.query_id] = query
    return tuple(
        sorted(relevant.values(), key=lambda item: item.query_id)
    )


def _seed_signals(
    candidate: ProfileSearchCandidateGroup,
    queries: Tuple[ProfileSearchQuery, ...],
) -> Tuple[ProfileSearchRankingSignal, ...]:
    matched_by_seed = {}
    for query in queries:
        match_mode = _match_mode(candidate.handle, query)
        if not match_mode:
            continue
        maximum = _SEED_MATCH_POINTS[query.seed_kind]
        points = (maximum * query.seed_score + 50) // 100
        seed_key = (query.seed_kind, _seed_identifier(query))
        match = (points, match_mode, query)
        current = matched_by_seed.get(seed_key)
        if current is None or (
            points,
            match_mode == "exact",
            query.query_id,
        ) > (
            current[0],
            current[1] == "exact",
            current[2].query_id,
        ):
            matched_by_seed[seed_key] = match
    matched = tuple(matched_by_seed.values())
    if not matched:
        return (
            ProfileSearchRankingSignal(
                code="search_result_only",
                points=5,
                detail=(
                    "Profile appeared in a bounded platform search, but its "
                    "handle did not match the search seed."
                ),
            ),
        )

    ordered_matches = sorted(
        matched,
        key=lambda item: (
            item[0],
            item[1] == "exact",
            item[2].seed_kind,
            item[2].query_id,
        ),
        reverse=True,
    )
    strongest_points, match_mode, strongest = ordered_matches[0]
    signals = [
        ProfileSearchRankingSignal(
            code=f"{match_mode}_{strongest.seed_kind}_match",
            points=strongest_points,
            detail=(
                "Candidate handle matches the strongest "
                f"{strongest.seed_kind.replace('_', ' ')} search seed."
            ),
        )
    ]
    additional = min(
        sum(
            min(6, (points + 9) // 10)
            for points, _, _ in ordered_matches[1:]
        ),
        18,
    )
    if additional:
        signals.append(
            ProfileSearchRankingSignal(
                code="additional_matching_seeds",
                points=additional,
                detail=(
                    f"{len(ordered_matches) - 1} additional bounded search "
                    "seed(s) "
                    "matched the candidate handle."
                ),
            )
        )
    return tuple(signals)


def _correlation_signals(
    candidate: ProfileSearchCandidateGroup,
) -> Tuple[ProfileSearchRankingSignal, ...]:
    observations = candidate.observations
    best_rank = min(item.evidence.result_rank for item in observations)
    rank_points = max(0, 11 - min(best_rank, 11))
    signals = [
        ProfileSearchRankingSignal(
            code="best_search_position",
            points=rank_points,
            detail=f"Best bounded provider-result position was {best_rank}.",
        )
    ]

    provider_count = len(
        {item.provenance.provider for item in observations}
    )
    provider_points = min(max(provider_count - 1, 0) * 4, 8)
    if provider_points:
        signals.append(
            ProfileSearchRankingSignal(
                code="provider_repeatability",
                points=provider_points,
                detail=(
                    f"Candidate appeared through {provider_count} search "
                    "providers; this supports retrieval repeatability, not "
                    "identity verification."
                ),
            )
        )

    source_count = len(
        {item.evidence.source_url.casefold() for item in observations}
    )
    source_points = min(max(source_count - 1, 0) * 2, 4)
    if source_points:
        signals.append(
            ProfileSearchRankingSignal(
                code="additional_profile_routes",
                points=source_points,
                detail=(
                    f"{source_count} distinct result URLs resolved to the "
                    "same canonical platform account."
                ),
            )
        )
    return tuple(signals)


def _priority(score: int) -> str:
    if score >= 70:
        return "high"
    if score >= 40:
        return "standard"
    return "low"


def rank_profile_search_candidates(
    candidates: Iterable[ProfileSearchCandidateGroup],
    *,
    queries: Iterable[ProfileSearchQuery],
) -> Tuple[RankedProfileSearchCandidate, ...]:
    """Rank candidates for analyst review using only explainable signals."""
    query_by_id = _query_index(queries)
    ranked = []
    for candidate in candidates:
        relevant_queries = _queries_for_candidate(candidate, query_by_id)
        signals = _seed_signals(candidate, relevant_queries)
        signals += _correlation_signals(candidate)
        score = sum(signal.points for signal in signals)
        if not 0 <= score <= 100:
            raise RuntimeError("Profile-search ranking score is out of bounds")
        ranked.append(
            RankedProfileSearchCandidate(
                candidate=candidate,
                discovery_score=score,
                review_priority=_priority(score),
                ranking_signals=signals,
            )
        )
    return tuple(
        sorted(
            ranked,
            key=lambda item: (
                -item.discovery_score,
                item.candidate.platform,
                item.candidate.handle,
                item.candidate.profile_url.casefold(),
                item.candidate.candidate_id,
            ),
        )
    )
