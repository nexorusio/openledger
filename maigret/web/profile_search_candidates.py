"""Canonical aggregation for native profile-search candidates."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, Mapping, Tuple

from maigret.web.profile_search_backend import ProfileSearchRun
from maigret.web.profile_search_contract import ProfileSearchCandidate
from maigret.web.profile_search_facebook import (
    facebook_candidates_from_evidence,
)
from maigret.web.profile_search_instagram import (
    instagram_candidates_from_evidence,
)
from maigret.web.profile_search_threads import (
    threads_candidates_from_evidence,
)
from maigret.web.profile_search_tiktok import (
    tiktok_candidates_from_evidence,
)
from maigret.web.profile_search_x import x_candidates_from_evidence


ProfileSearchAdapter = Callable[..., Tuple[ProfileSearchCandidate, ...]]

PROFILE_SEARCH_ADAPTERS: Mapping[str, ProfileSearchAdapter] = MappingProxyType(
    {
        "facebook": facebook_candidates_from_evidence,
        "instagram": instagram_candidates_from_evidence,
        "threads": threads_candidates_from_evidence,
        "tiktok": tiktok_candidates_from_evidence,
        "x": x_candidates_from_evidence,
    }
)


def candidates_from_profile_search_run(
    run: ProfileSearchRun,
) -> Tuple[ProfileSearchCandidate, ...]:
    """Dispatch one successful run through its platform adapter."""
    if run.error is not None or run.provenance is None:
        return ()
    adapter = PROFILE_SEARCH_ADAPTERS[run.query.platform]
    return adapter(run.query, run.evidence, run.provenance)


def _observation_identity(
    candidate: ProfileSearchCandidate,
) -> Tuple[str, ...]:
    return (
        candidate.provenance.provider,
        candidate.query_id,
        candidate.evidence.source_url.casefold(),
    )


def _observation_preference(
    candidate: ProfileSearchCandidate,
) -> Tuple[Any, ...]:
    """Choose the best duplicate deterministically, preferring search rank."""
    return (
        candidate.evidence.result_rank,
        candidate.evidence.source_url.casefold(),
        candidate.evidence.title.casefold(),
        candidate.evidence.snippet.casefold(),
        candidate.provenance.retrieved_at,
        candidate.provenance.provider_request_id,
    )


def _observation_sort(candidate: ProfileSearchCandidate) -> Tuple[Any, ...]:
    return (
        candidate.evidence.result_rank,
        candidate.provenance.provider,
        candidate.query_id,
        candidate.evidence.source_url.casefold(),
        candidate.provenance.retrieved_at,
        candidate.provenance.provider_request_id,
    )


@dataclass(frozen=True)
class ProfileSearchCandidateGroup:
    """One canonical account candidate with all distinct observations."""

    candidate_id: str
    platform: str
    profile_url: str
    handle: str
    observations: Tuple[ProfileSearchCandidate, ...]
    alternate_profile_urls: Tuple[str, ...] = ()
    account_status: str = field(default="candidate", init=False)
    identity_status: str = field(default="unverified", init=False)
    review_status: str = field(default="pending", init=False)

    @property
    def source_count(self) -> int:
        return len(self.observations)

    @property
    def query_count(self) -> int:
        return len({item.query_id for item in self.observations})

    def as_dict(self) -> Dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "platform": self.platform,
            "profile_url": self.profile_url,
            "alternate_profile_urls": list(self.alternate_profile_urls),
            "handle": self.handle,
            "account_status": self.account_status,
            "identity_status": self.identity_status,
            "review_status": self.review_status,
            "source_count": self.source_count,
            "query_count": self.query_count,
            "observations": [
                {
                    "query_id": item.query_id,
                    "evidence": item.evidence.as_dict(),
                    "provenance": item.provenance.as_dict(),
                }
                for item in self.observations
            ],
        }


def merge_profile_search_candidates(
    candidates: Iterable[ProfileSearchCandidate],
) -> Tuple[ProfileSearchCandidateGroup, ...]:
    """Merge canonical accounts while retaining distinct raw observations."""
    grouped: Dict[str, Dict[Tuple[str, ...], ProfileSearchCandidate]] = {}
    for candidate in candidates:
        observations = grouped.setdefault(candidate.candidate_id, {})
        key = _observation_identity(candidate)
        current = observations.get(key)
        if current is None or _observation_preference(
            candidate
        ) < _observation_preference(current):
            observations[key] = candidate

    merged = []
    for candidate_id, unique_observations in grouped.items():
        observations = tuple(
            sorted(
                unique_observations.values(), key=_observation_sort
            )
        )
        platforms = {item.platform for item in observations}
        handles = {item.handle.casefold() for item in observations}
        if len(platforms) != 1 or len(handles) != 1:
            raise ValueError(
                "Candidate identity contains conflicting accounts"
            )
        profile_urls = tuple(
            sorted(
                {item.profile_url for item in observations},
                key=lambda value: (len(value), value.casefold(), value),
            )
        )
        representative = min(
            observations,
            key=lambda item: (
                item.handle.casefold(),
                item.handle,
                _observation_sort(item),
            ),
        )
        merged.append(
            ProfileSearchCandidateGroup(
                candidate_id=candidate_id,
                platform=representative.platform,
                profile_url=profile_urls[0],
                alternate_profile_urls=profile_urls[1:],
                handle=representative.handle.casefold(),
                observations=observations,
            )
        )
    return tuple(
        sorted(
            merged,
            key=lambda item: (
                item.platform,
                item.handle,
                item.profile_url.casefold(),
                item.candidate_id,
            ),
        )
    )


def merge_profile_search_runs(
    runs: Iterable[ProfileSearchRun],
) -> Tuple[ProfileSearchCandidateGroup, ...]:
    """Adapt and aggregate successful runs; failed runs yield no candidate."""
    candidates = (
        candidate
        for run in runs
        for candidate in candidates_from_profile_search_run(run)
    )
    return merge_profile_search_candidates(candidates)
