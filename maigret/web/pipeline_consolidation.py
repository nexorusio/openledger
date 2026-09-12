"""Linear-space account/qualified-claim grouping with append-only provenance.

Grouping is equivalence of a hypothesis, never an attribution or truth decision.
No all-pairs comparison is used: dense cases scale with observations/memberships.
"""

from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from typing import Any, Dict, Iterable, Mapping, Optional
from urllib.parse import parse_qs, urlsplit

from maigret.web.pipeline_evidence import (
    ObservationContractError,
    canonical_source_url,
    fingerprint,
)

_PLATFORM_ALIASES = {
    "twitter": "x",
    "twitter.com": "x",
    "x.com": "x",
    "github.com": "github",
    "instagram.com": "instagram",
    "facebook.com": "facebook",
    "tiktok.com": "tiktok",
    "threads.net": "threads",
    "threads.com": "threads",
}
_SINGLE_VALUE_PREDICATES = frozenset(
    {
        "birth_date",
        "date_of_birth",
        "death_date",
        "date_of_death",
        "platform_identifier",
        "legal_identifier",
    }
)


def subject_claim_conflicts(items, included_accounts) -> list:
    """Check curated subject assertions after account attribution, at QC time.

    Collection groups remain account-scoped. Only inherently single-valued
    subject facts are compared here; account IDs, jobs, affiliations, addresses
    and unspecified identifier schemes are intentionally not single-valued.
    Birth/death aliases share a rule, and date precision must be compatible.
    """
    from calendar import monthrange
    from datetime import date

    aliases = {
        "birth_date": "date_of_birth",
        "date_of_birth": "date_of_birth",
        "death_date": "date_of_death",
        "date_of_death": "date_of_death",
    }

    def date_bounds(value):
        try:
            if not isinstance(value, str) or not re.fullmatch(
                r"\d{4}(?:-\d{2}(?:-\d{2})?)?", value
            ):
                return None
            parts = list(map(int, value.split("-")))
            year, month = parts[0], parts[1] if len(parts) > 1 else 1
            lower = date(year, month, parts[2] if len(parts) > 2 else 1)
            upper = (
                date(year, 12, 31)
                if len(parts) == 1
                else (
                    date(year, month, monthrange(year, month)[1])
                    if len(parts) == 2
                    else lower
                )
            )
            return lower, upper
        except ValueError:
            return None

    buckets = {}
    for item in items:
        claim = item.get("normalized", {})
        predicate = aliases.get(claim.get("predicate"))
        if item.get("kind") != "claim" or not predicate:
            continue
        if claim.get("account_key") and claim["account_key"] not in included_accounts:
            continue  # Its existing attribution blocker applies first.
        buckets.setdefault(predicate, []).append(item)
    conflicts = []
    for predicate, members in buckets.items():
        values = {fingerprint(item["normalized"].get("value")) for item in members}
        if len(values) < 2:
            continue
        bounds = [date_bounds(item["normalized"].get("value")) for item in members]
        if all(bounds) and max(bound[0] for bound in bounds) <= min(
            bound[1] for bound in bounds
        ):
            continue
        group_ids = sorted(item["group_id"] for item in members)
        conflicts.append(
            {
                "conflict_id": "subject-conflict:"
                + fingerprint([predicate, group_ids]),
                "kind": "contradictory_attributed_subject_values",
                "predicate": predicate,
                "group_ids": group_ids,
                "reason": "Included accounts assert incompatible subject facts; resolve the conflicting curated values before QC approval",
            }
        )
    return conflicts


@lru_cache(maxsize=1)
def _profile_parsers():
    from maigret.web.profile_search_facebook import parse_facebook_profile_url
    from maigret.web.profile_search_instagram import parse_instagram_profile_url
    from maigret.web.profile_search_threads import parse_threads_profile_url
    from maigret.web.profile_search_tiktok import parse_tiktok_profile_url
    from maigret.web.profile_search_x import parse_x_profile_url

    return (
        ("facebook", parse_facebook_profile_url),
        ("instagram", parse_instagram_profile_url),
        ("threads", parse_threads_profile_url),
        ("tiktok", parse_tiktok_profile_url),
        ("x", parse_x_profile_url),
    )


@lru_cache(maxsize=8192)
def _profile_reference(url: str):
    host = (urlsplit(url).hostname or "").removeprefix("www.")
    for platform, parser in _profile_parsers():
        parsed = parser(url)
        if parsed:
            stable_id = None
            if platform == "facebook" and "profile.php" in parsed.canonical_url:
                stable_id = (
                    parse_qs(urlsplit(parsed.canonical_url).query).get("id") or [None]
                )[0]
            return platform, parsed.canonical_url, parsed.handle, stable_id
    if host == "github.com":
        path = urlsplit(url).path.strip("/")
        reserved = {
            "about",
            "apps",
            "collections",
            "contact",
            "enterprise",
            "events",
            "explore",
            "features",
            "issues",
            "join",
            "login",
            "marketplace",
            "new",
            "notifications",
            "orgs",
            "organizations",
            "pricing",
            "pulls",
            "search",
            "security",
            "sessions",
            "settings",
            "site",
            "sponsors",
            "topics",
            "trending",
        }
        if (
            re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})", path)
            and path.casefold() not in reserved
        ):
            return (
                "github",
                "https://github.com/" + path.casefold(),
                path.casefold(),
                None,
            )
    # A known platform's post/login/search route must never fall through as an
    # arbitrary profile on that same platform.
    known = {
        "instagram.com",
        "facebook.com",
        "threads.net",
        "threads.com",
        "tiktok.com",
        "x.com",
        "twitter.com",
        "github.com",
    }
    if any(host == item or host.endswith("." + item) for item in known):
        return False
    return None


def canonical_account(
    account: Mapping[str, Any], *, observed_at: Any = None
) -> Optional[Dict[str, Any]]:
    """Use a platform stable ID or a validated full profile URL, never a handle."""
    platform = str(account.get("platform") or "").strip().casefold()
    platform = _PLATFORM_ALIASES.get(platform, platform)
    stable_id = account.get("stable_id") or account.get("platform_account_id")
    if stable_id is not None:
        if isinstance(stable_id, bool) or not isinstance(stable_id, (str, int)):
            raise ObservationContractError("Stable account ID must be text or integer")
        stable_id = str(stable_id).strip() or None
    url = canonical_source_url(
        account.get("canonical_url") or account.get("profile_url") or account.get("url")
    )
    handle = account.get("handle") or account.get("username")
    if url:
        reference = _profile_reference(url)
        if reference is False:
            url = None
        elif reference:
            detected_platform, url, handle, url_stable_id = reference
            if (
                platform
                and platform
                in {"x", "instagram", "facebook", "tiktok", "threads", "github"}
                and platform != detected_platform
            ):
                raise ObservationContractError(
                    "Account platform conflicts with profile URL"
                )
            platform = detected_platform
            if stable_id and url_stable_id and stable_id != url_stable_id:
                raise ObservationContractError(
                    "Account stable ID conflicts with profile URL"
                )
            stable_id = stable_id or url_stable_id
        elif urlsplit(url).path in {"", "/"}:
            # A provider/site homepage is not a canonical account locator.
            url = None
    if not platform or not (stable_id or url):
        return None
    return {
        "platform": platform,
        "stable_id": stable_id,
        "canonical_url": url,
        "handle": str(handle) if handle else None,
        "observed_at": observed_at,
        "identity_basis": "platform_stable_id" if stable_id else "profile_url",
        "identity_status": "unverified",
    }


def account_identity(observation: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    account = observation.get("account")
    if not isinstance(account, Mapping):
        return None
    account = canonical_account(account, observed_at=observation.get("observed_at"))
    if account is None:
        return None
    case_id, subject_id = str(observation["case_id"]), str(observation["subject_id"])
    # The scope is a hypothesis namespace; subject_id is not a positive binding.
    identity = [
        case_id,
        subject_id,
        account["platform"],
        account["identity_basis"],
        account["stable_id"] or account["canonical_url"],
    ]
    key = "acc:" + fingerprint(identity)
    physical_key = "account:" + fingerprint([identity[0], *identity[2:]])
    return {
        "id": key,
        "key": key,
        "canonical_key": key,
        "kind": "account",
        "case_id": case_id,
        "subject_id": subject_id,
        "physical_account_key": physical_key,
        **account,
    }


def _qualified_value(value: Any) -> Any:
    if isinstance(value, str):
        # NFC/whitespace equivalence is safe for displayed text; no global case,
        # punctuation, accent, phone-country or email-provider guessing.
        return " ".join(unicodedata.normalize("NFC", value).split())
    if isinstance(value, list):
        return [_qualified_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _qualified_value(item) for key, item in value.items()}
    return value


def qualified_claim_identity(
    claim: Mapping[str, Any],
    *,
    case_id: str,
    subject_id: str,
    account_key: Optional[str] = None,
) -> Dict[str, Any]:
    if claim.get("subject_id") and str(claim["subject_id"]) != str(subject_id):
        raise ObservationContractError("Claim subject is outside the observation scope")
    predicate = str(claim.get("predicate") or claim.get("field_name") or "").strip()
    if not predicate or "value" not in claim:
        raise ObservationContractError("A qualified claim requires predicate and value")
    qualifiers = _qualified_value(claim.get("qualifiers") or {})
    if not isinstance(qualifiers, dict):
        raise ObservationContractError("Claim qualifiers must be an object")
    for qualifier in ("role", "organization", "jurisdiction", "precision", "language"):
        if qualifier in claim:
            qualifiers[qualifier] = _qualified_value(claim[qualifier])
    validity = claim.get("validity") or {}
    valid_from = claim.get("valid_from") or validity.get("from")
    valid_to = claim.get("valid_to") or validity.get("to")
    value = _qualified_value(claim["value"])
    binding = claim.get("account_key") or account_key
    if predicate == "social_account" and binding:
        # Username spelling and detector-specific account dictionaries do not
        # create several equivalent account-existence claims. Raw forms remain
        # immutable in the supporting observations; attribution is still open.
        value = {"account_key": binding}
    hypothesis = [
        str(case_id),
        str(subject_id),
        binding,
        predicate,
        qualifiers,
        valid_from,
        valid_to,
    ]
    key = "clm:" + fingerprint([*hypothesis, value])
    return {
        "id": key,
        "key": key,
        "canonical_key": key,
        "kind": "claim",
        "case_id": str(case_id),
        "subject_id": str(subject_id),
        "account_key": binding,
        "predicate": predicate,
        "value": value,
        "qualifiers": qualifiers,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "hypothesis_key": "hyp:" + fingerprint(hypothesis),
    }


class _Origins:
    """Union known copied content and explicit derivation without quadratic edges."""

    def __init__(self):
        self.parent = {}

    def find(self, item):
        self.parent.setdefault(item, item)
        root = item
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[item] != item:
            item, self.parent[item] = self.parent[item], root
        return root

    def union(self, left, right):
        a, b = self.find(left), self.find(right)
        if a != b:
            # Stable representative independent of arrival order.
            self.parent[max(a, b)] = min(a, b)


def consolidate_observations(
    observations: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Group an arbitrary-size iterator, retaining one reference per membership.

    Replayed IDs with different documents fail closed. Copy-origin union uses
    explicit content fingerprints or lineage; an observation payload fingerprint
    is never misused as proof that generic status responses share an origin.
    """
    accounts, claims, seen, origin_by_observation = {}, {}, {}, {}
    origins, by_content, derived = _Origins(), {}, []
    by_origin_url = {}
    urls, single_values = {}, {}
    conflicts = []
    outcome_counts = {}
    ungrouped_observation_ids = []

    def member(collection, identity, observation):
        key = identity["key"]
        if key not in collection:
            collection[key] = {
                **identity,
                "observation_ids": set(),
                "origin_family_ids": set(),
                "unknown_observation_ids": set(),
                "conflicts": set(),
                "observed_times": set(),
                "source_urls": set(),
                "profile_urls": set(),
                "handles": set(),
            }
        group = collection[key]
        group["observation_ids"].add(observation["id"])
        origin = observation.get("origin_family_id")
        if origin:
            group["origin_family_ids"].add(origin)
            origins.find(origin)
        else:
            group["unknown_observation_ids"].add(observation["id"])
        if observation.get("observed_at"):
            group["observed_times"].add(observation["observed_at"])
        if observation.get("source_url"):
            group["source_urls"].add(observation["source_url"])
        if identity.get("handle"):
            group["handles"].add(identity["handle"])
        if identity.get("canonical_url"):
            group["profile_urls"].add(identity["canonical_url"])

    for observation in observations:
        oid = str(observation["id"])
        digest = fingerprint(observation)
        if oid in seen:
            if seen[oid] != digest:
                raise ObservationContractError(
                    "Observation ID replay contains changed evidence"
                )
            continue
        seen[oid] = digest
        status = observation.get("status") or "inconclusive"
        outcome_counts[status] = outcome_counts.get(status, 0) + 1
        origin = observation.get("origin_family_id")
        origin_by_observation[oid] = origin
        origin_url = (observation.get("dependence") or {}).get("origin_url")
        if origin and origin_url:
            if origin_url in by_origin_url:
                origins.union(origin, by_origin_url[origin_url])
            else:
                by_origin_url[origin_url] = origin
        content = observation.get("content_fingerprint")
        if origin and content:
            if content in by_content:
                origins.union(origin, by_content[content])
            else:
                by_content[content] = origin
        if observation.get("derived_from"):
            derived.append((oid, list(observation["derived_from"])))
        account = account_identity(observation)
        if not account and not observation.get("claims"):
            ungrouped_observation_ids.append(oid)
        if account:
            member(accounts, account, observation)
            url = account.get("canonical_url")
            if url:
                locator = (
                    account["case_id"],
                    account["subject_id"],
                    account["platform"],
                    url,
                )
                urls.setdefault(locator, set()).add(account["key"])
        for claim in observation.get("claims") or []:
            identity = qualified_claim_identity(
                claim,
                case_id=observation["case_id"],
                subject_id=observation["subject_id"],
                account_key=account["key"] if account else None,
            )
            member(claims, identity, observation)
            if identity["predicate"] in _SINGLE_VALUE_PREDICATES:
                single_values.setdefault(identity["hypothesis_key"], set()).add(
                    identity["key"]
                )

    # Resolve explicit origin inheritance, including children arriving before
    # parents. Unresolved/multi-origin summaries do not acquire new independence.
    pending = {oid: references for oid, references in derived}
    children = {}
    for oid, references in pending.items():
        for reference in references:
            children.setdefault(reference, []).append(oid)
    queue = list(origin_by_observation)
    cursor = 0
    while cursor < len(queue):
        parent_id = queue[cursor]
        cursor += 1
        parent_origin = origin_by_observation.get(parent_id)
        if not parent_origin:
            continue
        for child_id in children.get(parent_id, []):
            child_origin = origin_by_observation.get(child_id)
            if child_origin:
                origins.union(parent_origin, child_origin)
            elif len(pending[child_id]) == 1:
                origin_by_observation[child_id] = parent_origin
                queue.append(child_id)

    def add_conflict(kind, keys, collection, **detail):
        if len(keys) < 2:
            return
        ids = sorted(keys)
        conflict_id = "conflict:" + fingerprint([kind, ids, detail])
        conflicts.append(
            {
                "id": conflict_id,
                "kind": kind,
                "group_ids": ids,
                "requires_operator_review": True,
                **detail,
            }
        )
        for key in ids:
            collection[key]["conflicts"].add(conflict_id)

    for locator, keys in urls.items():
        stable_keys = {key for key in keys if accounts[key]["stable_id"]}
        if len(stable_keys) > 1:
            add_conflict(
                "profile_url_stable_id_collision",
                keys,
                accounts,
                canonical_url=locator[-1],
                explanation="The same profile URL has different stable IDs; handle reuse or detector conflict needs review.",
            )
        elif stable_keys and len(keys) > 1:
            add_conflict(
                "unresolved_account_continuity",
                keys,
                accounts,
                canonical_url=locator[-1],
                explanation="URL-only observations require a temporal continuity decision before binding to the stable ID.",
            )
    for keys in single_values.values():
        add_conflict("contradictory_qualified_values", keys, claims)

    def finish(group):
        inherited = {
            origin_by_observation[oid]
            for oid in group["unknown_observation_ids"]
            if origin_by_observation.get(oid)
        }
        families = {
            origins.find(origin) for origin in group["origin_family_ids"] | inherited
        }
        unknown_count = sum(
            not origin_by_observation.get(oid)
            for oid in group["unknown_observation_ids"]
        )
        result = {
            key: sorted(value) if isinstance(value, set) else value
            for key, value in group.items()
            if key != "unknown_observation_ids"
        }
        result.update(
            origin_family_ids=sorted(families),
            independent_origin_count=len(families),
            unknown_origin_count=unknown_count,
            observation_count=len(group["observation_ids"]),
            origin_by_observation={
                oid: (
                    origins.find(origin_by_observation[oid])
                    if origin_by_observation.get(oid)
                    else None
                )
                for oid in sorted(group["observation_ids"])
            },
            evidence_set_hash=fingerprint(
                sorted((oid, seen[oid]) for oid in group["observation_ids"])
            ),
        )
        return result

    finished_accounts = [finish(group) for group in accounts.values()]
    case_accounts = {}
    for group in finished_accounts:
        physical_key = group["physical_account_key"]
        if physical_key not in case_accounts:
            case_accounts[physical_key] = {
                "id": physical_key,
                "key": physical_key,
                "kind": "case_account",
                "case_id": group["case_id"],
                "platform": group["platform"],
                "stable_id": group["stable_id"],
                "identity_basis": group["identity_basis"],
                "account_hypothesis_ids": [],
                "subject_ids": set(),
                "observation_ids": set(),
                "profile_urls": set(),
                "handles": set(),
                "origin_family_ids": set(),
                "identity_status": "unverified",
            }
        physical = case_accounts[physical_key]
        physical["account_hypothesis_ids"].append(group["id"])
        physical["subject_ids"].add(group["subject_id"])
        for key in ("observation_ids", "profile_urls", "handles", "origin_family_ids"):
            physical[key].update(group[key])

    return {
        "accounts": finished_accounts,
        "case_accounts": [
            {
                key: sorted(value) if isinstance(value, (set, list)) else value
                for key, value in physical.items()
            }
            for physical in case_accounts.values()
        ],
        "claims": [finish(group) for group in claims.values()],
        "conflicts": conflicts,
        "observation_count": len(seen),
        "outcome_counts": outcome_counts,
        "ungrouped_observation_ids": sorted(ungrouped_observation_ids),
        "evidence_set_hash": fingerprint(sorted(seen.items())),
    }
