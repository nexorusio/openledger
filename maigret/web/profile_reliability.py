"""Conservative reliability controls for username-profile detections.

Maigret answers whether one site detector returned ``CLAIMED``.  That status is
useful raw collection output, but it is not by itself evidence that a public
account exists and never proves that an account belongs to the subject.  This
module keeps those distinctions explicit and centralizes the detector-health
registry consumed by both collection and report generation.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping
from urllib.parse import urlsplit


DETECTOR_HEALTH_SCHEMA_VERSION = 1
DETECTOR_HEALTH_STATES = frozenset(
    {"healthy", "degraded", "quarantined", "untested"}
)
PROFILE_CLASSIFICATIONS = frozenset({"supported", "candidate", "suppressed"})

# Fields populated from the returned profile rather than from the URL template.
# Untested detectors need more than one signal; one identity field is sufficient
# only after the detector has a reviewed healthy canary state. Subject identity
# remains unverified until separately corroborated by the investigation workflow.
PROFILE_IDENTITY_FIELDS = frozenset(
    {
        "bio",
        "description",
        "display_name",
        "fullname",
        "full_name",
        "name",
        "nickname",
        "screen_name",
        "username",
    }
)
STABLE_ACCOUNT_IDENTIFIER_FIELDS = frozenset(
    {
        "facebook_id",
        "facebook_uid",
        "flickr_id",
        "gaia_id",
        "github_id",
        "googleplus_uid",
        "id",
        "instagram_id",
        "instagram_pk",
        "mail_id",
        "mail_uid",
        "patreon_id",
        "pinterest_id",
        "reddit_id",
        "roblox_user_id",
        "sec_uid",
        "steam_id",
        "tiktok_id",
        "twitter_uid",
        "uid",
        "vk_id",
        "yandex_public_id",
        "yandex_uid",
        "yandex_znatoki_id",
        "youtube_channel_id",
    }
)
PROFILE_SUPPORTING_FIELDS = frozenset(
    {
        "avatar",
        "avatar_url",
        "created_at",
        "followers",
        "follower_count",
        "following",
        "following_count",
        "image",
        "image_url",
        "is_private",
        "is_verified",
        "location",
        "photo",
        "photo_url",
        "picture",
        "website",
    }
)

BLOCKED_OR_INTERSTITIAL_MARKERS = (
    "access denied",
    "captcha",
    "challenge required",
    "cloudflare",
    "login required",
    "rate limit",
    "sign in required",
    "temporarily blocked",
    "verify you are human",
)
GENERIC_PAGE_MARKERS = (
    "access denied",
    "create an account",
    "hear the world's sounds",
    "log in",
    "not found",
    "page isn't available",
    "page not found",
    "sign in",
    "something went wrong",
)


class DetectorHealthRegistryError(ValueError):
    """Raised when the reviewed detector-health registry is malformed."""


def empty_detector_health_registry() -> Dict[str, Any]:
    return {
        "schema_version": DETECTOR_HEALTH_SCHEMA_VERSION,
        "generated_at": None,
        "sites": {},
    }


def _bounded_text(value: Any, *, limit: int = 500) -> str:
    if isinstance(value, (str, int, float, bool)):
        return " ".join(str(value).split())[:limit]
    return ""


def _normalized_site_key(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


def _bounded_nonnegative_int(value: Any, *, field_name: str) -> int:
    try:
        number = int(value or 0)
    except (TypeError, ValueError) as error:
        raise DetectorHealthRegistryError(
            f"Invalid detector-health {field_name}"
        ) from error
    return max(0, min(1000, number))


def validate_detector_health_registry(value: Any) -> Dict[str, Any]:
    """Return a bounded, normalized registry or reject the whole document."""
    if not isinstance(value, dict):
        raise DetectorHealthRegistryError("Detector-health registry must be an object")
    if value.get("schema_version") != DETECTOR_HEALTH_SCHEMA_VERSION:
        raise DetectorHealthRegistryError("Unsupported detector-health schema version")
    raw_sites = value.get("sites")
    if not isinstance(raw_sites, dict):
        raise DetectorHealthRegistryError("Detector-health sites must be an object")
    if len(raw_sites) > 10_000:
        raise DetectorHealthRegistryError("Detector-health registry is too large")

    sites: Dict[str, Any] = {}
    for raw_name, raw_entry in raw_sites.items():
        if not isinstance(raw_entry, dict):
            raise DetectorHealthRegistryError("Invalid detector-health site entry")
        name = " ".join(
            str(raw_entry.get("site_name") or raw_name or "").split()
        )[:300]
        if not name:
            raise DetectorHealthRegistryError("Invalid detector-health site entry")
        state = str(raw_entry.get("state") or "untested").casefold()
        if state not in DETECTOR_HEALTH_STATES:
            raise DetectorHealthRegistryError(
                f"Invalid detector-health state for {name}"
            )
        sites[_normalized_site_key(name)] = {
            "site_name": name,
            "state": state,
            "consecutive_failures": _bounded_nonnegative_int(
                raw_entry.get("consecutive_failures"),
                field_name=f"consecutive failures for {name}",
            ),
            "consecutive_successes": _bounded_nonnegative_int(
                raw_entry.get("consecutive_successes"),
                field_name=f"consecutive successes for {name}",
            ),
            "last_checked_at": _bounded_text(raw_entry.get("last_checked_at")),
            "last_outcome": _bounded_text(raw_entry.get("last_outcome"), limit=40),
            "reason": _bounded_text(raw_entry.get("reason")),
        }

    return {
        "schema_version": DETECTOR_HEALTH_SCHEMA_VERSION,
        "generated_at": _bounded_text(value.get("generated_at")),
        "sites": sites,
    }


def load_detector_health_registry(path: str | Path) -> Dict[str, Any]:
    registry_path = Path(path)
    try:
        with registry_path.open(encoding="utf-8") as registry_file:
            value = json.load(registry_file)
    except FileNotFoundError:
        return empty_detector_health_registry()
    except (OSError, json.JSONDecodeError) as error:
        raise DetectorHealthRegistryError(
            "Detector-health registry could not be read"
        ) from error
    return validate_detector_health_registry(value)


def detector_health_for_site(registry: Mapping[str, Any], site_name: Any) -> str:
    sites = registry.get("sites") if isinstance(registry, Mapping) else None
    if not isinstance(sites, Mapping):
        return "untested"
    entry = sites.get(_normalized_site_key(site_name))
    if not isinstance(entry, Mapping):
        return "untested"
    state = str(entry.get("state") or "untested").casefold()
    return state if state in DETECTOR_HEALTH_STATES else "untested"


def evolve_detector_health_registry(
    previous: Mapping[str, Any],
    observations: Mapping[str, Mapping[str, Any]],
    *,
    checked_at: str | None = None,
) -> Dict[str, Any]:
    """Apply conservative canary state transitions to a reviewed registry.

    A semantic detector contradiction must occur in two consecutive canary runs
    before quarantine.  Network blocks and other unknown outcomes degrade a
    detector but do not masquerade as proof of a false positive.  Recovery from
    degraded or quarantined state likewise requires two consecutive clean runs.
    """
    normalized_previous = validate_detector_health_registry(dict(previous))
    timestamp = checked_at or datetime.now(timezone.utc).isoformat()
    updated_sites = dict(normalized_previous["sites"])

    for raw_name, raw_observation in observations.items():
        name = " ".join(str(raw_name or "").split())[:300]
        if not name or not isinstance(raw_observation, Mapping):
            continue
        outcome = str(raw_observation.get("outcome") or "unknown").casefold()
        if outcome not in {"pass", "fail", "unknown"}:
            outcome = "unknown"
        key = _normalized_site_key(name)
        prior = updated_sites.get(
            key,
            {
                "site_name": name,
                "state": "untested",
                "consecutive_failures": 0,
                "consecutive_successes": 0,
                "last_outcome": "",
                "last_checked_at": "",
                "reason": "",
            },
        )
        prior_state = str(prior.get("state") or "untested")
        prior_outcome = str(prior.get("last_outcome") or "")
        failures = int(prior.get("consecutive_failures") or 0)
        successes = int(prior.get("consecutive_successes") or 0)

        if outcome == "fail":
            failures = failures + 1 if prior_outcome == "fail" else 1
            successes = 0
            state = (
                "quarantined"
                if prior_state == "quarantined" or failures >= 2
                else "degraded"
            )
        elif outcome == "pass":
            successes = successes + 1 if prior_outcome == "pass" else 1
            failures = 0
            if prior_state == "quarantined" and successes < 2:
                state = "quarantined"
            elif prior_state == "degraded" and successes < 2:
                state = "degraded"
            else:
                state = "healthy"
        else:
            successes = 0
            state = "quarantined" if prior_state == "quarantined" else "degraded"

        updated_sites[key] = {
            "site_name": name,
            "state": state,
            "consecutive_failures": min(failures, 1000),
            "consecutive_successes": min(successes, 1000),
            "last_checked_at": _bounded_text(timestamp),
            "last_outcome": outcome,
            "reason": _bounded_text(raw_observation.get("reason")),
        }

    return {
        "schema_version": DETECTOR_HEALTH_SCHEMA_VERSION,
        "generated_at": _bounded_text(timestamp),
        "sites": updated_sites,
    }


def serialize_detector_health_registry(registry: Mapping[str, Any]) -> Dict[str, Any]:
    """Convert the case-folded in-memory representation to stable public JSON."""
    normalized = validate_detector_health_registry(dict(registry))
    entries = sorted(
        normalized["sites"].values(),
        key=lambda entry: entry["site_name"].casefold(),
    )
    return {
        "schema_version": DETECTOR_HEALTH_SCHEMA_VERSION,
        "generated_at": normalized.get("generated_at") or None,
        "sites": {
            entry["site_name"]: {
                "state": entry["state"],
                "consecutive_failures": entry["consecutive_failures"],
                "consecutive_successes": entry["consecutive_successes"],
                "last_checked_at": entry["last_checked_at"],
                "last_outcome": entry["last_outcome"],
                "reason": entry["reason"],
            }
            for entry in entries
        },
    }


def _has_public_profile_url(value: Any) -> bool:
    candidate = str(value or "").strip()
    if not candidate or len(candidate) > 2000 or "\\" in candidate:
        return False
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return False
    return bool(
        parsed.scheme.casefold() in {"http", "https"}
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
    )


def _useful_evidence(evidence: Any) -> Dict[str, str]:
    if not isinstance(evidence, Mapping):
        return {}
    useful: Dict[str, str] = {}
    for raw_key, raw_value in list(evidence.items())[:100]:
        key = str(raw_key or "").strip().casefold()
        if not key or key in {"_extractor", "extractor", "links"}:
            continue
        if isinstance(raw_value, (list, tuple, set)):
            text = " ".join(
                _bounded_text(item, limit=250)
                for item in list(raw_value)[:10]
                if _bounded_text(item, limit=250)
            )
        else:
            text = _bounded_text(raw_value, limit=1000)
        if text:
            useful[key] = text
    return useful


def classify_profile_detection(
    *,
    username: Any,
    site_name: Any,
    url: Any,
    evidence: Any,
    check_type: Any,
    health_state: Any = "untested",
    status_context: Any = "",
    status_error: Any = "",
) -> Dict[str, Any]:
    """Triage one raw ``CLAIMED`` result without asserting subject identity."""
    investigated_username = "".join(str(username or "").split()).lstrip("@").casefold()
    del site_name  # Retained for a stable call contract and future site policies.
    health = str(health_state or "untested").casefold()
    if health not in DETECTOR_HEALTH_STATES:
        health = "untested"
    method = str(check_type or "unknown").casefold()
    useful = _useful_evidence(evidence)
    keys = set(useful)
    context = " ".join(
        value
        for value in (
            _bounded_text(status_context, limit=500),
            _bounded_text(status_error, limit=500),
        )
        if value
    ).casefold()

    if health == "quarantined":
        return {
            "classification": "suppressed",
            "detection_confidence": "unreliable",
            "identity_status": "unverified",
            "health_state": health,
            "reason": "Detector is quarantined after repeated canary failures.",
            "signals": [],
        }
    if context and any(marker in context for marker in BLOCKED_OR_INTERSTITIAL_MARKERS):
        return {
            "classification": "suppressed",
            "detection_confidence": "unreliable",
            "identity_status": "unverified",
            "health_state": health,
            "reason": "The response was a block, challenge, login, or rate-limit page.",
            "signals": [],
        }
    if not _has_public_profile_url(url):
        return {
            "classification": "suppressed",
            "detection_confidence": "unreliable",
            "identity_status": "unverified",
            "health_state": health,
            "reason": "The detector did not return a safe public profile URL.",
            "signals": [],
        }

    reflected_username_keys = {
        key
        for key in keys.intersection({"screen_name", "username"})
        if "".join(useful[key].split()).lstrip("@").casefold()
        == investigated_username
    }
    identity_keys = sorted(
        keys.intersection(PROFILE_IDENTITY_FIELDS).difference(
            reflected_username_keys
        )
    )
    stable_id_keys = sorted(keys.intersection(STABLE_ACCOUNT_IDENTIFIER_FIELDS))
    supporting_keys = sorted(keys.intersection(PROFILE_SUPPORTING_FIELDS))
    generic_values = [
        value.casefold()
        for key, value in useful.items()
        if key in PROFILE_IDENTITY_FIELDS
    ]
    looks_generic = any(
        any(marker in value for marker in GENERIC_PAGE_MARKERS)
        for value in generic_values
    )
    signals = identity_keys + stable_id_keys + supporting_keys

    if looks_generic and not stable_id_keys:
        return {
            "classification": "suppressed",
            "detection_confidence": "unreliable",
            "identity_status": "unverified",
            "health_state": health,
            "reason": "Extracted metadata describes a generic or missing-page shell.",
            "signals": signals,
        }
    if health == "degraded":
        return {
            "classification": "candidate",
            "detection_confidence": "weak",
            "identity_status": "unverified",
            "health_state": health,
            "reason": "Detector is degraded and requires corroboration.",
            "signals": signals,
        }

    profile_specific = bool(stable_id_keys) or (
        bool(identity_keys) and len(signals) >= 2
    ) or (health == "healthy" and bool(identity_keys))
    if profile_specific:
        confidence = (
            "strong" if stable_id_keys or len(identity_keys) >= 2 else "moderate"
        )
        return {
            "classification": "supported",
            "detection_confidence": confidence,
            "identity_status": "unverified",
            "health_state": health,
            "reason": "Returned content contains profile-specific account evidence.",
            "signals": signals,
        }

    reason = (
        "Detector returned CLAIMED without profile-specific response evidence."
        if method in {"message", "status_code", "response_url"}
        else "Account existence requires independent corroboration."
    )
    return {
        "classification": "candidate",
        "detection_confidence": "weak",
        "identity_status": "unverified",
        "health_state": health,
        "reason": reason,
        "signals": signals,
    }
