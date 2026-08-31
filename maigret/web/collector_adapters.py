"""Isolated collector adapters and OpenLedger observation normalization."""

from __future__ import annotations

import asyncio
import importlib.util
import ipaddress
import json
import os
import re
import sys
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import aiohttp

from maigret.result import MaigretCheckStatus
from maigret.web.persona_intelligence import (
    claim_fingerprint,
    evidence_fingerprint,
)

USER_SCANNER_ENGINE = "user_scanner_email"
USER_SCANNER_TIMEOUT_SECONDS = 420
MAX_COLLECTOR_OUTPUT_BYTES = 8_000_000
MAX_OBSERVATIONS = 600

GITHUB_ENGINE = "github_public_profile"
GITHUB_API_VERSION = "2026-03-10"
GITHUB_API_BASE_URL = "https://api.github.com"
GITHUB_TIMEOUT_SECONDS = 15
GITHUB_MAX_RESPONSE_BYTES = 1_000_000
MAX_GITHUB_PROFILES_PER_JOB = 20

_GITHUB_LOGIN_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$")
_X_USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,15}$")


def user_scanner_available() -> bool:
    """Check availability without importing User Scanner into the web process."""
    return importlib.util.find_spec("user_scanner") is not None


async def _stop_subprocess(
    process: asyncio.subprocess.Process, communicate_task: asyncio.Task
) -> None:
    communicate_task.cancel()
    await asyncio.gather(communicate_task, return_exceptions=True)
    if process.returncode is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()


def user_scanner_email_targets(plan: Any) -> List[str]:
    """Return explicitly enabled email targets that can bind to one Persona."""
    if not isinstance(plan, dict):
        return []
    if not plan.get("enable_user_scanner_email"):
        return []
    if plan.get("processing_mode") != "same_subject":
        return []
    targets: List[str] = []
    for identifier in list(plan.get("identifiers") or []):
        if not isinstance(identifier, dict) or identifier.get("type") != "email":
            continue
        value = str(identifier.get("value") or "").strip().casefold()
        if value and value not in targets:
            targets.append(value)
    return targets[:1]


def _safe_public_url(value: Any) -> str:
    candidate = str(value or "").strip()
    if (
        not candidate
        or len(candidate) > 2000
        or "\\" in candidate
        or any(ord(character) < 32 for character in candidate)
    ):
        return ""
    try:
        parsed = urlparse(candidate)
    except ValueError:
        return ""
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        return ""
    hostname = parsed.hostname.casefold().rstrip(".")
    if hostname == "localhost" or hostname.endswith(
        (".localhost", ".local", ".internal", ".lan")
    ):
        return ""
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        if not address.is_global or address.is_multicast or address.is_reserved:
            return ""
    return candidate


def _bounded_mapping(value: Any, *, limit: int = 40) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    output: Dict[str, Any] = {}
    for raw_key, raw_value in list(value.items())[:limit]:
        key = str(raw_key).strip()[:100]
        if not key or isinstance(raw_value, (dict, list, tuple, set)):
            continue
        if isinstance(raw_value, (str, int, float, bool)):
            output[key] = (
                str(raw_value)[:2000] if isinstance(raw_value, str) else raw_value
            )
    return output


def _bounded_text(value: Any, *, limit: int) -> str:
    return " ".join(str(value or "").split())[:limit]


def _github_profile_login(value: Any) -> str:
    """Return a login only for an exact public github.com account URL."""
    candidate = _safe_public_url(value)
    if not candidate:
        return ""
    parsed = urlparse(candidate)
    if parsed.hostname.casefold().rstrip(".") not in {"github.com", "www.github.com"}:
        return ""
    try:
        port = parsed.port
    except ValueError:
        return ""
    if port not in {None, 80, 443} or parsed.query or parsed.fragment:
        return ""
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 1 or not _GITHUB_LOGIN_PATTERN.fullmatch(parts[0]):
        return ""
    return parts[0]


def github_profile_targets(
    general_results: Iterable[Any], plan: Any
) -> List[Dict[str, str]]:
    """Select only GitHub accounts already claimed by native discovery."""
    if not isinstance(plan, dict) or not plan.get("enable_github_profile_enrichment"):
        return []
    targets: List[Dict[str, str]] = []
    seen = set()
    for item in general_results:
        if not isinstance(item, (list, tuple)) or len(item) != 3:
            continue
        investigated_username, _identifier_type, results = item
        if not isinstance(results, dict):
            continue
        for site_name, site_data in results.items():
            if str(site_name).strip().casefold() != "github" or not isinstance(
                site_data, dict
            ):
                continue
            status = site_data.get("status")
            if getattr(status, "status", None) != MaigretCheckStatus.CLAIMED:
                continue
            login = _github_profile_login(site_data.get("url_user"))
            if not login or login.casefold() in seen:
                continue
            seen.add(login.casefold())
            targets.append(
                {
                    "investigated_username": str(investigated_username).strip()[:500],
                    "github_login": login,
                    "profile_url": f"https://github.com/{login}",
                }
            )
            if len(targets) >= MAX_GITHUB_PROFILES_PER_JOB:
                return targets
    return targets


def _github_diagnostic_observation(
    target: Dict[str, str],
    *,
    status: str,
    reason: str,
    rate_limit_remaining: str = "",
    rate_limit_reset: str = "",
) -> Dict[str, Any]:
    extra = {"api_version": GITHUB_API_VERSION}
    if rate_limit_remaining:
        extra["rate_limit_remaining"] = rate_limit_remaining[:20]
    if rate_limit_reset:
        extra["rate_limit_reset"] = rate_limit_reset[:40]
    return {
        "source_engine": GITHUB_ENGINE,
        "subject_type": "username",
        "subject_value": target["investigated_username"],
        "status": status,
        "site_name": "GitHub",
        "category": "developer",
        "source_url": target["profile_url"],
        "reason": reason[:1000],
        "extra": extra,
        "media": {},
    }


def normalize_github_public_profile(
    target: Dict[str, str], raw_profile: Any
) -> Dict[str, Any]:
    """Validate and minimize one native GitHub public-user response."""
    if not isinstance(raw_profile, dict):
        raise ValueError("GitHub returned a non-object profile")
    github_id = raw_profile.get("id")
    if isinstance(github_id, bool) or not isinstance(github_id, int) or github_id <= 0:
        raise ValueError("GitHub profile did not contain a valid numeric account ID")
    login = _bounded_text(raw_profile.get("login"), limit=39)
    if (
        not _GITHUB_LOGIN_PATTERN.fullmatch(login)
        or login.casefold() != target["github_login"].casefold()
    ):
        raise ValueError("GitHub returned a profile for an unexpected account")
    account_type = _bounded_text(raw_profile.get("type"), limit=40)
    if account_type != "User":
        observation = _github_diagnostic_observation(
            target,
            status="unsupported_account_type",
            reason=f"GitHub returned account type {account_type or 'unknown'}, not User.",
        )
        observation["source_record_id"] = f"github-account:{github_id}"
        observation["extra"].update(
            {"github_id": github_id, "login": login, "account_type": account_type}
        )
        return observation

    api_profile_url = _github_profile_login(raw_profile.get("html_url"))
    if not api_profile_url or api_profile_url.casefold() != login.casefold():
        raise ValueError("GitHub returned an invalid public profile URL")

    extra: Dict[str, Any] = {
        "api_version": GITHUB_API_VERSION,
        "github_id": github_id,
        "login": login,
        "account_type": account_type,
    }
    for key, limit in (
        ("name", 300),
        ("company", 500),
        ("location", 300),
        ("bio", 1200),
        ("created_at", 100),
        ("updated_at", 100),
    ):
        value = _bounded_text(raw_profile.get(key), limit=limit)
        if value:
            extra[key] = value
    blog = _safe_public_url(raw_profile.get("blog"))
    if blog:
        extra["blog"] = blog
    twitter_username = _bounded_text(raw_profile.get("twitter_username"), limit=15)
    if _X_USERNAME_PATTERN.fullmatch(twitter_username):
        extra["twitter_username"] = twitter_username
    for key in ("followers", "following", "public_repos", "public_gists"):
        value = raw_profile.get(key)
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and 0 <= value <= 10**15
        ):
            extra[key] = value
    avatar_url = _safe_public_url(raw_profile.get("avatar_url"))
    return {
        "source_engine": GITHUB_ENGINE,
        "subject_type": "username",
        "subject_value": target["investigated_username"],
        "status": "observed",
        "site_name": "GitHub",
        "category": "developer",
        "source_url": target["profile_url"],
        "source_record_id": f"github-user:{github_id}",
        "reason": "Public GitHub user profile enriched after native account discovery.",
        "extra": extra,
        "media": {"avatar": avatar_url} if avatar_url else {},
    }


async def run_github_public_profile(
    target: Dict[str, str],
    *,
    timeout_seconds: int = GITHUB_TIMEOUT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Fetch one fixed-origin public GitHub user record without credentials."""
    login = _github_profile_login(target.get("profile_url"))
    if (
        not login
        or login.casefold() != str(target.get("github_login") or "").casefold()
    ):
        raise ValueError("GitHub enrichment target is invalid")
    session_factory = session_factory or aiohttp.ClientSession
    timeout = aiohttp.ClientTimeout(total=max(1, min(int(timeout_seconds), 30)))
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": "OpenLedger-OSINT-Enrichment",
    }
    async with session_factory(timeout=timeout, headers=headers) as session:
        async with session.get(
            f"{GITHUB_API_BASE_URL}/users/{login}", allow_redirects=False
        ) as response:
            remaining = str(response.headers.get("X-RateLimit-Remaining") or "")
            reset = str(response.headers.get("X-RateLimit-Reset") or "")
            if response.status in {403, 429}:
                return _github_diagnostic_observation(
                    target,
                    status="rate_limited",
                    reason="GitHub public API rate limit was reached; enrichment was skipped.",
                    rate_limit_remaining=remaining,
                    rate_limit_reset=reset,
                )
            if response.status == 404:
                return _github_diagnostic_observation(
                    target,
                    status="not_found",
                    reason="The claimed GitHub profile was unavailable from the public API.",
                    rate_limit_remaining=remaining,
                    rate_limit_reset=reset,
                )
            if response.status != 200:
                raise RuntimeError(
                    f"GitHub public API returned HTTP {int(response.status)}"
                )
            content_length = response.headers.get("Content-Length")
            if content_length:
                try:
                    if int(content_length) > GITHUB_MAX_RESPONSE_BYTES:
                        raise RuntimeError("GitHub returned an oversized profile")
                except ValueError:
                    pass
            body = await response.content.read(GITHUB_MAX_RESPONSE_BYTES + 1)
            if len(body) > GITHUB_MAX_RESPONSE_BYTES:
                raise RuntimeError("GitHub returned an oversized profile")
    try:
        raw_profile = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("GitHub returned invalid JSON") from exc
    observation = normalize_github_public_profile(target, raw_profile)
    observation["extra"].update(
        {
            "rate_limit_remaining": remaining[:20],
            "rate_limit_reset": reset[:40],
        }
    )
    return observation


def _github_claim(
    *,
    field_name: str,
    value: Any,
    confidence: int,
    observation: Dict[str, Any],
    evidence_type: str,
    evidence_details: Optional[Dict[str, Any]] = None,
    fingerprint_value: Any = None,
) -> Dict[str, Any]:
    source_url = _safe_public_url(observation.get("source_url"))
    username = str(observation.get("subject_value") or "").strip()[:500]
    details: Dict[str, Any] = {
        "investigated_username": username,
        "human_review_required": True,
        "github_api_version": GITHUB_API_VERSION,
    }
    if evidence_details:
        details.update(evidence_details)
    evidence = {
        "evidence_type": evidence_type,
        "source_name": "GitHub",
        "source_url": source_url,
        "details": details,
    }
    if isinstance(value, dict):
        display_value = str(
            value.get("url") or value.get("identifier") or value.get("value") or ""
        ).strip()[:4000]
        normalized_value = json.dumps(value, sort_keys=True, ensure_ascii=False)[:4000]
    else:
        display_value = str(value).strip()[:4000]
        normalized_value = " ".join(str(value).split()).casefold()[:4000]
    return {
        "field_name": field_name,
        "value": value,
        "display_value": display_value,
        "normalized_value": normalized_value,
        "confidence": max(0, min(100, int(confidence))),
        "fingerprint": claim_fingerprint(
            field_name, value if fingerprint_value is None else fingerprint_value
        ),
        "source_engine": GITHUB_ENGINE,
        "source_record_id": str(observation.get("source_record_id") or "")[:500],
        "native_status": "observed",
        "observation_details": {
            "github_id": details.get("github_id"),
            "login": details.get("login"),
            "account_metadata": details.get("account_metadata", {}),
        },
        "evidence": [dict(evidence, fingerprint=evidence_fingerprint(evidence))],
    }


def extract_github_profile_claims(
    observations: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Convert bounded GitHub user observations into pending claim inputs."""
    candidates: List[Dict[str, Any]] = []
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        if observation.get("source_engine") != GITHUB_ENGINE:
            continue
        if str(observation.get("status") or "").casefold() != "observed":
            continue
        extra = observation.get("extra")
        if not isinstance(extra, dict) or extra.get("account_type") != "User":
            continue
        username = str(observation.get("subject_value") or "").strip()[:500]
        login = _bounded_text(extra.get("login"), limit=39)
        github_id = extra.get("github_id")
        source_url = _safe_public_url(observation.get("source_url"))
        if (
            not username
            or not source_url
            or not _GITHUB_LOGIN_PATTERN.fullmatch(login)
            or isinstance(github_id, bool)
            or not isinstance(github_id, int)
            or github_id <= 0
        ):
            continue
        metadata = {
            key: extra[key]
            for key in (
                "created_at",
                "updated_at",
                "followers",
                "following",
                "public_repos",
                "public_gists",
            )
            if key in extra
        }
        common_details = {
            "github_id": github_id,
            "login": login,
            "account_metadata": metadata,
            "profile_data_is_user_asserted": True,
        }
        account_value = {
            "platform": "GitHub",
            "url": source_url,
            "username": username,
        }
        candidates.append(
            _github_claim(
                field_name="social_account",
                value=account_value,
                confidence=80,
                observation=observation,
                evidence_type="github_public_profile",
                evidence_details=common_details,
            )
        )
        identifier_value = {
            "platform": "GitHub",
            "identifier_type": "github_id",
            "identifier": str(github_id),
        }
        candidates.append(
            _github_claim(
                field_name="platform_identifier",
                value=identifier_value,
                confidence=85,
                observation=observation,
                evidence_type="github_platform_identifier",
                evidence_details={
                    **common_details,
                    "identifier_type": "github_id",
                    "account_continuity_only": True,
                },
                fingerprint_value={
                    "platform": "github",
                    "identifier_type": "github_id",
                    "identifier": str(github_id),
                },
            )
        )
        for source_key, field_name, confidence in (
            ("name", "full_name", 80),
            ("company", "company", 70),
            ("location", "current_location", 65),
            ("bio", "summary", 70),
        ):
            value = _bounded_text(extra.get(source_key), limit=1200)
            if value:
                candidates.append(
                    _github_claim(
                        field_name=field_name,
                        value=value,
                        confidence=confidence,
                        observation=observation,
                        evidence_type="github_profile_field",
                        evidence_details={
                            **common_details,
                            "github_field": source_key,
                        },
                    )
                )
        blog = _safe_public_url(extra.get("blog"))
        if blog:
            candidates.append(
                _github_claim(
                    field_name="website",
                    value=blog,
                    confidence=70,
                    observation=observation,
                    evidence_type="github_profile_field",
                    evidence_details={**common_details, "github_field": "blog"},
                )
            )
        media = observation.get("media")
        if not isinstance(media, dict):
            media = {}
        avatar = _safe_public_url(media.get("avatar"))
        if avatar:
            candidates.append(
                _github_claim(
                    field_name="photograph",
                    value=avatar,
                    confidence=65,
                    observation=observation,
                    evidence_type="github_profile_field",
                    evidence_details={
                        **common_details,
                        "github_field": "avatar_url",
                    },
                )
            )
        twitter_username = _bounded_text(extra.get("twitter_username"), limit=15)
        if _X_USERNAME_PATTERN.fullmatch(twitter_username):
            candidates.append(
                _github_claim(
                    field_name="linked_profile_lead",
                    value=f"https://x.com/{twitter_username}",
                    confidence=50,
                    observation=observation,
                    evidence_type="linked_profile_lead",
                    evidence_details={
                        **common_details,
                        "github_field": "twitter_username",
                        "lead_status": "unverified",
                    },
                )
            )
    return candidates


def normalize_user_scanner_results(
    target_email: str, raw_results: Any
) -> List[Dict[str, Any]]:
    """Validate and bound native User Scanner results before persistence."""
    if not isinstance(raw_results, list):
        raise ValueError("User Scanner returned a non-list result")
    observations: List[Dict[str, Any]] = []
    for raw in raw_results[:MAX_OBSERVATIONS]:
        if not isinstance(raw, dict):
            continue
        status = str(raw.get("status") or "Error").strip()[:40]
        site_name = str(raw.get("site_name") or "Unknown source").strip()[:300]
        if not site_name:
            site_name = "Unknown source"
        observations.append(
            {
                "source_engine": USER_SCANNER_ENGINE,
                "subject_type": "email",
                "subject_value": target_email[:254],
                "status": status,
                "site_name": site_name,
                "category": str(raw.get("category") or "").strip()[:100],
                "source_url": _safe_public_url(raw.get("url")),
                "reason": str(raw.get("reason") or "").strip()[:1000],
                "extra": _bounded_mapping(raw.get("extra")),
                "media": {
                    key: url
                    for key, value in _bounded_mapping(
                        raw.get("media"), limit=12
                    ).items()
                    if (url := _safe_public_url(value))
                },
            }
        )
    return observations


async def run_user_scanner_email(
    target_email: str,
    *,
    timeout_seconds: int = USER_SCANNER_TIMEOUT_SECONDS,
    cancellation_check: Optional[Callable[[], bool]] = None,
) -> List[Dict[str, Any]]:
    """Run User Scanner outside the worker process and parse its JSON envelope."""
    if not user_scanner_available():
        raise RuntimeError("User Scanner is not installed in the worker image")
    if cancellation_check and cancellation_check():
        raise asyncio.CancelledError

    environment = dict(os.environ)
    environment.update(NO_COLOR="1", TERM="dumb")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "maigret.web.user_scanner_runner",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )
    request_payload = json.dumps({"email": target_email}).encode("utf-8")
    communicate_task = asyncio.create_task(process.communicate(request_payload))
    started_at = asyncio.get_running_loop().time()
    try:
        while True:
            done, _ = await asyncio.wait({communicate_task}, timeout=0.5)
            if done:
                stdout, stderr = communicate_task.result()
                break
            if cancellation_check and cancellation_check():
                raise asyncio.CancelledError
            if asyncio.get_running_loop().time() - started_at > timeout_seconds:
                raise asyncio.TimeoutError
    except asyncio.CancelledError:
        await _stop_subprocess(process, communicate_task)
        raise
    except asyncio.TimeoutError:
        await _stop_subprocess(process, communicate_task)
        raise RuntimeError("User Scanner exceeded its collection timeout") from None

    if process.returncode != 0:
        diagnostic = stderr.decode("utf-8", errors="replace").strip()[-2000:]
        raise RuntimeError(f"User Scanner failed: {diagnostic or 'unknown error'}")
    if len(stdout) > MAX_COLLECTOR_OUTPUT_BYTES:
        raise RuntimeError("User Scanner returned an oversized result")
    try:
        envelope = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("User Scanner returned invalid JSON") from exc
    if not isinstance(envelope, dict) or envelope.get("schema_version") != 1:
        raise RuntimeError("User Scanner returned an unsupported result schema")
    return normalize_user_scanner_results(target_email, envelope.get("results"))


def extract_user_scanner_claims(
    observations: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Convert positive email-registration observations into pending claims."""
    candidates: List[Dict[str, Any]] = []
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        if observation.get("source_engine") != USER_SCANNER_ENGINE:
            continue
        if str(observation.get("status") or "").casefold() != "registered":
            continue
        email = str(observation.get("subject_value") or "").strip().casefold()
        site_name = str(observation.get("site_name") or "Unknown source").strip()[:300]
        if not email or not site_name:
            continue
        source_url = _safe_public_url(observation.get("source_url"))
        value = {
            "platform": site_name,
            "email": email,
            "url": source_url,
        }
        normalized_value = f"{site_name.casefold()}\0{email}"
        fingerprint = claim_fingerprint("account_registration", value)
        evidence = {
            "evidence_type": "email_registration_probe",
            "source_name": site_name,
            "source_url": source_url,
            "details": {
                "subject_type": "email",
                "subject_value": email,
                "native_status": "Registered",
                "category": str(observation.get("category") or "")[:100],
                "extra": _bounded_mapping(observation.get("extra")),
                "media": _bounded_mapping(observation.get("media"), limit=12),
            },
        }
        candidates.append(
            {
                "field_name": "account_registration",
                "value": value,
                "display_value": f"{site_name}: {email}"[:4000],
                "normalized_value": normalized_value[:4000],
                # The probe supports registration, not ownership by the Persona.
                "confidence": 55,
                "fingerprint": fingerprint,
                "source_engine": USER_SCANNER_ENGINE,
                # User Scanner does not expose a native record identifier, so use
                # the stable claim fingerprint instead of retaining another copy
                # of the email address in the lineage index.
                "source_record_id": f"{USER_SCANNER_ENGINE}:{fingerprint}",
                "native_status": "registered",
                "evidence": [
                    dict(evidence, fingerprint=evidence_fingerprint(evidence))
                ],
            }
        )
    return candidates
