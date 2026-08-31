"""Isolated collector adapters and OpenLedger observation normalization."""

from __future__ import annotations

import asyncio
import importlib.util
import ipaddress
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, urlparse

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

UNFURL_ENGINE = "unfurl_url_analysis"
UNFURL_VERSION = "20260405"
UNFURL_PINNED_COMMIT = "a21ef7ce1896bd8db17aeeb990911877ab839dbe"
UNFURL_TIMEOUT_SECONDS = 20
UNFURL_MAX_OUTPUT_BYTES = 1_000_000
UNFURL_MAX_NODES = 80
UNFURL_PYTHON_EXECUTABLE = os.getenv(
    "OPENLEDGER_UNFURL_PYTHON", "/opt/openledger-unfurl/bin/python"
)
UNFURL_RUNNER_PATH = os.path.join(os.path.dirname(__file__), "unfurl_runner.py")

WAYBACK_ENGINE = "wayback_cdx"
WAYBACK_API_BASE_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_TIMEOUT_SECONDS = 20
WAYBACK_MAX_RESPONSE_BYTES = 1_000_000
WAYBACK_MAX_CAPTURES = 10
MAX_PROFILE_URL_EVIDENCE_TARGETS = 20

_GITHUB_LOGIN_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$")
_X_USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,15}$")
_SENSITIVE_URL_KEY_PATTERN = re.compile(
    r"(?:^|[_-])(?:access|api|auth|bearer|credential|jwt|key|pass|password|secret|"
    r"session|signature|signed|token)(?:$|[_-])",
    re.IGNORECASE,
)
_SENSITIVE_COMPACT_URL_KEYS = frozenset(
    {
        "accesstoken",
        "apikey",
        "authtoken",
        "bearertoken",
        "clientsecret",
        "credential",
        "jwt",
        "passphrase",
        "passwd",
        "password",
        "secret",
        "sessionid",
        "signature",
        "token",
    }
)
_UNFURL_DATA_TYPE_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
_CDX_TIMESTAMP_PATTERN = re.compile(r"^\d{14}$")
_CDX_DIGEST_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_WAYBACK_FIELDS = ["timestamp", "original", "statuscode", "mimetype", "digest"]


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


def _is_sensitive_url_key(value: Any) -> bool:
    key = str(value or "")
    compact = re.sub(r"[^a-z0-9]", "", key.casefold())
    return bool(
        _SENSITIVE_URL_KEY_PATTERN.search(key)
        or compact in _SENSITIVE_COMPACT_URL_KEYS
    )


def _url_has_sensitive_query_key(value: str) -> bool:
    try:
        parsed = urlparse(value)
        return any(
            _is_sensitive_url_key(key)
            for key, _item in parse_qsl(parsed.query, keep_blank_values=True)
        )
    except (TypeError, ValueError):
        return True


def claimed_profile_url_targets(
    general_results: Iterable[Any], plan: Any
) -> List[Dict[str, str]]:
    """Select exact public URLs already reported as claimed by native discovery."""
    if not isinstance(plan, dict) or not plan.get("enable_archived_url_evidence"):
        return []
    targets: List[Dict[str, str]] = []
    seen = set()
    for item in general_results:
        if not isinstance(item, (list, tuple)) or len(item) != 3:
            continue
        investigated_username, _identifier_type, results = item
        if not isinstance(results, dict):
            continue
        username = str(investigated_username or "").strip()[:500]
        if not username:
            continue
        for site_name, site_data in results.items():
            if not isinstance(site_data, dict):
                continue
            status = site_data.get("status")
            if getattr(status, "status", None) != MaigretCheckStatus.CLAIMED:
                continue
            profile_url = _safe_public_url(site_data.get("url_user"))
            if not profile_url:
                continue
            try:
                parsed = urlparse(profile_url)
                port = parsed.port
            except ValueError:
                continue
            # Fragments never reach an HTTP archive. Secret-shaped parameters are
            # excluded from both the archive query and deterministic decomposition.
            if parsed.fragment or port not in {None, 80, 443}:
                continue
            if _url_has_sensitive_query_key(profile_url):
                continue
            identity = profile_url.casefold()
            if identity in seen:
                continue
            seen.add(identity)
            targets.append(
                {
                    "investigated_username": username,
                    "site_name": _bounded_text(site_name, limit=300) or "Public source",
                    "profile_url": profile_url,
                }
            )
            if len(targets) >= MAX_PROFILE_URL_EVIDENCE_TARGETS:
                return targets
    return targets


def _redact_unfurl_node(raw_node: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_node, dict):
        return None
    data_type = _bounded_text(raw_node.get("data_type"), limit=100)
    if not _UNFURL_DATA_TYPE_PATTERN.fullmatch(data_type):
        return None
    node_id = raw_node.get("id")
    if isinstance(node_id, bool) or not isinstance(node_id, int) or node_id <= 0:
        return None
    key_value = raw_node.get("key")
    key = _bounded_text(key_value, limit=100) if key_value is not None else ""
    raw_value = raw_node.get("value")
    if not isinstance(raw_value, (str, int, float, bool)) and raw_value is not None:
        return None
    value = _bounded_text(raw_value, limit=1000)
    if _is_sensitive_url_key(key):
        value = "[redacted]"
    elif data_type == "url.query":
        try:
            names = [
                _bounded_text(name, limit=100)
                for name, _item in parse_qsl(value, keep_blank_values=True)
            ]
        except ValueError:
            names = []
        value = "query keys: " + ", ".join(name for name in names if name)
    parent_id = raw_node.get("parent_id")
    if isinstance(parent_id, list):
        parent_id = [
            item
            for item in parent_id[:8]
            if isinstance(item, int) and not isinstance(item, bool) and item > 0
        ]
    elif not (
        isinstance(parent_id, int) and not isinstance(parent_id, bool) and parent_id > 0
    ):
        parent_id = None
    return {
        "id": node_id,
        "data_type": data_type,
        "key": key or None,
        "value": value,
        "parent_id": parent_id,
    }


def normalize_unfurl_url_analysis(
    target: Dict[str, str], raw_envelope: Any
) -> Dict[str, Any]:
    """Validate a bounded offline Unfurl envelope for case evidence storage."""
    profile_url = _safe_public_url(target.get("profile_url"))
    if not profile_url or _url_has_sensitive_query_key(profile_url):
        raise ValueError("Unfurl target is invalid")
    if not isinstance(raw_envelope, dict):
        raise ValueError("Unfurl returned a non-object result")
    if (
        raw_envelope.get("schema_version") != 1
        or raw_envelope.get("engine") != "dfir-unfurl"
        or raw_envelope.get("version") != UNFURL_VERSION
        or raw_envelope.get("remote_lookups") is not False
    ):
        raise ValueError("Unfurl returned an unsupported or unsafe result contract")
    raw_nodes = raw_envelope.get("nodes")
    if not isinstance(raw_nodes, list):
        raise ValueError("Unfurl returned invalid nodes")
    nodes: List[Dict[str, Any]] = []
    for raw_node in raw_nodes[:UNFURL_MAX_NODES]:
        node = _redact_unfurl_node(raw_node)
        if node:
            nodes.append(node)
    if not nodes:
        raise ValueError("Unfurl returned no valid URL-analysis nodes")
    return {
        "source_engine": UNFURL_ENGINE,
        "subject_type": "username",
        "subject_value": _bounded_text(target.get("investigated_username"), limit=500),
        "status": "analyzed",
        "site_name": _bounded_text(target.get("site_name"), limit=300)
        or "Public source",
        "category": "url_analysis",
        "source_url": profile_url,
        "source_record_id": f"unfurl:{evidence_fingerprint({'url': profile_url})}",
        "reason": (
            "Offline deterministic URL decomposition; it does not establish account "
            "ownership or independently verify the profile."
        ),
        "extra": {
            "unfurl_version": UNFURL_VERSION,
            "unfurl_commit": UNFURL_PINNED_COMMIT,
            "remote_lookups": False,
            "node_count": len(nodes),
            "nodes": nodes,
            "structural_analysis_only": True,
            "human_review_required": True,
        },
        "media": {},
    }


async def run_unfurl_url_analysis(
    target: Dict[str, str],
    *,
    timeout_seconds: int = UNFURL_TIMEOUT_SECONDS,
    python_executable: str = UNFURL_PYTHON_EXECUTABLE,
) -> Dict[str, Any]:
    """Run pinned Unfurl offline in its dependency-isolated subprocess."""
    profile_url = _safe_public_url(target.get("profile_url"))
    if not profile_url or _url_has_sensitive_query_key(profile_url):
        raise ValueError("Unfurl target is invalid")
    environment = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": os.path.dirname(python_executable),
        "UNFURL_REMOTE_LOOKUPS": "0",
    }
    process = await asyncio.create_subprocess_exec(
        python_executable,
        UNFURL_RUNNER_PATH,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )
    request_payload = json.dumps(
        {"url": profile_url, "node_limit": UNFURL_MAX_NODES}
    ).encode("utf-8")
    communicate_task = asyncio.create_task(process.communicate(request_payload))
    try:
        stdout, stderr = await asyncio.wait_for(
            communicate_task, timeout=max(1, min(int(timeout_seconds), 60))
        )
    except asyncio.TimeoutError:
        await _stop_subprocess(process, communicate_task)
        raise RuntimeError("Unfurl exceeded its analysis timeout") from None
    except asyncio.CancelledError:
        await _stop_subprocess(process, communicate_task)
        raise
    if process.returncode != 0:
        diagnostic = stderr.decode("utf-8", errors="replace").strip()[-2000:]
        raise RuntimeError(f"Unfurl failed: {diagnostic or 'unknown error'}")
    if len(stdout) > UNFURL_MAX_OUTPUT_BYTES:
        raise RuntimeError("Unfurl returned an oversized result")
    try:
        envelope = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Unfurl returned invalid JSON") from exc
    return normalize_unfurl_url_analysis(target, envelope)


def _archive_url_identity(value: Any) -> Optional[tuple]:
    candidate = _safe_public_url(value)
    if not candidate:
        return None
    try:
        parsed = urlparse(candidate)
        port = parsed.port
    except ValueError:
        return None
    if port not in {None, 80, 443} or parsed.fragment:
        return None
    effective_port = port or (443 if parsed.scheme.casefold() == "https" else 80)
    return (
        parsed.hostname.casefold().rstrip("."),
        effective_port,
        parsed.path or "/",
        parsed.params,
        parsed.query,
    )


def _wayback_diagnostic_observation(
    target: Dict[str, str], *, status: str, reason: str, retry_after: str = ""
) -> Dict[str, Any]:
    extra: Dict[str, Any] = {"query_match_type": "exact"}
    if retry_after:
        extra["retry_after"] = retry_after[:100]
    return {
        "source_engine": WAYBACK_ENGINE,
        "subject_type": "username",
        "subject_value": _bounded_text(target.get("investigated_username"), limit=500),
        "status": status,
        "site_name": _bounded_text(target.get("site_name"), limit=300)
        or "Public source",
        "category": "archive",
        "source_url": _safe_public_url(target.get("profile_url")),
        "reason": reason[:1000],
        "extra": extra,
        "media": {},
    }


def _cdx_timestamp_iso(timestamp: str) -> str:
    try:
        parsed = datetime.strptime(timestamp, "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError("Wayback returned an invalid capture timestamp") from exc
    return parsed.isoformat().replace("+00:00", "Z")


def _wayback_replay_url(value: Any) -> str:
    candidate = _safe_public_url(value)
    if not candidate:
        return ""
    try:
        parsed = urlparse(candidate)
        port = parsed.port
    except ValueError:
        return ""
    if (
        parsed.scheme.casefold() != "https"
        or parsed.hostname.casefold().rstrip(".") != "web.archive.org"
        or port not in {None, 443}
        or not parsed.path.startswith("/web/")
    ):
        return ""
    return candidate


def normalize_wayback_capture_index(
    target: Dict[str, str], raw_rows: Any
) -> Dict[str, Any]:
    """Validate exact-match CDX rows without downloading archived page content."""
    profile_url = _safe_public_url(target.get("profile_url"))
    target_identity = _archive_url_identity(profile_url)
    if not profile_url or not target_identity:
        raise ValueError("Wayback target is invalid")
    if not isinstance(raw_rows, list):
        raise ValueError("Wayback returned a non-list result")
    if not raw_rows:
        return _wayback_diagnostic_observation(
            target,
            status="not_archived",
            reason="No exact public HTML captures were returned by the Wayback CDX API.",
        )
    if raw_rows[0] != _WAYBACK_FIELDS:
        raise ValueError("Wayback CDX response fields changed")
    captures: List[Dict[str, str]] = []
    seen = set()
    for row in raw_rows[1 : WAYBACK_MAX_CAPTURES + 1]:
        if not isinstance(row, list) or len(row) != len(_WAYBACK_FIELDS):
            continue
        timestamp, original, statuscode, mimetype, digest = [str(item) for item in row]
        if (
            not _CDX_TIMESTAMP_PATTERN.fullmatch(timestamp)
            or statuscode != "200"
            or mimetype.casefold() != "text/html"
            or not _CDX_DIGEST_PATTERN.fullmatch(digest)
            or _archive_url_identity(original) != target_identity
        ):
            continue
        record_key = (timestamp, digest)
        if record_key in seen:
            continue
        seen.add(record_key)
        replay_url = _wayback_replay_url(
            f"https://web.archive.org/web/{timestamp}id_/{original}"
        )
        if not replay_url:
            continue
        captures.append(
            {
                "captured_at": _cdx_timestamp_iso(timestamp),
                "timestamp": timestamp,
                "digest": digest,
                "original_url": original[:2000],
                "replay_url": replay_url,
            }
        )
    if not captures:
        raise ValueError("Wayback returned no valid exact capture rows")
    captures.sort(key=lambda item: item["timestamp"])
    latest = captures[-1]
    return {
        "source_engine": WAYBACK_ENGINE,
        "subject_type": "username",
        "subject_value": _bounded_text(target.get("investigated_username"), limit=500),
        "status": "archived",
        "site_name": _bounded_text(target.get("site_name"), limit=300)
        or "Public source",
        "category": "archive",
        "source_url": latest["replay_url"],
        "source_record_id": f"wayback:{latest['timestamp']}:{latest['digest']}",
        "reason": (
            "Exact-URL historical capture metadata; it supports historical URL "
            "presence but does not establish account ownership."
        ),
        "extra": {
            "queried_profile_url": profile_url,
            "query_match_type": "exact",
            "sample_direction": "latest",
            "sampled_capture_count": len(captures),
            "oldest_sampled_capture_at": captures[0]["captured_at"],
            "latest_sampled_capture_at": latest["captured_at"],
            "captures": captures,
            "archived_page_content_fetched": False,
            "historical_presence_only": True,
            "human_review_required": True,
        },
        "media": {},
    }


async def run_wayback_capture_index(
    target: Dict[str, str],
    *,
    timeout_seconds: int = WAYBACK_TIMEOUT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Query bounded exact-match CDX metadata from a fixed public endpoint."""
    profile_url = _safe_public_url(target.get("profile_url"))
    if not profile_url or not _archive_url_identity(profile_url):
        raise ValueError("Wayback target is invalid")
    session_factory = session_factory or aiohttp.ClientSession
    timeout = aiohttp.ClientTimeout(total=max(1, min(int(timeout_seconds), 30)))
    headers = {
        "Accept": "application/json",
        "User-Agent": "OpenLedger-OSINT-Enrichment",
    }
    params = [
        ("url", profile_url),
        ("output", "json"),
        ("fl", ",".join(_WAYBACK_FIELDS)),
        ("filter", "statuscode:200"),
        ("filter", "mimetype:text/html"),
        ("collapse", "digest"),
        ("matchType", "exact"),
        ("limit", f"-{WAYBACK_MAX_CAPTURES}"),
    ]
    async with session_factory(timeout=timeout, headers=headers) as session:
        async with session.get(
            WAYBACK_API_BASE_URL, params=params, allow_redirects=False
        ) as response:
            retry_after = str(response.headers.get("Retry-After") or "")
            if response.status in {403, 429}:
                return _wayback_diagnostic_observation(
                    target,
                    status="rate_limited",
                    reason=(
                        "The Wayback CDX public API rate limit was reached; archival "
                        "metadata collection was skipped."
                    ),
                    retry_after=retry_after,
                )
            if response.status == 404:
                return _wayback_diagnostic_observation(
                    target,
                    status="not_archived",
                    reason="No exact public HTML captures were found for this profile URL.",
                )
            if response.status != 200:
                raise RuntimeError(
                    f"Wayback CDX public API returned HTTP {int(response.status)}"
                )
            content_length = response.headers.get("Content-Length")
            if content_length:
                try:
                    if int(content_length) > WAYBACK_MAX_RESPONSE_BYTES:
                        raise RuntimeError("Wayback returned an oversized response")
                except ValueError:
                    pass
            body = await response.content.read(WAYBACK_MAX_RESPONSE_BYTES + 1)
            if len(body) > WAYBACK_MAX_RESPONSE_BYTES:
                raise RuntimeError("Wayback returned an oversized response")
    try:
        raw_rows = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Wayback returned invalid JSON") from exc
    return normalize_wayback_capture_index(target, raw_rows)


def _profile_url_evidence_claim(
    observation: Dict[str, Any], *, evidence_type: str, source_name: str
) -> Optional[Dict[str, Any]]:
    source_engine = str(observation.get("source_engine") or "")
    status = str(observation.get("status") or "").casefold()
    if source_engine == UNFURL_ENGINE and status != "analyzed":
        return None
    if source_engine == WAYBACK_ENGINE and status != "archived":
        return None
    username = _bounded_text(observation.get("subject_value"), limit=500)
    site_name = _bounded_text(observation.get("site_name"), limit=300)
    extra = observation.get("extra")
    if not username or not site_name or not isinstance(extra, dict):
        return None
    if source_engine == WAYBACK_ENGINE:
        captures = extra.get("captures")
        if not isinstance(captures, list) or not captures:
            return None
        # Bind evidence to the exact native-discovery URL. CDX may normalize URL
        # spelling even when the exact resource identity is unchanged.
        original_url = _safe_public_url(extra.get("queried_profile_url"))
        if not original_url or not any(
            isinstance(capture, dict)
            and _archive_url_identity(capture.get("original_url"))
            == _archive_url_identity(original_url)
            for capture in captures[:WAYBACK_MAX_CAPTURES]
        ):
            return None
        evidence_url = _wayback_replay_url(observation.get("source_url"))
        sampled_capture_count = extra.get("sampled_capture_count")
        if (
            isinstance(sampled_capture_count, bool)
            or not isinstance(sampled_capture_count, int)
            or not 1 <= sampled_capture_count <= WAYBACK_MAX_CAPTURES
        ):
            sampled_capture_count = min(len(captures), WAYBACK_MAX_CAPTURES)
        details = {
            "query_match_type": "exact",
            "sample_direction": "latest",
            "sampled_capture_count": sampled_capture_count,
            "oldest_sampled_capture_at": _bounded_text(
                extra.get("oldest_sampled_capture_at"), limit=100
            ),
            "latest_sampled_capture_at": _bounded_text(
                extra.get("latest_sampled_capture_at"), limit=100
            ),
            "capture_replay_urls": [
                replay
                for capture in captures[:WAYBACK_MAX_CAPTURES]
                if isinstance(capture, dict)
                and (replay := _wayback_replay_url(capture.get("replay_url")))
            ],
            "archived_page_content_fetched": False,
            "historical_presence_only": True,
            "does_not_establish_ownership": True,
            "human_review_required": True,
        }
    else:
        original_url = _safe_public_url(observation.get("source_url"))
        evidence_url = original_url
        raw_nodes = extra.get("nodes")
        if not isinstance(raw_nodes, list):
            return None
        nodes = [
            node
            for raw_node in raw_nodes[:UNFURL_MAX_NODES]
            if (node := _redact_unfurl_node(raw_node))
        ]
        if not nodes:
            return None
        details = {
            "unfurl_version": UNFURL_VERSION,
            "unfurl_commit": UNFURL_PINNED_COMMIT,
            "remote_lookups": False,
            "nodes": nodes[:UNFURL_MAX_NODES],
            "structural_analysis_only": True,
            "does_not_establish_ownership": True,
            "human_review_required": True,
        }
    if not original_url or not evidence_url:
        return None
    value = {"platform": site_name, "url": original_url, "username": username}
    evidence = {
        "evidence_type": evidence_type,
        "source_name": source_name,
        "source_url": evidence_url,
        "details": details,
    }
    return {
        "field_name": "social_account",
        "value": value,
        "display_value": original_url,
        "normalized_value": json.dumps(value, sort_keys=True, ensure_ascii=False)[
            :4000
        ],
        # Neither URL structure nor an archive capture proves Persona ownership.
        "confidence": 25,
        "fingerprint": claim_fingerprint("social_account", value),
        "source_engine": source_engine,
        "source_record_id": str(observation.get("source_record_id") or "")[:500],
        "native_status": status,
        "observation_details": details,
        "evidence": [dict(evidence, fingerprint=evidence_fingerprint(evidence))],
    }


def extract_profile_url_evidence_claims(
    observations: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach URL analysis/archive evidence to existing pending social accounts."""
    candidates: List[Dict[str, Any]] = []
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        if observation.get("source_engine") == UNFURL_ENGINE:
            candidate = _profile_url_evidence_claim(
                observation,
                evidence_type="deterministic_url_analysis",
                source_name="Unfurl URL analysis",
            )
        elif observation.get("source_engine") == WAYBACK_ENGINE:
            candidate = _profile_url_evidence_claim(
                observation,
                evidence_type="wayback_capture_index",
                source_name="Internet Archive Wayback Machine",
            )
        else:
            candidate = None
        if candidate:
            candidates.append(candidate)
    return candidates


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
