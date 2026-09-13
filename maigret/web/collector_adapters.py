"""Isolated collector adapters and OpenLedger observation normalization."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import ipaddress
import inspect
import json
import os
import re
import socket
import sys
import unicodedata
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, quote, unquote, urljoin, urlparse, urlunparse

import aiohttp
import pycountry
from lxml import etree, html as lxml_html

from maigret.result import MaigretCheckStatus
from maigret.web.persona_intelligence import (
    claim_fingerprint,
    evidence_fingerprint,
)
from maigret.web.provider_circuit_breaker import ProviderCircuitOpen, provider_circuits
from maigret.web.profile_discovery_policy import (
    ProfileDiscoveryPolicyError,
    profile_discovery_flag_enabled,
)

MAIGRET_PROVIDER = "maigret"
USER_SCANNER_PROVIDER = "user-scanner"
GOOGLE_PLACES_PROVIDER = "google-places"
WIKIDATA_PROVIDER = "wikidata"
GLEIF_PROVIDER = "gleif"
FR_BUSINESS_REGISTRY_PROVIDER = "fr-business-registry"
CLOUDFLARE_DNS_PROVIDER = "cloudflare-dns"
WIKIPEDIA_PROVIDER = "wikipedia"
ICIJ_PROVIDER = "icij-offshore-leaks"
UNFURL_PROVIDER = "unfurl"
WAYBACK_PROVIDER = "wayback"
GITHUB_PROVIDER = "github"

_TRANSIENT_PROVIDER_STATUSES = frozenset(
    {"error", "failed", "rate_limited", "timed_out", "unavailable"}
)


def _transient_provider_error(error: Exception) -> bool:
    """Classify transport/adapter failures without counting invalid input."""
    if isinstance(error, ProviderCircuitOpen):
        return False
    return isinstance(
        error,
        (RuntimeError, aiohttp.ClientError, asyncio.TimeoutError, TimeoutError, OSError),
    )


def _transient_provider_result(result: Any) -> bool:
    """Count explicit provider diagnostics, but never absence or partial evidence."""
    if isinstance(result, dict):
        return str(result.get("status") or "").strip().casefold() in (
            _TRANSIENT_PROVIDER_STATUSES
        )
    if not isinstance(result, list) or not result:
        return False
    statuses = []
    for item in result:
        if not isinstance(item, dict):
            return False
        status = str(item.get("status") or "").strip().casefold()
        extra = item.get("extra") if isinstance(item.get("extra"), dict) else {}
        if status == "error" and extra.get("scan_stage") != "adapter":
            return False
        statuses.append(status)
    return bool(statuses) and all(
        status in _TRANSIENT_PROVIDER_STATUSES for status in statuses
    )


def governed_provider(provider: str):
    """Apply the shared no-retry circuit policy to one async provider boundary."""

    def decorate(function):
        @wraps(function)
        async def guarded(*args, **kwargs):
            enablement_flag = (
                "maigret_enabled"
                if provider == MAIGRET_PROVIDER
                else (
                    "user_scanner_enabled"
                    if provider == USER_SCANNER_PROVIDER
                    else "enrichment_providers_enabled"
                )
            )
            if not profile_discovery_flag_enabled(enablement_flag):
                raise ProfileDiscoveryPolicyError(
                    f"Provider {provider} is disabled by server policy."
                )
            if not profile_discovery_flag_enabled(
                "provider_circuit_breakers_enabled"
            ):
                return await function(*args, **kwargs)
            return await provider_circuits.call(
                provider,
                lambda: function(*args, **kwargs),
                transient_error=_transient_provider_error,
                transient_result=_transient_provider_result,
            )

        return guarded

    return decorate

USER_SCANNER_ENGINE = "user_scanner_email"
USER_SCANNER_USERNAME_ENGINE = "user_scanner_username"
USER_SCANNER_TIMEOUT_SECONDS = 420
USER_SCANNER_USERNAME_PLATFORMS = (
    "facebook",
    "instagram",
    "threads",
    "tiktok",
    "x",
)
MAX_USER_SCANNER_USERNAME_TARGETS = 16
USER_SCANNER_USERNAME_PROCESS_CONCURRENCY = 2
MAX_COLLECTOR_OUTPUT_BYTES = 8_000_000
MAX_USER_SCANNER_USERNAME_TARGET_OUTPUT_BYTES = (
    MAX_COLLECTOR_OUTPUT_BYTES // MAX_USER_SCANNER_USERNAME_TARGETS
)
MAX_OBSERVATIONS = 600
SUBPROCESS_CLEANUP_SECONDS = 5.0
SUBPROCESS_TERMINATE_SECONDS = 4.0

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

WIKIDATA_ENGINE = "wikidata_affiliation"
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_QUERY_URL = "https://query.wikidata.org/sparql"
WIKIDATA_TIMEOUT_SECONDS = 30
WIKIDATA_MAX_RESPONSE_BYTES = 1_000_000
MAX_WIKIDATA_ENTITY_CANDIDATES = 5
MAX_WIKIDATA_AFFILIATED_PEOPLE = 50
MAX_WIKIDATA_CLASS_DEPTH = 4
MAX_WIKIDATA_CLASS_IDS = 50
MAX_WIKIDATA_CLASS_CLAIMS = 20
MAX_WIKIDATA_PROPERTY_STATEMENTS = 100

WIKIPEDIA_ENGINE = "wikipedia_public_biography"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_TIMEOUT_SECONDS = 20
WIKIPEDIA_MAX_RESPONSE_BYTES = 1_000_000
MAX_WIKIPEDIA_CANDIDATES = 5
WIKIPEDIA_MAX_EXTRACT_CHARS = 2_000

ICIJ_OFFSHORE_ENGINE = "icij_offshore_leaks"
ICIJ_RECONCILE_URL = "https://offshoreleaks.icij.org/api/v1/reconcile"
ICIJ_TIMEOUT_SECONDS = 30
ICIJ_MAX_RESPONSE_BYTES = 1_000_000
MAX_ICIJ_MATCHES = 5

_WIKIDATA_ID_PATTERN = re.compile(r"^Q[1-9][0-9]{0,19}$")
_WIKIDATA_ENTITY_URL_PATTERN = re.compile(
    r"^https?://www\.wikidata\.org/entity/(Q[1-9][0-9]{0,19})$"
)
_WIKIDATA_PROPERTY_URL_PATTERN = re.compile(
    r"^https?://www\.wikidata\.org/prop/direct/(P[1-9][0-9]{0,9})$"
)
_WIKIDATA_RELATIONSHIPS = {
    "P69": "educated at",
    "P108": "employer",
    "P463": "member of",
    "P1416": "affiliation",
    "P112": "founded by",
    "P169": "chief executive officer",
    "P488": "chairperson",
    "P1037": "director or manager",
    "P3320": "board member",
}
MAX_WIKIDATA_AFFILIATION_ROWS = (
    MAX_WIKIDATA_AFFILIATED_PEOPLE * len(_WIKIDATA_RELATIONSHIPS)
)
_WIKIDATA_ORGANIZATION_INSTANCE_IDS = frozenset(
    {
        "Q43229",    # organization
        "Q4830453",  # business
        "Q783794",   # company
        "Q6881511",  # enterprise
        "Q2385804",  # educational institution
        "Q3918",     # university
        "Q1664720",  # institute
        "Q163740",   # nonprofit organization
        "Q79913",    # non-governmental organization
        "Q484652",   # international organization
        "Q327333",   # government agency
        "Q48204",    # voluntary association
        "Q7278",     # political party
        "Q31855",    # research institute
    }
)
MAX_ORGANIZATION_RESOLUTION_CANDIDATES = 15
PUBLIC_WEB_ORGANIZATION_RESEARCH_ENGINE = "openai_public_web_research"
MAX_PUBLIC_WEB_ORGANIZATION_FINDINGS = 20

GOOGLE_PLACES_ENGINE = "google_places_business_search"
GOOGLE_PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
GOOGLE_PLACES_DETAILS_URL = "https://places.googleapis.com/v1/places"
GOOGLE_PLACES_TIMEOUT_SECONDS = 15
GOOGLE_PLACES_MAX_RESPONSE_BYTES = 256_000
MAX_GOOGLE_PLACES_CANDIDATES = 5
_GOOGLE_PLACE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{10,512}$")
_GOOGLE_BUSINESS_LOCATION_TYPES = frozenset(
    {
        "academic_department",
        "accounting",
        "association_or_organization",
        "bank",
        "business_center",
        "community_center",
        "consultant",
        "corporate_office",
        "coworking_space",
        "educational_institution",
        "employment_agency",
        "engineering_consultant",
        "farm",
        "finance",
        "general_contractor",
        "government_office",
        "insurance_agency",
        "internet_service_provider",
        "lawyer",
        "local_government_office",
        "manufacturer",
        "marketing_consultant",
        "non_profit_organization",
        "ranch",
        "real_estate_agency",
        "research_institute",
        "school",
        "software_company",
        "supplier",
        "telecommunications_service_provider",
        "television_studio",
        "travel_agency",
        "university",
        "wholesaler",
    }
)

GLEIF_ENGINE = "gleif_lei_registry"
GLEIF_API_URL = "https://api.gleif.org/api/v1/lei-records"
GLEIF_TIMEOUT_SECONDS = 30
GLEIF_MAX_RESPONSE_BYTES = 1_000_000
MAX_GLEIF_SEARCH_ROWS = 20
MAX_GLEIF_CANDIDATES = 5

FR_BUSINESS_REGISTRY_ENGINE = "fr_company_registry"
FR_BUSINESS_REGISTRY_URL = "https://recherche-entreprises.api.gouv.fr/search"
FR_BUSINESS_REGISTRY_TIMEOUT_SECONDS = 30
FR_BUSINESS_REGISTRY_MAX_RESPONSE_BYTES = 1_000_000
MAX_FR_BUSINESS_CANDIDATES = 5
MAX_REGISTRY_AFFILIATED_PEOPLE = 25

REGISTRY_SOURCE_NAMES = {
    GLEIF_ENGINE: "GLEIF Global LEI Index",
    FR_BUSINESS_REGISTRY_ENGINE: "French National Enterprise Directory",
}
REGISTRY_SOURCE_ENGINES = frozenset(REGISTRY_SOURCE_NAMES)

CLOUDFLARE_DNS_ENGINE = "cloudflare_dns_context"
CLOUDFLARE_DNS_URL = "https://cloudflare-dns.com/dns-query"
CLOUDFLARE_DNS_TIMEOUT_SECONDS = 20
CLOUDFLARE_DNS_MAX_RESPONSE_BYTES = 128_000
CLOUDFLARE_DNS_QUERY_TYPES = ("A", "AAAA", "MX", "NS")
MAX_DNS_RECORDS_PER_TYPE = 20
MAX_DNS_RECORDS_TOTAL = 40

OFFICIAL_WEBSITE_ENGINE = "official_website_public_content"
OFFICIAL_WEBSITE_TIMEOUT_SECONDS = 20
OFFICIAL_WEBSITE_MAX_RESPONSE_BYTES = 750_000
MAX_OFFICIAL_WEBSITE_ADDRESSES = 10
MAX_OFFICIAL_WEBSITE_CONTACTS = 10
MAX_OFFICIAL_WEBSITE_PEOPLE = 25
MAX_OFFICIAL_WEBSITE_LINKED_PROFILES = 2
MAX_OFFICIAL_WEBSITE_REDIRECTS = 1
MAX_OFFICIAL_WEBSITE_PAGES = 4

_LEI_PATTERN = re.compile(r"^[A-Z0-9]{20}$")
_SIREN_PATTERN = re.compile(r"^[0-9]{9}$")
_SIRET_PATTERN = re.compile(r"^[0-9]{14}$")
_ICIJ_NODE_ID_PATTERN = re.compile(r"^[1-9][0-9]{0,19}$")
_WIKIPEDIA_PAGE_URL_PATTERN = re.compile(
    r"^https://en\.wikipedia\.org/wiki/[^?#]{1,2000}$"
)
_LINKEDIN_COMPANY_URL_PATTERN = re.compile(
    r"^https://www\.linkedin\.com/company/([A-Za-z0-9][A-Za-z0-9_-]{0,99})/?$"
)

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
    """Stop and reap an adapter process within a fixed five-second window."""
    loop = asyncio.get_running_loop()
    cleanup_deadline = loop.time() + SUBPROCESS_CLEANUP_SECONDS
    communicate_task.cancel()
    if process.returncode is not None:
        remaining = max(0.0, cleanup_deadline - loop.time())
        if remaining:
            try:
                await asyncio.wait_for(
                    asyncio.gather(communicate_task, return_exceptions=True),
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                pass
        return
    try:
        process.terminate()
    except ProcessLookupError:
        pass
    else:
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=min(
                    SUBPROCESS_TERMINATE_SECONDS,
                    max(0.0, cleanup_deadline - loop.time()),
                ),
            )
        except asyncio.TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            remaining = max(0.0, cleanup_deadline - loop.time())
            if remaining:
                try:
                    await asyncio.wait_for(process.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    # SIGKILL has already been delivered. Do not let a broken child
                    # watcher keep the investigation in cancel_requested forever.
                    pass
    remaining = max(0.0, cleanup_deadline - loop.time())
    if remaining:
        try:
            await asyncio.wait_for(
                asyncio.gather(communicate_task, return_exceptions=True),
                timeout=remaining,
            )
        except asyncio.TimeoutError:
            pass


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


def user_scanner_username_targets(plan: Any) -> List[str]:
    """Return the analyst-approved, bounded username verification targets."""
    if not isinstance(plan, dict) or not plan.get("enable_user_scanner_username"):
        return []
    targets: List[str] = []
    for target in list(plan.get("search_targets") or []):
        if not isinstance(target, dict):
            continue
        value = str(target.get("value") or "").strip().lstrip("@")
        if value and value.casefold() not in {item.casefold() for item in targets}:
            targets.append(value[:128])
        if len(targets) >= MAX_USER_SCANNER_USERNAME_TARGETS:
            break
    return targets


def user_scanner_username_policy(plan: Any) -> Dict[str, Any]:
    """Return validated platform scope and the explicit third-party X consent."""
    if not isinstance(plan, dict):
        return {"platforms": [], "allow_vxtwitter": False}
    requested = plan.get("user_scanner_username_platforms")
    if not isinstance(requested, list):
        requested = list(USER_SCANNER_USERNAME_PLATFORMS)
    platforms = []
    for raw_platform in requested:
        platform = str(raw_platform or "").strip().casefold()
        if platform in USER_SCANNER_USERNAME_PLATFORMS and platform not in platforms:
            platforms.append(platform)
    return {
        "platforms": platforms,
        "allow_vxtwitter": bool(plan.get("allow_user_scanner_vxtwitter")),
    }


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


def _normalize_dns_hostname(value: Any) -> str:
    hostname = str(value or "").strip().rstrip(".").casefold()
    if not hostname or len(hostname) > 253:
        return ""
    try:
        hostname = hostname.encode("idna").decode("ascii")
    except (UnicodeError, UnicodeDecodeError):
        return ""
    labels = hostname.split(".")
    if len(labels) < 2 or any(
        not label
        or len(label) > 63
        or label.startswith("-")
        or label.endswith("-")
        or re.fullmatch(r"[a-z0-9-]+", label) is None
        for label in labels
    ):
        return ""
    return hostname


def normalize_official_website_url(value: Any) -> Optional[Dict[str, str]]:
    """Normalize an explicit public website without fetching its origin."""
    safe_url = _safe_public_url(value)
    if not safe_url:
        if str(value or "").strip():
            raise ValueError("Enter a valid public HTTP or HTTPS official website URL")
        return None
    parsed = urlparse(safe_url)
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("Enter a valid official website port") from error
    expected_port = 443 if parsed.scheme.casefold() == "https" else 80
    if port not in {None, expected_port}:
        raise ValueError(
            "Official website URLs may use only standard web ports matching their scheme"
        )
    domain = _normalize_dns_hostname(parsed.hostname)
    if not domain:
        raise ValueError("Enter a public official website domain")
    if parsed.query and _url_has_sensitive_query_key(safe_url):
        raise ValueError(
            "Official website URLs must not contain credential-like query parameters"
        )
    return {"url": safe_url, "domain": domain}


def _normalize_linkedin_company_url(value: Any) -> str:
    safe_url = _safe_public_url(value)
    if not safe_url:
        return ""
    parsed = urlparse(safe_url)
    try:
        port = parsed.port
    except ValueError:
        return ""
    if (
        parsed.scheme.casefold() != "https"
        or (parsed.hostname or "").casefold().rstrip(".") != "www.linkedin.com"
        or port not in {None, 443}
    ):
        return ""
    canonical = urlunparse(
        ("https", "www.linkedin.com", parsed.path.rstrip("/"), "", "", "")
    )
    return canonical if _LINKEDIN_COMPANY_URL_PATTERN.fullmatch(canonical) else ""


def _validated_public_addresses(values: Any) -> List[str]:
    addresses = []
    for raw_value in list(values or [])[:20]:
        value = raw_value[0] if isinstance(raw_value, (tuple, list)) else raw_value
        try:
            address = ipaddress.ip_address(str(value or "").split("%", 1)[0])
        except ValueError as error:
            raise ValueError("The website hostname returned an invalid address") from error
        if (
            not address.is_global
            or address.is_multicast
            or address.is_reserved
            or address.is_unspecified
            or address.is_loopback
            or address.is_link_local
            or address.is_private
        ):
            raise ValueError("The website hostname resolved to a non-public address")
        canonical = str(address)
        if canonical not in addresses:
            addresses.append(canonical)
    if not addresses:
        raise ValueError("The website hostname did not resolve to a public address")
    return addresses[:4]


async def _resolve_public_host(hostname: str, port: int) -> List[str]:
    loop = asyncio.get_running_loop()
    try:
        results = await loop.getaddrinfo(
            hostname,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except OSError as error:
        raise RuntimeError("The website hostname could not be resolved") from error
    return _validated_public_addresses([result[4][0] for result in results])


async def _resolved_public_addresses(
    resolver: Callable[..., Any], hostname: str, port: int
) -> List[str]:
    result = resolver(hostname, port)
    if inspect.isawaitable(result):
        result = await result
    return _validated_public_addresses(result)


def _pinned_request_target(url: str, address: str) -> tuple[str, str, str]:
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if not hostname:
        raise ValueError("The website URL has no hostname")
    port = parsed.port or (443 if parsed.scheme.casefold() == "https" else 80)
    ip_value = ipaddress.ip_address(address)
    request_host = f"[{ip_value}]" if ip_value.version == 6 else str(ip_value)
    if port not in {80, 443}:
        request_host += f":{port}"
    host_header = hostname if port in {80, 443} else f"{hostname}:{port}"
    target = urlunparse(
        (
            parsed.scheme.casefold(),
            request_host,
            parsed.path or "/",
            parsed.params,
            parsed.query,
            "",
        )
    )
    return target, hostname, host_header


async def _bounded_public_html_request(
    session: Any,
    url: str,
    *,
    resolver: Callable[..., Any],
    source_name: str,
    maximum_bytes: int,
) -> Dict[str, Any]:
    normalized = normalize_official_website_url(url)
    if not normalized:
        raise ValueError("A public website URL is required")
    parsed = urlparse(normalized["url"])
    port = parsed.port or (443 if parsed.scheme.casefold() == "https" else 80)
    addresses = await _resolved_public_addresses(resolver, normalized["domain"], port)
    request_url, server_hostname, host_header = _pinned_request_target(
        normalized["url"], addresses[0]
    )
    request_options: Dict[str, Any] = {
        "allow_redirects": False,
        "headers": {"Host": host_header},
    }
    if parsed.scheme.casefold() == "https":
        request_options["server_hostname"] = server_hostname
    async with session.get(request_url, **request_options) as response:
        if response.status in {301, 302, 303, 307, 308}:
            return {
                "status": "redirect",
                "location": str(response.headers.get("Location") or "")[:2000],
            }
        if response.status in {403, 429}:
            return {"status": "rate_limited"}
        if response.status == 404:
            return {"status": "not_found"}
        if response.status != 200:
            raise RuntimeError(
                f"{source_name} returned HTTP {int(response.status)}"
            )
        content_type = str(response.headers.get("Content-Type") or "").casefold()
        if content_type and not any(
            allowed in content_type
            for allowed in ("text/html", "application/xhtml+xml")
        ):
            raise RuntimeError(f"{source_name} did not return an HTML document")
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                declared_length = int(content_length)
            except (TypeError, ValueError) as error:
                raise RuntimeError(
                    f"{source_name} returned an invalid response length"
                ) from error
            if declared_length < 0 or declared_length > maximum_bytes:
                raise RuntimeError(f"{source_name} returned an oversized response")
        body = await response.content.read(maximum_bytes + 1)
        if len(body) > maximum_bytes:
            raise RuntimeError(f"{source_name} returned an oversized response")
    return {"status": "ok", "body": body}


def _public_html_document(body: Any, *, source_name: str):
    if not isinstance(body, (bytes, bytearray)) or not body:
        raise ValueError(f"{source_name} returned an empty HTML document")
    parser = etree.HTMLParser(
        recover=True,
        no_network=True,
        remove_comments=True,
        huge_tree=False,
    )
    try:
        document = lxml_html.fromstring(bytes(body), parser=parser)
    except (etree.ParserError, TypeError, Val}v◊ù≠¢Gß≤⁄Óù∆≠y›
        <label class="affiliation-jurisdiction-field">
            <span>Organization named by this role</span>
            <input type="text" name="organization_name" maxlength="500" value="{{ claim.suggested_organization_target }}" autocomplete="organization" readonly required>
        </label>
        <small>Confirm the exact organization named by the approved role. This opens a research case; it does not silently turn the role text into an approved affiliation fact.</small>
        {% endif %}
        <label class="affiliation-jurisdiction-field">
            <span>Registered jurisdiction <small>(optional)</small></span>
            <input type="text" name="jurisdiction" maxlength="100" placeholder="Indonesia, ID, or US-DE" autocomplete="off">
        </label>
        <label class="affiliation-jurisdiction-field">
            <span>Known official website <small>(optional)</small></span>
            <input type="url" name="official_website" maxlength="2000" placeholder="https://example.org" autocomplete="off">
        </label>
        <label class="affiliation-domain-toggle">
            <input type="checkbox" name="enable_domain_context" value="1">
            <span>Collect current DNS context</span>
        </label>
        <button type="submit" class="btn btn-sm btn-outline-secondary"><i data-lucide="building-2"></i> {{ 'Investigate organization from role' if claim.field_name == 'occupation' else 'Open affiliation case' }}</button>
        <small>Registry and DNS checks are independent. DNS remains observation-only and never establishes where the business operates.</small>
    </form>
    {% endif %}
</article>
{% endmacro %}

{% block title %}{{ persona.display_name }} | OpenLedger{% endblock %}
{% block breadcrumb %}Persona workspace{% endblock %}

{% block head %}
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="">
{% endblock %}

{% block content %}
<section class="persona-profile-header">
    <div class="persona-photo-frame">
        {% if approved_photograph %}
        <img src="{{ approved_photograph.display_value }}" alt="{{ persona.display_name }}" referrerpolicy="no-referrer" data-profile-image>
        {% endif %}
        <span class="persona-photo-placeholder" {% if approved_photograph %}hidden{% endif %}>
            <i data-lucide="user-round"></i>
            <small>No approved photograph</small>
        </span>
    </div>
    <div class="persona-profile-copy">
        <p class="eyebrow">Working evidence ¬∑ Draft Persona</p>
        <h1>{{ persona.display_name }}</h1>
        <p class="page-description">{{ persona.case_title }} ¬∑ Review evidence by subject area, decide each finding, then export an immutable investigation report snapshot.</p>
        <div class="persona-status-strip" aria-label="Persona evidence status">
            <span><strong>{{ review_counts.approved }}</strong> approved</span>
            <span><strong>{{ review_counts.pending }}</strong> pending</span>
            <span><strong>{{ review_counts.uncertain }}</strong> uncertain</span>
            <span><strong>{{ review_counts.rejected }}</strong> rejected</span>
        </div>
    </div>
    <div class="heading-actions persona-heading-actions">
        <a href="{{ url_for('pipeline.workspace', case_id=persona.case_id, persona_id=persona.id) }}" class="btn btn-primary">Open investigation assessment</a>
        <a href="{{ url_for('case_workspace', case_id=persona.case_id) }}" class="btn btn-outline-secondary">Open case</a>
        <a href="{{ url_for('case_timeline_workspace', case_id=persona.case_id, persona_id=persona.id) }}" class="btn btn-outline-secondary"><i data-lucide="list-tree"></i> Timeline</a>
        <a href="{{ url_for('configure_persona_investigation', persona_id=persona.id) }}" class="btn btn-primary"><i data-lucide="refresh-cw"></i> Configure investigation</a>
        {% if identity_enrichment and identity_enrichment.status in ('queued', 'running', 'cancel_requested') %}
        <a href="{{ url_for('live_results', job_id=identity_enrichment.job_id) }}" class="btn btn-outline-secondary"><i data-lucide="scan-search"></i> Earlier enrichment progress</a>
        {% endif %}
    </div>
</section>

{% if offshore_matches %}
<section class="offshore-alert" role="alert" aria-label="Potential Offshore Leaks match">
    <i data-lucide="triangle-alert"></i>
    <div>
        <strong>Potential ICIJ Offshore Leaks name match</strong>
        <p>An exact name appears in the ICIJ database. This is an investigative alert, not confirmed identity or evidence of wrongdoing. Compare the source record with independently verified identifiers before approving it.</p>
        <div class="offshore-alert-records">
            {% for match in offshore_matches %}
            <span>
                <b>{{ match.display_value }}</b>
                <em>{{ match.review_status | replace('_', ' ') | title }}</em>
                {% for evidence in match.evidence %}
                    {% if evidence.source_url %}<a href="{{ evidence.source_url }}" target="_blank" rel="noopener noreferrer">Review ICIJ source</a>{% endif %}
                {% endfor %}
            </span>
            {% endfor %}
        </div>
    </div>
</section>
{% endif %}

{% if identity_enrichment and identity_enrichment.status == 'completed' %}
<section class="panel-card identity-enrichment-card">
    <header class="panel-header">
        <div><h2>Earlier name-enrichment results</h2><p>This historical run is retained for lineage. New investigations search full-name sources from the initial query.</p></div>
        <span class="badge-soft">{{ identity_enrichment.completed_at }}</span>
    </header>
    <div class="panel-body identity-enrichment-body">
        {% if identity_enrichment.wikipedia_status == 'needs_selection' and approved_full_name %}
        <strong>Select the correct Wikipedia biography</strong>
        <p class="form-text">No biography is used until you choose one of the stored candidates.</p>
        <div class="affiliation-candidate-list">
            {% for candidate in identity_enrichment.wikipedia_candidates %}
            <form method="POST" action="{{ url_for('select_wikipedia_biography', persona_id=persona.id) }}" class="affiliation-candidate">
                <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
                <input type="hidden" name="source_claim_id" value="{{ approved_full_name.id }}">
                <input type="hidden" name="page_id" value="{{ candidate.page_id }}">
                <span><strong>{{ candidate.title }}</strong><small>{{ candidate.extract | truncate(220) }}</small></span>
                <button type="submit" class="btn btn-sm btn-outline-secondary">Use this biography</button>
            </form>
            {% endfor %}
        </div>
        {% elif identity_enrichment.wikipedia_status == 'observed' %}
        <p class="mb-0"><strong>Wikipedia biography found.</strong> Its introductory summary, page identifier, and available lead image were added to the review queue.</p>
        {% else %}
        <p class="mb-0">No unambiguous Wikipedia biography was proposed. Status: {{ identity_enrichment.wikipedia_status | default('unavailable', true) | replace('_', ' ') }}.</p>
        {% endif %}
        {% if identity_enrichment.offshore_status == 'no_match' %}
        <p class="public-record-clear mb-0"><i data-lucide="shield-check"></i> No exact-name ICIJ Offshore Leaks candidate was returned in this check.</p>
        {% endif %}
    </div>
</section>
{% endif %}

<section class="persona-lifecycle" aria-label="Evidence lifecycle">
    <div><span>1</span><strong>Collect</strong><small>Source adapters and cited AI research create pending proposals.</small></div>
    <i data-lucide="chevron-right"></i>
    <div><span>2</span><strong>Review</strong><small>The analyst approves, rejects, or marks each claim uncertain.</small></div>
    <i data-lucide="chevron-right"></i>
    <div><span>3</span><strong>Report</strong><small>Approved findings populate the working Persona and can be frozen into an investigation report.</small></div>
</section>

<section class="persona-notice" aria-label="AI and evidence policy">
    <i data-lucide="shield-check"></i>
    <div>
        <strong>AI proposes; the analyst decides</strong>
        <span>Cited AI findings may enter the review queue as pending suggestions, but AI cannot approve, reject, or make them canonical. Rejected records remain in the audit trail and stay hidden from the default Persona.</span>
    </div>
</section>

<section class="panel-card persona-ai-status">
    <header class="panel-header">
        <div><h2>AI evidence pipeline</h2><p>Assessment prose and structured Persona evidence are separate outputs.</p></div>
        {% if ai_analysis_status.session_id %}<a class="btn btn-outline-secondary" href="{{ url_for('results', session_id=ai_analysis_status.session_id) }}">Open assessment</a>{% endif %}
    </header>
    <div class="panel-body">{% include '_ai_pipeline_status.html' %}</div>
</section>

<nav class="persona-tabs" role="tablist" aria-label="Persona categories">
    {% for group in claim_groups %}
    <button type="button" role="tab" class="persona-tab {{ 'active' if loop.first }}" aria-selected="{{ 'true' if loop.first else 'false' }}" data-persona-tab="{{ group.key }}">{{ group.title }}</button>
    {% endfor %}
    <button type="button" role="tab" class="persona-tab" aria-selected="false" data-persona-tab="review">Review queue <span>{{ review_claims | length }}</span></button>
</nav>

<div class="persona-tab-workspace">
    {% for group in claim_groups %}
    <section class="panel-card persona-group persona-tab-panel" id="group-{{ group.key }}" data-persona-panel="{{ group.key }}" {% if not loop.first %}hidden{% endif %}>
        <header class="panel-header">
            <div>
                <h2>{{ group.title }}</h2>
                <p>{{ group.description }}</p>
            </div>
            <a href="{{ url_for('export_persona_pdf', persona_id=persona.id) }}" class="btn btn-outline-secondary persona-export-button"><i data-lucide="file-down"></i> Export investigation report</a>
        </header>
        <div class="panel-body persona-form">
            {% if group.key == 'online' %}
            <div class="form-text mb-3">
                <p>Confirmed accounts, pending account candidates, stable identifiers, and public profile leads are unified here. Inconclusive checks remain in collection history; an omitted account is not proof that it does not exist.</p>
            </div>
            {% endif %}
            {% for field in group.fields %}
            <div class="persona-field">
                <div class="persona-field-label">{{ field.label }}</div>
                <div class="persona-field-content">
                    {% if field.claims %}
                        {% for claim in field.claims %}{{ claim_record(claim, group.key) }}{% endfor %}
                    {% else %}
                    <div class="empty-field">
                        No evidence extracted.
                        {% if field.key == 'address' %}
                        OpenLedger does not infer a private or residential address.
                        {% elif field.key == 'current_location' %}
                        Email registration does not establish location; a public source must state a coarse place explicitly.
                        {% endif %}
                    </div>
                    {% endif %}
                </div>
            </div>
            {% endfor %}
        </div>
        {% if group.key == 'contact' %}
        <div class="persona-map-section">
            <div class="map-section-heading">
                <div><h3>Confirmed locations</h3><p>Approved places are mapped from analyst coordinates, cited AI proposals, or a generated place centroid.</p></div>
                <span class="badge-soft">{{ map_locations | length }} mapped</span>
            </div>
            {% if map_locations %}
            <div id="personaLocationMap" class="persona-location-map" aria-label="Approved persona locations"></div>
            <p class="map-disclosure">Map tiles are loaded from the configured tile provider. Use an internal tile server for isolated or sensitive deployments.</p>
            {% else %}
            <div class="map-empty"><i data-lucide="map-pin-off"></i><span>Approve a place to generate its map center automatically, or enter coordinates as an override.</span></div>
            {% endif %}
        </div>
        {% endif %}
    </section>
    {% endfor %}

    <section class="panel-card persona-group persona-tab-panel" data-persona-panel="review" hidden>
        <header class="panel-header">
            <div>
                <h2>Evidence review queue</h2>
                <p>Rejected records are retained for audit and reversal, but excluded from the default Persona and its approved outputs.</p>
            </div>
        </header>
        <div class="review-filter-bar" role="group" aria-label="Filter review records">
            <button type="button" class="review-filter active" data-review-filter="open">Needs review <span>{{ review_counts.pending + review_counts.uncertain }}</span></button>
            <button type="button" class="review-filter" data-review-filter="rejected">Rejected <span>{{ review_counts.rejected }}</span></button>
            <button type="button" class="review-filter" data-review-filter="all">All non-approved <span>{{ review_claims | length }}</span></button>
        </div>
        <div class="panel-body review-queue-list" id="reviewQueue">
            {% if review_claims %}
                {% for claim in review_claims %}
                <div class="review-queue-item" data-review-item="{{ claim.review_status }}">
                    <div class="review-field-name">{{ field_display_label(claim.field_name) }}</div>
                    {{ claim_record(claim, 'review') }}
                </div>
                {% endfor %}
            {% else %}
            <div class="empty-state compact"><i data-lucide="badge-check"></i><h3>No evidence needs review</h3></div>
            {% endif %}
        </div>
    </section>
</div>
{% endblock %}

{% block scripts %}
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
(() => {
    const tabs = Array.from(document.querySelectorAll('[data-persona-tab]'));
    const panels = Array.from(document.querySelectorAll('[data-persona-panel]'));
    const activateTab = (key) => {
        tabs.forEach((tab) => {
            const active = tab.dataset.personaTab === key;
            tab.classList.toggle('active', active);
            tab.setAttribute('aria-selected', String(active));
        });
        panels.forEach((panel) => { panel.hidden = panel.dataset.personaPanel !== key; });
        if (key === 'contact' && window.personaMap) setTimeout(() => window.personaMap.invalidateSize(), 0);
    };
    tabs.forEach((tab) => tab.addEventListener('click', () => activateTab(tab.dataset.personaTab)));
    if (window.location.hash === '#group-online') activateTab('online');

    document.querySelectorAll('[data-profile-image]').forEach((image) => {
        image.addEventListener('error', () => {
            image.hidden = true;
            const fallback = image.nextElementSibling;
            if (fallback) fallback.hidden = false;
        });
    });

    const reviewItems = Array.from(document.querySelectorAll('[data-review-item]'));
    document.querySelectorAll('[data-review-filter]').forEach((button) => {
        button.addEventListener('click', () => {
            document.querySelectorAll('[data-review-filter]').forEach((item) => item.classList.toggle('active', item === button));
            const filter = button.dataset.reviewFilter;
            reviewItems.forEach((item) => {
                item.hidden = filter === 'rejected'
                    ? item.dataset.reviewItem !== 'rejected'
                    : filter === 'open'
                        ? item.dataset.reviewItem === 'rejected'
                        : false;
            });
        });
    });
    reviewItems.forEach((item) => { item.hidden = item.dataset.reviewItem === 'rejected'; });

    const locations = {{ map_locations | tojson }};
    const mapElement = document.getElementById('personaLocationMap');
    if (mapElement && locations.length && window.L) {
        const map = L.map(mapElement, {scrollWheelZoom: false});
        window.personaMap = map;
        L.tileLayer({{ map_tile_url | tojson }}, {
            maxZoom: 19,
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);
        const bounds = [];
        locations.forEach((location) => {
            const point = [location.latitude, location.longitude];
            bounds.push(point);
            const popup = document.createElement('div');
            const title = document.createElement('strong'); title.textContent = location.label;
            const precision = location.coordinate_precision ? ` ¬∑ approximate ${location.coordinate_precision} center` : '';
            const detail = document.createElement('span'); detail.textContent = `${location.field_name.replace('_', ' ')} ¬∑ ${location.confidence}% confidence${precision}`;
            popup.append(title, document.createElement('br'), detail);
            L.marker(point).addTo(map).bindPopup(popup);
        });
        if (bounds.length === 1) map.setView(bounds[0], 8);
        else map.fitBounds(bounds, {padding: [28, 28]});
    }
})();
</script>
{% endblock %}
