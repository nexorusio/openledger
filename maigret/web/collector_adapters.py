"""Isolated collector adapters and OpenLedger observation normalization."""

from __future__ import annotations

import asyncio
import importlib.util
import ipaddress
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, quote, urlparse

import aiohttp
import pycountry

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

WIKIDATA_ENGINE = "wikidata_affiliation"
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_QUERY_URL = "https://query.wikidata.org/sparql"
WIKIDATA_TIMEOUT_SECONDS = 30
WIKIDATA_MAX_RESPONSE_BYTES = 1_000_000
MAX_WIKIDATA_ENTITY_CANDIDATES = 5
MAX_WIKIDATA_AFFILIATED_PEOPLE = 50

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
MAX_FR_BUSINESS_PEOPLE = 25

CLOUDFLARE_DNS_ENGINE = "cloudflare_dns_context"
CLOUDFLARE_DNS_URL = "https://cloudflare-dns.com/dns-query"
CLOUDFLARE_DNS_TIMEOUT_SECONDS = 20
CLOUDFLARE_DNS_MAX_RESPONSE_BYTES = 128_000
CLOUDFLARE_DNS_QUERY_TYPES = ("A", "AAAA", "MX", "NS")
MAX_DNS_RECORDS_PER_TYPE = 20
MAX_DNS_RECORDS_TOTAL = 40

_LEI_PATTERN = re.compile(r"^[A-Z0-9]{20}$")
_SIREN_PATTERN = re.compile(r"^[0-9]{9}$")
_SIRET_PATTERN = re.compile(r"^[0-9]{14}$")
_ICIJ_NODE_ID_PATTERN = re.compile(r"^[1-9][0-9]{0,19}$")
_WIKIPEDIA_PAGE_URL_PATTERN = re.compile(
    r"^https://en\.wikipedia\.org/wiki/[^?#]{1,2000}$"
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
    if port not in {None, 80, 443}:
        raise ValueError("Official website URLs may use only standard web ports")
    domain = _normalize_dns_hostname(parsed.hostname)
    if not domain:
        raise ValueError("Enter a public official website domain")
    return {"url": safe_url, "domain": domain}


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


def normalize_affiliation_name(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    normalized = " ".join(normalized.split())
    if not 2 <= len(normalized) <= 500:
        raise ValueError("Affiliation names must contain between 2 and 500 characters")
    if any(ord(character) < 32 for character in normalized):
        raise ValueError("Affiliation names cannot contain control characters")
    return normalized


def _affiliation_identity(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(normalized.split()).casefold()


def normalize_confirmed_person_name(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    normalized = " ".join(normalized.split())
    if not 2 <= len(normalized) <= 300:
        raise ValueError("Confirmed names must contain between 2 and 300 characters")
    if any(ord(character) < 32 for character in normalized):
        raise ValueError("Confirmed names cannot contain control characters")
    return normalized


def normalize_legal_jurisdiction(value: Any) -> Optional[Dict[str, str]]:
    """Resolve a country or ISO-3166 subdivision without accepting free-form scope."""
    raw = unicodedata.normalize("NFKC", str(value or "")).strip()
    if not raw:
        return None
    if len(raw) > 100 or any(ord(character) < 32 for character in raw):
        raise ValueError("Select a valid legal jurisdiction")
    code_candidate = raw.replace("_", "-").upper()
    country = None
    subdivision = None
    if re.fullmatch(r"[A-Z]{2}", code_candidate):
        country = pycountry.countries.get(alpha_2=code_candidate)
    elif re.fullmatch(r"[A-Z]{2}-[A-Z0-9]{1,3}", code_candidate):
        subdivision = pycountry.subdivisions.get(code=code_candidate)
    else:
        try:
            country = pycountry.countries.lookup(raw)
        except LookupError:
            country = None
    if subdivision is not None:
        country = pycountry.countries.get(alpha_2=subdivision.country_code)
        country_name = str(getattr(country, "name", subdivision.country_code))
        return {
            "code": str(subdivision.code),
            "label": f"{subdivision.name}, {country_name}"[:200],
            "country_code": str(subdivision.country_code),
        }
    if country is not None:
        return {
            "code": str(country.alpha_2),
            "label": str(country.name)[:200],
            "country_code": str(country.alpha_2),
        }
    raise ValueError(
        "Use a country name, two-letter country code, or ISO subdivision code such as US-DE"
    )


def _jurisdiction_matches(candidate: Any, jurisdiction: Dict[str, str]) -> bool:
    candidate_code = str(candidate or "").strip().upper()
    requested_code = jurisdiction["code"]
    if requested_code == jurisdiction["country_code"]:
        return candidate_code == requested_code or candidate_code.startswith(
            f"{requested_code}-"
        )
    return candidate_code == requested_code


def _bounded_address(value: Any) -> Dict[str, Any]:
    address = value if isinstance(value, dict) else {}
    lines = []
    for raw in list(address.get("addressLines") or [])[:4]:
        line = _bounded_text(raw, limit=300)
        if line:
            lines.append(line)
    return {
        "lines": lines,
        "city": _bounded_text(address.get("city"), limit=200),
        "region": _bounded_text(address.get("region"), limit=40),
        "country": _bounded_text(address.get("country"), limit=2).upper(),
        "postal_code": _bounded_text(address.get("postalCode"), limit=40),
    }


def normalize_gleif_legal_entities(
    affiliation_name: str,
    jurisdiction: Dict[str, str],
    payload: Any,
) -> List[Dict[str, Any]]:
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError("GLEIF returned an invalid legal-entity document")
    query_identity = _affiliation_identity(affiliation_name)
    candidates = []
    seen = set()
    for raw in rows[:MAX_GLEIF_SEARCH_ROWS]:
        if not isinstance(raw, dict):
            continue
        attributes = raw.get("attributes") if isinstance(raw.get("attributes"), dict) else {}
        entity = attributes.get("entity") if isinstance(attributes.get("entity"), dict) else {}
        registration = (
            attributes.get("registration")
            if isinstance(attributes.get("registration"), dict)
            else {}
        )
        lei = str(attributes.get("lei") or raw.get("id") or "").strip().upper()
        legal_name_record = (
            entity.get("legalName") if isinstance(entity.get("legalName"), dict) else {}
        )
        legal_name = _bounded_text(legal_name_record.get("name"), limit=500)
        legal_jurisdiction = _bounded_text(entity.get("jurisdiction"), limit=20).upper()
        if (
            not _LEI_PATTERN.fullmatch(lei)
            or lei in seen
            or not legal_name
            or not _jurisdiction_matches(legal_jurisdiction, jurisdiction)
        ):
            continue
        other_names = []
        exact_name_match = _affiliation_identity(legal_name) == query_identity
        for other in list(entity.get("otherNames") or [])[:20]:
            if not isinstance(other, dict):
                continue
            name = _bounded_text(other.get("name"), limit=500)
            if name and name not in other_names:
                other_names.append(name)
                if _affiliation_identity(name) == query_identity:
                    exact_name_match = True
        registered_at = entity.get("registeredAt")
        registered_at = registered_at if isinstance(registered_at, dict) else {}
        registered_as = _bounded_text(entity.get("registeredAs"), limit=100)
        source_url = f"https://api.gleif.org/api/v1/lei-records/{lei}"
        seen.add(lei)
        candidates.append(
            {
                "id": lei,
                "identifier_type": "lei",
                "legal_name": legal_name,
                "other_names": other_names[:10],
                "legal_jurisdiction": legal_jurisdiction,
                "jurisdiction_label": jurisdiction["label"],
                "registered_at": _bounded_text(registered_at.get("id"), limit=40),
                "registered_as": registered_as,
                "entity_status": _bounded_text(entity.get("status"), limit=40),
                "registration_status": _bounded_text(
                    registration.get("status"), limit=40
                ),
                "corroboration_level": _bounded_text(
                    registration.get("corroborationLevel"), limit=60
                ),
                "initial_registration_date": _bounded_text(
                    registration.get("initialRegistrationDate"), limit=40
                ),
                "last_update_date": _bounded_text(
                    registration.get("lastUpdateDate"), limit=40
                ),
                "legal_address": _bounded_address(entity.get("legalAddress")),
                "headquarters_address": _bounded_address(
                    entity.get("headquartersAddress")
                ),
                "exact_name_match": exact_name_match,
                "source_url": source_url,
            }
        )
    candidates.sort(key=lambda candidate: not candidate["exact_name_match"])
    return candidates[:MAX_GLEIF_CANDIDATES]


def normalize_fr_business_entities(
    affiliation_name: str,
    jurisdiction: Dict[str, str],
    payload: Any,
) -> List[Dict[str, Any]]:
    rows = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError("The French business registry returned an invalid document")
    query_identity = _affiliation_identity(affiliation_name)
    candidates = []
    seen = set()
    for raw in rows[:20]:
        if not isinstance(raw, dict):
            continue
        siren = str(raw.get("siren") or "").strip()
        legal_name = _bounded_text(
            raw.get("nom_raison_sociale") or raw.get("nom_complet"), limit=500
        )
        if not _SIREN_PATTERN.fullmatch(siren) or siren in seen or not legal_name:
            continue
        headquarters = raw.get("siege") if isinstance(raw.get("siege"), dict) else {}
        siret = str(headquarters.get("siret") or "").strip()
        if not _SIRET_PATTERN.fullmatch(siret):
            siret = ""
        establishments = []
        seen_establishments = set()
        for establishment in list(raw.get("matching_etablissements") or [])[:20]:
            if not isinstance(establishment, dict):
                continue
            establishment_siret = str(establishment.get("siret") or "").strip()
            if (
                not _SIRET_PATTERN.fullmatch(establishment_siret)
                or establishment_siret in seen_establishments
            ):
                continue
            establishment_address = _bounded_text(
                establishment.get("adresse"), limit=500
            )
            if not establishment_address:
                continue
            seen_establishments.add(establishment_siret)
            establishments.append(
                {
                    "siret": establishment_siret,
                    "address": establishment_address,
                    "city": _bounded_text(
                        establishment.get("libelle_commune")
                        or establishment.get("libelle_commune_etranger"),
                        limit=200,
                    ),
                    "postal_code": _bounded_text(
                        establishment.get("code_postal"), limit=40
                    ),
                    "status": (
                        "active"
                        if establishment.get("etat_administratif") == "A"
                        else "ceased"
                    ),
                }
            )
            if len(establishments) >= 5:
                break
        people = []
        people_seen = set()
        for leader in list(raw.get("dirigeants") or [])[:50]:
            if (
                not isinstance(leader, dict)
                or leader.get("type_dirigeant") != "personne physique"
            ):
                continue
            family_name = _bounded_text(leader.get("nom"), limit=200)
            given_names = _bounded_text(leader.get("prenoms"), limit=200)
            display_name = " ".join(part for part in (given_names, family_name) if part)
            role = _bounded_text(leader.get("qualite"), limit=300)
            identity = _affiliation_identity(display_name)
            if not display_name or not role or identity in people_seen:
                continue
            people_seen.add(identity)
            people.append({"display_name": display_name, "role": role})
            if len(people) >= MAX_FR_BUSINESS_PEOPLE:
                break
        seen.add(siren)
        candidates.append(
            {
                "id": siren,
                "identifier_type": "siren",
                "legal_name": legal_name,
                "legal_jurisdiction": jurisdiction["code"],
                "jurisdiction_label": jurisdiction["label"],
                "registered_at": "French National Enterprise Register",
                "registered_as": siren,
                "headquarters_identifier": siret,
                "entity_status": (
                    "active" if raw.get("etat_administratif") == "A" else "ceased"
                ),
                "creation_date": _bounded_text(raw.get("date_creation"), limit=40),
                "last_update_date": _bounded_text(raw.get("date_mise_a_jour"), limit=40),
                "legal_form_code": _bounded_text(raw.get("nature_juridique"), limit=40),
                "primary_activity_code": _bounded_text(
                    raw.get("activite_principale")
                    or headquarters.get("activite_principale"),
                    limit=40,
                ),
                "primary_activity_label": _bounded_text(
                    raw.get("libelle_activite_principale")
                    or headquarters.get("libelle_activite_principale"),
                    limit=500,
                ),
                "employee_band": _bounded_text(
                    raw.get("tranche_effectif_salarie"), limit=100
                ),
                "legal_address": {
                    "lines": [
                        _bounded_text(headquarters.get("adresse"), limit=500)
                    ]
                    if headquarters.get("adresse")
                    else [],
                    "city": _bounded_text(
                        headquarters.get("libelle_commune")
                        or headquarters.get("libelle_commune_etranger"),
                        limit=200,
                    ),
                    "region": _bounded_text(headquarters.get("region"), limit=40),
                    "country": "FR",
                    "postal_code": _bounded_text(
                        headquarters.get("code_postal"), limit=40
                    ),
                },
                "establishments": establishments,
                "people": people,
                "exact_name_match": _affiliation_identity(legal_name) == query_identity,
                "source_url": (
                    f"https://annuaire-entreprises.data.gouv.fr/entreprise/{siren}"
                ),
            }
        )
    candidates.sort(key=lambda candidate: not candidate["exact_name_match"])
    return candidates[:MAX_FR_BUSINESS_CANDIDATES]


def _wikidata_item_url(entity_id: Any) -> str:
    entity_id = str(entity_id or "").strip().upper()
    if not _WIKIDATA_ID_PATTERN.fullmatch(entity_id):
        return ""
    return f"https://www.wikidata.org/wiki/{entity_id}"


def normalize_wikidata_entity_candidates(
    affiliation_name: str, raw_payload: Any
) -> List[Dict[str, Any]]:
    query_identity = _affiliation_identity(affiliation_name)
    if not isinstance(raw_payload, dict) or not isinstance(
        raw_payload.get("search"), list
    ):
        raise ValueError("Wikidata entity search returned an invalid document")
    candidates = []
    seen = set()
    for raw in raw_payload["search"][:MAX_WIKIDATA_ENTITY_CANDIDATES]:
        if not isinstance(raw, dict):
            continue
        entity_id = str(raw.get("id") or "").strip().upper()
        label = _bounded_text(raw.get("label"), limit=500)
        if (
            not _WIKIDATA_ID_PATTERN.fullmatch(entity_id)
            or entity_id in seen
            or not label
        ):
            continue
        match = raw.get("match") if isinstance(raw.get("match"), dict) else {}
        matched_text = _bounded_text(match.get("text"), limit=500)
        exact_label = _affiliation_identity(label) == query_identity
        exact_alias = bool(
            matched_text and _affiliation_identity(matched_text) == query_identity
        )
        seen.add(entity_id)
        candidates.append(
            {
                "id": entity_id,
                "label": label,
                "description": _bounded_text(raw.get("description"), limit=1000),
                "url": _wikidata_item_url(entity_id),
                "exact_match": exact_label or exact_alias,
                "match_type": (
                    "label" if exact_label else "alias" if exact_alias else "related"
                ),
            }
        )
    return candidates


def _wikidata_claim_values(entity: Dict[str, Any], property_id: str) -> List[Any]:
    claims = entity.get("claims") if isinstance(entity.get("claims"), dict) else {}
    output = []
    for statement in list(claims.get(property_id) or [])[:100]:
        if not isinstance(statement, dict) or statement.get("rank") == "deprecated":
            continue
        mainsnak = statement.get("mainsnak")
        datavalue = mainsnak.get("datavalue") if isinstance(mainsnak, dict) else None
        value = datavalue.get("value") if isinstance(datavalue, dict) else None
        if value is not None:
            output.append(value)
    return output


def _wikidata_language_value(entity: Dict[str, Any], field: str, limit: int) -> str:
    values = entity.get(field) if isinstance(entity.get(field), dict) else {}
    preferred = values.get("en") if isinstance(values.get("en"), dict) else None
    if preferred:
        return _bounded_text(preferred.get("value"), limit=limit)
    for item in values.values():
        if isinstance(item, dict) and item.get("value"):
            return _bounded_text(item.get("value"), limit=limit)
    return ""


def normalize_wikidata_organization(entity_id: str, payload: Any) -> Dict[str, Any]:
    entity_id = str(entity_id or "").strip().upper()
    entities = payload.get("entities") if isinstance(payload, dict) else None
    entity = entities.get(entity_id) if isinstance(entities, dict) else None
    if not _WIKIDATA_ID_PATTERN.fullmatch(entity_id) or not isinstance(entity, dict):
        raise ValueError("The selected Wikidata organization is unavailable")
    label = _wikidata_language_value(entity, "labels", 500)
    if not label:
        raise ValueError("The selected Wikidata organization has no usable label")
    websites = []
    for raw in _wikidata_claim_values(entity, "P856")[:20]:
        value = _safe_public_url(raw)
        if value and value not in websites:
            websites.append(value)
        if len(websites) >= 5:
            break
    instance_of = []
    for raw in _wikidata_claim_values(entity, "P31"):
        value = raw.get("id") if isinstance(raw, dict) else None
        value = str(value or "").strip().upper()
        if _WIKIDATA_ID_PATTERN.fullmatch(value) and value not in instance_of:
            instance_of.append(value)
    return {
        "id": entity_id,
        "label": label,
        "description": _wikidata_language_value(entity, "descriptions", 1000),
        "url": _wikidata_item_url(entity_id),
        "official_websites": websites,
        "instance_of": instance_of[:20],
    }


def _wikidata_binding_id(value: Any, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, dict) or value.get("type") != "uri":
        return ""
    match = pattern.fullmatch(str(value.get("value") or ""))
    return match.group(1) if match else ""


def _wikidata_binding_text(value: Any, *, limit: int) -> str:
    if not isinstance(value, dict) or value.get("type") not in {"literal", "typed-literal"}:
        return ""
    return _bounded_text(value.get("value"), limit=limit)


def normalize_wikidata_affiliated_people(payload: Any) -> List[Dict[str, Any]]:
    results = payload.get("results") if isinstance(payload, dict) else None
    bindings = results.get("bindings") if isinstance(results, dict) else None
    if not isinstance(bindings, list):
        raise ValueError("Wikidata affiliation query returned invalid bindings")
    people: Dict[str, Dict[str, Any]] = {}
    for binding in bindings[:MAX_WIKIDATA_AFFILIATION_ROWS]:
        if not isinstance(binding, dict):
            continue
        person_id = _wikidata_binding_id(
            binding.get("person"), _WIKIDATA_ENTITY_URL_PATTERN
        )
        property_id = _wikidata_binding_id(
            binding.get("property"), _WIKIDATA_PROPERTY_URL_PATTERN
        )
        direction = _wikidata_binding_text(binding.get("direction"), limit=40)
        label = _wikidata_binding_text(binding.get("personLabel"), limit=500)
        if (
            not person_id
            or not label
            or property_id not in _WIKIDATA_RELATIONSHIPS
            or direction not in {"person_to_organization", "organization_to_person"}
        ):
            continue
        person = people.get(person_id)
        if person is None:
            if len(people) >= MAX_WIKIDATA_AFFILIATED_PEOPLE:
                continue
            person = {
                "id": person_id,
                "label": label,
                "description": _wikidata_binding_text(
                    binding.get("personDescription"), limit=1000
                ),
                "url": _wikidata_item_url(person_id),
                "relations": [],
            }
            people[person_id] = person
        relation = {
            "property_id": property_id,
            "label": _WIKIDATA_RELATIONSHIPS[property_id],
            "direction": direction,
        }
        if relation not in person["relations"]:
            person["relations"].append(relation)
    return list(people.values())[:MAX_WIKIDATA_AFFILIATED_PEOPLE]


def _wikidata_people_query(entity_id: str) -> str:
    if not _WIKIDATA_ID_PATTERN.fullmatch(entity_id):
        raise ValueError("Invalid Wikidata organization identifier")
    return f"""
SELECT DISTINCT ?person ?personLabel ?personDescription ?property ?direction WHERE {{
  {{
    SELECT DISTINCT ?person WHERE {{
      {{
        VALUES ?candidateProperty {{ wdt:P69 wdt:P108 wdt:P463 wdt:P1416 }}
        ?person ?candidateProperty wd:{entity_id} .
      }} UNION {{
        VALUES ?candidateProperty {{ wdt:P112 wdt:P169 wdt:P488 wdt:P1037 wdt:P3320 }}
        wd:{entity_id} ?candidateProperty ?person .
      }}
      ?person wdt:P31 wd:Q5 .
    }}
    ORDER BY ?person
    LIMIT {MAX_WIKIDATA_AFFILIATED_PEOPLE}
  }}
  {{
    VALUES ?property {{ wdt:P69 wdt:P108 wdt:P463 wdt:P1416 }}
    ?person ?property wd:{entity_id} .
    BIND("person_to_organization" AS ?direction)
  }} UNION {{
    VALUES ?property {{ wdt:P112 wdt:P169 wdt:P488 wdt:P1037 wdt:P3320 }}
    wd:{entity_id} ?property ?person .
    BIND("organization_to_person" AS ?direction)
  }}
  ?person wdt:P31 wd:Q5 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY ?person ?property ?direction
LIMIT {MAX_WIKIDATA_AFFILIATION_ROWS}
""".strip()


async def _bounded_wikidata_json(session: Any, url: str, *, params: Any):
    async with session.get(url, params=params, allow_redirects=False) as response:
        retry_after = str(response.headers.get("Retry-After") or "")[:40]
        if response.status in {403, 429}:
            return "rate_limited", None, retry_after
        if response.status == 404:
            return "not_found", None, retry_after
        if response.status != 200:
            raise RuntimeError(
                f"Wikidata public service returned HTTP {int(response.status)}"
            )
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                declared_length = int(content_length)
            except (TypeError, ValueError) as error:
                raise RuntimeError("Wikidata returned an invalid response length") from error
            if declared_length < 0 or declared_length > WIKIDATA_MAX_RESPONSE_BYTES:
                raise RuntimeError("Wikidata returned an oversized response")
        body = await response.content.read(WIKIDATA_MAX_RESPONSE_BYTES + 1)
        if len(body) > WIKIDATA_MAX_RESPONSE_BYTES:
            raise RuntimeError("Wikidata returned an oversized response")
    try:
        return "ok", json.loads(body), retry_after
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("Wikidata returned invalid JSON") from error


async def _bounded_registry_json(
    session: Any,
    url: str,
    *,
    params: Any,
    maximum_bytes: int,
    source_name: str,
):
    async with session.get(url, params=params, allow_redirects=False) as response:
        retry_after = str(response.headers.get("Retry-After") or "")[:40]
        if response.status in {403, 429}:
            return "rate_limited", None, retry_after
        if response.status == 404:
            return "not_found", None, retry_after
        if response.status != 200:
            raise RuntimeError(
                f"{source_name} public service returned HTTP {int(response.status)}"
            )
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
    try:
        return "ok", json.loads(body), retry_after
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"{source_name} returned invalid JSON") from error


def _wikidata_diagnostic(name: str, status: str, reason: str, candidates=None):
    return {
        "source_engine": WIKIDATA_ENGINE,
        "subject_type": "affiliation",
        "subject_value": name,
        "status": status,
        "source_url": "https://www.wikidata.org/",
        "source_record_id": f"wikidata-search:{claim_fingerprint('company', name)}",
        "reason": reason[:1000],
        "organization_candidates": list(candidates or [])[:5],
        "organization": None,
        "people": [],
        "extra": {"human_review_required": True, "automatic_approval_allowed": False},
    }


async def run_wikidata_affiliation_discovery(
    affiliation_name: str,
    *,
    selected_entity_id: Optional[str] = None,
    timeout_seconds: int = WIKIDATA_TIMEOUT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    affiliation_name = normalize_affiliation_name(affiliation_name)
    selected_entity_id = str(selected_entity_id or "").strip().upper()
    if selected_entity_id and not _WIKIDATA_ID_PATTERN.fullmatch(selected_entity_id):
        raise ValueError("Invalid selected Wikidata organization identifier")
    session_factory = session_factory or aiohttp.ClientSession
    timeout = aiohttp.ClientTimeout(total=max(1, min(int(timeout_seconds), 45)))
    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "OpenLedger-OSINT-Enrichment/1.0 "
            "(+https://github.com/nexorusio/openledger)"
        ),
    }
    candidates = []
    async with session_factory(timeout=timeout, headers=headers) as session:
        if not selected_entity_id:
            status, payload, _retry_after = await _bounded_wikidata_json(
                session,
                WIKIDATA_API_URL,
                params={
                    "action": "wbsearchentities",
                    "search": affiliation_name,
                    "language": "en",
                    "uselang": "en",
                    "type": "item",
                    "limit": MAX_WIKIDATA_ENTITY_CANDIDATES,
                    "format": "json",
                    "formatversion": "2",
                },
            )
            if status != "ok":
                return _wikidata_diagnostic(
                    affiliation_name, status, "Wikidata organization lookup was unavailable."
                )
            candidates = normalize_wikidata_entity_candidates(affiliation_name, payload)
            exact = [candidate for candidate in candidates if candidate["exact_match"]]
            if len(exact) != 1:
                return _wikidata_diagnostic(
                    affiliation_name,
                    "needs_selection" if candidates else "not_found",
                    (
                        "Select the correct Wikidata organization before people are extracted."
                        if candidates
                        else "Wikidata returned no usable organization candidates."
                    ),
                    candidates,
                )
            selected_entity_id = exact[0]["id"]

        status, payload, _retry_after = await _bounded_wikidata_json(
            session,
            WIKIDATA_API_URL,
            params={
                "action": "wbgetentities",
                "ids": selected_entity_id,
                "props": "labels|descriptions|claims",
                "languages": "en",
                "languagefallback": "1",
                "format": "json",
                "formatversion": "2",
            },
        )
        if status != "ok":
            return _wikidata_diagnostic(
                affiliation_name, status, "The selected organization was unavailable.", candidates
            )
        organization = normalize_wikidata_organization(selected_entity_id, payload)
        status, payload, _retry_after = await _bounded_wikidata_json(
            session,
            WIKIDATA_QUERY_URL,
            params={"query": _wikidata_people_query(selected_entity_id), "format": "json"},
        )
        if status != "ok":
            result = _wikidata_diagnostic(
                affiliation_name,
                status,
                "The organization resolved, but affiliation relations were unavailable.",
                candidates,
            )
            result["organization"] = organization
            return result
        people = normalize_wikidata_affiliated_people(payload)
    return {
        "source_engine": WIKIDATA_ENGINE,
        "subject_type": "affiliation",
        "subject_value": affiliation_name,
        "status": "observed",
        "source_url": organization["url"],
        "source_record_id": f"wikidata-organization:{selected_entity_id}",
        "reason": "Explicit public Wikidata affiliation statements.",
        "organization_candidates": candidates,
        "organization": organization,
        "people": people,
        "extra": {"human_review_required": True, "automatic_approval_allowed": False},
    }


def _registry_observation(
    *,
    source_engine: str,
    source_url: str,
    affiliation_name: str,
    jurisdiction: Dict[str, str],
    status: str,
    reason: str,
    candidates: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    candidates = list(candidates or [])[:5]
    exact = [candidate for candidate in candidates if candidate.get("exact_name_match")]
    selected_entity = exact[0] if len(exact) == 1 else None
    return {
        "source_engine": source_engine,
        "subject_type": "legal_entity",
        "subject_value": affiliation_name,
        "jurisdiction": dict(jurisdiction),
        "status": status,
        "source_url": source_url,
        "source_record_id": (
            f"{source_engine}-search:"
            f"{claim_fingerprint('company', jurisdiction['code'] + ':' + affiliation_name)}"
        ),
        "reason": reason[:1000],
        "candidates": candidates,
        "selected_entity": selected_entity,
        "extra": {
            "human_review_required": True,
            "automatic_approval_allowed": False,
            "zero_result_scope": source_engine,
        },
    }


async def run_gleif_legal_entity_search(
    affiliation_name: str,
    jurisdiction: Any,
    *,
    timeout_seconds: int = GLEIF_TIMEOUT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    affiliation_name = normalize_affiliation_name(affiliation_name)
    normalized_jurisdiction = (
        jurisdiction
        if isinstance(jurisdiction, dict)
        else normalize_legal_jurisdiction(jurisdiction)
    )
    if not isinstance(normalized_jurisdiction, dict):
        raise ValueError("A legal jurisdiction is required for registry search")
    normalized_jurisdiction = normalize_legal_jurisdiction(
        normalized_jurisdiction.get("code")
    )
    session_factory = session_factory or aiohttp.ClientSession
    timeout = aiohttp.ClientTimeout(total=max(1, min(int(timeout_seconds), 45)))
    headers = {
        "Accept": "application/vnd.api+json",
        "User-Agent": (
            "OpenLedger-OSINT-Enrichment/1.0 "
            "(+https://github.com/nexorusio/openledger)"
        ),
    }
    async with session_factory(timeout=timeout, headers=headers) as session:
        status, payload, _retry_after = await _bounded_registry_json(
            session,
            GLEIF_API_URL,
            params={
                "filter[entity.legalName]": affiliation_name,
                "filter[entity.legalAddress.country]": normalized_jurisdiction[
                    "country_code"
                ],
                "page[number]": 1,
                "page[size]": MAX_GLEIF_SEARCH_ROWS,
            },
            maximum_bytes=GLEIF_MAX_RESPONSE_BYTES,
            source_name="GLEIF",
        )
    if status != "ok":
        return _registry_observation(
            source_engine=GLEIF_ENGINE,
            source_url=GLEIF_API_URL,
            affiliation_name=affiliation_name,
            jurisdiction=normalized_jurisdiction,
            status=status,
            reason="The GLEIF legal-entity index was unavailable.",
        )
    candidates = normalize_gleif_legal_entities(
        affiliation_name, normalized_jurisdiction, payload
    )
    return _registry_observation(
        source_engine=GLEIF_ENGINE,
        source_url=GLEIF_API_URL,
        affiliation_name=affiliation_name,
        jurisdiction=normalized_jurisdiction,
        status="observed" if candidates else "not_found",
        reason=(
            "Jurisdiction-matched public LEI records."
            if candidates
            else (
                "GLEIF returned no jurisdiction-matched LEI record. This does not "
                "prove that the entity is not registered."
            )
        ),
        candidates=candidates,
    )


async def run_fr_business_registry_search(
    affiliation_name: str,
    jurisdiction: Any,
    *,
    timeout_seconds: int = FR_BUSINESS_REGISTRY_TIMEOUT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    affiliation_name = normalize_affiliation_name(affiliation_name)
    normalized_jurisdiction = (
        jurisdiction
        if isinstance(jurisdiction, dict)
        else normalize_legal_jurisdiction(jurisdiction)
    )
    if not isinstance(normalized_jurisdiction, dict):
        raise ValueError("A legal jurisdiction is required for registry search")
    normalized_jurisdiction = normalize_legal_jurisdiction(
        normalized_jurisdiction.get("code")
    )
    if normalized_jurisdiction["code"] != "FR":
        raise ValueError(
            "The French registry adapter accepts only the country-level FR jurisdiction"
        )
    session_factory = session_factory or aiohttp.ClientSession
    timeout = aiohttp.ClientTimeout(total=max(1, min(int(timeout_seconds), 45)))
    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "OpenLedger-OSINT-Enrichment/1.0 "
            "(+https://github.com/nexorusio/openledger)"
        ),
    }
    async with session_factory(timeout=timeout, headers=headers) as session:
        status, payload, _retry_after = await _bounded_registry_json(
            session,
            FR_BUSINESS_REGISTRY_URL,
            params={"q": affiliation_name, "page": 1, "per_page": 20},
            maximum_bytes=FR_BUSINESS_REGISTRY_MAX_RESPONSE_BYTES,
            source_name="French business registry",
        )
    if status != "ok":
        return _registry_observation(
            source_engine=FR_BUSINESS_REGISTRY_ENGINE,
            source_url=FR_BUSINESS_REGISTRY_URL,
            affiliation_name=affiliation_name,
            jurisdiction=normalized_jurisdiction,
            status=status,
            reason="The French national business search was unavailable.",
        )
    candidates = normalize_fr_business_entities(
        affiliation_name, normalized_jurisdiction, payload
    )
    return _registry_observation(
        source_engine=FR_BUSINESS_REGISTRY_ENGINE,
        source_url=FR_BUSINESS_REGISTRY_URL,
        affiliation_name=affiliation_name,
        jurisdiction=normalized_jurisdiction,
        status="observed" if candidates else "not_found",
        reason=(
            "Public records from the French National Enterprise Directory."
            if candidates
            else "The French public business search returned no matching entity."
        ),
        candidates=candidates,
    )


def normalize_cloudflare_dns_context(
    website: Any, payloads: Any
) -> Dict[str, Any]:
    normalized_website = (
        website
        if isinstance(website, dict)
        else normalize_official_website_url(website)
    )
    if not isinstance(normalized_website, dict):
        raise ValueError("An official website is required for DNS context")
    if not isinstance(payloads, dict):
        raise ValueError("Cloudflare DNS returned an invalid response set")
    domain = normalized_website["domain"]
    dns_types = {"A": 1, "NS": 2, "MX": 15, "AAAA": 28}
    records: Dict[str, List[Dict[str, Any]]] = {
        query_type.casefold(): [] for query_type in CLOUDFLARE_DNS_QUERY_TYPES
    }
    total = 0
    for query_type in CLOUDFLARE_DNS_QUERY_TYPES:
        payload = payloads.get(query_type)
        if not isinstance(payload, dict) or not isinstance(payload.get("Status"), int):
            continue
        answers = payload.get("Answer") or []
        if not isinstance(answers, list):
            continue
        seen = set()
        for answer in answers[:MAX_DNS_RECORDS_PER_TYPE]:
            if not isinstance(answer, dict) or answer.get("type") != dns_types[query_type]:
                continue
            owner = _normalize_dns_hostname(answer.get("name")) or domain
            raw_value = _bounded_text(answer.get("data"), limit=1000)
            priority = None
            if query_type in {"A", "AAAA"}:
                try:
                    address = ipaddress.ip_address(raw_value)
                except ValueError:
                    continue
                if (
                    (query_type == "A" and address.version != 4)
                    or (query_type == "AAAA" and address.version != 6)
                    or not address.is_global
                    or address.is_multicast
                    or address.is_reserved
                ):
                    continue
                value = str(address)
            elif query_type == "MX":
                match = re.fullmatch(r"([0-9]{1,5})\s+(.+)", raw_value)
                if not match:
                    continue
                priority = int(match.group(1))
                value = _normalize_dns_hostname(match.group(2))
                if not value:
                    continue
            else:
                value = _normalize_dns_hostname(raw_value)
                if not value:
                    continue
            identity = (owner, value, priority)
            if identity in seen:
                continue
            seen.add(identity)
            try:
                ttl = int(answer.get("TTL") or 0)
            except (TypeError, ValueError):
                ttl = 0
            record = {
                "owner": owner,
                "value": value,
                "ttl": max(0, min(ttl, 2_147_483_647)),
            }
            if priority is not None:
                record["priority"] = priority
            records[query_type.casefold()].append(record)
            total += 1
            if total >= MAX_DNS_RECORDS_TOTAL:
                break
        if total >= MAX_DNS_RECORDS_TOTAL:
            break
    return {
        "website_url": normalized_website["url"],
        "domain": domain,
        "records": records,
        "record_count": total,
        "registration_lookup_url": (
            "https://lookup.icann.org/en/lookup?name=" + quote(domain, safe="")
        ),
    }


async def run_cloudflare_dns_context(
    website: Any,
    *,
    timeout_seconds: int = CLOUDFLARE_DNS_TIMEOUT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    normalized_website = (
        website
        if isinstance(website, dict)
        else normalize_official_website_url(website)
    )
    if not isinstance(normalized_website, dict):
        raise ValueError("An official website is required for DNS context")
    normalized_website = normalize_official_website_url(normalized_website.get("url"))
    session_factory = session_factory or aiohttp.ClientSession
    timeout = aiohttp.ClientTimeout(total=max(1, min(int(timeout_seconds), 30)))
    headers = {
        "Accept": "application/dns-json",
        "User-Agent": (
            "OpenLedger-OSINT-Enrichment/1.0 "
            "(+https://github.com/nexorusio/openledger)"
        ),
    }
    payloads = {}
    failures = []
    rate_limited = False
    async with session_factory(timeout=timeout, headers=headers) as session:
        for query_type in CLOUDFLARE_DNS_QUERY_TYPES:
            try:
                status, payload, _retry_after = await _bounded_registry_json(
                    session,
                    CLOUDFLARE_DNS_URL,
                    params={"name": normalized_website["domain"], "type": query_type},
                    maximum_bytes=CLOUDFLARE_DNS_MAX_RESPONSE_BYTES,
                    source_name="Cloudflare DNS",
                )
            except Exception:
                failures.append(query_type)
                continue
            if status != "ok":
                failures.append(query_type)
                rate_limited = rate_limited or status == "rate_limited"
                continue
            payloads[query_type] = payload
    normalized = normalize_cloudflare_dns_context(normalized_website, payloads)
    if normalized["record_count"]:
        status = "partial" if failures else "observed"
        reason = (
            "Current public DNS records were collected; some record types were unavailable."
            if failures
            else "Current public DNS records for the supplied official website domain."
        )
    elif payloads:
        status = "partial" if failures else "not_found"
        reason = (
            "No usable A, AAAA, MX or NS records were returned; some queries were unavailable."
            if failures
            else "No usable A, AAAA, MX or NS records were returned."
        )
    else:
        status = "rate_limited" if rate_limited else "unavailable"
        reason = "The public DNS context source was unavailable."
    return {
        "source_engine": CLOUDFLARE_DNS_ENGINE,
        "subject_type": "organization_domain",
        "subject_value": normalized_website["domain"],
        "status": status,
        "source_url": CLOUDFLARE_DNS_URL,
        "source_record_id": (
            "cloudflare-dns:"
            f"{claim_fingerprint('website', normalized_website['domain'])}"
        ),
        "reason": reason,
        **normalized,
        "query_failures": failures,
        "extra": {
            "human_review_required": True,
            "automatic_approval_allowed": False,
            "operating_location_inference_allowed": False,
        },
    }


def _address_summary(address: Any) -> str:
    if not isinstance(address, dict):
        return ""
    parts = [
        *list(address.get("lines") or [])[:4],
        address.get("city"),
        address.get("region"),
        address.get("postal_code"),
        address.get("country"),
    ]
    return ", ".join(
        dict.fromkeys(_bounded_text(part, limit=500) for part in parts if part)
    )[:1500]


def build_business_context_assessment(
    registry_observations: Any,
    *,
    website: Any = None,
    website_source: str = "",
    dns_observation: Any = None,
) -> List[Dict[str, Any]]:
    """Build explicit, source-bounded context without inferring operations."""
    findings = []
    source_names = {
        GLEIF_ENGINE: "GLEIF Global LEI Index",
        FR_BUSINESS_REGISTRY_ENGINE: "French National Enterprise Directory",
    }
    for observation in list(registry_observations or [])[:10]:
        if not isinstance(observation, dict):
            continue
        entity = observation.get("selected_entity")
        if not isinstance(entity, dict):
            continue
        source_name = source_names.get(
            observation.get("source_engine"),
            str(observation.get("source_engine") or "Public registry"),
        )
        identifier = f"{str(entity.get('identifier_type') or '').upper()} {entity.get('id')}".strip()
        address = _address_summary(entity.get("legal_address"))
        jurisdiction = _bounded_text(
            entity.get("jurisdiction_label") or entity.get("legal_jurisdiction"),
            limit=200,
        )
        statement = (
            f"{entity.get('legal_name')} has an exact-name candidate record in "
            f"{jurisdiction or 'the selected jurisdiction'}"
        )
        if address:
            statement += f" with a listed legal or registered address at {address}"
        findings.append(
            {
                "category": "registered_legal_context",
                "conclusion": statement + ".",
                "basis": f"Direct public registry record {identifier} from {source_name}.",
                "limitation": (
                    "A legal or registered address does not prove every place where "
                    "the business has staff, customers, assets, or day-to-day operations."
                ),
                "source_name": source_name,
                "source_url": entity.get("source_url") or observation.get("source_url"),
            }
        )
        headquarters = _address_summary(entity.get("headquarters_address"))
        if headquarters and headquarters.casefold() != address.casefold():
            findings.append(
                {
                    "category": "reported_headquarters_context",
                    "conclusion": f"{source_name} lists a headquarters address at {headquarters}.",
                    "basis": f"Structured headquarters field in public record {identifier}.",
                    "limitation": (
                        "A reported headquarters address is source evidence, not proof of "
                        "the full geographic footprint or current physical presence."
                    ),
                    "source_name": source_name,
                    "source_url": entity.get("source_url") or observation.get("source_url"),
                }
            )
        activity = _bounded_text(entity.get("primary_activity_label"), limit=500)
        activity_code = _bounded_text(entity.get("primary_activity_code"), limit=40)
        if activity or activity_code:
            activity_value = activity or "an unlabelled registered activity"
            if activity_code:
                activity_value += f" ({activity_code})"
            findings.append(
                {
                    "category": "registered_activity_context",
                    "conclusion": f"The registry classifies the entity's primary activity as {activity_value}.",
                    "basis": f"Structured primary-activity field in public record {identifier}.",
                    "limitation": (
                        "A registry classification describes the filed principal activity; "
                        "it may not describe every current product, market, or revenue source."
                    ),
                    "source_name": source_name,
                    "source_url": entity.get("source_url") or observation.get("source_url"),
                }
            )
        establishments = list(entity.get("establishments") or [])[:5]
        if establishments:
            locations = "; ".join(
                f"{item.get('address')} ({item.get('status')})"
                for item in establishments
                if isinstance(item, dict) and item.get("address")
            )[:2000]
            if locations:
                findings.append(
                    {
                        "category": "registered_establishment_context",
                        "conclusion": f"The registry search returned establishment records at {locations}.",
                        "basis": f"Bounded public establishment rows linked to {identifier}.",
                        "limitation": (
                            "Registered establishments may be historical, administrative, "
                            "or differently scoped; verify status and current activity."
                        ),
                        "source_name": source_name,
                        "source_url": entity.get("source_url") or observation.get("source_url"),
                    }
                )
    normalized_website = None
    try:
        normalized_website = normalize_official_website_url(
            website.get("url") if isinstance(website, dict) else website
        )
    except ValueError:
        pass
    if normalized_website:
        source_label = (
            "analyst-supplied case input"
            if website_source == "operator_input"
            else "the selected Wikidata organization record"
        )
        findings.append(
            {
                "category": "official_website_context",
                "conclusion": f"{normalized_website['domain']} is the website domain supplied for this organization check.",
                "basis": f"Website association from {source_label}.",
                "limitation": (
                    "This association is a research lead. Review the website and corroborate "
                    "its ownership before treating it as an official operating channel."
                ),
                "source_name": source_label,
                "source_url": normalized_website["url"],
            }
        )
    if isinstance(dns_observation, dict):
        records = dns_observation.get("records") or {}
        record_labels = []
        for record_type in ("a", "aaaa", "mx", "ns"):
            values = [
                item.get("value")
                for item in list(records.get(record_type) or [])[:5]
                if isinstance(item, dict) and item.get("value")
            ]
            if values:
                record_labels.append(f"{record_type.upper()}: {', '.join(values)}")
        if record_labels:
            findings.append(
                {
                    "category": "technical_domain_context",
                    "conclusion": (
                        f"{dns_observation.get('domain')} currently advertises "
                        + "; ".join(record_labels)
                        + "."
                    )[:2500],
                    "basis": (
                        "Current bounded A, AAAA, MX and NS queries through "
                        "Cloudflare DNS-over-HTTPS."
                    ),
                    "limitation": (
                        "DNS, hosting, mail and nameserver geography describe technical "
                        "routing only. They do not establish incorporation, ownership, "
                        "staff location, or where the business operates."
                    ),
                    "source_name": "Cloudflare DNS-over-HTTPS",
                    "source_url": dns_observation.get("source_url"),
                }
            )
    return findings[:20]


def _wikidata_claim_candidate(field_name, value, confidence, person, organization, evidence_type, details):
    evidence = {
        "evidence_type": evidence_type,
        "source_name": "Wikidata",
        "source_url": person["url"],
        "details": {
            **details,
            "person_wikidata_id": person["id"],
            "organization_wikidata_id": organization["id"],
            "organization_wikidata_url": organization["url"],
            "official_organization_websites": organization.get("official_websites", [])[:5],
            "human_review_required": True,
            "automatic_approval_allowed": False,
        },
    }
    display_value = (
        str(value.get("identifier") or value.get("url") or "")
        if isinstance(value, dict)
        else _bounded_text(value, limit=4000)
    )
    normalized_value = (
        json.dumps(value, sort_keys=True, ensure_ascii=False)
        if isinstance(value, dict)
        else display_value.casefold()
    )
    return {
        "field_name": field_name,
        "value": value,
        "display_value": display_value,
        "normalized_value": normalized_value,
        "confidence": confidence,
        "fingerprint": claim_fingerprint(field_name, value),
        "source_engine": WIKIDATA_ENGINE,
        "source_record_id": f"wikidata-person:{person['id']}",
        "native_status": "observed",
        "observation_details": {"relations": person.get("relations", [])[:10]},
        "evidence": [dict(evidence, fingerprint=evidence_fingerprint(evidence))],
    }


def extract_wikidata_affiliation_people(observation: Any) -> List[Dict[str, Any]]:
    if (
        not isinstance(observation, dict)
        or observation.get("source_engine") != WIKIDATA_ENGINE
        or observation.get("status") != "observed"
        or not isinstance(observation.get("organization"), dict)
    ):
        return []
    organization = observation["organization"]
    organization_id = str(organization.get("id") or "").strip().upper()
    organization_label = _bounded_text(organization.get("label"), limit=500)
    if (
        not organization_label
        or not _WIKIDATA_ID_PATTERN.fullmatch(organization_id)
        or organization.get("url") != _wikidata_item_url(organization_id)
    ):
        return []
    organization = {**organization, "id": organization_id, "label": organization_label}
    output = []
    seen = set()
    for raw in list(observation.get("people") or [])[:MAX_WIKIDATA_AFFILIATED_PEOPLE]:
        if not isinstance(raw, dict):
            continue
        person_id = str(raw.get("id") or "").strip().upper()
        display_name = _bounded_text(raw.get("label"), limit=500)
        relations = [
            relation
            for relation in list(raw.get("relations") or [])[:10]
            if isinstance(relation, dict)
            and relation.get("property_id") in _WIKIDATA_RELATIONSHIPS
            and relation.get("direction")
            in {"person_to_organization", "organization_to_person"}
        ]
        if (
            not display_name
            or not relations
            or not _WIKIDATA_ID_PATTERN.fullmatch(person_id)
            or person_id in seen
            or _wikidata_item_url(person_id) != raw.get("url")
        ):
            continue
        seen.add(person_id)
        person = {**raw, "id": person_id, "label": display_name}
        identifier = {
            "platform": "Wikidata",
            "identifier_type": "wikidata_item_id",
            "identifier": person_id,
        }
        output.append(
            {
                "wikidata_id": person_id,
                "display_name": display_name,
                "claims": [
                    _wikidata_claim_candidate(
                        "full_name", display_name, 70, person, organization,
                        "wikidata_entity_label", {"description": person.get("description", "")}
                    ),
                    _wikidata_claim_candidate(
                        "company", organization_label, 65, person, organization,
                        "wikidata_affiliation_statement",
                        {"relations": relations, "relation_scope": "Explicit Wikidata statement; may be current or historical."},
                    ),
                    _wikidata_claim_candidate(
                        "platform_identifier", identifier, 70, person, organization,
                        "wikidata_entity_identifier", {"identifier_scope": "Public Wikidata entity identifier."}
                    ),
                ],
            }
        )
    return output


def _fr_registry_claim_candidate(
    field_name: str,
    value: str,
    *,
    person: Dict[str, Any],
    entity: Dict[str, Any],
) -> Dict[str, Any]:
    display_value = _bounded_text(value, limit=4000)
    siren = entity["id"]
    source_record_id = (
        f"fr-siren:{siren}:leader:"
        f"{claim_fingerprint('full_name', person['display_name'])}"
    )
    evidence = {
        "evidence_type": "official_business_registry",
        "source_name": "French National Enterprise Directory",
        "source_url": entity["source_url"],
        "details": {
            "legal_entity_name": entity["legal_name"],
            "legal_jurisdiction": entity["legal_jurisdiction"],
            "siren": siren,
            "siret": entity.get("headquarters_identifier", ""),
            "public_registry_role": person["role"],
            "entity_status": entity.get("entity_status", ""),
            "registry_last_updated_at": entity.get("last_update_date", ""),
            "human_review_required": True,
            "automatic_approval_allowed": False,
        },
    }
    return {
        "field_name": field_name,
        "value": display_value,
        "display_value": display_value,
        "normalized_value": display_value.casefold(),
        "confidence": 90,
        "fingerprint": claim_fingerprint(field_name, display_value),
        "source_engine": FR_BUSINESS_REGISTRY_ENGINE,
        "source_record_id": source_record_id,
        "native_status": "observed",
        "observation_details": {
            "legal_entity_identifier": siren,
            "legal_jurisdiction": entity["legal_jurisdiction"],
            "registry_role": person["role"],
        },
        "evidence": [dict(evidence, fingerprint=evidence_fingerprint(evidence))],
    }


def extract_fr_registry_affiliated_people(observation: Any) -> List[Dict[str, Any]]:
    """Create pending people proposals only for one deterministic exact entity match."""
    if (
        not isinstance(observation, dict)
        or observation.get("source_engine") != FR_BUSINESS_REGISTRY_ENGINE
        or observation.get("status") != "observed"
        or not isinstance(observation.get("selected_entity"), dict)
    ):
        return []
    entity = observation["selected_entity"]
    siren = str(entity.get("id") or "").strip()
    legal_name = _bounded_text(entity.get("legal_name"), limit=500)
    source_url = str(entity.get("source_url") or "")
    if (
        not _SIREN_PATTERN.fullmatch(siren)
        or not legal_name
        or source_url
        != f"https://annuaire-entreprises.data.gouv.fr/entreprise/{siren}"
        or entity.get("exact_name_match") is not True
    ):
        return []
    entity = {**entity, "id": siren, "legal_name": legal_name}
    output = []
    seen = set()
    for raw_person in list(entity.get("people") or [])[:MAX_FR_BUSINESS_PEOPLE]:
        if not isinstance(raw_person, dict):
            continue
        display_name = _bounded_text(raw_person.get("display_name"), limit=500)
        role = _bounded_text(raw_person.get("role"), limit=300)
        identity = _affiliation_identity(display_name)
        if not display_name or not role or identity in seen:
            continue
        seen.add(identity)
        person = {"display_name": display_name, "role": role}
        output.append(
            {
                "registry_person_key": f"fr:{siren}:{identity}",
                "display_name": display_name,
                "claims": [
                    _fr_registry_claim_candidate(
                        "full_name", display_name, person=person, entity=entity
                    ),
                    _fr_registry_claim_candidate(
                        "company", legal_name, person=person, entity=entity
                    ),
                    _fr_registry_claim_candidate(
                        "occupation", role, person=person, entity=entity
                    ),
                ],
            }
        )
    return output


async def _read_bounded_public_json(
    response: Any, *, source_name: str, maximum_bytes: int
) -> Any:
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
    try:
        return json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"{source_name} returned invalid JSON") from error


def normalize_wikipedia_candidates(
    confirmed_name: str, payload: Any
) -> List[Dict[str, Any]]:
    query = payload.get("query") if isinstance(payload, dict) else None
    pages = query.get("pages") if isinstance(query, dict) else None
    if not isinstance(pages, list):
        raise ValueError("Wikipedia returned an invalid page document")
    name_identity = _affiliation_identity(confirmed_name)
    output = []
    seen = set()
    for raw in pages[:MAX_WIKIPEDIA_CANDIDATES]:
        if not isinstance(raw, dict) or raw.get("missing") is True:
            continue
        page_id = raw.get("pageid")
        if not isinstance(page_id, int) or page_id <= 0 or page_id in seen:
            continue
        title = _bounded_text(raw.get("title"), limit=500)
        source_url = _safe_public_url(raw.get("fullurl"))
        if (
            not title
            or not _WIKIPEDIA_PAGE_URL_PATTERN.fullmatch(source_url)
            or raw.get("ns") not in {None, 0}
        ):
            continue
        pageprops = raw.get("pageprops")
        if isinstance(pageprops, dict) and "disambiguation" in pageprops:
            continue
        thumbnail = raw.get("thumbnail")
        thumbnail_url = (
            _safe_public_url(thumbnail.get("source"))
            if isinstance(thumbnail, dict)
            else ""
        )
        if thumbnail_url and urlparse(thumbnail_url).hostname.casefold() not in {
            "upload.wikimedia.org"
        }:
            thumbnail_url = ""
        seen.add(page_id)
        output.append(
            {
                "page_id": str(page_id),
                "title": title,
                "url": source_url,
                "extract": _bounded_text(
                    raw.get("extract"), limit=WIKIPEDIA_MAX_EXTRACT_CHARS
                ),
                "thumbnail_url": thumbnail_url,
                "exact_title_match": _affiliation_identity(title) == name_identity,
            }
        )
    return output


def _wikipedia_diagnostic(
    confirmed_name: str, status: str, reason: str, candidates=None
) -> Dict[str, Any]:
    return {
        "source_engine": WIKIPEDIA_ENGINE,
        "subject_type": "confirmed_person_name",
        "subject_value": confirmed_name,
        "status": status,
        "source_url": "https://en.wikipedia.org/",
        "source_record_id": (
            f"wikipedia-search:{claim_fingerprint('full_name', confirmed_name)}"
        ),
        "reason": reason[:1000],
        "page_candidates": list(candidates or [])[:MAX_WIKIPEDIA_CANDIDATES],
        "page": None,
        "extra": {"human_review_required": True, "automatic_approval_allowed": False},
    }


async def run_wikipedia_person_enrichment(
    confirmed_name: str,
    *,
    selected_page_id: Optional[str] = None,
    timeout_seconds: int = WIKIPEDIA_TIMEOUT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    confirmed_name = normalize_confirmed_person_name(confirmed_name)
    selected_page_id = str(selected_page_id or "").strip()
    if selected_page_id and (
        not selected_page_id.isdigit() or len(selected_page_id) > 20
    ):
        raise ValueError("Invalid selected Wikipedia page identifier")
    session_factory = session_factory or aiohttp.ClientSession
    timeout = aiohttp.ClientTimeout(total=max(1, min(int(timeout_seconds), 30)))
    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "OpenLedger-OSINT-Enrichment/1.0 "
            "(+https://github.com/nexorusio/openledger)"
        ),
    }
    common_params = {
        "action": "query",
        "prop": "extracts|pageimages|pageprops|info",
        "exintro": "1",
        "explaintext": "1",
        "exchars": str(WIKIPEDIA_MAX_EXTRACT_CHARS),
        "piprop": "thumbnail",
        "pithumbsize": "500",
        "inprop": "url",
        "redirects": "1",
        "format": "json",
        "formatversion": "2",
    }
    if selected_page_id:
        params = {**common_params, "pageids": selected_page_id}
    else:
        params = {
            **common_params,
            "generator": "search",
            "gsrsearch": confirmed_name,
            "gsrnamespace": "0",
            "gsrlimit": str(MAX_WIKIPEDIA_CANDIDATES),
        }
    async with session_factory(timeout=timeout, headers=headers) as session:
        async with session.get(
            WIKIPEDIA_API_URL, params=params, allow_redirects=False
        ) as response:
            if response.status in {403, 429}:
                return _wikipedia_diagnostic(
                    confirmed_name,
                    "rate_limited",
                    "Wikipedia temporarily limited the public lookup.",
                )
            if response.status != 200:
                raise RuntimeError(
                    f"Wikipedia public API returned HTTP {int(response.status)}"
                )
            payload = await _read_bounded_public_json(
                response,
                source_name="Wikipedia",
                maximum_bytes=WIKIPEDIA_MAX_RESPONSE_BYTES,
            )
    candidates = normalize_wikipedia_candidates(confirmed_name, payload)
    if selected_page_id:
        selected = [
            candidate
            for candidate in candidates
            if candidate["page_id"] == selected_page_id
        ]
        if len(selected) != 1:
            return _wikipedia_diagnostic(
                confirmed_name,
                "not_found",
                "The selected Wikipedia page is unavailable or unsuitable.",
            )
        page = selected[0]
    else:
        exact = [candidate for candidate in candidates if candidate["exact_title_match"]]
        if len(exact) != 1:
            return _wikipedia_diagnostic(
                confirmed_name,
                "needs_selection" if candidates else "not_found",
                (
                    "Select the correct Wikipedia biography before proposing details."
                    if candidates
                    else "Wikipedia returned no usable biography candidates."
                ),
                candidates,
            )
        page = exact[0]
    if not page["extract"]:
        return _wikipedia_diagnostic(
            confirmed_name,
            "not_found",
            "The selected Wikipedia page has no usable introductory extract.",
            candidates,
        )
    return {
        "source_engine": WIKIPEDIA_ENGINE,
        "subject_type": "confirmed_person_name",
        "subject_value": confirmed_name,
        "status": "observed",
        "source_url": page["url"],
        "source_record_id": f"wikipedia-page:{page['page_id']}",
        "reason": "Public Wikipedia biography proposed for analyst review.",
        "page_candidates": candidates,
        "page": page,
        "extra": {"human_review_required": True, "automatic_approval_allowed": False},
    }


def normalize_icij_offshore_matches(
    confirmed_name: str, payload: Any
) -> List[Dict[str, Any]]:
    results = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(results, list):
        raise ValueError("ICIJ returned an invalid reconciliation document")
    name_identity = _affiliation_identity(confirmed_name)
    output = []
    seen = set()
    for raw in results[:25]:
        if not isinstance(raw, dict):
            continue
        node_id = str(raw.get("id") or "").strip()
        name = _bounded_text(raw.get("name"), limit=500)
        types = raw.get("types") if isinstance(raw.get("types"), list) else []
        officer_type = any(
            isinstance(item, dict)
            and item.get("id")
            == "https://offshoreleaks.icij.org/schema/oldb/officer"
            for item in types[:10]
        )
        try:
            score = float(raw.get("score"))
        except (TypeError, ValueError):
            continue
        exact = (
            raw.get("match") is True
            and _affiliation_identity(name) == name_identity
            and 0 <= score <= 100
        )
        if (
            not exact
            or not officer_type
            or not _ICIJ_NODE_ID_PATTERN.fullmatch(node_id)
            or node_id in seen
        ):
            continue
        seen.add(node_id)
        output.append(
            {
                "node_id": node_id,
                "name": name,
                "description": _bounded_text(raw.get("description"), limit=1000),
                "score": score,
                "url": f"https://offshoreleaks.icij.org/nodes/{node_id}",
            }
        )
        if len(output) >= MAX_ICIJ_MATCHES:
            break
    return output


async def run_icij_offshore_match(
    confirmed_name: str,
    *,
    timeout_seconds: int = ICIJ_TIMEOUT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    confirmed_name = normalize_confirmed_person_name(confirmed_name)
    session_factory = session_factory or aiohttp.ClientSession
    timeout = aiohttp.ClientTimeout(total=max(1, min(int(timeout_seconds), 45)))
    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "OpenLedger-OSINT-Enrichment/1.0 "
            "(+https://github.com/nexorusio/openledger)"
        ),
    }
    async with session_factory(timeout=timeout, headers=headers) as session:
        async with session.post(
            ICIJ_RECONCILE_URL,
            json={"query": confirmed_name, "type": "Officer", "limit": MAX_ICIJ_MATCHES},
            allow_redirects=False,
        ) as response:
            if response.status in {403, 429}:
                status, payload = "rate_limited", None
            elif response.status == 200:
                status = "ok"
                payload = await _read_bounded_public_json(
                    response,
                    source_name="ICIJ Offshore Leaks",
                    maximum_bytes=ICIJ_MAX_RESPONSE_BYTES,
                )
            else:
                raise RuntimeError(
                    "ICIJ Offshore Leaks reconciliation returned "
                    f"HTTP {int(response.status)}"
                )
    matches = normalize_icij_offshore_matches(confirmed_name, payload) if payload else []
    observation_status = "potential_match" if matches else (
        "rate_limited" if status == "rate_limited" else "no_match"
    )
    return {
        "source_engine": ICIJ_OFFSHORE_ENGINE,
        "subject_type": "confirmed_person_name",
        "subject_value": confirmed_name,
        "status": observation_status,
        "source_url": "https://offshoreleaks.icij.org/",
        "source_record_id": (
            f"icij-offshore-search:{claim_fingerprint('full_name', confirmed_name)}"
        ),
        "reason": (
            "Exact-name candidates require independent identity confirmation. "
            "Database inclusion does not imply illegal or improper conduct."
        ),
        "matches": matches,
        "extra": {"human_review_required": True, "automatic_approval_allowed": False},
    }


def _public_record_claim_candidate(
    field_name: str,
    value: Any,
    display_value: str,
    confidence: int,
    source_engine: str,
    source_record_id: str,
    evidence_type: str,
    source_name: str,
    source_url: str,
    details: Dict[str, Any],
) -> Dict[str, Any]:
    evidence = {
        "evidence_type": evidence_type,
        "source_name": source_name,
        "source_url": source_url,
        "details": {
            **details,
            "human_review_required": True,
            "automatic_approval_allowed": False,
        },
    }
    return {
        "field_name": field_name,
        "value": value,
        "display_value": display_value,
        "normalized_value": (
            json.dumps(value, sort_keys=True, ensure_ascii=False)
            if isinstance(value, dict)
            else display_value.casefold()
        ),
        "confidence": confidence,
        "fingerprint": claim_fingerprint(field_name, value),
        "source_engine": source_engine,
        "source_record_id": source_record_id,
        "native_status": "observed",
        "evidence": [dict(evidence, fingerprint=evidence_fingerprint(evidence))],
    }


def extract_wikipedia_person_claims(observation: Any) -> List[Dict[str, Any]]:
    if (
        not isinstance(observation, dict)
        or observation.get("source_engine") != WIKIPEDIA_ENGINE
        or observation.get("status") != "observed"
        or not isinstance(observation.get("page"), dict)
    ):
        return []
    page = observation["page"]
    page_id = str(page.get("page_id") or "")
    title = _bounded_text(page.get("title"), limit=500)
    source_url = _safe_public_url(page.get("url"))
    extract = _bounded_text(page.get("extract"), limit=WIKIPEDIA_MAX_EXTRACT_CHARS)
    if (
        not page_id.isdigit()
        or not title
        or not extract
        or not _WIKIPEDIA_PAGE_URL_PATTERN.fullmatch(source_url)
    ):
        return []
    details = {
        "wikipedia_page_id": page_id,
        "wikipedia_title": title,
        "identity_scope": "Confirmed name lookup; article attribution still requires review.",
    }
    claims = [
        _public_record_claim_candidate(
            "summary",
            extract,
            extract,
            60,
            WIKIPEDIA_ENGINE,
            f"wikipedia-page:{page_id}",
            "wikipedia_introductory_extract",
            "Wikipedia",
            source_url,
            details,
        ),
        _public_record_claim_candidate(
            "platform_identifier",
            {
                "platform": "Wikipedia",
                "identifier_type": "wikipedia_page_id",
                "identifier": page_id,
                "url": source_url,
            },
            title,
            65,
            WIKIPEDIA_ENGINE,
            f"wikipedia-page:{page_id}",
            "wikipedia_page_identifier",
            "Wikipedia",
            source_url,
            details,
        ),
    ]
    thumbnail_url = _safe_public_url(page.get("thumbnail_url"))
    if thumbnail_url and urlparse(thumbnail_url).hostname.casefold() == "upload.wikimedia.org":
        claims.append(
            _public_record_claim_candidate(
                "photograph",
                thumbnail_url,
                thumbnail_url,
                55,
                WIKIPEDIA_ENGINE,
                f"wikipedia-page:{page_id}",
                "wikipedia_lead_image",
                "Wikipedia",
                source_url,
                details,
            )
        )
    return claims


def extract_icij_offshore_claims(observation: Any) -> List[Dict[str, Any]]:
    if (
        not isinstance(observation, dict)
        or observation.get("source_engine") != ICIJ_OFFSHORE_ENGINE
        or observation.get("status") != "potential_match"
    ):
        return []
    output = []
    for match in list(observation.get("matches") or [])[:MAX_ICIJ_MATCHES]:
        if not isinstance(match, dict):
            continue
        node_id = str(match.get("node_id") or "")
        name = _bounded_text(match.get("name"), limit=500)
        source_url = _safe_public_url(match.get("url"))
        if (
            not _ICIJ_NODE_ID_PATTERN.fullmatch(node_id)
            or source_url != f"https://offshoreleaks.icij.org/nodes/{node_id}"
            or not name
        ):
            continue
        value = {
            "provider": "ICIJ Offshore Leaks Database",
            "node_id": node_id,
            "name": name,
            "url": source_url,
        }
        output.append(
            _public_record_claim_candidate(
                "offshore_database_match",
                value,
                name,
                50,
                ICIJ_OFFSHORE_ENGINE,
                f"icij-offshore-node:{node_id}",
                "icij_exact_name_candidate",
                "ICIJ Offshore Leaks Database",
                source_url,
                {
                    "reconciliation_score": match.get("score"),
                    "dataset_description": _bounded_text(
                        match.get("description"), limit=1000
                    ),
                    "identity_warning": (
                        "An exact name is not sufficient to confirm identity. "
                        "Inclusion does not imply illegal or improper conduct."
                    ),
                },
            )
        )
    return output


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
