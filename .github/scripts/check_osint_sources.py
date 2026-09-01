#!/usr/bin/env python3
"""Validate OpenLedger's governed OSINT source registry and optional live contract."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = ROOT / "config" / "osint-sources.json"
SOURCE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_]{1,79}$")
REQUIRED_SOURCE_KEYS = {
    "id",
    "display_name",
    "status",
    "capability",
    "integration_mode",
    "catalog_reference",
    "official_documentation",
    "usage_terms",
    "api_version",
    "network_classification",
    "access",
    "trigger",
    "guardrails",
    "claim_candidates",
    "observation_only_fields",
    "maintenance",
}


class NoRedirectHandler(HTTPRedirectHandler):
    """urllib handler that rejects redirects during the fixed-origin smoke test."""

    def http_error_301(self, request, response, code, message, headers):
        raise HTTPError(request.full_url, code, message, headers, response)

    http_error_302 = http_error_301
    http_error_303 = http_error_301
    http_error_307 = http_error_301
    http_error_308 = http_error_301


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _date(value: object, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as error:
        raise ValueError(f"{field} must use YYYY-MM-DD") from error


def load_and_validate_registry(*, today: date | None = None) -> dict:
    today = today or date.today()
    with REGISTRY_PATH.open(encoding="utf-8") as registry_file:
        registry = json.load(registry_file)
    _require(registry.get("schema_version") == 1, "unsupported registry schema")
    policy = registry.get("maintenance_policy")
    _require(isinstance(policy, dict), "maintenance_policy must be an object")
    review_interval = policy.get("review_interval_days")
    stale_after = policy.get("stale_after_days")
    _require(
        isinstance(review_interval, int) and 1 <= review_interval <= 365,
        "review_interval_days must be between 1 and 365",
    )
    _require(
        isinstance(stale_after, int) and review_interval <= stale_after <= 365,
        "stale_after_days must be between the review interval and 365",
    )
    sources = registry.get("sources")
    _require(isinstance(sources, list) and sources, "sources must be a non-empty list")
    identifiers = set()
    for source in sources:
        _require(isinstance(source, dict), "every source must be an object")
        missing = sorted(REQUIRED_SOURCE_KEYS - set(source))
        _require(
            not missing, f"source is missing required fields: {', '.join(missing)}"
        )
        source_id = source["id"]
        _require(
            isinstance(source_id, str) and SOURCE_ID_PATTERN.fullmatch(source_id),
            f"invalid source id: {source_id!r}",
        )
        _require(source_id not in identifiers, f"duplicate source id: {source_id}")
        identifiers.add(source_id)
        _require(source["status"] == "active", f"{source_id}: source is not active")
        _require(
            source["integration_mode"]
            in {"native_public_api", "bundled_cli", "bundled_library"},
            f"{source_id}: unsupported integration mode",
        )
        for key in ("catalog_reference", "official_documentation", "usage_terms"):
            _require(
                str(source.get(key) or "").startswith("https://"),
                f"{source_id}: {key} must be an HTTPS URL",
            )
        _require(
            bool(str(source.get("api_version") or "").strip()),
            f"{source_id}: api_version must be recorded",
        )
        access = source["access"]
        _require(isinstance(access, dict), f"{source_id}: access must be an object")
        for key, expected in (
            ("genuinely_free", True),
            ("registration_required", False),
            ("credentials_required", False),
        ):
            _require(
                access.get(key) is expected,
                f"{source_id}: access.{key} must be {expected}",
            )
        trigger = source["trigger"]
        _require(isinstance(trigger, dict), f"{source_id}: trigger must be an object")
        _require(
            trigger.get("operator_opt_in_required") is True,
            f"{source_id}: operator opt-in must be required",
        )
        maximum_targets = trigger.get("maximum_targets_per_investigation")
        _require(
            isinstance(maximum_targets, int) and 1 <= maximum_targets <= 100,
            f"{source_id}: target cap must be between 1 and 100",
        )
        guardrails = source["guardrails"]
        _require(
            isinstance(guardrails, dict), f"{source_id}: guardrails must be an object"
        )
        _require(
            guardrails.get("human_review_required") is True,
            f"{source_id}: human review must be required",
        )
        _require(
            guardrails.get("automatic_approval_allowed") is False,
            f"{source_id}: automatic approval must remain disabled",
        )
        timeout = guardrails.get("timeout_seconds")
        response_cap = guardrails.get("maximum_response_bytes")
        _require(
            isinstance(timeout, int) and 1 <= timeout <= 600,
            f"{source_id}: timeout must be between 1 and 600 seconds",
        )
        _require(
            isinstance(response_cap, int) and 1 <= response_cap <= 10_000_000,
            f"{source_id}: response cap must be between 1 and 10000000 bytes",
        )
        _require(
            isinstance(source["claim_candidates"], list),
            f"{source_id}: claim_candidates must be a list",
        )
        _require(
            isinstance(source["observation_only_fields"], list),
            f"{source_id}: observation_only_fields must be a list",
        )
        maintenance = source["maintenance"]
        _require(
            isinstance(maintenance, dict), f"{source_id}: maintenance must be an object"
        )
        reviewed_at = _date(maintenance.get("reviewed_at"), f"{source_id}.reviewed_at")
        due_by = _date(maintenance.get("review_due_by"), f"{source_id}.review_due_by")
        _require(reviewed_at <= today, f"{source_id}: reviewed_at is in the future")
        _require(
            due_by >= reviewed_at, f"{source_id}: review_due_by precedes reviewed_at"
        )
        _require(
            (due_by - reviewed_at).days <= review_interval,
            f"{source_id}: review window exceeds the {review_interval}-day review rule",
        )
        _require(
            today <= due_by, f"{source_id}: source review is overdue since {due_by}"
        )
        if source["integration_mode"] == "native_public_api":
            _require(
                str(source.get("endpoint_origin") or "").startswith("https://"),
                f"{source_id}: endpoint_origin must be an HTTPS URL",
            )
            _require(
                source["network_classification"] == "fixed_public_api",
                f"{source_id}: unexpected network classification",
            )
            _require(
                guardrails.get("fixed_network_origin") is True,
                f"{source_id}: fixed network origin must be required",
            )
            _require(
                guardrails.get("redirects_allowed") is False,
                f"{source_id}: redirects must remain disabled",
            )
            additional_origins = source.get("additional_endpoint_origins", [])
            _require(
                isinstance(additional_origins, list)
                and all(str(origin).startswith("https://") for origin in additional_origins),
                f"{source_id}: additional endpoint origins must be HTTPS URLs",
            )
            if additional_origins:
                _require(
                    guardrails.get("fixed_network_origins")
                    == [source["endpoint_origin"], *additional_origins],
                    f"{source_id}: every runtime origin must be statically fixed",
                )
        else:
            _require(
                source["network_classification"] == "offline_local",
                f"{source_id}: bundled source must remain offline",
            )
            repository = str(source.get("upstream_repository") or "")
            _require(
                repository.startswith("https://"),
                f"{source_id}: repository-backed source needs an HTTPS upstream",
            )
            _require(
                bool(str(source.get("license") or "").strip()),
                f"{source_id}: repository-backed source needs a recorded license",
            )
            _require(
                re.fullmatch(r"[0-9a-f]{40}", str(source.get("pinned_commit") or ""))
                is not None,
                f"{source_id}: bundled source must pin an immutable commit",
            )
            _require(
                guardrails.get("network_access_allowed") is False,
                f"{source_id}: bundled runtime network access must be disabled",
            )
            _require(
                guardrails.get("remote_lookups_allowed") is False,
                f"{source_id}: bundled remote lookups must be disabled",
            )
            last_activity = _date(
                maintenance.get("last_upstream_activity_at"),
                f"{source_id}.last_upstream_activity_at",
            )
            _require(
                0 <= (today - last_activity).days <= stale_after,
                f"{source_id}: upstream activity is older than {stale_after} days",
            )
    return registry


def live_github_contract(source: dict) -> None:
    target = source["maintenance"].get("live_contract_target")
    _require(
        isinstance(target, str) and target, "GitHub live contract target is missing"
    )
    endpoint = source["endpoint_origin"]
    _require(endpoint == "https://api.github.com", "GitHub endpoint origin changed")
    request = Request(
        f"{endpoint}/users/{target}",
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": source["api_version"],
            "User-Agent": "OpenLedger-OSINT-Contract-Audit",
        },
    )
    opener = build_opener(NoRedirectHandler())
    try:
        with opener.open(request, timeout=15) as response:
            payload = response.read(1_000_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"GitHub live contract request failed: {error}") from error
    _require(len(payload) <= 1_000_000, "GitHub live contract response was oversized")
    try:
        profile = json.loads(payload)
    except json.JSONDecodeError as error:
        raise RuntimeError("GitHub live contract returned invalid JSON") from error
    _require(isinstance(profile, dict), "GitHub live contract returned a non-object")
    _require(
        profile.get("login", "").casefold() == target.casefold(),
        "GitHub login contract changed",
    )
    _require(profile.get("type") == "User", "GitHub public user type contract changed")
    _require(isinstance(profile.get("id"), int), "GitHub numeric ID contract changed")
    _require(
        str(profile.get("html_url") or "").startswith("https://github.com/"),
        "GitHub public profile URL contract changed",
    )


def live_wayback_contract(source: dict) -> None:
    target = source["maintenance"].get("live_contract_target")
    _require(
        isinstance(target, str) and target.startswith("https://"),
        "Wayback live contract target is missing",
    )
    endpoint = source["endpoint_origin"]
    _require(endpoint == "https://web.archive.org", "Wayback endpoint origin changed")
    query = urlencode(
        [
            ("url", target),
            ("output", "json"),
            ("fl", "timestamp,original,statuscode,mimetype,digest"),
            ("filter", "statuscode:200"),
            ("filter", "mimetype:text/html"),
            ("collapse", "digest"),
            ("matchType", "exact"),
            ("limit", "-1"),
        ]
    )
    request = Request(
        f"{endpoint}/cdx/search/cdx?{query}",
        headers={
            "Accept": "application/json",
            "User-Agent": "OpenLedger-OSINT-Contract-Audit",
        },
    )
    opener = build_opener(NoRedirectHandler())
    try:
        with opener.open(request, timeout=20) as response:
            payload = response.read(1_000_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"Wayback live contract request failed: {error}") from error
    _require(len(payload) <= 1_000_000, "Wayback live contract response was oversized")
    try:
        rows = json.loads(payload)
    except json.JSONDecodeError as error:
        raise RuntimeError("Wayback live contract returned invalid JSON") from error
    _require(isinstance(rows, list) and len(rows) >= 2, "Wayback returned no rows")
    _require(
        rows[0] == ["timestamp", "original", "statuscode", "mimetype", "digest"],
        "Wayback CDX field contract changed",
    )
    _require(
        isinstance(rows[1], list)
        and len(rows[1]) == 5
        and rows[1][2] == "200"
        and rows[1][3] == "text/html",
        "Wayback CDX row contract changed",
    )


def live_gleif_contract(source: dict) -> None:
    target = source["maintenance"].get("live_contract_target")
    _require(isinstance(target, str) and target, "GLEIF live target is missing")
    endpoint = source["endpoint_origin"]
    _require(endpoint == "https://api.gleif.org", "GLEIF endpoint origin changed")
    query = urlencode(
        {
            "filter[entity.legalName]": target,
            "filter[entity.legalAddress.country]": "US",
            "page[number]": "1",
            "page[size]": "5",
        }
    )
    request = Request(
        f"{endpoint}/api/v1/lei-records?{query}",
        headers={
            "Accept": "application/vnd.api+json",
            "User-Agent": "OpenLedger-OSINT-Contract-Audit/1.0 (+https://github.com/nexorusio/openledger)",
        },
    )
    try:
        with build_opener(NoRedirectHandler()).open(request, timeout=30) as response:
            payload = response.read(1_000_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"GLEIF live contract request failed: {error}") from error
    _require(len(payload) <= 1_000_000, "GLEIF live response was oversized")
    try:
        rows = json.loads(payload).get("data")
    except (AttributeError, json.JSONDecodeError) as error:
        raise RuntimeError("GLEIF live contract returned invalid JSON") from error
    _require(isinstance(rows, list) and rows, "GLEIF returned no LEI records")
    _require(
        any(
            isinstance(row, dict)
            and re.fullmatch(
                r"[A-Z0-9]{20}",
                str((row.get("attributes") or {}).get("lei") or ""),
            )
            and str(
                (((row.get("attributes") or {}).get("entity") or {}).get("legalName") or {}).get("name")
                or ""
            ).casefold()
            == target.casefold()
            for row in rows
        ),
        "GLEIF legal-name or LEI contract changed",
    )


def live_fr_company_registry_contract(source: dict) -> None:
    target = source["maintenance"].get("live_contract_target")
    _require(
        isinstance(target, str) and re.fullmatch(r"[0-9]{9}", target),
        "French registry live target is missing",
    )
    endpoint = source["endpoint_origin"]
    _require(
        endpoint == "https://recherche-entreprises.api.gouv.fr",
        "French registry endpoint origin changed",
    )
    query = urlencode({"q": target, "page": "1", "per_page": "5"})
    request = Request(
        f"{endpoint}/search?{query}",
        headers={
            "Accept": "application/json",
            "User-Agent": "OpenLedger-OSINT-Contract-Audit/1.0 (+https://github.com/nexorusio/openledger)",
        },
    )
    try:
        with build_opener(NoRedirectHandler()).open(request, timeout=30) as response:
            payload = response.read(1_000_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(
            f"French registry live contract request failed: {error}"
        ) from error
    _require(
        len(payload) <= 1_000_000,
        "French registry live response was oversized",
    )
    try:
        rows = json.loads(payload).get("results")
    except (AttributeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            "French registry live contract returned invalid JSON"
        ) from error
    _require(isinstance(rows, list) and rows, "French registry returned no entity")
    _require(
        any(
            isinstance(row, dict)
            and str(row.get("siren") or "") == target
            and bool(str(row.get("nom_complet") or row.get("nom_raison_sociale") or ""))
            for row in rows
        ),
        "French registry SIREN or legal-name contract changed",
    )


def live_cloudflare_dns_contract(source: dict) -> None:
    target = source["maintenance"].get("live_contract_target")
    _require(
        isinstance(target, str) and re.fullmatch(r"[a-z0-9.-]{3,253}", target),
        "Cloudflare DNS live target is missing",
    )
    endpoint = source["endpoint_origin"]
    _require(
        endpoint == "https://cloudflare-dns.com",
        "Cloudflare DNS endpoint origin changed",
    )
    query = urlencode({"name": target, "type": "A"})
    request = Request(
        f"{endpoint}/dns-query?{query}",
        headers={
            "Accept": "application/dns-json",
            "User-Agent": "OpenLedger-OSINT-Contract-Audit/1.0 (+https://github.com/nexorusio/openledger)",
        },
    )
    try:
        with build_opener(NoRedirectHandler()).open(request, timeout=20) as response:
            payload = response.read(128_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(
            f"Cloudflare DNS live contract request failed: {error}"
        ) from error
    _require(len(payload) <= 128_000, "Cloudflare DNS response was oversized")
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as error:
        raise RuntimeError("Cloudflare DNS returned invalid JSON") from error
    _require(document.get("Status") == 0, "Cloudflare DNS status contract changed")
    answers = document.get("Answer")
    _require(
        isinstance(answers, list)
        and any(
            isinstance(answer, dict)
            and answer.get("type") == 1
            and bool(str(answer.get("data") or ""))
            for answer in answers
        ),
        "Cloudflare DNS A-record contract changed",
    )


def live_wikidata_contract(source: dict) -> None:
    target = source["maintenance"].get("live_contract_target")
    _require(
        isinstance(target, str) and re.fullmatch(r"Q[1-9][0-9]{0,19}", target),
        "Wikidata live contract target is missing",
    )
    _require(
        source.get("additional_endpoint_origins") == ["https://query.wikidata.org"],
        "Wikidata query origin changed",
    )
    opener = build_opener(NoRedirectHandler())
    headers = {
        "Accept": "application/json",
        "User-Agent": "OpenLedger-OSINT-Contract-Audit/1.0 (+https://github.com/nexorusio/openledger)",
    }
    entity_query = urlencode(
        {
            "action": "wbgetentities",
            "ids": target,
            "props": "labels|claims",
            "languages": "en",
            "format": "json",
            "formatversion": "2",
        }
    )
    try:
        with opener.open(
            Request(f"https://www.wikidata.org/w/api.php?{entity_query}", headers=headers),
            timeout=30,
        ) as response:
            payload = response.read(1_000_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"Wikidata entity contract request failed: {error}") from error
    _require(len(payload) <= 1_000_000, "Wikidata entity response was oversized")
    entity = (json.loads(payload).get("entities") or {}).get(target)
    _require(isinstance(entity, dict), "Wikidata entity contract changed")

    sparql = f"SELECT ?instance WHERE {{ wd:{target} wdt:P31 ?instance }} LIMIT 1"
    query = urlencode({"query": sparql, "format": "json"})
    try:
        with opener.open(
            Request(f"https://query.wikidata.org/sparql?{query}", headers=headers),
            timeout=30,
        ) as response:
            payload = response.read(1_000_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"Wikidata query contract request failed: {error}") from error
    _require(len(payload) <= 1_000_000, "Wikidata query response was oversized")
    bindings = (json.loads(payload).get("results") or {}).get("bindings")
    _require(isinstance(bindings, list) and bindings, "Wikidata query returned no relation")


def live_wikipedia_contract(source: dict) -> None:
    target = source["maintenance"].get("live_contract_target")
    _require(isinstance(target, str) and target, "Wikipedia live target is missing")
    query = urlencode(
        {
            "action": "query",
            "generator": "search",
            "gsrsearch": target,
            "gsrnamespace": "0",
            "gsrlimit": "5",
            "prop": "extracts|pageimages|pageprops|info",
            "exintro": "1",
            "explaintext": "1",
            "exchars": "2000",
            "piprop": "thumbnail",
            "pithumbsize": "500",
            "inprop": "url",
            "redirects": "1",
            "format": "json",
            "formatversion": "2",
        }
    )
    request = Request(
        f"https://en.wikipedia.org/w/api.php?{query}",
        headers={
            "Accept": "application/json",
            "User-Agent": "OpenLedger-OSINT-Contract-Audit/1.0 (+https://github.com/nexorusio/openledger)",
        },
    )
    try:
        with build_opener(NoRedirectHandler()).open(request, timeout=20) as response:
            payload = response.read(1_000_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"Wikipedia live contract request failed: {error}") from error
    _require(len(payload) <= 1_000_000, "Wikipedia live response was oversized")
    pages = (json.loads(payload).get("query") or {}).get("pages")
    _require(isinstance(pages, list) and pages, "Wikipedia page contract changed")
    _require(
        any(page.get("title") == target and page.get("extract") for page in pages),
        "Wikipedia exact biography contract changed",
    )


def live_icij_offshore_contract(source: dict) -> None:
    target = source["maintenance"].get("live_contract_target")
    _require(isinstance(target, str) and target, "ICIJ live target is missing")
    body = json.dumps(
        {"query": target, "type": "Officer", "limit": 5}
    ).encode("utf-8")
    request = Request(
        "https://offshoreleaks.icij.org/api/v1/reconcile",
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "OpenLedger-OSINT-Contract-Audit/1.0 (+https://github.com/nexorusio/openledger)",
        },
    )
    try:
        with build_opener(NoRedirectHandler()).open(request, timeout=30) as response:
            payload = response.read(1_000_001)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"ICIJ live contract request failed: {error}") from error
    _require(len(payload) <= 1_000_000, "ICIJ live response was oversized")
    results = json.loads(payload).get("result")
    _require(isinstance(results, list) and results, "ICIJ reconciliation contract changed")
    _require(
        any(
            item.get("name") == target
            and item.get("match") is True
            and item.get("score") == 100.0
            for item in results
        ),
        "ICIJ exact-name reconciliation contract changed",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--live", action="store_true", help="run public API contract checks"
    )
    args = parser.parse_args()
    try:
        registry = load_and_validate_registry()
        if args.live:
            sources_by_id = {source["id"]: source for source in registry["sources"]}
            live_github_contract(sources_by_id["github_public_profile"])
            live_wayback_contract(sources_by_id["wayback_cdx"])
            live_gleif_contract(sources_by_id["gleif_lei_registry"])
            live_fr_company_registry_contract(
                sources_by_id["fr_company_registry"]
            )
            live_cloudflare_dns_contract(
                sources_by_id["cloudflare_dns_context"]
            )
            live_wikidata_contract(sources_by_id["wikidata_affiliation"])
            live_wikipedia_contract(sources_by_id["wikipedia_public_biography"])
            live_icij_offshore_contract(sources_by_id["icij_offshore_leaks"])
    except (OSError, ValueError, RuntimeError, StopIteration) as error:
        print(f"OSINT source audit failed: {error}", file=sys.stderr)
        return 1
    print(f"Validated {len(registry['sources'])} governed OSINT source(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
