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
    "endpoint_origin",
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
        for key in (
            "catalog_reference",
            "official_documentation",
            "usage_terms",
            "endpoint_origin",
        ):
            _require(
                str(source.get(key) or "").startswith("https://"),
                f"{source_id}: {key} must be an HTTPS URL",
            )
        _require(
            source["network_classification"] == "fixed_public_api",
            f"{source_id}: unexpected network classification",
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
        _require(
            guardrails.get("fixed_network_origin") is True,
            f"{source_id}: fixed network origin must be required",
        )
        _require(
            guardrails.get("redirects_allowed") is False,
            f"{source_id}: redirects must remain disabled",
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
        if source["integration_mode"] != "native_public_api":
            repository = str(source.get("upstream_repository") or "")
            _require(
                repository.startswith("https://"),
                f"{source_id}: repository-backed source needs an HTTPS upstream",
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--live", action="store_true", help="run public API contract checks"
    )
    args = parser.parse_args()
    try:
        registry = load_and_validate_registry()
        if args.live:
            github_source = next(
                source
                for source in registry["sources"]
                if source["id"] == "github_public_profile"
            )
            live_github_contract(github_source)
    except (OSError, ValueError, RuntimeError, StopIteration) as error:
        print(f"OSINT source audit failed: {error}", file=sys.stderr)
        return 1
    print(f"Validated {len(registry['sources'])} governed OSINT source(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
