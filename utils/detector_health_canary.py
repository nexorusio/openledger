#!/usr/bin/env python3
"""Run bounded Maigret detector canaries and propose health-state changes.

The command never edits Maigret's site database.  It writes a separate reviewed
health registry plus a detailed artifact.  Production consumes the registry
only after the generated change is merged.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import random
import re
import string
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from maigret.checking import maigret as maigret_search
from maigret.sites import MaigretDatabase, MaigretSite
from maigret.web.profile_reliability import (
    empty_detector_health_registry,
    evolve_detector_health_registry,
    load_detector_health_registry,
    serialize_detector_health_registry,
)


TRANSPORT_UNKNOWN_STATUS_CODES = frozenset(
    {401, 403, 407, 408, 425, 429, 451, 500, 502, 503, 504, 999}
)
TRANSPORT_UNKNOWN_MARKERS = (
    "access denied",
    "blocked",
    "captcha",
    "challenge",
    "cloudflare",
    "login required",
    "rate limit",
    "verify you are human",
)
MIN_HIGH_ENTROPY_BITS = 40.0


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _matches_site(site: MaigretSite, username: str) -> bool:
    return not site.regex_check or re.search(site.regex_check, username) is not None


def _randomize_username_template(template: str, rng: random.Random) -> str:
    """Randomize a declared handle without discarding its separators or shape."""
    characters = list(template)
    mutable = [
        index
        for index, character in enumerate(characters)
        if character.isalnum()
    ]
    if not mutable:
        return template

    selected = [index for index in mutable if rng.random() < 0.65]
    if not selected:
        selected = [rng.choice(mutable)]
    for index in selected:
        character = characters[index]
        if character.isdigit():
            alphabet = string.digits
        elif character.isupper():
            alphabet = string.ascii_uppercase
        else:
            alphabet = string.ascii_lowercase
        replacements = alphabet.replace(character, "") or alphabet
        characters[index] = rng.choice(replacements)
    return "".join(characters)


def _template_mutation_entropy_bits(template: str, candidate: str) -> float:
    """Conservatively estimate entropy added by randomized template positions."""
    if len(template) != len(candidate):
        return 0.0
    entropy = 0.0
    for original, replacement in zip(template, candidate):
        if original == replacement:
            continue
        if original.isdigit() and replacement.isdigit():
            entropy += math.log2(9)
        elif original.isalpha() and replacement.isalpha():
            entropy += math.log2(25)
    return entropy


def high_entropy_usernames(
    site: MaigretSite,
    *,
    samples: int,
    rng: random.Random,
) -> list[str]:
    """Create likely-missing handles while respecting site username syntax."""
    existing = {
        str(site.username_claimed or "").casefold(),
        str(site.username_unclaimed or "").casefold(),
    }
    generated: list[str] = []
    templates = [
        value
        for value in (
            str(site.username_unclaimed or "").strip(),
            str(site.username_claimed or "").strip(),
        )
        if value and _matches_site(site, value)
    ]
    alphabets = (string.ascii_lowercase + string.digits, string.ascii_lowercase, string.digits)
    lengths = (12, 10, 8, 15, 6)
    for _attempt in range(500):
        alphabet = alphabets[_attempt % len(alphabets)]
        length = lengths[(_attempt // len(alphabets)) % len(lengths)]
        if templates and _attempt % 2 == 0:
            template = templates[(_attempt // 2) % len(templates)]
            candidate = _randomize_username_template(
                template,
                rng,
            )
            entropy_bits = _template_mutation_entropy_bits(template, candidate)
        else:
            candidate = "".join(rng.choice(alphabet) for _ in range(length))
            entropy_bits = length * math.log2(len(alphabet))
        if (
            entropy_bits >= MIN_HIGH_ENTROPY_BITS
            and candidate.casefold() not in existing
            and candidate not in generated
            and _matches_site(site, candidate)
        ):
            generated.append(candidate)
            if len(generated) >= samples:
                break
    return generated


def build_probe_plan(
    site: MaigretSite,
    *,
    samples: int,
    rng: random.Random,
) -> list[Dict[str, str]]:
    probes: list[Dict[str, str]] = []
    known_claimed = str(site.username_claimed or "").strip()
    known_missing = str(site.username_unclaimed or "").strip()
    if known_claimed and _matches_site(site, known_claimed):
        probes.append(
            {
                "kind": "declared_existing",
                "username": known_claimed,
                "expected": "claimed",
            }
        )
    if known_missing and _matches_site(site, known_missing):
        probes.append(
            {
                "kind": "declared_missing",
                "username": known_missing,
                "expected": "available",
            }
        )
    probes.extend(
        {
            "kind": "high_entropy_missing",
            "username": username,
            "expected": "available",
        }
        for username in high_entropy_usernames(
            site,
            samples=samples,
            rng=rng,
        )
    )
    return probes


def evaluate_probe_results(results: Iterable[Mapping[str, Any]]) -> Dict[str, str]:
    results = list(results)
    contradictions = []
    unknowns = []
    kinds = {str(result.get("kind") or "") for result in results}
    for result in results:
        expected = str(result.get("expected") or "")
        actual = str(result.get("actual") or "unknown")
        label = f"{result.get('kind')}:{expected}->{actual}"
        if actual in {"unknown", "illegal", "error"}:
            unknowns.append(label)
        elif actual != expected:
            contradictions.append(label)

    required = {"declared_existing", "declared_missing", "high_entropy_missing"}
    missing_kinds = sorted(required.difference(kinds))
    if contradictions:
        return {
            "outcome": "fail",
            "reason": "Detector contradictions: " + ", ".join(contradictions[:8]),
        }
    if unknowns or missing_kinds:
        details = []
        if unknowns:
            details.append("unknown probes: " + ", ".join(unknowns[:8]))
        if missing_kinds:
            details.append("missing probe classes: " + ", ".join(missing_kinds))
        return {"outcome": "unknown", "reason": "; ".join(details)}
    return {"outcome": "pass", "reason": "All detector canaries matched expectations."}


def transport_aware_status(
    actual: str,
    *,
    http_status: Any = None,
    context: Any = "",
) -> str:
    """Keep anti-bot and transient transport responses out of failure streaks."""
    try:
        status_code = int(http_status)
    except (TypeError, ValueError):
        status_code = None
    context_text = " ".join(str(context or "").split()).casefold()
    if status_code in TRANSPORT_UNKNOWN_STATUS_CODES or any(
        marker in context_text for marker in TRANSPORT_UNKNOWN_MARKERS
    ):
        return "unknown"
    return actual


async def probe_site(
    site: MaigretSite,
    *,
    samples: int,
    rng_seed: int,
    timeout: int,
    semaphore: asyncio.Semaphore,
    logger: logging.Logger,
) -> Dict[str, Any]:
    plan = build_probe_plan(
        site,
        samples=samples,
        rng=random.Random(f"{rng_seed}:{site.name}"),
    )
    results = []
    for probe in plan:
        try:
            async with semaphore:
                raw = await maigret_search(
                    username=probe["username"],
                    site_dict={site.name: site},
                    logger=logger,
                    timeout=timeout,
                    id_type="username",
                    forced=True,
                    max_connections=1,
                    no_progressbar=True,
                    is_parsing_enabled=False,
                    retries=1,
                )
            site_result = raw.get(site.name) or {}
            result = site_result.get("status")
            actual = (
                result.status.value.casefold()
                if result is not None
                else "unknown"
            )
            context = (
                str(result.context or result.error or "")[:500]
                if result is not None
                else "No result returned"
            )
            http_status = site_result.get("http_status")
            actual = transport_aware_status(
                actual,
                http_status=http_status,
                context=context,
            )
        except Exception as error:  # Preserve a bounded diagnostic; continue all sites.
            actual = "error"
            context = type(error).__name__
            http_status = None
        results.append(
            {
                **probe,
                "actual": actual,
                "context": context,
                "http_status": http_status,
                "check_type": site.check_type,
                "protection": list(site.protection or [])[:20],
            }
        )

    evaluation = evaluate_probe_results(results)
    return {
        "site_name": site.name,
        **evaluation,
        "probe_count": len(results),
        "probes": results,
    }


async def run_canaries(
    sites: Mapping[str, MaigretSite],
    *,
    samples: int,
    seed: int,
    timeout: int,
    connections: int,
) -> Dict[str, Dict[str, Any]]:
    logger = logging.getLogger("openledger-detector-canary")
    logger.setLevel(logging.ERROR)
    semaphore = asyncio.Semaphore(connections)
    tasks = [
        probe_site(
            site,
            samples=samples,
            rng_seed=seed,
            timeout=timeout,
            semaphore=semaphore,
            logger=logger,
        )
        for site in sites.values()
    ]
    completed = await asyncio.gather(*tasks)
    return {result["site_name"]: result for result in completed}


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(
        description="Run bounded detector canaries and write a reviewable registry."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=root / "maigret" / "resources" / "data.json",
    )
    parser.add_argument(
        "--previous",
        type=Path,
        default=root / "maigret" / "resources" / "detector_health.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "maigret" / "resources" / "detector_health.json",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=root / "detector-health-report.json",
    )
    parser.add_argument("--top-sites", type=int, default=500)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--connections", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--site", action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 1 <= args.samples <= 10:
        raise SystemExit("--samples must be between 1 and 10")
    if not 1 <= args.connections <= 50:
        raise SystemExit("--connections must be between 1 and 50")
    if not 1 <= args.timeout <= 120:
        raise SystemExit("--timeout must be between 1 and 120")
    if not 1 <= args.top_sites <= 10_000:
        raise SystemExit("--top-sites must be between 1 and 10000")

    database = MaigretDatabase().load_from_path(str(args.db.resolve()))
    sites = database.ranked_sites_dict(
        top=args.top_sites,
        names=args.site,
        disabled=False,
        id_type="username",
    )
    if not sites:
        raise SystemExit("No matching username detectors were selected")

    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**32)
    checked_at = datetime.now(timezone.utc).isoformat()
    observations = asyncio.run(
        run_canaries(
            sites,
            samples=args.samples,
            seed=seed,
            timeout=args.timeout,
            connections=args.connections,
        )
    )
    previous = (
        load_detector_health_registry(args.previous)
        if args.previous.is_file()
        else empty_detector_health_registry()
    )
    updated = evolve_detector_health_registry(
        previous,
        observations,
        checked_at=checked_at,
    )
    serialized = serialize_detector_health_registry(updated)
    write_json(args.output, serialized)

    outcome_counts = {"pass": 0, "fail": 0, "unknown": 0}
    state_counts = {"healthy": 0, "degraded": 0, "quarantined": 0, "untested": 0}
    for observation in observations.values():
        outcome_counts[observation["outcome"]] += 1
    for entry in serialized["sites"].values():
        state_counts[entry["state"]] += 1
    report = {
        "schema_version": 1,
        "checked_at": checked_at,
        "seed": seed,
        "database": str(args.db),
        "selected_sites": len(sites),
        "outcome_counts": outcome_counts,
        "state_counts": state_counts,
        "observations": observations,
    }
    write_json(args.report, report)
    print(
        "Detector canaries completed: "
        f"{outcome_counts['pass']} passed, {outcome_counts['fail']} failed, "
        f"{outcome_counts['unknown']} unknown; "
        f"{state_counts['quarantined']} quarantined."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
