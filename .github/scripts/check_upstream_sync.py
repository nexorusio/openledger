#!/usr/bin/env python3
"""Classify and validate an automated Maigret upstream synchronization."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse


AUTO_MERGE_PATHS = {
    "maigret/resources/data.json",
    "maigret/resources/db_meta.json",
    "sites.md",
}
EXPECTED_DATA_URL = (
    "https://raw.githubusercontent.com/soxoj/maigret/main/"
    "maigret/resources/data.json"
)


def git(*args: str, cwd: Path) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def parse_version(value: str) -> tuple[int, ...]:
    numbers = re.findall(r"\d+", value)
    return tuple(int(part) for part in numbers[:3]) if numbers else (0,)


def current_version(root: Path) -> str:
    version_file = root / "maigret" / "__version__.py"
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)', version_file.read_text())
    if not match:
        raise ValueError("could not determine the bundled OpenLedger/Maigret version")
    return match.group(1)


def changed_paths(root: Path, base: str, head: str) -> list[tuple[str, str]]:
    output = git("diff", "--name-status", "--find-renames", f"{base}...{head}", cwd=root)
    changes: list[tuple[str, str]] = []
    for line in output.splitlines():
        fields = line.split("\t")
        if not fields:
            continue
        status = fields[0]
        # For a rename, both the old and new paths must be evaluated.
        for name in fields[1:]:
            changes.append((status, name))
    return changes


def validate_database(candidate: Path, base: Path) -> list[str]:
    errors: list[str] = []
    data_path = candidate / "maigret" / "resources" / "data.json"
    meta_path = candidate / "maigret" / "resources" / "db_meta.json"

    try:
        raw = data_path.read_bytes()
        data = json.loads(raw)
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return [f"site database or metadata is unreadable: {exc}"]

    if not isinstance(data, dict):
        errors.append("data.json must contain a JSON object")
        return errors
    for key in ("sites", "engines"):
        if not isinstance(data.get(key), dict):
            errors.append(f"data.json field {key!r} must be an object")
    if not isinstance(data.get("tags"), list):
        errors.append("data.json field 'tags' must be an array")

    sites = data.get("sites", {})
    candidate_count = len(sites) if isinstance(sites, dict) else 0
    if candidate_count < 1000:
        errors.append(f"implausibly small site database: {candidate_count} sites")

    try:
        base_data = json.loads(
            (base / "maigret" / "resources" / "data.json").read_text(encoding="utf-8")
        )
        base_count = len(base_data.get("sites", {}))
        if base_count and candidate_count < int(base_count * 0.95):
            errors.append(
                f"site count fell by more than 5% ({base_count} to {candidate_count})"
            )
    except (OSError, ValueError, AttributeError) as exc:
        errors.append(f"could not validate the base site count: {exc}")

    if meta.get("version") != 1:
        errors.append(f"unsupported database metadata version: {meta.get('version')!r}")
    if meta.get("sites_count") != candidate_count:
        errors.append(
            "db_meta.json sites_count does not match data.json "
            f"({meta.get('sites_count')!r} != {candidate_count})"
        )
    actual_hash = hashlib.sha256(raw).hexdigest()
    if meta.get("data_sha256") != actual_hash:
        errors.append("db_meta.json SHA-256 does not match data.json")
    if meta.get("data_url") != EXPECTED_DATA_URL:
        errors.append(f"unexpected database download URL: {meta.get('data_url')!r}")
    else:
        parsed = urlparse(meta["data_url"])
        if parsed.scheme != "https" or parsed.hostname != "raw.githubusercontent.com":
            errors.append("database download URL must use HTTPS on raw.githubusercontent.com")

    minimum = str(meta.get("min_maigret_version", "0"))
    installed = current_version(candidate)
    if parse_version(minimum) > parse_version(installed):
        errors.append(
            f"database requires version {minimum}, but OpenLedger bundles {installed}"
        )
    return errors


def write_summary(lines: list[str]) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--base-ref", default="main")
    parser.add_argument("--head-ref", default="HEAD")
    args = parser.parse_args()

    candidate = args.candidate.resolve()
    base = args.base.resolve()
    changes = changed_paths(candidate, args.base_ref, args.head_ref)
    paths = sorted({name for _, name in changes})
    deleted = sorted({name for status, name in changes if status.startswith("D")})
    unsafe = sorted(set(paths) - AUTO_MERGE_PATHS)

    errors: list[str] = []
    if not paths:
        errors.append("the synchronization contains no file changes")
    if deleted:
        errors.append("automatic synchronization may not delete files: " + ", ".join(deleted))
    if unsafe:
        errors.append("manual review required for: " + ", ".join(unsafe))
    if set(paths) & {"maigret/resources/data.json", "maigret/resources/db_meta.json"}:
        if not {
            "maigret/resources/data.json",
            "maigret/resources/db_meta.json",
        }.issubset(paths):
            errors.append("data.json and db_meta.json must change together")
        errors.extend(validate_database(candidate, base))

    lines = ["## Upstream synchronization integrity", "", "Changed files:"]
    lines.extend(f"- `{name}`" for name in paths)
    if errors:
        lines.extend(["", "**Automatic merge blocked.**"])
        lines.extend(f"- {error}" for error in errors)
        write_summary(lines)
        print("\n".join(errors), file=sys.stderr)
        return 1

    lines.extend(["", "**Eligible for automatic merge.**"])
    write_summary(lines)
    print("Upstream update is limited to the validated site-database artifacts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
