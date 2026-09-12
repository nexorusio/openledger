#!/usr/bin/env python3
"""Fingerprint the runtime source copied into a reviewed image.

The same inventory is hashed on the clean Git checkout, inside Docker RUN, and
by production startup. Runtime secrets/settings and generated Python caches are
excluded. Docker's source exclusions below match .dockerignore. This binds code,
collector data, templates, configuration, migrations and deployment entrypoints;
third-party installed dependencies remain bound by the final immutable image ID.
"""

import hashlib
import json
from pathlib import Path

SOURCE_DIRECTORIES = ("maigret", "config", "migrations", "schemas", "deploy")
SOURCE_FILES = (
    "Dockerfile",
    ".dockerignore",
    "pyproject.toml",
    "poetry.lock",
    "requirements.txt",
    "alembic.ini",
)


def source_fingerprint(root):
    root = Path(root)
    candidates = [root / name for name in SOURCE_FILES if (root / name).exists()]
    for name in SOURCE_DIRECTORIES:
        candidates.extend(path for path in (root / name).rglob("*") if path.is_file())
    checksums = {}
    for path in sorted(set(candidates)):
        relative = path.relative_to(root)
        if (
            "__pycache__" in relative.parts
            or path.suffix in {".pyc", ".pyo", ".log", ".sqlite", ".sqlite3"}
            or path.name == ".env"
            or path.name.startswith(".env.")
            or (path.suffix == ".txt" and str(relative) != "requirements.txt")
        ):
            continue
        if path.is_symlink():
            raise ValueError(
                "Runtime source symlinks are not allowed in the reviewed inventory"
            )
        checksums[str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not checksums:
        raise ValueError("The runtime source inventory is empty")
    encoded = json.dumps(checksums, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


if __name__ == "__main__":
    print(source_fingerprint(Path(__file__).resolve().parents[1]))
