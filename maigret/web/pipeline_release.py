"""Mandatory release identity and live-process attestation for the P2 pipeline.

This module does not select a pipeline. All released application execution uses
p2-e2e-v1. An unbuilt source checkout may run developer tests; a built image can
never opt out of build/schema checks through an environment flag.
"""

from __future__ import annotations

import json
import importlib.util
import os
from pathlib import Path
import re
import time

PIPELINE_ID = "p2-e2e-v1"
ENGINE_CONTRACT = "p2-e2e-v1"
SCHEMA_REVISION = "e2e3c9d1f703"
PREVIOUS_SCHEMA = "b3e9d7c4a610"
BUILD_PATH = Path(__file__).resolve().parents[2] / "openledger-build.json"
WORKER_PATH = Path("/tmp/openledger-worker-attestation.json")
SHA_RE = re.compile(r"[0-9a-f]{40}\Z")


def validate_build(build):
    if not isinstance(build, dict):
        raise RuntimeError("Missing release build identity")
    if "development" in build:
        raise RuntimeError("Release build identity cannot declare development mode")
    for key, expected in (
        ("pipeline_id", PIPELINE_ID),
        ("engine_contract", ENGINE_CONTRACT),
        ("schema_revision", SCHEMA_REVISION),
    ):
        if build.get(key) != expected:
            raise RuntimeError(f"Incompatible release {key}; required {expected}")
    for key in ("commit", "tree"):
        if not SHA_RE.fullmatch(str(build.get(key, ""))):
            raise RuntimeError(f"Release {key} must be a full Git SHA")
    if not re.fullmatch(r"[0-9a-f]{64}", str(build.get("source_digest", ""))):
        raise RuntimeError(
            "Release source_digest must identify the exact runtime source"
        )
    return build


def verify_source_content(build, *, root=None):
    root = root or Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "openledger_source_fingerprint", root / "deploy/source-fingerprint.py"
    )
    fingerprint = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fingerprint)
    if fingerprint.source_fingerprint(root) != build["source_digest"]:
        raise RuntimeError("Runtime source differs from the reviewed immutable build")


def build_identity():
    if BUILD_PATH.exists():
        try:
            return validate_build(json.loads(BUILD_PATH.read_text()))
        except (OSError, ValueError) as error:
            raise RuntimeError("Unreadable release build identity") from error
    if os.getenv("OPENLEDGER_RELEASE_REQUIRED", "").lower() in {"1", "true", "yes"}:
        raise RuntimeError("Released container has no immutable build identity")
    return {
        "pipeline_id": PIPELINE_ID,
        "engine_contract": ENGINE_CONTRACT,
        "schema_revision": SCHEMA_REVISION,
        "commit": None,
        "tree": None,
        "development": True,
    }


def runtime_attestation(store, *, role="app"):
    identity = build_identity()
    if role not in {"app", "worker"}:
        raise RuntimeError("Unknown pipeline runtime role")
    if not identity.get("development"):
        verify_source_content(identity)
        if store is None:
            raise RuntimeError("Released pipeline requires its migrated database")
        from sqlalchemy import text

        with store.engine.connect() as connection:
            revisions = list(
                connection.execute(
                    text("SELECT version_num FROM alembic_version ORDER BY version_num")
                ).scalars()
            )
        if revisions != [SCHEMA_REVISION]:
            raise RuntimeError(
                f"Pipeline requires schema {SCHEMA_REVISION}; migration preflight required"
            )
    return dict(identity, role=role, status="ready", pid=os.getpid())


def assert_runtime_ready(store, *, role="app"):
    """Raise before accepting requests/claiming work when release identity fails."""
    from maigret.web.connectors.registry import get_connector_registry

    # Resolve every reviewed callable before a process claims it is ready. This
    # validates the manifest only; no collection or credential lookup occurs.
    get_connector_registry()
    return runtime_attestation(store, role=role)


def publish_worker_attestation(store):
    """Called by the actual worker loop, after lock acquisition and while alive."""
    state = runtime_attestation(store, role="worker")
    state["heartbeat_at"] = time.time()
    temporary = WORKER_PATH.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(state, sort_keys=True))
    temporary.replace(WORKER_PATH)
    return state


def remove_worker_attestation():
    WORKER_PATH.unlink(missing_ok=True)


def verify_live_worker(*, max_age=15):
    """Check a heartbeat emitted by the running worker, not this helper process."""
    state = json.loads(WORKER_PATH.read_text())
    expected = build_identity()
    for key in (
        "pipeline_id",
        "engine_contract",
        "schema_revision",
        "commit",
        "tree",
        "source_digest",
    ):
        if state.get(key) != expected.get(key):
            raise RuntimeError(f"Worker has a different {key}")
    age = time.time() - float(state.get("heartbeat_at", 0))
    if (
        state.get("role") != "worker"
        or state.get("status") != "ready"
        or not 0 <= age <= max_age
    ):
        raise RuntimeError("Worker attestation is absent or stale")
    pid = state.get("pid")
    if type(pid) is not int or pid <= 0:
        raise RuntimeError("Worker attestation requires a positive process ID")
    os.kill(pid, 0)
    return state


if __name__ == "__main__":
    import sys

    try:
        if sys.argv[1:] == ["worker-health"]:
            print(json.dumps(verify_live_worker(), sort_keys=True))
        else:
            raise RuntimeError(
                "Usage: python -m maigret.web.pipeline_release worker-health"
            )
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        sys.exit(str(error))
