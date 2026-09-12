#!/usr/bin/env python3
"""Create/verify the external manifest for one reviewed immutable P2 image.

The operator's literal full commit plus reviewed manifest is the approval
boundary, not a signature. The manifest lives outside tracked source to avoid a
self-referential Git commit/tree. Verification never runs migration modules.
"""

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = "p2-e2e-v1"
SCHEMA = "e2e2b8d0a502"
PREVIOUS_SCHEMA = "b3e9d7c4a610"
ACCEPTED_SOURCE_SCHEMAS = [PREVIOUS_SCHEMA, "e2e1a7c9d401", SCHEMA]
SHA = re.compile(r"[0-9a-f]{40}\Z")
IMAGE = re.compile(r"(?:[a-zA-Z0-9._:/-]+@)?sha256:[0-9a-f]{64}\Z")


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def inspect_image(image):
    result = json.loads(command("docker", "image", "inspect", image))
    if len(result) != 1:
        raise ValueError("Expected exactly one locally available immutable image")
    return result[0]


def verify_checkout(commit, root=ROOT):
    if not SHA.fullmatch(commit):
        raise ValueError("A reviewed full lowercase commit SHA is required")
    if command("git", "-C", str(root), "rev-parse", "--verify", "HEAD") != commit:
        raise ValueError("HEAD is not the reviewed commit")
    # --no-optional-locks keeps the preflight read-only even for Git index refresh.
    if command(
        "git",
        "--no-optional-locks",
        "-C",
        str(root),
        "status",
        "--porcelain",
        "--untracked-files=all",
    ):
        raise ValueError("The repository has local changes")
    spec = importlib.util.spec_from_file_location(
        "p2_guard", root / "deploy/check-p2-release.py"
    )
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    guard.verify_p2_tree(root)
    return command("git", "-C", str(root), "rev-parse", "HEAD^{tree}")


def migration_checksums(root=ROOT):
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((root / "migrations/versions").glob("*.py"))
    }


def source_digest(root=ROOT):
    spec = importlib.util.spec_from_file_location(
        "release_source", root / "deploy/source-fingerprint.py"
    )
    fingerprint = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fingerprint)
    return fingerprint.source_fingerprint(root)


def validate_manifest(manifest, commit, tree, *, root=ROOT, inspect=True):
    if manifest.get("manifest_version") != 1:
        raise ValueError("Unknown release manifest version")
    expected = {
        "pipeline_id": PIPELINE,
        "engine_contract": PIPELINE,
        "schema_revision": SCHEMA,
        "commit": commit,
        "tree": tree,
        "source_digest": source_digest(root),
        "accepted_source_schemas": ACCEPTED_SOURCE_SCHEMAS,
        "migration_checksums": migration_checksums(root),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise ValueError(f"Release manifest mismatch: {key}")
    image = manifest.get("image", "")
    if not IMAGE.fullmatch(image) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}", str(manifest.get("image_id", ""))
    ):
        raise ValueError(
            "Release image must use an immutable sha256 ID or repository digest"
        )
    if inspect:
        actual = inspect_image(image)
        if actual.get("Id") != manifest["image_id"]:
            raise ValueError("Local image ID differs from reviewed image")
        labels = actual.get("Config", {}).get("Labels", {})
        for label, value in {
            "org.opencontainers.image.revision": commit,
            "io.openledger.tree": tree,
            "io.openledger.source-digest": source_digest(root),
            "io.openledger.pipeline": PIPELINE,
            "io.openledger.schema": SCHEMA,
            "io.openledger.engine-contract": PIPELINE,
        }.items():
            if labels.get(label) != value:
                raise ValueError(f"Stale or incompatible image label: {label}")
    return manifest


def verify_runtime(manifest, app, worker):
    for role, state in (("app", app), ("worker", worker)):
        if (
            state.get("status") != "ready"
            or state.get("role") != role
            or state.get("development")
        ):
            raise ValueError(f"{role} has no ready production attestation")
        for key in (
            "pipeline_id",
            "engine_contract",
            "schema_revision",
            "commit",
            "tree",
            "source_digest",
        ):
            if state.get(key) != manifest[key]:
                raise ValueError(f"{role} runtime mismatch: {key}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("create", "verify", "runtime"))
    parser.add_argument("--commit", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--image")
    parser.add_argument("--app", type=Path)
    parser.add_argument("--worker", type=Path)
    parser.add_argument("--field", choices=("image", "tree", "image_id"))
    args = parser.parse_args()
    tree = verify_checkout(args.commit)
    if args.action == "create":
        if not args.image or not IMAGE.fullmatch(args.image):
            raise ValueError("Specify the exact built image SHA256; tags are refused")
        if args.manifest.exists():
            raise ValueError(
                "Manifest already exists; never overwrite a reviewed release"
            )
        actual = inspect_image(args.image)
        manifest = dict(
            manifest_version=1,
            pipeline_id=PIPELINE,
            engine_contract=PIPELINE,
            schema_revision=SCHEMA,
            commit=args.commit,
            tree=tree,
            source_digest=source_digest(),
            image=args.image,
            image_id=actual["Id"],
            accepted_source_schemas=ACCEPTED_SOURCE_SCHEMAS,
            migration_checksums=migration_checksums(),
        )
        validate_manifest(manifest, args.commit, tree)
        with args.manifest.open("x") as stream:
            json.dump(manifest, stream, indent=2, sort_keys=True)
            stream.write("\n")
        print(
            f"Prepared candidate manifest: {args.manifest}. This is not merge/deployment authorization."
        )
        return
    if args.manifest.is_symlink() or not args.manifest.is_file():
        raise ValueError("Manifest must be an existing regular file")
    manifest = validate_manifest(
        json.loads(args.manifest.read_text()), args.commit, tree
    )
    if args.action == "runtime":
        if not args.app or not args.worker:
            raise ValueError("Both app and live worker attestation are required")
        app = json.loads(args.app.read_text())
        if app.get("status") != "ok":
            raise ValueError("Application health is not ready")
        verify_runtime(
            manifest, app.get("pipeline", {}), json.loads(args.worker.read_text())
        )
    print(
        manifest[args.field] if args.field else "Reviewed P2 pipeline release verified"
    )


if __name__ == "__main__":
    try:
        main()
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.CalledProcessError,
    ) as error:
        sys.exit(f"P2 update refused: {error}")
