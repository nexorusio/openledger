#!/usr/bin/env python3
"""Read-only accident guards for a separately reviewed, explicitly pinned P2 tree.

The operator's full commit pin is the approval boundary. The channel marker and
known artifact checks are additional safeguards, not release authentication.
Migration modules are parsed, never imported or executed during this check.
"""

import ast
from pathlib import Path
import sys

P2_REVISIONS = (
    "33a669de9c7c",
    "7f0f3a91c2de",
    "c18e7f42a9bd",
    "d43f8a2b9c10",
    "e91b7a4c2d6f",
    "a62f1d7e4b90",
    "f04c2a8d1b73",
    "b91e2f6a4c8d",
    "c47a1e9d5b20",
    "62c42b4e9a10",
    "7ab831f4d2c0",
    "8c4f2a1d9e70",
    "b3e9d7c4a610",
)
P3_ARTIFACT_PATTERNS = (
    "maigret/web/artifact_execution.py",
    "maigret/web/collection_accounting.py",
    "maigret/web/collection_orchestration.py",
    "maigret/web/evidence_correlation*.py",
    "maigret/web/governed_pivots.py",
    "maigret/web/location_records.py",
    "maigret/web/location_resolution.py",
    "maigret/web/location_routes.py",
    "maigret/web/profile_checkpoint.py",
    "maigret/web/worker_execution.py",
    "maigret/web/static/collection-progress.js",
    "maigret/web/static/investigation-builder.js",
    "maigret/web/templates/affiliation_sites.html",
    "schemas/evidence-correlation*.json",
    "schemas/investigation-token-plan*.json",
)


def verify_p2_tree(root):
    marker = root / "deploy" / "release-channel"
    if marker.is_symlink() or marker.read_text(encoding="utf-8").strip() != "p2":
        raise ValueError("This checkout is not marked for the P2 release channel.")
    for pattern in P3_ARTIFACT_PATTERNS:
        if any(root.glob(pattern)):
            raise ValueError(f"P3 artifact present: {pattern}")

    revisions = {}
    for path in (root / "migrations" / "versions").glob("*.py"):
        if path.is_symlink():
            raise ValueError("Migration symlinks are not permitted.")
        values = {}
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            if isinstance(node, ast.AnnAssign):
                targets = [node.target]
            elif isinstance(node, ast.Assign):
                targets = node.targets
            else:
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id in (
                    "revision",
                    "down_revision",
                    "branch_labels",
                    "depends_on",
                ):
                    if target.id in values:
                        raise ValueError("Ambiguous migration declaration.")
                    values[target.id] = ast.literal_eval(node.value)
        revision = values["revision"]
        if (
            revision in revisions
            or values["branch_labels"] is not None
            or values["depends_on"] is not None
        ):
            raise ValueError("Unexpected migration branch or dependency.")
        revisions[revision] = values["down_revision"]
    expected = dict(zip(P2_REVISIONS, (None,) + P2_REVISIONS[:-1]))
    if revisions != expected:
        raise ValueError(
            "Migration tree must be exactly the known P2 chain ending at b3e9d7c4a610."
        )


if __name__ == "__main__":
    try:
        verify_p2_tree(Path(__file__).resolve().parents[1])
    except (OSError, ValueError, SyntaxError, KeyError, TypeError) as error:
        sys.exit(f"P2 update refused: {error}")
    print("P2 release tree verified (schema b3e9d7c4a610).")
