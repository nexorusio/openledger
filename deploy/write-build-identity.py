#!/usr/bin/env python3
"""Bake validated source identity into the web image (never an environment override)."""

import importlib.util
import json
import os
from pathlib import Path

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "release", root / "maigret/web/pipeline_release.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
identity = module.validate_build(
    {
        "pipeline_id": module.PIPELINE_ID,
        "engine_contract": module.ENGINE_CONTRACT,
        "schema_revision": module.SCHEMA_REVISION,
        "commit": os.environ.get("OPENLEDGER_RELEASE_COMMIT"),
        "tree": os.environ.get("OPENLEDGER_RELEASE_TREE"),
        "source_digest": os.environ.get("OPENLEDGER_SOURCE_DIGEST"),
    }
)
module.verify_source_content(identity, root=root)
path = root / "openledger-build.json"
path.write_text(json.dumps(identity, sort_keys=True) + "\n")
path.chmod(0o444)
