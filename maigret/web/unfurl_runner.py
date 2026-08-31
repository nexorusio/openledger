"""Minimal stdin/stdout boundary for the isolated Unfurl runtime.

This file intentionally imports no OpenLedger package. The production image runs it
with Unfurl's dedicated virtual-environment interpreter so Unfurl's Python and
NetworkX requirements cannot mutate Maigret's dependency graph.
"""

from __future__ import annotations

import contextlib
import json
import sys

SCHEMA_VERSION = 1
MAX_INPUT_LENGTH = 2000
MAX_NODE_LIMIT = 80


def _scalar(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def main() -> int:
    try:
        request = json.load(sys.stdin)
        url = str(request.get("url") or "").strip()
        if not url or len(url) > MAX_INPUT_LENGTH:
            raise ValueError("a bounded URL is required")
        requested_limit = request.get("node_limit", MAX_NODE_LIMIT)
        if isinstance(requested_limit, bool):
            raise ValueError("node_limit must be an integer")
        node_limit = max(1, min(int(requested_limit), MAX_NODE_LIMIT))

        # Unfurl installs a stdout logging handler at import time. Redirect during
        # both import and parsing so stdout remains a machine-readable JSON channel.
        with contextlib.redirect_stdout(sys.stderr):
            import unfurl
            from unfurl.core import Unfurl

            parser = Unfurl(remote_lookups=False)
            parser.node_limit = node_limit
            parser.add_to_queue(data_type="url", key=None, value=url)
            parser.parse_queue()
            nodes = [
                {
                    "id": int(node.node_id),
                    "data_type": str(node.data_type or ""),
                    "key": _scalar(node.key),
                    "value": _scalar(node.value),
                    "parent_id": _scalar(node.parent_id),
                }
                for node in parser.nodes.values()
            ]
            version = str(unfurl.__version__)

        json.dump(
            {
                "schema_version": SCHEMA_VERSION,
                "engine": "dfir-unfurl",
                "version": version,
                "remote_lookups": False,
                "node_limit": node_limit,
                "nodes": nodes,
            },
            sys.stdout,
            ensure_ascii=False,
        )
        sys.stdout.write("\n")
        return 0
    except Exception as exc:
        print(f"unfurl adapter error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
