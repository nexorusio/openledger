"""Private subprocess entry point for isolated User Scanner collection."""

from __future__ import annotations

import contextlib
import json
import sys


def main() -> int:
    try:
        request = json.load(sys.stdin)
        email = str(request.get("email") or "").strip().casefold()
        if not email or len(email) > 254 or "@" not in email:
            raise ValueError("A valid email target is required")

        # Importing this module patches httpx clients globally. This process exists
        # specifically to keep that mutation outside the OpenLedger worker.
        from user_scanner.core.email_orchestrator import (
            run_email_full_batch,
            set_concurrency,
        )
        from user_scanner.core.helpers import ScanConfig, set_global_timeout

        set_concurrency(12)
        set_global_timeout(15.0)
        config = ScanConfig(
            allow_loud=False,
            no_nsfw=True,
            show_all=True,
            verbose=False,
            timeout=15.0,
        )
        with contextlib.redirect_stdout(sys.stderr):
            results = run_email_full_batch(email, config)
        json.dump(
            {
                "schema_version": 1,
                "engine": "user-scanner",
                "results": [result.to_dict() for result in results],
            },
            sys.stdout,
            ensure_ascii=False,
        )
        sys.stdout.write("\n")
        return 0
    except Exception as exc:
        print(f"user-scanner adapter error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
