"""Private subprocess entry point for isolated User Scanner collection."""

from __future__ import annotations

import contextlib
import json
import sys

_USERNAME_PLATFORMS = ("facebook", "instagram", "threads", "tiktok", "x")


def _scan_email(request: dict) -> list[dict]:
    email = str(request.get("email") or "").strip().casefold()
    if not email or len(email) > 254 or "@" not in email:
        raise ValueError("A valid email target is required")

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
    return [result.to_dict() for result in results]


def _scan_usernames(request: dict) -> list[dict]:
    targets = request.get("usernames")
    if not isinstance(targets, list) or not targets:
        raise ValueError("At least one username target is required")
    usernames: list[str] = []
    for raw_target in targets[:16]:
        target = str(raw_target or "").strip().lstrip("@")
        if not target or len(target) > 128:
            raise ValueError("Invalid username target")
        if target.casefold() not in {item.casefold() for item in usernames}:
            usernames.append(target)

    requested_platforms = request.get("platforms")
    if requested_platforms is None:
        requested_platforms = list(_USERNAME_PLATFORMS)
    if not isinstance(requested_platforms, list):
        raise ValueError("Username platforms must be a list")
    platforms = []
    for raw_platform in requested_platforms:
        platform = str(raw_platform or "").strip().casefold()
        if platform not in _USERNAME_PLATFORMS:
            raise ValueError("Unsupported username platform")
        if platform not in platforms:
            platforms.append(platform)

    allow_vxtwitter = request.get("allow_vxtwitter") is True
    active_platforms = [
        platform for platform in platforms if platform != "x" or allow_vxtwitter
    ]

    from user_scanner.core.cross_scan import CrossScanConfig, run_cross_scan
    from user_scanner.core.helpers import ScanConfig, find_module, set_global_timeout
    from user_scanner.core.orchestrator import run_user_module, set_concurrency

    set_concurrency(12)
    set_global_timeout(15.0)
    config = ScanConfig(
        allow_loud=False,
        no_nsfw=True,
        show_all=True,
        verbose=False,
        timeout=15.0,
    )
    modules = [
        module
        for platform in active_platforms
        for module in find_module(platform, is_email=False, no_nsfw=True)
    ]
    if len(modules) != len(active_platforms):
        raise RuntimeError("A configured username platform module is unavailable")

    serialized: list[dict] = []
    with contextlib.redirect_stdout(sys.stderr):
        for username in usernames:
            direct = run_user_module(modules, username, config) if modules else []
            for result in direct:
                result.update(
                    extra={
                        "scan_stage": "direct",
                        "seed_username": username,
                        "confidence": "candidate",
                    }
                )
            cross = (
                run_cross_scan(
                    direct,
                    config,
                    CrossScanConfig(
                        links="all",
                        modules=tuple(active_platforms),
                        emails="none",
                        sweep=0,
                        depth=1,
                    ),
                )
                if direct
                else []
            )
            for result in cross:
                result.update(
                    extra={
                        "scan_stage": "cross_scan",
                        "seed_username": username,
                    }
                )
            serialized.extend(result.to_dict() for result in [*direct, *cross])
            if "x" in platforms and not allow_vxtwitter:
                serialized.append(
                    {
                        "status": "Skipped",
                        "reason": (
                            "Disabled by OpenLedger policy because the pinned X "
                            "module contacts api.vxtwitter.com"
                        ),
                        "username": username,
                        "site_name": "X (Twitter)",
                        "category": "Social",
                        "url": f"https://x.com/{username}",
                        "extra": {
                            "scan_stage": "policy",
                            "seed_username": username,
                            "confidence": "candidate",
                        },
                        "media": {},
                    }
                )
    return serialized


def main() -> int:
    try:
        request = json.load(sys.stdin)
        # Importing this module patches httpx clients globally. This process exists
        # specifically to keep that mutation outside the OpenLedger worker.
        mode = str(request.get("mode") or "email").strip().casefold()
        if mode == "email":
            results = _scan_email(request)
        elif mode == "username":
            results = _scan_usernames(request)
        else:
            raise ValueError("Unsupported User Scanner mode")
        json.dump(
            {
                "schema_version": 1,
                "engine": "user-scanner",
                "mode": mode,
                "results": results,
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
