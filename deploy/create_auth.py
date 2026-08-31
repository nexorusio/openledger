#!/usr/bin/env python3
"""Create OpenLedger's protected application-authentication file."""

import base64
import hashlib
import json
import os
import re
import secrets
import sys
import uuid

SCHEMA_VERSION = 2
ITERATIONS = 600_000
MIN_PASSWORD_LENGTH = 12
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def load_existing_users(auth_path: str) -> list[dict]:
    try:
        with open(auth_path, encoding="utf-8") as auth_file:
            payload = json.load(auth_file)
    except FileNotFoundError:
        return []
    except (OSError, TypeError, ValueError):
        raise ValueError("The existing authentication file is invalid.")
    if payload.get("schema_version") == 1:
        username = payload.get("username")
        password = payload.get("password")
        revision = payload.get("revision")
        if (
            not isinstance(username, str)
            or not USERNAME_PATTERN.fullmatch(username)
            or not isinstance(password, dict)
            or not isinstance(revision, str)
        ):
            raise ValueError("The existing authentication file is invalid.")
        return [
            {
                "username": username,
                "role": "admin",
                "revision": revision,
                "password": password,
            }
        ]
    users = payload.get("users")
    if payload.get("schema_version") != SCHEMA_VERSION or not isinstance(users, list):
        raise ValueError("The existing authentication file is invalid.")
    return users


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: create_auth.py AUTH_FILE USERNAME", file=sys.stderr)
        return 2

    auth_path = os.path.abspath(sys.argv[1])
    username = sys.argv[2]
    password = sys.stdin.read()
    if not USERNAME_PATTERN.fullmatch(username):
        print("Invalid authentication username.", file=sys.stderr)
        return 2
    if len(password) < MIN_PASSWORD_LENGTH:
        print(
            f"Password must contain at least {MIN_PASSWORD_LENGTH} characters.",
            file=sys.stderr,
        )
        return 2

    salt = secrets.token_bytes(32)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ITERATIONS)
    password_record = {
        "algorithm": "pbkdf2_sha256",
        "iterations": ITERATIONS,
        "salt": base64.b64encode(salt).decode("ascii"),
        "digest": base64.b64encode(digest).decode("ascii"),
    }
    try:
        existing_users = load_existing_users(auth_path)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    replacement = {
        "username": username,
        "role": "admin",
        "revision": secrets.token_urlsafe(24),
        "password": password_record,
    }
    # This utility resets the single administrator account. Preserve analyst
    # accounts, but never leave an older administrator credential active.
    users = [
        user
        for user in existing_users
        if user.get("role") == "analyst"
        and str(user.get("username") or "").casefold() != username.casefold()
    ]
    users.insert(0, replacement)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "revision": secrets.token_urlsafe(24),
        "users": users,
    }

    auth_directory = os.path.dirname(auth_path)
    os.makedirs(auth_directory, mode=0o700, exist_ok=True)
    os.chmod(auth_directory, 0o700)
    temporary_path = f"{auth_path}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as auth_file:
            json.dump(payload, auth_file, indent=2)
            auth_file.write("\n")
            auth_file.flush()
            os.fsync(auth_file.fileno())
        os.replace(temporary_path, auth_path)
        os.chmod(auth_path, 0o600)
    except Exception:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
