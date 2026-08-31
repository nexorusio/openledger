#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
    exec sudo bash "${BASH_SOURCE[0]}" "$@"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AUTH_FILE="${1:-${REPO_ROOT}/runtime/secrets/auth.json}"
DEFAULT_USERNAME="admin"
OPENLEDGER_APP_UID="${OPENLEDGER_APP_UID:-10001}"
OPENLEDGER_APP_GID="${OPENLEDGER_APP_GID:-10001}"
AUTH_PASSWORD=""
trap 'unset AUTH_PASSWORD' EXIT

if [[ -f "${AUTH_FILE}" ]]; then
    EXISTING_USERNAME="$(python3 - "${AUTH_FILE}" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding='utf-8') as auth_file:
        payload = json.load(auth_file)
        if payload.get('schema_version') == 1:
            print(payload.get('username', ''))
        else:
            print(next(
                (
                    user.get('username', '')
                    for user in payload.get('users', [])
                    if user.get('role') == 'admin'
                ),
                '',
            ))
except (OSError, ValueError, TypeError):
    print('')
PY
)"
    DEFAULT_USERNAME="${EXISTING_USERNAME:-${DEFAULT_USERNAME}}"
fi

read -r -p "OpenLedger login username [${DEFAULT_USERNAME}]: " AUTH_USER
AUTH_USER="${AUTH_USER:-${DEFAULT_USERNAME}}"
if [[ ! "${AUTH_USER}" =~ ^[A-Za-z0-9_.-]{1,64}$ ]]; then
    echo "The username may contain letters, numbers, dots, underscores, and hyphens."
    exit 1
fi

read -r -s -p "OpenLedger login password (minimum 12 characters): " AUTH_PASSWORD
echo
if [[ ${#AUTH_PASSWORD} -lt 12 ]]; then
    echo "The password must contain at least 12 characters."
    exit 1
fi
read -r -s -p "Confirm OpenLedger login password: " AUTH_PASSWORD_CONFIRM
echo
if [[ "${AUTH_PASSWORD}" != "${AUTH_PASSWORD_CONFIRM}" ]]; then
    echo "The passwords do not match."
    exit 1
fi
unset AUTH_PASSWORD_CONFIRM

printf '%s' "${AUTH_PASSWORD}" | python3 "${REPO_ROOT}/deploy/create_auth.py" "${AUTH_FILE}" "${AUTH_USER}"
chown "${OPENLEDGER_APP_UID}:${OPENLEDGER_APP_GID}" "${AUTH_FILE}"
unset AUTH_PASSWORD
echo "Application login configured for ${AUTH_USER}."
