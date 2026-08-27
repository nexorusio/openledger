#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
    exec sudo bash "${BASH_SOURCE[0]}" "$@"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bash "${REPO_ROOT}/deploy/configure-auth.sh"

cd "${REPO_ROOT}/deploy"
docker compose restart app
echo "Password reset complete. Existing browser sessions have been invalidated."
