#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
    exec sudo bash "${BASH_SOURCE[0]}" "$@"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deploy"
ENV_FILE="${DEPLOY_DIR}/.env"
COMPOSE_FILE="${DEPLOY_DIR}/compose.yaml"
AUTH_FILE="${REPO_ROOT}/runtime/secrets/auth.json"

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Missing ${ENV_FILE}. Run deploy/install.sh first."
    exit 1
fi

cd "${REPO_ROOT}"
if [[ -n "$(git status --porcelain)" ]]; then
    echo "The repository has local changes. Review them before updating."
    git status --short
    exit 1
fi

git fetch origin
git pull --ff-only origin main

if ! command -v python3 >/dev/null 2>&1; then
    apt-get update
    apt-get install -y python3
fi

if [[ ! -s "${AUTH_FILE}" ]]; then
    echo "This update replaces the browser credential popup with an OpenLedger login page."
    echo "Configure the application login before the proxy authentication is removed."
    bash "${DEPLOY_DIR}/configure-auth.sh" "${AUTH_FILE}"
fi

docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" build --pull app
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" up -d
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" ps
