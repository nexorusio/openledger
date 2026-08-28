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
DATABASE_PASSWORD_FILE="${REPO_ROOT}/runtime/secrets/postgres_password"
BACKUP_DIR="${REPO_ROOT}/runtime/backups"
OPENLEDGER_APP_UID=10001
OPENLEDGER_APP_GID=10001

ensure_database_password() {
    local password_file="${DATABASE_PASSWORD_FILE}"
    local temporary_file
    if [[ -e "${password_file}" && ( ! -f "${password_file}" || -L "${password_file}" ) ]]; then
        echo "Database password path must be a regular file: ${password_file}"
        echo "Move the unexpected path aside, then run the updater again."
        exit 1
    fi
    if [[ ! -s "${password_file}" ]]; then
        install -d -m 0700 -o "${OPENLEDGER_APP_UID}" -g "${OPENLEDGER_APP_GID}" "${REPO_ROOT}/runtime/secrets"
        temporary_file="$(mktemp "${password_file}.XXXXXX")"
        openssl rand -hex 32 > "${temporary_file}"
        if [[ ! -s "${temporary_file}" ]]; then
            rm -f "${temporary_file}"
            echo "Database password generation failed."
            exit 1
        fi
        chown "${OPENLEDGER_APP_UID}:${OPENLEDGER_APP_GID}" "${temporary_file}"
        chmod 0600 "${temporary_file}"
        mv -f "${temporary_file}" "${password_file}"
    fi
    chown "${OPENLEDGER_APP_UID}:${OPENLEDGER_APP_GID}" "${password_file}"
    chmod 0600 "${password_file}"
}

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

if ! command -v python3 >/dev/null 2>&1 || ! command -v openssl >/dev/null 2>&1; then
    apt-get update
    apt-get install -y openssl python3
fi

if [[ ! -s "${AUTH_FILE}" ]]; then
    echo "This update replaces the browser credential popup with an OpenLedger login page."
    echo "Configure the application login before the proxy authentication is removed."
    bash "${DEPLOY_DIR}/configure-auth.sh" "${AUTH_FILE}"
fi

ensure_database_password
install -d -m 0750 -o "${OPENLEDGER_APP_UID}" -g "${OPENLEDGER_APP_GID}" "${REPO_ROOT}/runtime/reports"
install -d -m 0700 "${BACKUP_DIR}"
chown -R "${OPENLEDGER_APP_UID}:${OPENLEDGER_APP_GID}" \
    "${REPO_ROOT}/runtime/reports" \
    "${REPO_ROOT}/runtime/secrets"
chown "${OPENLEDGER_APP_UID}:${OPENLEDGER_APP_GID}" \
    "${REPO_ROOT}/runtime/web_settings.json"

echo "Starting the private case database..."
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" up -d db
DATABASE_READY=false
for _ in $(seq 1 30); do
    if docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" exec -T db pg_isready -U openledger -d openledger >/dev/null 2>&1; then
        DATABASE_READY=true
        break
    fi
    sleep 2
done
if [[ "${DATABASE_READY}" != "true" ]]; then
    echo "The OpenLedger database did not become ready. No migration was attempted."
    exit 1
fi

BACKUP_FILE="${BACKUP_DIR}/openledger-$(date -u +%Y%m%dT%H%M%SZ).dump"
umask 077
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" exec -T db \
    pg_dump --format=custom -U openledger -d openledger > "${BACKUP_FILE}"
chmod 0600 "${BACKUP_FILE}"
if [[ ! -s "${BACKUP_FILE}" ]] || ! docker compose \
    --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" exec -T db \
    pg_restore --list < "${BACKUP_FILE}" >/dev/null; then
    echo "Database backup verification failed. No migration was attempted."
    exit 1
fi
echo "Database backup written to ${BACKUP_FILE}."

docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" build --pull app
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" up -d
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" ps
