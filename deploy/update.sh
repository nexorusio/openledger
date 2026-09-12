#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: bash deploy/update.sh --commit <reviewed-full-P2-commit> [--check]

Deploy only a clean checkout already at the explicitly reviewed 40-character
P2 commit. This updater never fetches, pulls, or checks out a branch.
--check performs only the code and running database preflight; it changes nothing.
The existing openledger database must be running at revision b3e9d7c4a610.
If it is stopped, inspect/start the existing P2 database separately, then retry.
Older, missing, multiple, or later revisions require separate recovery review;
the updater never downgrades, stamps, or deletes database data.
See deploy/README.md before first activation on a restored server.
EOF
}

if [[ $# -eq 1 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then
    usage
    exit 0
fi
if [[ $# -lt 2 || $# -gt 3 || "$1" != "--commit" || ! "$2" =~ ^[0-9a-f]{40}$ ]]; then
    usage >&2
    exit 1
fi
APPROVED_COMMIT="$2"
CHECK_ONLY=false
if [[ $# -eq 3 ]]; then
    if [[ "$3" != "--check" ]]; then
        usage >&2
        exit 1
    fi
    CHECK_ONLY=true
fi

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
# Preserve the project name produced by the documented installer before it
# began passing --project-name explicitly. Changing this value would select a
# different database volume rather than update the installed application.
OPENLEDGER_COMPOSE_PROJECT=deploy

fail() {
    echo "P2 update refused: $*" >&2
    exit 1
}

verify_pinned_checkout() {
    local current_commit
    local checkout_status
    current_commit="$(git rev-parse --verify HEAD)"
    [[ "${current_commit}" == "${APPROVED_COMMIT}" ]] || \
        fail "HEAD is not the reviewed commit ${APPROVED_COMMIT}. Follow the pinned checkout steps in deploy/README.md."
    if ! checkout_status="$(GIT_OPTIONAL_LOCKS=0 git status --porcelain --untracked-files=all)"; then
        fail "Could not verify that the repository is clean."
    fi
    [[ -z "${checkout_status}" ]] || \
        fail "The repository has local changes. Review them before updating."
    python3 "${DEPLOY_DIR}/check-p2-release.py"
}

verify_running_database() {
    local database_container
    local database_revision
    # Probe the existing service directly. Compose interpolation can require a
    # missing secret, and must not generate one or start services for this check.
    database_container="$(docker ps --filter status=running \
        --filter "label=com.docker.compose.project=${OPENLEDGER_COMPOSE_PROJECT}" \
        --filter label=com.docker.compose.service=db --format '{{.ID}}')"
    [[ "${database_container}" =~ ^[0-9a-f]{12,64}$ ]] || \
        fail "Expected one running openledger database. Inspect/start the existing P2 database separately, then retry."
    if ! database_revision="$(docker exec \
        --env 'PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000' \
        "${database_container}" psql -X -w -qAt -v ON_ERROR_STOP=1 \
        -U openledger -d openledger \
        -c 'SELECT version_num FROM public.alembic_version ORDER BY version_num;')"; then
        fail "Could not read the existing database revision. No update was attempted."
    fi
    [[ "${database_revision}" == "b3e9d7c4a610" ]] || \
        fail "Database must have exactly P2 revision b3e9d7c4a610. No migration, downgrade, or service change was attempted."
    echo "Running database is at P2 revision b3e9d7c4a610."
}

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

ensure_searxng_secret() {
    local secret
    local environment_backup
    if grep -Eq '^[[:space:]]*SEARXNG_SECRET[[:space:]]*=' "${ENV_FILE}"; then
        return
    fi
    install -d -m 0700 "${BACKUP_DIR}"
    environment_backup="${BACKUP_DIR}/deploy.env.pre-searxng.$(date -u +%Y%m%dT%H%M%SZ)"
    cp -a "${ENV_FILE}" "${environment_backup}"
    chmod 0600 "${environment_backup}"
    secret="$(openssl rand -hex 32)"
    if [[ -z "${secret}" ]]; then
        echo "SearXNG local secret generation failed."
        exit 1
    fi
    umask 077
    printf "\nSEARXNG_SECRET='%s'\n" "${secret}" >> "${ENV_FILE}"
    chmod 0600 "${ENV_FILE}"
    unset secret
    echo "Environment backup written to ${environment_backup}."
}

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Missing ${ENV_FILE}. Restore the existing deployment configuration before updating."
    exit 1
fi

cd "${REPO_ROOT}"
for prerequisite in git python3 docker openssl; do
    command -v "${prerequisite}" >/dev/null 2>&1 || \
        fail "Missing ${prerequisite}; install it separately before retrying."
done

# These read-only checks precede ALL runtime writes, backups, builds, migrations,
# package installs, and service mutations. P3 ancestry is intentionally irrelevant:
# a forward rollback can have P3 parents while its approved tree is entirely P2.
verify_pinned_checkout
verify_running_database
if [[ "${CHECK_ONLY}" == "true" ]]; then
    echo "P2 preflight passed for ${APPROVED_COMMIT}. No changes made."
    exit 0
fi

ensure_searxng_secret
COMPOSE_PROFILE_ARGS=()
if grep -Eq "^[[:space:]]*OPENLEDGER_PROFILE_SEARCH_PROVIDER[[:space:]]*=[[:space:]]*['\"]?searxng['\"]?[[:space:]]*$" "${ENV_FILE}"; then
    COMPOSE_PROFILE_ARGS=(--profile self-hosted-search)
fi

compose() {
    docker compose "${COMPOSE_PROFILE_ARGS[@]}" \
        --project-name "${OPENLEDGER_COMPOSE_PROJECT}" \
        --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
}

compose config --quiet

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

BACKUP_FILE="${BACKUP_DIR}/openledger-$(date -u +%Y%m%dT%H%M%SZ).dump"
umask 077
compose exec -T db \
    pg_dump --format=custom -U openledger -d openledger > "${BACKUP_FILE}"
chmod 0600 "${BACKUP_FILE}"
if [[ ! -s "${BACKUP_FILE}" ]] || ! compose exec -T db \
    pg_restore --list < "${BACKUP_FILE}" >/dev/null; then
    echo "Database backup verification failed. No migration was attempted."
    exit 1
fi
echo "Database backup written to ${BACKUP_FILE}."

# Fail if a concurrent checkout or database change happened during backup.
verify_pinned_checkout
verify_running_database
compose build --pull app
compose up -d
compose ps
echo "P2 update completed from reviewed commit ${APPROVED_COMMIT}."
