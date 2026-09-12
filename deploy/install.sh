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
DEFAULT_DOMAIN="openledger.nexorus.io"
OPENLEDGER_APP_UID=10001
OPENLEDGER_APP_GID=10001
# Existing installations created from deploy/compose.yaml use Compose's
# directory-derived project name. Pin it explicitly so future commands address
# the same database volume instead of silently creating a second stack.
OPENLEDGER_COMPOSE_PROJECT=deploy

compose() {
    docker compose --project-name "${OPENLEDGER_COMPOSE_PROJECT}" \
        --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
}

# The installer cannot select an application release or bootstrap an older image.
if [[ $# -ne 4 || "$1" != "--commit" || ! "$2" =~ ^[0-9a-f]{40}$ || "$3" != "--manifest" ]]; then
    echo 'Usage: bash deploy/install.sh --commit <reviewed-full-P2-commit> --manifest <reviewed-release.json>' >&2
    exit 1
fi
APPROVED_COMMIT="$2"
MANIFEST="$(realpath -e "$4")"
cd "${REPO_ROOT}"
export OPENLEDGER_RELEASE_IMAGE
OPENLEDGER_RELEASE_IMAGE="$(python3 deploy/release-manifest.py verify --commit "${APPROVED_COMMIT}" --manifest "${MANIFEST}" --field image)"
if [[ -f "${ENV_FILE}" ]]; then
    echo 'Existing installation detected. Use the reviewed manifest with deploy/update.sh.' >&2
    exit 1
fi

ensure_database_password() {
    local password_file="${REPO_ROOT}/runtime/secrets/postgres_password"
    local temporary_file
    if [[ -e "${password_file}" && ( ! -f "${password_file}" || -L "${password_file}" ) ]]; then
        echo "Database password path must be a regular file: ${password_file}"
        echo "Move the unexpected path aside, then run the installer again."
        exit 1
    fi
    if [[ ! -s "${password_file}" ]]; then
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

install_docker() {
    if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
        return
    fi

    . /etc/os-release
    if [[ "${ID}" != "ubuntu" && "${ID}" != "debian" ]]; then
        echo "This installer supports Ubuntu and Debian Droplets."
        exit 1
    fi

    apt-get update
    apt-get install -y ca-certificates curl gnupg openssl python3
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL "https://download.docker.com/linux/${ID}/gpg" -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc

    printf '%s\n' "Types: deb" "URIs: https://download.docker.com/linux/${ID}" "Suites: ${VERSION_CODENAME}" "Components: stable" "Architectures: $(dpkg --print-architecture)" "Signed-By: /etc/apt/keyrings/docker.asc" > /etc/apt/sources.list.d/docker.sources

    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    systemctl enable --now docker
}

validate_domain() {
    if [[ ! "$1" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]]; then
        return 1
    fi
    if [[ "$1" != *.* || "$1" == http://* || "$1" == https://* ]]; then
        return 1
    fi
    return 0
}

read -r -p "Public domain [${DEFAULT_DOMAIN}]: " DOMAIN
DOMAIN="${DOMAIN:-${DEFAULT_DOMAIN}}"
if ! validate_domain "${DOMAIN}"; then
    echo "Enter a hostname only, for example openledger.nexorus.io."
    exit 1
fi

read -r -p "OpenAI model [gpt-5.6-terra]: " OPENAI_MODEL
OPENAI_MODEL="${OPENAI_MODEL:-gpt-5.6-terra}"
if [[ ! "${OPENAI_MODEL}" =~ ^[A-Za-z0-9._:-]+$ ]]; then
    echo "The model name contains unsupported characters."
    exit 1
fi

echo "Installing Docker if needed..."
install_docker
if ! command -v python3 >/dev/null 2>&1 || ! command -v openssl >/dev/null 2>&1; then
    apt-get update
    apt-get install -y openssl python3
fi

echo "Preparing persistent runtime directories..."
install -d -m 0750 -o "${OPENLEDGER_APP_UID}" -g "${OPENLEDGER_APP_GID}" "${REPO_ROOT}/runtime/reports"
install -d -m 0700 -o "${OPENLEDGER_APP_UID}" -g "${OPENLEDGER_APP_GID}" "${REPO_ROOT}/runtime/secrets"
if [[ ! -f "${REPO_ROOT}/runtime/web_settings.json" ]]; then
    install -m 0600 -o "${OPENLEDGER_APP_UID}" -g "${OPENLEDGER_APP_GID}" /dev/null "${REPO_ROOT}/runtime/web_settings.json"
    printf '{}\n' > "${REPO_ROOT}/runtime/web_settings.json"
fi
if [[ ! -f "${REPO_ROOT}/runtime/secrets/openai_api_key" ]]; then
    install -m 0600 -o "${OPENLEDGER_APP_UID}" -g "${OPENLEDGER_APP_GID}" /dev/null "${REPO_ROOT}/runtime/secrets/openai_api_key"
fi
ensure_database_password
chown "${OPENLEDGER_APP_UID}:${OPENLEDGER_APP_GID}" \
    "${REPO_ROOT}/runtime/web_settings.json" \
    "${REPO_ROOT}/runtime/secrets/openai_api_key"
install -d -m 0700 "${REPO_ROOT}/runtime/backups"

echo "Generating protected credentials..."
bash "${DEPLOY_DIR}/configure-auth.sh" "${AUTH_FILE}"
FLASK_SECRET_KEY="$(openssl rand -hex 32)"
SEARXNG_SECRET="$(openssl rand -hex 32)"

umask 077
{
    printf "OPENLEDGER_RELEASE_IMAGE='%s'\n" "${OPENLEDGER_RELEASE_IMAGE}"
    printf "DOMAIN='%s'\n" "${DOMAIN}"
    printf "FLASK_SECRET_KEY='%s'\n" "${FLASK_SECRET_KEY}"
    printf "SEARXNG_SECRET='%s'\n" "${SEARXNG_SECRET}"
    printf "OPENAI_MODEL='%s'\n" "${OPENAI_MODEL}"
    printf "OPENAI_API_BASE_URL='https://api.openai.com/v1'\n"
    printf "OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED='false'\n"
    printf "OPENLEDGER_PROFILE_SEARCH_PROVIDER='disabled'\n"
    printf "OPENLEDGER_PROFILE_SEARCH_TIMEOUT_SECONDS='10'\n"
    printf "OPENLEDGER_PROFILE_SEARCH_MAX_RESULTS='5'\n"
} > "${ENV_FILE}"
chmod 0600 "${ENV_FILE}"
unset FLASK_SECRET_KEY SEARXNG_SECRET

echo "Validating the Compose configuration..."
compose config --quiet

fail_closed() {
    compose stop app worker caddy
    echo 'Installation failed; no fallback to a previous pipeline is permitted.' >&2
}
trap fail_closed ERR
echo "Starting the reviewed P2 pipeline image..."
compose up -d --no-build

echo "Waiting for the application health check..."
READY=false
for _ in $(seq 1 60); do
    if compose exec -T app python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/healthz', timeout=3)" >/dev/null 2>&1; then
        READY=true
        break
    fi
    sleep 2
done

compose ps

if [[ "${READY}" != "true" ]]; then
    echo
    echo "OpenLedger did not become healthy within two minutes."
    echo "Inspect logs with: cd ${DEPLOY_DIR} && docker compose --project-name ${OPENLEDGER_COMPOSE_PROJECT} logs --tail=200"
    exit 1
fi

echo
compose exec -T app python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:5000/healthz', timeout=3).read().decode())" > "${REPO_ROOT}/runtime/backups/install-app.json"
compose exec -T worker python -m maigret.web.pipeline_release worker-health > "${REPO_ROOT}/runtime/backups/install-worker.json"
python3 deploy/release-manifest.py runtime --commit "${APPROVED_COMMIT}" --manifest "${MANIFEST}" --app "${REPO_ROOT}/runtime/backups/install-app.json" --worker "${REPO_ROOT}/runtime/backups/install-worker.json"
trap - ERR
echo "OpenLedger p2-e2e-v1 app and worker are running from the reviewed release."
echo "Open https://${DOMAIN} after DNS resolves and ports 80/443 are reachable."
echo "Use the application username and password configured during installation."
echo "Connect OpenAI from Settings after signing in."
