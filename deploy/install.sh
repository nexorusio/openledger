#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
    exec sudo bash "${BASH_SOURCE[0]}" "$@"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deploy"
ENV_FILE="${DEPLOY_DIR}/.env"
COMPOSE_FILE="${DEPLOY_DIR}/compose.yaml"
DEFAULT_DOMAIN="openledger.nexorus.io"

AUTH_PASSWORD=""
OPENAI_API_KEY_INPUT=""
trap 'unset AUTH_PASSWORD OPENAI_API_KEY_INPUT' EXIT

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
    apt-get install -y ca-certificates curl gnupg openssl
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

if [[ -f "${ENV_FILE}" ]]; then
    read -r -p "Existing deployment configuration found. Replace it? [y/N] " REPLACE_ENV
    if [[ ! "${REPLACE_ENV}" =~ ^[Yy]$ ]]; then
        echo "No changes made."
        exit 0
    fi
fi

read -r -p "Public domain [${DEFAULT_DOMAIN}]: " DOMAIN
DOMAIN="${DOMAIN:-${DEFAULT_DOMAIN}}"
if ! validate_domain "${DOMAIN}"; then
    echo "Enter a hostname only, for example openledger.nexorus.io."
    exit 1
fi

read -r -p "Browser login username [admin]: " AUTH_USER
AUTH_USER="${AUTH_USER:-admin}"
if [[ ! "${AUTH_USER}" =~ ^[A-Za-z0-9_.-]+$ ]]; then
    echo "The login username may contain letters, numbers, dots, underscores, and hyphens."
    exit 1
fi

read -r -s -p "Browser login password (minimum 12 characters): " AUTH_PASSWORD
echo
if [[ ${#AUTH_PASSWORD} -lt 12 ]]; then
    echo "The browser password must contain at least 12 characters."
    exit 1
fi
read -r -s -p "Confirm browser login password: " AUTH_PASSWORD_CONFIRM
echo
if [[ "${AUTH_PASSWORD}" != "${AUTH_PASSWORD_CONFIRM}" ]]; then
    echo "The passwords do not match."
    exit 1
fi
unset AUTH_PASSWORD_CONFIRM

read -r -s -p "OpenAI API key (leave empty to configure later): " OPENAI_API_KEY_INPUT
echo
if [[ "${OPENAI_API_KEY_INPUT}" == *"'"* ]]; then
    echo "The API key contains an unsupported quote character."
    exit 1
fi
read -r -p "OpenAI model [gpt-5.4]: " OPENAI_MODEL
OPENAI_MODEL="${OPENAI_MODEL:-gpt-5.4}"
if [[ ! "${OPENAI_MODEL}" =~ ^[A-Za-z0-9._:-]+$ ]]; then
    echo "The model name contains unsupported characters."
    exit 1
fi

echo "Installing Docker if needed..."
install_docker

echo "Preparing persistent runtime directories..."
install -d -m 0750 "${REPO_ROOT}/runtime/reports"
if [[ ! -f "${REPO_ROOT}/runtime/web_settings.json" ]]; then
    install -m 0600 /dev/null "${REPO_ROOT}/runtime/web_settings.json"
    printf '{}\n' > "${REPO_ROOT}/runtime/web_settings.json"
fi

echo "Generating protected credentials..."
AUTH_PASSWORD_HASH="$(docker run --rm caddy:2-alpine caddy hash-password --plaintext "${AUTH_PASSWORD}")"
FLASK_SECRET_KEY="$(openssl rand -hex 32)"
unset AUTH_PASSWORD

umask 077
{
    printf "DOMAIN='%s'\n" "${DOMAIN}"
    printf "AUTH_USER='%s'\n" "${AUTH_USER}"
    printf "AUTH_PASSWORD_HASH='%s'\n" "${AUTH_PASSWORD_HASH}"
    printf "FLASK_SECRET_KEY='%s'\n" "${FLASK_SECRET_KEY}"
    printf "OPENAI_API_KEY='%s'\n" "${OPENAI_API_KEY_INPUT}"
    printf "OPENAI_MODEL='%s'\n" "${OPENAI_MODEL}"
    printf "OPENAI_API_BASE_URL='https://api.openai.com/v1'\n"
} > "${ENV_FILE}"
chmod 0600 "${ENV_FILE}"
unset OPENAI_API_KEY_INPUT AUTH_PASSWORD_HASH FLASK_SECRET_KEY

echo "Validating the Compose configuration..."
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" config --quiet

echo "Building and starting OpenLedger. The first build can take several minutes..."
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" up -d --build

echo "Waiting for the application health check..."
READY=false
for _ in $(seq 1 60); do
    if docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" exec -T app python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/healthz', timeout=3)" >/dev/null 2>&1; then
        READY=true
        break
    fi
    sleep 2
done

docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" ps

if [[ "${READY}" != "true" ]]; then
    echo
    echo "OpenLedger did not become healthy within two minutes."
    echo "Inspect logs with: cd ${DEPLOY_DIR} && docker compose logs --tail=200"
    exit 1
fi

echo
echo "OpenLedger is running."
echo "Open https://${DOMAIN} after DNS resolves and ports 80/443 are reachable."
echo "The login username is ${AUTH_USER}. The password was not written to terminal output."
