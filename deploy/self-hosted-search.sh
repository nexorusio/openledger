#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
    exec sudo bash "${BASH_SOURCE[0]}" "$@"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${REPO_ROOT}/deploy/.env"
COMPOSE_FILE="${REPO_ROOT}/deploy/compose.yaml"
BACKUP_DIR="${REPO_ROOT}/runtime/backups"
MINIMUM_AVAILABLE_MEMORY_KIB=786432
MINIMUM_AVAILABLE_DISK_KIB=1048576

usage() {
    echo "Usage: sudo bash deploy/self-hosted-search.sh prepare|enable|disable|status"
}

if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi
ACTION="$1"
if [[ "${ACTION}" != "prepare" && "${ACTION}" != "enable" && \
      "${ACTION}" != "disable" && "${ACTION}" != "status" ]]; then
    usage
    exit 2
fi
if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Missing ${ENV_FILE}. Run deploy/install.sh first."
    exit 1
fi

cd "${REPO_ROOT}"

compose() {
    docker compose --profile self-hosted-search \
        --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
}

set_env_value() {
    local key="$1"
    local value="$2"
    if grep -Eq "^[[:space:]]*${key}[[:space:]]*=" "${ENV_FILE}"; then
        sed -i -E \
            "s|^[[:space:]]*${key}[[:space:]]*=.*$|${key}='${value}'|" \
            "${ENV_FILE}"
    else
        printf "\n%s='%s'\n" "${key}" "${value}" >> "${ENV_FILE}"
    fi
    chmod 0600 "${ENV_FILE}"
}

env_value() {
    local key="$1"
    sed -n -E \
        "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*['\"]?([^'\"]*)['\"]?[[:space:]]*$/\1/p" \
        "${ENV_FILE}" | tail -n 1
}

ensure_local_secret() {
    local secret
    if grep -Eq '^[[:space:]]*SEARXNG_SECRET[[:space:]]*=' "${ENV_FILE}"; then
        return
    fi
    secret="$(openssl rand -hex 32)"
    [[ -n "${secret}" ]]
    umask 077
    printf "\nSEARXNG_SECRET='%s'\n" "${secret}" >> "${ENV_FILE}"
    chmod 0600 "${ENV_FILE}"
    unset secret
}

confirm_no_active_investigation() {
    local confirmation
    read -r -p \
        "Confirm no investigation is running. Type CONTINUE to proceed: " \
        confirmation
    if [[ "${confirmation}" != "CONTINUE" ]]; then
        echo "No changes made."
        exit 1
    fi
}

backup_environment() {
    local backup_file
    install -d -m 0700 "${BACKUP_DIR}"
    backup_file="${BACKUP_DIR}/deploy.env.$(date -u +%Y%m%dT%H%M%SZ)"
    cp -a "${ENV_FILE}" "${backup_file}"
    chmod 0600 "${backup_file}"
    echo "Environment backup written to ${backup_file}."
}

check_capacity() {
    local available_memory_kib
    local available_disk_kib
    available_memory_kib="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
    available_disk_kib="$(df -Pk "${REPO_ROOT}" | awk 'NR == 2 {print $4}')"
    echo "Available memory: $((available_memory_kib / 1024)) MiB"
    echo "Available disk: $((available_disk_kib / 1024)) MiB"
    if (( available_memory_kib < MINIMUM_AVAILABLE_MEMORY_KIB )); then
        echo "At least 768 MiB of available memory is required. No changes made."
        exit 1
    fi
    if (( available_disk_kib < MINIMUM_AVAILABLE_DISK_KIB )); then
        echo "At least 1 GiB of available disk is required. No changes made."
        exit 1
    fi
}

wait_for_searxng() {
    local ready=false
    for _ in $(seq 1 30); do
        if compose exec -T searxng \
            /usr/local/searxng/.venv/bin/python -c \
            "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/', timeout=3)" \
            >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 2
    done
    if [[ "${ready}" != "true" ]]; then
        echo "Private SearXNG did not become ready. Search remains disabled."
        compose logs --tail=100 searxng
        exit 1
    fi
}

probe_search() {
    compose exec -T app python -c \
        "import json, urllib.parse, urllib.request; p=urllib.parse.urlencode({'q':'site:example.com \"Example Domain\"','format':'json','safesearch':'2','language':'all','categories':'general'}); r=urllib.request.urlopen('http://searxng:8080/search?'+p, timeout=10); d=json.load(r); assert isinstance(d.get('results'), list); print('private search probe valid')"
}

recreate_runtimes() {
    compose config --quiet
    compose up -d --no-deps --force-recreate app worker
    compose exec -T app python -c \
        "from maigret.web.profile_search_backend import load_profile_search_config; c=load_profile_search_config(); print('app provider='+c.provider)"
    compose exec -T worker python -c \
        "from maigret.web.profile_search_backend import load_profile_search_config; c=load_profile_search_config(); print('worker provider='+c.provider)"
}

show_status() {
    local service
    for service in app worker; do
        echo "${service} profile-search settings:"
        compose exec -T "${service}" env | grep -E \
            '^OPENLEDGER_(SEARCH_FIRST_DISCOVERY_ENABLED|PROFILE_SEARCH_PROVIDER|PROFILE_SEARCH_TIMEOUT_SECONDS|PROFILE_SEARCH_MAX_RESULTS)=' \
            | sort
    done
    compose ps --all
    curl -fsS "https://$(env_value DOMAIN)/healthz"
    echo
}

case "${ACTION}" in
    prepare)
        confirm_no_active_investigation
        check_capacity
        ensure_local_secret
        backup_environment
        set_env_value OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED false
        set_env_value OPENLEDGER_PROFILE_SEARCH_PROVIDER searxng
        compose config --quiet
        compose pull searxng
        compose up -d searxng
        wait_for_searxng
        probe_search
        recreate_runtimes
        show_status
        echo "Private search is prepared and verified; discovery remains disabled."
        ;;
    enable)
        confirm_no_active_investigation
        if [[ "$(env_value OPENLEDGER_PROFILE_SEARCH_PROVIDER)" != "searxng" ]]; then
            echo "Run the prepare action before enabling self-hosted search."
            exit 1
        fi
        wait_for_searxng
        probe_search
        backup_environment
        set_env_value OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED true
        recreate_runtimes
        show_status
        echo "Private search-first discovery is enabled."
        ;;
    disable)
        confirm_no_active_investigation
        backup_environment
        set_env_value OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED false
        set_env_value OPENLEDGER_PROFILE_SEARCH_PROVIDER disabled
        recreate_runtimes
        compose stop searxng || true
        show_status
        echo "Private search is disabled and its container is stopped."
        ;;
    status)
        show_status
        ;;
esac
