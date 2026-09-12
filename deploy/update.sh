#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: bash deploy/update.sh --commit <reviewed-full-P2-commit> --manifest <reviewed-release.json> [--check]

Deploy only p2-e2e-v1 from an exact clean reviewed commit and immutable image.
This updater never fetches, pulls, checks out, builds, or selects latest/main.
--check performs read-only checkout, image, manifest and database checks.
The running database must be b3e9d7c4a610, e2e1a7c9d401 or e2e2b8d0a502. A stopped database
must be inspected and started separately. No downgrade or legacy fallback exists.
An application/worker mismatch stops BOTH runtimes and preserves the new schema.
EOF
}
if [[ $# -eq 1 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then usage; exit 0; fi
if [[ $# -lt 4 || $# -gt 5 || "$1" != "--commit" || ! "$2" =~ ^[0-9a-f]{40}$ || "$3" != "--manifest" ]]; then
    usage >&2; exit 1
fi
APPROVED_COMMIT="$2"
MANIFEST="$(realpath -e "$4")"
CHECK_ONLY=false
if [[ $# -eq 5 ]]; then
    [[ "$5" == "--check" ]] || { usage >&2; exit 1; }
    CHECK_ONLY=true
fi
if [[ ${EUID} -ne 0 ]]; then
    exec sudo bash "${BASH_SOURCE[0]}" "$@"
fi
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deploy"
ENV_FILE="${DEPLOY_DIR}/.env"
COMPOSE_FILE="${DEPLOY_DIR}/compose.yaml"
BACKUP_DIR="${REPO_ROOT}/runtime/backups"
TARGET_SCHEMA=e2e2b8d0a502
fail() { echo "P2 update refused: $*" >&2; exit 1; }
cd "${REPO_ROOT}"
[[ -f "${ENV_FILE}" ]] || fail "Restore the existing deployment configuration first."
for prerequisite in git python3 docker tar flock; do
    command -v "${prerequisite}" >/dev/null 2>&1 || fail "Missing ${prerequisite}."
done
detect_compose_project() {
    # Existing deployments predate the explicit project-name installer. Derive
    # their actual Compose project from the one running database instead of
    # guessing a new project and pointing the updater at an empty stack.
    local projects=()
    mapfile -t projects < <(docker ps --filter status=running \
        --filter label=com.docker.compose.service=db \
        --format '{{.Label "com.docker.compose.project"}}' | sed '/^$/d')
    [[ ${#projects[@]} -eq 1 ]] || \
        fail "Expected one running OpenLedger database with a Compose project label."
    [[ "${projects[0]}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || \
        fail "Database Compose project label is invalid."
    printf '%s\n' "${projects[0]}"
}
# Preserve the actual project used by this installation. New installs use
# `deploy`; existing instances such as the production `openledger` project
# remain on their original database volume.
OPENLEDGER_COMPOSE_PROJECT="$(detect_compose_project)"
verify_release() {
    python3 "${DEPLOY_DIR}/release-manifest.py" verify --commit "${APPROVED_COMMIT}" --manifest "${MANIFEST}" "$@"
}
verify_running_database() {
    DATABASE_CONTAINER="$(docker ps --filter status=running \
        --filter "label=com.docker.compose.project=${OPENLEDGER_COMPOSE_PROJECT}" \
        --filter label=com.docker.compose.service=db --format '{{.ID}}')"
    [[ "${DATABASE_CONTAINER}" =~ ^[0-9a-f]{12,64}$ ]] || fail "Expected one running openledger database."
    if ! DATABASE_REVISION="$(docker exec \
        --env 'PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000' \
        "${DATABASE_CONTAINER}" psql -X -w -qAt -v ON_ERROR_STOP=1 -U openledger -d openledger \
        -c 'SELECT version_num FROM public.alembic_version ORDER BY version_num;')"; then
        fail "Could not read the existing database revision."
    fi
    [[ "${DATABASE_REVISION}" == "b3e9d7c4a610" || "${DATABASE_REVISION}" == "e2e1a7c9d401" || "${DATABASE_REVISION}" == "${TARGET_SCHEMA}" ]] || \
        fail "Database must have exactly approved source b3e9d7c4a610, e2e1a7c9d401 or target ${TARGET_SCHEMA}; no downgrade is permitted."
}
# All checks before --check exit are read-only. No service start, build, pull,
# migration, credential bootstrap, generated files or database writes occur here.
export OPENLEDGER_RELEASE_IMAGE
OPENLEDGER_RELEASE_IMAGE="$(verify_release --field image)"
verify_running_database
if [[ "${CHECK_ONLY}" == "true" ]]; then
    echo "P2 preflight passed for ${APPROVED_COMMIT}, pipeline p2-e2e-v1. No changes made."
    exit 0
fi
# Refuse missing original secrets rather than replacing an existing DB password.
for required in runtime/secrets/auth.json runtime/secrets/postgres_password runtime/web_settings.json; do
    [[ -s "${REPO_ROOT}/${required}" && ! -L "${REPO_ROOT}/${required}" ]] || fail "Restore ${required} before deployment."
done
[[ -d "${REPO_ROOT}/runtime/reports" ]] || fail "Restore the report evidence directory first."
# Existing deployment and secrets must be prepared before this approved release.
# Search provider flags select optional engines; they never select pipeline code.
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
umask 077
install -d -m 0700 "${BACKUP_DIR}"
exec 9>"${BACKUP_DIR}/update.lock"
flock -n 9 || fail "Another release update owns the deployment lock."
verify_release >/dev/null
verify_running_database
RELEASE_RECORD="${BACKUP_DIR}/release-${APPROVED_COMMIT}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -m 0700 "${RELEASE_RECORD}"
cp "${MANIFEST}" "${RELEASE_RECORD}/manifest.json"
MANIFEST="${RELEASE_RECORD}/manifest.json"
# Stop ingress first, then allow the worker its bounded graceful stop. Never
# migrate beneath a collecting worker or leave the old app serving after failure.
fail_closed() {
    local code=$?
    trap - EXIT INT TERM
    compose stop app worker caddy || echo 'CRITICAL: stopping failed; inspect containers immediately.' >&2
    echo "Release failed; application and worker are stopped. Evidence/schema retained. No automatic legacy rollback. Record: ${RELEASE_RECORD}" >&2
    exit "${code}"
}
# EXIT also catches explicit fail()/exit paths; ERR alone does not. Signals
# must stop unverified runtimes too, including interruption after compose up.
trap fail_closed EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
compose stop -t 90 caddy app worker
for service in app worker; do
    running="$(docker ps --filter status=running --filter "label=com.docker.compose.project=${OPENLEDGER_COMPOSE_PROJECT}" \
        --filter "label=com.docker.compose.service=${service}" --format '{{.ID}}')"
    [[ -z "${running}" ]] || { echo "Could not drain ${service}." >&2; false; }
done
BACKUP_FILE="${RELEASE_RECORD}/database.dump"
docker exec "${DATABASE_CONTAINER}" pg_dump --format=custom -U openledger -d openledger > "${BACKUP_FILE}"
[[ -s "${BACKUP_FILE}" ]]
docker exec -i "${DATABASE_CONTAINER}" pg_restore --list < "${BACKUP_FILE}" > "${RELEASE_RECORD}/database-index.txt"
tar -C "${REPO_ROOT}" -czf "${RELEASE_RECORD}/evidence-settings.tar.gz" \
    runtime/reports runtime/web_settings.json runtime/secrets deploy/.env
tar -tzf "${RELEASE_RECORD}/evidence-settings.tar.gz" > "${RELEASE_RECORD}/files-index.txt"
verify_release >/dev/null
verify_running_database
# Exact target only. No head, downgrade, stamp, volume deletion or database restore.
compose run --rm --no-deps migrate
verify_running_database
[[ "${DATABASE_REVISION}" == "${TARGET_SCHEMA}" ]]
compose up -d --no-deps --no-build app worker
READY=false
for _ in $(seq 1 45); do
    if compose exec -T app python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:5000/healthz', timeout=3).read().decode())" > "${RELEASE_RECORD}/app.json" 2>/dev/null && \
       compose exec -T worker python -m maigret.web.pipeline_release worker-health > "${RELEASE_RECORD}/worker.json" 2>/dev/null; then
        # Do not retry an incompatible identity: it is a wrong release, not boot lag.
        python3 "${DEPLOY_DIR}/release-manifest.py" runtime --commit "${APPROVED_COMMIT}" --manifest "${MANIFEST}" \
            --app "${RELEASE_RECORD}/app.json" --worker "${RELEASE_RECORD}/worker.json"
        READY=true
        break
    fi
    sleep 2
done
[[ "${READY}" == "true" ]]
# Verify actual container image IDs as well as their process-reported identity.
EXPECTED_IMAGE_ID="$(verify_release --field image_id)"
for service in app worker; do
    container="$(compose ps -q "${service}")"
    [[ "$(docker inspect --format '{{.Image}}' "${container}")" == "${EXPECTED_IMAGE_ID}" ]]
done
# Persist only the immutable image pin so restarts and optional search maintenance
# cannot silently select the old application tag. Preserve all other settings.
python3 - "${ENV_FILE}" "${OPENLEDGER_RELEASE_IMAGE}" <<'PY'
import os, pathlib, re, sys
path = pathlib.Path(sys.argv[1])
lines = [line for line in path.read_text().splitlines() if not re.match(r"\s*OPENLEDGER_RELEASE_IMAGE\s*=", line)]
lines.append("OPENLEDGER_RELEASE_IMAGE='" + sys.argv[2] + "'")
temporary = path.with_suffix('.env.release-tmp')
with temporary.open('w') as stream:
    stream.write('\n'.join(lines) + '\n')
    stream.flush()
    os.fsync(stream.fileno())
temporary.chmod(0o600)
os.replace(temporary, path)
PY
compose up -d --no-deps --no-build caddy
trap - EXIT INT TERM
compose ps
echo "P2 update completed: p2-e2e-v1, commit ${APPROVED_COMMIT}, image ${OPENLEDGER_RELEASE_IMAGE}, schema ${TARGET_SCHEMA}."
echo "App and worker identities match. Release evidence: ${RELEASE_RECORD}"
