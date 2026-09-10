#!/usr/bin/env bash
# Run the P3R full acceptance gate in a private loopback-only namespace.
# PostgreSQL is started inside that namespace so the worker, its spawned child,
# Flask, Playwright, and native transports have no external route.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: run_p3r_full_acceptance.sh <absolute-python-path>" >&2
    exit 2
fi

test_python="$1"
test_uid="$(id -u)"
test_gid="$(id -g)"
test_home="$HOME"
leaflet_js="${OPENLEDGER_TEST_LEAFLET_JS:?OPENLEDGER_TEST_LEAFLET_JS is required}"
leaflet_css="${OPENLEDGER_TEST_LEAFLET_CSS:?OPENLEDGER_TEST_LEAFLET_CSS is required}"
test -s "$leaflet_js"
test -s "$leaflet_css"

sudo unshare --net -- /bin/bash -eu -c '
    ip link set lo up
    test "$(ip -o link show | wc -l)" -eq 1
    test -z "$(ip route show)"
    test -z "$(ip -6 route show default)"

    test_python="$1"
    test_uid="$2"
    test_gid="$3"
    test_home="$4"
    leaflet_js="$5"
    leaflet_css="$6"
    pg_bindir="$(pg_config --bindir)"
    runtime_dir="$(mktemp -d)"
    chown "$test_uid:$test_gid" "$runtime_dir"
    install -d -o "$test_uid" -g "$test_gid" "$runtime_dir/data" "$runtime_dir/socket"
    postgres_pid=""

    cleanup() {
        if [ -n "$postgres_pid" ] && kill -0 "$postgres_pid" 2>/dev/null; then
            kill -TERM "$postgres_pid" || true
            wait "$postgres_pid" || true
        fi
        rm -rf "$runtime_dir"
    }
    trap cleanup EXIT INT TERM

    setpriv --no-new-privs --reuid="$test_uid" --regid="$test_gid" --clear-groups -- \
        "$pg_bindir/initdb" -D "$runtime_dir/data" --auth=trust \
        --username=openledger_test --no-locale --encoding=UTF8 >/dev/null
    setpriv --no-new-privs --reuid="$test_uid" --regid="$test_gid" --clear-groups -- \
        "$pg_bindir/postgres" -D "$runtime_dir/data" -k "$runtime_dir/socket" -h "" \
        >"$runtime_dir/postgres.log" 2>&1 &
    postgres_pid="$!"
    for attempt in $(seq 1 100); do
        if "$pg_bindir/pg_isready" -h "$runtime_dir/socket" -U openledger_test -d postgres >/dev/null; then
            break
        fi
        sleep 0.1
    done
    "$pg_bindir/pg_isready" -h "$runtime_dir/socket" -U openledger_test -d postgres >/dev/null
    setpriv --no-new-privs --reuid="$test_uid" --regid="$test_gid" --clear-groups -- \
        "$pg_bindir/createdb" -h "$runtime_dir/socket" -U openledger_test openledger_test

    database_url="postgresql+psycopg://openledger_test@/openledger_test?host=$runtime_dir/socket"
    test_environment=(
        env HOME="$test_home" PATH="$pg_bindir:/usr/local/bin:/usr/bin:/bin"
        DATABASE_URL="$database_url" OPENLEDGER_TEST_POSTGRES_URL="$database_url"
        OPENLEDGER_P3R_FULL_ACCEPTANCE=1 OPENLEDGER_P3R_OFFLINE_NAMESPACE=1
        OPENLEDGER_TEST_LEAFLET_JS="$leaflet_js" OPENLEDGER_TEST_LEAFLET_CSS="$leaflet_css"
    )
    setpriv --no-new-privs --reuid="$test_uid" --regid="$test_gid" --clear-groups --reset-env -- \
        "${test_environment[@]}" "$test_python" -m alembic upgrade head
    setpriv --no-new-privs --reuid="$test_uid" --regid="$test_gid" --clear-groups --reset-env -- \
        "${test_environment[@]}" "$test_python" -m pytest -q tests/test_p3r_full_acceptance.py tests/p3r_full_browser_journey.py tests/p3r_full_map_journey.py
' openledger-p3r-full-acceptance "$test_python" "$test_uid" "$test_gid" "$test_home" "$leaflet_js" "$leaflet_css"
