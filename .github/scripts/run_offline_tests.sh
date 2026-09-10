#!/usr/bin/env bash
# Run Linux CI tests with only a private loopback interface. This also isolates
# native transports (libcurl/c-ares) and child processes, not just Python sockets.
# Dependency installation happens before this command, outside the namespace.
set -euo pipefail

if [ "$#" -eq 0 ]; then
    echo "Usage: run_offline_tests.sh <absolute-python-path> <test arguments...>" >&2
    exit 2
fi

test_uid="$(id -u)"
test_gid="$(id -g)"

# A failed namespace setup is fatal: never fall back to the host network.
sudo unshare --net -- /bin/bash -eu -c '
    ip link set lo up
    test "$(ip -o link show | wc -l)" -eq 1
    test -z "$(ip route show)"
    test -z "$(ip -6 route show default)"
    test_uid="$1"
    test_gid="$2"
    shift 2
    # Prevent child processes regaining host-network privileges through sudo
    # or supplementary host-control groups. Restore the target user environment
    # too: sudo may have changed HOME to root before the UID drop.
    # Bound this offline regression gate to ten minutes, with 15 seconds to exit
    # after interruption; this is a CI guard, not a production runtime target.
    exec setpriv --no-new-privs --reuid="$test_uid" --regid="$test_gid" \
        --clear-groups --reset-env -- \
        timeout --signal=INT --kill-after=15s 600s "$@"
' openledger-offline-tests "$test_uid" "$test_gid" "$@"
