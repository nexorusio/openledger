#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $# -ne 4 || "$1" != "--commit" || ! "$2" =~ ^[0-9a-f]{40}$ || "$3" != "--manifest" ]]; then
    echo 'Usage: bash deploy/build-reviewed-release.sh --commit <reviewed-full-P2-commit> --manifest <candidate.json>' >&2
    exit 1
fi
P2_REVIEWED_COMMIT="$2"
P2_MANIFEST_OUTPUT="$4"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"
[[ ! -e "${P2_MANIFEST_OUTPUT}" ]] || { echo 'Manifest already exists; preserve reviewed release artifacts.' >&2; exit 1; }
test "$(git rev-parse --verify HEAD)" = "${P2_REVIEWED_COMMIT}"
test -z "$(git --no-optional-locks status --porcelain --untracked-files=all)"
python3 deploy/check-p2-release.py
P2_REVIEWED_TREE="$(git rev-parse HEAD^{tree})"
P2_SOURCE_DIGEST="$(python3 deploy/source-fingerprint.py)"
P2_IMAGE_RECORD="$(mktemp)"
trap 'rm -f "${P2_IMAGE_RECORD}"' EXIT
docker build --target web --build-arg "OPENLEDGER_RELEASE_COMMIT=${P2_REVIEWED_COMMIT}" \
    --build-arg "OPENLEDGER_RELEASE_TREE=${P2_REVIEWED_TREE}" \
    --build-arg "OPENLEDGER_SOURCE_DIGEST=${P2_SOURCE_DIGEST}" --iidfile "${P2_IMAGE_RECORD}" .
P2_REVIEWED_IMAGE="$(cat "${P2_IMAGE_RECORD}")"
python3 deploy/release-manifest.py create --commit "${P2_REVIEWED_COMMIT}" \
    --image "${P2_REVIEWED_IMAGE}" --manifest "${P2_MANIFEST_OUTPUT}"
echo 'Candidate image and manifest prepared. Merge and deployment require their separate authorizations.'
