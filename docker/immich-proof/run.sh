#!/usr/bin/env bash
# Reproducible proof of ISM's safety assumptions against a disposable stock Immich v3.4.1.
# Usage: docker/immich-proof/run.sh   (KEEP=1 leaves the stack and .work/ in place afterwards)
# The output is printed and saved to last-output.txt (gitignored). Exit code = failed checks.
set -euo pipefail
cd "$(dirname "$0")"

cleanup() {
  if [[ "${KEEP:-0}" != 1 ]]; then
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf .work
  fi
}

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf .work
mkdir -p .work/ext .work/ism-trash
trap cleanup EXIT
docker compose up -d --wait database redis
docker compose up -d server
set +e
python3 proof.py 2>&1 | tee last-output.txt
status=${PIPESTATUS[0]}
set -e
exit "$status"
