#!/bin/sh
# Container entrypoint (runs under tini). Checks that the data directory is writable by the container's
# uid and explains how to fix it, instead of letting SQLite fail with "unable to open database file".
set -eu

data_dir="${ISM_DATA_DIR:-/data}"
mounts_dir="${ISM_MOUNTS_DIR:-/mnt}"

[ "$#" -gt 0 ] || set -- /app/ism-server
case "$1" in
  /app/ism-server | ism-server) ;;
  *) exec "$@" ;;
esac

probe="$data_dir/.ism-write-test.$$"
if ! touch "$probe" 2>/dev/null; then
  cat >&2 <<MSG
entrypoint: ERROR: the data directory $data_dir is not writable by uid $(id -u) (gid $(id -g)).
It holds the database and the encryption key, so the server cannot start.
Fix the ownership of the host folder mounted at $data_dir (for example: sudo chown -R UID:GID <folder>),
or run the container as the user that owns it (compose: user: "UID:GID").
On Docker Desktop for Windows, use a named volume for $data_dir and omit the compose "user:" line.
MSG
  exit 1
fi
rm -f "$probe"

if [ -z "$(find "$mounts_dir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -n 1)" ]; then
  printf 'entrypoint: warning: no folders under %s; mount each library there, e.g. -v /volume1/photo:%s/photo\n' \
    "$mounts_dir" "$mounts_dir" >&2
fi

exec "$@"
