#!/bin/sh
# Docker HEALTHCHECK: succeeds when the server answers GET /healthz on its listen address.
set -eu

listen="${ISM_LISTEN:-0.0.0.0:8080}"
host="${listen%:*}"
port="${listen##*:}"
case "$host" in
  "" | "0.0.0.0" | "[::]" | "::" | "*") host="127.0.0.1" ;;
esac

exec curl --fail --silent --show-error --max-time 4 --output /dev/null "http://${host}:${port}/healthz"
