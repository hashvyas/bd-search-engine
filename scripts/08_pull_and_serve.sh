#!/usr/bin/env bash
# Node 4: start the search API if needed, pull the latest (or given) export from node3,
# verify it, and switch to it. Any failure keeps the current version live.
# Usage: bash scripts/08_pull_and_serve.sh [version]
set -euo pipefail
. "$(dirname "$0")/lib.sh"
load_env
serve_start
do_pull "${1:-}"
curl -sS "http://127.0.0.1:$SERVE_PORT/health"; echo
log "search UI: http://node4:$SERVE_PORT/"
