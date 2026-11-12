#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"
load_env
serve_start
do_pull "${1:-}"
curl -sS "http://127.0.0.1:$SERVE_PORT/health"; echo
log "search UI: http://node4:$SERVE_PORT/"
