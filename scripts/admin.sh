#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"
load_env
spark_submit src/ingest/admin.py "$@"
