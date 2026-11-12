#!/usr/bin/env bash
# Iceberg inspection and rollback (snapshots, time travel, ingest_log, doc_changes).
# Usage: bash scripts/admin.sh state | snapshots <table> | changes [phase] | log | count <table> [snapshot] | rollback <table> <snapshot>
set -euo pipefail
. "$(dirname "$0")/lib.sh"
load_env
spark_submit src/ingest/admin.py "$@"
