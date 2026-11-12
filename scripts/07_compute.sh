#!/usr/bin/env bash
# Node 3: build/update the inverted index, compute PageRank, export Parquet for serving.
# Usage: bash scripts/07_compute.sh <0|1>
set -euo pipefail
. "$(dirname "$0")/lib.sh"
PHASE="${1:?usage: 07_compute.sh <0|1>}"
load_env
ip -4 addr 2>/dev/null | grep -q "inet ${NODE3_IP:-x}/" || log "WARNING: this host does not have NODE3_IP; drivers should run on node3"
do_compute "$PHASE"
log "export ready in $EXPORT_DIR/$(cat "$EXPORT_DIR/LATEST"). Next on node4: bash scripts/08_pull_and_serve.sh"
