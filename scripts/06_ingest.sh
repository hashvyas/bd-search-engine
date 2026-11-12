#!/usr/bin/env bash
# Node 1: upload a phase file to HDFS and run the ingestion job on the Spark master on node3.
# Usage: bash scripts/06_ingest.sh <0|1> [file]   (default file: PHASE0_FILE / PHASE1_FILE from cluster.env)
set -euo pipefail
. "$(dirname "$0")/lib.sh"
PHASE="${1:?usage: 06_ingest.sh <0|1> [file]}"
load_env
case "$PHASE" in 0) default="$PHASE0_FILE" ;; 1) default="$PHASE1_FILE" ;; *) default="" ;; esac
FILE="${2:-$default}"
[ -n "$FILE" ] || die "no input file for phase $PHASE"
do_ingest "$PHASE" "$FILE"
log "phase $PHASE ingested. Next on node3: bash scripts/07_compute.sh $PHASE"
