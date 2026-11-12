#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"
PHASE="${1:?usage: 06_ingest.sh <0|1> [file]}"
load_env
case "$PHASE" in 0) default="$PHASE0_FILE" ;; 1) default="$PHASE1_FILE" ;; *) default="" ;; esac
FILE="${2:-$default}"
[ -n "$FILE" ] || die "no input file for phase $PHASE"
do_ingest "$PHASE" "$FILE"
log "phase $PHASE ingested. Next on node3: bash scripts/07_compute.sh $PHASE"
