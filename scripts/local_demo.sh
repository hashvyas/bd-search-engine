#!/usr/bin/env bash
set -euo pipefail
export LOCAL_MODE=1
export BDSE_HOME="${DEMO_HOME:-$HOME/bigdata/tmp/demo}"
export SERVE_PORT="${SERVE_PORT:-18000}"
if [ -z "${SPARK_PY:-}" ] && [ ! -x "${BDSE_VENV:-$HOME/.venvs/bdse}/bin/python" ] && command -v python3.10 >/dev/null; then
  export SPARK_PY="$(command -v python3.10)"
fi
. "$(dirname "$0")/lib.sh"
load_env
QUERIES=("computer science" "april month" "chemistry" "phase added page" "beard")

[ -x "$SPARK_HOME/bin/spark-submit" ] || die "spark-submit not found under SPARK_HOME=$SPARK_HOME"
ls "$SPARK_HOME"/jars/iceberg-spark-runtime-*.jar >/dev/null 2>&1 || die "no Iceberg runtime jar in $SPARK_HOME/jars"
"$SERVE_PY" -c "import fastapi, uvicorn, pyarrow" 2>/dev/null \
  || die "SERVE_PY=$SERVE_PY lacks fastapi/uvicorn/pyarrow (set SERVE_PY to a python that has them)"
command -v rsync >/dev/null || die "rsync missing"
log "demo home: $BDSE_HOME | spark python: $SPARK_PY | serve python: $SERVE_PY"

serve_stop 2>/dev/null || true
case "$BDSE_HOME" in "$HOME"/bigdata/tmp/*) rm -rf "$BDSE_HOME" ;; *) die "refusing to wipe $BDSE_HOME (must be under ~/bigdata/tmp)" ;; esac
ensure_dirs
render "$REPO_ROOT/config/templates/iceberg.conf.tpl" "$CONF_DIR/iceberg.conf"
export SPARK_CONF_DIR="$CONF_DIR/spark"
mkdir -p "$SPARK_CONF_DIR"
sed 's/^rootLogger.level = info/rootLogger.level = warn/' "$SPARK_HOME/conf/log4j2.properties.template" \
  > "$SPARK_CONF_DIR/log4j2.properties"
trap serve_stop EXIT

search() {
  curl -sS -G "http://127.0.0.1:$SERVE_PORT/search" --data-urlencode "q=$2" -d k=5 \
    | "$SERVE_PY" -c "import json,sys; d=json.load(sys.stdin); print('  [%s] %r -> %d hits (version %s)' % (sys.argv[1], d['query'], d['count'], d['version'])); [print('     %d. %-16s %-45s score=%.4f bm25=%.3f pr=%.5f' % (i+1, r['doc_id'], r['url'][:45], r['score'], r['bm25'], r['pagerank'])) for i, r in enumerate(d['results'])]" "$1"
}
health() { curl -sS "http://127.0.0.1:$SERVE_PORT/health" | "$SERVE_PY" -c "import json,sys; print(json.load(sys.stdin).get('version'))"; }

log "===== PHASE 0: ingest -> compute -> serve"
do_ingest 0 "$PHASE0_FILE"
do_compute 0
serve_start
do_pull
V0="$(health)"
for q in "${QUERIES[@]}"; do search "phase0" "$q"; done

log "===== PHASE 1: ingest -> compute -> serve"
do_ingest 1 "$PHASE1_FILE"
do_compute 1
do_pull
V1="$(health)"
[ "$V1" != "$V0" ] || die "version did not change after phase 1"
log "BEFORE ($V0) vs AFTER ($V1):"
"$SERVE_PY" - "$SERVING_DIR/$V0" "$SERVING_DIR/$V1" <<'EOF'
import sys, pyarrow.parquet as pq
a, b = (set(pq.read_table(p + "/docs.parquet").column("doc_id").to_pylist()) for p in sys.argv[1:3])
print(f"  docs: {len(a)} -> {len(b)}  removed={len(a - b)} added={len(b - a)} e.g. added {sorted(b - a)[:3]}")
EOF
for q in "${QUERIES[@]}"; do search "phase1" "$q"; done

log "===== IDEMPOTENCE: rerun Phase 0 on top of the current tables"
spark_submit src/ingest/admin.py state 2>/dev/null | grep '^STATE' | tee "$LOG_DIR/state-before.txt"
do_ingest 0 "$PHASE0_FILE" > /dev/null
spark_submit src/ingest/admin.py state 2>/dev/null | grep '^STATE' | tee "$LOG_DIR/state-after.txt"
grep -h '"status"' "$LOG_DIR/ingest-phase0.log" | tail -1 | sed 's/^/  /'
if cmp -s "$LOG_DIR/state-before.txt" "$LOG_DIR/state-after.txt"; then log "PASS: rerunning Phase 0 left snapshots, row counts and content hashes unchanged"
else die "FAIL: tables changed on rerun"; fi

log "===== BAD EXPORT: the old version must stay live"
do_pull "$V0" >/dev/null
log "live version now: $(health)"
BAD="${V1}-corrupt"
cp -r "$EXPORT_DIR/$V1" "$EXPORT_DIR/$BAD"
truncate -s 100 "$EXPORT_DIR/$BAD/index.parquet"
if (do_pull "$BAD") 2>&1 | tail -1; then die "FAIL: corrupt export was accepted by the pull step"; fi
cp -r "$EXPORT_DIR/$BAD" "$SERVING_DIR/$BAD"
code="$(curl -s -o "$LOG_DIR/bad-reload.json" -w '%{http_code}' -X POST "http://127.0.0.1:$SERVE_PORT/reload?version=$BAD")"
log "server answer to corrupt reload: HTTP $code $(cat "$LOG_DIR/bad-reload.json")"
[ "$code" = 409 ] && [ "$(health)" = "$V0" ] || die "FAIL: corrupt export changed the live version"
log "PASS: live version is still $(health) after a corrupt export (pull step and server both refused it)"
search "still-live" "computer science"

log "===== LOCAL DEMO COMPLETE (phase0=$V0, phase1=$V1)"
