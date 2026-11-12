#!/usr/bin/env bash
# Shared helpers. Source this file; do not run it.
# The pipeline steps (do_ingest, do_compute, do_pull) live here so that the cluster
# scripts 06-08 and local_demo.sh run the same code.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

role_node() {
  case "$1" in
    ingest) echo 1 ;; storage) echo 2 ;; compute) echo 3 ;; serve) echo 4 ;;
    *) die "unknown role '$1' (use ingest|storage|compute|serve)" ;;
  esac
}

load_env() {
  set -a
  # shellcheck disable=SC1091
  . "$REPO_ROOT/config/versions.env"
  # shellcheck disable=SC1091
  [ -f "$HOME/.bigdata_env" ] && . "$HOME/.bigdata_env"
  if [ "${LOCAL_MODE:-0}" != 1 ]; then
    [ -f "$REPO_ROOT/config/cluster.env" ] \
      || die "config/cluster.env missing: cp config/cluster.env.example config/cluster.env and fill it in"
    # shellcheck disable=SC1091
    . "$REPO_ROOT/config/cluster.env"
  fi
  : "${BDSE_HOME:=$HOME/bigdata}"
  : "${BDSE_VENV:=$HOME/.venvs/bdse}"
  : "${CONF_DIR:=$BDSE_HOME/conf}"
  : "${HADOOP_CONF_DIR:=$CONF_DIR/hadoop}"
  : "${SPARK_CONF_DIR:=$CONF_DIR/spark}"
  : "${PID_DIR:=$BDSE_HOME/pids}"
  : "${LOG_DIR:=$BDSE_HOME/logs}"
  : "${EXPORT_DIR:=$BDSE_HOME/exports}"
  : "${SERVING_DIR:=$BDSE_HOME/serving}"
  : "${SERVE_PORT:=8000}"
  : "${HDFS_NAMENODE_PORT:=9000}"
  : "${SPARK_MASTER_PORT:=7077}"
  : "${SPARK_MASTER_WEBUI_PORT:=8080}"
  : "${PHASE0_FILE:=data/toy/pages.jsonl}"
  : "${PHASE1_FILE:=data/toy/changes.json}"
  : "${SPARK_HOME:=/opt/spark}"
  : "${HADOOP_HOME:=/opt/hadoop}"
  if [ "${LOCAL_MODE:-0}" = 1 ]; then
    : "${DATA_ROOT:=file://$BDSE_HOME/dfs}"
    : "${WAREHOUSE:=file://$BDSE_HOME/warehouse}"
    : "${SPARK_MASTER_URL:=local[*]}"
  else
    : "${DATA_ROOT:=hdfs://node2:$HDFS_NAMENODE_PORT/bdse}"
    : "${WAREHOUSE:=hdfs://node2:$HDFS_NAMENODE_PORT/warehouse}"
    : "${SPARK_MASTER_URL:=spark://node3:$SPARK_MASTER_PORT}"
  fi
  # Spark needs the Python 3.10 env. The server only needs fastapi, uvicorn and pyarrow.
  if [ -z "${SPARK_PY:-}" ]; then
    if [ -x "$BDSE_VENV/bin/python" ]; then SPARK_PY="$BDSE_VENV/bin/python"; else SPARK_PY=python3; fi
  fi
  if [ -z "${SERVE_PY:-}" ]; then
    if [ -x "$BDSE_VENV/bin/python" ]; then SERVE_PY="$BDSE_VENV/bin/python"; else SERVE_PY=python3; fi
  fi
  PATH="$SPARK_HOME/bin:$HADOOP_HOME/bin:$PATH"
  set +a
}

ensure_dirs() { mkdir -p "$PID_DIR" "$LOG_DIR" "$CONF_DIR" "$SERVING_DIR"; }

# render <template> <output>: substitute only the ${VARS} that appear in the template, and fail if one is unset.
render() {
  local tpl="$1" out="$2" vars v
  vars="$(grep -o '\${[A-Za-z_][A-Za-z0-9_]*}' "$tpl" | sort -u | tr -d '${}' || true)"
  for v in $vars; do
    [ -n "${!v:-}" ] || die "variable $v (used by $(basename "$tpl")) is not set; check config/cluster.env"
    export "${v?}"
  done
  mkdir -p "$(dirname "$out")"
  envsubst "$(printf '${%s} ' $vars)" < "$tpl" > "$out"
  log "rendered $out"
}

pid_alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

valid_version() { [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]]; }

spark_submit() {
  local job="$1"; shift
  local props="$CONF_DIR/job.properties"
  ensure_dirs
  [ -f "$CONF_DIR/iceberg.conf" ] || die "$CONF_DIR/iceberg.conf missing (run 04_configure.sh)"
  # --properties-file replaces spark-defaults.conf, so merge the two into one file.
  if [ "${LOCAL_MODE:-0}" = 1 ]; then
    { cat "$CONF_DIR/iceberg.conf"; echo "spark.sql.shuffle.partitions=4"; echo "spark.ui.enabled=false"; } > "$props"
  else
    cat "$SPARK_CONF_DIR/spark-defaults.conf" "$CONF_DIR/iceberg.conf" > "$props"
  fi
  log "spark-submit $job $* (master $SPARK_MASTER_URL)"
  PYSPARK_PYTHON="$SPARK_PY" PYSPARK_DRIVER_PYTHON="$SPARK_PY" \
    "$SPARK_HOME/bin/spark-submit" --master "$SPARK_MASTER_URL" --properties-file "$props" \
    "$REPO_ROOT/$job" "$@"
}

do_ingest() {
  local phase="$1" file="$2"
  [[ "$phase" =~ ^[0-9]+$ ]] || die "phase must be a number"
  ensure_dirs
  case "$file" in /*) ;; *) file="$REPO_ROOT/$file" ;; esac
  [ -f "$file" ] || die "input file not found: $file"
  local hash name dest
  hash="$(sha256sum "$file" | cut -d' ' -f1)"
  name="$(basename "$file")"
  dest="$DATA_ROOT/raw/phase=$phase/$name"
  log "phase $phase: uploading $name (sha256 ${hash:0:12}...) to $dest"
  if [ "${LOCAL_MODE:-0}" = 1 ]; then
    mkdir -p "$(dirname "${dest#file://}")" && cp "$file" "${dest#file://}"
  else
    hdfs dfsadmin -safemode wait >/dev/null
    hdfs dfs -mkdir -p "$(dirname "$dest")"
    hdfs dfs -put -f "$file" "$dest"
  fi
  spark_submit src/ingest/ingest_job.py --phase "$phase" --input "$dest" --file-hash "$hash" --file-name "$name" \
    2>&1 | tee "$LOG_DIR/ingest-phase$phase.log"
}

do_compute() {
  local phase="$1"
  [[ "$phase" =~ ^[0-9]+$ ]] || die "phase must be a number"
  ensure_dirs
  spark_submit src/compute/build_index.py 2>&1 | tee "$LOG_DIR/index-phase$phase.log"
  spark_submit src/compute/pagerank.py 2>&1 | tee "$LOG_DIR/pagerank-phase$phase.log"
  spark_submit src/compute/export.py --phase "$phase" 2>&1 | tee "$LOG_DIR/export-phase$phase.log"
  grep -q '^EXPORT_VERSION=' "$LOG_DIR/export-phase$phase.log" || die "export did not report a version"
}

# do_pull [version]: copy an export into SERVING_DIR, verify it, then ask the server to switch to it.
do_pull() {
  local version="${1:-}" src
  ensure_dirs
  if [ "${LOCAL_MODE:-0}" = 1 ]; then
    [ -n "$version" ] || version="$(cat "$EXPORT_DIR/LATEST")"
    src="$EXPORT_DIR/$version/"
  else
    [ -n "${NODE3_USER:-}" ] || die "set NODE3_USER in config/cluster.env (run 'whoami' on node3)"
    : "${NODE3_EXPORT_DIR:=/home/$NODE3_USER/bigdata/exports}"
    local remote="${NODE3_USER}@node3"
    [ -n "$version" ] || version="$(ssh -o BatchMode=yes "$remote" cat "$NODE3_EXPORT_DIR/LATEST")"
    src="$remote:$NODE3_EXPORT_DIR/$version/"
  fi
  valid_version "$version" || die "bad version name '$version'"
  local staging="$SERVING_DIR/.staging/$version"
  rm -rf "$staging" && mkdir -p "$staging"
  log "pulling $src -> $staging"
  rsync -a --delete "$src" "$staging/"
  if ! (cd "$REPO_ROOT" && "$SERVE_PY" -m src.serve.loader verify "$staging"); then
    rm -rf "$staging"
    die "export $version failed verification; the live version is unchanged"
  fi
  if [ -d "$SERVING_DIR/$version" ]; then
    rm -rf "$staging"   # already pulled earlier. The server verifies it again on reload.
  else
    mv "$staging" "$SERVING_DIR/$version"
  fi
  serve_reload "$version"
}

serve_reload() {
  local out
  if ! out="$(curl -sS -f -X POST -H "X-Reload-Token: ${RELOAD_TOKEN:-}" \
      "http://127.0.0.1:$SERVE_PORT/reload?version=$1" 2>&1)"; then
    log "reload response: $out"
    die "server refused version $1; the live version is unchanged"
  fi
  log "reload: $out"
}

serve_start() {
  local pidf="$PID_DIR/search-api.pid"
  ensure_dirs
  if pid_alive "$pidf"; then log "search API already running (pid $(cat "$pidf"))"; return; fi
  if curl -sf "http://127.0.0.1:$SERVE_PORT/health" >/dev/null 2>&1; then
    die "port $SERVE_PORT already answers but $pidf has no live pid: stop that process first"
  fi
  log "starting search API on 0.0.0.0:$SERVE_PORT (python: $SERVE_PY)"
  # cd first, then background only the server, so $! is the python process (nohup execs it).
  (cd "$REPO_ROOT" || exit 1
   SERVING_DIR="$SERVING_DIR" nohup "$SERVE_PY" -m uvicorn src.serve.app:app \
     --host 0.0.0.0 --port "$SERVE_PORT" > "$LOG_DIR/search-api.log" 2>&1 &
   echo $! > "$pidf")
  local i
  for i in $(seq 1 30); do
    curl -sf "http://127.0.0.1:$SERVE_PORT/health" >/dev/null 2>&1 && { log "search API is up"; return; }
    sleep 1
  done
  die "search API did not start; see $LOG_DIR/search-api.log"
}

serve_stop() {
  local pidf="$PID_DIR/search-api.pid"
  if pid_alive "$pidf"; then
    local pid i; pid="$(cat "$pidf")"
    kill "$pid"
    for i in $(seq 1 10); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid"
    log "stopped search API"
  fi
  rm -f "$pidf"
}
