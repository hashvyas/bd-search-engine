#!/usr/bin/env bash
# Read-only health check for one role. Prints PASS/FAIL for each check. Changes nothing.
# Usage: bash scripts/doctor.sh <ingest|storage|compute|serve>
set -euo pipefail
. "$(dirname "$0")/lib.sh"
ROLE="${1:?usage: doctor.sh <ingest|storage|compute|serve>}"
N="$(role_node "$ROLE")"
FAILS=0
pass() { printf 'PASS  %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; FAILS=$((FAILS + 1)); }
check() { local name="$1"; shift; if out="$("$@" 2>&1)"; then pass "$name${out:+: $out}"; else fail "$name${out:+: $out}"; fi; }

if [ -f "$REPO_ROOT/config/cluster.env" ]; then pass "config/cluster.env present"; else fail "config/cluster.env missing (cp config/cluster.env.example config/cluster.env)"; fi
LOCAL_MODE=1 load_env   # load_env without dying on a missing cluster.env, then add cluster values if present
set -a
# shellcheck disable=SC1091
[ -f "$REPO_ROOT/config/cluster.env" ] && . "$REPO_ROOT/config/cluster.env"
set +a

java_ok() { local v; v="$("${JAVA_HOME:-/usr}/bin/java" -version 2>&1 | awk -F'"' '/version/{print $2; exit}')"; echo "$v"; [[ "$v" == "$JAVA_MAJOR".* ]]; }
hadoop_ok() { local v; v="$("$HADOOP_HOME/bin/hadoop" version 2>/dev/null | awk 'NR==1{print $2}')"; echo "${v:-not found}"; [ "$v" = "$HADOOP_VERSION" ]; }
spark_ok() { local v; v="$(ls "$SPARK_HOME"/jars/spark-core_*.jar 2>/dev/null | sed -E 's/.*-([0-9.]+)\.jar/\1/')"; echo "${v:-not found}"; [ "$v" = "$SPARK_VERSION" ]; }
iceberg_ok() { local f; f="$(cd "$SPARK_HOME/jars" 2>/dev/null && ls iceberg-spark-runtime-*.jar 2>/dev/null | tr '\n' ' ')"; echo "${f:-none}"; [ "$f" = "$ICEBERG_JAR " ]; }
py_imports() {  # <python> <modules...>
  local py="$1"; shift
  PYTHONPATH="$SPARK_HOME/python:$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip 2>/dev/null | head -1)" \
    "$py" -c "import sys; [__import__(m) for m in sys.argv[1:]]; print('%s %d.%d' % (sys.executable, *sys.version_info[:2]))" "$@"
}
zt_ok() {
  command -v zerotier-cli >/dev/null || { echo "zerotier-cli not installed"; return 1; }
  local line; line="$(sudo -n zerotier-cli listnetworks 2>/dev/null | grep "${ZT_NETWORK_ID:-none}" || zerotier-cli listnetworks 2>/dev/null | grep "${ZT_NETWORK_ID:-none}" || true)"
  echo "${line:-network ${ZT_NETWORK_ID:-?} not joined (or needs sudo)}"; [[ "$line" == *" OK "* ]]
}
resolves() { local ip; ip="$(getent hosts "$1" | awk '{print $1; exit}')"; echo "$1 -> ${ip:-unresolved}"; [ -n "$ip" ] && [[ "$ip" != 127.* ]]; }
port_open() { timeout 3 bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null; }
hdfs_report() { local live; live="$(timeout 30 "$HADOOP_HOME/bin/hdfs" dfsadmin -report 2>/dev/null | awk -F'[()]' '/Live datanodes/{print $2}')"; echo "live datanodes: ${live:-?}"; [ "${live:-0}" -ge 2 ]; }
spark_ui() { local w; w="$(curl -sf --max-time 5 "http://node3:${SPARK_MASTER_WEBUI_PORT}/json/" | grep -o '"aliveworkers" *: *[0-9]*' | grep -o '[0-9]*$')"; echo "alive workers: ${w:-unreachable}"; [ "${w:-0}" -ge 2 ]; }
free_mem() { local m; m="$(awk '/MemAvailable/{printf "%d", $2/1024}' /proc/meminfo)"; echo "${m} MiB available"; [ "$m" -ge 1536 ]; }

echo "== doctor: role $ROLE (node$N) on $(hostname)"
SPARK_PY_CHECK="${SPARK_PY:-$BDSE_VENV/bin/python}"
if [ "$ROLE" != serve ]; then
  check "java $JAVA_MAJOR" java_ok
  check "hadoop $HADOOP_VERSION" hadoop_ok
  check "spark $SPARK_VERSION" spark_ok
  check "iceberg runtime $ICEBERG_JAR" iceberg_ok
  check "python imports pyspark+py4j" py_imports "$SPARK_PY_CHECK" pyspark py4j
  check "rendered $CONF_DIR/iceberg.conf" test -f "$CONF_DIR/iceberg.conf"
fi
if [ "$ROLE" = serve ]; then check "python imports fastapi uvicorn pyarrow" py_imports "$SERVE_PY" fastapi uvicorn pyarrow; fi
check "zerotier network" zt_ok
for i in 1 2 3 4; do check "hostname node$i" resolves "node$i"; done

case "$ROLE" in
  ingest)  targets="node2:${HDFS_NAMENODE_PORT} node2:${HDFS_DATANODE_PORT:-9866} node3:${HDFS_DATANODE_PORT:-9866} node3:${SPARK_MASTER_PORT}" ;;
  storage) targets="node3:${SPARK_MASTER_PORT} node3:${HDFS_DATANODE_PORT:-9866}" ;;
  compute) targets="node2:${HDFS_NAMENODE_PORT} node2:${HDFS_DATANODE_PORT:-9866} node2:${SPARK_WORKER_PORT:-7078}" ;;
  serve)   targets="node3:22" ;;
esac
for t in $targets; do check "port $t reachable" port_open "${t%%:*}" "${t##*:}"; done

if [ "$ROLE" != serve ]; then
  check "HDFS report (2 DataNodes)" hdfs_report
  check "Spark master UI (2 workers)" spark_ui
fi
if [ "$ROLE" = serve ]; then
  check "search API /health" curl -sf --max-time 3 "http://127.0.0.1:$SERVE_PORT/health"
fi
check "free memory >= 1.5 GiB" free_mem

echo "== $FAILS check(s) failed"
[ "$FAILS" -eq 0 ]
