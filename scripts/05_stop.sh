#!/usr/bin/env bash
# Stop this role's daemons through their pid files. Safe to re-run.
# Usage: bash scripts/05_stop.sh <ingest|storage|compute|serve>
set -euo pipefail
. "$(dirname "$0")/lib.sh"
ROLE="${1:?usage: 05_stop.sh <ingest|storage|compute|serve>}"
role_node "$ROLE" >/dev/null
load_env
export HADOOP_PID_DIR="$PID_DIR" HADOOP_LOG_DIR="$LOG_DIR/hadoop" SPARK_PID_DIR="$PID_DIR" SPARK_LOG_DIR="$LOG_DIR/spark"

stop_hdfs() { if pid_alive "$PID_DIR/hadoop-$USER-$1.pid"; then hdfs --daemon stop "$1"; else log "$1 not running"; fi; }

case "$ROLE" in
  storage) "$SPARK_HOME/sbin/stop-worker.sh" || true; stop_hdfs datanode; stop_hdfs namenode ;;
  compute) "$SPARK_HOME/sbin/stop-worker.sh" || true; stop_hdfs datanode; "$SPARK_HOME/sbin/stop-master.sh" || true ;;
  serve)   serve_stop ;;
  ingest)  log "nothing to stop on node1" ;;
esac
