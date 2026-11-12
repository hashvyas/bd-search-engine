#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"
ROLE="${1:?usage: 05_start.sh <ingest|storage|compute|serve>}"
role_node "$ROLE" >/dev/null
load_env
export HADOOP_PID_DIR="$PID_DIR" HADOOP_LOG_DIR="$LOG_DIR/hadoop" SPARK_PID_DIR="$PID_DIR" SPARK_LOG_DIR="$LOG_DIR/spark"
MASTER="spark://node3:$SPARK_MASTER_PORT"

start_hdfs() {
  if pid_alive "$PID_DIR/hadoop-$USER-$1.pid"; then log "$1 already running"; else hdfs --daemon start "$1"; log "started $1"; fi
}
start_master() {
  if pid_alive "$PID_DIR/spark-$USER-org.apache.spark.deploy.master.Master-1.pid"; then log "spark master already running"
  else "$SPARK_HOME/sbin/start-master.sh"; fi
}
start_worker() {
  if pid_alive "$PID_DIR/spark-$USER-org.apache.spark.deploy.worker.Worker-1.pid"; then log "spark worker already running"
  else "$SPARK_HOME/sbin/start-worker.sh" "$MASTER"; fi
}
format_namenode_once() {
  if [ -f "$BDSE_HOME/hdfs/name/current/VERSION" ]; then log "NameNode already formatted"
  else hdfs namenode -format -nonInteractive -clusterId bdse-cluster; fi
}

case "$ROLE" in
  storage) format_namenode_once; start_hdfs namenode; start_hdfs datanode; start_worker ;;
  compute) start_master; start_hdfs datanode; start_worker ;;
  serve)   serve_start ;;
  ingest)  log "node1 is a client only: no daemons to start" ;;
esac
log "done. Check with: bash scripts/doctor.sh $ROLE"
