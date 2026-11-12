#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"
load_env
TARGET="${1:-node3}"
export HADOOP_PID_DIR="$PID_DIR" HADOOP_LOG_DIR="$LOG_DIR/hadoop" SPARK_PID_DIR="$PID_DIR" SPARK_LOG_DIR="$LOG_DIR/spark"
ensure_dirs
RESULT=0
digest_of() { grep -o 'PAGERANK_DIGEST=[0-9a-f]*' "$1" | cut -d= -f2; }
on_target() { if [ "$TARGET" = node3 ]; then bash -c "$1"; else ssh -o BatchMode=yes "$NODE3_USER@$TARGET" "$1"; fi; }

log "== 1. baseline PageRank (no failure)"
spark_submit src/compute/pagerank.py --no-write > "$LOG_DIR/fault-baseline.log" 2>&1
BASE="$(digest_of "$LOG_DIR/fault-baseline.log")"
log "baseline digest: $BASE"

log "== 2. PageRank with a worker killed mid-job (target: $TARGET)"
PAGERANK_ITER_SLEEP=4 spark_submit src/compute/pagerank.py --no-write > "$LOG_DIR/fault-killed.log" 2>&1 &
JOB=$!
until grep -q 'iteration 3/' "$LOG_DIR/fault-killed.log" 2>/dev/null; do
  kill -0 "$JOB" 2>/dev/null || { log "job ended before the kill"; break; }
  sleep 1
done
log "killing Spark Worker and executors on $TARGET (kill -9)"
on_target "pkill -9 -f org.apache.spark.deploy.worker.Worker; pkill -9 -f CoarseGrainedExecutorBackend" || true
wait "$JOB" || { log "FAIL: job did not survive the worker loss"; RESULT=1; }
AFTER="$(digest_of "$LOG_DIR/fault-killed.log")"
grep -E 'Lost executor|ExecutorLost|Removing executor|Resubmitt' "$LOG_DIR/fault-killed.log" | head -5 || true
if [ -n "$AFTER" ] && [ "$AFTER" = "$BASE" ]; then log "PASS: same PageRank digest after losing a worker ($AFTER)"
else log "FAIL: digest $AFTER differs from baseline $BASE"; RESULT=1; fi
log "restarting the worker on $TARGET"
on_target "cd '$REPO_ROOT' && rm -f '$PID_DIR'/spark-*Worker-1.pid && bash scripts/05_start.sh $([ "$TARGET" = node3 ] && echo compute || echo storage)" || true

log "== 3. HDFS read through the replica with node3's DataNode stopped"
TESTF="$DATA_ROOT/fault-demo/sample.bin"
head -c 8388608 /dev/urandom > "$LOG_DIR/sample.bin"
SUM="$(sha256sum < "$LOG_DIR/sample.bin" | cut -d' ' -f1)"
hdfs dfs -mkdir -p "$(dirname "$TESTF")"
hdfs dfs -put -f "$LOG_DIR/sample.bin" "$TESTF"
hdfs fsck "${TESTF#hdfs://node2:$HDFS_NAMENODE_PORT}" -files -blocks -locations | grep -E 'repl=|Live_repl|DatanodeInfoWithStorage' | head -5
hdfs --daemon stop datanode
log "node3 DataNode stopped; reading through the replica on node2"
if [ "$(hdfs dfs -cat "$TESTF" | sha256sum | cut -d' ' -f1)" = "$SUM" ]; then log "PASS: file read intact with one DataNode down"
else log "FAIL: file read failed or differs"; RESULT=1; fi
if spark_submit src/ingest/admin.py count documents 2>&1 | grep 'COUNT'; then log "PASS: Iceberg table readable with one DataNode down"
else log "FAIL: Iceberg read failed"; RESULT=1; fi
hdfs --daemon start datanode
log "node3 DataNode restarted"
hdfs dfs -rm -r -f -skipTrash "$(dirname "$TESTF")" >/dev/null

[ $RESULT -eq 0 ] && log "FAULT DEMO: ALL PASS" || log "FAULT DEMO: SOME CHECKS FAILED"
exit $RESULT
