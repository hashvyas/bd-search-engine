# Spark and HDFS fault tolerance

`scripts/fault_demo.sh` runs on **node3** after Phase 0 has been ingested. **It has not been run yet**: it needs the real 4-node cluster.

## What makes the cluster tolerate a failure
| Mechanism | Where | Effect |
|---|---|---|
| RDD/DataFrame lineage | Spark | Partitions lost with an executor are recomputed from their inputs |
| Reliable checkpoint every 3 PageRank iterations (`PAGERANK_CHECKPOINT_EVERY`) to `hdfs://node2:9000/bdse/checkpoints` | `pagerank.py` | Recompute starts from the last checkpoint, not from iteration 0. The checkpoint itself is replicated on HDFS |
| `spark.task.maxFailures=8` | spark-defaults | Each task survives up to 7 failed attempts (default 3). Killing a worker fails every task running on it |
| `spark.stage.maxConsecutiveAttempts=8` | spark-defaults | Lost shuffle output (fetch failure) resubmits the parent stage, up to 8 times |
| `spark.shuffle.io.maxRetries=10`, `retryWait=5s` | spark-defaults | Rides out short ZeroTier drops before declaring a fetch failure |
| `spark.network.timeout=60s`, `spark.executor.heartbeatInterval=10s`, master `spark.worker.timeout=30` | spark-defaults / spark-env | A dead executor or worker is noticed in under about a minute |
| `spark.excludeOnFailure.enabled=false` | spark-defaults | The restarted worker gets tasks again |
| `spark.speculation=false` | spark-defaults | Retries are caused only by real failures, so they are easy to see in the demo |
| `dfs.replication=2` over 2 DataNodes | hdfs-site | Every block exists on node2 and node3. One DataNode can be lost |
| `dfs.namenode.heartbeat.recheck-interval=20000` | hdfs-site | A dead DataNode is declared after about 70 s (default 10.5 min). Reads switch to the replica immediately anyway |
| `replace-datanode-on-failure.policy=NEVER` | hdfs-site | With only 2 DataNodes, writes continue on the survivor instead of failing |

Only the driver and the NameNode are single points of failure. That is a known limit of this layout (no HA).

## Commands
```bash
# node3, cluster up, Phase 0 ingested
bash scripts/fault_demo.sh          # kills node3's own worker and DataNode (no ssh needed)
bash scripts/fault_demo.sh node2    # kills node2's worker over ssh instead
```
Watch it live at http://node3:8080 (the worker goes DEAD, then back) and http://node2:9870 → Datanodes.

## Expected output (shape)
```
== 1. baseline PageRank (no failure)
baseline digest: 3f1c...e9
== 2. PageRank with a worker killed mid-job (target: node3)
killing Spark Worker and executors on node3 (kill -9)
... WARN TaskSchedulerImpl: Lost executor 1 on <node3-ip>: ...
... WARN TaskSetManager: Lost task 3.0 in stage 41.0 ... ExecutorLostFailure ...
PASS: same PageRank digest after losing a worker (3f1c...e9)
restarting the worker on node3
== 3. HDFS read through the replica with node3's DataNode stopped
/bdse/fault-demo/sample.bin 8388608 bytes, replicated: replication=2, 1 block(s) ...
node3 DataNode stopped; reading through the replica on node2
PASS: file read intact with one DataNode down
COUNT iceberg_catalog.search.documents = 1000
PASS: Iceberg table readable with one DataNode down
node3 DataNode restarted
FAULT DEMO: ALL PASS
```
How to read it:
- PageRank prints one line per iteration, and `(checkpointed)` every third. After the kill, the driver logs lost executors and resubmitted tasks. The remaining executor on node2 finishes the job.
- `PAGERANK_DIGEST` is a sha256 over the sorted `doc_id:pagerank` values (10 decimals). The same digest proves the result is identical.
- The HDFS step writes an 8 MB file with replication 2, shows the block locations (both DataNodes), stops node3's DataNode, and reads the file back (same sha256) plus the Iceberg `documents` table.

## Manual variant (for the viva)
```bash
# terminal A (node3): a slow PageRank
. scripts/lib.sh && load_env && PAGERANK_ITER_SLEEP=5 spark_submit src/compute/pagerank.py --no-write
# terminal B (node2), during iteration 3-5:
pkill -9 -f org.apache.spark.deploy.worker.Worker; pkill -9 -f CoarseGrainedExecutorBackend
# afterwards on node2:
bash scripts/05_start.sh storage
```
