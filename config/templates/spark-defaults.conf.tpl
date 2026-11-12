spark.master                         spark://node3:${SPARK_MASTER_PORT}

# Drivers run on node1 (ingest) and node3 (compute). Executors must reach them over ZeroTier.
spark.driver.host                    ${NODE_IP}
spark.driver.bindAddress             0.0.0.0
spark.driver.port                    ${SPARK_DRIVER_PORT}
spark.driver.blockManager.port       ${SPARK_DRIVER_BLOCKMANAGER_PORT}
spark.blockManager.port              ${SPARK_BLOCKMANAGER_PORT}
spark.port.maxRetries                16

spark.driver.memory                  ${SPARK_DRIVER_MEMORY}
spark.executor.memory                ${SPARK_EXECUTOR_MEMORY}
spark.executor.cores                 ${SPARK_EXECUTOR_CORES}
spark.sql.shuffle.partitions         ${SPARK_SHUFFLE_PARTITIONS}
spark.pyspark.python                 ${BDSE_VENV}/bin/python

# --- Fault tolerance (set on purpose, see docs/FAULT_TOLERANCE.md) ---
# A task may fail up to 8 times (default 4) before the job fails. Killing a worker fails every task it ran.
spark.task.maxFailures               8
# Fetch failures after an executor is lost re-run the parent stage. Allow more stage attempts.
spark.stage.maxConsecutiveAttempts   8
# Retry shuffle fetches before declaring a fetch failure (rides out short network drops on ZeroTier).
spark.shuffle.io.maxRetries          10
spark.shuffle.io.retryWait           5s
# Notice a dead executor in about 60 s instead of 120 s.
spark.network.timeout                60s
spark.executor.heartbeatInterval     10s
# Do not blacklist a node because one executor died: the restarted worker must get work again.
spark.excludeOnFailure.enabled       false
spark.speculation                    false
spark.cleaner.referenceTracking.cleanCheckpoints true
spark.eventLog.enabled               false
