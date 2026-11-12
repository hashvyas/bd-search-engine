spark.master                         spark://node3:${SPARK_MASTER_PORT}

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
spark.sql.session.timeZone           UTC
spark.pyspark.python                 ${BDSE_VENV}/bin/python

spark.task.maxFailures               8
spark.stage.maxConsecutiveAttempts   8
spark.shuffle.io.maxRetries          10
spark.shuffle.io.retryWait           5s
spark.network.timeout                60s
spark.executor.heartbeatInterval     10s
spark.excludeOnFailure.enabled       false
spark.speculation                    false
spark.cleaner.referenceTracking.cleanCheckpoints true
spark.eventLog.enabled               false
