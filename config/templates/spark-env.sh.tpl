export JAVA_HOME=${JAVA_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
# Spark standalone advertises the address it binds to, so it binds to the ZeroTier IP rather than 0.0.0.0.
export SPARK_LOCAL_IP=${NODE_IP}
export SPARK_MASTER_HOST=node3
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT}
export SPARK_WORKER_PORT=${SPARK_WORKER_PORT}
export SPARK_WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
export SPARK_WORKER_DIR=${BDSE_HOME}/spark-work
export SPARK_PID_DIR=${BDSE_HOME}/pids
export SPARK_LOG_DIR=${BDSE_HOME}/logs/spark
# Master declares a silent worker dead after 30 s (default 60) so the fault demo recovers faster.
export SPARK_MASTER_OPTS="-Dspark.worker.timeout=30"
export PYSPARK_PYTHON=${BDSE_VENV}/bin/python
export PYSPARK_DRIVER_PYTHON=${BDSE_VENV}/bin/python
