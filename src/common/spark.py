"""SparkSession factory. Master, Iceberg catalog and network settings come from
spark-submit (--master, --properties-file), not from code."""
from pyspark.sql import SparkSession

from src.common.config import DATA_ROOT


def get_spark(app):
    spark = SparkSession.builder.appName(app).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Reliable checkpoints go to HDFS on the cluster, so a lost executor does not lose them.
    spark.sparkContext.setCheckpointDir(f"{DATA_ROOT}/checkpoints/{app}")
    return spark
