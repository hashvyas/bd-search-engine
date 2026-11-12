from pyspark.sql import SparkSession

from src.common.config import DATA_ROOT


def get_spark(app):
    spark = SparkSession.builder.appName(app).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(f"{DATA_ROOT}/checkpoints/{app}")
    return spark
