# Project Iceberg settings, passed to spark-submit with --properties-file (pattern from docs/iceberg_guide.pdf).
# spark.jars stays unset: every node loads the runtime jar from $SPARK_HOME/jars.
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type=hadoop
spark.sql.catalog.iceberg_catalog.warehouse=${WAREHOUSE}
