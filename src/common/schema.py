"""Input schema and Iceberg table DDL (Spark side only)."""
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from src.common.config import table

CORRUPT = "_corrupt_record"

# Field names come from data/toy. `version` and `op` are optional. `version` is read as a
# string and cast later, so "3" and 3 are both accepted and anything else is rejected
# with a reason instead of failing the whole record.
INPUT_SCHEMA = StructType([
    StructField("doc_id", StringType()),
    StructField("url", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("links", ArrayType(StringType())),
    StructField("version", StringType()),
    StructField("op", StringType()),
    StructField(CORRUPT, StringType()),
])

DOC_COLUMNS = ["doc_id", "url", "title", "text", "links", "version", "has_version", "phase", "is_deleted"]

_PROPS = "TBLPROPERTIES ('format-version'='2')"

DDL = {
    "documents": f"""CREATE TABLE IF NOT EXISTS {table('documents')} (
        doc_id STRING, url STRING, title STRING, text STRING,
        links ARRAY<STRING>, version BIGINT, has_version BOOLEAN, phase INT, is_deleted BOOLEAN
    ) USING iceberg {_PROPS}""",
    "links": f"""CREATE TABLE IF NOT EXISTS {table('links')} (
        src STRING, dst STRING
    ) USING iceberg {_PROPS}""",
    "doc_changes": f"""CREATE TABLE IF NOT EXISTS {table('doc_changes')} (
        phase INT, from_snapshot BIGINT, to_snapshot BIGINT, doc_id STRING, change_type STRING
    ) USING iceberg {_PROPS}""",
    "ingest_log": f"""CREATE TABLE IF NOT EXISTS {table('ingest_log')} (
        phase INT, file_name STRING, file_hash STRING, status STRING,
        records_read BIGINT, rejected BIGINT, duplicates BIGINT, docs_changed BIGINT,
        links_inserted BIGINT, links_deleted BIGINT, links_dropped BIGINT,
        documents_snapshot BIGINT, links_snapshot BIGINT, ingested_at TIMESTAMP
    ) USING iceberg {_PROPS}""",
    "postings": f"""CREATE TABLE IF NOT EXISTS {table('postings')} (
        term STRING, doc_id STRING, tf INT,
        doc_phase INT, doc_version BIGINT
    ) USING iceberg {_PROPS}""",
}


def ensure_tables(spark, names):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table('x').rsplit('.', 1)[0]}")
    for name in names:
        spark.sql(DDL[name])


def current_snapshot(spark, name):
    """Current snapshot id of an Iceberg table, or None if it has none yet."""
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table(name)}.history "
        "WHERE is_current_ancestor ORDER BY made_current_at DESC LIMIT 1").collect()
    return rows[0][0] if rows else None
