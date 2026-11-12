USAGE = """Iceberg inspection and recovery helpers (run through scripts/admin.sh).

  state                      snapshot id, row count and content fingerprint for each table
  snapshots <table>          snapshot history
  changes [phase]            doc_changes summary
  log                        ingest_log
  count <table> [snapshot]   row count, optionally time-travelled
  rollback <table> <snap>    CALL system.rollback_to_snapshot
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.common.config import CATALOG, NAMESPACE, table
from src.common.schema import current_snapshot

DATA_TABLES = ["documents", "links", "doc_changes", "ingest_log"]


def fingerprint(spark, name):
    df = spark.table(table(name))
    return df.agg(F.count("*").alias("rows"), F.coalesce(F.sum(F.xxhash64(*df.columns)), F.lit(0)).alias("h")).first()


def state(spark):
    for name in DATA_TABLES:
        r = fingerprint(spark, name)
        print(f"STATE {name} snapshot={current_snapshot(spark, name)} rows={r['rows']} hash={r['h']}")
    live = spark.table(table("documents")).where(~F.col("is_deleted")).count()
    print(f"STATE live_documents={live}")


def main(argv):
    from src.common.spark import get_spark
    cmd, args = (argv[0], argv[1:]) if argv else ("state", [])
    spark = get_spark("admin")
    if cmd == "state":
        state(spark)
    elif cmd == "snapshots":
        spark.sql(f"SELECT committed_at, snapshot_id, parent_id, operation, summary['added-records'] AS added, "
                  f"summary['deleted-records'] AS deleted FROM {table(args[0])}.snapshots ORDER BY committed_at") \
            .show(100, truncate=False)
    elif cmd == "changes":
        df = spark.table(table("doc_changes"))
        if args:
            df = df.where(F.col("phase") == int(args[0]))
        df.groupBy("phase", "from_snapshot", "to_snapshot", "change_type").count().orderBy("to_snapshot").show(100, False)
    elif cmd == "log":
        spark.table(table("ingest_log")).orderBy("ingested_at").show(100, truncate=False)
    elif cmd == "count":
        src = table(args[0]) + (f" VERSION AS OF {int(args[1])}" if len(args) > 1 else "")
        print(f"COUNT {src} = {spark.sql(f'SELECT count(*) FROM {src}').first()[0]}")
    elif cmd == "rollback":
        spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('{NAMESPACE}.{args[0]}', {int(args[1])})").show()
    else:
        sys.exit(USAGE)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
