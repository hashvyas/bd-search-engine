"""Phase ingestion: validate -> dedup -> MERGE into Iceberg -> sync links -> diff snapshots -> log.

Idempotence: an incoming row is staged only if its rank (phase, version) is higher
than the stored row's rank, or if the doc is new. When nothing qualifies, no MERGE
runs, so the data tables get no new snapshot.

Usage: spark-submit ingest_job.py --phase 0 --input hdfs://.../pages.jsonl --file-hash <sha256>
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Window, functions as F  # noqa: E402

from src.common.config import DATA_ROOT, table  # noqa: E402
from src.common.schema import CORRUPT, DOC_COLUMNS, INPUT_SCHEMA, current_snapshot, ensure_tables  # noqa: E402

OP_ALIASES = {"UPSERT": "UPSERT", "INSERT": "UPSERT", "UPDATE": "UPSERT", "DELETE": "DELETE"}
LOG_SCHEMA = ("phase int, file_name string, file_hash string, status string, records_read bigint, "
              "rejected bigint, duplicates bigint, docs_changed bigint, links_inserted bigint, "
              "links_deleted bigint, links_dropped bigint, documents_snapshot bigint, links_snapshot bigint")
LOG_SCHEMA_COLS = [c.split()[0] for c in LOG_SCHEMA.split(", ")]


def read_input(spark, path):
    """Read JSON lines, or a single JSON array (detected from the first character)."""
    first = spark.read.text(path).limit(1).collect()
    multiline = bool(first) and first[0][0].lstrip().startswith("[")
    df = (spark.read.schema(INPUT_SCHEMA)
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", CORRUPT)
          .option("multiLine", multiline)
          .json(path))
    # seq keeps file order (it is used to break ties between duplicates). cache() is
    # needed before Spark lets us filter on the corrupt-record column alone.
    return df.withColumn("seq", F.monotonically_increasing_id()).cache()


def validate(df, phase):
    """Return (accepted, rejected). rejected has: doc_id, reason, raw, phase."""
    op_map = F.create_map(*[F.lit(x) for kv in OP_ALIASES.items() for x in kv])
    blank = lambda c: F.col(c).isNull() | (F.trim(F.col(c)) == "")  # noqa: E731
    d = (df
         .withColumn("doc_id", F.trim("doc_id"))
         .withColumn("op_raw", F.upper(F.trim(F.coalesce(F.col("op"), F.lit("UPSERT")))))
         .withColumn("op", op_map[F.col("op_raw")])
         # Integer versions, or timestamp-style versions (converted to epoch seconds).
         .withColumn("version_num", F.coalesce(F.col("version").cast("long"),
                                               F.unix_timestamp(F.to_timestamp(F.col("version"))))))
    upsert = F.col("op") == "UPSERT"
    reason = (F.when(F.col(CORRUPT).isNotNull(), "malformed_json_or_wrong_type")
              .when(F.col("op").isNull(), F.concat(F.lit("unknown_op:"), F.col("op_raw")))
              .when(blank("doc_id"), "missing_doc_id")
              .when(F.col("version").isNotNull() & F.col("version_num").isNull(), "invalid_version")
              .when(F.col("version_num") < 0, "negative_version")
              .when(upsert & blank("url"), "missing_url")
              .when(upsert & ~F.col("url").rlike(r"^https?://\S+$"), "invalid_url")
              .when(upsert & blank("title"), "missing_title")
              .when(upsert & F.col("text").isNull(), "missing_text"))
    d = d.withColumn("reason", reason)

    raw = F.coalesce(F.col(CORRUPT), F.to_json(F.struct("doc_id", "url", "title", "links", "version", "op_raw")))
    salvaged_id = F.regexp_extract(F.col(CORRUPT), r'"doc_id"\s*:\s*"([^"]*)"', 1)
    rejected = d.where(F.col("reason").isNotNull()).select(
        F.coalesce(F.col("doc_id"), salvaged_id).alias("doc_id"), "reason", raw.alias("raw"),
        F.lit(phase).alias("phase"))

    accepted = d.where(F.col("reason").isNull()).select(
        "doc_id", "url", "title", "text",
        # links are optional (40 toy records have none). Blank or null entries are dropped.
        F.filter(F.coalesce(F.col("links"), F.array().cast("array<string>")),
                 lambda x: x.isNotNull() & (F.trim(x) != "")).alias("links"),
        F.coalesce(F.col("version_num"), F.lit(0)).alias("version"),
        F.col("version_num").isNotNull().alias("has_version"),
        F.lit(phase).cast("int").alias("phase"),
        (F.col("op") == "DELETE").alias("is_deleted"),
        "seq")
    return accepted, rejected


def dedup(accepted):
    """Keep one row per doc_id: highest version wins, and the later line breaks ties.
    Returns (winners, superseded)."""
    w = Window.partitionBy("doc_id").orderBy(F.col("version").desc(), F.col("seq").desc())
    ranked = accepted.withColumn("_rn", F.row_number().over(w))
    winners = ranked.where("_rn = 1").select(*DOC_COLUMNS)
    # A DELETE keeps no payload: the tombstone only blocks lower-ranked re-inserts.
    winners = winners.select(
        "doc_id",
        *[F.when(F.col("is_deleted"), F.lit(None)).otherwise(F.col(c)).alias(c) for c in ("url", "title", "text")],
        F.when(F.col("is_deleted"), F.array().cast("array<string>")).otherwise(F.col("links")).alias("links"),
        "version", "has_version", "phase", "is_deleted")
    superseded = ranked.where("_rn > 1").select(
        "doc_id", F.lit("duplicate_superseded").alias("reason"),
        F.to_json(F.struct("doc_id", "version", "seq")).alias("raw"), "phase")
    return winners, superseded


def stage_changes(spark, winners):
    """Keep only incoming rows that win over what is stored. Checkpointed so later
    reads do not re-evaluate this after the MERGE has changed the target table.

    If both rows carry an explicit version, the higher version wins and the later phase breaks
    ties, so a stale update loses even in a later phase. Otherwise the later phase wins, then
    the higher version. A DELETE has no version, so it beats any earlier-phase row.
    """
    t = spark.table(table("documents")).select(
        "doc_id", F.col("phase").alias("t_phase"), F.col("version").alias("t_version"),
        F.col("has_version").alias("t_has_version"))
    s = winners.join(t, "doc_id", "left")
    by_version = (F.col("version") > F.col("t_version")) | (
        (F.col("version") == F.col("t_version")) & (F.col("phase") > F.col("t_phase")))
    by_phase = (F.col("phase") > F.col("t_phase")) | (
        (F.col("phase") == F.col("t_phase")) & (F.col("version") > F.col("t_version")))
    wins = (F.col("t_phase").isNull()
            | F.when(F.col("has_version") & F.col("t_has_version"), by_version).otherwise(by_phase))
    return s.where(wins).select(*DOC_COLUMNS).checkpoint()


def merge_documents(spark, staged):
    """staged holds only winning rows (stage_changes), so every match is an update."""
    staged.createOrReplaceTempView("staged_documents")
    spark.sql(f"""
        MERGE INTO {table('documents')} t
        USING staged_documents s
        ON t.doc_id = s.doc_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def live_documents(spark, snapshot=None):
    src = table("documents") + (f" VERSION AS OF {snapshot}" if snapshot else "")
    return spark.sql(f"SELECT * FROM {src}").where(~F.col("is_deleted"))


def sync_links(spark, phase):
    """Make the links table equal to the edges between live docs.

    Edges to a missing or deleted doc are dropped. documents.links keeps the raw
    list, so an edge comes back if its target is re-added later.
    Returns (inserted, deleted, dropped_df).
    """
    live = live_documents(spark)
    ids = live.select(F.col("doc_id").alias("dst"))
    edges = live.select(F.col("doc_id").alias("src"), F.explode("links").alias("dst")).distinct()
    valid = edges.join(ids, "dst", "left_semi").where(F.col("src") != F.col("dst"))
    dropped = edges.join(valid, ["src", "dst"], "left_anti").select(
        F.col("src").alias("doc_id"),
        F.when(F.col("src") == F.col("dst"), "self_link").otherwise("link_target_missing").alias("reason"),
        F.to_json(F.struct("src", "dst")).alias("raw"), F.lit(phase).alias("phase"))

    current = spark.table(table("links")).select("src", "dst")
    staged = (valid.join(current, ["src", "dst"], "left_anti").withColumn("action", F.lit("I"))
              .unionByName(current.join(valid, ["src", "dst"], "left_anti").withColumn("action", F.lit("D")))
              .checkpoint())
    counts = {r["action"]: r["count"] for r in staged.groupBy("action").count().collect()}
    if counts:
        staged.createOrReplaceTempView("staged_links")
        spark.sql(f"""
            MERGE INTO {table('links')} t
            USING staged_links s
            ON t.src = s.src AND t.dst = s.dst
            WHEN MATCHED AND s.action = 'D' THEN DELETE
            WHEN NOT MATCHED AND s.action = 'I' THEN INSERT (src, dst) VALUES (s.src, s.dst)
        """)
    return counts.get("I", 0), counts.get("D", 0), dropped


def snapshot_diff(spark, before, after):
    """Return doc ids that changed between two documents snapshots: doc_id, change_type."""
    def state(snap):
        if snap is None:
            return spark.table(table("documents")).select("doc_id").limit(0).withColumn("rev", F.lit(None).cast("string"))
        return live_documents(spark, snap).select("doc_id", F.concat_ws(":", "phase", "version").alias("rev"))

    b, a = state(before).withColumnRenamed("rev", "b"), state(after).withColumnRenamed("rev", "a")
    j = b.join(a, "doc_id", "full_outer")
    change = (F.when(F.col("b").isNull(), "added")
              .when(F.col("a").isNull(), "deleted")
              .when(F.col("a") != F.col("b"), "updated"))
    return j.select("doc_id", change.alias("change_type")).where(F.col("change_type").isNotNull())


def run(spark, phase, input_path, file_hash="", file_name=""):
    ensure_tables(spark, ["documents", "links", "doc_changes", "ingest_log"])
    seen = spark.table(table("ingest_log")).where(F.col("file_hash") == file_hash).count() if file_hash else 0
    if seen:
        print(f"[ingest] note: this file hash was already ingested {seen} time(s); expecting no changes")

    raw = read_input(spark, input_path)
    records_read = raw.count()
    accepted, rejected = validate(raw, phase)
    winners, superseded = dedup(accepted)

    before = current_snapshot(spark, "documents")
    staged = stage_changes(spark, winners)
    n_staged = staged.count()
    if n_staged:
        merge_documents(spark, staged)
    after = current_snapshot(spark, "documents")

    links_inserted, links_deleted, dropped = sync_links(spark, phase)

    changes_n = 0
    if after != before:
        changes = snapshot_diff(spark, before, after).checkpoint()
        changes_n = changes.count()
        (changes.select(F.lit(phase).cast("int").alias("phase"), F.lit(before).cast("long").alias("from_snapshot"),
                        F.lit(after).cast("long").alias("to_snapshot"), "doc_id", "change_type")
         .writeTo(table("doc_changes")).append())
        print("[ingest] changed docs by type:",
              {r["change_type"]: r["count"] for r in changes.groupBy("change_type").count().collect()})

    rejects = rejected.unionByName(superseded).unionByName(dropped)
    rejects.coalesce(1).write.mode("overwrite").json(f"{DATA_ROOT}/rejects/phase={phase}")
    reject_counts = {r["reason"]: r["count"] for r in rejects.groupBy("reason").count().collect()}

    summary = {
        "phase": phase, "file_name": file_name, "file_hash": file_hash,
        "status": "applied" if (n_staged or links_inserted or links_deleted) else "no_change",
        "records_read": records_read,
        "rejected": sum(v for k, v in reject_counts.items() if k not in ("duplicate_superseded", "link_target_missing", "self_link")),
        "duplicates": reject_counts.get("duplicate_superseded", 0),
        "docs_changed": n_staged,
        "links_inserted": links_inserted, "links_deleted": links_deleted,
        "links_dropped": reject_counts.get("link_target_missing", 0) + reject_counts.get("self_link", 0),
        "documents_snapshot": after, "links_snapshot": current_snapshot(spark, "links"),
    }
    # A rerun that changes nothing writes no log row either, so no table in the namespace gets a new snapshot.
    if summary["status"] == "applied":
        log_df = spark.createDataFrame([tuple(summary[c] for c in LOG_SCHEMA_COLS)], LOG_SCHEMA)
        log_df.withColumn("ingested_at", F.current_timestamp()).writeTo(table("ingest_log")).append()

    live = live_documents(spark).count()
    print("[ingest] reject reasons:", reject_counts)
    print("[ingest] summary:", json.dumps(dict(summary, live_documents=live, doc_change_rows=changes_n)))
    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--phase", type=int, required=True)
    p.add_argument("--input", required=True)
    p.add_argument("--file-hash", default="")
    p.add_argument("--file-name", default="")
    a = p.parse_args()
    from src.common.spark import get_spark
    spark = get_spark(f"ingest-phase{a.phase}")
    run(spark, a.phase, a.input, a.file_hash, a.file_name or Path(a.input).name)
    spark.stop()


if __name__ == "__main__":
    main()
