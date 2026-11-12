"""Build or update the inverted index (Iceberg table `postings`: term, doc_id, tf).

Incremental: every posting stores the (phase, version) of its doc. A doc is
re-indexed only when its stored rank no longer matches the live document.
Postings of deleted docs are removed. The first run builds everything.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F  # noqa: E402

from src.common.config import table  # noqa: E402
from src.common.schema import current_snapshot, ensure_tables  # noqa: E402
from src.common.tokenize import LOWER, MIN_LEN, SPLIT_REGEX, STOPWORDS, UPPER  # noqa: E402


def terms(df, text_col):
    """Spark built-in version of src.common.tokenize.tokenize: one row per token occurrence."""
    tok = F.explode(F.split(F.translate(F.col(text_col), UPPER, LOWER), SPLIT_REGEX)).alias("term")
    return (df.select("*", tok)
            .where((F.length("term") >= MIN_LEN) & ~F.col("term").isin(sorted(STOPWORDS)))
            .drop(text_col))


def postings_for(docs):
    """docs: doc_id, title, text, phase, version -> term, doc_id, tf, doc_phase, doc_version."""
    body = docs.select("doc_id", F.col("phase").alias("doc_phase"), F.col("version").alias("doc_version"),
                       F.concat_ws(" ", "title", "text").alias("body"))
    return (terms(body, "body")
            .groupBy("term", "doc_id", "doc_phase", "doc_version")
            .agg(F.count("*").cast("int").alias("tf"))
            .select("term", "doc_id", "tf", "doc_phase", "doc_version"))


def run(spark):
    ensure_tables(spark, ["postings"])
    live = (spark.table(table("documents")).where(~F.col("is_deleted"))
            .select("doc_id", "title", "text", "phase", "version"))
    rev = ["doc_id", "doc_phase", "doc_version"]
    live_rev = live.select("doc_id", F.col("phase").alias("doc_phase"), F.col("version").alias("doc_version"))
    indexed = spark.table(table("postings")).select(*rev).distinct()

    stale = indexed.join(live_rev, rev, "left_anti").select("doc_id").distinct().checkpoint()
    todo = live.join(indexed.withColumnRenamed("doc_phase", "phase").withColumnRenamed("doc_version", "version"),
                     ["doc_id", "phase", "version"], "left_anti")
    new_postings = postings_for(todo).checkpoint()  # materialized before postings is modified
    n_stale, n_new = stale.count(), new_postings.select("doc_id").distinct().count()

    if n_stale:
        stale.createOrReplaceTempView("stale_docs")
        spark.sql(f"MERGE INTO {table('postings')} t USING stale_docs s ON t.doc_id = s.doc_id "
                  "WHEN MATCHED THEN DELETE")
    if n_new:
        new_postings.writeTo(table("postings")).append()

    p = spark.table(table("postings"))
    print(f"[index] docs removed/updated: {n_stale}, docs (re)indexed: {n_new}, "
          f"postings: {p.count()}, terms: {p.select('term').distinct().count()}, "
          f"live docs: {live.count()}, postings snapshot: {current_snapshot(spark, 'postings')}")


def main():
    from src.common.spark import get_spark
    spark = get_spark("build-index")
    run(spark)
    spark.stop()


if __name__ == "__main__":
    main()
