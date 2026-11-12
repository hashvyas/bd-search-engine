import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.common.config import table
from src.common.schema import current_snapshot, ensure_tables
from src.common.tokenize import LOWER, MIN_LEN, SPLIT_REGEX, STOPWORDS, UPPER


def terms(df, text_col):
    tok = F.explode(F.split(F.translate(F.col(text_col), UPPER, LOWER), SPLIT_REGEX)).alias("term")
    return (df.select("*", tok)
            .where((F.length("term") >= MIN_LEN) & ~F.col("term").isin(sorted(STOPWORDS)))
            .drop(text_col))


def postings_for(docs):
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
    new_postings = postings_for(todo).checkpoint()
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
