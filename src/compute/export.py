"""Export the serving snapshot as plain Parquet to node3's local disk, atomically.

Spark writes single-file Parquet to DATA_ROOT/export_staging. The driver copies the
files into a local tmp dir and writes stats, the manifest and _SUCCESS, in that
order, then renames the tmp dir to EXPORT_DIR/<version> and updates EXPORT_DIR/LATEST.
A crash at any step leaves no directory under a version name.
"""
import argparse
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F  # noqa: E402

from src.common import config  # noqa: E402
from src.common.config import table  # noqa: E402
from src.common.manifest import SUCCESS, finalize_export  # noqa: E402
from src.common.schema import current_snapshot  # noqa: E402


def copy_single_parquet(spark, df, staging_dir, local_file):
    """Write df as one parquet file to staging_dir (HDFS or file://), then copy it to local_file."""
    df.coalesce(1).write.mode("overwrite").parquet(staging_dir)
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(staging_dir)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    parts = [s.getPath() for s in fs.listStatus(path)
             if s.getPath().getName().startswith("part-") and s.getPath().getName().endswith(".parquet")]
    if len(parts) != 1:
        raise RuntimeError(f"expected 1 parquet part in {staging_dir}, found {len(parts)}")
    # useRawLocalFileSystem=True: do not write .crc side files
    fs.copyToLocalFile(False, parts[0], jvm.org.apache.hadoop.fs.Path("file://" + str(local_file)), True)


def run(spark, phase):
    docs_snap = current_snapshot(spark, "documents")
    # The pagerank snapshot changes on every compute run, so rerunning PageRank gives a new export.
    version = f"v{phase}-{docs_snap}-{current_snapshot(spark, 'pagerank')}"
    export_root = Path(config.EXPORT_DIR)
    final = export_root / version
    if (final / SUCCESS).is_file():
        print(f"[export] {final} already complete; nothing to do")
        return version

    live = spark.table(table("documents")).where(~F.col("is_deleted")).select("doc_id", "url", "title")
    index = spark.table(table("postings")).join(live.select("doc_id"), "doc_id", "left_semi") \
        .select("term", "doc_id", "tf").orderBy("term", "doc_id")
    doc_len = index.groupBy("doc_id").agg(F.sum("tf").cast("long").alias("doc_len"))
    pr = spark.table(table("pagerank"))
    docs = (live.join(doc_len, "doc_id", "left").join(pr, "doc_id", "left")
            .select("doc_id", "url", "title",
                    F.coalesce("doc_len", F.lit(0)).cast("long").alias("doc_len"),
                    F.coalesce("pagerank", F.lit(0.0)).alias("pagerank"))
            .orderBy("doc_id").cache())
    agg = docs.agg(F.count("*").alias("n"), F.avg("doc_len").alias("avgdl")).first()
    n_index = index.count()

    export_root.mkdir(parents=True, exist_ok=True)
    tmp = export_root / f".tmp-{version}-{os.getpid()}"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    staging = f"{config.DATA_ROOT}/export_staging/{version}"
    copy_single_parquet(spark, index, f"{staging}/index", tmp / "index.parquet")
    copy_single_parquet(spark, docs, f"{staging}/docs", tmp / "docs.parquet")

    finalize_export(tmp, {
        "version": version, "phase": phase,
        "iceberg_snapshots": {"documents": docs_snap, "links": current_snapshot(spark, "links"),
                              "postings": current_snapshot(spark, "postings"),
                              "pagerank": current_snapshot(spark, "pagerank")},
        "row_counts": {"index": n_index, "docs": agg["n"]},
        "stats": {"N": agg["n"], "avgdl": float(agg["avgdl"] or 0.0), "k1": config.BM25_K1, "b": config.BM25_B,
                  "w_bm25": config.W_BM25, "w_pagerank": config.W_PAGERANK},
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    os.rename(tmp, final)  # atomic on one filesystem
    latest_tmp = export_root / ".LATEST.tmp"
    latest_tmp.write_text(version + "\n")
    os.replace(latest_tmp, export_root / "LATEST")
    print(f"[export] wrote {final} (docs={agg['n']}, postings={n_index}, avgdl={float(agg['avgdl'] or 0):.2f})")
    return version


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--phase", type=int, required=True)
    a = p.parse_args()
    from src.common.spark import get_spark
    spark = get_spark("export")
    print(f"EXPORT_VERSION={run(spark, a.phase)}")
    spark.stop()


if __name__ == "__main__":
    main()
