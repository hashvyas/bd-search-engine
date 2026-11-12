import argparse
import hashlib
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.common import config
from src.common.config import table


def pagerank(nodes, edges, iterations, damping=0.85, checkpoint_every=3, sleep_per_iter=0.0, log=print):
    if iterations < config.MIN_PAGERANK_ITERATIONS:
        raise ValueError(f"PageRank needs at least {config.MIN_PAGERANK_ITERATIONS} iterations")
    nodes = nodes.select("doc_id").distinct().cache()
    n = nodes.count()
    if n == 0:
        return nodes.withColumn("pagerank", F.lit(0.0))
    out = edges.groupBy("src").agg(F.count("*").alias("out_degree"))
    weighted = edges.join(out, "src").select("src", "dst", (1.0 / F.col("out_degree")).alias("w")).cache()
    dangling_nodes = nodes.join(out.withColumnRenamed("src", "doc_id"), "doc_id", "left_anti").cache()

    ranks = nodes.withColumn("pagerank", F.lit(1.0 / n))
    for i in range(1, iterations + 1):
        dangling = (dangling_nodes.join(ranks, "doc_id").agg(F.sum("pagerank")).first()[0]) or 0.0
        contrib = (weighted.join(ranks.withColumnRenamed("doc_id", "src"), "src")
                   .groupBy("dst").agg(F.sum(F.col("pagerank") * F.col("w")).alias("c")))
        new = (nodes.join(contrib.withColumnRenamed("dst", "doc_id"), "doc_id", "left")
               .select("doc_id", ((1 - damping) / n + damping * (F.coalesce("c", F.lit(0.0)) + dangling / n))
                       .alias("pagerank")))
        new = new.checkpoint() if i % checkpoint_every == 0 else new.cache()
        delta = new.join(ranks.withColumnRenamed("pagerank", "old"), "doc_id") \
                   .agg(F.sum(F.abs(F.col("pagerank") - F.col("old")))).first()[0]
        ranks.unpersist()
        ranks = new
        log(f"[pagerank] iteration {i}/{iterations} L1 delta={delta:.3e} dangling mass={dangling:.4f}"
            + (" (checkpointed)" if i % checkpoint_every == 0 else ""))
        if sleep_per_iter:
            time.sleep(sleep_per_iter)
    return ranks


def digest(rows):
    lines = sorted(f"{r['doc_id']}:{r['pagerank']:.10f}" for r in rows)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--iterations", type=int, default=config.PAGERANK_ITERATIONS)
    p.add_argument("--sleep-per-iter", type=float, default=float(config.env("PAGERANK_ITER_SLEEP", "0")))
    p.add_argument("--no-write", action="store_true", help="compute and print the digest only")
    a = p.parse_args()
    from src.common.spark import get_spark
    spark = get_spark("pagerank")
    nodes = spark.table(table("documents")).where(~F.col("is_deleted")).select("doc_id")
    edges = (spark.table(table("links"))
             .join(nodes.withColumnRenamed("doc_id", "src"), "src", "left_semi")
             .join(nodes.withColumnRenamed("doc_id", "dst"), "dst", "left_semi"))
    ranks = pagerank(nodes, edges, a.iterations, config.DAMPING, config.PAGERANK_CHECKPOINT_EVERY, a.sleep_per_iter)
    rows = ranks.collect()
    print(f"[pagerank] nodes={len(rows)} sum={sum(r['pagerank'] for r in rows):.6f}")
    for r in sorted(rows, key=lambda r: -r["pagerank"])[:5]:
        print(f"[pagerank] top {r['doc_id']}: {r['pagerank']:.6f}")
    print(f"PAGERANK_DIGEST={digest(rows)}")
    if not a.no_write:
        ranks.writeTo(table("pagerank")).using("iceberg").createOrReplace()
    spark.stop()


if __name__ == "__main__":
    main()
