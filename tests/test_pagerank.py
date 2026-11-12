import pytest

pytest.importorskip("pyspark")
from src.compute.pagerank import pagerank


def reference(nodes, edges, iters, d=0.85):
    n = len(nodes)
    out = {u: [v for (s, v) in edges if s == u] for u in nodes}
    pr = {u: 1.0 / n for u in nodes}
    for _ in range(iters):
        dangling = sum(pr[u] for u in nodes if not out[u])
        new = {u: (1 - d) / n + d * dangling / n for u in nodes}
        for u in nodes:
            for v in out[u]:
                new[v] += d * pr[u] / len(out[u])
        pr = new
    return pr


def run(spark, nodes, edges, iters=20):
    n = spark.createDataFrame([(x,) for x in nodes], "doc_id string")
    e = spark.createDataFrame(edges, "src string, dst string")
    return {r.doc_id: r.pagerank for r in pagerank(n, e, iters, 0.85, checkpoint_every=3, log=lambda *_: None).collect()}


def test_cycle_is_uniform(spark):
    pr = run(spark, ["a", "b", "c"], [("a", "b"), ("b", "c"), ("c", "a")])
    assert pr == pytest.approx({"a": 1 / 3, "b": 1 / 3, "c": 1 / 3}, abs=1e-12)


def test_dangling_node_matches_reference_and_sums_to_one(spark):
    nodes = ["a", "b", "c", "d", "e"]
    edges = [("a", "b"), ("a", "c"), ("b", "c"), ("c", "a"), ("c", "d")]
    pr = run(spark, nodes, edges, iters=15)
    assert pr == pytest.approx(reference(nodes, edges, 15), abs=1e-12)
    assert sum(pr.values()) == pytest.approx(1.0, abs=1e-9)
    assert max(pr, key=pr.get) == "c"


def test_minimum_iterations_enforced(spark):
    with pytest.raises(ValueError):
        run(spark, ["a"], [], iters=9)
