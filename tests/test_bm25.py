import math
from types import SimpleNamespace

import pytest

from src.serve.bm25 import bm25_scores, idf, rank

INDEX = SimpleNamespace(
    n_docs=2, avgdl=2.5, doc_ids=["d0", "d1"], urls=["u0", "u1"], titles=["t0", "t1"],
    doc_len=[2, 3], pagerank=[0.2, 0.8], max_pagerank=0.8,
    postings={"apple": [(0, 1), (1, 2)], "banana": [(0, 1)], "cherry": [(1, 1)]})


def test_idf_matches_snippet_formula():
    assert idf(2, 1) == pytest.approx(math.log(2.0))
    assert idf(2, 2) == pytest.approx(math.log(1.2))


def test_bm25_hand_computed():
    assert bm25_scores(["banana"], INDEX) == pytest.approx({0: 0.754913}, abs=1e-6)
    assert bm25_scores(["apple"], INDEX) == pytest.approx({0: 0.198568, 1: 0.237342}, abs=1e-6)
    assert bm25_scores(["zebra"], INDEX) == {}


def test_final_score_normalization():
    res = rank(["apple"], INDEX, k=10)
    assert [r["doc_id"] for r in res] == ["d1", "d0"]
    assert res[0]["score"] == pytest.approx(1.0)
    assert res[1]["score"] == pytest.approx(0.7 * 0.198568 / 0.237342 + 0.3 * 0.25, abs=1e-5)
    assert rank(["zebra"], INDEX) == []
