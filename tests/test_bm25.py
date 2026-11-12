"""BM25 and final score against hand-computed values (k1=1.2, b=0.75)."""
import math
from types import SimpleNamespace

import pytest

from src.serve.bm25 import bm25_scores, idf, rank

# d0 = "apple banana" (len 2), d1 = "apple apple cherry" (len 3). N = 2, avgdl = 2.5
INDEX = SimpleNamespace(
    n_docs=2, avgdl=2.5, doc_ids=["d0", "d1"], urls=["u0", "u1"], titles=["t0", "t1"],
    doc_len=[2, 3], pagerank=[0.2, 0.8], max_pagerank=0.8,
    postings={"apple": [(0, 1), (1, 2)], "banana": [(0, 1)], "cherry": [(1, 1)]})


def test_idf_matches_snippet_formula():
    assert idf(2, 1) == pytest.approx(math.log(2.0))      # ln(1 + 1.5/1.5)
    assert idf(2, 2) == pytest.approx(math.log(1.2))      # ln(1 + 0.5/2.5)


def test_bm25_hand_computed():
    # banana in d0: ln2 * (1*2.2) / (1 + 1.2*(0.25 + 0.75*2/2.5)) = 0.693147 * 2.2/2.02
    assert bm25_scores(["banana"], INDEX) == pytest.approx({0: 0.754913}, abs=1e-6)
    # apple: idf ln1.2. d0: 2.2/2.02 ; d1: 4.4/(2 + 1.2*(0.25 + 0.75*3/2.5)) = 4.4/3.38
    assert bm25_scores(["apple"], INDEX) == pytest.approx({0: 0.198568, 1: 0.237342}, abs=1e-6)
    assert bm25_scores(["zebra"], INDEX) == {}


def test_final_score_normalization():
    res = rank(["apple"], INDEX, k=10)
    assert [r["doc_id"] for r in res] == ["d1", "d0"]
    assert res[0]["score"] == pytest.approx(1.0)                                  # 0.7*1 + 0.3*1
    assert res[1]["score"] == pytest.approx(0.7 * 0.198568 / 0.237342 + 0.3 * 0.25, abs=1e-5)
    assert rank(["zebra"], INDEX) == []
