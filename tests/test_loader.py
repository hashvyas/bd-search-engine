import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq

from src.common.manifest import finalize_export
from src.serve.loader import Holder


def make_export(root, version, docs):
    d = root / version
    d.mkdir(parents=True)
    pq.write_table(pa.table({"doc_id": [x[0] for x in docs], "url": [f"https://x/{x[0]}" for x in docs],
                             "title": [x[0] for x in docs], "doc_len": [1] * len(docs),
                             "pagerank": [1.0 / len(docs)] * len(docs)}), d / "docs.parquet")
    pq.write_table(pa.table({"term": [x[1] for x in docs], "doc_id": [x[0] for x in docs],
                             "tf": [1] * len(docs)}), d / "index.parquet")
    finalize_export(d, {"version": version, "phase": 0, "row_counts": {"docs": len(docs), "index": len(docs)},
                        "stats": {"N": len(docs), "avgdl": 1.0, "k1": 1.2, "b": 0.75, "w_bm25": 0.7, "w_pagerank": 0.3}})
    return d


def test_good_reload_then_bad_exports_keep_old_version(tmp_path):
    h = Holder(tmp_path)
    make_export(tmp_path, "v0", [("1", "apple"), ("2", "pear")])
    assert h.reload("v0").version == "v0"
    assert (tmp_path / "CURRENT").read_text().strip() == "v0"

    corrupt = make_export(tmp_path, "v1", [("1", "apple"), ("3", "plum")])
    (corrupt / "index.parquet").write_bytes(b"not parquet")
    with pytest.raises(ValueError, match="checksum"):
        h.reload("v1")

    unfinished = make_export(tmp_path, "v2", [("1", "apple")])
    (unfinished / "_SUCCESS").unlink()
    with pytest.raises(ValueError, match="_SUCCESS"):
        h.reload("v2")

    with pytest.raises(ValueError):
        h.reload("../etc")
    assert h.current.version == "v0" and "pear" in h.current.postings
    assert (tmp_path / "CURRENT").read_text().strip() == "v0"


def test_restart_loads_current(tmp_path):
    make_export(tmp_path, "v0", [("1", "apple")])
    Holder(tmp_path).reload("v0")
    assert Holder(tmp_path).load_current().version == "v0"
