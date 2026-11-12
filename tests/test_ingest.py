import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F

from src.common.config import table
from src.common.schema import current_snapshot
from src.ingest import ingest_job as ij


def doc(i, links=(), **kw):
    d = {"doc_id": str(i), "url": f"https://simple.wikipedia.org/wiki/D{i}", "title": f"Doc {i}",
         "text": f"text of doc {i}", "links": list(links)}
    d.update(kw)
    return d


def test_validation_reasons(spark, jsonl):
    path = jsonl([
        doc(1),
        '{"doc_id": "broken", "url":',
        {k: v for k, v in doc(2).items() if k != "links"},
        doc(3, url=""),
        doc(4, url="not-a-url"),
        doc(5, title="  "),
        {k: v for k, v in doc(6).items() if k != "text"},
        doc(7, op="RENAME"),
        doc(8, version="abc"),
        doc(9, version=-1),
        {"doc_id": "10", "op": "DELETE"},
        doc("", links=[]),
        {**doc(11), "links": "12"},
        doc(12, version="3"),
        doc(13, version="2024-05-01 10:00:00"),
    ])
    accepted, rejected = ij.validate(ij.read_input(spark, path), phase=0)
    reasons = {r["doc_id"]: r["reason"] for r in rejected.collect()}
    assert reasons == {
        "broken": "malformed_json_or_wrong_type", "3": "missing_url", "4": "invalid_url", "5": "missing_title",
        "6": "missing_text", "7": "unknown_op:RENAME", "8": "invalid_version", "9": "negative_version",
        "": "missing_doc_id", "11": "malformed_json_or_wrong_type"}
    ok = {r["doc_id"]: r for r in accepted.collect()}
    assert set(ok) == {"1", "2", "10", "12", "13"}
    assert ok["2"]["links"] == [] and ok["10"]["is_deleted"] and ok["12"]["version"] == 3
    assert ok["13"]["version"] > 1_700_000_000 and ok["13"]["has_version"] and not ok["1"]["has_version"]


def test_dedup_highest_version_wins_then_last_line(spark, jsonl):
    path = jsonl([doc(1, title="v1", version=1), doc(1, title="v3", version=3), doc(1, title="v2", version=2),
                  doc(2, title="first"), doc(2, title="second")])
    accepted, _ = ij.validate(ij.read_input(spark, path), phase=0)
    winners, superseded = ij.dedup(accepted)
    titles = {r["doc_id"]: r["title"] for r in winners.collect()}
    assert titles == {"1": "v3", "2": "second"}
    assert superseded.count() == 3


def test_pipeline_links_idempotence_and_snapshot_diff(clean, jsonl):
    spark = clean
    phase0 = jsonl([doc(1, ["2", "3", "missing", "1"]), doc(2, ["1"]), doc(3, ["2"]), doc(3, ["2"])])
    s0 = ij.run(spark, 0, phase0, "h0")
    assert s0["status"] == "applied" and s0["docs_changed"] == 3 and s0["duplicates"] == 1
    links = {(r.src, r.dst) for r in spark.table(table("links")).collect()}
    assert links == {("1", "2"), ("1", "3"), ("2", "1"), ("3", "2")}
    assert s0["links_dropped"] == 2

    snaps = (current_snapshot(spark, "documents"), current_snapshot(spark, "links"))
    again = ij.run(spark, 0, phase0, "h0")
    assert again["status"] == "no_change" and again["docs_changed"] == 0
    assert (current_snapshot(spark, "documents"), current_snapshot(spark, "links")) == snaps

    phase1 = jsonl([{"doc_id": "3", "op": "DELETE"}, doc(2, ["1"], title="Doc 2 new", op="UPSERT"),
                    doc(4, ["3", "1"], op="UPSERT")])
    before = current_snapshot(spark, "documents")
    s1 = ij.run(spark, 1, phase1, "h1")
    changes = {r.doc_id: r.change_type for r in spark.table(table("doc_changes"))
               .where(F.col("from_snapshot") == before).collect()}
    assert changes == {"3": "deleted", "2": "updated", "4": "added"}
    links = {(r.src, r.dst) for r in spark.table(table("links")).collect()}
    assert links == {("1", "2"), ("2", "1"), ("4", "1")}
    assert s1["links_deleted"] == 2 and s1["links_inserted"] == 1

    snap = current_snapshot(spark, "documents")
    assert ij.run(spark, 0, phase0, "h0")["status"] == "no_change"
    assert current_snapshot(spark, "documents") == snap
    live = {r.doc_id for r in ij.live_documents(spark).collect()}
    assert live == {"1", "2", "4"}
    assert spark.table(table("ingest_log")).count() == 2


def test_explicit_versions_beat_phase_order(clean, jsonl):
    spark = clean
    ij.run(spark, 0, jsonl([doc(1, title="v5", version=5), doc(2, title="v5", version=5), doc(3, version=1)]))
    ij.run(spark, 1, jsonl([doc(1, title="stale v3", version=3, op="UPSERT"),
                            doc(2, title="v6", version=6, op="UPSERT"),
                            {"doc_id": "3", "op": "DELETE"}]))
    titles = {r.doc_id: r.title for r in ij.live_documents(spark).collect()}
    assert titles == {"1": "v5", "2": "v6"}
