"""Validation, dedup, link cleanup, idempotent merge, snapshot diff (Spark + Iceberg)."""
import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from src.common.config import table  # noqa: E402
from src.common.schema import current_snapshot  # noqa: E402
from src.ingest import ingest_job as ij  # noqa: E402


def doc(i, links=(), **kw):
    d = {"doc_id": str(i), "url": f"https://simple.wikipedia.org/wiki/D{i}", "title": f"Doc {i}",
         "text": f"text of doc {i}", "links": list(links)}
    d.update(kw)
    return d


def test_validation_reasons(spark, jsonl):
    path = jsonl([
        doc(1),
        '{"doc_id": "broken", "url":',                        # truncated JSON, like data/toy
        {k: v for k, v in doc(2).items() if k != "links"},    # links are optional
        doc(3, url=""),
        doc(4, url="not-a-url"),
        doc(5, title="  "),
        {k: v for k, v in doc(6).items() if k != "text"},
        doc(7, op="RENAME"),
        doc(8, version="abc"),
        doc(9, version=-1),
        {"doc_id": "10", "op": "DELETE"},                     # delete needs only doc_id
        doc("", links=[]),
        doc(11, links="12"),                                  # links must be an array
        doc(12, version="3"),                                 # numeric strings are accepted
    ])
    accepted, rejected = ij.validate(ij.read_input(spark, path), phase=0)
    reasons = {r["doc_id"]: r["reason"] for r in rejected.collect()}
    assert reasons == {
        "broken": "malformed_json_or_wrong_type", "3": "missing_url", "4": "invalid_url", "5": "missing_title",
        "6": "missing_text", "7": "unknown_op:RENAME", "8": "invalid_version", "9": "negative_version",
        "": "missing_doc_id", "11": "malformed_json_or_wrong_type"}
    ok = {r["doc_id"]: r for r in accepted.collect()}
    assert set(ok) == {"1", "2", "10", "12"}
    assert ok["2"]["links"] == [] and ok["10"]["is_deleted"] and ok["12"]["version"] == 3


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
    assert links == {("1", "2"), ("1", "3"), ("2", "1"), ("3", "2")}   # missing target and self-link dropped
    assert s0["links_dropped"] == 2

    # Rerunning the same phase changes nothing: no new snapshot, same contents.
    snaps = (current_snapshot(spark, "documents"), current_snapshot(spark, "links"))
    again = ij.run(spark, 0, phase0, "h0")
    assert again["status"] == "no_change" and again["docs_changed"] == 0
    assert (current_snapshot(spark, "documents"), current_snapshot(spark, "links")) == snaps

    # Phase 1: delete 3, update 2, add 4 (which links to the deleted 3 and to 1).
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

    # Re-ingesting Phase 0 after Phase 1 loses on rank (phase 0 < 1): tombstones block re-inserts.
    snap = current_snapshot(spark, "documents")
    assert ij.run(spark, 0, phase0, "h0")["status"] == "no_change"
    assert current_snapshot(spark, "documents") == snap
    live = {r.doc_id for r in ij.live_documents(spark).collect()}
    assert live == {"1", "2", "4"}
    assert spark.table(table("ingest_log")).count() == 4
