# Plan

**Goal:** a mini search engine across 4 nodes: Spark ingestion into Iceberg on HDFS, a Spark inverted index and PageRank, a Parquet export, and FastAPI serving that combines BM25 and PageRank.

## Data (checked in data/toy)
- `pages.jsonl`: JSON lines with `doc_id, url, title, text, links[]`. It has 20 truncated or malformed lines, 20 exact duplicates and 40 records without `links`. No record has a `version` field.
- `changes.json`: JSON lines (not an array) with `op` ∈ {`UPSERT`, `DELETE`}. A DELETE has only `doc_id`. Some UPSERT links point to documents that are deleted in the same file.

## Iceberg tables (`iceberg_catalog.search.*`)
| table | key | purpose |
|---|---|---|
| documents | doc_id | latest row per doc and tombstones (`is_deleted`). Stores `phase` and `version`; rank = (phase, version) |
| links | (src, dst) | edges between live docs only. Recomputed after each merge from `documents.links` |
| doc_changes | append | added/updated/deleted doc ids, from diffing the before and after snapshots |
| ingest_log | append | phase, file hash, counts, snapshot ids, status, timestamp |
| postings | (term, doc_id) | inverted index, updated incrementally per changed doc |
| pagerank | doc_id | replaced on every run |

## Pipeline
1. **Ingest (node1)**: `spark.read.json` in PERMISSIVE mode, then validation with reject reasons, dedup (rank wins, later line breaks ties), then MERGE only rows that would change. If nothing would change, no MERGE runs, so no snapshot is created. Then the links are synced, the snapshots diffed and the log row written.
2. **Compute (node3)**: tokenizer built from Spark built-in functions (same rules as `tokenize.py`). Postings updated via MERGE delete and append. DataFrame PageRank: d=0.85, ≥10 iterations, dangling mass redistributed, reliable checkpoint every k iterations.
3. **Export (node3)**: parquet written to HDFS staging, copied to a local tmp dir, sha256 manifest, `_SUCCESS` written last, then `rename` to `exports/<version>`, then `LATEST` written.
4. **Serve (node4)**: rsync into staging, verify, move, then `POST /reload`. The loader builds every structure before swapping one reference, so on failure the old version stays live.

## Order of work
common → ingest → compute → serve → scripts/templates → checks + local_demo → docs.
All shared shell logic lives in `scripts/lib.sh`, so `local_demo.sh` exercises the same code as scripts 06–08 without running them.
