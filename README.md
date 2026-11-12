# Distributed Search Engine (Big Data 2026, UE24CS343AB2)

A mini search engine over Simple English Wikipedia, spread across four Ubuntu 24.04 machines joined by ZeroTier:

```
node1 ingest ──spark-submit──▶ node3 Spark master ──tasks──▶ workers on node2 + node3
   │ upload                         │ index, PageRank, export          │
   ▼                                ▼                                  ▼
HDFS (NameNode node2, DataNodes node2+node3, replication 2) ── Iceberg tables (Hadoop catalog)
                                    │ Parquet export (node3 local disk, atomic rename)
                                    ▼
node4 FastAPI: rsync, verify, swap in memory, BM25 x 0.7 + PageRank x 0.3, web page
```

- **Ingestion** (`src/ingest/ingest_job.py`): validates records with reject reasons, deduplicates (highest version wins), drops links to missing docs, and uses `MERGE INTO` Iceberg with tombstones for deletes. Rerunning a phase changes nothing. Changed doc ids come from diffing snapshots.
- **Compute** (`src/compute/`): incremental inverted index, DataFrame PageRank (d=0.85, ≥10 iterations, dangling mass handled, HDFS checkpoints), and a checksummed Parquet export.
- **Serving** (`src/serve/`): `GET /search?q=&k=10`, `GET /health`, `POST /reload?version=`, a web page at `/`. A bad export never replaces the live version.

## Quick start
- Cluster setup, step by step per node: **[docs/RUNBOOK.md](docs/RUNBOOK.md)**. The same steps are in [docs/Setup_Guide.pdf](docs/Setup_Guide.pdf).
- One-machine rehearsal: `bash scripts/local_demo.sh`
- Fault-tolerance demo: [docs/FAULT_TOLERANCE.md](docs/FAULT_TOLERANCE.md)
- Decisions, spec gaps and versions: [docs/ASSUMPTIONS.md](docs/ASSUMPTIONS.md)
- The spec PDF is not in the repo; teammates get it from the course site.

## Layout
```
config/    versions.env (pins), cluster.env.example, templates/ (rendered with envsubst)
scripts/   01_install 02_zerotier 03_hosts 04_configure 05_start/05_stop 06_ingest 07_compute
           08_pull_and_serve doctor admin fault_demo local_demo lib (shared logic)
src/       common/ (config, schema, tokenize, manifest)  ingest/  compute/  serve/
tests/     pytest: validation, dedup, links, idempotent merge, snapshot diff, PageRank, BM25,
           tokenizer parity, atomic swap
```
Stack: Java 11, Hadoop 3.3.6, Spark 3.3.4 (Scala 2.12), Iceberg 1.4.3, Python 3.10, FastAPI.
