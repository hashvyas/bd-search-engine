# Assumptions, decisions and spec gaps

The PDF (`docs/Distributed_Search_Engine.pdf`) is the source of truth. Where it is silent, the decision is listed here.

The spec PDF is not in the repo; teammates get it from the course site.

## Conflicts with or gaps in the PDF
1. **Phase 1 file name.** The PDF says `changes.jsonl`. The toy dataset ships `changes.json`, and it is JSON lines, not an array. The ingestion job looks at the first character and accepts both formats. `PHASE1_FILE` in `cluster.env` sets the path.
2. **No `version` field in the data.** The PDF says "highest version wins", but neither toy file has a `version`. Decision: `version` is optional. It is stored as 0 with `has_version = false` when absent.
   - It may be an integer or a timestamp string; a timestamp becomes epoch seconds.
   - Within one file, the highest version wins and the **later line** breaks ties.
   - Against the stored row: if **both rows carry an explicit version, the higher version wins** (the later phase breaks ties), so a stale Phase 1 update loses.
   - Otherwise **the later phase wins**, then the higher version. A versionless Phase 1 DELETE therefore always applies.
3. **Operations in Phase 1.** Only `UPSERT` and `DELETE` occur. `INSERT` and `UPDATE` are accepted as aliases of `UPSERT`. A missing `op` (all of Phase 0) means `UPSERT`. Any other op is rejected (`unknown_op:<OP>`).
4. **DELETE → tombstone.** A delete keeps the row with `is_deleted = true` and drops its payload. Without tombstones, re-running Phase 0 after Phase 1 would re-insert deleted docs and break "ingesting twice changes nothing". A delete for an unknown doc still writes a tombstone, which blocks a later lower-ranked insert.
5. **Required fields.** `doc_id` (non-blank) always. For an UPSERT also `url` (must match `^https?://`), `title` (non-blank) and `text` (non-null; may be empty). `links` is optional, because 40 toy records lack it, and defaults to `[]`. Blank link entries are dropped. Malformed JSON and wrong structural types (for example `links` as a string) are rejected as `malformed_json_or_wrong_type`. Numbers given where a string is expected are accepted as text (Spark JSON behaviour). `version` must cast to a non-negative integer.
6. **Link cleanup** runs against the final live doc set *after* the merge. Toy Phase 1 adds pages that link to docs deleted in the same file. Self-links are dropped too. `documents.links` keeps the raw list, and the `links` table holds only valid edges, so an edge comes back if its target is re-added. Dropped links go to the rejects file with reason `link_target_missing` or `self_link`.
7. **Rejects location:** `DATA_ROOT/rejects/phase=<p>/` (JSON: doc_id, reason, raw, phase). It is overwritten on each run of that phase, so reruns stay idempotent. Superseded duplicates are written there too (`duplicate_superseded`).
8. **Idempotence scope.** A rerun commits no new snapshot to **any** table: `documents`, `links`, `doc_changes` or `ingest_log`.
   - The source is pre-filtered to rows that would win, and the MERGE is skipped when that set is empty.
   - A no-change rerun is only printed (`status: no_change`); `ingest_log` records applied runs only.
   - The rejects files (plain JSON, not Iceberg) are rewritten with identical content.
   - `scripts/admin.sh state` fingerprints all four tables.
9. **Change detection.** After a merge, the job diffs the `documents` snapshot before and after the merge (time travel, `VERSION AS OF`) and appends `added/updated/deleted` rows to `doc_changes`. The indexer keeps its own incremental state: each posting stores the doc's `(phase, version)`, and only docs whose rank changed are re-indexed. This also heals itself after a rollback.
10. **Normalization.** As in the PDF: `0.7 * bm25 / max_bm25 + 0.3 * pagerank / max_pagerank`. `max_bm25` is taken over the docs that match the query. `max_pagerank` is global, over all live docs. Only docs with at least one query term are returned.
11. **Serving directory.** The PDF example is `/serving/vN/`. We use `$BDSE_HOME/serving/<version>` (default `~/bigdata/serving`), so no root access is needed. Version names are `v<phase>-<documents snapshot>-<pagerank snapshot>`. Every compute run (`07_compute.sh`) creates a new export version, because the pagerank table gets a new snapshot each time.
13. **`/reload` auth.** Off by default (lab network). If `RELOAD_TOKEN` is set in `cluster.env`, `/reload` requires a matching `X-Reload-Token` header, and the scripts send it. `/search` and `/health` stay open.
14. **`NODE3_USER`** has no default (WSL usernames differ). `NODE3_EXPORT_DIR` defaults to `/home/$NODE3_USER/bigdata/exports`.
12. **PDF overlay.** The tech-stack table on page 5 is cut off by the page overlay. The system summary in the task prompt was used as the fallback.

## Deviations from the course material
- **Iceberg 1.4.3 instead of 1.8.1** (guide). 1.4.3 is already on the reference machine's Spark classpath, and two runtime jars would conflict. It supports MERGE INTO, snapshots, time travel, `rollback_to_snapshot` and the Hadoop catalog on Spark 3.3. Pinned in `config/versions.env`.
- **The jar lives in `$SPARK_HOME/jars`, not `/opt/iceberg` + `spark.jars`** (guide). Every node, including workers, then loads the same jar, and `spark.jars` stays unset.
- **The guide's catalog pattern is followed**: catalog `iceberg_catalog`, `type=hadoop`, warehouse `hdfs://node2:9000/warehouse`. The settings are passed with `--properties-file`. Because `--properties-file` replaces `spark-defaults.conf`, `lib.sh` concatenates the rendered `spark-defaults.conf` and `iceberg.conf` into `job.properties`.
- **Configs are rendered into `~/bigdata/conf/{hadoop,spark}`** (`HADOOP_CONF_DIR`, `SPARK_CONF_DIR`). `$SPARK_HOME/conf` and `$HADOOP_HOME/etc/hadoop` are never modified, so other labs on the same machine are unaffected.
- **bm25_snippet.py bug.** It reads `row["length"]` from the index rows, which have no length column. We take `doc_len` from `docs.parquet`. Otherwise the snippet's IDF `ln(1 + (N-df+0.5)/(df+0.5))`, `avgdl = max(mean, 1)` and the per-occurrence handling of query terms are kept.

## Design decisions
- **Tokenizer:** lowercases ASCII A–Z only, splits on `[^a-z0-9]+`, drops tokens shorter than 2 characters and a small stopword list. Non-ASCII letters act as separators ("Café" → "caf", "Zürich" → "rich"). Accented queries therefore look odd, but they match the index exactly. This guarantees exact parity between `tokenize.py` (server) and the Spark built-in functions (indexer). There is no Python UDF, so executors need no project code and `--py-files` is not needed. The title is indexed together with the text. Wikitext markup is not stripped, so tokens like `thumb` and `px` exist. That is acceptable for this scale.
- **PageRank:** DataFrames. Dangling mass is spread evenly (ranks sum to 1). Default 20 iterations; fewer than 10 raises an error. Reliable checkpoint to `DATA_ROOT/checkpoints` every 3 iterations. The final ranks replace the `pagerank` table (`createOrReplace`, which keeps Iceberg history).
- **Export:** Spark writes single-file Parquet to HDFS staging. The driver copies it to node3's local disk and writes `stats.json`, `manifest.json` (sha256 of every file, row counts, Iceberg snapshot ids) and `_SUCCESS` last, then `rename()`s the directory and updates `LATEST`. Every compute run produces a new version directory, because the pagerank snapshot is part of the name.
- **Serving swap:** the loader verifies `_SUCCESS`, the manifest, the checksums and the row counts, builds every structure, then assigns one reference. `CURRENT` is written only after a successful swap, and a restart reloads it. `/reload` returns 409 and keeps the old version on any failure.
- **Spark binding:** HDFS daemons bind `0.0.0.0` and advertise `nodeX` (`dfs.datanode.hostname`, `use.datanode.hostname`). Spark master and workers bind to the node's ZeroTier IP (`SPARK_LOCAL_IP`), because Spark standalone advertises its bind address and `0.0.0.0` would be unreachable. Drivers bind `0.0.0.0`, advertise `spark.driver.host = NODE_IP` and use fixed driver and block-manager ports.
- **Hostname trap:** `03_hosts.sh` comments out `127.x` lines that name `node1`–`node4`. Otherwise daemons bind to loopback.
- **Python:** PySpark 3.3.4 fails on Python 3.12 (verified: `PicklingError` from the bundled cloudpickle). `01_install.sh` runs that test and builds a uv-managed **Python 3.10** env on every node (`PYTHON_VERSION=3.10`, so the minor version is the same everywhere).
- **ZeroTier install:** uses the official installer, which sets up ZeroTier's GPG-signed apt repository. No separate checksum is available for that script. Hadoop and Spark (sha512), Iceberg (sha1) and uv (sha256) are verified.
- **fault_demo.sh** runs on node3 and by default kills node3's own worker and DataNode, so it needs no ssh. `fault_demo.sh node2` kills the remote worker over ssh.
- **`ICEBERG_NAMESPACE`** defaults to `search`, so tables are `iceberg_catalog.search.*`.

## Verified on the reference machine (WSL, LOCAL_MODE)
`scripts/local_demo.sh` passed end to end on data/toy.
- Phase 0: 1040 lines read, 20 rejected (malformed), 20 duplicates, 1000 live docs.
- Phase 1: 50 deleted, 25 added, 25 updated, leaving 975 live docs. 7 links to deleted docs were dropped.
- Rerunning Phase 0 left snapshot ids, row counts and content hashes unchanged.
- A corrupt export was refused by the pull step and by `/reload` (HTTP 409), and the old version stayed live.

A scratch `spark-submit` check (pytest is not installed here) reproduced the assertions of `test_ingest.py`, `test_pagerank.py` (3-node cycle) and the tokenizer parity test, and they all match. That covers validation reasons, timestamp versions, dedup, link cleanup, the snapshot diff, idempotent reruns, the stale-version rule and the PageRank cycle. A teammate reviewer ran the pure-Python tests (BM25, loader, tokenizer) on Python 3.10, and they pass. The pytest suite itself has never been run with Spark.

Cluster scripts 01–08 and `fault_demo.sh` were checked with `bash -n` only.

## MISSING ON THIS MACHINE (the reference WSL used to build the repo)
- **pytest** is not installed for any Python (3.12, 3.10 or `~/venv`). Per the rules it was not installed, so **the test suite was not run here**. Teammates get it from `requirements.txt`.
- **fastapi, uvicorn and pyarrow are missing from system Python 3.12 and 3.10.** An already existing venv `~/venv` (Python 3.12) has them. `local_demo.sh` was run with `SERVE_PY=~/venv/bin/python`. Nothing was installed.
- **PySpark on Python 3.12 fails.** The existing `/usr/bin/python3.10` was used as `SPARK_PY` for the local demo.
- **shellcheck** is not installed. Scripts were checked with `bash -n` only.
- No `~/.venvs/bdse` (not created here, by rule). On teammates' nodes `01_install.sh` creates it.
