# Runbook: from clone to a working search

Four Ubuntu 24.04 machines (VMs or WSL2), joined by ZeroTier.

| Node | Role arg | Runs |
|---|---|---|
| node1 | `ingest` | client only: holds the dataset, runs the ingestion driver |
| node2 | `storage` | HDFS NameNode + DataNode, Spark worker |
| node3 | `compute` | Spark master + worker, HDFS DataNode, drivers for index/PageRank/export, ssh server |
| node4 | `serve` | FastAPI + web page |

Every script is idempotent: re-running it is safe. Run them from the repo root as `bash scripts/<name>.sh`.

## 0. Every node: clone and configure
```bash
git clone <repo-url> bd-search-engine && cd bd-search-engine
cp config/cluster.env.example config/cluster.env
nano config/cluster.env        # ZT_NETWORK_ID now. The NODEx_IP values come after step 2.
```
The values in `cluster.env` must be **identical on all four nodes**. It is gitignored, so share it in your team chat. Set `NODE3_USER` to the output of `whoami` on node3.

**node1 only:** `data/toy/` (`pages.jsonl`, `changes.json`) is in git. Other folders under `data/` are not (too large), so put the course dataset there, or point `PHASE0_FILE`/`PHASE1_FILE` at your copies.

**WSL2 only, once per machine** (from inside WSL):
```bash
sudo tee /etc/wsl.conf >/dev/null <<'EOF'
[boot]
systemd=true
[network]
generateHosts = false
EOF
```
Then run `wsl --shutdown` from Windows PowerShell and reopen Ubuntu. Give WSL at least 4 GB RAM (`%UserProfile%\.wslconfig`: `[wsl2]` `memory=4GB`).

## 1. Every node: install
```bash
bash scripts/01_install.sh <role>     # e.g. storage on node2
source ~/.bigdata_env
```
This installs Java 11, Hadoop 3.3.6, Spark 3.3.4 and the Iceberg 1.4.3 jar (not on node4), plus a Python 3.10 env at `~/.venvs/bdse`. Anything already installed in the right version is skipped. It stops with a clear message if a wrong version is found.

## 2. Every node: ZeroTier
```bash
bash scripts/02_zerotier.sh
```
While it waits, one teammate opens https://my.zerotier.com → the network → **Members** and ticks **Auth** for the new node. The script then prints the node's ZeroTier IP. Put all four IPs into `NODE1_IP`..`NODE4_IP` in `config/cluster.env` **on every node**.

## 3. Every node: hostnames
```bash
bash scripts/03_hosts.sh
getent hosts node1 node2 node3 node4   # must show the ZeroTier IPs, never 127.x
```

## 4. Every node: render configs
```bash
bash scripts/04_configure.sh <role>
```

## 5. ssh from node4 to node3 (node4 pulls exports over rsync+ssh)
On node3: `sudo service ssh start`
On node4:
```bash
ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519   # skip if you already have a key
ssh-copy-id <NODE3_USER>@node3
ssh <NODE3_USER>@node3 true                        # must not ask for a password
```
`fault_demo.sh node2` needs the same setup from node3 to node2.

## 6. Start daemons (in this order)
```bash
# node2
bash scripts/05_start.sh storage    # formats the NameNode only on the very first start
# node3
bash scripts/05_start.sh compute
# node4
bash scripts/05_start.sh serve
```
Check every node: `bash scripts/doctor.sh <role>`. Every line should say PASS (the search API has no index yet).
Web UIs: NameNode http://node2:9870, Spark master http://node3:8080 (2 workers).

## 7. Phase 0 (arrive at the evaluation with this already done)
```bash
# node1
bash scripts/06_ingest.sh 0          # uploads PHASE0_FILE to HDFS, runs the ingestion job on the cluster
# node3
bash scripts/07_compute.sh 0         # index, PageRank, export to ~/bigdata/exports/<version>
# node4
bash scripts/08_pull_and_serve.sh    # rsync, verify, POST /reload
```
Open http://node4:8000/ and search. API: `curl 'http://node4:8000/search?q=computer&k=10'`, `curl http://node4:8000/health`.

## 8. Phase 1 (live during the evaluation)
```bash
bash scripts/06_ingest.sh 1          # node1
bash scripts/07_compute.sh 1         # node3
bash scripts/08_pull_and_serve.sh    # node4: the version in /health changes
```
If any step fails, node4 keeps serving Phase 0. `08` refuses an export that fails its checks, and the server refuses it again on `/reload`.

## 9. Idempotence and Iceberg demos
```bash
bash scripts/admin.sh state          # snapshot id, rows and content hash of documents/links/doc_changes
bash scripts/06_ingest.sh 1          # rerun the same phase: status=no_change
bash scripts/admin.sh state          # identical output
bash scripts/admin.sh snapshots documents
bash scripts/admin.sh changes 1      # added/updated/deleted doc ids from the snapshot diff
bash scripts/admin.sh log            # ingest_log: phase, file hash, snapshot id, timestamp
bash scripts/admin.sh count documents <old_snapshot_id>      # time travel
bash scripts/admin.sh rollback documents <old_snapshot_id>   # recover from a bad update
```
To roll serving back without touching Iceberg: `bash scripts/08_pull_and_serve.sh <old_version>`.

## 10. Fault tolerance (node3)
`bash scripts/fault_demo.sh`. See `docs/FAULT_TOLERANCE.md`.

## Final run on the main corpus
Copy the main dataset to node1 (for example `data/main/`). Set `PHASE0_FILE` and `PHASE1_FILE` in `cluster.env`. For a clean start, stop everything and on node2 wipe HDFS with `hdfs dfs -rm -r -skipTrash /warehouse /bdse`. Then run steps 7 and 8.
**Do this early.** The main corpus may carry a `version` field the toy data lacks. Check the `[ingest] reject reasons` line, and `hdfs dfs -cat /bdse/rejects/phase=0/*.json | head`, to be sure valid records are not being rejected.

## Single-machine rehearsal
`bash scripts/local_demo.sh` runs the whole flow with `LOCAL_MODE=1` (no HDFS, local Spark) under `~/bigdata/tmp/demo`. It needs Spark plus the Iceberg jar and a Python with fastapi, uvicorn and pyarrow (`SERVE_PY=...`).

Tests: `~/.venvs/bdse/bin/python -m pytest -q tests` (Spark tests need `SPARK_HOME`).

## Troubleshooting
| Symptom | Fix |
|---|---|
| `doctor` says `nodeX -> 127.0.1.1` | Re-run `03_hosts.sh`. On WSL set `generateHosts = false` (step 0). |
| Spark worker never shows in the master UI | Check `getent hosts node3` on the worker and port 7077 (`doctor`). See the log in `~/bigdata/logs/spark/`. |
| Job hangs at "Initial job has not accepted any resources" | Executors cannot reach the driver: `spark.driver.host` must be this node's ZeroTier IP (re-run `04_configure.sh <role>`). Check that ports 7079/7080/7090 are not firewalled (`sudo ufw status`). Or the worker memory is lower than `SPARK_EXECUTOR_MEMORY`. |
| HDFS: `could only be written to 0 of the 1 minReplication nodes` | DataNodes are not registered. Check `hdfs dfsadmin -report`, the DataNode logs, and `dfs.datanode.hostname` (node2/node3 must resolve on every node). |
| DataNode dies with `Incompatible clusterIDs` | The NameNode was reformatted. On that DataNode: `bash scripts/05_stop.sh <role>; rm -rf ~/bigdata/hdfs/data; bash scripts/05_start.sh <role>`. |
| NameNode in safe mode | Wait for both DataNodes. `hdfs dfsadmin -safemode wait`. |
| Executor `OutOfMemoryError` / WSL freezes | Lower `SPARK_EXECUTOR_MEMORY` / `SPARK_WORKER_MEMORY`, or raise the WSL memory in `.wslconfig`. Keep worker memory ≥ executor memory + 512m. |
| `Address already in use` | Another process holds the port: `ss -ltnp | grep <port>`. Stop it or change the port in `cluster.env` on all nodes, then re-run `04_configure.sh`. |
| `08` says the server refused the version | `/reload` reports why (checksum, missing `_SUCCESS`). Re-run `07_compute.sh`, then `08`. The old version stays live meanwhile. |
| ZeroTier status `ACCESS_DENIED` | The node is not yet authorized in my.zerotier.com. |
| ZeroTier on WSL never gets an IP | `sudo zerotier-one -d` if systemd is off. Check `sudo zerotier-cli listnetworks`. |
