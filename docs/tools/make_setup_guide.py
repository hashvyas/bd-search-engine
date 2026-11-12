"""Builds docs/Setup_Guide.pdf with the standard library only. Edit the content below, then run:
    python3 docs/tools/make_setup_guide.py docs/Setup_Guide.pdf
Keep it in sync with docs/RUNBOOK.md."""
import sys
import textwrap

W, H, M = 595, 842, 50          # A4 points, margin
STYLE = {  # font, size, leading, wrap chars
    "title": ("F2", 20, 26, 50), "h1": ("F2", 14, 20, 70), "h2": ("F2", 11.5, 16, 85),
    "p": ("F1", 10, 13.5, 100), "li": ("F1", 10, 13.5, 95), "code": ("F3", 8.6, 11, 100),
    "small": ("F1", 8.5, 11, 115),
}

C = []
def T(s): C.append(("title", s))
def H1(s): C.append(("h1", s))
def H2(s): C.append(("h2", s))
def P(s): C.append(("p", s))
def LI(*items): C.extend(("li", s) for s in items)
def CODE(s): C.append(("code", textwrap.dedent(s).strip("\n")))
def SP(): C.append(("sp", ""))
def BR(): C.append(("page", ""))

T("Distributed Search Engine - Team Setup Guide")
C.append(("small", "Big Data 2026 (UE24CS343AB2). For Ubuntu 24.04 VMs or WSL2. Repo: bd-search-engine. Full details: docs/RUNBOOK.md"))
SP()
H1("1. Who runs what")
CODE("""
Node   Role arg   Runs                                               Teammate
node1  ingest     client only: dataset, ingestion driver             ________
node2  storage    HDFS NameNode + DataNode, Spark worker             ________
node3  compute    Spark master + worker, DataNode, compute drivers   ________
node4  serve      FastAPI search service + web page                  ________
""")
P("Every script is idempotent: re-running it is safe. Always run from the repo root as: bash scripts/<name>.sh. "
  "Minimum per machine: 2 CPU cores, 4 GB RAM (node2/node3), 10 GB free disk.")
H1("2. Before you start (every machine)")
LI("- Ubuntu 24.04 (VM or WSL2) with sudo rights and internet access.",
   "- A ZeroTier account. ONE teammate creates the network at my.zerotier.com and shares the 16-character Network ID.",
   "- Agree on ONE config/cluster.env for the team. It is gitignored, so share it in your team chat. The values must be identical on all four nodes.")
H2("WSL2 only (once per Windows machine)")
P("Inside Ubuntu (WSL) run:")
CODE("""
sudo tee /etc/wsl.conf >/dev/null <<'EOF'
[boot]
systemd=true
[network]
generateHosts = false
EOF
""")
P("Then in Windows PowerShell: wsl --shutdown, and reopen Ubuntu. Give WSL enough memory: create "
  "%UserProfile%\\.wslconfig with the two lines [wsl2] and memory=4GB. Without generateHosts = false, WSL "
  "rewrites /etc/hosts on every start and the node names stop resolving.")
H1("3. Step by step (do each step on all four nodes before the next)")
H2("Step 0 - Clone and create the config")
CODE("""
git clone <repo-url> bd-search-engine && cd bd-search-engine
cp config/cluster.env.example config/cluster.env
nano config/cluster.env        # set ZT_NETWORK_ID now; node IPs after step 2
""")
P("node1 only: data/ is not in git. Download the course dataset and put the files in data/toy/ (pages.jsonl, changes.json), "
  "or point PHASE0_FILE / PHASE1_FILE in cluster.env at wherever you saved them.")
H2("Step 1 - Install (5-15 min)")
CODE("""
bash scripts/01_install.sh <role>      # ingest | storage | compute | serve
source ~/.bigdata_env
""")
P("Installs Java 11, Hadoop 3.3.6, Spark 3.3.4 and the Iceberg 1.4.3 jar (not on node4), plus a Python 3.10 env in "
  "~/.venvs/bdse. Every download is checksum-verified. Tools already present in the right version are skipped. A wrong "
  "version stops the script with a message telling you what to fix.")
H2("Step 2 - Join ZeroTier")
CODE("bash scripts/02_zerotier.sh")
P("While it waits: the network owner opens my.zerotier.com -> Networks -> your network -> Members, and ticks 'Auth' for the "
  "new member. The script prints this node's ZeroTier IP. Collect all four IPs and fill NODE1_IP .. NODE4_IP in "
  "config/cluster.env on EVERY node. Also set NODE3_USER to the output of 'whoami' on node3 (WSL usernames differ; node4's "
  "rsync fails if it is wrong). Optional: set RELOAD_TOKEN to any secret so only your scripts can switch versions.")
H2("Step 3 - Hostnames")
CODE("""
bash scripts/03_hosts.sh
getent hosts node1 node2 node3 node4    # must show ZeroTier IPs, never 127.x
""")
H2("Step 4 - Render configs")
CODE("bash scripts/04_configure.sh <role>")
P("Configs go to ~/bigdata/conf. Nothing in $SPARK_HOME/conf or $HADOOP_HOME/etc/hadoop is changed.")
H2("Step 5 - ssh from node4 to node3 (for pulling exports)")
CODE("""
# node3:
sudo service ssh start
# node4:
ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519     # skip if a key exists
ssh-copy-id <NODE3_USER>@node3
ssh <NODE3_USER>@node3 true                          # must not ask for a password
""")
H2("Step 6 - Start daemons (in this order)")
CODE("""
node2:  bash scripts/05_start.sh storage     # formats HDFS only the first time
node3:  bash scripts/05_start.sh compute
node4:  bash scripts/05_start.sh serve
every node:  bash scripts/doctor.sh <role>   # each line should say PASS
""")
P("Web UIs: HDFS http://node2:9870 (2 live DataNodes) and Spark http://node3:8080 (2 alive workers). Stop with "
  "bash scripts/05_stop.sh <role>. After a reboot, run 05_start.sh again (no systemd is used).")
H1("4. Running the pipeline")
H2("Phase 0 - must already be serving when you arrive at the evaluation")
CODE("""
node1:  bash scripts/06_ingest.sh 0        # upload pages.jsonl to HDFS + ingestion job
node3:  bash scripts/07_compute.sh 0       # inverted index + PageRank + Parquet export
node4:  bash scripts/08_pull_and_serve.sh  # pull, verify, switch version
""")
P("Search: open http://node4:8000/ or run curl 'http://node4:8000/search?q=computer&k=10'. Check the live version with "
  "curl http://node4:8000/health")
H2("Phase 1 - triggered live during the evaluation")
CODE("""
node1:  bash scripts/06_ingest.sh 1
node3:  bash scripts/07_compute.sh 1
node4:  bash scripts/08_pull_and_serve.sh  # /health shows the new version
""")
P("If any step fails, node4 keeps serving Phase 0: an export without _SUCCESS or with a bad checksum is refused.")
H2("Idempotency test and Iceberg time travel (any Spark node, e.g. node1)")
CODE("""
bash scripts/admin.sh state            # snapshot ids + row counts + content hashes
bash scripts/06_ingest.sh 1            # same phase again -> status no_change
bash scripts/admin.sh state            # identical output = idempotent
bash scripts/admin.sh snapshots documents
bash scripts/admin.sh changes 1        # added / updated / deleted doc ids
bash scripts/admin.sh count documents <snapshot_id>      # time travel
bash scripts/admin.sh rollback documents <snapshot_id>   # undo a bad update
""")
H2("Fault tolerance demo (node3)")
CODE("bash scripts/fault_demo.sh        # kills a Spark worker mid-PageRank, then stops a DataNode")
P("Expected: 'PASS: same PageRank digest after losing a worker' and 'PASS: file read intact with one DataNode down'. "
  "See docs/FAULT_TOLERANCE.md for the settings behind this.")
H2("Final run on the main corpus")
P("Copy the main dataset to node1 (e.g. data/main/) and set PHASE0_FILE / PHASE1_FILE in cluster.env. For a clean start, "
  "on node2 run: hdfs dfs -rm -r -skipTrash /warehouse /bdse. Then run Phase 0 again. Do this EARLY: the main corpus may "
  "carry a version field the toy data lacks. Read the '[ingest] reject reasons' line and the rejects files "
  "(hdfs dfs -cat /bdse/rejects/phase=0/*.json | head) to be sure good records are not being rejected.")
H1("5. Rehearse on one machine first (optional)")
CODE("bash scripts/local_demo.sh       # needs Spark, the Iceberg jar and the 3.10 env")
P("This runs Phase 0, Phase 1, the idempotence check and the bad-export check locally under ~/bigdata/tmp/demo. "
  "Unit tests: ~/.venvs/bdse/bin/python -m pytest -q tests")
H1("6. Troubleshooting")
for sym, fix in [
    ("doctor shows nodeX -> 127.0.1.1", "Re-run 03_hosts.sh. On WSL set generateHosts = false (section 2)."),
    ("Worker missing in Spark UI", "On the worker: getent hosts node3, then doctor.sh <role> (port 7077). Logs: ~/bigdata/logs/spark/"),
    ("Job stuck: 'Initial job has not accepted any resources'",
     "Executors cannot reach the driver. Re-run 04_configure.sh <role> on the driver node. Open ports 7079/7080/7090 (sudo ufw status). Worker memory must be >= executor memory."),
    ("HDFS: 'could only be written to 0 ... nodes'", "DataNodes not registered: hdfs dfsadmin -report, check DataNode logs in ~/bigdata/logs/hadoop/."),
    ("DataNode: 'Incompatible clusterIDs'", "NameNode was reformatted. On that DataNode: 05_stop.sh <role>; rm -rf ~/bigdata/hdfs/data; 05_start.sh <role>."),
    ("Out of memory / WSL freezes", "Lower SPARK_EXECUTOR_MEMORY and SPARK_WORKER_MEMORY in cluster.env (all nodes), re-run 04_configure.sh, restart."),
    ("'Address already in use'", "ss -ltnp | grep <port>. Stop the other process or change the port in cluster.env on all nodes."),
    ("ZeroTier ACCESS_DENIED", "Not authorized yet: tick 'Auth' for the member in my.zerotier.com."),
    ("08 says server refused the version", "The reason (checksum / missing _SUCCESS) is printed. Re-run 07_compute.sh, then 08. The old version stays live."),
]:
    C.append(("h2", sym))
    C.append(("p", fix))


def esc(s):
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)").encode("latin-1", "replace").decode("latin-1")


def layout():
    pages, ops, y = [], [], H - M
    def newpage():
        nonlocal ops, y
        if ops:
            pages.append(ops)
        ops, y = [], H - M
    for kind, text in C:
        if kind == "page":
            newpage(); continue
        if kind == "sp":
            y -= 8; continue
        font, size, lead, wrap = STYLE[kind]
        if kind == "code":
            lines = text.split("\n")
        else:
            lines = textwrap.wrap(text, wrap, subsequent_indent="  " if kind == "li" else "") or [""]
        before = {"title": 0, "h1": 12, "h2": 6, "code": 2, "p": 2, "li": 0, "small": 0}[kind]
        block = before + lead * len(lines) + (6 if kind == "code" else 0)
        if kind in ("h1", "h2"):
            block += 40  # keep a heading with what follows
        if y - block < M and block < H - 2 * M:
            newpage()
        y -= before
        if kind == "code":
            top = y - 2
            ops.append(f"0.94 0.95 0.97 rg {M - 4} {top - lead * len(lines) - 6:.1f} {W - 2 * M + 8} {lead * len(lines) + 6:.1f} re f 0 g")
            y -= 3
        if kind == "h1":
            ops.append(f"0.2 0.35 0.6 RG 0.8 w {M} {y - 5:.1f} m {W - M} {y - 5:.1f} l S")
        for line in lines:
            y -= lead
            if y < M:
                newpage(); y -= lead
            color = "0.15 0.3 0.55 rg" if kind in ("title", "h1") else "0 g"
            ops.append(f"BT {color} /{font} {size} Tf {M} {y:.1f} Td ({esc(line)}) Tj ET")
        if kind == "code":
            y -= 6
    newpage()
    return pages


def write(path):
    pages = layout()
    objs = ["<< /Type /Catalog /Pages 2 0 R >>", None,
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Courier /Encoding /WinAnsiEncoding >>"]
    kids = []
    for i, ops in enumerate(pages, 1):
        ops = ops + [f"BT 0.5 g /F1 8 Tf {W - M - 60} 25 Td (Page {i} of {len(pages)}) Tj ET"]
        stream = "\n".join(ops).encode("latin-1")
        objs.append(f"<< /Length {len(stream)} >>\nstream\n".encode("latin-1") + stream + b"\nendstream")
        content_id = len(objs)
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {W} {H}] /Contents {content_id} 0 R "
                    "/Resources << /Font << /F1 3 0 R /F2 4 0 R /F3 5 0 R >> >> >>")
        kids.append(len(objs))
    objs[1] = f"<< /Type /Pages /Kids [{' '.join(f'{k} 0 R' for k in kids)}] /Count {len(kids)} >>"
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for n, o in enumerate(objs, 1):
        offsets.append(len(out))
        out += f"{n} 0 obj\n".encode() + (o if isinstance(o, bytes) else o.encode("latin-1")) + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    out += b"".join(f"{o:010d} 00000 n \n".encode() for o in offsets)
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    open(path, "wb").write(out)
    print(f"wrote {path}: {len(pages)} pages, {len(out)} bytes")


write(sys.argv[1])
