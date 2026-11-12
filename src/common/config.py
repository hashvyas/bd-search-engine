import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def env(name, default=None):
    value = os.environ.get(name, "")
    return value if value != "" else default


LOCAL_MODE = env("LOCAL_MODE", "0") == "1"
CATALOG = "iceberg_catalog"
NAMESPACE = env("ICEBERG_NAMESPACE", "search")
DATA_ROOT = env("DATA_ROOT", "hdfs://node2:9000/bdse").rstrip("/")
EXPORT_DIR = env("EXPORT_DIR", str(Path.home() / "bigdata" / "exports"))
SERVING_DIR = env("SERVING_DIR", str(Path.home() / "bigdata" / "serving"))

BM25_K1 = float(env("BM25_K1", "1.2"))
BM25_B = float(env("BM25_B", "0.75"))
W_BM25 = 0.7
W_PAGERANK = 0.3
DAMPING = float(env("PAGERANK_DAMPING", "0.85"))
PAGERANK_ITERATIONS = int(env("PAGERANK_ITERATIONS", "20"))
PAGERANK_CHECKPOINT_EVERY = int(env("PAGERANK_CHECKPOINT_EVERY", "3"))
MIN_PAGERANK_ITERATIONS = 10


def table(name):
    return f"{CATALOG}.{NAMESPACE}.{name}"


def add_repo_to_path():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
