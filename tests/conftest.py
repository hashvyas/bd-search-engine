import glob
import os
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
WORK = Path(tempfile.mkdtemp(prefix="bdse-tests-"))
os.environ["DATA_ROOT"] = f"file://{WORK}/dfs"
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
for p in [f"{spark_home}/python", *glob.glob(f"{spark_home}/python/lib/py4j-*-src.zip")]:
    if p not in sys.path:
        sys.path.append(p)

TABLES = ["documents", "links", "doc_changes", "ingest_log", "postings", "pagerank"]


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("bdse-tests")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
         .config("spark.sql.catalog.iceberg_catalog.warehouse", f"file://{WORK}/warehouse")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    s.sparkContext.setCheckpointDir(f"file://{WORK}/checkpoints")
    yield s
    s.stop()


@pytest.fixture
def clean(spark):
    from src.common.config import table
    for t in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table(t)} PURGE")
    return spark


@pytest.fixture
def jsonl(tmp_path):
    import json
    counter = iter(range(1000))

    def write(lines):
        p = tmp_path / f"input{next(counter)}.jsonl"
        p.write_text("\n".join(x if isinstance(x, str) else json.dumps(x) for x in lines) + "\n")
        return f"file://{p}"
    return write
