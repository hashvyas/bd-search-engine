"""FastAPI search service (node4). Reads only the Parquet export. No Spark, HDFS or Iceberg.

Run: SERVING_DIR=... uvicorn src.serve.app:app --host 0.0.0.0 --port 8000
"""
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from src.common import config
from src.common.tokenize import tokenize
from src.serve.bm25 import rank
from src.serve.loader import Holder

log = logging.getLogger("uvicorn.error")
holder = Holder(config.SERVING_DIR)
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_app):
    try:
        idx = holder.load_current()
        log.info("loaded %s", idx.version if idx else "nothing (no CURRENT yet)")
    except Exception as e:  # noqa: BLE001 - start empty rather than crash
        log.error("could not load CURRENT export: %s", e)
    yield


app = FastAPI(title="Mini search engine", lifespan=lifespan)


@app.get("/health")
def health():
    idx = holder.current
    if idx is None:
        return {"status": "empty", "version": None}
    return {"status": "ok", "version": idx.version, "phase": idx.phase, "docs": idx.n_docs,
            "terms": len(idx.postings), "loaded_at": idx.loaded_at,
            "iceberg_snapshots": idx.manifest.get("iceberg_snapshots")}


@app.get("/search")
def search(q: str = Query(..., min_length=1, max_length=500), k: int = Query(10, ge=1, le=100)):
    idx = holder.current  # one reference read: a concurrent reload cannot mix versions
    if idx is None:
        raise HTTPException(503, "no index loaded")
    s = idx.manifest["stats"]
    results = rank(tokenize(q), idx, k, s["k1"], s["b"], s["w_bm25"], s["w_pagerank"])
    return {"version": idx.version, "query": q, "terms": tokenize(q), "count": len(results), "results": results}


@app.post("/reload")
def reload(version: str):
    old = holder.current.version if holder.current else None
    try:
        idx = holder.reload(version)
    except Exception as e:  # noqa: BLE001 - any failure keeps the old version
        log.error("reload of %s failed, keeping %s: %s", version, old, e)
        raise HTTPException(409, {"error": str(e), "live_version": old})
    log.info("switched %s -> %s", old, idx.version)
    return {"status": "ok", "previous": old, "version": idx.version}


@app.get("/")
def index_page():
    return FileResponse(STATIC / "index.html")
