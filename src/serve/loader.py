import os
import re
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from src.common.manifest import verify_export

VERSION_RE = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass
class Index:
    version: str
    phase: int
    manifest: dict
    n_docs: int
    avgdl: float
    doc_ids: list
    urls: list
    titles: list
    doc_len: list
    pagerank: list
    max_pagerank: float
    postings: dict
    loaded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def load_index(export_dir):
    manifest = verify_export(export_dir)
    d = Path(export_dir)
    docs = pq.read_table(d / "docs.parquet").to_pydict()
    idx = pq.read_table(d / "index.parquet").to_pydict()
    if len(docs["doc_id"]) != manifest["row_counts"]["docs"] or len(idx["term"]) != manifest["row_counts"]["index"]:
        raise ValueError(f"{d}: row counts do not match manifest")
    pos = {doc_id: i for i, doc_id in enumerate(docs["doc_id"])}
    postings = {}
    for term, doc_id, tf in zip(idx["term"], idx["doc_id"], idx["tf"]):
        if doc_id not in pos:
            raise ValueError(f"{d}: index references unknown doc {doc_id}")
        postings.setdefault(term, []).append((pos[doc_id], tf))
    stats = manifest["stats"]
    return Index(
        version=manifest["version"], phase=manifest["phase"], manifest=manifest,
        n_docs=int(stats["N"]), avgdl=float(stats["avgdl"]),
        doc_ids=docs["doc_id"], urls=docs["url"], titles=docs["title"],
        doc_len=docs["doc_len"], pagerank=docs["pagerank"],
        max_pagerank=max(docs["pagerank"], default=0.0), postings=postings)


class Holder:

    def __init__(self, serving_dir):
        self.serving_dir = Path(serving_dir)
        self.current = None
        self._lock = threading.Lock()

    def reload(self, version):
        if not VERSION_RE.match(version or ""):
            raise ValueError(f"invalid version name: {version!r}")
        with self._lock:
            new = load_index(self.serving_dir / version)
            self.current = new
            tmp = self.serving_dir / ".CURRENT.tmp"
            tmp.write_text(version + "\n")
            os.replace(tmp, self.serving_dir / "CURRENT")
            return new

    def load_current(self):
        pointer = self.serving_dir / "CURRENT"
        if pointer.is_file():
            return self.reload(pointer.read_text().strip())
        return None


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "verify":
        sys.exit("usage: python -m src.serve.loader verify <export_dir>")
    try:
        m = load_index(sys.argv[2]).manifest
    except Exception as e:
        sys.exit(f"VERIFY FAILED: {e}")
    print(f"VERIFY OK version={m['version']} phase={m['phase']} rows={m['row_counts']}")
