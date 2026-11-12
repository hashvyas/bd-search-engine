"""Export manifest: write it on node3, verify it on node4. Pure Python."""
import hashlib
import json
import os
from pathlib import Path

MANIFEST = "manifest.json"
SUCCESS = "_SUCCESS"
REQUIRED_FILES = ("index.parquet", "docs.parquet", "stats.json")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fsync_write(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())


def finalize_export(tmp_dir, meta):
    """Write stats.json, the manifest with checksums, and _SUCCESS last.

    meta must hold version, phase, row_counts and stats. The caller then renames tmp_dir.
    """
    tmp = Path(tmp_dir)
    _fsync_write(tmp / "stats.json", json.dumps(meta["stats"], indent=2, sort_keys=True))
    manifest = dict(meta, files={name: sha256_file(tmp / name) for name in REQUIRED_FILES})
    _fsync_write(tmp / MANIFEST, json.dumps(manifest, indent=2, sort_keys=True))
    _fsync_write(tmp / SUCCESS, "")
    return manifest


def verify_export(export_dir):
    """Return the manifest. Raise ValueError if the export is incomplete or corrupt."""
    d = Path(export_dir)
    if not (d / SUCCESS).is_file():
        raise ValueError(f"{d}: missing {SUCCESS} (export did not finish)")
    try:
        manifest = json.loads((d / MANIFEST).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ValueError(f"{d}: unreadable {MANIFEST}: {e}") from e
    for key in ("version", "phase", "row_counts", "files", "stats"):
        if key not in manifest:
            raise ValueError(f"{d}: manifest missing '{key}'")
    for name in REQUIRED_FILES:
        expected = manifest["files"].get(name)
        if not (d / name).is_file():
            raise ValueError(f"{d}: missing {name}")
        if expected != sha256_file(d / name):
            raise ValueError(f"{d}: checksum mismatch for {name}")
    return manifest
