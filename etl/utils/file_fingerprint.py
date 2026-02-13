import hashlib
from pathlib import Path
from datetime import datetime


def compute_file_hash(path: Path) -> str:
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_file_metadata(path: Path):
    stat = path.stat()
    return {
        "file_name": path.name,
        "file_hash": compute_file_hash(path),
        "file_modified_ts": datetime.fromtimestamp(stat.st_mtime)
    }
