"""Stable identities and atomic local manifests."""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock


def canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def checksum(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf8") as out:
        out.write(canonical(value))
        out.flush()
        os.fsync(out.fileno())
    os.replace(out.name, path)


class Registry(dict):
    """Trusted operators only. Registration collisions fail at import."""

    def register(self, name):
        def install(function):
            if name in self:
                raise ValueError(f"Duplicate strategy {name}")
            self[name] = function
            return function

        return install

    def resolve(self, name):
        if name not in self:
            raise ValueError(f"Unknown strategy {name}")
        return self[name]


_process_writer = Lock()


@contextmanager
def writer_lock(directory: Path):
    """Serialize threads and processes, including across Docker Desktop bind mounts."""
    directory = directory.resolve()
    root = Path(os.environ.get("MEDLENS_LOCK_DIR", directory))
    root.mkdir(parents=True, exist_ok=True)
    path = root / ("writer-" + digest(str(directory)) + ".lock")
    with _process_writer, path.open("a+b") as lock:
        lock.seek(0)
        fcntl.lockf(lock, fcntl.LOCK_EX)
        try:
            yield path
        finally:
            fcntl.lockf(lock, fcntl.LOCK_UN)
