"""Operational metadata has a small shared envelope and immutable run-scoped facts."""

from datetime import datetime, timezone
from importlib import metadata as packages
import os
import platform
from pathlib import Path
import subprocess

from .common import canonical, checksum, digest
from .storage import delta_write


def now():
    return datetime.now(timezone.utc).isoformat()


def _git_state(root):
    """Prefer the checkout that supplies the code, or build-time evidence in Docker."""
    try:
        def git(*args):
            return subprocess.check_output(
                ["git", "-C", str(root), *args], stderr=subprocess.DEVNULL, text=True
            ).strip()

        dirty = bool(git("status", "--porcelain", "--untracked-files=normal"))
        try:
            revision = git("rev-parse", "--verify", "HEAD")
        except subprocess.CalledProcessError:
            revision = None
        return revision, dirty
    except (OSError, subprocess.CalledProcessError):
        return os.getenv("MEDLENS_BUILD_REVISION") or None, {
            "clean": False, "dirty": True
        }.get(os.getenv("MEDLENS_BUILD_STATE"))


def build_info(spark):
    # Research.root may point to a different repository containing only dataset
    # contracts. Attribute the build to the package that actually executes.
    package_root = Path(__file__).resolve().parent
    root = package_root.parent.parent
    files = {
        p.relative_to(package_root).as_posix(): checksum(p)
        for p in sorted(package_root.rglob("*.py"))
    }
    inputs = {
        name: checksum(root / name)
        for name in ("requirements.lock", "pyproject.toml", "Dockerfile", "docker/jars.lock")
        if (root / name).is_file()
    }
    revision, dirty = _git_state(root)
    runtime = {
        "python": platform.python_version(),
        "platform": platform.machine(),
        "spark": spark.version,
        "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.runtime.version"),
        "delta": packages.version("delta-spark"),
        "packages": {dist.metadata["Name"]: dist.version for dist in packages.distributions()
                     if dist.metadata["Name"]},
    }
    return {
        **runtime,
        "runtime_hash": digest(runtime),
        "source_hash": digest(files),
        "build_inputs": inputs,
        "logic_hash": digest([files, inputs, runtime]),
        "git_revision": revision,
        "git_dirty": dirty,
        "image_id": os.getenv("MEDLENS_IMAGE_ID") or None,
    }


def event(run_id, stage, status, **detail):
    print(
        canonical(
            dict(time=now(), run_id=run_id, stage=stage, status=status, **detail)
        ),
        flush=True,
    )


def journal(spark, directory, manifest, facts):
    rows = [
        (
            manifest["run_id"],
            manifest["dataset"],
            manifest["delivery"],
            kind,
            key,
            canonical(data),
        )
        for kind, key, data in facts
    ]
    schema = "run_id string, dataset string, delivery string, kind string, object_id string, detail string"
    frame = spark.createDataFrame(rows, schema).coalesce(1)
    return delta_write(
        spark,
        frame,
        directory / "metadata" / "journal",
        "journal",
        manifest["run_id"],
        strategy="append",
    )


def published_execution(spark, directory, run_id):
    """Read the execution at the journal commit that published this run."""
    import json
    from pyspark.sql import functions as F
    from .storage import committed_ref, read

    ref = committed_ref(spark, directory / "metadata" / "journal", "journal", run_id)
    if ref is None:
        return None
    fact = (
        read(spark, ref)
        .where((F.col("run_id") == run_id) & (F.col("kind") == "execution"))
        .select("detail")
        .first()
    )
    if fact is None:
        raise ValueError(f"Missing execution fact for committed run {run_id}")
    return dict(json.loads(fact.detail), metadata=ref)
