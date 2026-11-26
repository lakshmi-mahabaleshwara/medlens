"""Small orchestration helpers for fresh Jupyter kernels."""

import os
import json

from .common import write_json
from .pipeline import Research


def workspace():
    return Research(
        os.getenv("MEDLENS_DATASET"),
        os.getenv("MEDLENS_ENVIRONMENT", "local"),
        os.getenv("MEDLENS_EXPERIMENT") or None,
        json.loads(os.getenv("MEDLENS_OVERRIDES", "{}")),
    )


def notebook_run(research, section):
    c = research.config
    producers = {s.output: s.op for s in c.stages.values()}
    groups = {
        "ingestion": {"ingest"},
        "profiling": {"profile", "validate"},
        "standardization": {"map", "validate"},
        "readiness": {"readiness"},
    }
    stages = []
    for name, stage in c.stages.items():
        if not stage.enabled or stage.op not in groups[section]:
            continue
        if stage.op == "validate":
            is_mapped = producers[stage.inputs[0]] == "map"
            if is_mapped != (section == "standardization"):
                continue
        stages.append(name)
    location = research.output / "notebooks" / c.dataset / research.config_hash
    previous = {
        "profiling": "ingestion",
        "standardization": "profiling",
        "readiness": "standardization",
    }
    parent = os.getenv("MEDLENS_PARENT") or None
    if parent is None and section in previous:
        path = location / (previous[section] + ".json")
        if path.exists():
            parent = json.loads(path.read_text())["run_id"]
    result = research.run(stages, parent)
    write_json(location / (section + ".json"), {"run_id": result["run_id"]})
    return result
