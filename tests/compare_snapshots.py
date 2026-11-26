"""Compare logical multisets from two full published runs, including duplicate multiplicity."""

import argparse
import json
from pathlib import Path

from medlens.config import resolve
from medlens.runtime import session
from medlens.storage import read


def compare(spark, left, right):
    assert left["status"] == right["status"] == "success"
    assert left["dataset"] == right["dataset"] and left["delivery"] == right["delivery"]
    assert left["tables"].keys() == right["tables"].keys()
    verified = {}
    for name, ref in left["tables"].items():
        other = right["tables"][name]
        assert ref["stats"] == other["stats"], name
        assert ref["schema"] == other["schema"], name
        if ref["format"] == "csv":
            assert ref["files"] == other["files"], name
        else:
            a = read(spark, ref).drop("run_id")
            b = read(spark, other).drop("run_id")
            assert not a.exceptAll(b).take(1) and not b.exceptAll(a).take(1), name
        verified[name] = ref["stats"]["rows"]

    def graph(manifest):
        return {
            (step["stage"], tuple(sorted(step["inputs"])))
            for step in manifest["stages"]
        }

    assert graph(left) == graph(right)
    return verified


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("manifests", nargs=2, type=Path)
    paths = parser.parse_args().manifests
    a, b = (json.loads(p.read_text()) for p in paths)
    spark = session(resolve(a["dataset"])[0].spark)
    try:
        print(json.dumps(compare(spark, a, b), sort_keys=True))
    finally:
        spark.stop()
