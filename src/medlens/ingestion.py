"""CSV occurrence identity is independent of partition order and Spark execution."""

import csv
from pathlib import Path

from pyspark.sql.types import LongType, StringType, StructField, StructType

from .common import checksum, digest

PROVENANCE = [
    "_record_id",
    "_payload",
    "_file",
    "_checksum",
    "_ordinal",
    "_offset",
    "_length",
    "_issue",
    "_dataset",
    "_version",
    "_table",
]


def inventory(root, spec):
    base = Path(root).resolve()
    files = {}
    for pattern in spec.files:
        matches = sorted(base.glob(pattern))
        if not matches:
            raise FileNotFoundError(str(base / pattern))
        for path in matches:
            relative = str(path.resolve().relative_to(base))
            if relative in files:
                raise ValueError(f"Overlapping input patterns for {relative}")
            files[relative] = {
                "name": relative,
                "sha256": checksum(path),
                "bytes": path.stat().st_size,
            }
    return sorted(files.values(), key=lambda item: item["name"])


def records(root, entry, spec, dataset, version, table):
    path = Path(root) / entry["name"]
    if checksum(path) != entry["sha256"]:
        raise ValueError(f"Source checksum changed for {path}")
    with path.open("rb") as stream:

        def lines():
            while raw := stream.readline():
                yield raw.decode("utf-8-sig" if stream.tell() == len(raw) else "utf8")

        reader = csv.reader(lines(), strict=True, delimiter=spec.delimiter)
        if next(reader, None) != spec.columns:
            raise ValueError(
                f"Header mismatch for {entry['name']}. Expected {spec.columns}"
            )
        ordinal = 0
        while True:
            start = stream.tell()
            try:
                row = next(reader)
            except StopIteration:
                break
            except (UnicodeDecodeError, csv.Error) as exc:
                raise ValueError(
                    f"Unrecoverable CSV at {entry['name']} bytes {start}..{entry['bytes']}"
                ) from exc
            ordinal += 1
            issue = (
                None
                if len(row) == len(spec.columns)
                else f"Expected {len(spec.columns)} fields, got {len(row)}"
            )
            values = [None if v == spec.null_token else v for v in row]
            fields = (values + [None] * len(spec.columns))[: len(spec.columns)]
            rid = digest(
                [dataset, version, table, entry["name"], entry["sha256"], ordinal]
            )
            yield (
                *fields,
                rid,
                digest(row),
                entry["name"],
                entry["sha256"],
                ordinal,
                start,
                stream.tell() - start,
                issue,
                dataset,
                version,
                table,
            )


def csv_frame(spark, ref):
    from .config import Source

    spec = Source.model_validate(ref["reader"])
    schema = StructType(
        [
            StructField(
                c,
                LongType() if c in ("_ordinal", "_offset", "_length") else StringType(),
            )
            for c in spec.columns + PROVENANCE
        ]
    )

    def read_file(entry):
        return records(
            ref["path"], entry, spec, ref["dataset"], ref["delivery"], ref["id"]
        )

    rdd = spark.sparkContext.parallelize(ref["files"], len(ref["files"])).flatMap(
        read_file
    )
    return spark.createDataFrame(rdd, schema)
