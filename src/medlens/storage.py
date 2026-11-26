"""Version-pinned file and Delta bindings. Callers serialize writes per workspace."""

import json
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from .common import Registry, canonical, digest
from .ingestion import csv_frame

READERS = Registry()
READERS.register("csv")(csv_frame)


@READERS.register("delta")
def read_delta(spark, ref):
    if "physical_id" in ref:
        actual = DeltaTable.forPath(spark, ref["path"]).detail().select("id").first()[0]
        if actual != ref["physical_id"]:
            raise ValueError(
                f"Registered Delta table identity changed at {ref['path']}"
            )
    return (
        spark.read.format("delta")
        .option("versionAsOf", ref["version"])
        .load(ref["path"])
    )


def read(spark, ref):
    return READERS.resolve(ref["format"])(spark, ref)


def enforce(actual, expected, evolve=False):
    old = {f.name: f.dataType for f in expected}
    new = {f.name: f.dataType for f in actual}
    if any(new.get(k) != v for k, v in old.items()):
        raise ValueError("Breaking schema change")
    if new.keys() != old.keys() and not evolve:
        raise ValueError("Undeclared schema evolution")
    if any(not f.nullable and f.name not in old for f in actual):
        raise ValueError("New columns must be nullable")


def committed_ref(spark, path, table_id, batch, schema_version="1", mutable=True):
    """Recover a batch at its original commit, even after later publications."""
    path = str(Path(path).resolve())
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    table = DeltaTable.forPath(spark, path)
    metadata = canonical(
        {"identity": batch, "schema_version": schema_version, "table": table_id}
    )
    matches = (
        table.history()
        .where(F.col("userMetadata").isin(metadata, batch))
        .select("version")
        .take(1)
    )
    if not matches:
        return None
    return _reference(
        path,
        table_id,
        schema_version,
        batch,
        matches[0].version,
        table.detail().select("id").first()[0],
        mutable,
    )


def _reference(path, table_id, schema_version, batch, version, physical_id, mutable):
    ref = dict(
        id=table_id,
        format="delta",
        path=path,
        version=version,
        physical_id=physical_id,
        identity=batch,
        schema_version=schema_version,
    )
    if mutable:
        previous = (
            dict(path=path, physical_id=physical_id, version=version - 1)
            if version
            else None
        )
        ref.update(
            batch_identity=batch,
            predecessor=previous,
            identity=digest([batch, physical_id, previous]),
        )
    return ref


def delta_write(
    spark,
    frame,
    path,
    table_id,
    identity,
    *,
    strategy="snapshot",
    keys=(),
    evolve=False,
    schema_version="1",
    partitions=(),
):
    path = str(Path(path).resolve())
    exists = DeltaTable.isDeltaTable(spark, path)
    if strategy not in {"append", "snapshot", "merge"}:
        raise ValueError(f"Unknown write strategy {strategy}")
    metadata = canonical(
        {"identity": identity, "schema_version": schema_version, "table": table_id}
    )

    if exists:
        table = DeltaTable.forPath(spark, path)
        detail = table.detail().select("id", "partitionColumns").first()
        if list(partitions) != detail.partitionColumns:
            raise ValueError("Partition layout changed")
        history = (
            table.history()
            .where(F.col("userMetadata").isin(metadata, identity))
            .select("version")
            .take(1)
        )
        if history:
            return _reference(
                path,
                table_id,
                schema_version,
                identity,
                history[0][0],
                detail.id,
                strategy != "snapshot",
            )
        if strategy == "snapshot":
            raise ValueError(
                "Immutable snapshot already exists with another write identity"
            )
        existing = table.toDF()
        enforce(frame.schema, existing.schema, evolve)
        if set(frame.columns) != set(existing.columns):
            previous_write = table.history(1).first().userMetadata or "{}"
            prior_version = json.loads(previous_write)["schema_version"]
            if prior_version == schema_version:
                raise ValueError(
                    "Additive schema evolution requires a new schema version"
                )
    if strategy == "merge":
        if not keys or any(k not in frame.columns for k in keys):
            raise ValueError("Merge requires declared keys")
        for key in keys:
            if frame.where(F.col(key).isNull()).take(1):
                raise ValueError("Null merge key")
        if frame.groupBy(*keys).count().where("count > 1").take(1):
            raise ValueError("Ambiguous merge keys")
    if exists and strategy == "merge":
        condition = " AND ".join(f"target.`{k}` = incoming.`{k}`" for k in keys)
        builder = table.alias("target").merge(frame.alias("incoming"), condition)
        if evolve:
            builder = builder.withSchemaEvolution()
        setting = "spark.databricks.delta.commitInfo.userMetadata"
        previous = spark.conf.get(setting, None)
        spark.conf.set(setting, metadata)
        try:
            builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        finally:
            if previous is None:
                spark.conf.unset(setting)
            else:
                spark.conf.set(setting, previous)
    else:
        writer = (
            frame.write.format("delta")
            .mode("append" if exists else "error")
            .option("userMetadata", metadata)
            .option("mergeSchema", str(evolve).lower())
        )
        if not exists:
            writer = (
                writer.option("delta.enableExpiredLogCleanup", "false")
                .option("delta.logRetentionDuration", "interval 36500 days")
                .option("delta.deletedFileRetentionDuration", "interval 36500 days")
            )
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.save(path)
    table = DeltaTable.forPath(spark, path)
    return _reference(
        path,
        table_id,
        schema_version,
        identity,
        table.history(1).first()["version"],
        table.detail().select("id").first()[0],
        strategy != "snapshot",
    )
