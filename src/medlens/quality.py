"""Bind quality evidence by its issue contract and join it at record scope."""

from functools import reduce

from pyspark.sql import functions as F


SCOPE_FIELDS = {
    "dataset": "dataset",
    "dataset_version": "delivery",
    "table_id": "validated_table",
}


def quality_frame(inputs, names=None, refs=None):
    required = {"rule_id", "source_record_id"}
    names = (
        names
        if names is not None
        else [k for k, v in inputs.items() if required <= set(v.columns)]
    )
    frames = []
    scopes = set()
    for name in names:
        frame = inputs[name]
        if not required <= set(frame.columns):
            raise ValueError(
                f"Quality input {name} requires rule_id and source_record_id"
            )
        ref = (refs or {}).get(name, {})
        scope = {}
        for column, key in SCOPE_FIELDS.items():
            value = ref.get(key)
            if column in frame.columns:
                fallback = (
                    F.lit(value)
                    if value is not None
                    else F.raise_error(
                        F.lit(f"Missing {column} scope in quality input {name}")
                    )
                )
                scope[column] = F.coalesce(F.col(column), fallback)
            elif value is not None:
                scope[column] = F.lit(value)
        if scope:
            frame = frame.withColumns(scope)
        scopes.add(frozenset(SCOPE_FIELDS).intersection(frame.columns))
        frames.append(frame)
    if len(scopes) > 1:
        raise ValueError(
            "Incompatible quality scopes: supply dataset, delivery and validated table provenance before combining issue inputs"
        )
    return (
        reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
        if frames
        else None
    )


def record_flags(frame, issues, table_id):
    if issues is None:
        return frame.withColumn("_source_flags", F.array().cast("array<string>"))
    keys = ["_record_id"]
    for target, origin in [("_dataset", "dataset"), ("_version", "dataset_version")]:
        if origin in issues.columns:
            if target not in frame.columns:
                raise ValueError(f"Cannot apply scoped issues without {target}")
            issues = issues.withColumnRenamed(origin, target)
            keys.append(target)
    scoped = "table_id" in issues.columns
    value = F.struct("rule_id", "table_id") if scoped else F.col("rule_id")
    flags = (
        issues.withColumnRenamed("source_record_id", "_record_id")
        .groupBy(*keys)
        .agg(F.collect_set(value).alias("_source_flags"))
    )
    joined = frame.join(flags, keys, "left")
    if scoped:
        source_table = F.col("_table") if "_table" in frame.columns else F.lit(table_id)
        matches = F.filter(
            "_source_flags",
            lambda x: (x.table_id == table_id) | (x.table_id == source_table),
        )
        joined = joined.withColumn(
            "_source_flags", F.transform(matches, lambda x: x.rule_id)
        )
    return joined.select(
        *frame.columns, F.coalesce("_source_flags", F.array()).alias("_source_flags")
    )
