"""Deterministic metrics at table, column, value and patient grains."""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .transformation import finite

PROFILE_SCHEMA = (
    "table_id string, metric string, field string, category string, value string"
)


def profile(frame, table_id, policy, roles, types, numeric_when=None):
    metrics = set(policy)
    numeric_when = numeric_when or {}
    columns = [f for f in frame.schema if not f.name.startswith("_")]
    rows, expressions, labels = [], [], []

    def add(metric, field, expression):
        labels.append((metric, field))
        expressions.append(expression.alias(f"m{len(labels)}"))

    if "rows" in metrics:
        add("rows", None, F.count("*"))
    for field in columns:
        name, col = field.name, F.col(field.name)
        if "columns" in metrics:
            rows.append((table_id, "type", name, None, field.dataType.simpleString()))
        if "missing" in metrics:
            add("nulls", name, F.count(F.when(col.isNull(), 1)))
            if isinstance(field.dataType, StringType):
                add("empty", name, F.count(F.when(col == "", 1)))
        if "distinct" in metrics:
            add("distinct", name, F.countDistinct(col))
        if "distribution" in metrics:
            kind = types.get(name, field.dataType.simpleString())
            if kind in ("double", "long", "bigint") or name in numeric_when:
                number = col.try_cast("double")
                applicable = F.lit(True)
                for condition, expected in numeric_when.get(name, {}).items():
                    applicable = applicable & (F.col(condition) == expected)
                add(
                    "numeric_parse_failures",
                    name,
                    F.count(F.when(applicable & ~finite(number), 1)),
                )
                number = F.when(
                    applicable & finite(number), number.try_cast("decimal(38,12)")
                )
                for metric, fn in [
                    ("min", F.min),
                    ("max", F.max),
                    ("mean", F.avg),
                    ("numeric_count", F.count),
                ]:
                    add(metric, name, fn(number))
                add("median", name, F.median(number))
            if kind in ("date", "timestamp"):
                parsed = col.try_cast(kind)
                add("earliest", name, F.min(parsed))
                add("latest", name, F.max(parsed))
    for role in ("entity", "patient"):
        if role + "_counts" in metrics and role in roles:
            add(
                role + "_count",
                roles[role],
                F.countDistinct(F.nullif(F.col(roles[role]), F.lit(""))),
            )
    if expressions:
        values = frame.agg(*expressions).first()
        rows.extend(
            (table_id, metric, field, None, None if val is None else str(val))
            for (metric, field), val in zip(labels, values)
        )
    if "duplicates" in metrics:
        counts = frame.groupBy("_payload").count().where("count > 1")
        stats = counts.agg(F.count("*"), F.sum(F.col("count") - 1)).first()
        rows.extend(
            [
                (table_id, "duplicate_groups", None, None, str(stats[0])),
                (table_id, "excess_occurrences", None, None, str(stats[1] or 0)),
            ]
        )
    spark = frame.sparkSession
    result = spark.createDataFrame(rows, PROFILE_SCHEMA)

    def frequencies(grouped, metric, field, category):
        return grouped.select(
            F.lit(table_id),
            F.lit(metric),
            F.lit(field),
            F.col(category).cast("string"),
            F.col("count").cast("string"),
        )

    if "frequency" in metrics:
        for role in ("code", "unit"):
            if role in roles:
                field = roles[role]
                result = result.union(
                    frequencies(frame.groupBy(field).count(), "frequency", field, field)
                )
    if "patient_distribution" in metrics and "patient" in roles:
        field = roles["patient"]
        grouped = (
            frame.where(F.col(field).isNotNull() & (F.col(field) != ""))
            .groupBy(field)
            .count()
        )
        counts = (
            grouped.withColumnRenamed("count", "records").groupBy("records").count()
        )
        result = result.union(
            frequencies(counts, "patient_distribution", field, "records")
        )
    return result
