"""Evaluate named evidence criteria on canonical contracts and pinned quality results."""

from functools import reduce

from pyspark.sql import functions as F

from .quality import quality_frame, record_flags
from .transformation import finite, present

RESULT_SCHEMA = (
    "task string, patient_id string, status string, reasons array<string>, "
    "evidence array<string>, criterion_counts string"
)


def usable(frame, criterion, issues):
    c = criterion
    if c.exclude_rules and "_quality_flags" in frame.columns:
        excluded = F.array(*[F.lit(rule) for rule in c.exclude_rules])
        frame = frame.where(F.size(F.array_intersect("_quality_flags", excluded)) == 0)
    if c.exclude_rules and issues is not None:
        frame = record_flags(frame, issues, c.entity)
    if c.exclude_rules and "_source_flags" in frame.columns:
        excluded = F.array(*[F.lit(rule) for rule in c.exclude_rules])
        frame = frame.where(F.size(F.array_intersect("_source_flags", excluded)) == 0)
    frame = frame.where(F.col("_issue").isNull() & present(F.col(c.patient_field)))
    if c.numeric_field:
        frame = frame.where(finite(F.col(c.numeric_field)))
    if c.date_field:
        frame = frame.where(F.col(c.date_field).isNotNull()).withColumn(
            "_date", F.to_date(c.date_field)
        )
    if c.distinct_field:
        frame = frame.where(present(F.col(c.distinct_field)))
    for key in c.pair_keys:
        frame = frame.where(present(F.col(key)))
    return frame


def measures(frame, criterion, issues):
    c = criterion
    frame = usable(frame, c, issues)
    for field, allowed in c.filter.items():
        frame = frame.where(F.col(field).isin(allowed))
    if c.strategy == "paired_dates":
        keys = [c.patient_field, c.date_field, *c.pair_keys]
        frame = frame.where(F.col(c.pair_field).isin(c.pair_values))
        pairs = frame.groupBy(*keys).agg(
            F.countDistinct(c.pair_field).alias("_components")
        )
        frame = frame.join(
            pairs.where(F.col("_components") == len(c.pair_values)).select(*keys), keys
        )
    if c.strategy in ("paired_dates", "distinct_dates"):
        count = F.countDistinct("_date")
    elif c.strategy == "span_days":
        count = F.datediff(F.max("_date"), F.min("_date"))
    else:
        count = F.countDistinct(c.distinct_field or "_record_id")
    return frame.groupBy(c.patient_field).agg(
        count.alias("n"), F.sort_array(F.collect_set("_record_id")).alias("ids")
    )


def population_counts(frame, key):
    return frame.groupBy(key).count()


def identifiable(key):
    return present(F.col(key)) & (F.col("count") == 1)


def unresolved(frame, c):
    """Count candidate rows only when the other discriminators still match."""
    candidate = F.lit(True)
    unknown = F.lit(False)
    filters = dict(c.filter)
    if c.pair_field:
        filters[c.pair_field] = c.pair_values
    for field, allowed in filters.items():
        matches = F.col(field).isin(allowed)
        if field in c.uncertain_fields:
            missing = ~present(F.col(field))
            unknown = unknown | missing
            matches = matches | missing
        candidate = candidate & matches
    return (
        usable(frame, c, None)
        .where(candidate & unknown)
        .groupBy(c.patient_field)
        .agg(F.sort_array(F.collect_set("_record_id")).alias("unresolved_ids"))
    )


def assess(
    config,
    frames,
    quality_inputs=None,
    cache=lambda frame: frame,
    table_ids=None,
    refs=None,
):
    population = frames[config.population]
    spark = population.sparkSession
    key = config.patient_key
    patients = (
        population_counts(population, key)
        .where(identifiable(key))
        .select(F.col(key).alias("patient_id"))
    )
    issues = quality_frame(frames, quality_inputs, refs)
    policy, aggregates = [], []
    sources = {}
    for task, required in config.tasks.items():
        for position, name in enumerate(required):
            c = config.criteria[name]
            policy.append((task, name, position, c.threshold, c.entity in frames))
    for name in sorted({row[1] for row in policy}):
        c = config.criteria[name]
        if c.entity not in frames:
            continue
        if c.entity not in sources:
            sources[c.entity] = cache(
                record_flags(
                    frames[c.entity], issues, (table_ids or {}).get(c.entity, c.entity)
                )
            )
        frame = sources[c.entity]
        values = measures(frame, c, None)
        if c.uncertain_fields:
            values = values.join(unresolved(frame, c), c.patient_field, "full")
        else:
            values = values.withColumn(
                "unresolved_ids", F.array().cast("array<string>")
            )
        aggregates.append(
            values.select(
                F.col(c.patient_field).alias("patient_id"),
                F.lit(name).alias("criterion"),
                "n",
                "ids",
                "unresolved_ids",
            )
        )
    grid = patients.crossJoin(
        F.broadcast(
            spark.createDataFrame(
                policy,
                "task string, criterion string, position int, threshold int, available boolean",
            )
        )
    )
    schema = "patient_id string, criterion string, n long, ids array<string>, unresolved_ids array<string>"
    measured = (
        reduce(lambda a, b: a.unionByName(b), aggregates)
        if aggregates
        else spark.createDataFrame([], schema)
    )
    rows = grid.join(measured, ["patient_id", "criterion"], "left").fillna({"n": 0})
    reason = (
        F.when(~F.col("available"), F.lit("unavailable:"))
        .when(F.col("n") >= F.col("threshold"), F.lit(None))
        .when(F.size("unresolved_ids") > 0, F.lit("unresolved:"))
        .otherwise(F.lit("insufficient:"))
    )
    rows = rows.withColumn("reason", F.concat(reason, "criterion"))
    result = rows.groupBy("task", "patient_id").agg(
        F.sort_array(
            F.collect_list(
                F.when(F.col("reason").isNotNull(), F.struct("position", "reason"))
            )
        ).alias("ordered"),
        F.sort_array(
            F.array_distinct(
                F.flatten(
                    F.collect_list(
                        F.array_union(
                            F.coalesce("ids", F.array()),
                            F.coalesce("unresolved_ids", F.array()),
                        )
                    )
                )
            )
        ).alias("evidence"),
        F.sort_array(
            F.collect_list(F.when(F.col("available"), F.struct("criterion", "n")))
        ).alias("counts"),
    )
    reasons = F.transform("ordered", lambda r: r.reason)
    return result.withColumn("reasons", reasons).select(
        "task",
        "patient_id",
        F.when(
            F.exists("reasons", lambda r: r.startswith("insufficient:")), "ineligible"
        )
        .when(F.size("reasons") > 0, "indeterminate")
        .otherwise("eligible")
        .alias("status"),
        "reasons",
        "evidence",
        F.to_json(F.map_from_entries("counts")).alias("criterion_counts"),
    )
