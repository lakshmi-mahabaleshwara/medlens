"""Build row-preserving rule checks and traceable issue records."""

from pyspark.sql import Window, functions as F

from .common import Registry
from .transformation import finite, present

RULES = Registry()


@RULES.register("required")
def required(r):
    return ~present(F.col(r.field))


@RULES.register("type")
def invalid_type(r):
    return present(F.col(r.field)) & F.col(r.field).try_cast(r.kind).isNull()


@RULES.register("timestamp")
def invalid_time(r):
    return present(F.col(r.field)) & F.col(r.field).try_cast("timestamp").isNull()


@RULES.register("numeric")
def invalid_numeric(r):
    return ~finite(F.col(r.field).try_cast("double"))


@RULES.register("ordered")
def reversed_interval(r):
    return F.col(r.field).try_cast("timestamp") > F.col(r.other).try_cast("timestamp")


@RULES.register("unique")
def duplicate_key(r):
    return present(F.col(r.field)) & (
        F.count("*").over(Window.partitionBy(r.field)) > 1
    )


@RULES.register("duplicate")
def duplicate_payload(r):
    return F.count("*").over(Window.partitionBy("_payload")) > 1


@RULES.register("units")
def unknown_unit(r):
    return ~F.coalesce(F.col(r.field).isin(r.allowed), F.lit(False))


ISSUE_SCHEMA = (
    "rule_id string, rule_version string, severity string, classification string, "
    "field string, message string, source_record_id string, dataset string, "
    "dataset_version string, table_id string, run_id string"
)


def validate(frame, table_id, rules, references, run_id):
    checks = []
    for index, (name, rule) in enumerate(rules.items()):
        if not rule.enabled:
            continue
        if rule.op == "required_column":
            if rule.field not in frame.columns:
                raise ValueError(f"Missing required column {table_id}.{rule.field}")
            continue
        if rule.op == "reference":
            key, count = f"_parent_key_{index}", f"_parent_count_{index}"
            # Keep uniqueness at the parent key grain, even if duplicate
            # parents have different owners. A composite join alone hides that ambiguity.
            owners = {
                child: (parent, f"_parent_match_{index}_{n}")
                for n, (child, parent) in enumerate(rule.match_fields.items())
            }
            parents = references[rule.reference].groupBy(rule.other).agg(
                F.count("*").alias(count),
                *[F.first(parent).alias(alias) for parent, alias in owners.values()],
            ).withColumnRenamed(rule.other, key)
            frame = frame.join(parents, frame[rule.field] == parents[key], "left")
            valid = F.coalesce(F.col(count), F.lit(0)) == 1
            for child, (_, alias) in owners.items():
                valid = valid & present(frame[child]) & F.coalesce(
                    frame[child] == parents[alias], F.lit(False)
                )
            invalid = present(F.col(rule.field)) & ~valid
        else:
            invalid = RULES.resolve(rule.op)(rule)
        for field, value in rule.where.items():
            invalid = invalid & (F.col(field) == value)
        issue = F.struct(
            *[
                F.lit(v).alias(k)
                for k, v in [
                    ("rule_id", name),
                    ("rule_version", rule.version),
                    ("severity", rule.severity),
                    ("classification", rule.classification),
                    ("field", rule.field),
                    ("message", rule.message),
                ]
            ]
        )
        flag = f"_violation_{index}"
        frame = frame.withColumn(flag, invalid)
        checks.append(F.when(F.col(flag), issue))
    parse = F.struct(
        F.lit("malformed_record").alias("rule_id"),
        F.lit("1").alias("rule_version"),
        F.lit("error").alias("severity"),
        F.lit("confirmed_violation").alias("classification"),
        F.lit("_issue").alias("field"),
        F.col("_issue").alias("message"),
    )
    checks.append(F.when(F.col("_issue").isNotNull(), parse))
    return frame.select(
        "_record_id",
        "_dataset",
        "_version",
        F.explode(F.filter(F.array(*checks), lambda x: x.isNotNull())).alias("issue"),
    ).select(
        "issue.*",
        F.col("_record_id").alias("source_record_id"),
        F.col("_dataset").alias("dataset"),
        F.col("_version").alias("dataset_version"),
        F.lit(table_id).alias("table_id"),
        F.lit(run_id).alias("run_id"),
    )
