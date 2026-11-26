"""Compile finite declarative mappings into typed Spark expressions."""

from pyspark.sql import functions as F

from .common import Registry
from .ingestion import PROVENANCE

OPERATORS = Registry()
REACTIONS = "array<struct<code:string,description:string,severity:string>>"


def present(col):
    return col.isNotNull() & (F.trim(col.cast("string")) != "")


def finite(col):
    return col.isNotNull() & ~F.isnan(col) & (F.abs(col) != F.lit(float("inf")))


@OPERATORS.register("field")
def field(e):
    return F.col(e.field)


@OPERATORS.register("cast")
def cast(e):
    return F.col(e.field).try_cast(e.kind)


@OPERATORS.register("timestamp")
def timestamp(e):
    return (
        F.try_to_timestamp(F.col(e.field), F.lit(e.format))
        if e.format
        else F.col(e.field).try_cast("timestamp")
    )


@OPERATORS.register("precision")
def precision(e):
    col = F.col(e.field)
    return F.when(
        present(col),
        F.when(col.rlike(r"^\d{4}-\d{2}-\d{2}$"), "date").otherwise("datetime"),
    )


@OPERATORS.register("literal")
def literal(e):
    return F.lit(e.value)


@OPERATORS.register("namespace")
def namespace(e):
    col = F.col(e.field)
    return F.when(
        present(col), F.to_json(F.array(F.col("_dataset"), F.lit(e.namespace), col))
    )


@OPERATORS.register("normalize")
def normalize(e):
    return F.lower(F.trim(F.col(e.field)))


@OPERATORS.register("values")
def value_map(e):
    pairs = [F.lit(item) for pair in e.values.items() for item in pair]
    return F.element_at(F.create_map(*pairs), F.col(e.field))


@OPERATORS.register("when")
def conditional(e):
    return F.when(F.col(e.predicate) == e.equals, OPERATORS.resolve(e.then.op)(e.then))


@OPERATORS.register("reactions")
def reactions(e):
    groups = [
        F.struct(
            *[F.col(group[k]).alias(k) for k in ("code", "description", "severity")]
        )
        for group in e.groups
    ]
    return F.filter(
        F.array(*groups),
        lambda x: present(x.code) | present(x.description) | present(x.severity),
    )


def standardize(frame, mapping, entity, lookups):
    columns = []
    flags = []
    for name in entity.field_order:
        expr = mapping.fields[name]
        if expr.op == "lookup":
            lookup = lookups[expr.lookup].select(
                F.col(expr.key).alias("_lookup_key"),
                F.col(expr.result).alias("_lookup_" + name),
            )
            if lookup.groupBy("_lookup_key").count().where("count > 1").take(1):
                raise ValueError(f"Non-unique lookup {expr.lookup}")
            frame = frame.join(
                lookup, frame[expr.field] == lookup["_lookup_key"], "left"
            ).drop("_lookup_key")
            value = F.col("_lookup_" + name)
        else:
            value = OPERATORS.resolve(expr.op)(expr)
        if expr.op in ("cast", "timestamp", "lookup", "values"):
            source = F.col(expr.field)
            flags.append(
                F.when(present(source) & value.isNull(), F.lit("unmapped:" + name))
            )
        if expr.op == "when" and expr.then.op == "cast":
            applicable = F.col(expr.predicate) == expr.equals
            invalid = ~finite(value) if expr.then.kind == "double" else value.isNull()
            flags.append(F.when(applicable & invalid, F.lit("invalid:" + name)))
        kind = entity.fields[name]
        columns.append(
            value.cast(REACTIONS if kind == "reactions" else kind).alias(name)
        )
    quality = F.filter(F.array(*flags).cast("array<string>"), lambda x: x.isNotNull())
    return frame.select(
        *columns,
        *PROVENANCE,
        quality.alias("_mapping_flags"),
        F.lit(mapping.version).alias("_mapping_version"),
    )
