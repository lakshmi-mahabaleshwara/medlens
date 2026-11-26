import pytest
from pyspark.sql import functions as F

from medlens.config import resolve, Rule, Expression, Entity, Mapping
from medlens.runtime import session
from medlens.storage import delta_write, read
from medlens.validation import validate
from medlens.transformation import standardize
from medlens.ingestion import PROVENANCE


@pytest.fixture(scope="module")
def spark():
    s = session(resolve("clinic")[0].spark)
    yield s
    s.stop()


def test_storage_versions(spark, tmp_path):
    path = tmp_path / "managed"
    data = spark.createDataFrame([(1, "first")], "k long, v string")
    first = delta_write(spark, data, path, "t", "a", strategy="append")
    assert delta_write(spark, data, path, "t", "a", strategy="append") == first
    newer = data.withColumn("added", F.lit("value"))
    with pytest.raises(ValueError, match="evolution"):
        delta_write(spark, newer, path, "t", "b", strategy="append")
    newer = newer.select(
        "k", "v", F.when(F.col("k") == 1, F.lit("value")).alias("added")
    )
    second = delta_write(
        spark, newer, path, "t", "b", strategy="append", evolve=True, schema_version="2"
    )
    assert read(spark, first).count() == 1 and read(spark, second).count() == 2
    merge_path = tmp_path / "merge"
    delta_write(spark, data, merge_path, "u", "base", strategy="merge", keys=["k"])
    with pytest.raises(ValueError, match="Ambiguous"):
        delta_write(
            spark,
            data.union(data),
            merge_path,
            "u",
            "bad",
            strategy="merge",
            keys=["k"],
        )
    merged = delta_write(
        spark,
        data.withColumn("v", F.lit("next")),
        merge_path,
        "u",
        "up",
        strategy="merge",
        keys=["k"],
    )
    assert read(spark, merged).first().v == "next"
    with pytest.raises(ValueError, match="snapshot"):
        delta_write(spark, data, merge_path, "u", "other")


def test_rule_registry(spark):
    frame = spark.createDataFrame(
        [("id1", None, "x", "invalid"), ("id2", "", "x", "2020-01-01")],
        "_record_id string, v string, _payload string, stamp string",
    )
    frame = (
        frame.withColumn("_dataset", F.lit("clinic"))
        .withColumn("_version", F.lit("v1"))
        .withColumn("_issue", F.lit(None).cast("string"))
    )
    rules = {
        k: Rule(op=k, field=f, message=k)
        for k, f in [("duplicate", "v"), ("required", "v"), ("timestamp", "stamp")]
    }
    rows = validate(frame, "events", rules, {}, "run").collect()
    assert sorted(r.rule_id for r in rows) == [
        "duplicate",
        "duplicate",
        "required",
        "required",
        "timestamp",
    ]
    assert {r.source_record_id for r in rows} == {"id1", "id2"}


def test_lookup(spark):
    frame = spark.createDataFrame([("a",), ("b",)], "src string")
    for col in PROVENANCE:
        frame = frame.withColumn(col, F.lit(None).cast("string"))
    lookup = spark.createDataFrame([("a", "known")], "k string, v string")
    expr = Expression(
        op="lookup", field="src", lookup="dictionary", key="k", result="v"
    )
    mapping = Mapping(
        source="events", entity="concept", version="1", fields={"name": expr}
    )
    entity = Entity(version="1", fields={"name": "string"}, key="name")
    rows = standardize(frame, mapping, entity, {"dictionary": lookup}).collect()
    assert {r.name for r in rows} == {"known", None}
    with pytest.raises(ValueError, match="Non-unique"):
        standardize(frame, mapping, entity, {"dictionary": lookup.union(lookup)})


def test_namespaces_and_controlled_mappings(spark):
    entity = Entity(
        version="1",
        fields={"identity": "string", "normalized": "string"},
        key="identity",
    )
    mapping = Mapping(
        source="events",
        entity="concept",
        version="1",
        fields={
            "identity": Expression(op="namespace", field="src", namespace="patient"),
            "normalized": Expression(op="normalize", field="src"),
        },
    )
    frame = spark.createDataFrame(
        [(" 01 ", "clinic"), (" 01 ", "other")], "src string, _dataset string"
    )
    for col in set(PROVENANCE) - {"_dataset"}:
        frame = frame.withColumn(col, F.lit(None).cast("string"))
    result = standardize(frame, mapping, entity, {}).collect()
    assert len({r.identity for r in result}) == 2
    assert {r.normalized for r in result} == {"01"}


def test_distinct_dates_and_paired_evidence(spark):
    from medlens.readiness import measures
    from medlens.config import Criterion

    data = [
        ("a", "2020-01-01 08:00", "v1", "s", "1"),
        ("a", "2020-01-01 08:00", "v1", "d", "2"),
        ("a", "2020-02-01 08:00", "v2", "s", "3"),
        ("a", "2020-02-01 09:00", "v2", "d", "4"),
        ("a", "2020-03-01 10:00", "v3", "s", "5"),
        ("a", "2020-03-01 10:00", "v3", "d", "6"),
    ]
    frame = spark.createDataFrame(
        data,
        "patient string, time string, encounter string, code string, _record_id string",
    )
    frame = frame.withColumn("time", F.col("time").cast("timestamp")).withColumn(
        "_issue", F.lit(None)
    )
    c = Criterion(
        entity="observation",
        patient_field="patient",
        strategy="paired_dates",
        threshold=2,
        date_field="time",
        pair_field="code",
        pair_values=["s", "d"],
        pair_keys=["encounter"],
    )
    row = measures(frame, c, None).first()
    assert row.n == 2 and row.ids == ["1", "2", "5", "6"]


def test_mutable_result_includes_prior_target(spark, tmp_path):
    batch = spark.createDataFrame([(2,)], "id long")
    for strategy in ("append", "merge"):
        refs = []
        for value in (10, 20):
            path = tmp_path / strategy / str(value)
            initial = delta_write(
                spark,
                spark.createDataFrame([(value,)], batch.schema),
                path,
                "events",
                "initial",
                strategy=strategy,
                keys=["id"],
            )
            result = delta_write(
                spark, batch, path, "events", "common", strategy=strategy, keys=["id"]
            )
            assert result["predecessor"] == {
                k: initial[k] for k in ("path", "physical_id", "version")
            }
            assert sorted(r.id for r in read(spark, result).collect()) == [2, value]
            delta_write(
                spark, batch, path, "events", "later", strategy=strategy, keys=["id"]
            )
            assert (
                delta_write(
                    spark,
                    batch,
                    path,
                    "events",
                    "common",
                    strategy=strategy,
                    keys=["id"],
                )
                == result
            )
            refs.append(result)
        assert refs[0]["identity"] != refs[1]["identity"]


def test_quality_aliases_and_uncertainty(spark):
    from types import SimpleNamespace
    from medlens.config import Criterion
    from medlens.readiness import assess
    from medlens.quality import quality_frame, record_flags

    pop = spark.createDataFrame(
        [(k,) for k in ("u", "v", "w", " ", "d", "d")], "id string"
    )
    events = (
        spark.createDataFrame(
            [
                ("u", "one", None, "pct"),
                ("v", "two", "tested", "pct"),
                ("w", "three", None, "unrelated"),
            ],
            "id string, _record_id string, concept string, unit string",
        )
        .withColumn("_issue", F.lit(None))
        .withColumn("_table", F.lit("labs"))
    )
    quality = spark.createDataFrame(
        [("two", "reject", "labs")],
        "source_record_id string, rule_id string, table_id string",
    )
    extra = spark.createDataFrame(
        [("two", "secondary", "labs"), ("one", "reject", "elsewhere")], quality.schema
    )
    inputs = {"patient": pop, "labs": events, "renamed_issues": quality, "extra": extra}
    flags = record_flags(
        events, quality_frame(inputs, ["renamed_issues", "extra"]), "labs"
    )
    assert {r.id: set(r._source_flags) for r in flags.collect()} == {
        "u": set(),
        "v": {"secondary", "reject"},
        "w": set(),
    }
    criterion = Criterion(
        entity="labs",
        patient_field="id",
        strategy="count",
        threshold=1,
        filter={"unit": ["pct"], "concept": ["tested"]},
        uncertain_fields=["concept"],
        exclude_rules=["reject"],
    )
    cfg = SimpleNamespace(
        population="patient",
        patient_key="id",
        criteria={"c": criterion},
        tasks={"study": ["c"]},
    )
    result = {
        r.patient_id: (r.status, r.reasons, r.evidence)
        for r in assess(cfg, inputs).collect()
    }
    assert result == {
        "u": ("indeterminate", ["unresolved:c"], ["one"]),
        "v": ("ineligible", ["insufficient:c"], []),
        "w": ("ineligible", ["insufficient:c"], []),
    }
    inputs["direct"] = inputs.pop("renamed_issues")
    assert {
        r.patient_id: (r.status, r.reasons, r.evidence)
        for r in assess(cfg, inputs, ["direct", "extra"]).collect()
    } == result


def test_resolved_config_replays_column_order(spark):
    from medlens.common import canonical
    from medlens.config import Config

    cfg, _, _ = resolve("clinic")
    replay = Config.model_validate_json(canonical(cfg.model_dump()))
    source = spark.createDataFrame(
        [], ", ".join(f"{k} string" for k in cfg.sources["labs"].columns + PROVENANCE)
    )
    mapping = cfg.mappings["observation"]
    mapping.fields = dict(reversed(list(mapping.fields.items())))
    original = standardize(source, mapping, cfg.entities["observation"], {})
    restored = standardize(
        source, replay.mappings["observation"], replay.entities["observation"], {}
    )
    assert original.schema == restored.schema


def test_mixed_quality_scopes_preserve_exclusion_and_delivery(spark):
    from medlens.quality import quality_frame, record_flags

    events = spark.createDataFrame(
        [
            ("r", "clinic", "first", "labs"),
            ("r", "clinic", "second", "labs"),
            ("r", "other", "first", "labs"),
            ("r", "clinic", "first", "visits"),
        ],
        "_record_id string, _dataset string, _version string, _table string",
    )
    sparse = spark.createDataFrame(
        [("r", "exclude")], "source_record_id string, rule_id string"
    )
    scoped = spark.createDataFrame(
        [("other", "unrelated", "clinic", "first", "labs")],
        "source_record_id string, rule_id string, dataset string, dataset_version string, table_id string",
    )
    bindings = {
        "sparse": {"dataset": "clinic", "delivery": "first", "validated_table": "labs"}
    }

    def flagged(inputs):
        issues = quality_frame(inputs, refs=bindings)
        return [
            (row._dataset, row._version, row._table)
            for row in record_flags(events, issues, "canonical").collect()
            if "exclude" in row._source_flags
        ]

    assert (
        flagged({"sparse": sparse})
        == flagged({"full": scoped, "sparse": sparse})
        == [("clinic", "first", "labs")]
    )
    null_scope = sparse.withColumn("dataset_version", F.lit(None).cast("string"))
    assert flagged({"sparse": null_scope, "full": scoped}) == [
        ("clinic", "first", "labs")
    ]
    with pytest.raises(ValueError, match="Incompatible quality scopes"):
        quality_frame({"sparse": sparse, "full": scoped})


def test_ownership_requires_one_unambiguous_parent(spark):
    rule = Rule(op="reference", field="fk", reference="parents", other="pk",
                match_fields={"person": "owner"}, message="Check ownership")
    events = spark.createDataFrame(
        [("ok", "one", "A"), ("wrong", "one", "B"), ("ambiguous", "two", "A"),
         ("unknown_owner", "one", None), ("optional", None, "A")],
        "_record_id string, fk string, person string",
    ).selectExpr("*", "'example' AS _dataset", "'1' AS _version", "cast(null as string) AS _issue")
    parents = spark.createDataFrame([("one", "A"), ("two", "A"), ("two", "B")],
                                    "pk string, owner string")
    issues = validate(events, "events", {"owned": rule}, {"parents": parents}, "test").collect()
    assert sorted(i.source_record_id for i in issues) == ["ambiguous", "unknown_owner", "wrong"]
