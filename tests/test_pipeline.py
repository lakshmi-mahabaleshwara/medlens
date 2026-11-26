from pathlib import Path
import json

from medlens.pipeline import Research
from medlens.common import checksum
from medlens.storage import read


def test_configuration_only_onboarding_and_replay(tmp_path):
    overrides = {
        "storage": str(tmp_path / "tables"),
        "outputs": str(tmp_path / "outputs"),
    }
    r = Research("clinic", overrides=overrides)
    try:
        first = r.run()
        tables = first["tables"]
        assert tables["labs"]["format"] == "csv"
        assert not (r.storage / "tables" / "labs").exists()
        assert tables["observation"]["stats"]["rows"] == 7
        obs = r.frame(first, "observation").collect()
        assert [(o.raw_value, o.numeric_value) for o in obs if o.raw_type == "T"] == [
            ("negative", None)
        ]
        assert len({o._record_id for o in obs}) == 7
        assert all(json.loads(o.patient_id)[0] == "clinic" for o in obs)
        issues = r.frame(first, "issues_labs").collect()
        assert {v.rule_id for v in issues} == {
            "duplicate_payload",
            "numeric_parse",
            "a1_unit",
            "patient_reference",
            "encounter_reference",
        }
        assert {v.source_record_id for v in issues}.issubset(
            {o._record_id for o in obs}
        )
        result = r.frame(first, "readiness").collect()
        assert (
            sum(v.status == "eligible" for v in result if v.task == "hba1c_trend") == 1
        )
        assert (
            sum(v.status == "indeterminate" for v in result if v.task == "diabetes")
            == 2
        )
        eligible = next(
            v for v in result if v.task == "hba1c_trend" and v.status == "eligible"
        )
        occurrence = next(o for o in obs if o._record_id in eligible.evidence)
        path = Path(tables["labs"]["path"]) / occurrence._file
        assert checksum(path) == occurrence._checksum
        with path.open("rb") as stream:
            stream.seek(occurrence._offset)
            assert b"A1" in stream.read(occurrence._length)
        facts = read(r.spark, first["metadata"]).where("kind = 'lineage'").collect()
        assert any(json.loads(f.detail)["input"] == "labs" for f in facts)
        second = r.run()
        assert {k: v["identity"] for k, v in second["tables"].items()} == {
            k: v["identity"] for k, v in tables.items()
        }
        assert (
            r.frame(second, "readiness").exceptAll(r.frame(first, "readiness")).count()
            == 0
        )
        branch = r.run(["profile_labs"], parent=first["run_id"])
        assert branch["parent"] == first["run_id"] and len(branch["stages"]) == 1
    finally:
        r.close()
    copy = Research(
        "clinic",
        overrides=overrides | {"sources": {"labs": {"materialize_source": True}}},
    )
    try:
        materialized = copy.run(
            ["profile_labs"], parent=first["run_id"]
        )  # pinned external input
        assert materialized["tables"]["labs"]["format"] == "csv"
        both = copy.run(["ingest_labs", "profile_labs"])
        ref = both["tables"]["labs"]
        assert ref["format"] == "delta" and ref["source_ref"]["format"] == "csv"
        assert (
            copy.frame(both, "profile_labs")
            .exceptAll(copy.frame(first, "profile_labs"))
            .count()
            == 0
        )
    finally:
        copy.close()
    alternate = Research(
        "clinic",
        overrides=overrides
        | {
            "mappings": {
                "observation": {"fields": {"numeric_value": {"op": "literal"}}}
            }
        },
    )
    try:
        changed = alternate.run(["map_observation"], parent=first["run_id"])
        assert (
            alternate.frame(changed, "observation")
            .where("numeric_value IS NOT NULL")
            .count()
            == 0
        )
    finally:
        alternate.close()
    rebound = Research(
        "clinic",
        overrides=overrides
        | {
            "bindings": {
                "observation": {
                    "run": changed["run_id"],
                    "table": "observation",
                    "columns": list(alternate.config.entities["observation"].fields),
                }
            },
            "stages": {
                "map_observation": {"enabled": False},
                "assess": {"enabled": False},
            },
        },
    )
    try:
        child = rebound.run(["profile_people"], parent=first["run_id"])
        assert (
            "readiness" not in child["tables"]
            and "issues_observation" not in child["tables"]
        )
        assert (
            child["tables"]["observation"]["identity"]
            == changed["tables"]["observation"]["identity"]
        )
        # Simulate a direct definition-file edit bypassing overlay guards.
        rebound.config.entities["patient"].fields["birth_date"] = "string"
        import pytest

        with pytest.raises(ValueError, match="Published entity contract changed"):
            rebound._register_contract("patient", rebound.config.entities["patient"])
    finally:
        rebound.close()
    resumed = Research("clinic", overrides=overrides)
    try:
        rebuilt = resumed.run(["assess"], parent=child["run_id"])
        assert (
            rebuilt["tables"]["observation"]["identity"]
            == changed["tables"]["observation"]["identity"]
        )
        assert (
            resumed.frame(rebuilt, "readiness")
            .where("task = 'hba1c_trend' AND status = 'eligible'")
            .count()
            == 0
        )
    finally:
        resumed.close()


def test_recovery_drift_and_validation_gate(tmp_path, monkeypatch):
    import pytest
    from medlens import pipeline
    from shutil import copytree

    raw = tmp_path / "raw"
    copytree(Path("tests/fixtures/clinic"), raw)
    config = {
        "raw": str(raw),
        "storage": str(tmp_path / "tables"),
        "outputs": str(tmp_path / "out"),
    }
    r = Research("clinic", overrides=config)
    callback = pipeline.journal
    try:

        def interrupt(*args):
            raise RuntimeError("simulated interruption after data commit")

        monkeypatch.setattr(pipeline, "journal", interrupt)
        with pytest.raises(RuntimeError, match="simulated interruption"):
            r.run(["profile_people"])
        monkeypatch.setattr(pipeline, "journal", callback)
        good = r.run(["profile_people"])
        assert good["tables"]["profile_people"]["version"] == 0
        (raw / "people.csv").write_text("person,born\nchanged,2000-01-01\n")
        with pytest.raises(ValueError, match="drift"):
            r.run(["profile_people"], parent=good["run_id"])
        with pytest.raises(ValueError, match="drift"):
            r.run(["ingest_people"])
    finally:
        r.close()


def test_explicit_binding_and_failed_gate(tmp_path):
    import pytest

    config = {"storage": str(tmp_path / "delta"), "outputs": str(tmp_path / "out")}
    study = Research("clinic", overrides=config)
    try:
        parent = study.run(["validate_labs"])
    finally:
        study.close()
    branch = Research(
        "clinic",
        overrides=config
        | {
            "bindings": {
                "registered_labs": {
                    "run": parent["run_id"],
                    "table": "labs",
                    "columns": ["reading"],
                }
            },
            "stages": {"profile_labs": {"inputs": ["registered_labs"]}},
            "metrics": ["rows"],
        },
    )
    try:
        result = branch.run(["profile_labs"])
        assert [s["stage"] for s in result["stages"]] == ["profile_labs"]
        assert branch.frame(result, "profile_labs").first().value == "7"
    finally:
        branch.close()
    blocked = Research(
        "clinic",
        overrides=config
        | {"stages": {"map_observation": {"quality_limits": {"issues_labs": 0}}}},
    )
    try:
        with pytest.raises(ValueError, match="confirmed violations"):
            blocked.run(["map_observation"], parent=parent["run_id"])
        assert blocked.manifest(parent["run_id"])["status"] == "success"
    finally:
        blocked.close()


def test_journal_recovers_projection_and_lost_ack(tmp_path, monkeypatch):
    from medlens import pipeline

    study = Research(
        "clinic",
        overrides={
            "outputs": str(tmp_path / "out"),
            "storage": str(tmp_path / "delta"),
        },
    )
    save = pipeline.write_json
    append = pipeline.journal
    try:

        def lose_projection(path, obj):
            if path.name == "manifest.json" and obj["status"] == "success":
                raise OSError("projection unavailable")
            save(path, obj)

        monkeypatch.setattr(pipeline, "write_json", lose_projection)
        first = study.run(["ingest_people"])
        manifest_path = study.output / "runs" / first["run_id"] / "manifest.json"
        assert json.loads(manifest_path.read_text())["status"] == "publishing"
        assert study.manifest(first["run_id"]) == first
        monkeypatch.setattr(pipeline, "write_json", save)

        def lose_ack(*args):
            append(*args)
            raise RuntimeError("lost acknowledgement")

        monkeypatch.setattr(pipeline, "journal", lose_ack)
        second = study.run(["ingest_people"])
        assert study.manifest(first["run_id"]) == first
        assert study.manifest(second["run_id"]) == second
        facts = (
            read(study.spark, second["metadata"]).where("kind = 'execution'").collect()
        )
        assert sorted(
            (r.run_id, json.loads(r.detail)["status"]) for r in facts
        ) == sorted([(first["run_id"], "success"), (second["run_id"], "success")])
    finally:
        study.close()


def test_config_edits_cannot_reuse_cached_results(tmp_path):
    import pytest
    from medlens.common import digest

    options = {
        "storage": str(tmp_path / "tables"),
        "outputs": str(tmp_path / "runs"),
        "metrics": ["rows"],
    }
    study = Research("clinic", overrides=options)
    try:
        first = study.run(["profile_people"])
        study.config.metrics[:] = ["columns"]
        with pytest.raises(ValueError, match="changed after resolution"):
            study.run(["profile_people"])
        assert len(list((study.output / "runs").iterdir())) == 1
    finally:
        study.close()
    revised = Research("clinic", overrides=options | {"metrics": ["columns"]})
    try:
        second = revised.run(["profile_people"])
        exported = json.loads(
            (revised.output / "runs" / second["run_id"] / "resolved.json").read_text()
        )
        assert digest(exported) == second["config_hash"] != first["config_hash"]
        assert {
            r.metric for r in revised.frame(second, "profile_people").collect()
        } == {"type"}
    finally:
        revised.close()


def test_analytical_batch_and_table_statistics(tmp_path):
    from pyspark.sql import functions as F

    raw = tmp_path / "raw"
    raw.mkdir()
    for batch in ("first", "second"):
        (raw / f"{batch}.csv").write_text(f"person,born\n{batch},\n")
        options = {
            "raw": str(raw),
            "storage": str(tmp_path / "delta"),
            "outputs": str(tmp_path / "outputs"),
            "sources": {
                batch: {"files": [f"{batch}.csv"], "columns": ["person", "born"]}
            },
            "rules": {
                "people": {
                    "missing_birth": {
                        "op": "required",
                        "field": "born",
                        "message": "Missing date",
                    }
                }
            },
            "stages": {
                "ingest_people": {"subject": batch},
                "validate_people": {"write": {"strategy": "append", "batch_id": batch}},
                "assess": {
                    "inputs": ["patient"],
                    "quality_inputs": [],
                    "write": {"strategy": "merge", "keys": ["patient_id", "task"]},
                },
            },
        }
        study = Research("clinic", overrides=options)
        try:
            run = study.run(["assess"])
            cumulative = 1 if batch == "first" else 2
            quality = run["tables"]["issues_people"]
            assert quality["stats"]["batch"]["issues_by_rule"] == {"missing_birth": 1}
            assert quality["stats"]["batch"]["evaluated_by_rule"]["missing_birth"] == 1
            assert quality["stats"]["table"] == {
                "rows": cumulative,
                "issues_by_rule": {"missing_birth": cumulative},
            }
            decisions = run["tables"]["readiness"]
            stats = decisions["stats"]
            assert (
                stats["batch"]["population_rows"]
                == stats["batch"]["unique_identifiable_patients"]
                == 1
            )
            assert stats["batch"]["rows"] == len(study.config.tasks)
            assert all(r["count"] == 1 for r in stats["batch"]["tasks"])
            assert (
                stats["table"]["rows"]
                == stats["rows"]
                == cumulative * len(study.config.tasks)
            )
            assert all(r["count"] == cumulative for r in stats["table"]["tasks"])
            assert decisions["keys"] == [] and decisions["merge_keys"] == [
                "patient_id",
                "task",
            ]
            journal = read(study.spark, run["metadata"]).where(
                (F.col("kind") == "table_registry") & (F.col("run_id") == run["run_id"])
            )
            refs = {r.object_id: json.loads(r.detail) for r in journal.collect()}
            assert refs["issues_people"] == quality and refs["readiness"] == decisions
            if batch == "second":
                retry = study.run(["assess"])
                for name in ("issues_people", "readiness"):
                    assert (
                        retry["tables"][name]["version"]
                        == run["tables"][name]["version"]
                    )
                    assert (
                        retry["tables"][name]["stats"] == run["tables"][name]["stats"]
                    )
        finally:
            study.close()


def test_encounter_ownership_excludes_cross_patient_measurements(tmp_path):
    import csv
    import shutil

    raw = tmp_path / "raw"
    shutil.copytree("tests/fixtures/clinic", raw)
    with (raw / "labs.csv").open("a", newline="") as out:
        csv.writer(out).writerows([
            ["p2", "v1", "01/01/2020", "A1", "7", "pct", "N"],
            ["p2", "v2", "01/02/2021", "A1", "8", "pct", "N"],
        ])
    study = Research("clinic", overrides={
        "raw": str(raw), "storage": str(tmp_path / "tables"), "outputs": str(tmp_path / "out"),
    })
    try:
        run = study.run(["assess"])
        rows = study.frame(run, "labs").where("person='p2' AND visit IN ('v1','v2')").collect()
        issues = study.frame(run, "issues_labs").where("rule_id='encounter_reference'").collect()
        assert len(rows) == 2
        assert {r._record_id for r in rows} <= {i.source_record_id for i in issues}
        trends = study.frame(run, "readiness").where("task='hba1c_trend'").collect()
        assert {json.loads(r.patient_id)[2] for r in trends if r.status == "eligible"} == {"p1"}
    finally:
        study.close()
