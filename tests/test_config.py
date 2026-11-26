import pytest
import yaml

from medlens.common import Registry
from medlens.config import UniqueLoader, merge, resolve


def test_overlay_order_and_hash():
    first, hash1, _ = resolve("clinic", experiment="longitudinal")
    assert first.criteria["history"].threshold == 730
    config, hash2, _ = resolve(
        "clinic",
        experiment="longitudinal",
        overrides={"criteria": {"history": {"threshold": 600}}},
    )
    assert config.criteria["history"].threshold == 600
    assert hash1 != hash2
    assert resolve("clinic", experiment="longitudinal")[1] == hash1
    assert config.entities["patient"].fields["birth_date"] == "date"


def test_conflicts_and_safe_loading():
    with pytest.raises(ValueError, match="Conflicting"):
        merge({"rule": {"a": 1}}, {"rule": {"a": 2}}, peer=True)
    with pytest.raises(ValueError, match="Duplicate"):
        yaml.load("a: 1\na: 2", Loader=UniqueLoader)
    with pytest.raises(yaml.YAMLError):
        yaml.load("!!python/name:os.getcwd", Loader=UniqueLoader)


def test_graph_and_registration():
    c, _, _ = resolve("synthea")
    assert len(c.sources) == 6
    assert c.order()[-1] == "assess"
    with pytest.raises(ValueError):
        resolve("clinic", overrides={"stages": {"assess": {"inputs": ["missing"]}}})
    registry = Registry()
    registry.register("named")(str)
    assert registry.resolve("named") is str
    with pytest.raises(ValueError):
        registry.register("named")(int)
    with pytest.raises(ValueError):
        registry.resolve("nope")


@pytest.mark.parametrize(
    "overrides",
    [
        {"stages": {"assess": {"output": "labs"}}},
        {"stages": {"ingest_labs": {"enabled": False}}},
        {"stages": {"validate_labs": {"inputs": ["labs"]}}},
        {"entities": {"observation": {"fields": {"numeric_value": "string"}}}},
        {"rules": {"labs": {"numeric_parse": {"op": "arbitrary_expression"}}}},
    ],
)
def test_reject_incompatible_overrides(overrides):
    with pytest.raises(ValueError):
        resolve("clinic", overrides=overrides)


def test_review_must_match_versioned_mapping():
    review = dict(
        version="1",
        mapping="observation",
        mapping_version="1",
        target_field="concept",
        source_code="A1",
        concept="hba1c",
        system="fixture",
        system_version="1",
        unit="pct",
        value_type="N",
        reviewer="fixture-reviewer",
        evidence="fixture-only",
    )
    assert (
        resolve("clinic", overrides={"terminology_reviews": {"fixture": review}})[0]
        .terminology_reviews["fixture"]
        .unit
        == "pct"
    )
    with pytest.raises(ValueError, match="does not match"):
        resolve(
            "clinic",
            overrides={
                "terminology_reviews": {"fixture": dict(review, concept="other")}
            },
        )


def test_bootstrap_published_entity_version(tmp_path):
    from types import SimpleNamespace
    from medlens.common import write_json
    from medlens.pipeline import Research

    cfg, _, _ = resolve("clinic")
    workspace = SimpleNamespace(
        storage=tmp_path / "data", output=tmp_path / "out", config=cfg
    )
    old = cfg.model_dump()
    entity = cfg.entities["patient"]
    # The earliest publications predate profiling roles and explicit field ordering.
    del old["entities"]["patient"]["field_order"], old["entities"]["patient"]["roles"]
    write_json(workspace.output / "runs" / "original" / "resolved.json", old)
    write_json(
        workspace.storage / "registry" / "snapshots" / "original.json",
        dict(
            role="canonical",
            id="patient",
            schema_version=entity.version,
            producer="original",
            schema={"fields": [{"name": field} for field in entity.field_order]},
        ),
    )
    entity.fields["birth_date"] = "string"
    with pytest.raises(ValueError, match="Published entity contract changed"):
        Research._register_contract(workspace, "patient", entity)
    entity.fields["birth_date"] = "date"
    assert Research._register_contract(
        workspace, "patient", entity
    ) == Research._register_contract(workspace, "patient", entity)


def test_duplicate_pair_components_fail_preflight():
    with pytest.raises(ValueError, match="Duplicate pair_values in criterion bp_pairs"):
        resolve(
            "clinic",
            overrides={
                "criteria": {
                    "bp_pairs": {"pair_values": ["systolic", "diastolic", "diastolic"]}
                }
            },
        )


def test_strict_peers_and_selector_evidence():
    for first, second in [(True, 1), (1, True), ([False], [0])]:
        with pytest.raises(ValueError, match="Conflicting"):
            merge({"k": first}, {"k": second}, peer=True)
    assert "configs/selection.yaml" in resolve()[2]


@pytest.mark.parametrize("overrides", [
    {"criteria": {"history": {"exclude_rules": ["unknown_rule"]}}},
    {"stages": {"ingest_labs": {"write": {"strategy": "append", "batch_id": "batch"}}}},
    {"mappings": {"patient": {"fields": {"gender": {"op": "when", "predicate": "person",
     "equals": "a", "then": {"op": "lookup", "field": "person", "lookup": "x", "key": "y", "result": "z"}}}}}},
])
def test_release_configuration_contracts(overrides):
    with pytest.raises(ValueError):
        resolve("clinic", overrides=overrides)


def test_secret_free_failure_diagnostics(tmp_path, monkeypatch):
    import json
    import shutil
    import subprocess
    from uuid import uuid4
    from medlens.pipeline import Research

    shutil.copytree("configs", tmp_path / "configs")
    marker = uuid4().hex
    monkeypatch.setenv("MEDLENS_ROOT", str(tmp_path))
    for overrides in ({"credential": marker}, {"rules": {"labs": {"numeric_parse": {"message": marker}}}}):
        with pytest.raises(ValueError) as failure:
            Research("clinic", overrides=overrides)
        assert marker not in str(failure.value)
        process = subprocess.run(["medlens", "--dataset", "clinic", "--overrides", json.dumps(overrides)],
                                 capture_output=True, text=True)
        assert process.returncode and marker not in process.stdout + process.stderr
    manifests = list(tmp_path.rglob("manifest.json"))
    assert len(manifests) == 4 and all(marker not in p.read_text() for p in manifests)
    assert not list(tmp_path.rglob("resolved.json")) and not list(tmp_path.rglob("_delta_log"))


def test_known_secret_cannot_be_embedded_in_public_fields(monkeypatch):
    from uuid import uuid4

    secret = uuid4().hex
    monkeypatch.setenv("MEDLENS_SECRET_TEST", secret)
    with pytest.raises(ValueError, match="Protected value"):
        resolve("clinic", overrides={"mappings": {"patient": {"fields": {
            "gender": {"op": "literal", "value": f"embedded {secret}"}
        }}}})
