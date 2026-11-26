"""Validated research contracts and deterministic six-level configuration overlays."""

from copy import deepcopy
from graphlib import TopologicalSorter
import os
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .common import canonical, checksum, digest

ID = Annotated[str, Field(pattern=r"^[A-Za-z][A-Za-z0-9_]*$")]
Type = Literal["string", "date", "timestamp", "double", "long", "reactions"]
Metric = Literal[
    "rows",
    "columns",
    "distinct",
    "missing",
    "distribution",
    "duplicates",
    "frequency",
    "patient_counts",
    "entity_counts",
    "patient_distribution",
]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)


class Selection(Model):
    dataset: ID


class Source(Model):
    files: list[str] = Field(min_length=1)
    columns: list[ID] = Field(min_length=1)
    format: Literal["csv"] = "csv"
    delimiter: str = Field(default=",", min_length=1, max_length=1)
    null_token: str | None = None
    primary_key: list[ID] = []
    types: dict[ID, Type] = {}
    numeric_when: dict[ID, dict[ID, str]] = {}
    roles: dict[str, ID] = {}
    materialize_source: bool = False
    schema_version: str = "1"

    @model_validator(mode="after")
    def valid_fields(self):
        if self.null_token == "":
            raise ValueError(
                "Empty strings and null tokens must remain distinguishable"
            )
        if len(set(self.columns)) != len(self.columns):
            raise ValueError("Repeated source field")
        for name in [
            *self.primary_key,
            *self.types,
            *self.roles.values(),
            *self.numeric_when,
            *(k for conditions in self.numeric_when.values() for k in conditions),
        ]:
            if name not in self.columns:
                raise ValueError(f"Unknown source field {name}")
        return self


class Entity(Model):
    roles: dict[str, ID] = {}
    version: str
    fields: dict[ID, Type]
    key: ID
    field_order: list[ID] = []

    @model_validator(mode="after")
    def ordered_fields(self):
        if not self.field_order:
            self.field_order = sorted(self.fields)
        if sorted(self.field_order) != sorted(self.fields):
            raise ValueError("field_order must list each canonical field exactly once")
        return self


class Expression(Model):
    op: Literal[
        "field",
        "literal",
        "cast",
        "timestamp",
        "namespace",
        "normalize",
        "values",
        "when",
        "lookup",
        "reactions",
        "precision",
    ]
    field: ID | Literal["_record_id"] | None = None
    value: str | None = None
    kind: Type = "string"
    namespace: str | None = None
    format: str | None = None
    values: dict[str, str] = {}
    predicate: ID | None = None
    equals: str | None = None
    then: "Expression | None" = None
    groups: list[dict[ID, ID]] = []
    lookup: ID | None = None
    key: ID | None = None
    result: ID | None = None

    @model_validator(mode="after")
    def arguments(self):
        required = {
            "namespace": ["field", "namespace"],
            "when": ["predicate", "equals", "then"],
            "lookup": ["field", "lookup", "key", "result"],
            "reactions": ["groups"],
            "literal": [],
            "values": ["field", "values"],
        }
        applicable = {
            "cast": {"field", "kind"}, "literal": {"value"},
            "timestamp": {"field", "format"},
        }.get(self.op, set(required.get(self.op, ["field"])))
        for attr, spec in type(self).model_fields.items():
            if attr != "op" and attr not in applicable and getattr(self, attr) != spec.default:
                raise ValueError(f"{attr} does not apply to {self.op}")
        for attr in required.get(self.op, ["field"]):
            if not getattr(self, attr):
                raise ValueError(f"{self.op} requires {attr}")
        return self


class Mapping(Model):
    source: ID
    entity: ID
    version: str
    fields: dict[ID, Expression]


class Rule(Model):
    op: Literal[
        "required_column",
        "required",
        "type",
        "unique",
        "reference",
        "timestamp",
        "ordered",
        "duplicate",
        "numeric",
        "units",
    ]
    field: ID
    other: ID | None = None
    reference: ID | None = None
    match_fields: dict[ID, ID] = {}
    kind: Type = "string"
    where: dict[ID, str] = {}
    allowed: list[str] = []
    version: str = "1"
    classification: Literal[
        "confirmed_violation", "potential_anomaly", "expected_missingness"
    ] = "confirmed_violation"
    severity: Literal["info", "warning", "error"] = "error"
    message: str
    enabled: bool = True


class Criterion(Model):
    entity: ID
    patient_field: ID
    strategy: Literal["count", "distinct_dates", "paired_dates", "span_days"]
    threshold: int = Field(ge=1)
    date_field: ID | None = None
    distinct_field: ID | None = None
    filter: dict[ID, list[str]] = {}
    numeric_field: ID | None = None
    pair_field: ID | None = None
    pair_values: list[str] = []
    pair_keys: list[ID] = []
    uncertain_fields: list[ID] = []
    exclude_rules: list[str] = []
    version: str = "1"


class WritePolicy(Model):
    batch_id: ID | None = None
    strategy: Literal["snapshot", "append", "merge"] = "snapshot"
    location: str | None = None
    keys: list[ID] = []
    partitions: list[ID] = []
    additive_evolution: bool = False


class Binding(Model):
    rule_ids: list[ID] = []
    run: str = Field(pattern=r"^[a-f0-9]{32}$")
    table: ID
    columns: list[ID] = Field(min_length=1)


class Stage(Model):
    op: Literal["ingest", "map", "profile", "validate", "readiness"]
    output: ID
    inputs: list[ID] = []
    subject: ID
    write: WritePolicy = WritePolicy()
    quality_inputs: list[ID] | None = None
    quality_limits: dict[ID, int] = {}
    enabled: bool = True


class Spark(Model):
    cores: int = Field(ge=1, le=64)
    memory_gb: int = Field(ge=1, le=128)
    partitions: int = Field(ge=1, le=1024)
    timezone: str = "UTC"


class ConceptScope(Model):
    scope: Literal["dataset-local"]
    version: str
    rationale: str


class TerminologyReview(Model):
    """Versioned evidence for a specific local code-to-concept decision."""

    version: str = Field(min_length=1)
    mapping: ID
    mapping_version: str
    target_field: ID
    source_code: str
    concept: str
    system: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    value_type: str = Field(min_length=1)
    reviewer: str = Field(min_length=1)
    evidence: str = Field(min_length=1)


class Config(Model):
    public_rule_messages: list[str] = []
    config_version: str
    dataset: ID
    version: str = Field(pattern=r"^[a-zA-Z0-9_-][a-zA-Z0-9_.-]*$")
    raw: str
    storage: str
    outputs: str
    spark: Spark
    sources: dict[ID, Source]
    entities: dict[ID, Entity]
    mappings: dict[ID, Mapping]
    rules: dict[ID, dict[ID, Rule]]
    criteria: dict[ID, Criterion]
    tasks: dict[ID, list[ID]]
    population: ID
    patient_key: ID
    metrics: list[Metric]
    stages: dict[ID, Stage]
    bindings: dict[ID, Binding] = {}
    export_profiles: bool = False
    concepts: dict[str, ConceptScope] = {}
    terminology_reviews: dict[ID, TerminologyReview] = {}

    @model_validator(mode="after")
    def references(self):
        for name, review in self.terminology_reviews.items():
            mapping = self.mappings.get(review.mapping)
            expr = mapping.fields.get(review.target_field) if mapping else None
            if (
                not expr
                or expr.op != "values"
                or mapping.version != review.mapping_version
                or expr.values.get(review.source_code) != review.concept
            ):
                raise ValueError(
                    f"Terminology review {name} does not match registered code mapping"
                )
        for name, m in self.mappings.items():
            entity = self.entities[m.entity]
            source = self.sources[m.source]
            if set(m.fields) != set(entity.fields) or entity.key not in entity.fields:
                raise ValueError(f"Mapping {name} must cover the entity contract")

            def check(expr):
                names = [expr.field, expr.predicate] + [
                    v for g in expr.groups for v in g.values()
                ]
                for col in filter(None, names):
                    if col not in source.columns and col != "_record_id":
                        raise ValueError(f"Unregistered field {name}.{col}")
                if expr.then:
                    if expr.then.op == "lookup":
                        raise ValueError("Lookup requires a top-level mapping field")
                    check(expr.then)
                if expr.format is not None and expr.op != "timestamp":
                    raise ValueError("A date format applies only to timestamp mappings")
                if expr.op == "reactions" and any(
                    set(group) != {"code", "description", "severity"} for group in expr.groups
                ):
                    raise ValueError("Invalid reaction group fields")

            for expr in m.fields.values():
                check(expr)
        for task, criteria in self.tasks.items():
            if not criteria or set(criteria) - self.criteria.keys():
                raise ValueError(f"Invalid criteria for {task}")
        known_rules = {"malformed_record"}
        for rules in self.rules.values():
            known_rules.update(rules)
        for binding in self.bindings.values():
            if binding.rule_ids and not {"rule_id", "source_record_id"} <= set(binding.columns):
                raise ValueError("External rule IDs require a quality-table binding")
            known_rules.update(binding.rule_ids)
        for criterion_id, c in self.criteria.items():
            if set(c.exclude_rules) - known_rules:
                raise ValueError(f"Unknown exclusion rule in criterion {criterion_id}")
            fields = self.entities[c.entity].fields
            for col in filter(
                None,
                [
                    c.patient_field,
                    c.date_field,
                    c.distinct_field,
                    c.numeric_field,
                    c.pair_field,
                    *c.pair_keys,
                    *c.filter,
                    *c.uncertain_fields,
                ],
            ):
                if col not in fields:
                    raise ValueError(f"Invalid criterion field {col}")
            if set(c.uncertain_fields) - (set(c.filter) | {c.pair_field}):
                raise ValueError(
                    "Uncertainty fields require a criterion filter or pair field"
                )
            if c.strategy != "count" and not c.date_field:
                raise ValueError("Date-based criteria require date_field")
            if c.strategy == "paired_dates" and len(c.pair_values) != len(
                set(c.pair_values)
            ):
                raise ValueError(f"Duplicate pair_values in criterion {criterion_id}")
            if c.strategy == "paired_dates" and (
                not c.pair_field or len(set(c.pair_values)) < 2
            ):
                raise ValueError("Invalid pairing contract")
        order = self.order()
        contracts = {alias: pin.columns for alias, pin in self.bindings.items()}
        for name in order:
            stage = self.stages[name]
            if any(limit < 0 for limit in stage.quality_limits.values()) or set(
                stage.quality_limits
            ) - set(stage.inputs):
                raise ValueError(f"Invalid quality gate for {name}")
            for binding in stage.quality_inputs or []:
                if binding not in stage.inputs or not {
                    "rule_id",
                    "source_record_id",
                } <= set(contracts[binding]):
                    raise ValueError(f"Invalid quality input {binding}")
            for binding in stage.quality_limits:
                if "classification" not in contracts[binding]:
                    raise ValueError(f"Expected a quality table for {binding}")
            if stage.write.strategy == "append" and not stage.write.batch_id:
                raise ValueError("Append requires an explicit batch ID")
            if stage.write.strategy == "merge" and not stage.write.keys:
                raise ValueError("Merge policy requires keys")
            if stage.quality_inputs is not None and stage.op not in ("map", "readiness"):
                raise ValueError(f"Unsupported quality_inputs for {name}")
            if stage.write.batch_id and stage.write.strategy != "append":
                raise ValueError("Only append writes use batch_id")
            if stage.write.keys and stage.write.strategy != "merge":
                raise ValueError("Only merge writes use keys")
            if stage.op == "readiness" and stage.subject != self.population:
                raise ValueError("Readiness subject must match the configured population")
            if stage.op == "ingest":
                if stage.write != WritePolicy():
                    raise ValueError("Ingestion uses the registered source materialization policy")
                if stage.inputs:
                    raise ValueError("Source registration has no upstream inputs")
                contracts[stage.output] = self.sources[stage.subject].columns
                continue
            if not stage.inputs:
                raise ValueError(f"Stage {name} requires registered inputs")
            fields = set(contracts[stage.inputs[0]])
            if stage.op == "validate":
                for rid, rule in self.rules[stage.subject].items():
                    if rule.field not in fields and rule.op != "required_column":
                        raise ValueError(f"Unknown validation field {rid}.{rule.field}")
                    if rule.where.keys() - fields:
                        raise ValueError(f"Invalid condition for {rid}")
                    if rule.op == "ordered" and rule.other not in fields:
                        raise ValueError(f"Invalid interval rule {rid}")
                    if rule.match_fields and rule.op != "reference":
                        raise ValueError(f"match_fields requires a reference rule: {rid}")
                    if rule.op == "reference" and (
                        rule.reference not in stage.inputs
                        or rule.other not in contracts[rule.reference]
                        or rule.match_fields.keys() - fields
                        or set(rule.match_fields.values()) - set(contracts[rule.reference])
                    ):
                        raise ValueError(f"Unbound reference for {rid}")
                contracts[stage.output] = [
                    "rule_id",
                    "source_record_id",
                    "severity",
                    "classification",
                ]
            elif stage.op == "map":
                m = self.mappings[stage.subject]
                if not set(self.sources[m.source].columns).issubset(fields):
                    raise ValueError(
                        f"Input schema incompatible with mapping {stage.subject}"
                    )
                for expr in m.fields.values():
                    if expr.op == "lookup" and (
                        expr.lookup not in stage.inputs
                        or {expr.key, expr.result} - set(contracts[expr.lookup])
                    ):
                        raise ValueError(f"Unbound lookup in {name}")
                contracts[stage.output] = list(self.entities[m.entity].fields)
            elif stage.op == "readiness":
                if (
                    self.population not in stage.inputs
                    or self.patient_key not in contracts[self.population]
                ):
                    raise ValueError("Missing population contract")
                contracts[stage.output] = [
                    "task",
                    "patient_id",
                    "status",
                    "reasons",
                    "evidence",
                    "criterion_counts",
                ]
            else:
                contracts[stage.output] = [
                    "table_id",
                    "metric",
                    "field",
                    "category",
                    "value",
                ]
            if set(stage.write.partitions + stage.write.keys) - set(
                contracts[stage.output]
            ):
                raise ValueError(f"Invalid partition or write key for {name}")
        return self

    def order(self):
        enabled = {n: s for n, s in self.stages.items() if s.enabled}
        producers = {s.output: n for n, s in enabled.items()}
        if len(producers) != len(enabled) or producers.keys() & self.bindings.keys():
            raise ValueError("Competing stage outputs")
        deps = {}
        for n, stage in enabled.items():
            if stage.op == "ingest" and stage.subject not in self.sources:
                raise ValueError(f"Unknown source for {n}")
            if stage.op == "map" and stage.subject not in self.mappings:
                raise ValueError(f"Unknown mapping for {n}")
            missing = set(stage.inputs) - producers.keys() - self.bindings.keys()
            if missing:
                raise ValueError(f"Missing stage inputs {missing}")
            deps[n] = sorted(producers[i] for i in stage.inputs if i in producers)
        return list(TopologicalSorter(dict(sorted(deps.items()))).static_order())


class UniqueLoader(yaml.SafeLoader):
    pass


def _mapping(loader, node):
    pairs = loader.construct_pairs(node, deep=True)
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate YAML key {key} at {node.start_mark}")
        result[key] = value
    return result


UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def merge(target, update, *, peer=False, path=""):
    for key, value in update.items():
        location = f"{path}.{key}"
        if key in target and isinstance(value, dict) and isinstance(target[key], dict):
            expression = location.startswith(".mappings.") and ".fields." in location
            if not peer and expression and "op" in value and value["op"] != target[key].get("op"):
                target[key] = deepcopy(value)
            else:
                merge(target[key], value, peer=peer, path=location)
        elif peer and key in target and canonical(target[key]) != canonical(value):
            raise ValueError(f"Conflicting definitions at {location}")
        else:
            target[key] = deepcopy(value)
    return target


def resolve(
    dataset=None, environment="local", experiment=None, overrides=None, root=None
):
    root = Path(root or os.environ.get("MEDLENS_ROOT", ".")).resolve()
    cfg = root / "configs"
    evidence = {}
    if dataset is None:
        selector = cfg / "selection.yaml"
        dataset = Selection.model_validate(yaml.load(selector.read_text(), Loader=UniqueLoader)).dataset
        evidence[str(selector.relative_to(root))] = checksum(selector)

    def layer(folder, selection):
        if Path(selection).name != selection or selection in (".", ".."):
            raise ValueError("Invalid configuration selection")
        path = cfg / folder / selection
        files = (
            sorted(path.glob("*.yaml"))
            if path.is_dir()
            else [path.with_suffix(".yaml")]
        )
        if not files:
            raise ValueError(f"Empty configuration layer {path}")
        return files

    layers = [
        [cfg / "base.yaml"],
        sorted((cfg / "entities").glob("*.yaml")),
        layer("datasets", dataset),
        layer("environments", environment),
    ]
    if experiment:
        layers.append(layer("experiments", experiment))
    result = {}
    for level, files in enumerate(layers):
        combined = {}
        for path in files:
            try:
                merge(combined, yaml.load(path.read_text(), Loader=UniqueLoader), peer=True)
            except ValueError as exc:
                raise ValueError(f"{exc} (peer files: {', '.join(p.name for p in files)})") from None
            evidence[str(path.relative_to(root))] = checksum(path)
        if level > 1:
            for entity_id, change in combined.get("entities", {}).items():
                old = result.get("entities", {}).get(entity_id)
                if (
                    old
                    and merge(deepcopy(old), change) != old
                    and change.get("version", old["version"]) == old["version"]
                ):
                    raise ValueError(
                        f"Changing {entity_id} requires a new entity schema version"
                    )
        merge(result, combined)
    # YAML policy is the reviewed public vocabulary. Runtime input can select
    # public text but cannot publish a new free-text message in an immutable journal.
    runtime = overrides if overrides is not None else {}
    if not isinstance(runtime, dict):
        raise ValueError("Runtime overrides must be an object")
    if "public_rule_messages" in runtime:
        raise ValueError("Public messages must be registered in configuration files")
    public_messages = set(result.get("public_rule_messages", []))
    for rules in result.get("rules", {}).values():
        public_messages.update(rule["message"] for rule in rules.values() if "message" in rule)
    for rules in runtime.get("rules", {}).values():
        for rule in rules.values():
            if "message" in rule and rule["message"] not in public_messages:
                raise ValueError("Unregistered public rule message in runtime overrides")
    for entity_id, change in runtime.get("entities", {}).items():
        old = result["entities"].get(entity_id)
        if (
            old
            and merge(deepcopy(old), change) != old
            and change.get("version", old["version"]) == old["version"]
        ):
            raise ValueError(
                f"Changing {entity_id} requires a new entity schema version"
            )
    config = Config.model_validate(merge(result, runtime))
    # The entire resolved policy, including public prose and mapping literals,
    # is publishable. Never let an explicitly protected value cross that boundary.
    protected = [value for name, value in os.environ.items()
                 if name.startswith("MEDLENS_SECRET_") and value]
    if protected:
        def contains_secret(node):
            if isinstance(node, dict):
                return any(contains_secret(k) or contains_secret(v) for k, v in node.items())
            if isinstance(node, list):
                return any(contains_secret(v) for v in node)
            return isinstance(node, str) and any(value in node for value in protected)

        if contains_secret([config.model_dump(), runtime, evidence, environment, experiment]):
            raise ValueError("Protected value in publishable research configuration")
    if config.dataset != dataset:
        raise ValueError("Selected dataset and registered dataset differ")
    return config, digest(config.model_dump()), evidence
