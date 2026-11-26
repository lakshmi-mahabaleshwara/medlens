"""Run configured stage graphs and publish version-pinned research snapshots."""

from copy import deepcopy
import json
import os
from pathlib import Path
from uuid import uuid4

import yaml
from pyspark.sql import functions as F

from . import __version__
from .common import digest, write_json, writer_lock
from .config import Config, Source, resolve
from .ingestion import inventory
from .metadata import build_info, event, journal, now, published_execution
from .profiling import profile
from .quality import quality_frame, record_flags
from .readiness import assess, identifiable, population_counts
from .transformation import present
from .runtime import session
from .storage import delta_write, read
from .transformation import standardize
from .validation import validate


def _outcome_counts(op, frame):
    if op == "readiness":
        tasks = frame.groupBy("task", "status").count().orderBy("task", "status")
        return {"tasks": [row.asDict() for row in tasks.collect()]}
    counts = frame.groupBy("rule_id").count()
    return {"issues_by_rule": {row.rule_id: row["count"] for row in counts.collect()}}


class Research:
    """Public notebook and CLI interface. One instance owns its local Spark session."""

    def __init__(
        self,
        dataset=None,
        environment="local",
        experiment=None,
        overrides=None,
        root=None,
    ):
        self.root = Path(root or os.environ.get("MEDLENS_ROOT", ".")).resolve()
        dataset = dataset or os.environ.get("MEDLENS_DATASET")
        try:
            self.config, self._config_hash, self.config_files = resolve(
                dataset, environment, experiment, overrides, self.root
            )
        except (KeyError, ValueError, OSError, yaml.YAMLError, TypeError, AttributeError) as error:
            run_id = uuid4().hex
            write_json(
                self.root / "outputs" / "runs" / run_id / "manifest.json",
                {
                    "run_id": run_id,
                    "status": "failed",
                    "stage": "configuration",
                    "finished": now(),
                    "error": type(error).__name__,
                },
            )
            raise ValueError(f"Invalid research configuration ({type(error).__name__}); attempt {run_id}") from None
        self.selection = dict(
            environment=environment,
            experiment=experiment,
            overrides=deepcopy(overrides or {}),
        )
        self.storage = (self.root / self.config.storage).resolve()
        self.output = (self.root / self.config.outputs).resolve()
        self.raw = (self.root / self.config.raw).resolve()
        if any(path.is_relative_to(self.raw) for path in (self.storage, self.output)):
            raise ValueError(
                "Managed outputs must be outside the immutable source root"
            )
        self.storage.mkdir(parents=True, exist_ok=True)
        self.spark = session(self.config.spark)
        self.build = build_info(self.spark)
        self.frames = {}

    @property
    def config_hash(self):
        return self._config_hash

    def close(self):
        self.spark.stop()

    def frame(self, manifest, table):
        return read(self.spark, manifest["tables"][table])

    def manifest(self, run_id):
        if not run_id.isalnum():
            raise ValueError("Invalid run identifier")
        path = self.output / "runs" / run_id / "manifest.json"
        result = json.loads(path.read_text()) if path.exists() else None
        if result is None or (
            result["status"] != "success"
            and result.get("writer_root") == str(self.storage)
        ):
            committed = published_execution(self.spark, self.storage, run_id)
            if committed is not None:
                result = committed
                self._project_manifest(path, result)
        if result is None:
            raise FileNotFoundError(path)
        if result["status"] != "success":
            raise ValueError("Run did not publish a snapshot")
        return result

    @staticmethod
    def _project_manifest(path, manifest):
        try:
            write_json(path, manifest)
        except OSError:
            event(manifest["run_id"], "manifest", "projection_pending")

    def _inputs(self, stage, tables):
        frames = {}
        for name in stage.inputs:
            ref = tables[name]
            key = ref["identity"]
            if key not in self.frames:
                frame = read(self.spark, ref)
                self.frames[key] = frame.cache() if ref["format"] == "csv" else frame
            frames[name] = self.frames[key]
        return frames

    def _ingest(self, stage, c):
        spec = c.sources[stage.subject]
        files = inventory(self.raw, spec)
        identity = digest(
            [c.dataset, c.version, stage.subject, files, spec.model_dump()]
        )
        ref = dict(
            id=stage.subject,
            format=spec.format,
            path=str(self.raw),
            files=files,
            identity=identity,
            version=digest(files),
            delivery=c.version,
            dataset=c.dataset,
            reader=spec.model_dump(),
            role="source",
            schema_version=spec.schema_version,
        )
        anchor = (
            self.storage
            / "registry"
            / c.dataset
            / c.version
            / (stage.subject + ".json")
        )
        if (
            anchor.exists()
            and json.loads(anchor.read_text())["version"] != ref["version"]
        ):
            raise ValueError(f"Source drift under existing delivery {stage.subject}")
        frame = read(self.spark, ref).cache()
        self.frames[identity] = frame
        counts = (
            frame.agg(F.count("*").alias("rows"), F.count("_issue").alias("malformed"))
            .first()
            .asDict()
        )
        if inventory(self.raw, spec) != files:
            raise ValueError("Source changed during ingestion")
        ref.update(
            stats=counts,
            schema=frame.schema.jsonValue(),
            keys=spec.primary_key,
            write_strategy="external",
            partitions=[],
        )
        write_json(anchor, ref)
        if spec.materialize_source:
            copied = delta_write(
                self.spark,
                frame,
                self.storage / "tables" / stage.output / identity,
                stage.output,
                identity,
                schema_version=spec.schema_version,
            )
            return dict(
                ref, **(copied | {"source_ref": ref, "write_strategy": "snapshot"})
            )
        return ref

    def _process(self, stage, inputs, run_id, refs, c):
        table_ids = {key: refs[key]["id"] for key in inputs}
        first = inputs[stage.inputs[0]]
        if stage.op == "map":
            mapping = c.mappings[stage.subject]
            frame = standardize(first, mapping, c.entities[mapping.entity], inputs)
            quality = quality_frame(
                {k: inputs[k] for k in stage.inputs[1:]}, stage.quality_inputs, refs
            )
            frame = record_flags(frame, quality, table_ids[stage.inputs[0]])
            source_flags = F.col("_source_flags")
            frame = frame.withColumn(
                "_quality_flags",
                F.sort_array(
                    F.array_union(
                        F.col("_mapping_flags"), F.coalesce(source_flags, F.array())
                    )
                ),
            ).drop("_mapping_flags", "_source_flags")
            return frame, "canonical", c.entities[mapping.entity].version
        if stage.op == "profile":
            spec = c.sources.get(stage.subject)
            entity = c.entities.get(stage.subject)
            roles = spec.roles if spec else (entity.roles if entity else {})
            types = spec.types if spec else (entity.fields if entity else {})
            return (
                profile(
                    first,
                    stage.inputs[0],
                    c.metrics,
                    roles,
                    types,
                    spec.numeric_when if spec else {},
                ),
                "metadata",
                "1",
            )
        if stage.op == "validate":
            return (
                validate(
                    first,
                    table_ids[stage.inputs[0]],
                    c.rules[stage.subject],
                    inputs,
                    run_id,
                ),
                "metadata",
                "1",
            )
        return (
            assess(
                c, inputs, stage.quality_inputs, self._cache_readiness, table_ids, refs
            ),
            "metadata",
            "1",
        )

    def _cache_readiness(self, frame):
        cached = frame.persist()
        self.frames[id(cached)] = cached
        return cached

    def _register_contract(self, name, entity):
        contract = entity.model_dump(
            include={"version", "fields", "key", "field_order"}
        )
        signature = digest(contract)
        anchor = (
            self.storage
            / "registry"
            / "entities"
            / name
            / (digest(entity.version) + ".json")
        )
        if anchor.exists():
            if json.loads(anchor.read_text())["digest"] != signature:
                raise ValueError(
                    f"Published entity contract changed: {name} {entity.version}"
                )
            return signature
        # Bootstrap from the versioned schemas of publications predating the ledger.
        for path in (self.storage / "registry" / "snapshots").glob("*.json"):
            ref = json.loads(path.read_text())
            if (
                ref.get("role") != "canonical"
                or ref["schema_version"] != entity.version
            ):
                continue
            if "entity" in ref:
                if ref["entity"] == name and ref["entity_contract"] != signature:
                    raise ValueError(
                        f"Published entity contract changed: {name} {entity.version}"
                    )
                continue
            resolved = self.output / "runs" / ref["producer"] / "resolved.json"
            if resolved.exists():
                cfg = json.loads(resolved.read_text())
            else:
                # The output root can move independently of the managed journal.
                publication = published_execution(
                    self.spark, self.storage, ref["producer"]
                )
                if publication is None:
                    raise ValueError(
                        f"Cannot verify historic contract without {resolved}"
                    )
                fact = (
                    read(self.spark, publication["metadata"])
                    .where(
                        (F.col("run_id") == ref["producer"])
                        & (F.col("kind") == "configuration")
                    )
                    .select("detail")
                    .first()
                )
                if fact is None:
                    raise ValueError(
                        f"Missing archived configuration for {ref['producer']}"
                    )
                cfg = json.loads(fact.detail)
            for stage in cfg["stages"].values():
                if stage["op"] != "map" or stage["output"] != ref["id"]:
                    continue
                if cfg["mappings"][stage["subject"]]["entity"] != name:
                    continue
                old = dict(cfg["entities"][name])
                old.setdefault(
                    "field_order",
                    [
                        f["name"]
                        for f in ref["schema"]["fields"]
                        if f["name"] in old["fields"]
                    ],
                )
                if digest({k: old[k] for k in contract}) != signature:
                    raise ValueError(
                        f"Published entity contract changed: {name} {entity.version}"
                    )
        write_json(anchor, {"digest": signature, "contract": contract})
        return signature

    def _execute(self, stage, tables, run_id, c):
        if stage.op == "ingest":
            return self._ingest(stage, c)
        inputs = [tables[name]["identity"] for name in stage.inputs]
        identity = digest(
            [
                self.build["logic_hash"],
                self.spark.version,
                self.config_hash,
                stage.model_dump(),
                inputs,
            ]
        )
        index = self.storage / "registry" / "snapshots" / (identity + ".json")
        contract = (
            self._register_contract(
                c.mappings[stage.subject].entity,
                c.entities[c.mappings[stage.subject].entity],
            )
            if stage.op == "map"
            else None
        )
        if stage.write.strategy == "snapshot" and index.exists():
            ref = json.loads(index.read_text())
            read(self.spark, ref).schema
            return ref
        frame, role, version = self._process(
            stage,
            self._inputs(stage, tables),
            run_id,
            tables,
            c,
        )
        policy = stage.write
        base = (self.storage / (policy.location or "tables") / stage.output).resolve()
        base.relative_to(self.storage)
        if policy.strategy == "append":
            batch = (
                self.storage
                / "registry"
                / "batches"
                / c.dataset
                / c.version
                / stage.output
                / (policy.batch_id + ".json")
            )
            if batch.exists() and json.loads(batch.read_text())["identity"] != identity:
                raise ValueError(
                    f"Append batch {policy.batch_id} already identifies another computation"
                )
            write_json(batch, {"identity": identity})
        target = (
            base / identity
            if policy.strategy == "snapshot"
            else base / c.dataset / c.version
        )
        ref = delta_write(
            self.spark,
            frame,
            target,
            stage.output,
            identity,
            schema_version=version,
            strategy=policy.strategy,
            keys=policy.keys,
            partitions=policy.partitions,
            evolve=policy.additive_evolution,
        )
        result = read(self.spark, ref)
        stats = {"rows": result.count()}
        if stage.op in {"validate", "readiness"}:
            table_stats = {"rows": stats["rows"], **_outcome_counts(stage.op, result)}
            if policy.strategy == "snapshot":
                stats = batch_stats = table_stats
            else:
                batch_stats = {
                    "rows": frame.count(),
                    **_outcome_counts(stage.op, frame),
                }
                stats.update(table=table_stats, batch=batch_stats)
        if stage.op == "readiness":
            population = self._inputs(stage, tables)[c.population]
            batch_stats["population_rows"] = population.count()
            identities = population_counts(population, c.patient_key)
            ambiguous = identities.where(~identifiable(c.patient_key))
            batch_stats["excluded_identity_rows"] = (
                ambiguous.agg(F.sum("count")).first()[0] or 0
            )
            batch_stats["unique_identifiable_patients"] = (
                batch_stats["population_rows"] - batch_stats["excluded_identity_rows"]
            )
            batch_stats["distinct_patient_keys"] = (
                population.select(c.patient_key)
                .where(present(F.col(c.patient_key)))
                .distinct()
                .count()
            )
        if stage.op == "validate":
            source = self._inputs(stage, tables)[stage.inputs[0]]
            counts = []
            for name, rule in c.rules[stage.subject].items():
                if rule.enabled:
                    condition = F.lit(True)
                    for field, value in rule.where.items():
                        condition = condition & (F.col(field) == value)
                    counts.append(F.count(F.when(condition, 1)).alias(name))
            batch_stats["evaluated_by_rule"] = (
                source.agg(*counts).first().asDict() if counts else {}
            )
        ref.update(
            role=role,
            dataset=c.dataset,
            delivery=c.version,
            stats=stats,
            producer=run_id,
            schema=frame.schema.jsonValue(),
            write_strategy=policy.strategy,
            merge_keys=list(policy.keys) if policy.strategy == "merge" else [],
            partitions=policy.partitions,
            keys=[c.entities[c.mappings[stage.subject].entity].key]
            if stage.op == "map"
            else [],
        )
        if stage.op == "profile" and c.export_profiles:
            result.write.mode("overwrite").json(
                str(self.output / "profiles" / identity)
            )
        if stage.op == "validate" and policy.strategy == "snapshot":
            ref["validated_table"] = tables[stage.inputs[0]]["id"]
        if contract:
            ref["entity_contract"] = contract
            ref["entity"] = c.mappings[stage.subject].entity
        ref["dependencies"] = dict(zip(stage.inputs, inputs))
        write_json(index.with_name(ref["identity"] + ".json"), ref)
        return ref

    def _published_inputs(self, ref):
        if ref["role"] == "source":
            return {}
        if "dependencies" in ref:
            return ref["dependencies"]
        # Legacy manifests recorded the same edge on the producing execution.
        producer = self.manifest(ref["producer"])
        for step in producer["stages"]:
            if step.get("output") == ref["identity"]:
                return step["inputs"]
        raise ValueError(f"Missing published dependencies for {ref['id']}")

    def run(self, stages=None, parent=None):
        # Isolate all stage work from later edits in an interactive caller.
        payload = self.config.model_dump()
        if digest(payload) != self.config_hash:
            raise ValueError(
                "Configuration changed after resolution. Create a new Research instance with overrides."
            )
        c = Config.model_validate(payload)
        parent = self.manifest(parent) if parent else None
        if parent and (parent["dataset"], parent["delivery"]) != (c.dataset, c.version):
            raise ValueError("Parent belongs to another dataset delivery")
        requested = set(c.order() if stages is None else stages)
        if requested - set(c.order()):
            raise ValueError(f"Unknown or disabled stage {requested - set(c.order())}")
        tables = dict(parent["tables"]) if parent else {}
        for alias, pin in c.bindings.items():
            source_manifest = self.manifest(pin.run)
            ref = source_manifest["tables"][pin.table]
            declared = {field["name"] for field in ref["schema"]["fields"]}
            if set(pin.columns) - declared:
                raise ValueError(f"Incompatible registered binding {alias}")
            if (ref["dataset"], ref["delivery"]) != (c.dataset, c.version):
                raise ValueError(f"Unreviewed cross-delivery binding {alias}")
            tables[alias] = ref
        producers = {s.output: n for n, s in c.stages.items() if s.enabled}
        # Explicit pins are immutable imports. Inherited derived tables must still
        # have the versions actually used by their producer, including old graphs.
        imports = set(c.bindings)
        if parent:
            imports.update(parent.get("imports", []))
            legacy_config = self.output / "runs" / parent["run_id"] / "resolved.json"
            imports.update(json.loads(legacy_config.read_text()).get("bindings", {}))
        for name in requested:
            output = c.stages[name].output
            tables.pop(output, None)
            imports.discard(output)
        lineage = {alias: self._published_inputs(ref) for alias, ref in tables.items()}
        while True:
            stale = [
                alias
                for alias, inputs in lineage.items()
                if alias in tables
                and alias not in c.bindings
                and any(
                    tables.get(key, {}).get("identity") != version
                    for key, version in inputs.items()
                    if alias not in imports
                    or (key != alias and key in parent["tables"])
                )
            ]
            if not stale:
                break
            for alias in stale:
                del tables[alias]

        def dependencies(name):
            for item in c.stages[name].inputs:
                if item in tables:
                    continue
                upstream = producers[item]
                if item not in tables and upstream not in requested:
                    requested.add(upstream)
                    dependencies(upstream)

        for name in list(requested):
            dependencies(name)
        run_id = uuid4().hex
        run_dir = self.output / "runs" / run_id
        run_dir.mkdir(parents=True)
        manifest = dict(
            run_id=run_id,
            dataset=c.dataset,
            delivery=c.version,
            parent=parent["run_id"] if parent else None,
            started=now(),
            status="running",
            config_hash=self.config_hash,
            config_files=self.config_files,
            selection=self.selection,
            pipeline_version=__version__,
            spark=self.spark.version,
            build=self.build,
            stages=[],
            tables=tables,
            imports=sorted(imports & tables.keys()),
        )
        write_json(run_dir / "resolved.json", c.model_dump())
        write_json(run_dir / "manifest.json", manifest)
        facts = [
            ("configuration", self.config_hash, c.model_dump()),
            (
                "dataset",
                c.dataset,
                {
                    "version": c.version,
                    "sources": list(c.sources),
                    "concepts": c.model_dump()["concepts"],
                    "terminology_reviews": c.model_dump()["terminology_reviews"],
                },
            ),
        ]
        with writer_lock(self.storage):
            try:
                # The OS releases this lock on process death. Inside it, a persisted
                # running attempt with the same writer root has lost its publisher.
                for prior_path in (self.output / "runs").glob("*/manifest.json"):
                    if prior_path.parent == run_dir:
                        continue
                    prior = json.loads(prior_path.read_text())
                    if prior.get("status") in {"running", "publishing"} and prior.get(
                        "writer_root"
                    ) == str(self.storage):
                        prior = published_execution(
                            self.spark, self.storage, prior["run_id"]
                        ) or dict(prior, status="interrupted", finished=now())
                        write_json(prior_path, prior)
                manifest["writer_root"] = str(self.storage)
                write_json(run_dir / "manifest.json", manifest)
                for ref in tables.values():
                    source = ref.get("source_ref", ref)
                    if source["format"] == "csv":
                        if (
                            inventory(
                                source["path"], Source.model_validate(source["reader"])
                            )
                            != source["files"]
                        ):
                            raise ValueError(f"Pinned source drift {source['id']}")
                for name in c.order():
                    if name not in requested:
                        continue
                    stage = c.stages[name]
                    record = dict(
                        stage=name,
                        op=stage.op,
                        started=now(),
                        status="running",
                        inputs={k: tables[k]["identity"] for k in stage.inputs},
                    )
                    manifest["stages"].append(record)
                    event(run_id, name, "started")
                    for binding, limit in stage.quality_limits.items():
                        violations = (
                            read(self.spark, tables[binding])
                            .where(F.col("classification") == "confirmed_violation")
                            .count()
                        )
                        if violations > limit:
                            raise ValueError(
                                f"Quality gate {binding} found {violations} confirmed violations, limit {limit}"
                            )
                    ref = self._execute(stage, tables, run_id, c)
                    tables[stage.output] = ref
                    record.update(
                        finished=now(),
                        status="success",
                        output=ref["identity"],
                        stats=ref["stats"],
                    )
                    event(run_id, name, "success", rows=ref["stats"]["rows"])
                    facts.extend(
                        [
                            ("stage", name, record),
                            ("table_registry", stage.output, ref),
                            ("schema_registry", stage.output, ref["schema"]),
                        ]
                    )
                    for key, identity in record["inputs"].items():
                        facts.append(
                            (
                                "lineage",
                                name,
                                dict(
                                    input=key,
                                    version=identity,
                                    output=stage.output,
                                    target_version=ref["identity"],
                                ),
                            )
                        )
                    if "predecessor" in ref:
                        record["target_predecessor"] = ref["predecessor"]
                        facts.append(
                            (
                                "lineage",
                                name,
                                dict(
                                    input=stage.output,
                                    relationship="target_predecessor",
                                    predecessor=ref["predecessor"],
                                    output=stage.output,
                                    target_version=ref["identity"],
                                ),
                            )
                        )
                    if stage.op == "ingest":
                        facts.append(("source_inventory", stage.subject, ref["files"]))
                    write_json(run_dir / "manifest.json", manifest)
                for alias, ref in tables.items():
                    for key, version in self._published_inputs(ref).items():
                        if alias in imports and (key == alias or key not in tables):
                            continue
                        if tables.get(key, {}).get("identity") != version:
                            raise ValueError(
                                f"Incompatible lineage for {alias}: conflicting input {key}"
                            )
                manifest.update(status="publishing", finished=now())
                write_json(run_dir / "manifest.json", manifest)
                manifest["status"] = "success"
                facts.append(("execution", run_id, dict(manifest)))
                manifest["metadata"] = journal(
                    self.spark, self.storage, manifest, facts
                )
                write_json(run_dir / "manifest.json", manifest)
            except Exception as error:
                if manifest["status"] == "success":
                    # A commit can succeed even when its acknowledgement or JSON
                    # projection fails. Never append a competing terminal decision.
                    committed = published_execution(self.spark, self.storage, run_id)
                    if committed is not None:
                        self._project_manifest(run_dir / "manifest.json", committed)
                        return committed
                    facts = [fact for fact in facts if fact[0] != "execution"]
                manifest.update(
                    status="failed", finished=now(), error=str(error)[:4000]
                )
                if manifest["stages"] and manifest["stages"][-1]["status"] == "running":
                    manifest["stages"][-1].update(status="failed", finished=now())
                write_json(run_dir / "manifest.json", manifest)
                try:
                    facts.append(("execution", run_id, dict(manifest)))
                    journal(self.spark, self.storage, manifest, facts)
                except Exception as metadata_error:
                    manifest["metadata_error"] = str(metadata_error)[:1000]
                    write_json(run_dir / "manifest.json", manifest)
                event(run_id, "execution", "failed", manifest=str(run_dir))
                raise
            finally:
                for frame in self.frames.values():
                    if frame.is_cached:
                        frame.unpersist()
                self.frames.clear()
        return manifest
