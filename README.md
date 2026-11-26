# MedLens

MedLens is a local research framework for studying how healthcare data quality affects the evidence available for clinical reasoning. Phase 1 registers immutable sources, profiles and validates records, maps them into canonical clinical entities and reports cohort eligibility. Each published run has a resolved configuration, exact table versions and an audit trail to the original bytes.

## Run the supplied study

Use Docker with the Compose plugin. Compose shares a small Linux-managed volume for writer locks across the notebook service and one-shot containers. The supplied local environment requests two Spark cores and a 3 GiB driver. The initial image build downloads the pinned Python packages and Delta JARs. A running image can execute the study offline. Keep the project on a local filesystem with working file locks.

Place the six Synthea files in `data/raw/synthea/csv/`. From the repository root:

```sh
scripts/start-lab
docker compose exec -T lab medlens --dataset synthea
```

For a self-contained check with the bundled independent dataset:

```sh
docker compose exec -T lab medlens --dataset clinic
docker compose exec -T lab pytest -q
```

The CLI prints the path to the published manifest. In the container, `/work/outputs` maps to `./outputs` on the host. Each execution writes `runs/<run-id>/manifest.json` and `resolved.json`. Use a manifest whose status is `success`. Its `tables` dictionary contains logical references, roles, schemas, locations, versions and statistics. For the default Synthea graph, `tables.readiness.stats.tasks` contains the patient counts by task and status. Statuses with no patients have a count of zero.

To run the walkthrough interactively, retrieve the Jupyter access token locally with `docker compose exec lab jupyter server list`, then open `http://127.0.0.1:8891/lab` and enter the token. Set `MEDLENS_PORT` before starting Compose to choose another host port. The five notebooks under `notebooks/` run in numeric order, each with a fresh kernel. To execute and save all five:

```sh
docker compose exec -T lab sh -ec 'mkdir -p outputs/executed; jupyter nbconvert --execute --to notebook --ExecutePreprocessor.timeout=600 --output-dir=/work/outputs/executed notebooks/*.ipynb'
```

The raw mount is read-only. Delta tables, notebooks and output manifests persist on the host across container recreation. Use `docker compose down` to stop the service. Raw datasets and generated outputs are excluded from Git and from the build context. Version the small YAML contracts and test fixtures alongside the package.

## Select stages and reproduce a snapshot

Stage IDs are declared in each dataset’s `pipeline.yaml`. Selecting a stage automatically includes its enabled upstream producers. An existing manifest can supply pinned inputs. For example:

```sh
docker compose exec -T lab medlens --dataset clinic --stage profile_labs
docker compose exec -T lab medlens --dataset synthea --stage assess --experiment longitudinal
```

To branch from a published run, supply its ID to `--parent`. For instance, a full clinic run can be the parent of `--stage profile_labs --parent RUN_ID --overrides '{"metrics":["rows","missing"]}'`. This creates another run with its own configuration and output reference. Each parent remains readable. A run that changes an upstream producer drops inherited descendants whose inputs could be stale.

From a notebook or Python process launched in the image:

```python
from medlens.pipeline import Research
from medlens.storage import read

study = Research("clinic")
snapshot = study.run()
decisions = study.frame(snapshot, "readiness")
decisions.select("task", "status", "criterion_counts").show()
study.frame(snapshot, "issues_labs").select("rule_id", "source_record_id").show()
read(study.spark, snapshot["metadata"]).where("kind = 'lineage'").show(truncate=False)
study.close()
```

To restore a published run in a new session, use `study.manifest(run_id)` and `study.frame(manifest, table_id)`. Keep the original immutable files, entire Delta directories including transaction logs, configuration snapshot and manifests at their registered mount paths. A file inventory digest identifies a source delivery. Managed table references identify a specific Delta table and commit. The supplied pipeline performs no vacuum or snapshot expiration.

## Onboard a dataset

1. Create `configs/datasets/<id>/` using `clinic/` as a working example. Put the files under a read-only mount accessible to the local Spark process. Give the delivery a version that identifies this exact set of bytes.
2. In `dataset.yaml`, declare the files, ordered case-sensitive headers, dialect, null token, keys, logical types and semantic column roles. Register sources in place by default. For a particular source, `materialize_source: true` also publishes a source-shaped Delta snapshot with a reference to the original files.
3. In `mappings.yaml`, map every field of the appropriate canonical entity. Use a typed literal null for unavailable fields. Preserve native codes, units, source values and time precision. Record the meaning and scope of each concept mapping. The supplied observation mappings are dataset-local.
4. In `policies.yaml`, declare rule IDs, severity, classification, clinical concept filters, exclusions and task thresholds. In `pipeline.yaml`, bind operators to logical input/output IDs and enable the required stages. The inputs of a reference check or lookup must include its reference table. Configure which entities are available to readiness.
5. Run the new dataset first on a small fixture with hand-calculated expectations. Inspect source counts, issue locators, canonical mapping flags and task evidence. Repeat under the same delivery version and compare outputs. Allocate a new version when source bytes change.

Configuration precedence runs from framework defaults through entity contracts, dataset, environment, experiment and explicit overrides. Use `--environment`, `--experiment` and a JSON object passed to `--overrides`. Dictionaries merge by key, including stable rule and stage IDs. Lists replace as a unit. Duplicate YAML keys and conflicting definitions within a layer fail preflight. An entity contract change requires a new schema version. Set canonical column order with `field_order`, bind issue tables with `quality_inputs`, and use `uncertain_fields` for unresolved terminology in eligibility criteria. Publication, lineage and recovery are documented in the [architecture guide](docs/architecture/design.md).

The checked-in implementation exposes a local Python API and CLI. See the [architecture and extension guide](docs/architecture/design.md), [research protocol](docs/research/protocol.md), [architecture decisions](docs/adr/0001-phase-one.md), [verification record](docs/verification/phase-1.md), and [approved implementation plan](docs/phase-1-plan.md).

### Release provenance and public configuration

Run `scripts/start-lab` from the checkout to build and start the local lab with Git and Docker provenance. For a release candidate, use `scripts/start-lab --release` from a clean, committed checkout, then execute the pipeline in that service with `docker compose exec -T lab medlens --dataset synthea`. Each manifest records the source digest, build-input hashes, installed Python distributions, actual Spark, Java and Delta versions, Git revision and dirty state, and the local Docker image ID. Retain the image alongside research artifacts for archival replay. Direct builds without checkout evidence or an injected image ID leave the corresponding fields null.

Resolved configurations, including YAML text, are public research artifacts. Store credentials outside them. Phase 1 local sources require no credentials. To protect a known value from accidental inclusion anywhere in the policy, supply it to the application process through an environment variable prefixed `MEDLENS_SECRET_`. The resolver rejects matching literals. Runtime rule messages must select existing YAML rule text or an entry in the YAML `public_rule_messages` list. Register new public wording in a dataset overlay. Rejected configuration attempts record the failure type and attempt ID. Previously published snapshots retain their historical contents.

A reference rule can specify `match_fields` as a mapping from child columns to parent columns. MedLens first requires exactly one parent for a present foreign key and then checks each configured field on that parent. The shipped encounter-reference rules use this to confirm patient ownership and flag mismatches on the original source occurrence. Blank foreign keys retain the rule's optional-link semantics; configure a required rule when encounter evidence is mandatory. For pinned external quality inputs, declare their additional rule identifiers with `bindings.<alias>.rule_ids`.
