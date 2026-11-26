import argparse
import json
from .pipeline import Research


def overrides_json(text):
    try:
        value = json.loads(text)
    except ValueError:
        raise argparse.ArgumentTypeError("Overrides must be a valid JSON object") from None
    if not isinstance(value, dict):
        raise argparse.ArgumentTypeError("Overrides must be a JSON object")
    return value


def main():
    parser = argparse.ArgumentParser(description="Publish a MedLens research snapshot")
    parser.add_argument("--dataset")
    parser.add_argument("--environment", default="local")
    parser.add_argument("--experiment")
    parser.add_argument("--overrides", type=overrides_json, default={})
    parser.add_argument(
        "--stage", action="append", help="Repeat to select stage IDs, with dependencies"
    )
    parser.add_argument("--parent", help="Published run ID with pinned input tables")
    args = parser.parse_args()
    research = Research(args.dataset, args.environment, args.experiment, args.overrides)
    try:
        manifest = research.run(args.stage, args.parent)
        print(research.output / "runs" / manifest["run_id"] / "manifest.json")
    finally:
        research.close()
