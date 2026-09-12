"""Freeze/evaluate a P2 reference set; never deploy or activate an artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from maigret.web.pipeline_probability import canonical_json, digest
from maigret.web.pipeline_probability_eval import (
    build_split_manifest,
    export_reviewed_artifact,
    train_and_evaluate,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["freeze", "evaluate", "export-reviewed"])
    parser.add_argument("--dataset")
    parser.add_argument("--split")
    parser.add_argument("--event", choices=["account_attribution", "claim_correctness"])
    parser.add_argument("--evaluation")
    parser.add_argument("--approval-reference")
    parser.add_argument("--output")
    parser.add_argument("--model-id")
    args = parser.parse_args()
    if args.action == "export-reviewed":
        if not args.evaluation or not args.approval_reference or not args.output:
            parser.error(
                "export-reviewed requires --evaluation, --approval-reference and --output"
            )
        evaluated = json.loads(Path(args.evaluation).read_text(encoding="utf-8"))
        artifact = export_reviewed_artifact(evaluated, args.approval_reference)
        with Path(args.output).open("xb") as stream:
            stream.write(canonical_json(artifact))
        print(
            json.dumps(
                {
                    "artifact": args.output,
                    "artifact_sha256": artifact["artifact_sha256"],
                    "note": "Runtime activation still requires this exact externally configured digest",
                }
            )
        )
        return
    if not args.dataset or not args.split or not args.event:
        parser.error("freeze/evaluate require --dataset, --split and --event")
    dataset = json.loads(Path(args.dataset).read_text(encoding="utf-8"))
    split_path = Path(args.split)
    if args.action == "freeze":
        manifest = build_split_manifest(dataset, args.event)
        with split_path.open("x", encoding="utf-8") as stream:
            json.dump(manifest, stream, indent=2, ensure_ascii=False, allow_nan=False)
        print(
            json.dumps(
                {
                    "split": str(split_path),
                    "manifest_digest": manifest["manifest_digest"],
                    "counts": {
                        name: len(ids) for name, ids in manifest["splits"].items()
                    },
                }
            )
        )
        return
    if not args.output or not args.model_id:
        parser.error("evaluate requires --output and --model-id")
    if Path(args.output).exists():
        parser.error("output already exists; frozen reports cannot be overwritten")
    manifest = json.loads(split_path.read_text(encoding="utf-8"))
    # Exclusive durable marker precedes all fitting; after a crash a reviewer must
    # investigate the consumed evaluation rather than silently retrying test tuning.
    marker = split_path.with_name(split_path.name + ".evaluation-used")
    with marker.open("x", encoding="utf-8") as stream:
        json.dump(
            {
                "split_digest": digest(manifest),
                "model_id": args.model_id,
                "output": args.output,
                "status": "locked_test_consumed",
            },
            stream,
        )
    result = train_and_evaluate(
        dataset, manifest, event=args.event, model_id=args.model_id
    )
    with Path(args.output).open("xb") as stream:
        stream.write(canonical_json(result))
    print(
        json.dumps(
            {
                "report": args.output,
                "artifact_sha256": result["artifact_sha256"],
                "gates": result["report"]["gates"],
                "release_eligible": result["report"]["release_eligible"],
                "note": "Review and separately pin an eligible artifact before runtime activation",
            }
        )
    )


if __name__ == "__main__":
    main()
