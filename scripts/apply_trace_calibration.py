"""Apply Azure Functions public trace calibration to a MINT experiment config.

Usage:
    python scripts/download_azure_trace.py --output-dir data/azure_trace --days 1 2
    python scripts/apply_trace_calibration.py \
        --trace data/azure_trace/function_benchmark_data_1.csv \
        --config configs/mint_aws_real.yaml \
        --output configs/mint_aws_real_tracecal.yaml

The script calibrates the core adaptive stress DAGs (wide_branch and
deep_mixed) from the same trace:

  * branch probabilities: when the trace has no functions named like our DAG
    nodes, the top-k most-called trace functions are mapped to the branch
    successors by frequency rank (documented convention, see
    mint.trace_profile.calibrate_branch_probabilities), preserving the
    trace's call-frequency skew;
  * stage gap / warm duration / cold-start model: trace quantiles.

The trace source is recorded in experiment.trace_calibration.source for
reproducibility.  planner.branch_probabilities is left as the wide_branch
flat map (the multi-branch consumer in the Markov model); deep_mixed uses
planner.branch_probability_left.  Run the matrix with the generated config,
e.g.  bash scripts/run_core_matrix.sh --real configs/mint_aws_real_tracecal.yaml
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

from mint.trace_profile import (
    apply_trace_calibration,
    calibrate_branch_probabilities,
    flatten_branch_probabilities,
    load_trace_profile,
)
from mint.utils import load_yaml
from mint.workloads import get_workload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calibrate a MINT config from a serverless invocation trace."
    )
    parser.add_argument("--trace", required=True, help="Per-invocation trace CSV.")
    parser.add_argument("--config", required=True, help="Source experiment YAML.")
    parser.add_argument("--output", required=True, help="Output calibrated YAML.")
    parser.add_argument(
        "--dags",
        nargs="+",
        default=["wide_branch", "deep_mixed"],
        help="DAGs to calibrate branch probabilities for.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_yaml(args.config)
    profile = load_trace_profile(args.trace, source=str(Path(args.trace).resolve()))
    for dag_name in args.dags:
        apply_trace_calibration(config, profile, get_workload(dag_name))
    if "wide_branch" in args.dags:
        # The Markov multi-branch model consumes the flat 4-branch map.
        config.setdefault("planner", {})["branch_probabilities"] = dict(
            flatten_branch_probabilities(
                calibrate_branch_probabilities(profile, get_workload("wide_branch"))
            )
        )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(config, fh, sort_keys=False)
    calibration: dict[str, Any] = config["experiment"].get("trace_calibration", {})
    print(f"Wrote {output_path}")
    print(f"trace source: {calibration.get('source')}")
    print(f"branch mapping: {calibration.get('branch_mapping')}")
    print(f"calibrated DAGs: {sorted(calibration.get('branch_probabilities', {}))}")
    print(
        "branch probabilities: "
        f"{calibration.get('branch_probabilities', {}).get('wide_branch')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
