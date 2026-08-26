"""Apply Azure Functions public trace calibration to a MINT experiment config.

Usage:
    python scripts/download_azure_trace.py --output-dir data/azure_trace --days 1
    python scripts/apply_trace_calibration.py \
        --trace-dir data/azure_trace \
        --config configs/mint_aws_real.yaml \
        --output configs/mint_aws_real_tracecal.yaml

    # legacy per-invocation CSV path:
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
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.trace_profile import (
    apply_trace_calibration,
    calibrate_branch_probabilities,
    flatten_branch_probabilities,
    load_aggregate_profile,
    load_trace_profile,
)
from mint.utils import load_yaml
from mint.workloads import get_workload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calibrate a MINT config from a serverless invocation trace."
    )
    parser.add_argument(
        "--trace",
        help="Per-invocation trace CSV (legacy Azure per-invocation format).",
    )
    parser.add_argument(
        "--trace-dir",
        help=(
            "Directory with the official Azure 2019 aggregate CSVs "
            "(invocations_per_function_md / function_durations_percentiles / "
            "app_memory_percentiles)."
        ),
    )
    parser.add_argument("--config", required=True, help="Source experiment YAML.")
    parser.add_argument(
        "--output",
        default="configs/mint_aws_real_tracecal.yaml",
        help="Output calibrated YAML (default: configs/mint_aws_real_tracecal.yaml).",
    )
    parser.add_argument(
        "--dags",
        nargs="+",
        default=["wide_branch", "deep_mixed"],
        help="DAGs to calibrate branch probabilities for.",
    )
    return parser.parse_args(argv)


def _day_from_name(path: Path) -> int:
    match = re.search(r"\.d(\d{2})\.", path.name)
    return int(match.group(1)) if match else -1


def find_aggregate_trace_files(
    trace_dir: str | Path,
) -> tuple[Path, Path, Path | None]:
    """Locate the official 2019 aggregate CSVs in a directory.

    Pairs the invocations and durations files by day (preferring the lowest
    day) and reuses the last published memory day when the chosen day lacks a
    memory file (days 13/14 are not published).
    """
    trace_dir = Path(trace_dir)
    invocations = {
        _day_from_name(path): path
        for path in trace_dir.glob("invocations_per_function_md.anon.d*.csv")
    }
    durations = {
        _day_from_name(path): path
        for path in trace_dir.glob("function_durations_percentiles.anon.d*.csv")
    }
    memory = {
        _day_from_name(path): path
        for path in trace_dir.glob("app_memory_percentiles.anon.d*.csv")
    }
    if not invocations or not durations:
        raise ValueError(
            "--trace-dir must contain invocations_per_function_md.anon.d*.csv "
            "and function_durations_percentiles.anon.d*.csv"
        )
    common_days = sorted(set(invocations) & set(durations))
    day = common_days[0]
    memory_path = memory.get(day)
    if memory_path is None and memory:
        memory_path = memory[max(memory)]
    return invocations[day], durations[day], memory_path


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if bool(args.trace) == bool(args.trace_dir):
        print(
            "Exactly one of --trace or --trace-dir is required",
            file=sys.stderr,
        )
        return 2
    config = load_yaml(args.config)
    if args.trace_dir:
        invocations, durations, memory = find_aggregate_trace_files(args.trace_dir)
        print(f"aggregate invocations: {invocations}")
        print(f"aggregate durations:   {durations}")
        print(f"aggregate memory:      {memory if memory is not None else '(default 128 MiB)'}")
        profile = load_aggregate_profile(
            invocations,
            durations,
            memory,
            source=str(invocations.resolve()),
        )
    else:
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
    print(f"total trace invocations: {profile.total_invocations}")
    print(f"trace functions: {len(profile.call_counts)}")
    print(
        "branch probabilities: "
        f"{calibration.get('branch_probabilities', {}).get('wide_branch')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
