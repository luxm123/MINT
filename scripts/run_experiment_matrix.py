from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import itertools
import json
import random
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.controller import MintController
from mint.branch_history import read_branch_records
from mint.utils import append_jsonl, ensure_dir, load_yaml
from mint.workloads import get_workload


SUMMARY_FIELDS = [
    "timestamp",
    "dag",
    "baseline",
    "planner_type",
    "effective_planner",
    "budget",
    "repetitions",
    "output_dir",
    "workflow_runs",
    "end_to_end_latency_ms_avg",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "cold_start_count",
    "cold_start_rate",
    "total_warmup",
    "useful_warmup",
    "wasted_warmup",
    "missed_warmup",
    "unserved_intent_cold_start",
    "uncovered_cold_start",
    "useful_warmup_ratio",
    "execute_count",
    "delay_count",
    "cancel_pending_count",
    "invalidate_executed_count",
    "replacement_warmup_count",
    "cancel_count",
    "replace_count",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a reproducible MINT experiment matrix.")
    parser.add_argument("--config", default="configs/mint_aws.yaml")
    parser.add_argument("--dags", nargs="+", default=["chain", "fanout", "branch", "join"])
    parser.add_argument(
        "--baselines",
        nargs="+",
        default=["no_warmup", "periodic_keepwarm", "static_dag", "orion_like", "mint_offline", "mint_full"],
    )
    parser.add_argument("--budgets", nargs="+", type=int, default=[1, 2, 3])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--cooldown-sec", type=float, default=0.0)
    parser.add_argument("--profile-mismatch", action="store_true", help="Enable controlled branch-profile mismatch for adaptive stress DAGs.")
    parser.add_argument("--timing-jitter-ms", type=float, default=0.0, help="Controlled per-stage timing jitter for adaptive stress DAGs.")
    parser.add_argument("--branch-seed", type=int, default=None, help="Override the config's deterministic branch seed.")
    parser.add_argument("--randomize-order", action="store_true")
    parser.add_argument("--order-seed", type=int, default=0, help="Seed used only to randomize matrix configuration order.")
    parser.add_argument("--dry-run", action="store_true", help="Never call AWS; enabled by default unless --confirm-real-run is used.")
    parser.add_argument("--confirm-real-run", action="store_true", help="Required to allow real AWS Lambda invocation.")
    parser.add_argument("--output-root", default="results/matrix")
    return parser.parse_args(argv)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)


def effective_planner_for_baseline(baseline: str) -> str:
    mapping = {
        "no_warmup": "none",
        "periodic_keepwarm": "periodic",
        "static_dag": "static",
        "static_dag_unlimited": "static",
        "orion_like": "orion_like",
        "path_aware_greedy": "runtime_greedy",
        "xanadu_like": "xanadu_like",
        "oracle_path": "oracle",
        "mint_offline": "heuristic",
        "mint_offline_unlimited": "heuristic",
        "mint_full": "heuristic",
        "mint_markov_offline": "markov",
        "mint_markov_no_runtime_reval": "markov",
        "mint_markov_no_long_horizon": "markov",
        "mint_markov_full": "markov",
    }
    return mapping.get(baseline, "unknown")


def _write_matrix_outputs(output_root: Path, rows: list[dict[str, Any]], manifest: dict[str, Any]) -> None:
    ensure_dir(output_root)
    csv_path = output_root / "summary_matrix.csv"
    json_path = output_root / "summary_matrix.json"
    manifest_path = output_root / "experiment_manifest.json"

    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows([{field: row.get(field, "") for field in SUMMARY_FIELDS} for row in rows])
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2, sort_keys=True)
    with manifest_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)


def _materialize_initial_history(base_config: dict[str, Any]) -> list[str]:
    planner = base_config.setdefault("planner", {})
    records = list(planner.get("historical_branch_records", []))
    history_path = planner.pop("branch_history_path", None)
    if history_path:
        records.extend(read_branch_records(Path(history_path)))
    planner["historical_branch_records"] = records
    return records


def _adaptive_branch_trace(config: dict[str, Any], repetitions: int, seed: int) -> list[str]:
    experiment = config.get("experiment", {})
    configured = list(experiment.get("branch_trace", []))
    if configured:
        return [str(configured[index % len(configured)]) for index in range(repetitions)]
    branches = ("f2", "f3", "f4", "f5")
    phases = experiment.get("branch_probability_phases", [])
    trace: list[str] = []
    for index in range(repetitions):
        probabilities = next(
            (
                phase.get("probabilities", {})
                for phase in phases
                if int(phase.get("start", 0)) <= index < int(phase.get("end", 2**31))
            ),
            {branch: 0.25 for branch in branches},
        )
        weights = [max(0.0, float(probabilities.get(branch, 0.0))) for branch in branches]
        trace.append(random.Random(f"{seed}:{index}").choices(branches, weights=weights, k=1)[0])
    return trace


def _run_one(
    base_config: dict[str, Any],
    dag_name: str,
    baseline: str,
    budget: int,
    repetitions: int,
    dry_run: bool,
    output_root: Path,
    timestamp: str,
    profile_mismatch: bool = False,
    timing_jitter_ms: float = 0.0,
    branch_seed: int = 0,
    branch_trace: list[str] | None = None,
) -> dict[str, Any]:
    config = copy.deepcopy(base_config)
    exp = config.setdefault("experiment", {})
    exp["dag"] = dag_name
    exp["baseline"] = baseline
    exp["warmup_budget"] = budget
    exp["repetitions"] = repetitions
    exp["dry_run"] = dry_run
    exp["profile_mismatch"] = profile_mismatch
    exp["timing_jitter_ms"] = timing_jitter_ms
    exp["branch_seed"] = branch_seed
    if branch_trace is not None:
        exp["branch_trace"] = list(branch_trace)

    run_stamp = _utc_timestamp()
    output_dir = output_root / f"{run_stamp}_{_safe_name(dag_name)}_{_safe_name(baseline)}_B{budget}"
    exp["output_dir"] = str(output_dir)

    print(f"START dag={dag_name} baseline={baseline} budget={budget} repetitions={repetitions} dry_run={dry_run}")
    controller = MintController(
        config=config,
        dag=get_workload(dag_name),
        baseline=baseline,
        dry_run=dry_run,
        output_dir=output_dir,
    )
    summary = controller.run(repetitions)
    with (output_dir / "summary.json").open("r", encoding="utf-8") as fh:
        summary = json.load(fh)

    row = {
        "timestamp": timestamp,
        "dag": dag_name,
        "baseline": baseline,
        "planner_type": controller.planner_type,
        "effective_planner": effective_planner_for_baseline(baseline),
        "budget": budget,
        "repetitions": repetitions,
        "output_dir": str(output_dir),
    }
    row.update(summary)
    print(f"END dag={dag_name} baseline={baseline} budget={budget} summary={summary}")
    return row


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    dry_run = bool(args.dry_run or not args.confirm_real_run)
    if not dry_run and not args.confirm_real_run:
        print("Refusing real AWS matrix run: pass --confirm-real-run when dry-run is disabled.", file=sys.stderr)
        return 2

    output_root = ensure_dir(args.output_root)
    failed_path = output_root / "failed_runs.jsonl"
    failed_path.write_text("", encoding="utf-8")

    base_config = load_yaml(args.config)
    initial_history = _materialize_initial_history(base_config)
    resolved_branch_seed = int(
        args.branch_seed
        if args.branch_seed is not None
        else base_config.get("experiment", {}).get("branch_seed", 0)
    )
    planner_cfg = base_config.get("planner", {})
    history_fingerprint = hashlib.sha256(
        json.dumps(initial_history, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    timestamp = _utc_timestamp()
    configs = list(itertools.product(args.dags, args.baselines, args.budgets))
    if args.randomize_order:
        random.Random(args.order_seed).shuffle(configs)
    traces = {
        dag_name: _adaptive_branch_trace(base_config, args.repetitions, resolved_branch_seed)
        for dag_name in args.dags
        if dag_name == "adaptive_branch"
    }

    manifest = {
        "timestamp": timestamp,
        "config": args.config,
        "dags": args.dags,
        "baselines": args.baselines,
        "budgets": args.budgets,
        "repetitions": args.repetitions,
        "cooldown_sec": args.cooldown_sec,
        "profile_mismatch": args.profile_mismatch,
        "timing_jitter_ms": args.timing_jitter_ms,
        "branch_seed": resolved_branch_seed,
        "randomize_order": args.randomize_order,
        "order_seed": args.order_seed,
        "realized_config_order": [list(item) for item in configs],
        "dry_run": dry_run,
        "planner_type": base_config.get("planner", {}).get("type", "heuristic"),
        "output_root": str(output_root),
        "run_count": len(configs),
        "history_isolation": "independent_controller_per_strategy_from_deepcopied_config",
        "initial_history_size": len(initial_history),
        "initial_history_sha256": history_fingerprint,
        "branch_history_window": planner_cfg.get("branch_history_window"),
        "branch_prior_alpha": planner_cfg.get("branch_prior_alpha", 0.0),
        "materialized_branch_traces": traces,
        "branch_trace_sha256": {
            dag_name: hashlib.sha256(json.dumps(trace, separators=(",", ":")).encode("utf-8")).hexdigest()
            for dag_name, trace in traces.items()
        },
    }
    rows: list[dict[str, Any]] = []
    _write_matrix_outputs(output_root, rows, manifest)

    for index, (dag_name, baseline, budget) in enumerate(configs):
        try:
            row = _run_one(
                base_config,
                dag_name,
                baseline,
                budget,
                args.repetitions,
                dry_run,
                output_root,
                timestamp,
                args.profile_mismatch,
                args.timing_jitter_ms,
                resolved_branch_seed,
                traces.get(dag_name),
            )
            rows.append(row)
            _write_matrix_outputs(output_root, rows, manifest)
        except Exception as exc:
            failure = {
                "timestamp": _utc_timestamp(),
                "dag": dag_name,
                "baseline": baseline,
                "budget": budget,
                "repetitions": args.repetitions,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
            append_jsonl(failed_path, failure)
            print(f"FAILED dag={dag_name} baseline={baseline} budget={budget} error={exc}", file=sys.stderr)
        if args.cooldown_sec > 0 and index < len(configs) - 1:
            print(f"Cooling down for {args.cooldown_sec} sec")
            time.sleep(args.cooldown_sec)

    _write_matrix_outputs(output_root, rows, manifest)
    print(f"Wrote {output_root / 'summary_matrix.csv'}")
    print(f"Wrote {output_root / 'summary_matrix.json'}")
    print(f"Wrote {failed_path}")
    print(f"Wrote {output_root / 'experiment_manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
