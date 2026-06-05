from __future__ import annotations

import argparse
import copy
import csv
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
from mint.utils import append_jsonl, ensure_dir, load_yaml
from mint.workloads import get_workload


SUMMARY_FIELDS = [
    "timestamp",
    "dag",
    "baseline",
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
    "uncovered_cold_start",
    "useful_warmup_ratio",
    "execute_count",
    "delay_count",
    "cancel_count",
    "replace_count",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a reproducible MINT experiment matrix.")
    parser.add_argument("--config", default="configs/mint_aws.yaml")
    parser.add_argument("--dags", nargs="+", default=["chain", "fanout", "branch", "join"])
    parser.add_argument("--baselines", nargs="+", default=["no_warmup", "static_dag", "mint_offline", "mint_full"])
    parser.add_argument("--budgets", nargs="+", type=int, default=[1, 2, 3])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--cooldown-sec", type=float, default=0.0)
    parser.add_argument("--randomize-order", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Never call AWS; enabled by default unless --confirm-real-run is used.")
    parser.add_argument("--confirm-real-run", action="store_true", help="Required to allow real AWS Lambda invocation.")
    parser.add_argument("--output-root", default="results/matrix")
    return parser.parse_args(argv)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)


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


def _run_one(
    base_config: dict[str, Any],
    dag_name: str,
    baseline: str,
    budget: int,
    repetitions: int,
    dry_run: bool,
    output_root: Path,
    timestamp: str,
) -> dict[str, Any]:
    config = copy.deepcopy(base_config)
    exp = config.setdefault("experiment", {})
    exp["dag"] = dag_name
    exp["baseline"] = baseline
    exp["warmup_budget"] = budget
    exp["repetitions"] = repetitions
    exp["dry_run"] = dry_run

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
    timestamp = _utc_timestamp()
    configs = list(itertools.product(args.dags, args.baselines, args.budgets))
    if args.randomize_order:
        random.shuffle(configs)

    manifest = {
        "timestamp": timestamp,
        "config": args.config,
        "dags": args.dags,
        "baselines": args.baselines,
        "budgets": args.budgets,
        "repetitions": args.repetitions,
        "cooldown_sec": args.cooldown_sec,
        "randomize_order": args.randomize_order,
        "dry_run": dry_run,
        "output_root": str(output_root),
        "run_count": len(configs),
    }
    rows: list[dict[str, Any]] = []
    _write_matrix_outputs(output_root, rows, manifest)

    for index, (dag_name, baseline, budget) in enumerate(configs):
        try:
            row = _run_one(base_config, dag_name, baseline, budget, args.repetitions, dry_run, output_root, timestamp)
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
