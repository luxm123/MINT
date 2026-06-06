from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.aws_client import invoke_lambda
from mint.events import InvocationEvent, SchedulerDecision, WarmupEvent, WorkflowRunSummary
from mint.intent_planner import plan_intents
from mint.metrics import compute_summary
from mint.scheduler import WarmupAction, schedule_intents
from mint.utils import append_jsonl, ensure_dir, load_yaml, monotonic_sec, new_id
from mint.workloads import get_workload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a delay-shift supplemental experiment.")
    parser.add_argument("--config", default="configs/mint_aws.yaml")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--upstream-delay-ms", type=float, default=1200)
    parser.add_argument("--baseline", choices=["static_dag", "mint_markov_full"], default="mint_markov_full")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-real-run", action="store_true")
    parser.add_argument("--output-dir", default="results/delay_shift")
    return parser.parse_args(argv)


def _dry_run_enabled(args: argparse.Namespace, config: dict[str, Any]) -> bool:
    return bool(args.dry_run or config.get("experiment", {}).get("dry_run", True))


def _shift_downstream_intents(intents: list[Any], upstream_delay_ms: float) -> list[Any]:
    shift_sec = max(0.001, upstream_delay_ms / 1000.0)
    shifted = []
    for intent in intents:
        if intent.stage > 0:
            planned = intent.planned_time_sec + shift_sec
            shifted.append(
                replace(
                    intent,
                    planned_time_sec=planned,
                    window_start_sec=planned,
                    window_end_sec=planned + max(1.0, shift_sec),
                )
            )
        else:
            shifted.append(intent)
    return shifted


def _actions_for_baseline(intents: list[Any], baseline: str, config: dict[str, Any]) -> list[WarmupAction]:
    if baseline == "static_dag":
        budget = int(config.get("experiment", {}).get("warmup_budget", 1))
        ranked = sorted(intents, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        actions = []
        for index, intent in enumerate(ranked):
            action_type = "execute" if index < budget else "replace"
            reason = "delay_shift_static_within_budget" if action_type == "execute" else "delay_shift_static_budget_exceeded"
            actions.append(WarmupAction(action_type, intent, intent.offline_gain, reason))
        return actions
    runtime_state = {
        "now_sec": 0.0,
        "call_probability": {intent.logical_name: 1.0 for intent in intents},
        "hot_until": {},
        "path_benefit": {intent.logical_name: intent.criticality for intent in intents},
    }
    return schedule_intents(intents, runtime_state, int(config.get("experiment", {}).get("warmup_budget", 1)), config)


def run_delay_shift(config: dict[str, Any], args: argparse.Namespace, dry_run: bool) -> dict[str, Any]:
    output_dir = ensure_dir(args.output_dir)
    events_path = output_dir / "events.jsonl"
    runs_path = output_dir / "runs.csv"
    summary_path = output_dir / "summary.json"
    analysis_path = output_dir / "delay_analysis.csv"
    for path in [events_path, runs_path, summary_path, analysis_path]:
        if path.exists():
            path.unlink()

    dag = get_workload("chain")
    exp = config.setdefault("experiment", {})
    exp["dag"] = "chain"
    exp["baseline"] = args.baseline
    exp["repetitions"] = args.repetitions
    exp["output_dir"] = str(output_dir)
    exp["dry_run"] = dry_run
    if args.baseline == "mint_markov_full":
        config.setdefault("planner", {})["type"] = "markov"

    aws_map = config.get("aws", {}).get("lambda_functions", {})
    platform = config.get("platform", {})
    retention_sec = float(platform.get("default_retention_sec", 300))
    cold_ms = float(platform.get("default_cold_start_ms", 800))
    warm_ms = float(platform.get("default_warm_duration_ms", 100))
    stages = dag.stages()
    run_rows = []

    for index in range(args.repetitions):
        run_id = new_id("delay-run")
        start = monotonic_sec()
        intents = _shift_downstream_intents(plan_intents(dag, config), args.upstream_delay_ms)
        actions = _actions_for_baseline(intents, args.baseline, config)
        hot_until: dict[str, float] = {}
        warm_count = 0
        cold_count = 0
        total_latency = 0.0

        for action in actions:
            intent = action.intent
            append_jsonl(
                events_path,
                SchedulerDecision(
                    event_type="scheduler_decision",
                    run_id=run_id,
                    intent_id=intent.intent_id,
                    function_name=intent.function_name,
                    logical_name=intent.logical_name,
                    action=action.action_type,
                    action_reason=action.action_reason,
                    gain=action.gain,
                    planned_time_sec=intent.planned_time_sec,
                ).to_dict(),
            )
            if action.action_type == "execute":
                invoke_lambda(
                    intent.function_name,
                    {"function_name": intent.logical_name, "run_id": run_id, "invocation_type": "warmup", "sleep_ms": 1},
                    invocation_type="Event",
                    dry_run=dry_run,
                    region_name=config.get("aws", {}).get("region"),
                )
                hot_until[intent.logical_name] = monotonic_sec() + retention_sec
                warm_count += 1
                append_jsonl(
                    events_path,
                    WarmupEvent(
                        event_type="warmup",
                        run_id=run_id,
                        function_name=intent.function_name,
                        logical_name=intent.logical_name,
                        intent_id=intent.intent_id,
                        action=action.action_type,
                        useful=True,
                        action_reason=action.action_reason,
                        gain=action.gain,
                    ).to_dict(),
                )

        for logical in ["f1", "f2", "f3"]:
            sleep_ms = args.upstream_delay_ms if logical == "f1" else 10
            was_warm = hot_until.get(logical, 0.0) > monotonic_sec()
            invoke_lambda(
                aws_map.get(logical, logical),
                {"function_name": logical, "run_id": run_id, "invocation_type": "real", "sleep_ms": sleep_ms},
                dry_run=dry_run,
                region_name=config.get("aws", {}).get("region"),
            )
            latency = warm_ms + (0.0 if was_warm else cold_ms) + (args.upstream_delay_ms if logical == "f1" else 0.0)
            total_latency += latency
            cold_count += int(not was_warm)
            hot_until[logical] = monotonic_sec() + retention_sec
            append_jsonl(
                events_path,
                InvocationEvent(
                    event_type="invocation",
                    run_id=run_id,
                    function_name=aws_map.get(logical, logical),
                    logical_name=logical,
                    invocation_type="real",
                    cold_start=not was_warm,
                    latency_ms=round(latency, 3),
                    stage=stages.get(logical, 0),
                    status="ok",
                ).to_dict(),
            )

        latency_ms = round(max((monotonic_sec() - start) * 1000.0, total_latency), 3)
        summary_event = WorkflowRunSummary(
            event_type="workflow_summary",
            run_id=run_id,
            dag="chain",
            baseline=args.baseline,
            planner_type=config.get("planner", {}).get("type", "heuristic"),
            latency_ms=latency_ms,
            cold_start_count=cold_count,
            warmup_count=warm_count,
        )
        append_jsonl(events_path, summary_event.to_dict())
        run_rows.append(summary_event.to_dict())

    with runs_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["run_id", "dag", "baseline", "planner_type", "latency_ms", "cold_start_count", "warmup_count", "status"])
        writer.writeheader()
        for row in run_rows:
            writer.writerow({key: row.get(key) for key in writer.fieldnames})

    summary = compute_summary(events_path)
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)
    with analysis_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "baseline",
                "repetitions",
                "upstream_delay_ms",
                "delay_count",
                "missed_warmup",
                "unserved_intent_cold_start",
                "cold_start_count",
                "end_to_end_latency_ms_avg",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "baseline": args.baseline,
                "repetitions": args.repetitions,
                "upstream_delay_ms": args.upstream_delay_ms,
                "delay_count": summary.get("delay_count", 0),
                "missed_warmup": summary.get("missed_warmup", 0),
                "unserved_intent_cold_start": summary.get("unserved_intent_cold_start", 0),
                "cold_start_count": summary.get("cold_start_count", 0),
                "end_to_end_latency_ms_avg": summary.get("end_to_end_latency_ms_avg", 0),
            }
        )
    return summary


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_yaml(args.config)
    dry_run = _dry_run_enabled(args, config)
    if not dry_run and not args.confirm_real_run:
        print("Refusing real AWS delay-shift run: pass --confirm-real-run when dry-run is disabled.", file=sys.stderr)
        return 2
    summary = run_delay_shift(config, args, dry_run)
    print(f"dry_run={dry_run}")
    print(f"output_dir={args.output_dir}")
    print(f"summary={summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
