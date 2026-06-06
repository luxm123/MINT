from __future__ import annotations

import argparse
import copy
import csv
import json
import sys
import time
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
    parser.add_argument("--baseline", choices=["static_dag", "mint_markov_full"])
    parser.add_argument("--baselines", nargs="+", choices=["static_dag", "mint_markov_full"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-real-run", action="store_true")
    parser.add_argument("--output-dir", default="results/delay_shift")
    return parser.parse_args(argv)


def _dry_run_enabled(args: argparse.Namespace, config: dict[str, Any]) -> bool:
    return bool(args.dry_run or config.get("experiment", {}).get("dry_run", True))


def _selected_baselines(args: argparse.Namespace) -> list[str]:
    if args.baselines:
        return args.baselines
    if args.baseline:
        return [args.baseline]
    return ["mint_markov_full"]


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
        return [
            WarmupAction(
                "execute" if index < budget else "replace",
                intent,
                intent.offline_gain,
                "delay_shift_static_within_budget" if index < budget else "delay_shift_static_budget_exceeded",
            )
            for index, intent in enumerate(ranked)
        ]
    runtime_state = {
        "now_sec": 0.0,
        "call_probability": {intent.logical_name: 1.0 for intent in intents},
        "hot_until": {},
        "path_benefit": {intent.logical_name: intent.criticality for intent in intents},
    }
    return schedule_intents(intents, runtime_state, int(config.get("experiment", {}).get("warmup_budget", 1)), config)


def _invoke_warmup(
    *,
    events_path: Path,
    run_id: str,
    intent: Any,
    action_reason: str,
    gain: float,
    dry_run: bool,
    region: str | None,
    delayed: bool,
    original_planned_time_sec: float | None = None,
    delayed_warmup_time_sec: float | None = None,
    delay_reason: str | None = None,
) -> None:
    invoke_lambda(
        intent.function_name,
        {"function_name": intent.logical_name, "run_id": run_id, "invocation_type": "warmup", "sleep_ms": 1},
        invocation_type="Event",
        dry_run=dry_run,
        region_name=region,
    )
    event = WarmupEvent(
        event_type="warmup",
        run_id=run_id,
        function_name=intent.function_name,
        logical_name=intent.logical_name,
        intent_id=intent.intent_id,
        action="delayed_execute" if delayed else "execute",
        useful=True,
        action_reason=action_reason,
        gain=gain,
    ).to_dict()
    if delayed:
        delay_duration = max(0.0, float(delayed_warmup_time_sec or 0.0))
        event.update(
            {
                "original_planned_time_sec": original_planned_time_sec,
                "delayed_warmup_time_sec": delayed_warmup_time_sec,
                "delay_reason": delay_reason,
                "delay_duration_sec": delay_duration,
                "served_after_delay": True,
            }
        )
    append_jsonl(events_path, event)


def _run_one_baseline(
    *,
    config: dict[str, Any],
    baseline: str,
    repetitions: int,
    upstream_delay_ms: float,
    dry_run: bool,
    output_dir: Path,
) -> dict[str, Any]:
    events_path = output_dir / "events.jsonl"
    runs_path = output_dir / "runs.csv"
    summary_path = output_dir / "summary.json"
    for path in [events_path, runs_path, summary_path]:
        if path.exists():
            path.unlink()
    dag = get_workload("chain")
    config = copy.deepcopy(config)
    exp = config.setdefault("experiment", {})
    exp["dag"] = "chain"
    exp["baseline"] = baseline
    exp["repetitions"] = repetitions
    exp["output_dir"] = str(output_dir)
    exp["dry_run"] = dry_run
    if baseline == "mint_markov_full":
        config.setdefault("planner", {})["type"] = "markov"

    aws_map = config.get("aws", {}).get("lambda_functions", {})
    region = config.get("aws", {}).get("region")
    platform = config.get("platform", {})
    retention_sec = float(platform.get("default_retention_sec", 300))
    cold_ms = float(platform.get("default_cold_start_ms", 800))
    warm_ms = float(platform.get("default_warm_duration_ms", 100))
    stages = dag.stages()
    run_rows = []
    delayed_execute_count = 0
    served_after_delay_count = 0
    delay_saved_cold_start_count = 0
    controller_wall_clock_ms_total = 0.0

    for _ in range(repetitions):
        run_id = new_id("delay-run")
        start = monotonic_sec()
        intents = _shift_downstream_intents(plan_intents(dag, config), upstream_delay_ms)
        actions = _actions_for_baseline(intents, baseline, config)
        hot_until: dict[str, float] = {}
        delayed_queue: dict[str, WarmupAction] = {}
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
                _invoke_warmup(
                    events_path=events_path,
                    run_id=run_id,
                    intent=intent,
                    action_reason=action.action_reason,
                    gain=action.gain,
                    dry_run=dry_run,
                    region=region,
                    delayed=False,
                )
                hot_until[intent.logical_name] = monotonic_sec() + retention_sec
                warm_count += 1
            elif action.action_type == "delay":
                delayed_queue[intent.logical_name] = action

        for logical in ["f1", "f2", "f3"]:
            if logical != "f1" and logical in delayed_queue:
                action = delayed_queue[logical]
                delayed_time = float(action.scheduled_time_sec or action.intent.window_start_sec)
                if not dry_run:
                    time.sleep(min(0.05, max(0.0, delayed_time)))
                _invoke_warmup(
                    events_path=events_path,
                    run_id=run_id,
                    intent=action.intent,
                    action_reason="delay_shift_rescheduled_execute",
                    gain=action.gain,
                    dry_run=dry_run,
                    region=region,
                    delayed=True,
                    original_planned_time_sec=action.intent.planned_time_sec,
                    delayed_warmup_time_sec=delayed_time,
                    delay_reason=action.action_reason,
                )
                hot_until[logical] = monotonic_sec() + retention_sec
                warm_count += 1
                delayed_execute_count += 1
                served_after_delay_count += 1
                delay_saved_cold_start_count += 1

            sleep_ms = upstream_delay_ms if logical == "f1" else 10
            was_warm = hot_until.get(logical, 0.0) > monotonic_sec()
            invoke_lambda(
                aws_map.get(logical, logical),
                {"function_name": logical, "run_id": run_id, "invocation_type": "real", "sleep_ms": sleep_ms},
                dry_run=dry_run,
                region_name=region,
            )
            latency = warm_ms + (0.0 if was_warm else cold_ms) + (upstream_delay_ms if logical == "f1" else 0.0)
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

        controller_wall_clock_ms = round((monotonic_sec() - start) * 1000.0, 3)
        controller_wall_clock_ms_total += controller_wall_clock_ms
        latency_ms = round(max(controller_wall_clock_ms, total_latency), 3)
        summary_event = WorkflowRunSummary(
            event_type="workflow_summary",
            run_id=run_id,
            dag="chain",
            baseline=baseline,
            planner_type=config.get("planner", {}).get("type", "heuristic"),
            latency_ms=latency_ms,
            cold_start_count=cold_count,
            warmup_count=warm_count,
        )
        append_jsonl(events_path, summary_event.to_dict())
        row = summary_event.to_dict()
        row["controller_wall_clock_ms"] = controller_wall_clock_ms
        run_rows.append(row)

    with runs_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "run_id",
                "dag",
                "baseline",
                "planner_type",
                "latency_ms",
                "controller_wall_clock_ms",
                "cold_start_count",
                "warmup_count",
                "status",
            ],
        )
        writer.writeheader()
        for row in run_rows:
            writer.writerow({key: row.get(key) for key in writer.fieldnames})

    summary = compute_summary(events_path)
    summary.update(
        {
            "baseline": baseline,
            "repetitions": repetitions,
            "upstream_delay_ms": upstream_delay_ms,
            "delayed_execute_count": delayed_execute_count,
            "delayed_warmup_count": delayed_execute_count,
            "served_after_delay_count": served_after_delay_count,
            "delay_saved_cold_start_count": delay_saved_cold_start_count,
            "controller_wall_clock_latency_ms_avg": round(controller_wall_clock_ms_total / repetitions, 3) if repetitions else 0.0,
        }
    )
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)
    return summary


def run_delay_shift(config: dict[str, Any], args: argparse.Namespace, dry_run: bool) -> list[dict[str, Any]]:
    output_dir = ensure_dir(args.output_dir)
    analysis_path = output_dir / "delay_analysis.csv"
    for child in ["events.jsonl", "runs.csv", "summary.json", "delay_analysis.csv"]:
        path = output_dir / child
        if path.exists():
            path.unlink()

    baselines = _selected_baselines(args)
    summaries = []
    for baseline in baselines:
        baseline_dir = output_dir if len(baselines) == 1 else ensure_dir(output_dir / baseline)
        summaries.append(
            _run_one_baseline(
                config=config,
                baseline=baseline,
                repetitions=args.repetitions,
                upstream_delay_ms=args.upstream_delay_ms,
                dry_run=dry_run,
                output_dir=baseline_dir,
            )
        )

    static_latency = next((row["end_to_end_latency_ms_avg"] for row in summaries if row["baseline"] == "static_dag"), None)
    mint_latency = next((row["end_to_end_latency_ms_avg"] for row in summaries if row["baseline"] == "mint_markov_full"), None)
    with analysis_path.open("w", newline="", encoding="utf-8") as fh:
        fieldnames = [
            "baseline",
            "repetitions",
            "upstream_delay_ms",
            "delay_count",
            "delayed_execute_count",
            "delayed_warmup_count",
            "served_after_delay_count",
            "delay_saved_cold_start_count",
            "missed_warmup",
            "unserved_intent_cold_start",
            "cold_start_count",
            "end_to_end_latency_ms_avg",
            "controller_wall_clock_latency_ms_avg",
            "static_latency_ms_avg",
            "mint_latency_ms_avg",
        ]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(
                {
                    "baseline": summary["baseline"],
                    "repetitions": summary["repetitions"],
                    "upstream_delay_ms": summary["upstream_delay_ms"],
                    "delay_count": summary.get("delay_count", 0),
                    "delayed_execute_count": summary.get("delayed_execute_count", 0),
                    "delayed_warmup_count": summary.get("delayed_warmup_count", 0),
                    "served_after_delay_count": summary.get("served_after_delay_count", 0),
                    "delay_saved_cold_start_count": summary.get("delay_saved_cold_start_count", 0),
                    "missed_warmup": summary.get("missed_warmup", 0),
                    "unserved_intent_cold_start": summary.get("unserved_intent_cold_start", 0),
                    "cold_start_count": summary.get("cold_start_count", 0),
                    "end_to_end_latency_ms_avg": summary.get("end_to_end_latency_ms_avg", 0),
                    "controller_wall_clock_latency_ms_avg": summary.get("controller_wall_clock_latency_ms_avg", 0),
                    "static_latency_ms_avg": static_latency,
                    "mint_latency_ms_avg": mint_latency,
                }
            )
    return summaries


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_yaml(args.config)
    dry_run = _dry_run_enabled(args, config)
    if not dry_run and not args.confirm_real_run:
        print("Refusing real AWS delay-shift run: pass --confirm-real-run when dry-run is disabled.", file=sys.stderr)
        return 2
    summaries = run_delay_shift(config, args, dry_run)
    print(f"dry_run={dry_run}")
    print(f"output_dir={args.output_dir}")
    print(f"summaries={summaries}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
