from __future__ import annotations

import csv
import json
import random
from dataclasses import asdict
from pathlib import Path
from typing import Any

from mint.aws_client import invoke_lambda
from mint.events import InvocationEvent, SchedulerDecision, WarmupEvent, WorkflowRunSummary
from mint.intent_planner import WarmupIntent, plan_intents
from mint.metrics import compute_summary
from mint.scheduler import schedule_intents
from mint.utils import append_jsonl, ensure_dir, monotonic_sec, new_id
from mint.workloads import WorkflowDAG, get_workload


SUPPORTED_BASELINES = {"no_warmup", "periodic", "independent", "static_dag", "mint_offline", "mint_full"}


class MintController:
    def __init__(
        self,
        config: dict[str, Any],
        dag: WorkflowDAG | None = None,
        baseline: str | None = None,
        dry_run: bool = True,
        output_dir: str | Path | None = None,
    ) -> None:
        exp_cfg = config.get("experiment", {})
        self.config = config
        self.dag = dag or get_workload(exp_cfg.get("dag", "chain"))
        self.baseline = baseline or exp_cfg.get("baseline", "mint_full")
        if self.baseline not in SUPPORTED_BASELINES:
            raise ValueError(f"Unsupported baseline: {self.baseline}")
        self.dry_run = dry_run
        self.output_dir = ensure_dir(output_dir or exp_cfg.get("output_dir", "results/default"))
        self.events_path = self.output_dir / "events.jsonl"
        self.runs_path = self.output_dir / "runs.csv"
        self.summary_path = self.output_dir / "summary.json"
        self._hot_until: dict[str, float] = {}
        self._warmups: set[str] = set()

    def run(self, repetitions: int) -> dict[str, Any]:
        summaries = [self.run_once(index) for index in range(repetitions)]
        self._write_runs_csv(summaries)
        summary = compute_summary(self.events_path)
        with self.summary_path.open("w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, sort_keys=True)
        return summary

    def run_once(self, index: int) -> dict[str, Any]:
        run_id = new_id("run")
        context = {"branch": "left" if index % 2 == 0 else "right"}
        intents = plan_intents(self.dag, self.config)
        selected_nodes = self._resolve_path(context)
        start = monotonic_sec()
        cold_count = 0
        warmup_count = 0
        total_invocation_latency_ms = 0.0

        if self.baseline != "no_warmup":
            warmup_count += self._run_warmups(run_id, intents, selected_nodes, start)

        stages = self.dag.stages()
        aws_map = self.config.get("aws", {}).get("lambda_functions", {})
        for logical in selected_nodes:
            function_name = aws_map.get(logical, logical)
            now = monotonic_sec()
            was_warm = self._hot_until.get(logical, 0.0) > now
            payload = {"function_name": logical, "run_id": run_id, "invocation_type": "real", "sleep_ms": 10}
            response = invoke_lambda(
                function_name=function_name,
                payload=payload,
                dry_run=self.dry_run,
                region_name=self.config.get("aws", {}).get("region"),
            )
            latency_ms = self._simulated_latency_ms(was_warm)
            total_invocation_latency_ms += latency_ms
            cold = not was_warm
            cold_count += int(cold)
            self._hot_until[logical] = monotonic_sec() + float(self.config.get("platform", {}).get("default_retention_sec", 300))
            event = InvocationEvent(
                event_type="invocation",
                run_id=run_id,
                function_name=function_name,
                logical_name=logical,
                invocation_type="real",
                cold_start=cold,
                latency_ms=latency_ms,
                stage=stages.get(logical, 0),
                status="ok" if response.get("status_code", 200) < 400 else "error",
            )
            append_jsonl(self.events_path, event.to_dict())

        latency_ms = round(max((monotonic_sec() - start) * 1000.0, total_invocation_latency_ms), 3)
        summary_event = WorkflowRunSummary(
            event_type="workflow_summary",
            run_id=run_id,
            dag=self.dag.name,
            baseline=self.baseline,
            latency_ms=latency_ms,
            cold_start_count=cold_count,
            warmup_count=warmup_count,
        )
        append_jsonl(self.events_path, summary_event.to_dict())
        return summary_event.to_dict()

    def _run_warmups(self, run_id: str, intents: list[WarmupIntent], selected_nodes: list[str], start_sec: float) -> int:
        if self.baseline in {"periodic", "independent", "static_dag", "mint_offline"}:
            actions = [
                type("StaticAction", (), {"action_type": "execute", "intent": intent, "gain": intent.offline_gain, "action_reason": self.baseline})()
                for intent in intents
            ]
        else:
            call_probability = {node: (1.0 if node in selected_nodes else 0.0) for node in self.dag.nodes}
            runtime_state = {
                "now_sec": 0.0,
                "call_probability": call_probability,
                "hot_until": self._hot_until,
                "path_benefit": {intent.logical_name: intent.criticality for intent in intents},
            }
            actions = schedule_intents(
                intents,
                runtime_state,
                int(self.config.get("experiment", {}).get("warmup_budget", 1)),
                self.config,
            )

        count = 0
        for action in actions:
            intent = action.intent
            decision = SchedulerDecision(
                event_type="scheduler_decision",
                run_id=run_id,
                intent_id=intent.intent_id,
                function_name=intent.function_name,
                logical_name=intent.logical_name,
                action=action.action_type,
                action_reason=action.action_reason,
                gain=action.gain,
                planned_time_sec=intent.planned_time_sec,
            )
            append_jsonl(self.events_path, decision.to_dict())
            if action.action_type != "execute":
                continue
            payload = {"function_name": intent.logical_name, "run_id": run_id, "invocation_type": "warmup", "sleep_ms": 1}
            invoke_lambda(intent.function_name, payload, invocation_type="Event", dry_run=self.dry_run, region_name=self.config.get("aws", {}).get("region"))
            useful = intent.logical_name in selected_nodes
            if useful:
                self._warmups.add(f"{run_id}:{intent.logical_name}")
            self._hot_until[intent.logical_name] = monotonic_sec() + float(self.config.get("platform", {}).get("default_retention_sec", 300))
            warmup_event = WarmupEvent(
                event_type="warmup",
                run_id=run_id,
                function_name=intent.function_name,
                logical_name=intent.logical_name,
                intent_id=intent.intent_id,
                action=action.action_type,
                useful=useful,
                action_reason=action.action_reason,
                gain=action.gain,
            )
            append_jsonl(self.events_path, warmup_event.to_dict())
            count += 1
        return count

    def _resolve_path(self, context: dict[str, Any]) -> list[str]:
        ready = list(self.dag.entry_nodes)
        completed: list[str] = []
        seen: set[str] = set()
        while ready:
            node = ready.pop(0)
            if node in seen:
                continue
            seen.add(node)
            completed.append(node)
            for child in self.dag.next_nodes(node, context):
                parents = self.dag.predecessors[child]
                if all(parent in seen or parent not in completed + ready for parent in parents if self.dag.name == "branch"):
                    ready.append(child)
                elif all(parent in seen for parent in parents):
                    ready.append(child)
        return completed

    def _simulated_latency_ms(self, warm: bool) -> float:
        platform = self.config.get("platform", {})
        base = float(platform.get("default_warm_duration_ms", 100))
        cold = 0.0 if warm else float(platform.get("default_cold_start_ms", 800))
        return round(base + cold + random.uniform(0, 15), 3)

    def _write_runs_csv(self, summaries: list[dict[str, Any]]) -> None:
        ensure_dir(self.runs_path.parent)
        with self.runs_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["run_id", "dag", "baseline", "latency_ms", "cold_start_count", "warmup_count", "status"])
            writer.writeheader()
            for row in summaries:
                writer.writerow({key: row.get(key) for key in writer.fieldnames})
