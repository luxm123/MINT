from __future__ import annotations

import csv
import copy
import json
import random
from pathlib import Path
from typing import Any

from mint.aws_client import invoke_lambda
from mint.events import InvocationEvent, SchedulerDecision, WarmupEvent, WorkflowRunSummary
from mint.intent_planner import WarmupIntent, plan_intents
from mint.metrics import compute_summary
from mint.scheduler import WarmupAction, schedule_intents
from mint.utils import append_jsonl, ensure_dir, monotonic_sec, new_id
from mint.workloads import WorkflowDAG, get_workload


SUPPORTED_BASELINES = {
    "no_warmup",
    "periodic",
    "periodic_keepwarm",
    "independent",
    "static_dag",
    "static_dag_unlimited",
    "orion_like",
    "path_aware_greedy",
    "oracle_path",
    "mint_offline",
    "mint_offline_unlimited",
    "mint_full",
    "mint_markov_offline",
    "mint_markov_full",
}


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
        self.planner_type = self._planner_type_for_baseline()
        self._workflow_index = 0

    def run(self, repetitions: int) -> dict[str, Any]:
        summaries = [self.run_once(index) for index in range(repetitions)]
        self._write_runs_csv(summaries)
        summary = compute_summary(self.events_path)
        with self.summary_path.open("w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, sort_keys=True)
        return summary

    def run_once(self, index: int) -> dict[str, Any]:
        run_id = new_id("run")
        context = self._run_context(index)
        planner_config = self._planner_config()
        intents = plan_intents(self.dag, planner_config)
        selected_nodes = self._resolve_path(context)
        start = monotonic_sec()
        cold_count = 0
        warmup_count = 0
        total_invocation_latency_ms = 0.0

        if self.baseline != "no_warmup":
            warmup_count += self._run_warmups(run_id, intents, selected_nodes, start, index)

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
            latency_ms += self._timing_jitter_ms(index, stages.get(logical, 0))
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
            planner_type=self.planner_type,
            latency_ms=latency_ms,
            cold_start_count=cold_count,
            warmup_count=warmup_count,
        )
        append_jsonl(self.events_path, summary_event.to_dict())
        return summary_event.to_dict()

    def _run_warmups(self, run_id: str, intents: list[WarmupIntent], selected_nodes: list[str], start_sec: float, workflow_index: int = 0) -> int:
        if self.baseline == "path_aware_greedy":
            actions = self._path_aware_greedy_actions(intents, selected_nodes)
        elif self.baseline == "oracle_path":
            actions = self._oracle_path_actions(intents, selected_nodes)
        elif self.baseline in {
            "periodic",
            "periodic_keepwarm",
            "independent",
            "static_dag",
            "static_dag_unlimited",
            "orion_like",
            "mint_offline",
            "mint_offline_unlimited",
            "mint_markov_offline",
        }:
            actions = self._static_baseline_actions(intents, workflow_index)
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

    def _planner_type_for_baseline(self) -> str:
        if self.baseline in {"mint_markov_offline", "mint_markov_full"}:
            return "markov"
        if self.baseline in {"mint_offline", "mint_full"}:
            return "heuristic"
        if self.baseline == "periodic_keepwarm":
            return "periodic"
        if self.baseline == "orion_like":
            return "orion_like"
        if self.baseline == "path_aware_greedy":
            return "runtime_greedy"
        if self.baseline == "oracle_path":
            return "oracle"
        return self.config.get("planner", {}).get("type", "heuristic")

    def _planner_config(self) -> dict[str, Any]:
        planner_config = copy.deepcopy(self.config)
        planner_config.setdefault("planner", {})["type"] = self._intent_planner_type()
        return planner_config

    def _intent_planner_type(self) -> str:
        if self.baseline in {"mint_markov_offline", "mint_markov_full"}:
            return "markov"
        if self.baseline in {"mint_offline", "mint_full"}:
            return "heuristic"
        return "heuristic"

    def _warmup_budget(self) -> int:
        return max(0, int(self.config.get("experiment", {}).get("warmup_budget", 1)))

    def _path_aware_greedy_actions(self, intents: list[WarmupIntent], selected_nodes: list[str]) -> list[WarmupAction]:
        selected = set(selected_nodes)
        now = monotonic_sec()
        budget = self._warmup_budget()
        actions: list[WarmupAction] = []
        candidates: list[WarmupIntent] = []
        for intent in intents:
            if intent.logical_name not in selected:
                actions.append(WarmupAction("cancel", intent, intent.offline_gain, "path_aware_not_on_selected_path"))
            elif self._hot_until.get(intent.logical_name, 0.0) > now:
                actions.append(WarmupAction("cancel", intent, intent.offline_gain, "path_aware_already_hot"))
            else:
                candidates.append(intent)

        ranked = sorted(candidates, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        for index, intent in enumerate(ranked):
            if index < budget:
                actions.append(WarmupAction("execute", intent, intent.offline_gain, "path_aware_greedy_within_budget"))
            else:
                actions.append(WarmupAction("replace", intent, intent.offline_gain, "path_aware_greedy_budget_exceeded"))
        return actions

    def _oracle_path_actions(self, intents: list[WarmupIntent], selected_nodes: list[str]) -> list[WarmupAction]:
        selected = set(selected_nodes)
        now = monotonic_sec()
        budget = self._warmup_budget()
        actions: list[WarmupAction] = []
        candidates: list[WarmupIntent] = []
        for intent in intents:
            if intent.logical_name not in selected:
                actions.append(WarmupAction("cancel", intent, intent.offline_gain, "oracle_not_on_real_path"))
            elif self._hot_until.get(intent.logical_name, 0.0) > now:
                actions.append(WarmupAction("cancel", intent, intent.offline_gain, "oracle_already_hot"))
            else:
                candidates.append(intent)

        ranked = sorted(candidates, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        for index, intent in enumerate(ranked):
            if index < budget:
                actions.append(WarmupAction("execute", intent, intent.offline_gain, "oracle_path_upper_bound"))
            else:
                actions.append(WarmupAction("replace", intent, intent.offline_gain, "oracle_budget_exceeded"))
        return actions

    def _static_baseline_actions(self, intents: list[WarmupIntent], workflow_index: int = 0) -> list[WarmupAction]:
        if self.baseline == "periodic_keepwarm":
            ranked = self._periodic_keepwarm_ranked_intents(intents, workflow_index)
        elif self.baseline == "orion_like":
            ranked = self._orion_like_ranked_intents(intents)
        else:
            ranked = sorted(intents, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        unlimited = self.baseline in {"static_dag_unlimited", "mint_offline_unlimited", "periodic", "independent"}
        budget = len(ranked) if unlimited else self._warmup_budget()
        actions: list[WarmupAction] = []
        for index, intent in enumerate(ranked):
            if index < budget:
                actions.append(WarmupAction("execute", intent, intent.offline_gain, f"{self.baseline}_within_budget"))
            else:
                actions.append(WarmupAction("replace", intent, intent.offline_gain, f"{self.baseline}_budget_exceeded"))
        return actions

    def _periodic_keepwarm_ranked_intents(self, intents: list[WarmupIntent], workflow_index: int = 0) -> list[WarmupIntent]:
        ordered = sorted(intents, key=lambda item: item.logical_name)
        if not ordered:
            return []
        shift = workflow_index % len(ordered)
        return ordered[shift:] + ordered[:shift]

    def _orion_like_ranked_intents(self, intents: list[WarmupIntent]) -> list[WarmupIntent]:
        lookahead = float(self.config.get("baseline", {}).get("orion_like_lookahead_sec", self.config.get("baseline", {}).get("orion_lookahead_sec", 0.5)))
        slack = float(self.config.get("baseline", {}).get("orion_stage_slack_sec", 0.25))
        stages = self.dag.stages()
        downstream = self.dag.downstream_counts()
        return sorted(
            intents,
            key=lambda item: (
                0 if stages.get(item.logical_name, item.stage) > 0 else 1,
                max(0.0, item.planned_time_sec - lookahead - slack),
                -stages.get(item.logical_name, item.stage),
                downstream.get(item.logical_name, 0),
                -item.offline_gain,
                item.logical_name,
            ),
        )

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
                if self.dag.name == "mixed" and child == "f4" and any(parent in seen for parent in parents):
                    ready.append(child)
                elif self.dag.name == "wide_branch" and child == "f6" and any(parent in seen for parent in parents):
                    ready.append(child)
                elif self.dag.name == "deep_mixed" and child == "f6" and any(parent in seen for parent in parents):
                    ready.append(child)
                elif all(parent in seen or parent not in completed + ready for parent in parents if self.dag.name == "branch"):
                    ready.append(child)
                elif all(parent in seen for parent in parents):
                    ready.append(child)
        return completed

    def _run_context(self, index: int) -> dict[str, Any]:
        branch_seed = int(self.config.get("experiment", {}).get("branch_seed", 0))
        if self.dag.name == "wide_branch":
            mismatch = bool(self.config.get("experiment", {}).get("profile_mismatch", False))
            branch_index = (index + branch_seed) % 4
            if mismatch:
                branch_index = (branch_index * 2 + 1) % 4
            return {"branch_index": branch_index}
        return {"branch": "left" if (index + branch_seed) % 2 == 0 else "right"}

    def _timing_jitter_ms(self, index: int, stage: int) -> float:
        jitter = float(self.config.get("experiment", {}).get("timing_jitter_ms", 0.0))
        if jitter <= 0 or self.dag.name != "deep_mixed":
            return 0.0
        return round(((index + stage) % 3 - 1) * jitter / 2.0, 3)

    def _simulated_latency_ms(self, warm: bool) -> float:
        platform = self.config.get("platform", {})
        base = float(platform.get("default_warm_duration_ms", 100))
        cold = 0.0 if warm else float(platform.get("default_cold_start_ms", 800))
        return round(base + cold + random.uniform(0, 15), 3)

    def _write_runs_csv(self, summaries: list[dict[str, Any]]) -> None:
        ensure_dir(self.runs_path.parent)
        with self.runs_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["run_id", "dag", "baseline", "planner_type", "latency_ms", "cold_start_count", "warmup_count", "status"])
            writer.writeheader()
            for row in summaries:
                writer.writerow({key: row.get(key) for key in writer.fieldnames})
