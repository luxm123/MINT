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
from mint.utils import append_jsonl, ensure_dir, monotonic_sec, new_id, utc_now_iso
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
    "mint_markov_no_runtime_reval",
    "mint_markov_no_long_horizon",
    "mint_markov_full",
}

class InvalidRealObservation(RuntimeError):
    pass


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
        cold_count = 0
        warmup_count = 0

        if self.baseline != "no_warmup":
            warmup_count += self._run_warmups(run_id, intents, selected_nodes, monotonic_sec(), index)

        workflow_start = monotonic_sec()
        workflow_start_time = utc_now_iso()
        stages = self.dag.stages()
        aws_map = self.config.get("aws", {}).get("lambda_functions", {})
        for logical in selected_nodes:
            function_name = aws_map.get(logical, logical)
            now = monotonic_sec()
            was_warm = self._hot_until.get(logical, 0.0) > now
            payload = {"function_name": logical, "run_id": run_id, "invocation_type": "real", "sleep_ms": 10}
            response: dict[str, Any] = {}
            try:
                response = invoke_lambda(
                    function_name=function_name,
                    payload=payload,
                    dry_run=self.dry_run,
                    region_name=self.config.get("aws", {}).get("region"),
                )
                observed = self._observed_invocation_metrics(
                    response=response,
                    was_warm=was_warm,
                    workflow_index=index,
                    stage=stages.get(logical, 0),
                )
            except Exception as exc:
                observed = self._failed_observation(response, exc)
                append_jsonl(
                    self.events_path,
                    InvocationEvent(
                        event_type="invocation",
                        run_id=run_id,
                        function_name=function_name,
                        logical_name=logical,
                        invocation_type="real",
                        cold_start=observed["cold_start"],
                        request_id=observed["request_id"],
                        execution_environment_id=observed["execution_environment_id"],
                        latency_ms=observed["latency_ms"],
                        function_duration_ms=observed["function_duration_ms"],
                        stage=stages.get(logical, 0),
                        status="error",
                        error_type=observed["error_type"],
                        error_message=observed["error_message"],
                    ).to_dict(),
                )
                raise
            latency_ms = observed["latency_ms"]
            cold = observed["cold_start"]
            cold_count += int(cold)
            self._hot_until[logical] = monotonic_sec() + float(self.config.get("platform", {}).get("default_retention_sec", 300))
            event = InvocationEvent(
                event_type="invocation",
                run_id=run_id,
                function_name=function_name,
                logical_name=logical,
                invocation_type="real",
                cold_start=cold,
                request_id=observed["request_id"],
                execution_environment_id=observed["execution_environment_id"],
                latency_ms=latency_ms,
                function_duration_ms=observed["function_duration_ms"],
                stage=stages.get(logical, 0),
                status=observed["status"],
                error_type=observed["error_type"],
                error_message=observed["error_message"],
            )
            append_jsonl(self.events_path, event.to_dict())

        latency_ms = round((monotonic_sec() - workflow_start) * 1000.0, 3)
        workflow_end_time = utc_now_iso()
        summary_event = WorkflowRunSummary(
            event_type="workflow_summary",
            run_id=run_id,
            dag=self.dag.name,
            baseline=self.baseline,
            planner_type=self.planner_type,
            latency_ms=latency_ms,
            cold_start_count=cold_count,
            warmup_count=warmup_count,
            start_time=workflow_start_time,
            end_time=workflow_end_time,
        )
        append_jsonl(self.events_path, summary_event.to_dict())
        return summary_event.to_dict()

    def _run_warmups(self, run_id: str, intents: list[WarmupIntent], selected_nodes: list[str], start_sec: float, workflow_index: int = 0) -> int:
        if self.baseline == "path_aware_greedy":
            actions = self._path_aware_greedy_actions(intents)
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
        }:
            actions = self._static_baseline_actions(intents, workflow_index)
        elif self.baseline == "mint_markov_offline":
            actions = self._markov_offline_actions(intents)
        elif self.baseline == "mint_markov_no_runtime_reval":
            actions = self._markov_no_runtime_reval_actions(intents)
        else:
            call_probability = self._profile_call_probability()
            if self.baseline == "mint_markov_no_long_horizon":
                path_benefit = self._runtime_local_benefit(intents, call_probability)
            else:
                path_benefit = self._runtime_path_benefit(intents)
            runtime_state = {
                "now_sec": 0.0,
                "call_probability": call_probability,
                "frontier": self._observable_frontier(),
                "hot_until": dict(self._hot_until),
                "path_benefit": path_benefit,
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
            response: dict[str, Any] = {}
            try:
                response = invoke_lambda(
                    intent.function_name,
                    payload,
                    invocation_type="RequestResponse",
                    dry_run=self.dry_run,
                    region_name=self.config.get("aws", {}).get("region"),
                )
                observed = self._observed_invocation_metrics(
                    response=response,
                    was_warm=self._hot_until.get(intent.logical_name, 0.0) > monotonic_sec(),
                    workflow_index=workflow_index,
                    stage=intent.stage,
                )
            except Exception as exc:
                observed = self._failed_observation(response, exc)
                append_jsonl(
                    self.events_path,
                    WarmupEvent(
                        event_type="warmup",
                        run_id=run_id,
                        function_name=intent.function_name,
                        logical_name=intent.logical_name,
                        intent_id=intent.intent_id,
                        action=action.action_type,
                        useful=False,
                        action_reason=action.action_reason,
                        gain=action.gain,
                        invocation_type="warmup",
                        cold_start=observed["cold_start"],
                        request_id=observed["request_id"],
                        execution_environment_id=observed["execution_environment_id"],
                        latency_ms=observed["latency_ms"],
                        function_duration_ms=observed["function_duration_ms"],
                        status="error",
                        error_type=observed["error_type"],
                        error_message=observed["error_message"],
                    ).to_dict(),
                )
                raise
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
                invocation_type="warmup",
                cold_start=observed["cold_start"],
                request_id=observed["request_id"],
                execution_environment_id=observed["execution_environment_id"],
                latency_ms=observed["latency_ms"],
                function_duration_ms=observed["function_duration_ms"],
                status=observed["status"],
                error_type=observed["error_type"],
                error_message=observed["error_message"],
            )
            append_jsonl(self.events_path, warmup_event.to_dict())
            count += 1
        return count

    def _planner_type_for_baseline(self) -> str:
        if self.baseline in {"mint_markov_offline", "mint_markov_no_runtime_reval", "mint_markov_no_long_horizon", "mint_markov_full"}:
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
        if self.baseline in {"mint_markov_offline", "mint_markov_no_runtime_reval", "mint_markov_no_long_horizon", "mint_markov_full"}:
            return "markov"
        if self.baseline in {"mint_offline", "mint_full"}:
            return "heuristic"
        return "heuristic"

    def _warmup_budget(self) -> int:
        return max(0, int(self.config.get("experiment", {}).get("warmup_budget", 1)))

    def _observable_frontier(self) -> list[str]:
        return list(self.dag.entry_nodes)

    def _profile_call_probability(self) -> dict[str, float]:
        probabilities = {node: 1.0 for node in self.dag.nodes}
        if self.dag.name == "branch":
            for node in {"f2", "f3", "f4", "f5"} & set(self.dag.nodes):
                probabilities[node] = 0.5
        elif self.dag.name == "mixed":
            for node in {"f2", "f3"} & set(self.dag.nodes):
                probabilities[node] = 0.5
        elif self.dag.name == "wide_branch":
            for node in {"f2", "f3", "f4", "f5"} & set(self.dag.nodes):
                probabilities[node] = 0.25
        elif self.dag.name == "deep_mixed":
            for node in {"f2", "f3", "f4", "f5"} & set(self.dag.nodes):
                probabilities[node] = 0.5
        elif self.dag.name == "greedy_trap":
            for node in {"f2", "f3", "f4"} & set(self.dag.nodes):
                probabilities[node] = 1.0 / 3.0
        return probabilities

    def _runtime_path_benefit(self, intents: list[WarmupIntent]) -> dict[str, float]:
        stages = self.dag.stages()
        downstream = self.dag.downstream_counts()
        predecessors = self.dag.predecessors
        call_probability = self._profile_call_probability()
        convergence_nodes = {node for node, parents in predecessors.items() if len(parents) > 1}
        max_downstream = max(downstream.values() or [1])
        max_stage = max(stages.values() or [1])
        max_fan_in = max((len(parents) for parents in predecessors.values()), default=1)
        max_offline_gain = max((max(0.0, intent.offline_gain) for intent in intents), default=1.0) or 1.0
        cold_ms = float(self.config.get("platform", {}).get("default_cold_start_ms", 800))
        cold_scale = max(cold_ms / 800.0, 0.1)

        benefits: dict[str, float] = {}
        for intent in intents:
            node = intent.logical_name
            p_call = float(call_probability.get(node, 1.0))
            downstream_score = downstream.get(node, 0) / max(max_downstream, 1)
            stage_score = stages.get(node, intent.stage) / max(max_stage, 1)
            fan_in_score = max(0, len(predecessors.get(node, [])) - 1) / max(max_fan_in - 1, 1)
            suffix_score = 1.0 if self._has_convergence_ancestor(node, convergence_nodes) else 0.0
            offline_score = max(0.0, intent.offline_gain) / max_offline_gain
            reachability_score = 0.5 + 0.5 * min(max(p_call, 0.0), 1.0)
            structural_value = (
                1.0
                + 1.2 * downstream_score
                + 0.8 * stage_score
                + 1.4 * suffix_score
                + 0.8 * fan_in_score
                + 0.5 * offline_score
            )
            benefits[node] = round(structural_value * reachability_score * cold_scale, 4)
        return benefits

    def _runtime_local_benefit(self, intents: list[WarmupIntent], call_probability: dict[str, float] | None = None) -> dict[str, float]:
        call_probability = call_probability or self._profile_call_probability()
        max_offline_gain = max((max(0.0, intent.offline_gain) for intent in intents), default=1.0) or 1.0
        cold_ms = float(self.config.get("platform", {}).get("default_cold_start_ms", 800))
        cold_scale = max(cold_ms / 800.0, 0.1)
        benefits: dict[str, float] = {}
        for intent in intents:
            local_gain = max(0.0, intent.offline_gain) / max_offline_gain
            benefits[intent.logical_name] = round(max(local_gain, 0.05) * cold_scale, 4)
        return benefits

    def _has_convergence_ancestor(self, node: str, convergence_nodes: set[str]) -> bool:
        if node in convergence_nodes:
            return True
        reverse = self.dag.predecessors
        queue = list(reverse.get(node, []))
        seen: set[str] = set()
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            if current in convergence_nodes:
                return True
            queue.extend(reverse.get(current, []))
        return False

    def _path_aware_greedy_actions(self, intents: list[WarmupIntent]) -> list[WarmupAction]:
        call_probability = self._profile_call_probability()
        now = monotonic_sec()
        budget = self._warmup_budget()
        actions: list[WarmupAction] = []
        candidates: list[tuple[WarmupIntent, float]] = []
        for intent in intents:
            p_call = float(call_probability.get(intent.logical_name, 0.0))
            expected_gain = self._path_aware_expected_gain(intent, p_call)
            if p_call <= 0.0:
                actions.append(WarmupAction("cancel", intent, expected_gain, "path_aware_not_profile_reachable"))
            elif self._hot_until.get(intent.logical_name, 0.0) > now:
                actions.append(WarmupAction("cancel", intent, expected_gain, "path_aware_already_hot"))
            else:
                candidates.append((intent, expected_gain))

        ranked = sorted(candidates, key=lambda item: (-item[1], item[0].stage, item[0].planned_time_sec, item[0].logical_name))
        for index, (intent, expected_gain) in enumerate(ranked):
            if index < budget:
                actions.append(WarmupAction("execute", intent, expected_gain, "path_aware_profile_expected_gain_within_budget"))
            else:
                actions.append(WarmupAction("replace", intent, expected_gain, "path_aware_greedy_budget_exceeded"))
        return actions

    def _path_aware_expected_gain(self, intent: WarmupIntent, p_call: float) -> float:
        gain = intent.offline_gain * p_call
        if p_call < 1.0 and any(len(self.dag.successors[parent]) > 1 for parent in self.dag.predecessors.get(intent.logical_name, [])):
            cold_ms = float(self.config.get("platform", {}).get("default_cold_start_ms", 800))
            gain += cold_ms * p_call * (1.6 + (1.0 - p_call))
        return round(gain, 4)

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

    def _markov_offline_actions(self, intents: list[WarmupIntent]) -> list[WarmupAction]:
        ranked = sorted(intents, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        budget = self._warmup_budget()
        actions: list[WarmupAction] = []
        for index, intent in enumerate(ranked):
            if index < budget:
                actions.append(WarmupAction("execute", intent, intent.offline_gain, "mint_markov_offline_top_intent_within_budget"))
            else:
                actions.append(WarmupAction("replace", intent, intent.offline_gain, "mint_markov_offline_budget_exceeded"))
        return actions

    def _markov_no_runtime_reval_actions(self, intents: list[WarmupIntent]) -> list[WarmupAction]:
        ranked = sorted(intents, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        budget = self._warmup_budget()
        now = monotonic_sec()
        executed = 0
        actions: list[WarmupAction] = []
        for intent in ranked:
            if self._hot_until.get(intent.logical_name, 0.0) > now:
                actions.append(WarmupAction("cancel", intent, intent.offline_gain, "mint_markov_no_runtime_reval_already_hot"))
            elif executed < budget:
                actions.append(WarmupAction("execute", intent, intent.offline_gain, "mint_markov_no_runtime_reval_offline_rank_within_budget"))
                executed += 1
            else:
                actions.append(WarmupAction("cancel", intent, intent.offline_gain, "mint_markov_no_runtime_reval_budget_exceeded"))
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
                elif self.dag.name == "greedy_trap" and child == "f5" and any(parent in seen for parent in parents):
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
        if self.dag.name == "greedy_trap":
            mismatch = bool(self.config.get("experiment", {}).get("profile_mismatch", False))
            branch_index = (index + branch_seed) % 3
            if mismatch:
                branch_index = (branch_index * 2 + 1) % 3
            return {"branch_index": branch_index}
        return {"branch": "left" if (index + branch_seed) % 2 == 0 else "right"}

    def _timing_jitter_ms(self, index: int, stage: int) -> float:
        jitter = float(self.config.get("experiment", {}).get("timing_jitter_ms", 0.0))
        if jitter <= 0 or self.dag.name not in {"deep_mixed", "greedy_trap"}:
            return 0.0
        return round(((index + stage) % 3 - 1) * jitter / 2.0, 3)

    def _simulated_latency_ms(self, warm: bool) -> float:
        platform = self.config.get("platform", {})
        base = float(platform.get("default_warm_duration_ms", 100))
        cold = 0.0 if warm else float(platform.get("default_cold_start_ms", 800))
        return round(base + cold + random.uniform(0, 15), 3)

    def _observed_invocation_metrics(self, response: dict[str, Any], was_warm: bool, workflow_index: int, stage: int) -> dict[str, Any]:
        if self.dry_run:
            latency_ms = self._simulated_latency_ms(was_warm)
            latency_ms += self._timing_jitter_ms(workflow_index, stage)
            return {
                "latency_ms": round(latency_ms, 3),
                "function_duration_ms": round(latency_ms, 3),
                "cold_start": not was_warm,
                "request_id": "",
                "execution_environment_id": "",
                "status": "ok",
                "error_type": "",
                "error_message": "",
            }

        payload = response.get("payload") if isinstance(response.get("payload"), dict) else {}
        required = ("cold_start", "execution_environment_id", "request_id", "duration_ms", "invocation_type", "status")
        missing = [key for key in required if key not in payload]
        if missing:
            raise InvalidRealObservation(f"real Lambda response missing required fields: {', '.join(missing)}")
        empty = [key for key in ("execution_environment_id", "request_id") if not str(payload.get(key) or "").strip()]
        if empty:
            raise InvalidRealObservation(f"real Lambda response has empty required fields: {', '.join(empty)}")
        if payload["invocation_type"] not in {"real", "warmup"}:
            raise InvalidRealObservation(f"invalid invocation_type in real Lambda response: {payload['invocation_type']!r}")
        status_code = response.get("status_code")
        function_error = str(response.get("function_error") or "")
        payload_status = str(payload.get("status") or "")
        status = "ok" if isinstance(status_code, int) and status_code < 400 and not function_error and payload_status == "ok" else "error"
        if status != "ok":
            raise InvalidRealObservation(
                f"real Lambda invocation failed: status_code={status_code}, function_error={function_error!r}, "
                f"payload_status={payload_status!r}, error={payload.get('error_message', '')!r}"
            )
        latency_ms = self._coerce_observed_latency(response.get("client_elapsed_ms"))
        if latency_ms is None:
            raise InvalidRealObservation("real Lambda response missing valid client_elapsed_ms; simulated fallback is forbidden")
        function_duration_ms = self._coerce_observed_latency(payload.get("duration_ms"))
        if function_duration_ms is None:
            raise InvalidRealObservation("real Lambda response missing valid duration_ms")
        return {
            "latency_ms": round(latency_ms, 3),
            "function_duration_ms": round(function_duration_ms, 3),
            "cold_start": bool(payload["cold_start"]),
            "request_id": str(payload["request_id"]),
            "execution_environment_id": str(payload["execution_environment_id"]),
            "status": status,
            "error_type": str(payload.get("error_type") or function_error),
            "error_message": str(payload.get("error_message") or ""),
        }

    def _failed_observation(self, response: dict[str, Any], exc: Exception) -> dict[str, Any]:
        payload = response.get("payload") if isinstance(response.get("payload"), dict) else {}
        return {
            "latency_ms": self._coerce_observed_latency(response.get("client_elapsed_ms")) or 0.0,
            "function_duration_ms": self._coerce_observed_latency(payload.get("duration_ms")) or 0.0,
            "cold_start": bool(payload.get("cold_start", False)),
            "request_id": str(payload.get("request_id") or response.get("response_metadata_request_id") or ""),
            "execution_environment_id": str(payload.get("execution_environment_id") or ""),
            "status": "error",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }

    @staticmethod
    def _coerce_observed_latency(value: Any) -> float | None:
        if value is None:
            return None
        try:
            latency_ms = float(value)
        except (TypeError, ValueError):
            return None
        if latency_ms < 0:
            return None
        return latency_ms

    def _write_runs_csv(self, summaries: list[dict[str, Any]]) -> None:
        ensure_dir(self.runs_path.parent)
        with self.runs_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["run_id", "dag", "baseline", "planner_type", "latency_ms", "cold_start_count", "warmup_count", "status"])
            writer.writeheader()
            for row in summaries:
                writer.writerow({key: row.get(key) for key in writer.fieldnames})
