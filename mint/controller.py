from __future__ import annotations

import csv
import copy
import json
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from mint.aws_client import invoke_lambda
from mint.branch_history import BranchHistoryModel
from mint.events import BranchModelEvent, InvocationEvent, SchedulerDecision, WarmupEvent, WorkflowRunSummary
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
        self._executed_warmups_by_run: dict[str, list[str]] = {}
        self._last_environment_ids: dict[str, str] = {}
        self.planner_type = self._planner_type_for_baseline()
        self._workflow_index = 0
        self.function_pool = str(exp_cfg.get("function_pool", self.baseline))
        pools = config.get("aws", {}).get("lambda_function_pools", {})
        configured_pool = exp_cfg.get("function_pool")
        if configured_pool and configured_pool not in pools:
            raise ValueError(f"Unknown Lambda function pool: {configured_pool}")
        self.function_map = dict(pools.get(self.function_pool, config.get("aws", {}).get("lambda_functions", {})))
        missing_nodes = [node for node in self.dag.nodes if node not in self.function_map]
        if not self.dry_run and missing_nodes:
            raise ValueError(f"Lambda function pool {self.function_pool!r} is missing DAG nodes: {', '.join(missing_nodes)}")
        self._branch_history = BranchHistoryModel.from_config(
            self._branch_names(), config.get("planner", {})
        ) if self._branch_names() else None
        self._active_model_snapshot: dict[str, Any] = {}

    def run(self, repetitions: int) -> dict[str, Any]:
        summaries = [self.run_once(index) for index in range(repetitions)]
        return self.finalize(summaries)

    def finalize(self, summaries: list[dict[str, Any]]) -> dict[str, Any]:
        self._write_runs_csv(summaries)
        summary = compute_summary(self.events_path)
        with self.summary_path.open("w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, sort_keys=True)
        return summary

    def reset_runtime_state(self) -> None:
        """Forget controller-inferred heat after an external function-pool reset."""
        self._hot_until.clear()
        self._warmups.clear()
        self._executed_warmups_by_run.clear()

    def observed_environment_ids(self) -> dict[str, str]:
        return dict(self._last_environment_ids)

    def run_once(
        self,
        index: int,
        *,
        block_id: str = "",
        strategy_order: str = "",
        planned_arrival_sec: float | None = None,
        planned_arrival_time: str = "",
    ) -> dict[str, Any]:
        run_id = new_id("run")
        self._active_model_snapshot = self._branch_history.snapshot() if self._branch_history else {}
        planner_config = self._planner_config()
        intents = plan_intents(self.dag, planner_config)
        context = self._run_context(index)
        selected_nodes = self._resolve_path(context)
        if self._active_model_snapshot:
            append_jsonl(
                self.events_path,
                BranchModelEvent(
                    event_type="branch_model",
                    run_id=run_id,
                    decision_phase="initial",
                    history_size=int(self._active_model_snapshot["history_size"]),
                    branch_counts=json.dumps(self._active_model_snapshot["counts"], sort_keys=True),
                    branch_probabilities=json.dumps(self._active_model_snapshot["probabilities"], sort_keys=True),
                ).to_dict(),
            )
        initial_environment_ids = json.dumps(self._last_environment_ids, sort_keys=True)
        cold_count = 0
        warmup_count = 0
        planned_arrival_sec = monotonic_sec() if planned_arrival_sec is None else planned_arrival_sec
        warmup_lead_sec = max(0.0, float(self.config.get("experiment", {}).get("warmup_lead_sec", 0.0)))
        warmup_start_sec = planned_arrival_sec - warmup_lead_sec
        wait_for_warmup_sec = warmup_start_sec - monotonic_sec()
        if wait_for_warmup_sec > 0:
            time.sleep(wait_for_warmup_sec)

        if self.baseline != "no_warmup":
            warmup_count += self._run_warmups(run_id, intents, selected_nodes, monotonic_sec(), index)
        warmup_completed_sec = monotonic_sec()

        wait_for_arrival_sec = planned_arrival_sec - monotonic_sec()
        if wait_for_arrival_sec > 0:
            time.sleep(wait_for_arrival_sec)
        workflow_start = monotonic_sec()
        workflow_start_time = utc_now_iso()
        arrival_lateness_ms = round(max(0.0, workflow_start - planned_arrival_sec) * 1000.0, 3)
        warmup_overrun_ms = (
            round(max(0.0, warmup_completed_sec - planned_arrival_sec) * 1000.0, 3)
            if warmup_count > 0
            else 0.0
        )
        stages = self.dag.stages()
        runtime_executor: ThreadPoolExecutor | None = None
        runtime_pending: tuple[Future[dict[str, Any]], WarmupIntent, float] | None = None
        if self.dag.name == "adaptive_branch" and self.baseline == "mint_markov_full":
            runtime_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mint-runtime-warmup")
        for logical in selected_nodes:
            if runtime_pending and runtime_pending[1].logical_name == logical:
                self._complete_runtime_warmup(run_id, runtime_pending)
                runtime_pending = None
            function_name = self.function_map.get(logical, logical)
            now = monotonic_sec()
            was_warm = self._hot_until.get(logical, 0.0) > now
            payload = {"function_name": logical, "run_id": run_id, "invocation_type": "real", "sleep_ms": 10}
            if logical == "f1" and self.dag.name == "adaptive_branch":
                payload["branch"] = self._context_branch(context)
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
                        observed_branch=observed.get("observed_branch", ""),
                    ).to_dict(),
                )
                raise
            latency_ms = observed["latency_ms"]
            cold = observed["cold_start"]
            cold_count += int(cold)
            self._last_environment_ids[logical] = observed["execution_environment_id"]
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
                observed_branch=observed.get("observed_branch", ""),
            )
            append_jsonl(self.events_path, event.to_dict())
            if logical == "f1" and self.dag.name == "adaptive_branch" and self.baseline == "mint_markov_full":
                observed_branch = str(observed.get("observed_branch") or "")
                expected_branch = self._context_branch(context)
                if observed_branch != expected_branch:
                    raise InvalidRealObservation(
                        f"f1 branch observation mismatch: expected={expected_branch!r}, observed={observed_branch!r}"
                    )
                if runtime_executor is None:
                    raise RuntimeError("adaptive runtime executor was not initialized")
                runtime_pending = self._runtime_revise_after_branch(
                    run_id, intents, observed_branch, index, runtime_executor
                )
                warmup_count += int(runtime_pending is not None)

        if runtime_pending:
            self._complete_runtime_warmup(run_id, runtime_pending)
        if runtime_executor:
            runtime_executor.shutdown(wait=True)
        if (
            self.dag.name == "adaptive_branch"
            and self.baseline == "mint_markov_full"
            and warmup_count > self._warmup_budget()
        ):
            raise RuntimeError(
                f"real warmup budget exceeded: executed={warmup_count}, budget={self._warmup_budget()}"
            )

        latency_ms = round((monotonic_sec() - planned_arrival_sec) * 1000.0, 3)
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
            block_id=block_id,
            function_pool=self.function_pool,
            branch_seed=int(self.config.get("experiment", {}).get("branch_seed", 0)),
            strategy_order=strategy_order,
            planned_arrival_time=planned_arrival_time,
            actual_start_time=workflow_start_time,
            arrival_lateness_ms=arrival_lateness_ms,
            warmup_overrun_ms=warmup_overrun_ms,
            initial_environment_ids=initial_environment_ids,
        )
        append_jsonl(self.events_path, summary_event.to_dict())
        observed_branch = self._context_branch(context)
        if self._branch_history and observed_branch:
            self._branch_history.observe(observed_branch)
        return summary_event.to_dict()

    def _run_warmups(self, run_id: str, intents: list[WarmupIntent], selected_nodes: list[str], start_sec: float, workflow_index: int = 0) -> int:
        if self.dag.name == "adaptive_branch":
            intents = [intent for intent in intents if intent.logical_name not in self.dag.entry_nodes]
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
                self._initial_warmup_budget(),
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
                decision_phase="initial",
                model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                branch_probabilities=json.dumps(self._active_model_snapshot.get("probabilities", {}), sort_keys=True),
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
            self._last_environment_ids[intent.logical_name] = observed["execution_environment_id"]
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
            self._executed_warmups_by_run.setdefault(run_id, []).append(intent.logical_name)
        return count

    def _runtime_revise_after_branch(
        self,
        run_id: str,
        intents: list[WarmupIntent],
        observed_branch: str,
        workflow_index: int,
        executor: ThreadPoolExecutor,
    ) -> tuple[Future[dict[str, Any]], WarmupIntent, float] | None:
        if observed_branch not in self.dag.nodes:
            raise ValueError(f"observed branch is not a DAG node: {observed_branch}")
        actual_targets = self.dag.reachable_from([observed_branch])
        executed = list(self._executed_warmups_by_run.get(run_id, []))
        intent_by_node = {intent.logical_name: intent for intent in intents}
        probabilities = json.dumps(self._active_model_snapshot.get("probabilities", {}), sort_keys=True)

        append_jsonl(
            self.events_path,
            BranchModelEvent(
                event_type="branch_model",
                run_id=run_id,
                decision_phase="runtime_after_f1",
                history_size=int(self._active_model_snapshot.get("history_size", 0)),
                branch_counts=json.dumps(self._active_model_snapshot.get("counts", {}), sort_keys=True),
                branch_probabilities=probabilities,
                observed_branch=observed_branch,
            ).to_dict(),
        )
        for node in executed:
            if node in actual_targets:
                continue
            intent = intent_by_node.get(node)
            if intent is None:
                continue
            append_jsonl(
                self.events_path,
                SchedulerDecision(
                    event_type="scheduler_decision",
                    run_id=run_id,
                    intent_id=intent.intent_id,
                    function_name=intent.function_name,
                    logical_name=node,
                    action="cancel",
                    action_reason="runtime_invalidated_after_f1_already_executed_wasted",
                    gain=0.0,
                    planned_time_sec=intent.planned_time_sec,
                    decision_phase="runtime_after_f1",
                    model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                    branch_probabilities=probabilities,
                ).to_dict(),
            )

        remaining_budget = self._warmup_budget() - len(executed)
        if remaining_budget <= 0:
            return None
        now = monotonic_sec()
        candidates = [
            intent_by_node[node]
            for node in actual_targets - {observed_branch}
            if node in intent_by_node
            and node not in executed
            and self._hot_until.get(node, 0.0) <= now
        ]
        if not candidates:
            return None
        from mint.markov_policy import MarkovPolicyAnalyzer, MarkovState

        runtime_analyzer = MarkovPolicyAnalyzer(
            self.dag,
            self._planner_config(),
            budget=remaining_budget,
        )
        retention = runtime_analyzer.transition_model.retention_buckets
        runtime_state = MarkovState(
            frontier=(observed_branch,),
            hot_ttl=tuple(sorted((node, retention) for node in executed)),
            completed=tuple(sorted(set(self.dag.entry_nodes))),
            branch_path=observed_branch,
            time_bucket=1,
        )
        runtime_analyzer.analyze(runtime_state)
        runtime_gains = {
            candidate.logical_name: runtime_analyzer.marginal_gain(runtime_state, candidate.logical_name)
            for candidate in candidates
        }
        candidates = [candidate for candidate in candidates if runtime_gains[candidate.logical_name] > 0]
        if not candidates:
            return None
        # Branch observation conditions the state before recomputing every
        # reachable candidate's Q(no-op)-Q(warm node) marginal gain.
        intent = min(
            candidates,
            key=lambda item: (
                -runtime_gains[item.logical_name],
                self.dag.stages().get(item.logical_name, item.stage),
                item.logical_name,
            ),
        )
        target = intent.logical_name
        runtime_gain = runtime_gains[target]
        replaced = next((old for old in executed if old not in actual_targets), "")
        append_jsonl(
            self.events_path,
            SchedulerDecision(
                event_type="scheduler_decision",
                run_id=run_id,
                intent_id=intent.intent_id,
                function_name=intent.function_name,
                logical_name=target,
                action="replace",
                action_reason="runtime_branch_observation_parallel_successor_warmup",
                gain=runtime_gain,
                planned_time_sec=intent.planned_time_sec,
                decision_phase="runtime_after_f1",
                model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                branch_probabilities=probabilities,
                supersedes_intent_id=intent_by_node[replaced].intent_id if replaced else "",
            ).to_dict(),
        )
        started = monotonic_sec()
        future = executor.submit(self._invoke_runtime_warmup, run_id, intent, workflow_index)
        return future, intent, started

    def _invoke_runtime_warmup(self, run_id: str, intent: WarmupIntent, workflow_index: int) -> dict[str, Any]:
        payload = {"function_name": intent.logical_name, "run_id": run_id, "invocation_type": "warmup", "sleep_ms": 1}
        response = invoke_lambda(
            intent.function_name,
            payload,
            invocation_type="RequestResponse",
            dry_run=self.dry_run,
            region_name=self.config.get("aws", {}).get("region"),
        )
        observed = self._observed_invocation_metrics(
            response,
            self._hot_until.get(intent.logical_name, 0.0) > monotonic_sec(),
            workflow_index,
            intent.stage,
        )
        return observed

    def _complete_runtime_warmup(
        self,
        run_id: str,
        pending: tuple[Future[dict[str, Any]], WarmupIntent, float],
    ) -> None:
        future, intent, overlap_started_sec = pending
        wait_started_sec = monotonic_sec()
        observed = future.result()
        wait_ms = round((monotonic_sec() - wait_started_sec) * 1000.0, 3)
        self._last_environment_ids[intent.logical_name] = observed["execution_environment_id"]
        self._hot_until[intent.logical_name] = monotonic_sec() + float(
            self.config.get("platform", {}).get("default_retention_sec", 300)
        )
        self._warmups.add(f"{run_id}:{intent.logical_name}")
        self._executed_warmups_by_run.setdefault(run_id, []).append(intent.logical_name)
        append_jsonl(
            self.events_path,
            WarmupEvent(
                event_type="warmup",
                run_id=run_id,
                function_name=intent.function_name,
                logical_name=intent.logical_name,
                intent_id=intent.intent_id,
                action="replace",
                useful=True,
                action_reason="runtime_branch_observation_parallel_successor_warmup",
                gain=intent.offline_gain,
                invocation_type="warmup",
                cold_start=observed["cold_start"],
                request_id=observed["request_id"],
                execution_environment_id=observed["execution_environment_id"],
                latency_ms=observed["latency_ms"],
                function_duration_ms=observed["function_duration_ms"],
                status=observed["status"],
                error_type=observed["error_type"],
                error_message=observed["error_message"],
                overlap_duration_ms=round((monotonic_sec() - overlap_started_sec) * 1000.0, 3),
                blocking_wait_ms=wait_ms,
            ).to_dict(),
        )

    def _initial_warmup_budget(self) -> int:
        total = self._warmup_budget()
        if self.dag.name == "adaptive_branch" and self.baseline == "mint_markov_full":
            configured = int(self.config.get("experiment", {}).get("adaptive_initial_warmup_budget", 1))
            return max(0, min(total, configured))
        return total

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
        planner_config.setdefault("aws", {})["lambda_functions"] = dict(self.function_map)
        if self._active_model_snapshot:
            planner_config.setdefault("planner", {})["branch_probabilities"] = dict(
                self._active_model_snapshot["probabilities"]
            )
        return planner_config

    def _branch_names(self) -> tuple[str, ...]:
        if self.dag.name in {"wide_branch", "adaptive_branch"}:
            return ("f2", "f3", "f4", "f5")
        if self.dag.name == "greedy_trap":
            return ("f2", "f3", "f4")
        if self.dag.name in {"branch", "mixed", "deep_mixed"}:
            return ("left", "right")
        return tuple()

    @staticmethod
    def _context_branch(context: dict[str, Any]) -> str:
        if "branch" in context:
            return str(context["branch"])
        if "branch_index" in context:
            return ("f2", "f3", "f4", "f5")[int(context["branch_index"]) % 4]
        return ""

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
        elif self.dag.name == "adaptive_branch":
            learned = self._active_model_snapshot.get("probabilities", {})
            leaf_by_branch = {"f2": "f6", "f3": "f7", "f4": "f8", "f5": "f9"}
            for branch, leaf in leaf_by_branch.items():
                probability = float(learned.get(branch, 0.25))
                probabilities[branch] = probability
                probabilities[leaf] = probability
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
        if self.dag.name == "adaptive_branch":
            trace = self.config.get("experiment", {}).get("branch_trace", [])
            if trace:
                return {"branch": str(trace[index % len(trace)])}
            phases = self.config.get("experiment", {}).get("branch_probability_phases", [])
            probabilities = None
            for phase in phases:
                if int(phase.get("start", 0)) <= index < int(phase.get("end", 2**31)):
                    probabilities = phase.get("probabilities")
                    break
            probabilities = probabilities or {branch: 0.25 for branch in self._branch_names()}
            rng = random.Random(f"{branch_seed}:{index}")
            branches = list(self._branch_names())
            weights = [max(0.0, float(probabilities.get(branch, 0.0))) for branch in branches]
            return {"branch": rng.choices(branches, weights=weights, k=1)[0]}
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
            dry_payload = response.get("payload") if isinstance(response.get("payload"), dict) else {}
            return {
                "latency_ms": round(latency_ms, 3),
                "function_duration_ms": round(latency_ms, 3),
                "cold_start": not was_warm,
                "request_id": "",
                "execution_environment_id": "",
                "status": "ok",
                "error_type": "",
                "error_message": "",
                "observed_branch": str(dry_payload.get("observed_branch") or dry_payload.get("branch") or ""),
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
            "observed_branch": str(payload.get("observed_branch") or ""),
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
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "run_id",
                    "dag",
                    "baseline",
                    "planner_type",
                    "latency_ms",
                    "cold_start_count",
                    "warmup_count",
                    "status",
                    "block_id",
                    "function_pool",
                    "branch_seed",
                    "strategy_order",
                    "planned_arrival_time",
                    "actual_start_time",
                    "arrival_lateness_ms",
                    "warmup_overrun_ms",
                    "initial_environment_ids",
                ],
            )
            writer.writeheader()
            for row in summaries:
                writer.writerow({key: row.get(key) for key in writer.fieldnames})
