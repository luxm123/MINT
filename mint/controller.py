from __future__ import annotations

import csv
import copy
import json
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Any

from mint.aws_client import invoke_lambda
from mint.branch_history import BranchHistoryModel
from mint.events import (
    BranchModelEvent,
    IntentLifecycleEvent,
    InvocationEvent,
    SchedulerDecision,
    WarmupEvent,
    WorkflowRunSummary,
)
from mint.faascache import FaasCacheProfile, GdsfCache
from mint.intent_lifecycle import IntentBudgetLedger, IntentState, TransitionResult
from mint.intent_planner import WarmupIntent, plan_intents
from mint.metrics import compute_summary
from mint.orion import OrionBundle, OrionProfile, build_bundles, decide as orion_decide
from mint.scheduler import WarmupAction, schedule_intents
from mint.utils import append_jsonl, ensure_dir, monotonic_sec, new_id, utc_now_iso
from mint.workloads import WorkflowDAG, get_workload
from mint.xanadu import most_likely_path


SUPPORTED_BASELINES = {
    "no_warmup",
    "periodic",
    "periodic_keepwarm",
    "independent",
    "static_dag",
    "static_dag_unlimited",
    "orion_like",
    "orion_full",
    "faascache",
    "xanadu_full",
    "xanadu_like",
    "path_aware_greedy",
    "oracle_path",
    "provisioned_concurrency",
    "mint_offline",
    "mint_offline_unlimited",
    "mint_full",
    "mint_markov_offline",
    "mint_markov_no_runtime_reval",
    "mint_markov_no_long_horizon",
    "mint_markov_no_cancel",
    "mint_markov_cancel_only",
    "mint_markov_full",
}


@dataclass
class RuntimeWarmupTask:
    future: Future[dict[str, Any]]
    intent: WarmupIntent
    lifecycle_intent_id: str
    overlap_started_sec: float
    gain: float
    action: str
    target_hit: bool
    demand_submit_sec: float | None
    action_reason: str
    activation_event: Event
    scheduled_submit_sec: float


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
        if self.baseline in {"mint_markov_no_cancel", "mint_markov_cancel_only"}:
            budget = int(exp_cfg.get("warmup_budget", 1))
            if budget != 2:
                raise ValueError(
                    f"{self.baseline} is an intent-maintenance ablation defined only for warmup_budget=2; got {budget}"
                )
        if self.dag.name == "adaptive_branch" and self.baseline in {
            "mint_markov_no_cancel",
            "mint_markov_cancel_only",
            "mint_markov_full",
        }:
            budget = int(exp_cfg.get("warmup_budget", 1))
            initial_budget = int(exp_cfg.get("adaptive_initial_warmup_budget", 1))
            if budget != 2 or initial_budget != 1:
                raise ValueError(
                    "adaptive_branch intent-maintenance experiments require "
                    f"warmup_budget=2 and adaptive_initial_warmup_budget=1; "
                    f"got budget={budget}, initial={initial_budget}"
                )
        if self.baseline in {
            "mint_markov_no_cancel",
            "mint_markov_cancel_only",
            "mint_markov_full",
        } and self.dag.branch_rules:
            if len(self.dag.branch_rules) > 1:
                raise ValueError(
                    "the current runtime intent-maintenance implementation supports "
                    "one dynamic decision point per workflow run"
                )
        if bool(exp_cfg.get("local_only", False)) and not dry_run:
            raise ValueError(
                "this configuration is marked experiment.local_only=true and cannot "
                "be used for a real AWS run"
            )
        self.dry_run = dry_run
        self.output_dir = ensure_dir(output_dir or exp_cfg.get("output_dir", "results/default"))
        self.events_path = self.output_dir / "events.jsonl"
        self.runs_path = self.output_dir / "runs.csv"
        self.summary_path = self.output_dir / "summary.json"
        self._hot_until: dict[str, float] = {}
        self._executed_warmups_by_run: dict[str, list[str]] = {}
        self._intent_ledgers_by_run: dict[str, IntentBudgetLedger] = {}
        self._pending_intents_by_run: dict[str, list[WarmupIntent]] = {}
        self._scheduled_tasks_by_run: dict[str, list[RuntimeWarmupTask]] = {}
        self._warmup_failures_by_run: dict[str, int] = {}
        self._scheduler_failures_by_run: dict[str, int] = {}
        self._last_environment_ids: dict[str, str] = {}
        self.planner_type = self._planner_type_for_baseline()
        self._workflow_index = 0
        self.function_pool = str(exp_cfg.get("function_pool", self.baseline))
        if self.baseline == "orion_full":
            self._orion_bundles = build_bundles(self.dag)
            self._orion_ema: dict[str, float] = {
                node: 0.0 for node in self.dag.nodes
            }
            self._orion_warm_bundle_ids: set[str] = set()
            self._orion_memory_by_bundle: dict[str, int] = {}
            self._orion_pending_bundles: dict[str, tuple[OrionBundle, int]] = {}
        else:
            self._orion_bundles = []
            self._orion_ema = {}
            self._orion_warm_bundle_ids = set()
            self._orion_memory_by_bundle = {}
            self._orion_pending_bundles = {}
        if self.baseline == "provisioned_concurrency":
            self._provisioned_nodes = set(self._provisioning_plan())
            self._provisioned_slots = len(self._provisioned_nodes)
            for node in self._provisioned_nodes:
                self._hot_until[node] = float("inf")
        else:
            self._provisioned_nodes = set()
            self._provisioned_slots = 0
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
        if self.baseline == "faascache":
            self._faascache = GdsfCache(
                self._warmup_budget(), self._faascache_profile()
            )
            self._faascache.seed_frequencies(self._profile_call_probability())
        else:
            self._faascache = None

    def run(self, repetitions: int) -> dict[str, Any]:
        reset_each_run = bool(self.config.get("experiment", {}).get("reset_runtime_state_each_run", False))
        summaries = []
        for index in range(repetitions):
            if index > 0 and reset_each_run:
                self.reset_runtime_state()
            summaries.append(self.run_once(index))
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
        self._workflow_index = index
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
                    workflow_index=index,
                    decision_phase="initial",
                    history_size=int(self._active_model_snapshot["history_size"]),
                    branch_counts=json.dumps(self._active_model_snapshot["counts"], sort_keys=True),
                    branch_probabilities=json.dumps(self._active_model_snapshot["probabilities"], sort_keys=True),
                ).to_dict(),
            )
        initial_environment_ids = json.dumps(self._last_environment_ids, sort_keys=True)
        cold_count = 0
        warmup_count = 0
        self._warmup_failures_by_run[run_id] = 0
        self._scheduler_failures_by_run[run_id] = 0
        if self._runtime_replanning_enabled():
            self._intent_ledgers_by_run[run_id] = IntentBudgetLedger(self._warmup_budget())
        warmup_lead_sec = max(0.0, float(self.config.get("experiment", {}).get("warmup_lead_sec", 0.0)))
        if planned_arrival_sec is None:
            # Generic run()/run_once() callers still need a real pre-arrival
            # window. Otherwise warmup_start is already in the past and every
            # initial warmup necessarily overruns the advertised arrival.
            planned_arrival_sec = monotonic_sec() + warmup_lead_sec
        warmup_start_sec = planned_arrival_sec - warmup_lead_sec
        wait_for_warmup_sec = warmup_start_sec - monotonic_sec()
        if wait_for_warmup_sec > 0:
            time.sleep(wait_for_warmup_sec)

        runtime_executor: ThreadPoolExecutor | None = None
        runtime_pending_tasks: list[RuntimeWarmupTask] = []
        try:
            if self.baseline != "no_warmup":
                warmup_count += self._run_warmups(
                    run_id, intents, selected_nodes, planned_arrival_sec, index
                )
            if self._runtime_replanning_enabled():
                self._reserve_predicted_pending_intent(run_id, intents, index)
            warmup_completed_sec = monotonic_sec()

            wait_for_arrival_sec = planned_arrival_sec - monotonic_sec()
            if wait_for_arrival_sec > 0:
                time.sleep(wait_for_arrival_sec)
            workflow_start = monotonic_sec()
            workflow_start_time = utc_now_iso()
            arrival_lateness_ms = round(
                max(0.0, workflow_start - planned_arrival_sec) * 1000.0, 3
            )
            warmup_overrun_ms = (
                round(max(0.0, warmup_completed_sec - planned_arrival_sec) * 1000.0, 3)
                if warmup_count > 0
                else 0.0
            )
            stages = self.dag.stages()
            if self._runtime_replanning_enabled():
                runtime_executor = ThreadPoolExecutor(
                    max_workers=max(2, self._warmup_budget()),
                    thread_name_prefix="mint-runtime-warmup",
                )
                for pending_intent in list(
                    self._pending_intents_by_run.get(run_id, [])
                ):
                    task = self._schedule_pending_intent(
                        run_id,
                        pending_intent,
                        index,
                        runtime_executor,
                        scheduled_submit_sec=workflow_start
                        + self._pending_delay_sec(pending_intent),
                    )
                    if task is not None:
                        runtime_pending_tasks.append(task)
        except BaseException:
            if self._runtime_replanning_enabled():
                try:
                    self._cancel_unsubmitted_pending(
                        run_id, index, "workflow_setup_failed_before_submission"
                    )
                except Exception:
                    pass
            for task in list(runtime_pending_tasks):
                runtime_pending_tasks.remove(task)
                try:
                    self._complete_runtime_warmup(run_id, task)
                except Exception:
                    pass
            if runtime_executor is not None:
                runtime_executor.shutdown(wait=True, cancel_futures=True)
            self._discard_run_lifecycle_state(run_id)
            raise

        business_end_sec: float | None = None
        workflow_end_time = ""
        primary_error: BaseException | None = None
        try:
            for path_index, logical in enumerate(selected_nodes):
                function_name = self.function_map.get(logical, logical)
                stage_jitter_ms = self._timing_jitter_ms(
                    index, stages.get(logical, 0)
                )
                payload = {
                    "function_name": logical,
                    "run_id": run_id,
                    "invocation_type": "real",
                    "sleep_ms": max(1.0, 10.0 + stage_jitter_ms),
                }
                expected_branch = self._revealed_successor(logical, context)
                if expected_branch:
                    payload["branch"] = expected_branch
                response: dict[str, Any] = {}
                at_demand_decision: SchedulerDecision | None = None
                pending_task = self._find_task_for_node(
                    runtime_pending_tasks, logical
                )
                if pending_task is not None:
                    if pending_task.future.done():
                        runtime_pending_tasks.remove(pending_task)
                        # The worker has already completed.  Record the demand
                        # boundary after observing completion, which guarantees
                        # its invoke-end precedes this business demand.
                        pending_task.demand_submit_sec = monotonic_sec()
                        self._complete_runtime_warmup(run_id, pending_task)
                    else:
                        # Freeze the conservative demand boundary without doing
                        # file I/O before the real call.  A warmup finishing
                        # after this instant is classified as late even if the
                        # two SDK calls race within a few microseconds.
                        pending_task.demand_submit_sec = monotonic_sec()
                        at_demand_decision = SchedulerDecision(
                            event_type="scheduler_decision",
                            run_id=run_id,
                            workflow_index=index,
                            intent_id=pending_task.intent.intent_id,
                            function_name=pending_task.intent.function_name,
                            logical_name=pending_task.intent.logical_name,
                            action="in_flight_at_demand",
                            action_reason="warmup_not_ready_at_demand_business_continues_without_waiting",
                            gain=pending_task.gain,
                            planned_time_sec=pending_task.intent.planned_time_sec,
                            decision_phase="runtime_at_demand",
                            model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                            branch_probabilities=json.dumps(
                                self._active_model_snapshot.get("probabilities", {}), sort_keys=True
                            ),
                            decision_node=logical,
                        )
                was_warm = self._hot_until.get(logical, 0.0) > monotonic_sec()
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
                    if at_demand_decision is not None:
                        append_jsonl(self.events_path, at_demand_decision.to_dict())
                    append_jsonl(
                        self.events_path,
                        InvocationEvent(
                            event_type="invocation",
                            run_id=run_id,
                            workflow_index=index,
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
                if at_demand_decision is not None:
                    append_jsonl(self.events_path, at_demand_decision.to_dict())
                latency_ms = observed["latency_ms"]
                cold = observed["cold_start"]
                cold_count += int(cold)
                self._last_environment_ids[logical] = observed["execution_environment_id"]
                self._hot_until[logical] = monotonic_sec() + float(self.config.get("platform", {}).get("default_retention_sec", 300))
                event = InvocationEvent(
                    event_type="invocation",
                    run_id=run_id,
                    workflow_index=index,
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
                self._observe_orion_invocation(logical)
                self._observe_faascache_invocation(logical)
                if expected_branch and self._runtime_replanning_enabled():
                    observed_branch = str(observed.get("observed_branch") or "")
                    if observed_branch != expected_branch:
                        raise InvalidRealObservation(
                            f"branch observation mismatch at {logical}: "
                            f"expected={expected_branch!r}, observed={observed_branch!r}"
                        )
                    if runtime_executor is None:
                        raise RuntimeError("runtime warmup executor was not initialized")
                    runtime_pending_tasks = self._runtime_revise_after_branch(
                        run_id, intents, observed_branch, index, runtime_executor,
                        completed_nodes=set(selected_nodes[: path_index + 1]),
                        decision_node=logical,
                    )

            business_end_sec = monotonic_sec()
            workflow_end_time = utc_now_iso()
            for task in list(runtime_pending_tasks):
                runtime_pending_tasks.remove(task)
                self._complete_runtime_warmup(run_id, task)
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            if primary_error is not None and self._runtime_replanning_enabled():
                self._cancel_unsubmitted_pending(run_id, index, "workflow_failed_before_submission")
            for task in list(runtime_pending_tasks):
                runtime_pending_tasks.remove(task)
                try:
                    self._complete_runtime_warmup(run_id, task)
                except Exception:
                    if primary_error is None:
                        raise
            if primary_error is None and self._runtime_replanning_enabled():
                self._cancel_unsubmitted_pending(run_id, index, "workflow_cleanup_before_submission")
            if runtime_executor:
                runtime_executor.shutdown(wait=True, cancel_futures=True)
            if primary_error is not None:
                self._discard_run_lifecycle_state(run_id)
        reserved_budget = 0
        consumed_budget = warmup_count
        if self._runtime_replanning_enabled():
            budget_snapshot = self._intent_ledgers_by_run[run_id].snapshot()
            reserved_budget = budget_snapshot.reserved_budget
            consumed_budget = budget_snapshot.consumed_budget
            warmup_count = consumed_budget
            if consumed_budget > self._warmup_budget():
                raise RuntimeError(
                    f"real warmup budget exceeded: executed={consumed_budget}, budget={self._warmup_budget()}"
                )

        if business_end_sec is None:
            raise RuntimeError("workflow ended without a business completion timestamp")
        latency_ms = round((business_end_sec - planned_arrival_sec) * 1000.0, 3)
        provisioned_duration_sec = round(
            self._provisioned_slots
            * max(0.0, business_end_sec - workflow_start),
            6,
        )
        summary_event = WorkflowRunSummary(
            event_type="workflow_summary",
            run_id=run_id,
            workflow_index=index,
            dag=self.dag.name,
            baseline=self.baseline,
            planner_type=self.planner_type,
            latency_ms=latency_ms,
            cold_start_count=cold_count,
            warmup_count=warmup_count,
            provisioned_slots=self._provisioned_slots,
            provisioned_duration_sec=provisioned_duration_sec,
            reserved_budget=reserved_budget,
            consumed_budget=consumed_budget,
            budget_limit=self._warmup_budget(),
            unused_budget=max(0, self._warmup_budget() - consumed_budget),
            warmup_error_count=self._warmup_failures_by_run.get(run_id, 0),
            scheduler_error_count=self._scheduler_failures_by_run.get(run_id, 0),
            scheduler_status=(
                "degraded"
                if (
                    self._warmup_failures_by_run.get(run_id, 0)
                    or self._scheduler_failures_by_run.get(run_id, 0)
                )
                else "ok"
            ),
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
        result = summary_event.to_dict()
        self._discard_run_lifecycle_state(run_id)
        return result

    def _run_warmups(
        self,
        run_id: str,
        intents: list[WarmupIntent],
        selected_nodes: list[str],
        planned_arrival_sec: float,
        workflow_index: int = 0,
    ) -> int:
        exclude_entry_nodes = bool(
            self.config.get("experiment", {}).get(
                "exclude_entry_nodes_from_warmup", False
            )
        )
        if self._runtime_replanning_enabled() or exclude_entry_nodes:
            intents = [intent for intent in intents if intent.logical_name not in self.dag.entry_nodes]
        if self.baseline == "provisioned_concurrency":
            actions = []
        elif self.baseline == "orion_full":
            actions = self._orion_full_actions(intents)
        elif self.baseline == "faascache":
            actions = self._faascache_full_actions(intents)
        elif self.baseline == "xanadu_full":
            actions = self._xanadu_full_actions(intents)
        elif self.baseline in {"path_aware_greedy", "xanadu_like"}:
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
            actions = self._markov_joint_actions(intents, self._warmup_budget(), "mint_markov_offline")
        elif self.baseline == "mint_markov_no_runtime_reval":
            actions = self._markov_joint_actions(intents, self._warmup_budget(), "mint_markov_no_runtime_reval")
        elif self.baseline in {"mint_markov_no_cancel", "mint_markov_cancel_only", "mint_markov_full"}:
            actions = self._markov_joint_actions(intents, self._initial_warmup_budget(), self.baseline)
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
            decision_action = action.action_type
            if decision_action == "cancel_pending":
                # A planner candidate that was never reserved is merely not
                # selected.  The cancel_pending label is reserved for an
                # accepted PENDING -> CANCELLED lifecycle transition.
                decision_action = "not_selected"
            decision = SchedulerDecision(
                event_type="scheduler_decision",
                run_id=run_id,
                workflow_index=workflow_index,
                intent_id=intent.intent_id,
                function_name=intent.function_name,
                logical_name=intent.logical_name,
                action=decision_action,
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
            lifecycle_intent_id = intent.intent_id
            if self._runtime_replanning_enabled():
                self._create_and_reserve_lifecycle_intent(
                    run_id,
                    intent,
                    workflow_index,
                    decision_phase="initial",
                    reason="initial_joint_action_reserved",
                )
                submit_result = self._intent_ledgers_by_run[run_id].submit(
                    lifecycle_intent_id, reason="initial_warmup_call_submitted"
                )
                self._append_lifecycle_result(run_id, submit_result, workflow_index, "initial")
                self._require_lifecycle_transition(submit_result)
                count += 1
            payload = {"function_name": intent.logical_name, "run_id": run_id, "invocation_type": "warmup", "sleep_ms": 1}
            response: dict[str, Any] = {}
            invoke_start_sec = monotonic_sec()
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
                invoke_end_sec = monotonic_sec()
            except Exception as exc:
                invoke_end_sec = monotonic_sec()
                observed = self._failed_observation(response, exc)
                if self._runtime_replanning_enabled():
                    failed = self._intent_ledgers_by_run[run_id].fail(
                        lifecycle_intent_id, reason=f"initial_warmup_failed:{type(exc).__name__}"
                    )
                    self._append_lifecycle_result(run_id, failed, workflow_index, "initial")
                    self._require_lifecycle_transition(failed)
                append_jsonl(
                    self.events_path,
                    WarmupEvent(
                        event_type="warmup",
                        run_id=run_id,
                        workflow_index=workflow_index,
                        function_name=intent.function_name,
                        logical_name=intent.logical_name,
                        intent_id=intent.intent_id,
                        action=action.action_type,
                        useful=False,
                        target_hit=intent.logical_name in selected_nodes,
                        ready_before_deadline=False,
                        readiness_deadline_type="planned_arrival",
                        ready_before_arrival=False,
                        ready_before_node_demand=None,
                        ready_before_demand=False,
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
                        overlap_duration_ms=round(
                            max(0.0, invoke_end_sec - invoke_start_sec) * 1000.0, 3
                        ),
                        warmup_wall_ms=round(
                            max(0.0, invoke_end_sec - invoke_start_sec) * 1000.0, 3
                        ),
                        missed_at_arrival=intent.logical_name in selected_nodes,
                        missed_at_node_demand=False,
                        missed_at_demand=intent.logical_name in selected_nodes,
                    ).to_dict(),
                )
                if not self._runtime_replanning_enabled():
                    count += 1
                self._warmup_failures_by_run[run_id] = self._warmup_failures_by_run.get(run_id, 0) + 1
                self._orion_pending_bundles.pop(intent.intent_id, None)
                continue
            target_hit = intent.logical_name in selected_nodes
            ready_before_arrival = bool(
                target_hit and invoke_end_sec <= planned_arrival_sec
            )
            if self._runtime_replanning_enabled():
                succeeded = self._intent_ledgers_by_run[run_id].succeed(
                    lifecycle_intent_id, reason="initial_warmup_succeeded"
                )
                self._append_lifecycle_result(run_id, succeeded, workflow_index, "initial")
                self._require_lifecycle_transition(succeeded)
            self._last_environment_ids[intent.logical_name] = observed["execution_environment_id"]
            self._hot_until[intent.logical_name] = invoke_end_sec + float(
                self.config.get("platform", {}).get("default_retention_sec", 300)
            )
            warmup_event = WarmupEvent(
                event_type="warmup",
                run_id=run_id,
                workflow_index=workflow_index,
                function_name=intent.function_name,
                logical_name=intent.logical_name,
                intent_id=intent.intent_id,
                action=action.action_type,
                useful=ready_before_arrival,
                target_hit=target_hit,
                ready_before_deadline=ready_before_arrival,
                readiness_deadline_type="planned_arrival",
                ready_before_arrival=ready_before_arrival,
                ready_before_node_demand=None,
                ready_before_demand=ready_before_arrival,
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
                overlap_duration_ms=round(
                    max(0.0, invoke_end_sec - invoke_start_sec) * 1000.0, 3
                ),
                warmup_wall_ms=round(
                    max(0.0, invoke_end_sec - invoke_start_sec) * 1000.0, 3
                ),
                missed_at_arrival=target_hit and not ready_before_arrival,
                missed_at_node_demand=False,
                missed_at_demand=target_hit and not ready_before_arrival,
            )
            append_jsonl(self.events_path, warmup_event.to_dict())
            if not self._runtime_replanning_enabled():
                count += 1
            self._executed_warmups_by_run.setdefault(run_id, []).append(intent.logical_name)
            if self.baseline == "orion_full":
                self._apply_orion_bundle_success(run_id, intent, observed, invoke_end_sec)
            if self.baseline == "faascache" and self._faascache is not None:
                self._faascache.insert(intent.logical_name)
        return count

    def _create_and_reserve_lifecycle_intent(
        self,
        run_id: str,
        intent: WarmupIntent,
        workflow_index: int,
        *,
        decision_phase: str,
        reason: str,
    ) -> None:
        ledger = self._intent_ledgers_by_run[run_id]
        created = ledger.create(
            intent.intent_id,
            intent.logical_name,
            metadata={"function_name": intent.function_name, "decision_phase": decision_phase},
            scheduled_start_time=intent.planned_time_sec,
        )
        self._append_lifecycle_result(run_id, created, workflow_index, decision_phase)
        self._require_lifecycle_transition(created)
        reserved = ledger.reserve(intent.intent_id, reason=reason)
        self._append_lifecycle_result(run_id, reserved, workflow_index, decision_phase)
        self._require_lifecycle_transition(reserved)

    def _append_lifecycle_result(
        self,
        run_id: str,
        result: TransitionResult,
        workflow_index: int,
        decision_phase: str,
        *,
        submission_lateness_ms: float = 0.0,
        submission_offset_ms: float = 0.0,
    ) -> None:
        for transition in result.transitions:
            append_jsonl(
                self.events_path,
                IntentLifecycleEvent(
                    event_type="intent_lifecycle",
                    run_id=run_id,
                    workflow_index=workflow_index,
                    intent_id=transition.intent_id,
                    function_name=self.function_map.get(transition.target, transition.target),
                    logical_name=transition.target,
                    state_before=transition.state_before,
                    state_after=transition.state_after,
                    action=transition.operation,
                    reason=transition.reason,
                    reserved_budget=result.snapshot.reserved_budget,
                    consumed_budget=result.snapshot.consumed_budget,
                    actual_call_submitted=transition.actual_call_submitted,
                    supersedes_intent_id=transition.supersedes_intent_id,
                    decision_phase=decision_phase,
                    accepted=True,
                    submission_lateness_ms=submission_lateness_ms,
                    submission_offset_ms=submission_offset_ms,
                    transition_seq=transition.transition_seq,
                ).to_dict(),
            )

    def _append_lifecycle_rejection(
        self,
        run_id: str,
        intent: WarmupIntent,
        result: TransitionResult,
        workflow_index: int,
        decision_phase: str,
        *,
        submission_lateness_ms: float = 0.0,
        submission_offset_ms: float = 0.0,
    ) -> None:
        record = self._intent_ledgers_by_run[run_id].get_record(intent.intent_id)
        state = record.state.value if record else ""
        append_jsonl(
            self.events_path,
            IntentLifecycleEvent(
                event_type="intent_lifecycle",
                run_id=run_id,
                workflow_index=workflow_index,
                intent_id=intent.intent_id,
                function_name=intent.function_name,
                logical_name=intent.logical_name,
                state_before=state,
                state_after=state,
                action=f"{result.operation}_rejected",
                reason=result.reason,
                reserved_budget=result.snapshot.reserved_budget,
                consumed_budget=result.snapshot.consumed_budget,
                actual_call_submitted=False,
                decision_phase=decision_phase,
                accepted=False,
                submission_lateness_ms=submission_lateness_ms,
                submission_offset_ms=submission_offset_ms,
                transition_seq=result.decision_seq,
            ).to_dict(),
        )

    @staticmethod
    def _require_lifecycle_transition(result: TransitionResult) -> None:
        if not result.accepted:
            raise RuntimeError(f"intent lifecycle {result.operation} rejected: {result.reason}")

    def _reserve_predicted_pending_intent(
        self,
        run_id: str,
        intents: list[WarmupIntent],
        workflow_index: int,
    ) -> None:
        """Reserve up to (B - initial) predicted descendants as pending intents.

        The runtime intent-maintenance design keeps one immediate warmup slot
        plus one or more pending slots, so a budget B reserves at most B-1
        predicted descendants before the branch is revealed.  Each pending
        intent holds one reserved budget unit and is reconciled against the
        realized path after branch revelation.
        """
        ledger = self._intent_ledgers_by_run[run_id]
        if ledger.snapshot().available_budget <= 0:
            return
        prediction = self._predicted_branch_successor()
        if prediction is None:
            return
        decision_node, predicted_branch = prediction
        executed = set(self._executed_warmups_by_run.get(run_id, []))
        pending: list[WarmupIntent] = []
        for _ in range(max(0, ledger.snapshot().available_budget)):
            known_targets = {record.target for record in ledger.records()}
            candidate = self._best_runtime_candidate(
                intents,
                predicted_branch,
                completed_nodes={decision_node},
                excluded_nodes=executed | known_targets,
                budget=ledger.snapshot().available_budget,
            )
            if candidate is None:
                break
            intent, gain = candidate
            probabilities = json.dumps(
                self._active_model_snapshot.get("probabilities", {}), sort_keys=True
            )
            append_jsonl(
                self.events_path,
                SchedulerDecision(
                    event_type="scheduler_decision",
                    run_id=run_id,
                    workflow_index=workflow_index,
                    intent_id=intent.intent_id,
                    function_name=intent.function_name,
                    logical_name=intent.logical_name,
                    action="plan_pending",
                    action_reason="predicted_descendant_reserved_until_branch_reveal",
                    gain=gain,
                    planned_time_sec=intent.planned_time_sec,
                    decision_phase="initial",
                    model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                    branch_probabilities=probabilities,
                    decision_node=decision_node,
                ).to_dict(),
            )
            self._create_and_reserve_lifecycle_intent(
                run_id,
                intent,
                workflow_index,
                decision_phase="initial_pending",
                reason="predicted_descendant_pending_until_branch_reveal",
            )
            pending.append(intent)
        self._pending_intents_by_run[run_id] = pending

    def _predicted_branch_successor(self) -> tuple[str, str] | None:
        if not self.dag.branch_rules:
            return None
        stages = self.dag.stages()
        decision_node = min(self.dag.branch_rules, key=lambda node: (stages.get(node, 0), node))
        successors = list(self.dag.successors.get(decision_node, []))
        if not successors:
            return None
        learned = self._active_model_snapshot.get("probabilities", {})
        profile = self._profile_call_probability()
        aliases: dict[str, float] = {}
        if len(successors) == 2 and "left" in learned and "right" in learned:
            aliases = {
                successors[0]: float(learned["left"]),
                successors[1]: float(learned["right"]),
            }
        predicted = min(
            successors,
            key=lambda node: (
                -float(learned.get(node, aliases.get(node, profile.get(node, 0.0)))),
                node,
            ),
        )
        return decision_node, predicted

    def _best_runtime_candidate(
        self,
        intents: list[WarmupIntent],
        branch: str,
        *,
        completed_nodes: set[str],
        excluded_nodes: set[str],
        budget: int,
    ) -> tuple[WarmupIntent, float] | None:
        from mint.markov_policy import MarkovPolicyAnalyzer, MarkovState

        if budget <= 0:
            return None
        intent_by_node = {intent.logical_name: intent for intent in intents}
        reachable = self.dag.reachable_from([branch])
        now = monotonic_sec()
        candidates = [
            intent_by_node[node]
            for node in reachable - {branch}
            if node in intent_by_node
            and node not in excluded_nodes
            and node not in completed_nodes
            and self._hot_until.get(node, 0.0) <= now
        ]
        if not candidates:
            return None
        analyzer = MarkovPolicyAnalyzer(self.dag, self._planner_config(), budget=max(1, budget))
        retention = analyzer.transition_model.retention_buckets
        state = MarkovState(
            frontier=(branch,),
            hot_ttl=tuple(sorted((node, retention) for node in excluded_nodes if node in self.dag.nodes)),
            completed=tuple(sorted(completed_nodes)),
            branch_path=branch,
            time_bucket=1,
        )
        analyzer.analyze(state)
        gains = {
            candidate.logical_name: analyzer.marginal_gain(state, candidate.logical_name)
            for candidate in candidates
        }
        profitable = [candidate for candidate in candidates if gains[candidate.logical_name] > 0]
        if not profitable:
            return None
        intent = min(
            profitable,
            key=lambda item: (
                -gains[item.logical_name],
                self.dag.stages().get(item.logical_name, item.stage),
                item.logical_name,
            ),
        )
        return intent, gains[intent.logical_name]

    def _runtime_revise_after_branch(
        self,
        run_id: str,
        intents: list[WarmupIntent],
        observed_branch: str,
        workflow_index: int,
        executor: ThreadPoolExecutor,
        completed_nodes: set[str],
        decision_node: str,
    ) -> list[RuntimeWarmupTask]:
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
                workflow_index=workflow_index,
                decision_phase="runtime_after_branch",
                history_size=int(self._active_model_snapshot.get("history_size", 0)),
                branch_counts=json.dumps(self._active_model_snapshot.get("counts", {}), sort_keys=True),
                branch_probabilities=probabilities,
                observed_branch=observed_branch,
                decision_node=decision_node,
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
                    workflow_index=workflow_index,
                    intent_id=intent.intent_id,
                    function_name=intent.function_name,
                    logical_name=node,
                    action="invalidate_executed",
                    action_reason="runtime_invalidated_after_f1_already_executed_wasted",
                    gain=0.0,
                    planned_time_sec=intent.planned_time_sec,
                    decision_phase="runtime_after_branch",
                    model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                    branch_probabilities=probabilities,
                    decision_node=decision_node,
                ).to_dict(),
            )

        ledger = self._intent_ledgers_by_run[run_id]
        pending_tasks = list(self._scheduled_tasks_by_run.get(run_id, []))
        if not pending_tasks:
            return []
        pending_targets = {task.intent.logical_name for task in pending_tasks}
        retained: list[RuntimeWarmupTask] = []

        for scheduled_task in pending_tasks:
            pending_intent = scheduled_task.intent
            record = ledger.get_record(pending_intent.intent_id)
            if record is None:
                raise RuntimeError(
                    f"pending intent missing from lifecycle ledger: {pending_intent.intent_id}"
                )

            pending_is_valid = pending_intent.logical_name in actual_targets
            if pending_is_valid:
                reason = (
                    "runtime_branch_confirmed_pending_intent"
                    if record.state is IntentState.PENDING
                    else "runtime_branch_confirmed_intent_already_in_flight"
                )
                append_jsonl(
                    self.events_path,
                    SchedulerDecision(
                        event_type="scheduler_decision",
                        run_id=run_id,
                        workflow_index=workflow_index,
                        intent_id=pending_intent.intent_id,
                        function_name=pending_intent.function_name,
                        logical_name=pending_intent.logical_name,
                        action="execute_pending",
                        action_reason=reason,
                        gain=pending_intent.offline_gain,
                        planned_time_sec=pending_intent.planned_time_sec,
                        decision_phase="runtime_after_branch",
                        model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                        branch_probabilities=probabilities,
                        decision_node=decision_node,
                    ).to_dict(),
                )
                retained.append(
                    self._activate_scheduled_task(
                        scheduled_task,
                        action="execute_pending",
                        useful=True,
                        action_reason=reason,
                        gain=pending_intent.offline_gain,
                        activate=True,
                    )
                )
                continue

            if self.baseline == "mint_markov_no_cancel":
                append_jsonl(
                    self.events_path,
                    SchedulerDecision(
                        event_type="scheduler_decision",
                        run_id=run_id,
                        workflow_index=workflow_index,
                        intent_id=pending_intent.intent_id,
                        function_name=pending_intent.function_name,
                        logical_name=pending_intent.logical_name,
                        action="execute_pending",
                        action_reason="ablation_no_cancel_executes_stale_pending_intent",
                        gain=pending_intent.offline_gain,
                        planned_time_sec=pending_intent.planned_time_sec,
                        decision_phase="runtime_after_branch",
                        model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                        branch_probabilities=probabilities,
                        decision_node=decision_node,
                    ).to_dict(),
                )
                retained.append(
                    self._activate_scheduled_task(
                        scheduled_task,
                        action="execute_pending",
                        useful=False,
                        action_reason="ablation_no_cancel_executes_stale_pending_intent",
                        gain=pending_intent.offline_gain,
                        activate=False,
                    )
                )
                continue

            if record.state is not IntentState.PENDING:
                rejected = ledger.cancel_pending(
                    pending_intent.intent_id,
                    reason="runtime_cancel_race_lost_to_submission",
                )
                retained.append(
                    self._handle_cancel_race_lost(
                        run_id,
                        pending_intent,
                        scheduled_task,
                        rejected,
                        workflow_index,
                        probabilities,
                        decision_node,
                    )
                )
                continue

            replacement = self._best_runtime_candidate(
                intents,
                observed_branch,
                completed_nodes=completed_nodes,
                excluded_nodes=set(executed) | pending_targets,
                budget=1,
            )
            if self.baseline == "mint_markov_cancel_only" or replacement is None:
                cancelled = ledger.cancel_pending(
                    pending_intent.intent_id,
                    reason=(
                        "ablation_cancel_only_releases_reservation"
                        if self.baseline == "mint_markov_cancel_only"
                        else "runtime_invalid_pending_without_profitable_replacement"
                    ),
                )
                if not cancelled.accepted:
                    retained.append(
                        self._handle_cancel_race_lost(
                            run_id,
                            pending_intent,
                            scheduled_task,
                            cancelled,
                            workflow_index,
                            probabilities,
                            decision_node,
                        )
                    )
                    continue
                self._append_lifecycle_result(
                    run_id, cancelled, workflow_index, "runtime_after_branch"
                )
                self._require_lifecycle_transition(cancelled)
                scheduled_task.activation_event.set()
                self._remove_scheduled_task(
                    run_id, pending_intent.intent_id
                )
                append_jsonl(
                    self.events_path,
                    SchedulerDecision(
                        event_type="scheduler_decision",
                        run_id=run_id,
                        workflow_index=workflow_index,
                        intent_id=pending_intent.intent_id,
                        function_name=pending_intent.function_name,
                        logical_name=pending_intent.logical_name,
                        action="cancel_pending",
                        action_reason=cancelled.transitions[0].reason,
                        gain=0.0,
                        planned_time_sec=pending_intent.planned_time_sec,
                        decision_phase="runtime_after_branch",
                        model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                        branch_probabilities=probabilities,
                        decision_node=decision_node,
                    ).to_dict(),
                )
                continue

            replacement_intent, runtime_gain = replacement
            replaced = ledger.atomic_replace(
                pending_intent.intent_id,
                replacement_intent.intent_id,
                replacement_intent.logical_name,
                metadata={
                    "function_name": replacement_intent.function_name,
                    "decision_phase": "runtime_after_branch",
                },
                scheduled_start_time=replacement_intent.planned_time_sec,
                reason="runtime_invalid_pending_replaced_atomically",
            )
            if not replaced.accepted:
                retained.append(
                    self._handle_cancel_race_lost(
                        run_id,
                        pending_intent,
                        scheduled_task,
                        replaced,
                        workflow_index,
                        probabilities,
                        decision_node,
                    )
                )
                continue
            self._append_lifecycle_result(
                run_id, replaced, workflow_index, "runtime_after_branch"
            )
            self._require_lifecycle_transition(replaced)
            scheduled_task.activation_event.set()
            self._remove_scheduled_task(run_id, pending_intent.intent_id)
            append_jsonl(
                self.events_path,
                SchedulerDecision(
                    event_type="scheduler_decision",
                    run_id=run_id,
                    workflow_index=workflow_index,
                    intent_id=pending_intent.intent_id,
                    function_name=pending_intent.function_name,
                    logical_name=pending_intent.logical_name,
                    action="cancel_pending",
                    action_reason="runtime_invalid_pending_cancelled_before_submission",
                    gain=0.0,
                    planned_time_sec=pending_intent.planned_time_sec,
                    decision_phase="runtime_after_branch",
                    model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                    branch_probabilities=probabilities,
                    decision_node=decision_node,
                ).to_dict(),
            )
            append_jsonl(
                self.events_path,
                SchedulerDecision(
                    event_type="scheduler_decision",
                    run_id=run_id,
                    workflow_index=workflow_index,
                    intent_id=replacement_intent.intent_id,
                    function_name=replacement_intent.function_name,
                    logical_name=replacement_intent.logical_name,
                    action="replacement_warmup",
                    action_reason="runtime_branch_observation_parallel_successor_warmup",
                    gain=runtime_gain,
                    planned_time_sec=replacement_intent.planned_time_sec,
                    decision_phase="runtime_after_branch",
                    model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                    branch_probabilities=probabilities,
                    supersedes_intent_id=pending_intent.intent_id,
                    decision_node=decision_node,
                ).to_dict(),
            )
            new_task = self._schedule_pending_intent(
                run_id,
                replacement_intent,
                workflow_index,
                executor,
                scheduled_submit_sec=monotonic_sec(),
                action="replacement_warmup",
                useful=True,
                action_reason="runtime_branch_observation_parallel_successor_warmup",
                gain=runtime_gain,
                activate_immediately=True,
            )
            if new_task is not None:
                retained.append(new_task)

        self._pending_intents_by_run[run_id] = [
            task.intent for task in retained
        ]
        return retained

    def _handle_cancel_race_lost(
        self,
        run_id: str,
        intent: WarmupIntent,
        task: RuntimeWarmupTask,
        rejected: TransitionResult,
        workflow_index: int,
        probabilities: str,
        decision_node: str,
    ) -> RuntimeWarmupTask:
        self._append_lifecycle_rejection(
            run_id, intent, rejected, workflow_index, "runtime_after_branch"
        )
        append_jsonl(
            self.events_path,
            SchedulerDecision(
                event_type="scheduler_decision",
                run_id=run_id,
                workflow_index=workflow_index,
                intent_id=intent.intent_id,
                function_name=intent.function_name,
                logical_name=intent.logical_name,
                action="cancel_race_lost",
                action_reason="pending_intent_already_in_flight_no_replacement_budget",
                gain=0.0,
                planned_time_sec=intent.planned_time_sec,
                decision_phase="runtime_after_branch",
                model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                branch_probabilities=probabilities,
                decision_node=decision_node,
            ).to_dict(),
        )
        return self._activate_scheduled_task(
            task,
            action="execute_pending",
            useful=False,
            action_reason="cancel_race_lost_stale_in_flight_intent",
            gain=intent.offline_gain,
            activate=False,
        )

    def _schedule_pending_intent(
        self,
        run_id: str,
        intent: WarmupIntent,
        workflow_index: int,
        executor: ThreadPoolExecutor,
        *,
        scheduled_submit_sec: float,
        action: str = "execute_pending",
        useful: bool = False,
        action_reason: str = "pending_intent_waiting_for_schedule",
        gain: float | None = None,
        activate_immediately: bool = False,
    ) -> RuntimeWarmupTask | None:
        activation_event = Event()
        try:
            future = executor.submit(
                self._run_scheduled_pending_intent,
                run_id,
                intent,
                workflow_index,
                activation_event,
                scheduled_submit_sec,
            )
        except Exception as exc:
            cancelled = self._intent_ledgers_by_run[run_id].cancel_pending(
                intent.intent_id, reason=f"scheduler_worker_failed:{type(exc).__name__}"
            )
            self._append_lifecycle_result(run_id, cancelled, workflow_index, "scheduler")
            self._require_lifecycle_transition(cancelled)
            self._remove_pending_intent(run_id, intent.intent_id)
            self._scheduler_failures_by_run[run_id] = (
                self._scheduler_failures_by_run.get(run_id, 0) + 1
            )
            append_jsonl(
                self.events_path,
                SchedulerDecision(
                    event_type="scheduler_decision",
                    run_id=run_id,
                    workflow_index=workflow_index,
                    function_name=intent.function_name,
                    logical_name=intent.logical_name,
                    intent_id=intent.intent_id,
                    action="scheduler_error",
                    action_reason=f"pending_worker_submission_failed:{type(exc).__name__}",
                    gain=0.0,
                    planned_time_sec=intent.planned_time_sec,
                    decision_phase="scheduler",
                ).to_dict(),
            )
            return None
        task = RuntimeWarmupTask(
            future=future,
            intent=intent,
            lifecycle_intent_id=intent.intent_id,
            overlap_started_sec=monotonic_sec(),
            gain=intent.offline_gain if gain is None else gain,
            action=action,
            target_hit=useful,
            demand_submit_sec=None,
            action_reason=action_reason,
            activation_event=activation_event,
            scheduled_submit_sec=scheduled_submit_sec,
        )
        self._scheduled_tasks_by_run.setdefault(run_id, []).append(task)
        if activate_immediately:
            activation_event.set()
        return task

    @staticmethod
    def _activate_scheduled_task(
        task: RuntimeWarmupTask,
        *,
        action: str,
        useful: bool,
        action_reason: str,
        gain: float,
        activate: bool,
    ) -> RuntimeWarmupTask:
        task.action = action
        task.target_hit = useful
        task.action_reason = action_reason
        task.gain = gain
        if activate:
            task.activation_event.set()
        return task

    @staticmethod
    def _find_task_for_node(
        tasks: list[RuntimeWarmupTask],
        logical_name: str,
    ) -> RuntimeWarmupTask | None:
        for task in tasks:
            if task.intent.logical_name == logical_name:
                return task
        return None

    def _find_task_by_intent_id(
        self,
        run_id: str,
        intent_id: str,
    ) -> RuntimeWarmupTask | None:
        for task in self._scheduled_tasks_by_run.get(run_id, []):
            if task.lifecycle_intent_id == intent_id:
                return task
        return None

    def _remove_pending_intent(self, run_id: str, intent_id: str) -> None:
        pending = self._pending_intents_by_run.get(run_id)
        if pending is None:
            return
        for index, intent in enumerate(pending):
            if intent.intent_id == intent_id:
                pending.pop(index)
                break

    def _remove_scheduled_task(self, run_id: str, intent_id: str) -> None:
        tasks = self._scheduled_tasks_by_run.get(run_id)
        if tasks is None:
            return
        for index, task in enumerate(tasks):
            if task.lifecycle_intent_id == intent_id:
                tasks.pop(index)
                break

    def _run_scheduled_pending_intent(
        self,
        run_id: str,
        intent: WarmupIntent,
        workflow_index: int,
        activation_event: Event,
        scheduled_submit_sec: float,
    ) -> dict[str, Any]:
        wait_sec = max(0.0, scheduled_submit_sec - monotonic_sec())
        activation_event.wait(timeout=wait_sec)
        submit_time = monotonic_sec()
        offset_ms = round((submit_time - scheduled_submit_sec) * 1000.0, 3)
        lateness_ms = max(0.0, offset_ms)
        submitted = self._intent_ledgers_by_run[run_id].submit(
            intent.intent_id, reason="scheduled_warmup_call_submitted"
        )
        if not submitted.accepted:
            self._append_lifecycle_rejection(
                run_id,
                intent,
                submitted,
                workflow_index,
                "scheduler",
                submission_lateness_ms=lateness_ms,
                submission_offset_ms=offset_ms,
            )
            record = self._intent_ledgers_by_run[run_id].get_record(intent.intent_id)
            if record and record.state is IntentState.CANCELLED:
                return {"cancelled_before_submit": True}
            raise RuntimeError(f"scheduled submit rejected: {submitted.reason}")
        # The ledger claim and invocation attempt must be adjacent. In
        # particular, do not perform JSONL I/O in the small interval where a
        # branch-observation thread can see IN_FLIGHT but the SDK call has not
        # even been attempted yet.
        invoke_start_sec = monotonic_sec()
        try:
            observed = self._invoke_runtime_warmup(run_id, intent, workflow_index)
        except BaseException:
            self._append_lifecycle_result(
                run_id,
                submitted,
                workflow_index,
                "scheduler",
                submission_lateness_ms=lateness_ms,
                submission_offset_ms=offset_ms,
            )
            raise
        self._append_lifecycle_result(
            run_id,
            submitted,
            workflow_index,
            "scheduler",
            submission_lateness_ms=lateness_ms,
            submission_offset_ms=offset_ms,
        )
        observed["_warmup_submit_sec"] = submit_time
        observed["_warmup_invoke_start_sec"] = invoke_start_sec
        observed["_warmup_invoke_end_sec"] = monotonic_sec()
        return observed

    def _cancel_unsubmitted_pending(
        self,
        run_id: str,
        workflow_index: int,
        reason: str,
    ) -> None:
        pending_intents = list(self._pending_intents_by_run.get(run_id, []))
        if not pending_intents:
            return
        ledger = self._intent_ledgers_by_run[run_id]
        for intent in pending_intents:
            record = ledger.get_record(intent.intent_id)
            if record is None or record.state is not IntentState.PENDING:
                continue
            cancelled = ledger.cancel_pending(intent.intent_id, reason=reason)
            if not cancelled.accepted:
                self._append_lifecycle_rejection(
                    run_id, intent, cancelled, workflow_index, "cleanup"
                )
                continue
            self._append_lifecycle_result(
                run_id, cancelled, workflow_index, "cleanup"
            )
            self._require_lifecycle_transition(cancelled)
            self._remove_pending_intent(run_id, intent.intent_id)
            task = self._find_task_by_intent_id(run_id, intent.intent_id)
            if task is not None:
                task.activation_event.set()
            self._remove_scheduled_task(run_id, intent.intent_id)
            append_jsonl(
                self.events_path,
                SchedulerDecision(
                    event_type="scheduler_decision",
                    run_id=run_id,
                    workflow_index=workflow_index,
                    intent_id=intent.intent_id,
                    function_name=intent.function_name,
                    logical_name=intent.logical_name,
                    action="cancel_pending",
                    action_reason=reason,
                    gain=0.0,
                    planned_time_sec=intent.planned_time_sec,
                    decision_phase="cleanup",
                    model_history_size=int(self._active_model_snapshot.get("history_size", 0)),
                    branch_probabilities=json.dumps(
                        self._active_model_snapshot.get("probabilities", {}), sort_keys=True
                    ),
                ).to_dict(),
            )

    def _pending_delay_sec(self, intent: WarmupIntent) -> float:
        configured = self.config.get("experiment", {}).get("adaptive_pending_delay_sec")
        if configured is not None:
            return max(0.0, float(configured))
        if not self.dry_run:
            return max(0.0, float(intent.planned_time_sec))
        # Local controllers execute stages without wall-clock sleeps, so using
        # the full offline stage timestamp would manufacture a long blocking
        # delay. The pilot default is an explicit short scheduling window;
        # formal runs must freeze this value in their config.
        return max(0.001, min(0.05, float(intent.planned_time_sec)))

    def _discard_run_lifecycle_state(self, run_id: str) -> None:
        self._intent_ledgers_by_run.pop(run_id, None)
        self._pending_intents_by_run.pop(run_id, None)
        self._scheduled_tasks_by_run.pop(run_id, None)
        self._warmup_failures_by_run.pop(run_id, None)
        self._scheduler_failures_by_run.pop(run_id, None)
        self._executed_warmups_by_run.pop(run_id, None)

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
        pending: RuntimeWarmupTask,
    ) -> bool:
        future = pending.future
        intent = pending.intent
        overlap_started_sec = pending.overlap_started_sec
        wait_started_sec = monotonic_sec()
        try:
            observed = future.result()
        except Exception as exc:
            observed = self._failed_observation({}, exc)
            failed = self._intent_ledgers_by_run[run_id].fail(
                pending.lifecycle_intent_id,
                reason=f"runtime_warmup_failed:{type(exc).__name__}",
            )
            self._append_lifecycle_result(
                run_id, failed, self._workflow_index, "runtime_after_branch"
            )
            self._require_lifecycle_transition(failed)
            self._clear_current_scheduled_task(run_id, pending)
            self._warmup_failures_by_run[run_id] = self._warmup_failures_by_run.get(run_id, 0) + 1
            append_jsonl(
                self.events_path,
                WarmupEvent(
                    event_type="warmup",
                    run_id=run_id,
                    workflow_index=self._workflow_index,
                    function_name=intent.function_name,
                    logical_name=intent.logical_name,
                    intent_id=intent.intent_id,
                    action=pending.action,
                    useful=False,
                    target_hit=pending.target_hit,
                    ready_before_deadline=False,
                    readiness_deadline_type="node_demand",
                    ready_before_arrival=None,
                    ready_before_node_demand=False,
                    ready_before_demand=False,
                    action_reason=f"{pending.action_reason}:failed",
                    gain=pending.gain,
                    invocation_type="warmup",
                    status="error",
                    error_type=observed["error_type"],
                    error_message=observed["error_message"],
                    overlap_duration_ms=round((monotonic_sec() - overlap_started_sec) * 1000.0, 3),
                    blocking_wait_ms=round((monotonic_sec() - wait_started_sec) * 1000.0, 3),
                    missed_at_arrival=False,
                    missed_at_node_demand=(
                        pending.target_hit and pending.demand_submit_sec is not None
                    ),
                    missed_at_demand=(
                        pending.target_hit and pending.demand_submit_sec is not None
                    ),
                ).to_dict(),
            )
            return False
        if observed.get("cancelled_before_submit"):
            self._clear_current_scheduled_task(run_id, pending)
            return False
        collected_sec = monotonic_sec()
        submit_to_collect_ms = round(
            max(0.0, collected_sec - float(observed.get("_warmup_submit_sec", collected_sec))) * 1000.0,
            3,
        )
        warmup_wall_ms = round(
            max(
                0.0,
                float(observed.get("_warmup_invoke_end_sec", collected_sec))
                - float(observed.get("_warmup_invoke_start_sec", collected_sec)),
            )
            * 1000.0,
            3,
        )
        invoke_end_sec = float(observed.get("_warmup_invoke_end_sec", collected_sec))
        ready_before_node_demand = bool(
            pending.target_hit
            and pending.demand_submit_sec is not None
            and invoke_end_sec <= pending.demand_submit_sec
        )
        succeeded = self._intent_ledgers_by_run[run_id].succeed(
            pending.lifecycle_intent_id, reason=f"{pending.action}_succeeded"
        )
        self._append_lifecycle_result(
            run_id, succeeded, self._workflow_index, "runtime_after_branch"
        )
        self._require_lifecycle_transition(succeeded)
        self._clear_current_scheduled_task(run_id, pending)
        self._last_environment_ids[intent.logical_name] = observed["execution_environment_id"]
        self._hot_until[intent.logical_name] = invoke_end_sec + float(
            self.config.get("platform", {}).get("default_retention_sec", 300)
        )
        self._executed_warmups_by_run.setdefault(run_id, []).append(intent.logical_name)
        append_jsonl(
            self.events_path,
            WarmupEvent(
                event_type="warmup",
                run_id=run_id,
                workflow_index=self._workflow_index,
                function_name=intent.function_name,
                logical_name=intent.logical_name,
                intent_id=intent.intent_id,
                action=pending.action,
                useful=ready_before_node_demand,
                target_hit=pending.target_hit,
                ready_before_deadline=ready_before_node_demand,
                readiness_deadline_type="node_demand",
                ready_before_arrival=None,
                ready_before_node_demand=ready_before_node_demand,
                ready_before_demand=ready_before_node_demand,
                action_reason=pending.action_reason,
                gain=pending.gain,
                invocation_type="warmup",
                cold_start=observed["cold_start"],
                request_id=observed["request_id"],
                execution_environment_id=observed["execution_environment_id"],
                latency_ms=observed["latency_ms"],
                function_duration_ms=observed["function_duration_ms"],
                status=observed["status"],
                error_type=observed["error_type"],
                error_message=observed["error_message"],
                overlap_duration_ms=warmup_wall_ms,
                blocking_wait_ms=0.0,
                submit_to_collect_ms=submit_to_collect_ms,
                warmup_wall_ms=warmup_wall_ms,
                missed_at_arrival=False,
                missed_at_node_demand=(
                    pending.target_hit
                    and pending.demand_submit_sec is not None
                    and not ready_before_node_demand
                ),
                missed_at_demand=(
                    pending.target_hit
                    and pending.demand_submit_sec is not None
                    and not ready_before_node_demand
                ),
            ).to_dict(),
        )
        return True

    def _clear_current_scheduled_task(self, run_id: str, task: RuntimeWarmupTask) -> None:
        tasks = self._scheduled_tasks_by_run.get(run_id)
        if tasks is not None:
            for index, current in enumerate(tasks):
                if current.lifecycle_intent_id == task.lifecycle_intent_id:
                    tasks.pop(index)
                    break
        pending = self._pending_intents_by_run.get(run_id)
        if pending is not None:
            for index, current_intent in enumerate(pending):
                if current_intent.intent_id == task.lifecycle_intent_id:
                    pending.pop(index)
                    break

    def _initial_warmup_budget(self) -> int:
        total = self._warmup_budget()
        if self._runtime_replanning_enabled():
            configured = int(self.config.get("experiment", {}).get("adaptive_initial_warmup_budget", 1))
            return max(0, min(total, configured))
        return total

    def _runtime_replanning_enabled(self) -> bool:
        return self.baseline in {
            "mint_markov_no_cancel",
            "mint_markov_cancel_only",
            "mint_markov_full",
        } and bool(self.dag.branch_rules)

    def _revealed_successor(self, logical_name: str, context: dict[str, Any]) -> str:
        if logical_name not in self.dag.branch_rules:
            return ""
        successors = self.dag.next_nodes(logical_name, context)
        if len(successors) != 1:
            raise ValueError(
                f"dynamic branch {logical_name!r} must reveal exactly one successor; got {successors!r}"
            )
        return successors[0]

    def _planner_type_for_baseline(self) -> str:
        if self.baseline in {
            "mint_markov_offline",
            "mint_markov_no_runtime_reval",
            "mint_markov_no_long_horizon",
            "mint_markov_no_cancel",
            "mint_markov_cancel_only",
            "mint_markov_full",
        }:
            return "markov"
        if self.baseline in {"mint_offline", "mint_full"}:
            return "heuristic"
        if self.baseline == "periodic_keepwarm":
            return "periodic"
        if self.baseline == "orion_like":
            return "orion_like"
        if self.baseline == "orion_full":
            return "orion_full"
        if self.baseline == "faascache":
            return "faascache"
        if self.baseline == "path_aware_greedy":
            return "runtime_greedy"
        if self.baseline == "xanadu_full":
            return "xanadu_full"
        if self.baseline == "xanadu_like":
            return "xanadu_like"
        if self.baseline == "oracle_path":
            return "oracle"
        if self.baseline == "provisioned_concurrency":
            return "provisioned"
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
        if self.baseline in {
            "mint_markov_offline",
            "mint_markov_no_runtime_reval",
            "mint_markov_no_long_horizon",
            "mint_markov_no_cancel",
            "mint_markov_cancel_only",
            "mint_markov_full",
        }:
            return "markov"
        if self.baseline in {"mint_offline", "mint_full"}:
            return "heuristic"
        return "heuristic"

    def _warmup_budget(self) -> int:
        return max(0, int(self.config.get("experiment", {}).get("warmup_budget", 1)))

    def _observable_frontier(self) -> list[str]:
        return list(self.dag.entry_nodes)

    def _profile_call_probability(self) -> dict[str, float]:
        calibrated = self.config.get("experiment", {}).get("trace_calibration", {})
        branch_probabilities = calibrated.get("branch_probabilities", {})
        if branch_probabilities:
            probabilities = {node: 1.0 for node in self.dag.nodes}
            dag_branches = branch_probabilities.get(self.dag.name)
            if not (
                dag_branches
                and all(isinstance(value, dict) for value in dag_branches.values())
            ):
                # Legacy flat format {decision_node: {target: probability}}.
                dag_branches = branch_probabilities
            for _decision_node, mapping in dag_branches.items():
                for target, probability in mapping.items():
                    if target in probabilities and isinstance(
                        probability, (int, float)
                    ):
                        probabilities[target] = float(probability)
            return probabilities
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
        label = "xanadu_like" if self.baseline == "xanadu_like" else "path_aware"
        call_probability = self._profile_call_probability()
        now = monotonic_sec()
        budget = self._warmup_budget()
        actions: list[WarmupAction] = []
        candidates: list[tuple[WarmupIntent, float]] = []
        for intent in intents:
            p_call = float(call_probability.get(intent.logical_name, 0.0))
            expected_gain = self._path_aware_expected_gain(intent, p_call)
            if p_call <= 0.0:
                actions.append(WarmupAction("cancel_pending", intent, expected_gain, f"{label}_not_profile_reachable"))
            elif self._hot_until.get(intent.logical_name, 0.0) > now:
                actions.append(WarmupAction("cancel_pending", intent, expected_gain, f"{label}_already_hot"))
            else:
                candidates.append((intent, expected_gain))

        ranked = sorted(candidates, key=lambda item: (-item[1], item[0].stage, item[0].planned_time_sec, item[0].logical_name))
        for index, (intent, expected_gain) in enumerate(ranked):
            if index < budget:
                actions.append(WarmupAction("execute", intent, expected_gain, f"{label}_profile_expected_gain_within_budget"))
            else:
                actions.append(WarmupAction("cancel_pending", intent, expected_gain, f"{label}_budget_exceeded"))
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
                actions.append(WarmupAction("cancel_pending", intent, intent.offline_gain, "oracle_not_on_real_path"))
            elif self._hot_until.get(intent.logical_name, 0.0) > now:
                actions.append(WarmupAction("cancel_pending", intent, intent.offline_gain, "oracle_already_hot"))
            else:
                candidates.append(intent)

        ranked = sorted(candidates, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        for index, intent in enumerate(ranked):
            if index < budget:
                actions.append(WarmupAction("execute", intent, intent.offline_gain, "oracle_path_upper_bound"))
            else:
                actions.append(WarmupAction("cancel_pending", intent, intent.offline_gain, "oracle_budget_exceeded"))
        return actions

    def _orion_profile(self) -> OrionProfile:
        baseline_cfg = self.config.get("baseline", {}).get("orion_full", {})
        base_cold = float(
            self.config.get("platform", {}).get("default_cold_start_ms", 800)
        )
        return OrionProfile(
            memory_options=tuple(
                int(memory)
                for memory in baseline_cfg.get("memory_options", (128, 256, 512, 1024))
            ),
            decay=float(baseline_cfg.get("decay", 0.5)),
            base_cold_ms=base_cold,
        )

    def _orion_call_probability(self) -> dict[str, float]:
        ema = self._orion_ema
        max_value = max(ema.values()) if ema else 0.0
        if max_value <= 0.0:
            return self._profile_call_probability()
        return {
            node: round(ema[node] / max_value, 6) for node in self.dag.nodes
        }

    def _orion_full_actions(self, intents: list[WarmupIntent]) -> list[WarmupAction]:
        profile = self._orion_profile()
        call_probability = self._orion_call_probability()
        downstream = self.dag.downstream_counts()
        max_downstream = max(downstream.values() or [1])
        downstream_weight = {
            node: 1.0 + downstream.get(node, 0) / max(max_downstream, 1)
            for node in self.dag.nodes
        }
        now = monotonic_sec()
        hot_nodes = {
            node for node, until in self._hot_until.items() if until > now
        }
        decisions = orion_decide(
            profile,
            self._orion_bundles,
            call_probability,
            downstream_weight,
            hot_nodes,
            self._orion_warm_bundle_ids,
            self._warmup_budget(),
        )
        intent_by_node = {intent.logical_name: intent for intent in intents}
        actions: list[WarmupAction] = []
        for decision in decisions:
            intent = intent_by_node.get(decision.bundle.representative)
            if intent is None:
                continue
            actions.append(
                WarmupAction(
                    "execute",
                    intent,
                    decision.gain,
                    f"orion_full_bundle:{decision.bundle.bundle_id}:mem{decision.memory_mb}",
                )
            )
            self._orion_pending_bundles[intent.intent_id] = (
                decision.bundle,
                decision.memory_mb,
            )
        return actions

    def _apply_orion_bundle_success(
        self,
        run_id: str,
        intent: WarmupIntent,
        observed: dict[str, Any],
        invoke_end_sec: float,
    ) -> None:
        pending = self._orion_pending_bundles.pop(intent.intent_id, None)
        if pending is None:
            return
        bundle, memory_mb = pending
        retention = float(
            self.config.get("platform", {}).get("default_retention_sec", 300)
        )
        for member in bundle.members:
            self._hot_until[member] = invoke_end_sec + retention
            self._last_environment_ids[member] = observed["execution_environment_id"]
        self._orion_warm_bundle_ids.add(bundle.bundle_id)
        self._orion_memory_by_bundle[bundle.bundle_id] = memory_mb

    def _observe_orion_invocation(self, logical_name: str) -> None:
        if self.baseline != "orion_full":
            return
        decay = self._orion_profile().decay
        for node in self._orion_ema:
            self._orion_ema[node] *= decay
        self._orion_ema[logical_name] += 1.0 - decay

    def _faascache_profile(self) -> FaasCacheProfile:
        baseline_cfg = self.config.get("baseline", {}).get("faascache", {})
        cold_ms = self.config.get("platform", {}).get("cold_start_ms", {})
        return FaasCacheProfile(
            frequency_decay=float(baseline_cfg.get("frequency_decay", 0.0)),
            size_mb=baseline_cfg.get("size_mb"),
            cold_start_ms=dict(cold_ms) if cold_ms else None,
            base_cold_ms=float(
                self.config.get("platform", {}).get("default_cold_start_ms", 800)
            ),
        )

    def _faascache_full_actions(self, intents: list[WarmupIntent]) -> list[WarmupAction]:
        """GDSF selection: fill/replace the B keep-warm slots by value."""
        cache = self._faascache
        if cache is None:
            return []
        now = monotonic_sec()
        budget = self._warmup_budget()
        actions: list[WarmupAction] = []
        candidates: list[WarmupIntent] = []
        for intent in intents:
            node = intent.logical_name
            if self._hot_until.get(node, 0.0) > now:
                actions.append(
                    WarmupAction(
                        "cancel_pending",
                        intent,
                        cache.value(node),
                        "faascache_already_hot",
                    )
                )
            else:
                candidates.append(intent)
        ranked = sorted(
            candidates,
            key=lambda intent: (-cache.value(intent.logical_name), intent.logical_name),
        )
        targets = set(
            cache.allocate((intent.logical_name for intent in ranked), budget)
        )
        for intent in ranked:
            if intent.logical_name in targets:
                actions.append(
                    WarmupAction(
                        "execute",
                        intent,
                        cache.value(intent.logical_name),
                        "faascache_gdsf_top_value",
                    )
                )
            else:
                actions.append(
                    WarmupAction(
                        "cancel_pending",
                        intent,
                        cache.value(intent.logical_name),
                        "faascache_not_in_top_value",
                    )
                )
        return actions

    def _observe_faascache_invocation(self, logical_name: str) -> None:
        if self.baseline != "faascache" or self._faascache is None:
            return
        self._faascache.observe(logical_name)

    def _xanadu_call_probability(self) -> dict[str, float]:
        """MLP probabilities: trace/static profile overlaid with the learned
        branch model where the workload exposes explicit branch names."""
        probabilities = self._profile_call_probability()
        learned = self._active_model_snapshot.get("probabilities", {})
        if not learned:
            return probabilities
        if self.dag.name in {"branch", "mixed", "deep_mixed"}:
            mapping = {"left": "f2", "right": "f3"}
            for branch_key, node in mapping.items():
                if branch_key in learned and node in probabilities:
                    probabilities[node] = float(learned[branch_key])
        else:
            for node in self.dag.nodes:
                if node in learned:
                    probabilities[node] = float(learned[node])
        return probabilities

    def _xanadu_full_actions(self, intents: list[WarmupIntent]) -> list[WarmupAction]:
        """Xanadu MLP selection: warm the earliest-stage most-likely-path
        members that are not already warm, within budget B."""
        call_probability = self._xanadu_call_probability()
        path = most_likely_path(self.dag, call_probability)
        path_set = set(path)
        now = monotonic_sec()
        budget = self._warmup_budget()
        stages = self.dag.stages()
        ordered = sorted(
            (
                intent
                for intent in intents
                if intent.logical_name in path_set
                and intent.logical_name not in self.dag.entry_nodes
            ),
            key=lambda intent: (
                stages.get(intent.logical_name, 0),
                intent.logical_name,
            ),
        )
        targets: list[str] = []
        for intent in ordered:
            if len(targets) >= budget:
                break
            if self._hot_until.get(intent.logical_name, 0.0) <= now:
                targets.append(intent.logical_name)
        target_set = set(targets)
        actions: list[WarmupAction] = []
        for intent in intents:
            node = intent.logical_name
            if node in self.dag.entry_nodes:
                reason = "xanadu_full_entry_not_prewarmed"
                action_type = "cancel_pending"
            elif node not in path_set:
                reason = "xanadu_full_not_on_mle_path"
                action_type = "cancel_pending"
            elif node in target_set:
                reason = "xanadu_full_mle_jit"
                action_type = "execute"
            elif self._hot_until.get(node, 0.0) > now:
                reason = "xanadu_full_already_hot"
                action_type = "cancel_pending"
            else:
                reason = "xanadu_full_budget_exceeded"
                action_type = "cancel_pending"
            actions.append(
                WarmupAction(
                    action_type,
                    intent,
                    intent.offline_gain,
                    reason,
                )
            )
        return actions

    def _provisioning_plan(self) -> list[str]:
        if self.baseline != "provisioned_concurrency":
            return []
        budget = self._warmup_budget()
        call_probability = self._profile_call_probability()
        downstream = self.dag.downstream_counts()
        scored = sorted(
            self.dag.nodes,
            key=lambda node: (
                -(
                    float(call_probability.get(node, 1.0))
                    * (1.0 + downstream.get(node, 0))
                ),
                node,
            ),
        )
        return scored[: max(0, budget)]

    def _markov_offline_actions(self, intents: list[WarmupIntent]) -> list[WarmupAction]:
        ranked = sorted(intents, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        budget = self._warmup_budget()
        actions: list[WarmupAction] = []
        for index, intent in enumerate(ranked):
            if index < budget:
                actions.append(WarmupAction("execute", intent, intent.offline_gain, "mint_markov_offline_top_intent_within_budget"))
            else:
                actions.append(WarmupAction("cancel_pending", intent, intent.offline_gain, "mint_markov_offline_budget_exceeded"))
        return actions

    def _markov_joint_actions(
        self,
        intents: list[WarmupIntent],
        budget: int,
        label: str,
    ) -> list[WarmupAction]:
        """Select the analyzer's joint action, not the top-B singleton gains."""
        from mint.markov_policy import MarkovAction, MarkovPolicyAnalyzer

        analyzer = MarkovPolicyAnalyzer(self.dag, self._planner_config(), budget=budget)
        initial = analyzer.transition_model.initial_state()
        policy = analyzer.analyze(initial)
        selected = set(policy.get(initial, MarkovAction(tuple())).warmup_functions)
        now = monotonic_sec()
        actions: list[WarmupAction] = []
        for intent in intents:
            gain = analyzer.marginal_gain(initial, intent.logical_name)
            if intent.logical_name in selected and self._hot_until.get(intent.logical_name, 0.0) <= now:
                actions.append(WarmupAction("execute", intent, gain, f"{label}_joint_q_action"))
            else:
                reason = f"{label}_already_hot" if intent.logical_name in selected else f"{label}_not_in_joint_q_action"
                actions.append(WarmupAction("cancel_pending", intent, gain, reason))
        return actions

    def _markov_no_runtime_reval_actions(self, intents: list[WarmupIntent]) -> list[WarmupAction]:
        ranked = sorted(intents, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
        budget = self._warmup_budget()
        now = monotonic_sec()
        executed = 0
        actions: list[WarmupAction] = []
        for intent in ranked:
            if self._hot_until.get(intent.logical_name, 0.0) > now:
                actions.append(WarmupAction("cancel_pending", intent, intent.offline_gain, "mint_markov_no_runtime_reval_already_hot"))
            elif executed < budget:
                actions.append(WarmupAction("execute", intent, intent.offline_gain, "mint_markov_no_runtime_reval_offline_rank_within_budget"))
                executed += 1
            else:
                actions.append(WarmupAction("cancel_pending", intent, intent.offline_gain, "mint_markov_no_runtime_reval_budget_exceeded"))
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
                actions.append(WarmupAction("cancel_pending", intent, intent.offline_gain, f"{self.baseline}_budget_exceeded"))
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
        if self.dag.name == "deep_mixed":
            mismatch = bool(self.config.get("experiment", {}).get("profile_mismatch", False))
            if mismatch:
                # Controlled branch-profile mismatch: the planner profile stays
                # 50/50, while the realized path skews to 1/3 vs 2/3.
                return {"branch": "left" if (index + branch_seed) % 3 == 0 else "right"}
            return {"branch": "left" if (index + branch_seed) % 2 == 0 else "right"}
        return {"branch": "left" if (index + branch_seed) % 2 == 0 else "right"}

    def _timing_jitter_ms(self, index: int, stage: int) -> float:
        jitter = float(self.config.get("experiment", {}).get("timing_jitter_ms", 0.0))
        if jitter <= 0 or self.dag.name not in {"wide_branch", "deep_mixed", "greedy_trap"}:
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
                    "workflow_index",
                    "dag",
                    "baseline",
                    "planner_type",
                    "latency_ms",
                    "cold_start_count",
                    "warmup_count",
                    "reserved_budget",
                    "consumed_budget",
                    "budget_limit",
                    "unused_budget",
                    "warmup_error_count",
                    "scheduler_status",
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
