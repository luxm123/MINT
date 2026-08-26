from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

import mint.controller as controller_mod
from mint.controller import MintController
from mint.workloads import get_workload


def _config(
    output_dir: Path,
    baseline: str,
    dag_name: str,
    budget: int,
    *,
    branch_seed: int = 42,
    profile_mismatch: bool = True,
    timing_jitter_ms: float = 800.0,
) -> dict[str, Any]:
    dag = get_workload(dag_name)
    return {
        "aws": {"lambda_functions": {node: f"mint-{node}" for node in dag.nodes}},
        "experiment": {
            "baseline": baseline,
            "warmup_budget": budget,
            "output_dir": str(output_dir),
            "branch_seed": branch_seed,
            "profile_mismatch": profile_mismatch,
            "timing_jitter_ms": timing_jitter_ms,
        },
        "platform": {
            "default_retention_sec": 300,
            "default_cold_start_ms": 800,
            "default_warm_duration_ms": 100,
        },
        "planner": {"type": "markov", "horizon": 5},
        "scheduler": {
            "enable_delay": True,
            "enable_cancel": True,
            "enable_replace": True,
            "gain_threshold": 0.0,
        },
    }


def _events(output_dir: Path) -> list[dict[str, Any]]:
    path = output_dir / "events.jsonl"
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


ALLOWED_LIFECYCLE_EDGES = {
    ("", "planned"),
    ("planned", "pending"),
    ("planned", "planned"),
    ("pending", "in_flight"),
    ("pending", "cancelled"),
    ("pending", "pending"),
    ("in_flight", "succeeded"),
    ("in_flight", "failed"),
    ("in_flight", "in_flight"),
    ("cancelled", "cancelled"),
    ("failed", "failed"),
    ("succeeded", "succeeded"),
}


def _lifecycle_by_intent(events: list[dict[str, Any]]) -> dict[str, list[tuple[str, str, bool]]]:
    by_intent: dict[str, list[tuple[str, str, bool]]] = {}
    for event in events:
        if event.get("event_type") != "intent_lifecycle":
            continue
        intent_id = str(event["intent_id"])
        by_intent.setdefault(intent_id, []).append(
            (
                str(event["state_before"]),
                str(event["state_after"]),
                bool(event.get("actual_call_submitted")),
            )
        )
    return by_intent


def _assert_valid_lifecycle(events: list[dict[str, Any]]) -> None:
    by_intent = _lifecycle_by_intent(events)
    if not by_intent:
        # Runs that reuse already-hot Lambda environments may warm nothing and
        # therefore create no new intents.
        return
    for intent_id, transitions in by_intent.items():
        # Normal intents start with create ("" -> planned); replacement
        # intents are born inside atomic_replace directly as planned -> pending.
        assert transitions[0][:2] in {("", "planned"), ("planned", "pending")}, (
            intent_id,
            transitions[0][:2],
        )
        for (before, after, submitted) in transitions:
            assert (before, after) in ALLOWED_LIFECYCLE_EDGES, (intent_id, before, after)
            assert submitted == ((before, after) == ("pending", "in_flight")), (
                intent_id,
                before,
                after,
                submitted,
            )
        final_state = transitions[-1][1]
        assert final_state in {"succeeded", "failed", "cancelled"}, (intent_id, final_state)


def _summaries(output_dir: Path) -> list[dict[str, Any]]:
    return [
        event
        for event in _events(output_dir)
        if event.get("event_type") == "workflow_summary"
    ]


def test_b3_wide_branch_budget_and_lifecycle_invariants(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "wide_b3"
    config = _config(output_dir, "mint_markov_full", "wide_branch", 3)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="mint_markov_full",
        dry_run=True,
    )

    summary = controller.run(3)
    events = _events(output_dir)
    summaries = _summaries(output_dir)

    assert summary["aborted_run_count"] == 0
    assert summary["scheduler_error_total"] == 0
    assert summary["consumed_budget_total"] <= 9
    assert summary["consumed_budget_total"] == summary["total_warmup"]
    assert len(summaries) == 3
    for run in summaries:
        assert run["budget_limit"] == 3
        assert run["consumed_budget"] <= 3
        assert run["reserved_budget"] == 0
        assert run["unused_budget"] == 3 - run["consumed_budget"]
        run_events = [event for event in events if event.get("run_id") == run["run_id"]]
        _assert_valid_lifecycle(run_events)
    assert controller._pending_intents_by_run == {}
    assert controller._scheduled_tasks_by_run == {}


def test_b3_wide_branch_reserves_two_pending_slots_with_default_initial_one(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "wide_pending"
    config = _config(output_dir, "mint_markov_full", "wide_branch", 3)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="mint_markov_full",
        dry_run=True,
    )

    controller.run_once(0)
    events = _events(output_dir)
    plan_pending = [
        event
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "plan_pending"
    ]
    assert 1 <= len(plan_pending) <= 2
    assert {event["logical_name"] for event in plan_pending} <= {"f6", "f7"}
    executed = [
        event
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "execute"
    ]
    assert len(executed) == 1


def test_b3_custom_initial_two_reserves_one_pending(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "wide_initial2"
    config = _config(output_dir, "mint_markov_full", "wide_branch", 3)
    config["experiment"]["adaptive_initial_warmup_budget"] = 2
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="mint_markov_full",
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)
    plan_pending = [
        event
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "plan_pending"
    ]
    executed = [
        event
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "execute"
    ]
    assert len(plan_pending) <= 1
    assert len(executed) == 2
    assert result["warmup_count"] <= 3
    _assert_valid_lifecycle(events)


def test_b3_deep_mixed_mismatch_cancels_or_replaces_stale_pending_within_budget(tmp_path):
    dag = get_workload("deep_mixed")
    output_dir = tmp_path / "deep_mixed_b3"
    config = _config(
        output_dir,
        "mint_markov_full",
        "deep_mixed",
        3,
        branch_seed=43,
    )
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="mint_markov_full",
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)
    assert controller._run_context(0) == {"branch": "right"}
    assert result["consumed_budget"] <= 3
    runtime_actions = {
        event["action"]
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("decision_phase") == "runtime_after_branch"
    }
    assert {"cancel_pending", "replacement_warmup"} <= runtime_actions, runtime_actions
    replacement_warmups = [
        event["logical_name"]
        for event in events
        if event.get("event_type") == "warmup"
        and event.get("action") == "replacement_warmup"
    ]
    assert replacement_warmups == ["f5"]
    _assert_valid_lifecycle(events)


def test_b3_concurrent_pending_submissions_stay_within_budget(tmp_path, monkeypatch):
    def slow_dry_invoke(
        function_name: str,
        payload: dict[str, Any],
        invocation_type: str = "RequestResponse",
        dry_run: bool = True,
        region_name: str | None = None,
    ) -> dict[str, Any]:
        del invocation_type, dry_run, region_name
        if payload["invocation_type"] == "warmup":
            time.sleep(0.02)
        return {
            "dry_run": True,
            "function_name": function_name,
            "payload": payload,
            "status_code": 200,
        }

    monkeypatch.setattr(controller_mod, "invoke_lambda", slow_dry_invoke)
    dag = get_workload("deep_mixed")
    output_dir = tmp_path / "deep_mixed_concurrent"
    config = _config(output_dir, "mint_markov_full", "deep_mixed", 3)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="mint_markov_full",
        dry_run=True,
    )

    summary = controller.run(2)
    events = _events(output_dir)
    assert summary["aborted_run_count"] == 0
    assert summary["consumed_budget_total"] <= 6
    assert summary["scheduler_error_total"] == 0
    _assert_valid_lifecycle(events)
    for run in _summaries(output_dir):
        assert run["consumed_budget"] <= 3
        assert run["reserved_budget"] == 0


def test_b1_still_runs_single_immediate_warmup_without_pending(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "wide_b1"
    config = _config(output_dir, "mint_markov_full", "wide_branch", 1)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="mint_markov_full",
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)
    plan_pending = [
        event
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "plan_pending"
    ]
    assert result["warmup_count"] == 1
    assert result["consumed_budget"] == 1
    assert plan_pending == []
