from __future__ import annotations

import json
import time
from pathlib import Path
from threading import Event
from typing import Any

import pytest

import mint.controller as controller_mod
from mint.controller import MintController
from mint.workloads import get_workload


ABLATIONS = (
    "mint_markov_no_cancel",
    "mint_markov_cancel_only",
    "mint_markov_full",
)


def _adaptive_config(
    output_dir: Path,
    baseline: str,
    actual_branch: str,
) -> dict[str, Any]:
    dag = get_workload("adaptive_branch")
    return {
        "aws": {
            "lambda_functions": {node: f"mint-{node}" for node in dag.nodes},
        },
        "experiment": {
            "baseline": baseline,
            "warmup_budget": 2,
            "adaptive_initial_warmup_budget": 1,
            "branch_trace": [actual_branch],
            "output_dir": str(output_dir),
        },
        "platform": {
            "default_retention_sec": 300,
            "default_cold_start_ms": 800,
            "default_warm_duration_ms": 100,
        },
        "planner": {
            "type": "markov",
            "horizon": 5,
            "historical_branch_records": (
                ["f2"] * 70 + ["f3"] * 10 + ["f4"] * 10 + ["f5"] * 10
            ),
        },
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


def _capturing_invoke(
    calls: list[tuple[str, str]],
    *,
    delay_warmup: str = "",
    delay_sec: float = 0.0,
    fail_warmup: str = "",
):
    def invoke(
        function_name: str,
        payload: dict[str, Any],
        invocation_type: str = "RequestResponse",
        dry_run: bool = True,
        region_name: str | None = None,
    ) -> dict[str, Any]:
        del invocation_type, dry_run, region_name
        logical_name = str(payload["function_name"])
        call_type = str(payload["invocation_type"])
        calls.append((call_type, logical_name))
        if call_type == "warmup" and logical_name == delay_warmup:
            time.sleep(delay_sec)
        if call_type == "warmup" and logical_name == fail_warmup:
            raise TimeoutError(f"synthetic {logical_name} warmup timeout")
        return {
            "dry_run": True,
            "function_name": function_name,
            "payload": payload,
            "status_code": 200,
        }

    return invoke


def _warmup_targets(events: list[dict[str, Any]]) -> list[str]:
    return [
        str(event["logical_name"])
        for event in events
        if event.get("event_type") == "warmup"
    ]


def _runtime_decisions(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("decision_phase") == "runtime_after_branch"
    ]


def _true_cancellations(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if event.get("event_type") == "intent_lifecycle"
        and event.get("state_before") == "pending"
        and event.get("state_after") == "cancelled"
    ]


@pytest.mark.parametrize("baseline", ABLATIONS)
def test_correct_prediction_executes_reserved_leaf_without_cancel_or_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    baseline: str,
) -> None:
    output_dir = tmp_path / baseline
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(controller_mod, "invoke_lambda", _capturing_invoke(calls))
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f2"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)
    runtime = _runtime_decisions(events)
    f6_lifecycle = [
        event
        for event in events
        if event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f6"
    ]

    assert [node for call_type, node in calls if call_type == "warmup"] == ["f2", "f6"]
    assert _warmup_targets(events) == ["f2", "f6"]
    assert any(
        event.get("logical_name") == "f6" and event.get("action") == "execute_pending"
        for event in runtime
    )
    assert not any(
        event.get("action") in {"cancel_pending", "replacement_warmup"}
        for event in runtime
    )
    assert not _true_cancellations(events)
    assert not any(
        event.get("action") == "replacement_warmup"
        for event in events
        if event.get("event_type") == "intent_lifecycle"
    )
    assert [(event["state_before"], event["state_after"]) for event in f6_lifecycle] == [
        ("", "planned"),
        ("planned", "pending"),
        ("pending", "in_flight"),
        ("in_flight", "succeeded"),
    ]
    assert result["warmup_count"] == 2
    assert result["consumed_budget"] == 2
    assert result["reserved_budget"] == 0
    assert result["budget_limit"] == 2


@pytest.mark.parametrize(
    "baseline",
    ("mint_markov_no_cancel", "mint_markov_cancel_only", "mint_markov_full"),
)
def test_adaptive_intent_ablations_require_two_total_and_one_initial_budget(
    tmp_path: Path,
    baseline: str,
) -> None:
    wrong_total = _adaptive_config(tmp_path / "total", baseline, "f2")
    wrong_total["experiment"]["warmup_budget"] = 3
    with pytest.raises(ValueError, match="warmup_budget=2"):
        MintController(
            wrong_total,
            dag=get_workload("adaptive_branch"),
            baseline=baseline,
            dry_run=True,
        )

    wrong_initial = _adaptive_config(tmp_path / "initial", baseline, "f2")
    wrong_initial["experiment"]["adaptive_initial_warmup_budget"] = 2
    with pytest.raises(ValueError, match="adaptive_initial_warmup_budget=1"):
        MintController(
            wrong_initial,
            dag=get_workload("adaptive_branch"),
            baseline=baseline,
            dry_run=True,
        )


def test_runtime_intent_maintenance_runs_budget_three_on_wide_branch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "wide"
    config = _adaptive_config(output_dir, "mint_markov_full", "f2")
    config["experiment"]["warmup_budget"] = 3
    config["aws"]["lambda_functions"] = {
        node: f"mint-{node}" for node in dag.nodes
    }
    monkeypatch.setattr(controller_mod, "invoke_lambda", _capturing_invoke([]))

    controller = MintController(
        config,
        dag=dag,
        baseline="mint_markov_full",
        dry_run=True,
    )
    result = controller.run_once(0)
    events = _events(output_dir)

    assert result["warmup_count"] <= 3
    assert result["consumed_budget"] <= 3
    assert result["reserved_budget"] == 0
    assert result["budget_limit"] == 3
    plan_pending = [
        event
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "plan_pending"
    ]
    assert 1 <= len(plan_pending) <= 2
    assert {event["logical_name"] for event in plan_pending} <= {"f6", "f7"}
    assert controller._pending_intents_by_run == {}
    assert controller._scheduled_tasks_by_run == {}


@pytest.mark.parametrize(
    (
        "baseline",
        "expected_warmups",
        "expected_runtime_actions",
        "expected_consumed",
        "expected_true_cancels",
    ),
    (
        (
            "mint_markov_no_cancel",
            ["f2", "f6"],
            {"invalidate_executed", "execute_pending"},
            2,
            0,
        ),
        (
            "mint_markov_cancel_only",
            ["f2"],
            {"invalidate_executed", "cancel_pending"},
            1,
            1,
        ),
        (
            "mint_markov_full",
            ["f2", "f8"],
            {"invalidate_executed", "cancel_pending", "replacement_warmup"},
            2,
            1,
        ),
    ),
)
def test_wrong_prediction_has_distinct_ablation_behavior_and_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    baseline: str,
    expected_warmups: list[str],
    expected_runtime_actions: set[str],
    expected_consumed: int,
    expected_true_cancels: int,
) -> None:
    output_dir = tmp_path / baseline
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(controller_mod, "invoke_lambda", _capturing_invoke(calls))
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f4"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)
    runtime = _runtime_decisions(events)
    warmup_events = [event for event in events if event.get("event_type") == "warmup"]

    assert [node for call_type, node in calls if call_type == "warmup"] == expected_warmups
    assert _warmup_targets(events) == expected_warmups
    assert {event["action"] for event in runtime} == expected_runtime_actions
    assert len(_true_cancellations(events)) == expected_true_cancels
    assert result["warmup_count"] == expected_consumed
    assert result["consumed_budget"] == expected_consumed
    assert result["reserved_budget"] == 0
    assert result["consumed_budget"] <= result["budget_limit"] == 2

    if baseline == "mint_markov_no_cancel":
        assert [event["useful"] for event in warmup_events] == [False, False]
        assert not any(event.get("logical_name") == "f8" for event in warmup_events)
    elif baseline == "mint_markov_cancel_only":
        assert [event["useful"] for event in warmup_events] == [False]
        assert not any(
            call_type == "warmup" and node in {"f6", "f8"}
            for call_type, node in calls
        )
    else:
        assert [event["target_hit"] for event in warmup_events] == [False, True]
        assert not any(
            call_type == "warmup" and node == "f6" for call_type, node in calls
        )


def test_full_wrong_prediction_logs_cancel_then_atomic_replacement_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(controller_mod, "invoke_lambda", _capturing_invoke(calls))
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f4"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    controller.run_once(0)
    events = _events(output_dir)

    f1_invocation_index = next(
        index
        for index, event in enumerate(events)
        if event.get("event_type") == "invocation" and event.get("logical_name") == "f1"
    )
    runtime_decision_indices = {
        (event["logical_name"], event["action"]): index
        for index, event in enumerate(events)
        if event.get("event_type") == "scheduler_decision"
        and event.get("decision_phase") == "runtime_after_branch"
    }
    cancel_lifecycle_index = next(
        index
        for index, event in enumerate(events)
        if event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f6"
        and event.get("state_before") == "pending"
        and event.get("state_after") == "cancelled"
    )
    replacement_reserve_index = next(
        index
        for index, event in enumerate(events)
        if event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f8"
        and event.get("state_before") == "planned"
        and event.get("state_after") == "pending"
    )
    replacement_submit_index = next(
        index
        for index, event in enumerate(events)
        if event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f8"
        and event.get("state_before") == "pending"
        and event.get("state_after") == "in_flight"
    )
    replacement_success_index = next(
        index
        for index, event in enumerate(events)
        if event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f8"
        and event.get("state_before") == "in_flight"
        and event.get("state_after") == "succeeded"
    )
    cancel_event = events[cancel_lifecycle_index]
    replacement_event = events[replacement_reserve_index]

    assert f1_invocation_index < runtime_decision_indices[("f2", "invalidate_executed")]
    assert (
        runtime_decision_indices[("f2", "invalidate_executed")]
        < runtime_decision_indices[("f6", "cancel_pending")]
        < runtime_decision_indices[("f8", "replacement_warmup")]
    )
    assert f1_invocation_index < cancel_lifecycle_index < replacement_reserve_index
    assert replacement_reserve_index < replacement_submit_index < replacement_success_index
    assert cancel_event["actual_call_submitted"] is False
    assert events[replacement_submit_index]["actual_call_submitted"] is True
    assert replacement_event["supersedes_intent_id"] == cancel_event["intent_id"]
    assert runtime_decision_indices[("f8", "replacement_warmup")] < replacement_submit_index


def test_failed_replacement_is_consumed_logged_and_degrades_without_aborting_business(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        controller_mod,
        "invoke_lambda",
        _capturing_invoke(calls, fail_warmup="f8"),
    )
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f4"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    result = controller.run_once(0)

    events = _events(output_dir)
    failed_lifecycle = next(
        event
        for event in events
        if event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f8"
        and event.get("state_before") == "in_flight"
        and event.get("state_after") == "failed"
    )
    submitted_lifecycle = next(
        event
        for event in events
        if event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f8"
        and event.get("state_before") == "pending"
        and event.get("state_after") == "in_flight"
    )
    failure = next(
        event
        for event in events
        if event.get("event_type") == "warmup"
        and event.get("logical_name") == "f8"
        and event.get("status") == "error"
    )

    assert [node for call_type, node in calls if call_type == "warmup"] == ["f2", "f8"]
    assert submitted_lifecycle["actual_call_submitted"] is True
    assert submitted_lifecycle["consumed_budget"] == 2
    assert submitted_lifecycle["reserved_budget"] == 0
    assert failed_lifecycle["consumed_budget"] == 2
    assert failed_lifecycle["reserved_budget"] == 0
    assert failure["action"] == "replacement_warmup"
    assert failure["error_type"] == "TimeoutError"
    assert result["status"] == "ok"
    assert result["scheduler_status"] == "degraded"
    assert result["warmup_error_count"] == 1
    assert result["consumed_budget"] == 2
    assert result["reserved_budget"] == 0
    assert [
        event["logical_name"]
        for event in events
        if event.get("event_type") == "invocation"
    ] == ["f1", "f4", "f8"]
    assert next(
        event
        for event in events
        if event.get("event_type") == "invocation" and event.get("logical_name") == "f8"
    )["cold_start"] is True


def test_failed_initial_warmup_is_consumed_and_business_still_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        controller_mod,
        "invoke_lambda",
        _capturing_invoke(calls, fail_warmup="f2"),
    )
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f2"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)

    assert [node for call_type, node in calls if call_type == "warmup"] == ["f2", "f6"]
    assert [
        event["logical_name"]
        for event in events
        if event.get("event_type") == "invocation"
    ] == ["f1", "f2", "f6"]
    assert result["status"] == "ok"
    assert result["scheduler_status"] == "degraded"
    assert result["warmup_error_count"] == 1
    assert result["consumed_budget"] == 2
    assert result["reserved_budget"] == 0
    assert any(
        event.get("event_type") == "warmup"
        and event.get("logical_name") == "f2"
        and event.get("status") == "error"
        for event in events
    )


def test_scheduler_worker_submission_failure_is_not_a_warmup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RejectingExecutor:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        def submit(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise RuntimeError("synthetic executor rejection")

        def shutdown(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    monkeypatch.setattr(controller_mod, "ThreadPoolExecutor", RejectingExecutor)
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f4"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)

    assert result["status"] == "ok"
    assert result["scheduler_status"] == "degraded"
    assert result["scheduler_error_count"] == 1
    assert result["warmup_error_count"] == 0
    assert sum(
        event.get("event_type") == "warmup" and event.get("status") == "error"
        for event in events
    ) == 0
    assert any(
        event.get("event_type") == "scheduler_decision"
        and event.get("action") == "scheduler_error"
        for event in events
    )


def test_initial_warmup_finishing_after_planned_arrival_is_not_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        controller_mod,
        "invoke_lambda",
        _capturing_invoke(calls, delay_warmup="f2", delay_sec=0.04),
    )
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f2"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )
    planned_arrival = controller_mod.monotonic_sec() + 0.01

    result = controller.run_once(0, planned_arrival_sec=planned_arrival)
    events = _events(output_dir)
    initial = next(
        event
        for event in events
        if event.get("event_type") == "warmup" and event.get("logical_name") == "f2"
    )

    assert initial["target_hit"] is True
    assert initial["readiness_deadline_type"] == "planned_arrival"
    assert initial["ready_before_deadline"] is False
    assert initial["ready_before_arrival"] is False
    assert initial["ready_before_node_demand"] is None
    assert initial["ready_before_demand"] is False
    assert initial["useful"] is False
    assert initial["missed_at_arrival"] is True
    assert initial["missed_at_node_demand"] is False
    assert initial["missed_at_demand"] is True
    assert result["warmup_overrun_ms"] > 0
    assert result["latency_ms"] >= result["warmup_overrun_ms"]


def test_setup_failure_releases_pending_and_discards_per_run_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f4"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    original = controller._reserve_predicted_pending_intent

    def reserve_then_fail(run_id, intents, workflow_index):
        original(run_id, intents, workflow_index)
        raise RuntimeError("synthetic setup failure")

    monkeypatch.setattr(controller, "_reserve_predicted_pending_intent", reserve_then_fail)

    with pytest.raises(RuntimeError, match="synthetic setup failure"):
        controller.run_once(0)

    assert controller._intent_ledgers_by_run == {}
    assert controller._pending_intents_by_run == {}
    assert controller._scheduled_tasks_by_run == {}
    assert controller._warmup_failures_by_run == {}


def test_slow_irrelevant_no_cancel_warmup_does_not_extend_business_latency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_no_cancel"

    def run_case(label: str, delay_sec: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        output_dir = tmp_path / label
        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            controller_mod,
            "invoke_lambda",
            _capturing_invoke(calls, delay_warmup="f6", delay_sec=delay_sec),
        )
        controller = MintController(
            _adaptive_config(output_dir, baseline, "f4"),
            dag=get_workload("adaptive_branch"),
            baseline=baseline,
            dry_run=True,
        )
        result = controller.run_once(0)
        assert [node for call_type, node in calls if call_type == "warmup"] == ["f2", "f6"]
        return result, _events(output_dir)

    fast, _ = run_case("fast", 0.0)
    slow_delay_sec = 0.4
    slow, slow_events = run_case("slow", slow_delay_sec)

    # The irrelevant stale f6 call is still awaited for audit/cleanup before
    # run_once returns, but its tail must not be charged to workflow latency.
    assert slow["latency_ms"] - fast["latency_ms"] < slow_delay_sec * 1000.0 * 0.5
    assert slow["consumed_budget"] == 2
    assert any(
        event.get("event_type") == "warmup"
        and event.get("logical_name") == "f6"
        and event.get("useful") is False
        for event in slow_events
    )


def test_slow_useful_replacement_does_not_block_business_at_demand(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        controller_mod,
        "invoke_lambda",
        _capturing_invoke(calls, delay_warmup="f8", delay_sec=0.3),
    )
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f4"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)
    replacement = next(
        event
        for event in events
        if event.get("event_type") == "warmup" and event.get("logical_name") == "f8"
    )
    business_f8 = next(
        event
        for event in events
        if event.get("event_type") == "invocation" and event.get("logical_name") == "f8"
    )

    # Business latency must stay far below the 300ms replacement warmup tail;
    # the threshold is deliberately kept loose enough to avoid CI timing noise.
    assert result["latency_ms"] < 250
    assert replacement["target_hit"] is True
    assert replacement["readiness_deadline_type"] == "node_demand"
    assert replacement["ready_before_deadline"] is False
    assert replacement["ready_before_arrival"] is None
    assert replacement["ready_before_node_demand"] is False
    assert replacement["ready_before_demand"] is False
    assert replacement["useful"] is False
    assert replacement["missed_at_node_demand"] is True
    assert replacement["missed_at_demand"] is True
    assert replacement["blocking_wait_ms"] == 0.0
    assert replacement["warmup_wall_ms"] >= 250
    assert business_f8["cold_start"] is True
    assert any(
        event.get("event_type") == "scheduler_decision"
        and event.get("action") == "in_flight_at_demand"
        for event in events
    )


def test_replacement_completed_during_branch_work_is_ready_before_demand(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    replacement_done = Event()

    def invoke(
        function_name: str,
        payload: dict[str, Any],
        invocation_type: str = "RequestResponse",
        dry_run: bool = True,
        region_name: str | None = None,
    ) -> dict[str, Any]:
        del invocation_type, dry_run, region_name
        logical_name = str(payload["function_name"])
        call_type = str(payload["invocation_type"])
        if call_type == "warmup" and logical_name == "f8":
            replacement_done.set()
        if call_type == "real" and logical_name == "f4":
            assert replacement_done.wait(timeout=2)
            # Yield once so the worker can publish its terminal result before
            # the controller reaches the f8 demand boundary.
            time.sleep(0.01)
        return {
            "dry_run": True,
            "function_name": function_name,
            "payload": payload,
            "status_code": 200,
        }

    monkeypatch.setattr(controller_mod, "invoke_lambda", invoke)
    controller = MintController(
        _adaptive_config(output_dir, baseline, "f4"),
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    controller.run_once(0)
    events = _events(output_dir)
    replacement = next(
        event
        for event in events
        if event.get("event_type") == "warmup" and event.get("logical_name") == "f8"
    )

    assert replacement["target_hit"] is True
    assert replacement["readiness_deadline_type"] == "node_demand"
    assert replacement["ready_before_deadline"] is True
    assert replacement["ready_before_arrival"] is None
    assert replacement["ready_before_node_demand"] is True
    assert replacement["ready_before_demand"] is True
    assert replacement["useful"] is True
    assert replacement["missed_at_node_demand"] is False
    assert replacement["missed_at_demand"] is False


def test_business_failure_cancels_unsubmitted_pending_before_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    calls: list[tuple[str, str]] = []

    def fail_f1(
        function_name: str,
        payload: dict[str, Any],
        invocation_type: str = "RequestResponse",
        dry_run: bool = True,
        region_name: str | None = None,
    ) -> dict[str, Any]:
        del invocation_type, dry_run, region_name
        logical_name = str(payload["function_name"])
        call_type = str(payload["invocation_type"])
        calls.append((call_type, logical_name))
        if call_type == "real" and logical_name == "f1":
            raise RuntimeError("synthetic f1 business failure")
        return {
            "dry_run": True,
            "function_name": function_name,
            "payload": payload,
            "status_code": 200,
        }

    monkeypatch.setattr(controller_mod, "invoke_lambda", fail_f1)
    config = _adaptive_config(output_dir, baseline, "f4")
    config["experiment"]["adaptive_pending_delay_sec"] = 0.2
    controller = MintController(
        config,
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    with pytest.raises(RuntimeError, match="synthetic f1 business failure"):
        controller.run_once(0)

    events = _events(output_dir)
    assert [node for call_type, node in calls if call_type == "warmup"] == ["f2"]
    assert any(
        event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f6"
        and event.get("state_before") == "pending"
        and event.get("state_after") == "cancelled"
        and event.get("reason") == "workflow_failed_before_submission"
        for event in events
    )
    assert any(
        event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f6"
        and event.get("action") == "submit_rejected"
        and event.get("reason") == "invalid_state:cancelled"
        for event in events
    )


def test_timer_submission_wins_cancel_race_without_replacement_or_budget_refund(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    f6_in_flight = Event()
    release_f6 = Event()
    calls: list[tuple[str, str]] = []

    def race_invoke(
        function_name: str,
        payload: dict[str, Any],
        invocation_type: str = "RequestResponse",
        dry_run: bool = True,
        region_name: str | None = None,
    ) -> dict[str, Any]:
        del invocation_type, dry_run, region_name
        logical_name = str(payload["function_name"])
        call_type = str(payload["invocation_type"])
        calls.append((call_type, logical_name))
        if call_type == "warmup" and logical_name == "f6":
            f6_in_flight.set()
            assert release_f6.wait(timeout=2)
        elif call_type == "real" and logical_name == "f1":
            assert f6_in_flight.wait(timeout=2)
        elif call_type == "real" and logical_name == "f4":
            release_f6.set()
        return {
            "dry_run": True,
            "function_name": function_name,
            "payload": payload,
            "status_code": 200,
        }

    monkeypatch.setattr(controller_mod, "invoke_lambda", race_invoke)
    config = _adaptive_config(output_dir, baseline, "f4")
    config["experiment"]["adaptive_pending_delay_sec"] = 0.0
    controller = MintController(
        config,
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )

    result = controller.run_once(0)
    events = _events(output_dir)

    assert [node for call_type, node in calls if call_type == "warmup"] == ["f2", "f6"]
    assert result["consumed_budget"] == 2
    assert result["reserved_budget"] == 0
    assert not any(
        event.get("event_type") == "warmup" and event.get("logical_name") == "f8"
        for event in events
    )
    rejected = next(
        event
        for event in events
        if event.get("event_type") == "intent_lifecycle"
        and event.get("logical_name") == "f6"
        and event.get("action") == "cancel_pending_rejected"
    )
    assert rejected["accepted"] is False
    assert rejected["state_before"] == "in_flight"
    assert rejected["consumed_budget"] == 2
    assert any(
        event.get("event_type") == "scheduler_decision"
        and event.get("action") == "cancel_race_lost"
        for event in events
    )


def test_timer_can_win_after_pending_precheck_without_atomic_replace_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = "mint_markov_full"
    output_dir = tmp_path / baseline
    f6_submitted = Event()
    calls: list[tuple[str, str]] = []

    def invoke(
        function_name: str,
        payload: dict[str, Any],
        invocation_type: str = "RequestResponse",
        dry_run: bool = True,
        region_name: str | None = None,
    ) -> dict[str, Any]:
        del invocation_type, dry_run, region_name
        logical_name = str(payload["function_name"])
        call_type = str(payload["invocation_type"])
        calls.append((call_type, logical_name))
        if call_type == "warmup" and logical_name == "f6":
            f6_submitted.set()
        return {
            "dry_run": True,
            "function_name": function_name,
            "payload": payload,
            "status_code": 200,
        }

    monkeypatch.setattr(controller_mod, "invoke_lambda", invoke)
    config = _adaptive_config(output_dir, baseline, "f4")
    config["experiment"]["adaptive_pending_delay_sec"] = 0.01
    controller = MintController(
        config,
        dag=get_workload("adaptive_branch"),
        baseline=baseline,
        dry_run=True,
    )
    original_candidate = controller._best_runtime_candidate

    def delayed_candidate(*args, **kwargs):
        branch = args[1] if len(args) > 1 else kwargs.get("branch")
        if branch == "f4":
            assert f6_submitted.wait(timeout=2)
        return original_candidate(*args, **kwargs)

    monkeypatch.setattr(controller, "_best_runtime_candidate", delayed_candidate)

    result = controller.run_once(0)
    events = _events(output_dir)

    assert result["consumed_budget"] == 2
    assert result["reserved_budget"] == 0
    assert [node for call_type, node in calls if call_type == "warmup"] == ["f2", "f6"]
    assert any(
        event.get("event_type") == "intent_lifecycle"
        and event.get("action") == "atomic_replace_rejected"
        and str(event.get("reason", "")).startswith("old_intent_not_pending:")
        and event.get("state_before") in {"in_flight", "succeeded"}
        for event in events
    )
    assert any(
        event.get("event_type") == "scheduler_decision"
        and event.get("action") == "cancel_race_lost"
        for event in events
    )
