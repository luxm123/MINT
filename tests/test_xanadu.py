from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mint.controller import MintController
from mint.workloads import get_workload
from mint.xanadu import most_likely_path, select_jit_targets


def _config(output_dir: Path, baseline: str, dag_name: str, budget: int) -> dict[str, Any]:
    dag = get_workload(dag_name)
    return {
        "aws": {"lambda_functions": {node: f"mint-{node}" for node in dag.nodes}},
        "experiment": {
            "baseline": baseline,
            "warmup_budget": budget,
            "output_dir": str(output_dir),
            "branch_seed": 42,
            "profile_mismatch": True,
            "timing_jitter_ms": 800.0,
        },
        "platform": {
            "default_retention_sec": 300,
            "default_cold_start_ms": 800,
            "default_warm_duration_ms": 100,
        },
        "planner": {"type": "heuristic"},
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


def test_most_likely_path_wide_branch_uniform_breaks_ties_deterministically():
    dag = get_workload("wide_branch")
    probabilities = {
        "f1": 1.0,
        "f2": 0.25,
        "f3": 0.25,
        "f4": 0.25,
        "f5": 0.25,
        "f6": 1.0,
        "f7": 1.0,
    }
    assert most_likely_path(dag, probabilities) == ["f1", "f2", "f6", "f7"]


def test_most_likely_path_deep_mixed_uses_probabilities():
    dag = get_workload("deep_mixed")
    probabilities = {
        "f1": 1.0,
        "f2": 0.9,
        "f3": 0.1,
        "f4": 0.9,
        "f5": 0.1,
        "f6": 1.0,
        "f7": 1.0,
        "f8": 1.0,
    }
    assert most_likely_path(dag, probabilities) == [
        "f1",
        "f2",
        "f4",
        "f6",
        "f7",
        "f8",
    ]


def test_select_jit_targets_orders_by_stage_and_excludes_entry():
    dag = get_workload("deep_mixed")
    path = ["f1", "f2", "f4", "f6", "f7", "f8"]
    now = 100.0
    targets = select_jit_targets(dag, path, {}, now, 3)
    assert targets == ["f2", "f4", "f6"]
    # Already-hot members are skipped without consuming budget.
    hot = {"f4": now + 10.0}
    targets = select_jit_targets(dag, path, hot, now, 3)
    assert targets == ["f2", "f6", "f7"]


def test_xanadu_call_probability_overlays_learned_branches(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "unused_xanadu"
    config = _config(output_dir, "xanadu_full", "wide_branch", 2)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="xanadu_full",
        dry_run=True,
    )
    controller._active_model_snapshot = {
        "probabilities": {"f3": 0.7, "f2": 0.1, "f4": 0.1, "f5": 0.1}
    }
    probabilities = controller._xanadu_call_probability()
    assert probabilities["f3"] == 0.7
    assert probabilities["f2"] == 0.1


def test_xanadu_full_warms_earliest_mle_stages_within_budget(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "xanadu_full"
    config = _config(output_dir, "xanadu_full", "wide_branch", 3)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="xanadu_full",
        dry_run=True,
    )

    summary = controller.run(2)
    events = _events(output_dir)
    warmups = [event for event in events if event.get("event_type") == "warmup"]
    assert controller.planner_type == "xanadu_full"
    assert summary["aborted_run_count"] == 0
    assert summary["consumed_budget_total"] <= 6
    assert summary["total_warmup"] == len(warmups)
    assert warmups
    assert all(
        event["action_reason"].startswith("xanadu_full_")
        for event in warmups
    )
    # Before any branch observations the MLP tie-breaks to f1 -> f2 -> f6 -> f7;
    # entry f1 is never warmed, so the first-run JIT targets are f2, f6, f7.
    first_run_warmups = [
        event for event in warmups if event.get("workflow_index") == 0
    ]
    assert {event["logical_name"] for event in first_run_warmups} <= {
        "f2",
        "f6",
        "f7",
    }
    assert all(event["logical_name"] != "f1" for event in warmups)


def test_xanadu_full_respects_learned_mlp(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "xanadu_learned"
    config = _config(output_dir, "xanadu_full", "wide_branch", 1)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["planner"] = {
        "type": "heuristic",
        "historical_branch_records": ["f3"] * 8 + ["f2", "f4", "f5"],
    }
    controller = MintController(
        config,
        dag=dag,
        baseline="xanadu_full",
        dry_run=True,
    )
    controller.run(1)
    events = _events(output_dir)
    warmups = [
        event
        for event in events
        if event.get("event_type") == "warmup"
    ]
    # Learned MLP favors the f3 branch, so with B=1 the earliest path member
    # f3 is the JIT target.
    assert warmups
    assert all(event["logical_name"] == "f3" for event in warmups)
