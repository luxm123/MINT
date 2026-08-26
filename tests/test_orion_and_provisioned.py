from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mint.controller import MintController
from mint.orion import (
    OrionProfile,
    build_bundles,
    cold_latency_ms,
    decide,
    memory_cost_units,
)
from mint.workloads import get_workload


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


def test_orion_bundles_cover_dag_paths():
    wide = get_workload("wide_branch")
    wide_bundles = build_bundles(wide)
    assert len(wide_bundles) == 4
    assert {bundle.representative for bundle in wide_bundles} == {"f2", "f3", "f4", "f5"}
    assert all(set(bundle.members) == {"f6", "f7", bundle.representative} for bundle in wide_bundles)

    deep = get_workload("deep_mixed")
    deep_bundles = build_bundles(deep)
    assert len(deep_bundles) == 2
    assert {frozenset(bundle.members) for bundle in deep_bundles} == {
        frozenset({"f2", "f4", "f6", "f7", "f8"}),
        frozenset({"f3", "f5", "f6", "f7", "f8"}),
    }


def test_orion_cold_latency_and_memory_cost():
    assert memory_cost_units(128) == 1.0
    assert memory_cost_units(1024) == 8.0
    assert cold_latency_ms(1024, 800.0) < cold_latency_ms(128, 800.0)
    assert cold_latency_ms(128, 800.0) == 800.0


def test_orion_decide_respects_budget_hot_nodes_and_cache():
    dag = get_workload("wide_branch")
    bundles = build_bundles(dag)
    profile = OrionProfile()
    call_probability = {
        "f1": 1.0,
        "f2": 0.25,
        "f3": 0.25,
        "f4": 0.25,
        "f5": 0.25,
        "f6": 1.0,
        "f7": 1.0,
    }
    downstream = dag.downstream_counts()
    max_downstream = max(downstream.values())
    weight = {node: 1.0 + downstream.get(node, 0) / max_downstream for node in dag.nodes}

    decisions = decide(
        profile,
        bundles,
        call_probability,
        weight,
        hot_nodes=set(),
        warm_bundle_ids=set(),
        budget=2,
    )
    assert len(decisions) == 2
    assert len({decision.bundle.bundle_id for decision in decisions}) == 2
    assert all(decision.memory_mb in profile.memory_options for decision in decisions)
    assert decisions[0].score >= decisions[1].score

    # Hot nodes and the warm container cache remove candidates.
    hot = {"f6", "f7", "f2"}
    cached = {decisions[0].bundle.bundle_id}
    fewer = decide(
        profile,
        bundles,
        call_probability,
        weight,
        hot_nodes=hot,
        warm_bundle_ids=cached,
        budget=3,
    )
    assert all(decision.bundle.bundle_id not in cached for decision in fewer)
    assert all(not all(member in hot for member in decision.bundle.members) for decision in fewer)


def test_orion_full_controller_warms_bundles_within_budget(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "orion_full"
    config = _config(output_dir, "orion_full", "wide_branch", 2)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="orion_full",
        dry_run=True,
    )

    summary = controller.run(2)
    events = _events(output_dir)
    warmups = [event for event in events if event.get("event_type") == "warmup"]
    assert controller.planner_type == "orion_full"
    assert summary["aborted_run_count"] == 0
    assert summary["consumed_budget_total"] <= 4
    assert len(warmups) == summary["total_warmup"]
    assert warmups
    assert all(
        event["action_reason"].startswith("orion_full_bundle:")
        for event in warmups
    )
    assert controller._orion_warm_bundle_ids


def test_orion_full_bundle_members_are_hot_on_realized_path(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "orion_hot"
    config = _config(output_dir, "orion_full", "wide_branch", 4)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="orion_full",
        dry_run=True,
    )

    controller.run(1)
    events = _events(output_dir)
    invocations = [
        event for event in events if event.get("event_type") == "invocation"
    ]
    warm_bundles = {
        event["action_reason"] for event in events if event.get("event_type") == "warmup"
    }
    assert warm_bundles
    # With B=4 every bundle is warmed, so every downstream invocation is warm.
    assert all(event["cold_start"] is False for event in invocations if event["logical_name"] != "f1")


def test_provisioned_concurrency_has_no_warmups_and_no_cold_starts(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "provisioned"
    config = _config(output_dir, "provisioned_concurrency", "wide_branch", 3)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="provisioned_concurrency",
        dry_run=True,
    )

    summary = controller.run(2)
    events = _events(output_dir)
    invocations = [
        event for event in events if event.get("event_type") == "invocation"
    ]
    assert controller.planner_type == "provisioned"
    assert summary["total_warmup"] == 0
    assert summary["cold_start_count"] == 2
    assert summary["provisioned_slots_total"] == 6
    assert summary["consumed_budget_total"] == 0
    assert invocations
    cold_nodes = {
        event["logical_name"] for event in invocations if event["cold_start"]
    }
    assert cold_nodes <= {"f2", "f3", "f4", "f5"}
    assert {"f1", "f6", "f7"} <= {
        event["logical_name"]
        for event in invocations
        if event["cold_start"] is False
    }
    assert {event["logical_name"] for event in invocations} <= set(dag.nodes)
    assert {"f1", "f3", "f6", "f7"} <= {
        event["logical_name"] for event in invocations
    }


def test_provisioning_plan_picks_top_value_functions(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "provision_plan"
    config = _config(output_dir, "provisioned_concurrency", "wide_branch", 2)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="provisioned_concurrency",
        dry_run=True,
    )
    assert controller._provisioned_nodes == {"f1", "f6"}
    assert controller._provisioned_slots == 2
