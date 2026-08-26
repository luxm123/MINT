from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mint.controller import MintController
from mint.faascache import FaasCacheProfile, GdsfCache
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


def test_gdsf_value_uses_cost_frequency_size_and_aging():
    profile = FaasCacheProfile(base_cold_ms=800.0, size_mb={"f2": 2.0})
    cache = GdsfCache(2, profile)
    cache.seed_frequencies({"f2": 1.0, "f3": 2.0, "f6": 1.0})
    assert cache.value("f2") == 400.0
    assert cache.value("f3") == 1600.0
    assert cache.value("f6") == 800.0
    cache.aging = 100.0
    assert cache.value("f6") == 900.0


def test_insert_evicts_lowest_value_and_advances_aging():
    cache = GdsfCache(2)
    cache.seed_frequencies({"f2": 1.0, "f3": 0.5, "f6": 2.0})
    assert cache.insert("f2") is None
    assert cache.insert("f3") is None
    assert cache.contents == {"f2", "f3"}
    evicted = cache.insert("f6")
    assert evicted == "f3"
    assert cache.contents == {"f2", "f6"}
    # The aging factor L is the pre-eviction value of the evicted item.
    assert cache.aging == 400.0
    assert cache.value("f6") == 2000.0


def test_allocate_fills_free_slots_and_replaces_low_value_resident():
    cache = GdsfCache(2)
    cache.seed_frequencies({"f2": 1.0, "f3": 0.5, "f6": 2.0})
    targets = cache.allocate(["f6", "f2", "f3"], 2)
    assert targets == ["f6", "f2"]
    for node in targets:
        assert cache.insert(node) is None
    assert cache.contents == {"f6", "f2"}

    cache.seed_frequencies({"f7": 3.0})
    assert cache.allocate(["f7"], 1) == ["f7"]
    assert cache.insert("f7") == "f2"
    assert cache.contents == {"f7", "f6"}


def test_observe_populates_cache_and_evicts():
    cache = GdsfCache(1)
    cache.seed_frequencies({"f1": 1.0})
    assert cache.observe("f1") is None
    assert cache.contents == {"f1"}
    assert cache.frequency["f1"] == 2.0
    evicted = cache.observe("f6")
    assert evicted == "f1"
    assert cache.contents == {"f6"}
    assert cache.aging == 1600.0


def test_allocate_respects_zero_budget_and_capacity():
    cache = GdsfCache(0)
    assert cache.allocate(["f2"], 3) == []
    assert GdsfCache(2).allocate(["f2"], 0) == []


def test_faascache_controller_respects_budget_and_cache_capacity(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "faascache"
    config = _config(output_dir, "faascache", "wide_branch", 3)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="faascache",
        dry_run=True,
    )

    summary = controller.run(3)
    events = _events(output_dir)
    warmups = [event for event in events if event.get("event_type") == "warmup"]
    assert controller.planner_type == "faascache"
    assert summary["aborted_run_count"] == 0
    assert summary["consumed_budget_total"] <= 9
    assert summary["total_warmup"] == len(warmups)
    assert warmups
    assert all(
        event["action_reason"].startswith("faascache_")
        for event in warmups
    )
    assert all(event["logical_name"] in dag.nodes for event in warmups)
    assert len(controller._faascache.contents) <= 3


def test_faascache_never_exceeds_budget_per_run(tmp_path):
    dag = get_workload("deep_mixed")
    output_dir = tmp_path / "faascache_budget"
    config = _config(output_dir, "faascache", "deep_mixed", 2)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="faascache",
        dry_run=True,
    )

    controller.run(5)
    events = _events(output_dir)
    summaries = [
        event for event in events if event.get("event_type") == "workflow_summary"
    ]
    assert len(summaries) == 5
    assert all(event["consumed_budget"] <= 2 for event in summaries)


def test_faascache_frequency_learns_from_realized_invocations(tmp_path):
    dag = get_workload("wide_branch")
    output_dir = tmp_path / "faascache_learn"
    config = _config(output_dir, "faascache", "wide_branch", 3)
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(
        config,
        dag=dag,
        baseline="faascache",
        dry_run=True,
    )

    controller.run(2)
    events = _events(output_dir)
    invoked = {
        event["logical_name"]
        for event in events
        if event.get("event_type") == "invocation"
    }
    assert invoked
    for node in invoked:
        prior = 1.0 if node in {"f1", "f6", "f7"} else 0.25
        assert controller._faascache.frequency[node] >= prior + 1.0
