import mint.controller as controller_mod
from mint.controller import MintController
from mint.workloads import get_workload
import json


def _config(tmp_path, baseline):
    return {
        "aws": {"lambda_functions": {f"f{i}": f"mint-f{i}" for i in range(1, 9)}},
        "experiment": {"baseline": baseline, "warmup_budget": 2, "output_dir": str(tmp_path / baseline)},
        "platform": {"default_retention_sec": 300, "default_cold_start_ms": 800, "default_warm_duration_ms": 100},
        "scheduler": {"enable_delay": True, "enable_cancel": True, "enable_replace": True, "gain_threshold": 0.0},
    }


def test_static_dag_and_mint_offline_obey_warmup_budget(tmp_path):
    for baseline in ("static_dag", "mint_offline"):
        controller = MintController(_config(tmp_path, baseline), dag=get_workload("chain"), baseline=baseline, dry_run=True)
        summary = controller.run(3)
        assert summary["total_warmup"] == 6
        assert summary["execute_count"] == 6
        assert summary["replace_count"] == 3


def test_unlimited_static_baseline_can_warm_all_nodes(tmp_path):
    controller = MintController(
        _config(tmp_path, "static_dag_unlimited"),
        dag=get_workload("chain"),
        baseline="static_dag_unlimited",
        dry_run=True,
    )
    summary = controller.run(3)
    assert summary["total_warmup"] == 9
    assert summary["replace_count"] == 0


def test_periodic_keepwarm_and_orion_like_obey_warmup_budget(tmp_path):
    for baseline in ("periodic_keepwarm", "orion_like"):
        controller = MintController(_config(tmp_path, baseline), dag=get_workload("chain"), baseline=baseline, dry_run=True)
        summary = controller.run(2)
        assert summary["total_warmup"] == 4
        assert summary["execute_count"] == 4
        assert summary["replace_count"] == 2


def _events(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_path_aware_greedy_does_not_use_full_selected_path(tmp_path):
    dag = get_workload("wide_branch")
    config = _config(tmp_path, "path_aware_greedy")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["experiment"].update({"warmup_budget": 4, "branch_seed": 1, "profile_mismatch": False})
    controller = MintController(config, dag=dag, baseline="path_aware_greedy", dry_run=True)
    summary = controller.run(1)
    assert summary["total_warmup"] <= 4
    assert summary["execute_count"] <= 4
    events = _events(tmp_path / "path_aware_greedy" / "events.jsonl")
    invoked = {event["logical_name"] for event in events if event.get("event_type") == "invocation"}
    warmed = {event["logical_name"] for event in events if event.get("event_type") == "warmup"}
    assert "f2" in warmed
    assert "f3" in invoked
    assert "f2" not in invoked
    assert any(node not in dag.entry_nodes for node in warmed)
    assert any(event.get("action") == "replace" for event in events if event.get("event_type") == "scheduler_decision")


def test_path_aware_greedy_uses_profile_future_candidates_and_budget(tmp_path):
    dag = get_workload("deep_mixed")
    config = _config(tmp_path, "path_aware_greedy")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["experiment"].update({"warmup_budget": 2, "branch_seed": 0})
    controller = MintController(config, dag=dag, baseline="path_aware_greedy", dry_run=True)
    summary = controller.run(1)

    events = _events(tmp_path / "path_aware_greedy" / "events.jsonl")
    warmed = [event["logical_name"] for event in events if event.get("event_type") == "warmup"]
    decisions = [event for event in events if event.get("event_type") == "scheduler_decision"]
    assert summary["total_warmup"] == 2
    assert len(warmed) == 2
    assert any(node not in dag.entry_nodes for node in warmed)
    assert sum(1 for event in decisions if event.get("action") == "replace") == len(dag.nodes) - 2
    assert not any(event.get("action") == "delay" for event in decisions)


def test_oracle_path_only_warms_real_path_and_budget(tmp_path):
    dag = get_workload("wide_branch")
    config = _config(tmp_path, "oracle_path")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["experiment"].update({"warmup_budget": 2, "branch_seed": 1, "profile_mismatch": False})
    controller = MintController(config, dag=dag, baseline="oracle_path", dry_run=True)
    summary = controller.run(1)
    assert summary["total_warmup"] <= 2
    assert summary["execute_count"] <= 2
    events = _events(tmp_path / "oracle_path" / "events.jsonl")
    invoked = {event["logical_name"] for event in events if event.get("event_type") == "invocation"}
    warmed = {event["logical_name"] for event in events if event.get("event_type") == "warmup"}
    assert warmed <= invoked
    assert warmed != {"f1"}
    assert controller.planner_type == "oracle"


def test_mint_markov_full_uses_profile_probabilities_not_full_selected_path(tmp_path, monkeypatch):
    captured = {}

    def fake_schedule(intents, runtime_state, budget, config):
        captured["call_probability"] = runtime_state["call_probability"]
        captured["frontier"] = runtime_state["frontier"]
        return []

    monkeypatch.setattr(controller_mod, "schedule_intents", fake_schedule)
    dag = get_workload("wide_branch")
    config = _config(tmp_path, "mint_markov_full")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["experiment"].update({"warmup_budget": 2, "branch_seed": 0, "profile_mismatch": False})
    config["planner"] = {"type": "markov", "horizon": 5}
    controller = MintController(config, dag=dag, baseline="mint_markov_full", dry_run=True)
    controller.run(1)

    assert captured["frontier"] == ["f1"]
    probabilities = captured["call_probability"]
    assert {probabilities[node] for node in ["f2", "f3", "f4", "f5"]} == {0.25}
    assert probabilities["f6"] == 1.0
    assert probabilities["f7"] == 1.0


def test_greedy_trap_workload_has_profile_branch_and_common_suffix():
    dag = get_workload("greedy_trap")
    assert dag.entry_nodes == ["f1"]
    assert {"f2", "f3", "f4"} <= set(dag.successors["f1"])
    assert dag.predecessors["f5"] == ["f2", "f3", "f4"]
    assert dag.successors["f7"] == ["f8"]


def test_greedy_trap_mint_and_path_aware_targets_differ_without_path_leak(tmp_path):
    dag = get_workload("greedy_trap")
    common_exp = {"warmup_budget": 2, "branch_seed": 42, "profile_mismatch": True, "timing_jitter_ms": 800}

    greedy_config = _config(tmp_path, "path_aware_greedy")
    greedy_config["experiment"].update(common_exp)
    MintController(greedy_config, dag=dag, baseline="path_aware_greedy", dry_run=True).run(1)

    mint_config = _config(tmp_path, "mint_markov_full")
    mint_config["experiment"].update(common_exp)
    mint_config["planner"] = {"type": "markov", "horizon": 6}
    MintController(mint_config, dag=dag, baseline="mint_markov_full", dry_run=True).run(1)

    greedy_events = _events(tmp_path / "path_aware_greedy" / "events.jsonl")
    mint_events = _events(tmp_path / "mint_markov_full" / "events.jsonl")
    greedy_warmups = [event["logical_name"] for event in greedy_events if event.get("event_type") == "warmup"]
    mint_warmups = [event["logical_name"] for event in mint_events if event.get("event_type") == "warmup"]
    assert greedy_warmups != mint_warmups
    assert any(node in {"f2", "f3", "f4"} for node in greedy_warmups)
    assert any(node in {"f5", "f6", "f7", "f8"} for node in mint_warmups)

    greedy_decisions = [event for event in greedy_events if event.get("event_type") == "scheduler_decision"]
    mint_decisions = [event for event in mint_events if event.get("event_type") == "scheduler_decision"]
    assert sum(1 for event in greedy_decisions if event.get("action") == "execute") <= 2
    assert any(event.get("action") in {"cancel", "replace"} for event in mint_decisions)
