import mint.controller as controller_mod
from mint.controller import InvalidRealObservation, MintController
from mint.workloads import get_workload
import json
import pytest


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


def test_oracle_path_can_use_selected_nodes_for_decision(tmp_path):
    dag = get_workload("wide_branch")
    config = _config(tmp_path, "oracle_path")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    controller = MintController(config, dag=dag, baseline="oracle_path", dry_run=True)
    intents = controller._planner_config()
    planned = controller_mod.plan_intents(dag, intents)

    actions = controller._oracle_path_actions(planned, ["f1", "f4", "f6", "f7"])
    executed = [action.intent.logical_name for action in actions if action.action_type == "execute"]
    cancelled = [action.intent.logical_name for action in actions if action.action_type == "cancel"]
    assert set(executed) <= {"f1", "f4", "f6", "f7"}
    assert any(node not in {"f1", "f4", "f6", "f7"} for node in cancelled)


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


def test_mint_markov_offline_executes_only_offline_top_budget_without_runtime_rescheduling(tmp_path, monkeypatch):
    def fail_schedule(*args, **kwargs):
        raise AssertionError("offline ablation must not invoke runtime scheduler")

    monkeypatch.setattr(controller_mod, "schedule_intents", fail_schedule)
    dag = get_workload("greedy_trap")
    config = _config(tmp_path, "mint_markov_offline")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["experiment"].update({"warmup_budget": 2, "branch_seed": 42, "profile_mismatch": True})
    config["planner"] = {"type": "markov", "horizon": 6}
    controller = MintController(config, dag=dag, baseline="mint_markov_offline", dry_run=True)
    controller.run(1)

    events = _events(tmp_path / "mint_markov_offline" / "events.jsonl")
    executed = [event["logical_name"] for event in events if event.get("event_type") == "warmup"]
    decisions = [event for event in events if event.get("event_type") == "scheduler_decision"]
    planned = controller_mod.plan_intents(dag, controller._planner_config())
    expected = [intent.logical_name for intent in sorted(planned, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))[:2]]
    assert executed == expected
    assert all(event["action_reason"].startswith("mint_markov_offline_") for event in decisions)
    assert not any(event["action"] in {"delay", "cancel"} for event in decisions)


def test_mint_markov_no_runtime_reval_uses_offline_rank_budget_and_hot_guard(tmp_path, monkeypatch):
    def fail_schedule(*args, **kwargs):
        raise AssertionError("no-runtime ablation must not invoke runtime scheduler")

    monkeypatch.setattr(controller_mod, "schedule_intents", fail_schedule)
    dag = get_workload("greedy_trap")
    config = _config(tmp_path, "mint_markov_no_runtime_reval")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["experiment"].update({"warmup_budget": 2, "branch_seed": 42, "profile_mismatch": True})
    config["planner"] = {"type": "markov", "horizon": 6}
    controller = MintController(config, dag=dag, baseline="mint_markov_no_runtime_reval", dry_run=True)
    controller.run(2)

    events = _events(tmp_path / "mint_markov_no_runtime_reval" / "events.jsonl")
    run_ids = [event["run_id"] for event in events if event.get("event_type") == "workflow_summary"]
    assert len(run_ids) == 2
    for run_id in run_ids:
        decisions = [event for event in events if event.get("event_type") == "scheduler_decision" and event["run_id"] == run_id]
        executes = [event for event in decisions if event["action"] == "execute"]
        assert len(executes) <= 2
        assert not any(event["action"] == "replace" for event in decisions)
    second_run_decisions = [event for event in events if event.get("event_type") == "scheduler_decision" and event["run_id"] == run_ids[1]]
    assert any(event["action"] == "cancel" and event["action_reason"] == "mint_markov_no_runtime_reval_already_hot" for event in second_run_decisions)


def test_mint_markov_no_long_horizon_uses_runtime_scheduler_without_structural_benefit(tmp_path, monkeypatch):
    captured = {}

    def fail_structural_benefit(self, intents):
        raise AssertionError("no-long-horizon ablation must not compute structural long-horizon benefit")

    def fake_schedule(intents, runtime_state, budget, config):
        captured["runtime_state"] = runtime_state
        captured["budget"] = budget
        return [controller_mod.WarmupAction("execute", intents[0], 1.0, "test_runtime_execute")]

    monkeypatch.setattr(MintController, "_runtime_path_benefit", fail_structural_benefit)
    monkeypatch.setattr(controller_mod, "schedule_intents", fake_schedule)
    dag = get_workload("greedy_trap")
    config = _config(tmp_path, "mint_markov_no_long_horizon")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["experiment"].update({"warmup_budget": 2})
    config["planner"] = {"type": "markov", "horizon": 6}
    MintController(config, dag=dag, baseline="mint_markov_no_long_horizon", dry_run=True).run(1)

    runtime_state = captured["runtime_state"]
    assert captured["budget"] == 2
    assert runtime_state["frontier"] == ["f1"]
    assert runtime_state["hot_until"] == {}
    assert runtime_state["call_probability"]["f2"] == 1.0 / 3.0
    assert set(runtime_state["path_benefit"]) <= set(dag.nodes)


def test_mint_markov_full_and_no_long_horizon_share_scheduler_inputs_except_path_benefit(tmp_path, monkeypatch):
    captured = {}

    def fake_schedule(intents, runtime_state, budget, config):
        baseline = config["experiment"]["baseline"]
        captured[baseline] = {
            "intent_names": [intent.logical_name for intent in intents],
            "runtime_state": runtime_state,
            "budget": budget,
        }
        return []

    monkeypatch.setattr(controller_mod, "schedule_intents", fake_schedule)
    dag = get_workload("deep_mixed")
    common_exp = {"warmup_budget": 2, "branch_seed": 42, "profile_mismatch": True, "timing_jitter_ms": 800}
    for baseline in ("mint_markov_full", "mint_markov_no_long_horizon"):
        config = _config(tmp_path, baseline)
        config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
        config["experiment"].update(common_exp)
        config["planner"] = {"type": "markov", "horizon": 6}
        MintController(config, dag=dag, baseline=baseline, dry_run=True).run(1)

    full = captured["mint_markov_full"]
    no_long = captured["mint_markov_no_long_horizon"]
    assert full["intent_names"] == no_long["intent_names"]
    assert full["budget"] == no_long["budget"] == 2
    for key in ("now_sec", "call_probability", "frontier", "hot_until"):
        assert full["runtime_state"][key] == no_long["runtime_state"][key]
    assert full["runtime_state"]["path_benefit"] != no_long["runtime_state"]["path_benefit"]


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


def test_mint_no_long_horizon_and_full_rank_different_targets_on_greedy_trap(tmp_path):
    dag = get_workload("greedy_trap")
    common_exp = {"warmup_budget": 2, "branch_seed": 42, "profile_mismatch": True, "timing_jitter_ms": 800}

    no_long_config = _config(tmp_path, "mint_markov_no_long_horizon")
    no_long_config["experiment"].update(common_exp)
    no_long_config["planner"] = {"type": "markov", "horizon": 6}
    MintController(no_long_config, dag=dag, baseline="mint_markov_no_long_horizon", dry_run=True).run(1)

    full_config = _config(tmp_path, "mint_markov_full")
    full_config["experiment"].update(common_exp)
    full_config["planner"] = {"type": "markov", "horizon": 6}
    MintController(full_config, dag=dag, baseline="mint_markov_full", dry_run=True).run(1)

    no_long_events = _events(tmp_path / "mint_markov_no_long_horizon" / "events.jsonl")
    full_events = _events(tmp_path / "mint_markov_full" / "events.jsonl")
    no_long_warmups = [event["logical_name"] for event in no_long_events if event.get("event_type") == "warmup"]
    full_warmups = [event["logical_name"] for event in full_events if event.get("event_type") == "warmup"]
    assert no_long_warmups != full_warmups
    assert "f1" in no_long_warmups
    assert any(node in {"f5", "f6", "f7", "f8"} for node in full_warmups)


def test_mint_variants_have_explainable_action_differences_without_path_leak(tmp_path):
    dag = get_workload("greedy_trap")
    common_exp = {"warmup_budget": 2, "branch_seed": 42, "profile_mismatch": True, "timing_jitter_ms": 800}
    variants = ("mint_markov_no_runtime_reval", "mint_markov_no_long_horizon", "mint_markov_full")
    warmups = {}
    actions = {}
    for baseline in variants:
        config = _config(tmp_path, baseline)
        config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
        config["experiment"].update(common_exp)
        config["planner"] = {"type": "markov", "horizon": 6}
        MintController(config, dag=dag, baseline=baseline, dry_run=True).run(1)
        events = _events(tmp_path / baseline / "events.jsonl")
        warmups[baseline] = [event["logical_name"] for event in events if event.get("event_type") == "warmup"]
        actions[baseline] = [event["action"] for event in events if event.get("event_type") == "scheduler_decision"]

    assert warmups["mint_markov_no_runtime_reval"] != warmups["mint_markov_full"]
    assert warmups["mint_markov_no_long_horizon"] != warmups["mint_markov_full"]
    assert "replace" not in actions["mint_markov_no_runtime_reval"]
    assert "replace" in actions["mint_markov_full"]


def test_real_aws_run_records_lambda_observed_metrics(tmp_path, monkeypatch):
    observed = {
        "f1": {"cold_start": False, "client_elapsed_ms": 11.5, "duration_ms": 3.0, "request_id": "req-1", "execution_environment_id": "env-a"},
        "f2": {"cold_start": True, "client_elapsed_ms": 22.5, "duration_ms": 4.0, "request_id": "req-2", "execution_environment_id": "env-b"},
        "f3": {"cold_start": False, "client_elapsed_ms": 33.5, "duration_ms": 5.0, "request_id": "req-3", "execution_environment_id": "env-a"},
    }

    def fake_invoke_lambda(function_name, payload, invocation_type="RequestResponse", dry_run=True, region_name=None):
        logical = payload["function_name"]
        metrics = observed[logical]
        return {
            "dry_run": False,
            "function_name": function_name,
            "status_code": 200,
            "client_elapsed_ms": metrics["client_elapsed_ms"],
            "payload": {
                "cold_start": metrics["cold_start"],
                "duration_ms": metrics["duration_ms"],
                "request_id": metrics["request_id"],
                "execution_environment_id": metrics["execution_environment_id"],
                "invocation_type": payload["invocation_type"],
                "status": "ok",
            },
        }

    monkeypatch.setattr(controller_mod, "invoke_lambda", fake_invoke_lambda)
    config = _config(tmp_path, "no_warmup")
    config["experiment"]["timing_jitter_ms"] = 800
    config["platform"]["default_cold_start_ms"] = 900
    controller = MintController(config, dag=get_workload("chain"), baseline="no_warmup", dry_run=False)
    summary = controller.run(1)

    events = _events(tmp_path / "no_warmup" / "events.jsonl")
    invocations = [event for event in events if event.get("event_type") == "invocation"]
    assert [event["logical_name"] for event in invocations] == ["f1", "f2", "f3"]
    assert [event["cold_start"] for event in invocations] == [False, True, False]
    assert [event["latency_ms"] for event in invocations] == [11.5, 22.5, 33.5]
    assert [event["request_id"] for event in invocations] == ["req-1", "req-2", "req-3"]
    assert [event["execution_environment_id"] for event in invocations] == ["env-a", "env-b", "env-a"]
    assert [event["function_duration_ms"] for event in invocations] == [3.0, 4.0, 5.0]
    assert summary["cold_start_count"] == 1
    assert summary["invocation_latency_ms_avg"] == 22.5


def test_real_aws_run_forbids_simulated_metric_fallback(tmp_path, monkeypatch):
    def fake_invoke_lambda(function_name, payload, invocation_type="RequestResponse", dry_run=True, region_name=None):
        return {
            "dry_run": False,
            "function_name": function_name,
            "status_code": 200,
            "payload": {
                "cold_start": True,
                "duration_ms": 4.0,
                "request_id": "req-1",
                "execution_environment_id": "env-a",
                "invocation_type": payload["invocation_type"],
                "status": "ok",
            },
        }

    monkeypatch.setattr(controller_mod, "invoke_lambda", fake_invoke_lambda)
    monkeypatch.setattr(
        MintController,
        "_simulated_latency_ms",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("real mode must never simulate")),
    )
    controller = MintController(_config(tmp_path, "no_warmup"), dag=get_workload("chain"), baseline="no_warmup", dry_run=False)

    with pytest.raises(InvalidRealObservation, match="client_elapsed_ms"):
        controller.run(1)


def test_controller_uses_selected_function_pool_for_real_and_warmup_calls(tmp_path, monkeypatch):
    called = []

    def fake_invoke_lambda(function_name, payload, invocation_type="RequestResponse", dry_run=True, region_name=None):
        called.append((function_name, payload["invocation_type"]))
        return {
            "dry_run": False,
            "function_name": function_name,
            "status_code": 200,
            "client_elapsed_ms": 5.0,
            "payload": {
                "cold_start": False,
                "duration_ms": 1.0,
                "request_id": f"req-{len(called)}",
                "execution_environment_id": f"env-{function_name}",
                "invocation_type": payload["invocation_type"],
                "status": "ok",
            },
        }

    monkeypatch.setattr(controller_mod, "invoke_lambda", fake_invoke_lambda)
    config = _config(tmp_path, "mint_markov_full")
    config["experiment"].update({"function_pool": "pool_b", "warmup_lead_sec": 0})
    config["aws"]["lambda_function_pools"] = {
        "pool_b": {f"f{i}": f"pool-b-f{i}" for i in range(1, 9)}
    }
    controller = MintController(config, dag=get_workload("wide_branch"), baseline="mint_markov_full", dry_run=False)
    controller.run_once(0, planned_arrival_sec=controller_mod.monotonic_sec())

    assert called
    assert all(name.startswith("pool-b-") for name, _ in called)
    assert any(kind == "warmup" for _, kind in called)
    assert any(kind == "real" for _, kind in called)


def test_warmup_overrun_is_counted_in_end_to_end_latency(tmp_path, monkeypatch):
    def slow_dry_invoke(function_name, payload, invocation_type="RequestResponse", dry_run=True, region_name=None):
        import time

        time.sleep(0.02)
        return {"dry_run": True, "function_name": function_name, "payload": payload, "status_code": 200}

    monkeypatch.setattr(controller_mod, "invoke_lambda", slow_dry_invoke)
    config = _config(tmp_path, "mint_markov_full")
    config["experiment"]["warmup_lead_sec"] = 0
    controller = MintController(config, dag=get_workload("chain"), baseline="mint_markov_full", dry_run=True)
    planned = controller_mod.monotonic_sec()
    result = controller.run_once(
        0,
        block_id="block-0000",
        strategy_order="mint_markov_full,no_warmup",
        planned_arrival_sec=planned,
        planned_arrival_time="2026-01-01T00:00:00+00:00",
    )

    assert result["arrival_lateness_ms"] >= 35
    assert 0 < result["warmup_overrun_ms"] <= result["arrival_lateness_ms"]
    assert result["latency_ms"] >= result["arrival_lateness_ms"]
    assert result["block_id"] == "block-0000"


def test_no_executed_warmup_never_reports_warmup_overrun(tmp_path, monkeypatch):
    monkeypatch.setattr(controller_mod, "schedule_intents", lambda *args, **kwargs: [])
    config = _config(tmp_path, "mint_markov_full")
    config["experiment"]["warmup_lead_sec"] = 0
    controller = MintController(config, dag=get_workload("chain"), baseline="mint_markov_full", dry_run=True)

    result = controller.run_once(0, planned_arrival_sec=controller_mod.monotonic_sec())

    assert result["warmup_count"] == 0
    assert result["warmup_overrun_ms"] == 0.0


def test_adaptive_markov_learns_from_past_then_replaces_after_f1(tmp_path):
    dag = get_workload("adaptive_branch")
    config = _config(tmp_path, "mint_markov_full")
    config["aws"]["lambda_functions"] = {node: f"mint-{node}" for node in dag.nodes}
    config["experiment"].update({"warmup_budget": 2, "branch_trace": ["f4"]})
    config["planner"] = {
        "type": "markov",
        "horizon": 5,
        "historical_branch_records": ["f2"] * 70 + ["f3"] * 10 + ["f4"] * 10 + ["f5"] * 10,
    }
    controller = MintController(config, dag=dag, baseline="mint_markov_full", dry_run=True)

    controller.run_once(0)
    events = _events(tmp_path / "mint_markov_full" / "events.jsonl")
    model_events = [event for event in events if event.get("event_type") == "branch_model"]
    decisions = [event for event in events if event.get("event_type") == "scheduler_decision"]
    warmups = [event for event in events if event.get("event_type") == "warmup"]

    probabilities = json.loads(model_events[0]["branch_probabilities"])
    assert probabilities == {"f2": 0.7, "f3": 0.1, "f4": 0.1, "f5": 0.1}
    assert {event["logical_name"] for event in warmups if event["action"] == "execute"} == {"f2", "f6"}
    assert {event["logical_name"] for event in warmups if event["action"] == "replace"} == {"f4", "f8"}
    assert any(
        event["decision_phase"] == "runtime_after_f1"
        and event["logical_name"] == "f2"
        and event["action"] == "cancel"
        for event in decisions
    )
    assert any(
        event["decision_phase"] == "runtime_after_f1"
        and event["logical_name"] == "f8"
        and event["action"] == "replace"
        for event in decisions
    )
    invocation_path = [event["logical_name"] for event in events if event.get("event_type") == "invocation"]
    assert invocation_path == ["f1", "f4", "f8"]

    controller.run_once(1)
    events = _events(tmp_path / "mint_markov_full" / "events.jsonl")
    next_initial = [
        event for event in events
        if event.get("event_type") == "branch_model" and event.get("decision_phase") == "initial"
    ][1]
    assert next_initial["history_size"] == 101
    assert json.loads(next_initial["branch_counts"])["f4"] == 11
