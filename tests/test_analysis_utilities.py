import json

import pandas as pd

from mint.controller import MintController
from mint.workloads import get_workload
from scripts.analyze_baseline_overlap import analyze_overlap
from scripts.prepare_pareto_data import prepare_pareto


def _write_events(path, targets):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for index, target in enumerate(targets):
        intent_id = f"i-{target}-{index}"
        rows.append(
            {
                "event_type": "scheduler_decision",
                "run_id": "r1",
                "intent_id": intent_id,
                "logical_name": target,
                "action": "execute",
                "planned_time_sec": index,
                "gain": 1.0,
            }
        )
        rows.append(
            {
                "event_type": "warmup",
                "run_id": "r1",
                "intent_id": intent_id,
                "logical_name": target,
                "function_name": f"mint-{target}",
                "action": "execute",
                "action_reason": "fixture",
                "gain": 1.0,
                "useful": True,
            }
        )
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")


def test_overlap_script_generates_targets_and_overlap(tmp_path):
    results = tmp_path / "results"
    configs = [
        ("chain", 1, "static_dag", ["f1", "f2"]),
        ("chain", 1, "orion_like", ["f1", "f2"]),
        ("chain", 1, "mint_markov_full", ["f2", "f3"]),
    ]
    rows = []
    for dag, budget, baseline, targets in configs:
        output_dir = results / f"{dag}_{baseline}_B{budget}"
        _write_events(output_dir / "events.jsonl", targets)
        rows.append({"dag": dag, "budget": budget, "baseline": baseline, "output_dir": str(output_dir)})
    pd.DataFrame(rows).to_csv(results / "summary_matrix.csv", index=False)

    paths = analyze_overlap(results, results / "overlap")
    targets = pd.read_csv(paths["targets"])
    overlap = pd.read_csv(paths["overlap"])
    sequence = pd.read_csv(paths["sequence"])
    timing = pd.read_csv(paths["timing"])
    frequency = pd.read_csv(paths["frequency"])
    report = paths["report"].read_text(encoding="utf-8")
    assert set(targets["baseline"]) == {"static_dag", "orion_like", "mint_markov_full"}
    assert not overlap.empty
    assert not sequence.empty
    assert not timing.empty
    assert not frequency.empty
    assert "WARNING high overlap" in report


def test_prepare_pareto_data_outputs_csv(tmp_path):
    matrix = tmp_path / "summary_matrix.csv"
    pd.DataFrame(
        [
            {
                "dag": "chain",
                "baseline": "no_warmup",
                "budget": 1,
                "end_to_end_latency_ms_avg": 1000,
                "p95_latency_ms": 1200,
                "p99_latency_ms": 1300,
                "total_warmup": 0,
                "cold_start_rate": 1.0,
                "wasted_warmup": 0,
            },
            {
                "dag": "chain",
                "baseline": "static_dag",
                "budget": 1,
                "end_to_end_latency_ms_avg": 800,
                "p95_latency_ms": 900,
                "p99_latency_ms": 950,
                "total_warmup": 3,
                "cold_start_rate": 0.3,
                "wasted_warmup": 1,
            },
            {
                "dag": "chain",
                "baseline": "mint_markov_offline",
                "budget": 1,
                "end_to_end_latency_ms_avg": 750,
                "p95_latency_ms": 875,
                "p99_latency_ms": 925,
                "total_warmup": 3,
                "cold_start_rate": 0.25,
                "wasted_warmup": 1,
            },
            {
                "dag": "chain",
                "baseline": "mint_markov_full",
                "budget": 1,
                "end_to_end_latency_ms_avg": 700,
                "p95_latency_ms": 850,
                "p99_latency_ms": 900,
                "total_warmup": 2,
                "cold_start_rate": 0.2,
                "wasted_warmup": 0,
            },
        ]
    ).to_csv(matrix, index=False)
    paths = prepare_pareto(matrix, tmp_path / "pareto")
    pareto = pd.read_csv(paths["latency"])
    no_warmup = pareto[pareto["baseline"] == "no_warmup"].iloc[0]
    mint = pareto[pareto["baseline"] == "mint_markov_full"].iloc[0]
    assert pd.isna(no_warmup["latency_per_warmup"])
    assert mint["is_pareto_efficient_latency"]
    assert mint["warmup_reduction_vs_static"] > 0
    assert paths["p95"].exists()
    assert paths["report"].exists()


def test_mixed_workload_executes_branch_join_path(tmp_path):
    dag = get_workload("mixed")
    assert dag.entry_nodes == ["f1"]
    assert dag.terminal_nodes == ["f5"]
    assert ("f2", "f4") in dag.edges
    assert ("f3", "f4") in dag.edges

    config = {
        "aws": {"lambda_functions": {f"f{i}": f"mint-f{i}" for i in range(1, 6)}},
        "experiment": {"baseline": "no_warmup", "warmup_budget": 1, "output_dir": str(tmp_path / "mixed")},
        "platform": {"default_retention_sec": 300, "default_cold_start_ms": 800, "default_warm_duration_ms": 100},
        "planner": {"type": "heuristic"},
    }
    controller = MintController(config, dag=dag, baseline="no_warmup", dry_run=True)
    summary = controller.run(2)
    assert summary["workflow_runs"] == 2
    assert summary["cold_start_count"] >= 4


def test_adaptive_stress_workloads_execute_one_branch(tmp_path):
    for name, expected_terminal in [("wide_branch", "f7"), ("deep_mixed", "f8")]:
        dag = get_workload(name)
        config = {
            "aws": {"lambda_functions": {node: f"mint-{node}" for node in dag.nodes}},
            "experiment": {
                "baseline": "mint_markov_full",
                "warmup_budget": 2,
                "output_dir": str(tmp_path / name),
                "profile_mismatch": True,
                "timing_jitter_ms": 100,
            },
            "platform": {"default_retention_sec": 300, "default_cold_start_ms": 800, "default_warm_duration_ms": 100},
            "planner": {"type": "markov", "horizon": 6},
        }
        controller = MintController(config, dag=dag, baseline="mint_markov_full", dry_run=True)
        summary = controller.run(2)
        assert summary["workflow_runs"] == 2
        events = [
            json.loads(line)
            for line in (tmp_path / name / "events.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        invoked = [event["logical_name"] for event in events if event.get("event_type") == "invocation"]
        assert expected_terminal in invoked
        assert len(set(invoked) & {"f2", "f3", "f4", "f5"}) < 5


def test_periodic_and_orion_differ_from_static_sequence(tmp_path):
    dag = get_workload("mixed")
    base_config = {
        "aws": {"lambda_functions": {f"f{i}": f"mint-f{i}" for i in range(1, 6)}},
        "experiment": {"warmup_budget": 2},
        "platform": {"default_retention_sec": 300, "default_cold_start_ms": 800, "default_warm_duration_ms": 100},
        "planner": {"type": "heuristic"},
    }

    sequences = {}
    for baseline in ("periodic_keepwarm", "static_dag", "orion_like", "mint_markov_offline"):
        config = dict(base_config)
        config["experiment"] = dict(base_config["experiment"], baseline=baseline, output_dir=str(tmp_path / baseline))
        controller = MintController(config, dag=dag, baseline=baseline, dry_run=True)
        controller.run(2)
        events = [
            json.loads(line)
            for line in (tmp_path / baseline / "events.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        sequences[baseline] = [
            (event.get("logical_name"), event.get("action"), event.get("gain"))
            for event in events
            if event.get("event_type") == "warmup"
        ]

    assert sequences["periodic_keepwarm"] != sequences["static_dag"]
    assert sequences["orion_like"] != sequences["static_dag"]
    assert sequences["mint_markov_offline"] != sequences["static_dag"]
