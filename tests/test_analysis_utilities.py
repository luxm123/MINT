import json

import pandas as pd

from mint.controller import MintController
from mint.workloads import get_workload
from scripts.analyze_baseline_overlap import analyze_overlap
from scripts.prepare_pareto_data import prepare_pareto


def _write_events(path, targets):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"event_type": "warmup", "run_id": "r1", "logical_name": target, "action": "execute", "useful": True}
        for target in targets
    ]
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
    report = paths["report"].read_text(encoding="utf-8")
    assert set(targets["baseline"]) == {"static_dag", "orion_like", "mint_markov_full"}
    assert not overlap.empty
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
                "baseline": "mint_markov_full",
                "budget": 1,
                "end_to_end_latency_ms_avg": 700,
                "p95_latency_ms": 850,
                "p99_latency_ms": 900,
                "total_warmup": 5,
                "cold_start_rate": 0.2,
                "wasted_warmup": 1,
            },
        ]
    ).to_csv(matrix, index=False)
    paths = prepare_pareto(matrix, tmp_path / "pareto")
    pareto = pd.read_csv(paths["latency"])
    no_warmup = pareto[pareto["baseline"] == "no_warmup"].iloc[0]
    assert pd.isna(no_warmup["latency_per_warmup"])
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
