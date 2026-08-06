import csv
import json

from scripts import analyze_strategy_differences, run_experiment_matrix


def test_paired_strategy_report_enforces_trace_history_and_budget(tmp_path):
    output_root = tmp_path / "paired"
    rc = run_experiment_matrix.main(
        [
            "--config", "configs/mint_adaptive_local.yaml",
            "--dags", "adaptive_branch",
            "--baselines", *analyze_strategy_differences.CORE_BASELINES,
            "--budgets", "2",
            "--repetitions", "4",
            "--cooldown-sec", "0",
            "--dry-run",
            "--output-root", str(output_root),
        ]
    )
    assert rc == 0
    report = analyze_strategy_differences.analyze(output_root, 4, 2, "f5", 2)
    assert report["quality_gates"] == {
        "same_branch_trace": True,
        "same_history_snapshots": True,
        "no_warmup_errors": True,
        "runtime_replanning_after_branch_reveal": True,
        "expected_runs": 4,
        "budget": 2,
    }
    assert [row["workflow_index"] for row in report["paired_rows"]] == list(range(4))

    manifest = json.loads((output_root / "experiment_manifest.json").read_text(encoding="utf-8"))
    assert manifest["branch_seed"] == 20260804
    assert manifest["initial_history_size"] == 10
    assert len(manifest["materialized_branch_traces"]["adaptive_branch"]) == 4
    rows = list(csv.DictReader((output_root / "summary_matrix.csv").open(newline="", encoding="utf-8")))
    assert next(row for row in rows if row["baseline"] == "xanadu_like")["effective_planner"] == "xanadu_like"

