import json

import pandas as pd

from scripts import run_experiment_matrix
from scripts import run_delay_shift_experiment
from scripts.prepare_paper_tables import prepare_tables


def test_experiment_matrix_dry_run_generates_summary(tmp_path):
    output_root = tmp_path / "matrix"
    rc = run_experiment_matrix.main(
        [
            "--config",
            "configs/mint_aws.yaml",
            "--dags",
            "chain",
            "--baselines",
            "no_warmup",
            "static_dag",
            "--budgets",
            "2",
            "--repetitions",
            "1",
            "--cooldown-sec",
            "0",
            "--dry-run",
            "--output-root",
            str(output_root),
        ]
    )
    assert rc == 0
    matrix_csv = output_root / "summary_matrix.csv"
    matrix_json = output_root / "summary_matrix.json"
    manifest = output_root / "experiment_manifest.json"
    assert matrix_csv.exists()
    assert matrix_json.exists()
    assert manifest.exists()
    rows = pd.read_csv(matrix_csv)
    assert len(rows) == 2
    assert "uncovered_cold_start" in rows.columns
    assert "effective_planner" in rows.columns
    assert set(rows["baseline"]) == {"no_warmup", "static_dag"}
    planners = dict(zip(rows["baseline"], rows["effective_planner"]))
    assert planners["no_warmup"] == "none"
    assert planners["static_dag"] == "static"


def test_experiment_matrix_records_failed_run_and_continues(tmp_path):
    output_root = tmp_path / "matrix_fail"
    rc = run_experiment_matrix.main(
        [
            "--config",
            "configs/mint_aws.yaml",
            "--dags",
            "chain",
            "missing_dag",
            "--baselines",
            "no_warmup",
            "--budgets",
            "1",
            "--repetitions",
            "1",
            "--dry-run",
            "--output-root",
            str(output_root),
        ]
    )
    assert rc == 0
    rows = pd.read_csv(output_root / "summary_matrix.csv")
    assert len(rows) == 1
    failures = (output_root / "failed_runs.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(failures) == 1
    failure = json.loads(failures[0])
    assert failure["dag"] == "missing_dag"


def test_experiment_matrix_defaults_to_dry_run_without_confirm(tmp_path):
    output_root = tmp_path / "matrix_safe"
    rc = run_experiment_matrix.main(
        [
            "--config",
            "configs/mint_aws.yaml",
            "--dags",
            "chain",
            "--baselines",
            "no_warmup",
            "--budgets",
            "1",
            "--repetitions",
            "1",
            "--output-root",
            str(output_root),
        ]
    )
    assert rc == 0
    manifest = json.loads((output_root / "experiment_manifest.json").read_text(encoding="utf-8"))
    assert manifest["dry_run"] is True


def test_prepare_paper_tables_from_small_matrix_csv(tmp_path):
    matrix_csv = tmp_path / "summary_matrix.csv"
    run_dirs = {}
    for name, latencies in {
        "no_warmup": [900, 1100],
        "static_dag": [650, 750],
        "mint_markov_offline": [600, 700],
        "mint_markov_full": [450, 550],
    }.items():
        run_dir = tmp_path / name
        run_dir.mkdir()
        pd.DataFrame({"run_id": ["r1", "r2"], "latency_ms": latencies}).to_csv(run_dir / "runs.csv", index=False)
        run_dirs[name] = str(run_dir)
    rows = pd.DataFrame(
        [
            {
                "timestamp": "t",
                "dag": "chain",
                "baseline": "no_warmup",
                "planner_type": "markov",
                "effective_planner": "none",
                "budget": 2,
                "repetitions": 1,
                "output_dir": run_dirs["no_warmup"],
                "workflow_runs": 1,
                "end_to_end_latency_ms_avg": 1000,
                "p50_latency_ms": 1000,
                "p95_latency_ms": 1000,
                "p99_latency_ms": 1000,
                "cold_start_count": 3,
                "cold_start_rate": 1.0,
                "total_warmup": 0,
                "useful_warmup": 0,
                "wasted_warmup": 0,
                "missed_warmup": 0,
                "uncovered_cold_start": 3,
                "useful_warmup_ratio": 0,
                "execute_count": 0,
                "delay_count": 0,
                "cancel_count": 0,
                "replace_count": 0,
            },
            {
                "timestamp": "t",
                "dag": "chain",
                "baseline": "static_dag",
                "planner_type": "markov",
                "effective_planner": "static",
                "budget": 2,
                "repetitions": 1,
                "output_dir": run_dirs["static_dag"],
                "workflow_runs": 1,
                "end_to_end_latency_ms_avg": 700,
                "p50_latency_ms": 700,
                "p95_latency_ms": 700,
                "p99_latency_ms": 700,
                "cold_start_count": 1,
                "cold_start_rate": 0.33,
                "total_warmup": 4,
                "useful_warmup": 2,
                "wasted_warmup": 2,
                "missed_warmup": 1,
                "uncovered_cold_start": 0,
                "useful_warmup_ratio": 0.5,
                "execute_count": 4,
                "delay_count": 0,
                "cancel_count": 0,
                "replace_count": 2,
            },
            {
                "timestamp": "t",
                "dag": "chain",
                "baseline": "mint_markov_offline",
                "planner_type": "markov",
                "effective_planner": "markov",
                "budget": 2,
                "repetitions": 1,
                "output_dir": run_dirs["mint_markov_offline"],
                "workflow_runs": 1,
                "end_to_end_latency_ms_avg": 650,
                "p50_latency_ms": 650,
                "p95_latency_ms": 650,
                "p99_latency_ms": 650,
                "cold_start_count": 1,
                "cold_start_rate": 0.33,
                "total_warmup": 4,
                "useful_warmup": 2,
                "wasted_warmup": 2,
                "missed_warmup": 1,
                "uncovered_cold_start": 0,
                "useful_warmup_ratio": 0.5,
                "execute_count": 4,
                "delay_count": 0,
                "cancel_count": 0,
                "replace_count": 2,
            },
            {
                "timestamp": "t",
                "dag": "chain",
                "baseline": "mint_markov_full",
                "planner_type": "markov",
                "effective_planner": "markov",
                "budget": 2,
                "repetitions": 1,
                "output_dir": run_dirs["mint_markov_full"],
                "workflow_runs": 1,
                "end_to_end_latency_ms_avg": 500,
                "p50_latency_ms": 500,
                "p95_latency_ms": 500,
                "p99_latency_ms": 500,
                "cold_start_count": 0,
                "cold_start_rate": 0,
                "total_warmup": 2,
                "useful_warmup": 2,
                "wasted_warmup": 0,
                "missed_warmup": 0,
                "uncovered_cold_start": 0,
                "useful_warmup_ratio": 1,
                "execute_count": 2,
                "delay_count": 0,
                "cancel_count": 1,
                "replace_count": 1,
            },
        ]
    )
    rows.to_csv(matrix_csv, index=False)
    paths = prepare_tables(matrix_csv, tmp_path / "paper_tables")
    for path in paths.values():
        assert path.exists()
    warmup = pd.read_csv(paths["warmup"])
    mint = warmup[warmup["baseline"] == "mint_markov_full"].iloc[0]
    assert mint["mint_vs_static_dag_warmup_reduction_ratio"] == 0.5
    assert mint["mint_vs_offline_warmup_reduction_ratio"] == 0.5
    assert mint["mint_vs_no_warmup_latency_reduction_ratio"] == 0.5
    assert round(mint["mint_vs_static_dag_latency_reduction_ratio"], 6) == round((700 - 500) / 700, 6)
    assert round(mint["mint_vs_offline_latency_reduction_ratio"], 6) == round((650 - 500) / 650, 6)
    assert paths["overall"].exists()
    assert paths["improvement"].exists()
    variability = pd.read_csv(paths["variability"])
    assert set(variability["baseline"]) == {"no_warmup", "static_dag", "mint_markov_offline", "mint_markov_full"}


def test_delay_shift_dry_run_generates_delay_and_outputs(tmp_path):
    output_dir = tmp_path / "delay"
    rc = run_delay_shift_experiment.main(
        [
            "--config",
            "configs/mint_aws.yaml",
            "--baseline",
            "mint_markov_full",
            "--repetitions",
            "2",
            "--upstream-delay-ms",
            "1200",
            "--dry-run",
            "--output-dir",
            str(output_dir),
        ]
    )
    assert rc == 0
    assert (output_dir / "events.jsonl").exists()
    assert (output_dir / "summary.json").exists()
    assert (output_dir / "delay_analysis.csv").exists()
    analysis = pd.read_csv(output_dir / "delay_analysis.csv")
    assert int(analysis["delay_count"].iloc[0]) > 0


def test_delay_shift_refuses_real_run_without_confirm(tmp_path):
    config_path = tmp_path / "real.yaml"
    config_path.write_text(
        """
aws:
  region: us-east-1
  lambda_functions:
    f1: mint-f1
    f2: mint-f2
    f3: mint-f3
experiment:
  warmup_budget: 2
  dry_run: false
platform:
  default_retention_sec: 300
  default_cold_start_ms: 800
  default_warm_duration_ms: 100
planner:
  type: markov
  horizon: 5
scheduler:
  enable_delay: true
  enable_cancel: true
  enable_replace: true
  gain_threshold: 0.0
""",
        encoding="utf-8",
    )
    rc = run_delay_shift_experiment.main(
        [
            "--config",
            str(config_path),
            "--baseline",
            "mint_markov_full",
            "--repetitions",
            "1",
            "--output-dir",
            str(tmp_path / "blocked"),
        ]
    )
    assert rc == 2
