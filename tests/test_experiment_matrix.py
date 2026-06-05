import json

import pandas as pd

from scripts import run_experiment_matrix
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
    assert set(rows["baseline"]) == {"no_warmup", "static_dag"}


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
    rows = pd.DataFrame(
        [
            {
                "timestamp": "t",
                "dag": "chain",
                "baseline": "no_warmup",
                "budget": 2,
                "repetitions": 1,
                "output_dir": "x",
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
                "budget": 2,
                "repetitions": 1,
                "output_dir": "x",
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
                "baseline": "mint_offline",
                "budget": 2,
                "repetitions": 1,
                "output_dir": "x",
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
                "baseline": "mint_full",
                "budget": 2,
                "repetitions": 1,
                "output_dir": "x",
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
    mint = warmup[warmup["baseline"] == "mint_full"].iloc[0]
    assert mint["mint_vs_static_dag_warmup_reduction_ratio"] == 0.5
    assert mint["mint_vs_mint_offline_warmup_reduction_ratio"] == 0.5
    assert mint["mint_vs_no_warmup_latency_reduction_ratio"] == 0.5
