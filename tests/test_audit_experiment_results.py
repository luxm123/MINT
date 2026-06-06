import json

import pandas as pd

from scripts.audit_experiment_results import EXPECTED_BASELINES, EXPECTED_BUDGETS, EXPECTED_DAGS, audit_matrix, main


def _complete_rows():
    rows = []
    for dag in EXPECTED_DAGS:
        for baseline in EXPECTED_BASELINES:
            for budget in EXPECTED_BUDGETS:
                effective = {
                    "no_warmup": "none",
                    "periodic_keepwarm": "periodic",
                    "static_dag": "static",
                    "orion_like": "orion_like",
                    "mint_markov_offline": "markov",
                    "mint_markov_full": "markov",
                }[baseline]
                warmups = 0 if baseline == "no_warmup" else budget * 10
                rows.append(
                    {
                        "timestamp": "t",
                        "dag": dag,
                        "baseline": baseline,
                        "planner_type": "markov",
                        "effective_planner": effective,
                        "budget": budget,
                        "repetitions": 10,
                        "output_dir": "x",
                        "workflow_runs": 10,
                        "end_to_end_latency_ms_avg": 1000 - (100 if baseline == "mint_markov_full" else 0),
                        "p50_latency_ms": 900,
                        "p95_latency_ms": 1100,
                        "p99_latency_ms": 1200,
                        "cold_start_count": 2 if baseline == "mint_markov_full" else 5,
                        "cold_start_rate": 0.2 if baseline == "mint_markov_full" else 0.5,
                        "total_warmup": warmups,
                        "useful_warmup": warmups,
                        "wasted_warmup": 0,
                        "missed_warmup": 0,
                        "unserved_intent_cold_start": 0,
                        "uncovered_cold_start": 0 if baseline != "no_warmup" else 5,
                        "useful_warmup_ratio": 0 if baseline == "no_warmup" else 1,
                        "execute_count": warmups,
                        "delay_count": 0,
                        "cancel_count": 1 if baseline == "mint_markov_full" else 0,
                        "replace_count": 0,
                    }
                )
    return rows


def _write_matrix(tmp_path, rows):
    matrix = tmp_path / "summary_matrix.csv"
    pd.DataFrame(rows).to_csv(matrix, index=False)
    failed = tmp_path / "failed_runs.jsonl"
    failed.write_text("", encoding="utf-8")
    return matrix, failed


def test_audit_complete_matrix_passes(tmp_path):
    matrix, failed = _write_matrix(tmp_path, _complete_rows())
    report = audit_matrix(matrix, failed)
    assert report["ok"] is True
    assert report["error_count"] == 0


def test_audit_missing_config_fails(tmp_path):
    rows = _complete_rows()[:-1]
    matrix, failed = _write_matrix(tmp_path, rows)
    report = audit_matrix(matrix, failed)
    assert report["ok"] is False
    assert any(issue["check"] == "missing_configs" for issue in report["issues"])


def test_audit_bad_workflow_runs_fails(tmp_path):
    rows = _complete_rows()
    rows[0]["workflow_runs"] = 9
    matrix, failed = _write_matrix(tmp_path, rows)
    report = audit_matrix(matrix, failed)
    assert report["ok"] is False
    assert any(issue["check"] == "workflow_runs" for issue in report["issues"])


def test_audit_bad_effective_planner_fails(tmp_path):
    rows = _complete_rows()
    rows[0]["effective_planner"] = "markov"
    matrix, failed = _write_matrix(tmp_path, rows)
    report = audit_matrix(matrix, failed)
    assert report["ok"] is False
    assert any(issue["check"] == "effective_planner" for issue in report["issues"])


def test_audit_nan_or_negative_latency_fails(tmp_path):
    rows = _complete_rows()
    rows[0]["end_to_end_latency_ms_avg"] = -1
    rows[1]["p50_latency_ms"] = float("nan")
    matrix, failed = _write_matrix(tmp_path, rows)
    report = audit_matrix(matrix, failed)
    assert report["ok"] is False
    assert any(issue["check"] == "nan_or_empty" for issue in report["issues"])
    assert any(issue["check"] == "end_to_end_latency_ms_avg" for issue in report["issues"])


def test_audit_warning_does_not_fail(tmp_path):
    rows = _complete_rows()
    for row in rows:
        if row["baseline"] == "mint_markov_full":
            row["cancel_count"] = 0
    matrix, failed = _write_matrix(tmp_path, rows)
    report = audit_matrix(matrix, failed)
    assert report["ok"] is True
    assert report["warning_count"] >= 1


def test_audit_cli_writes_reports(tmp_path):
    matrix, failed = _write_matrix(tmp_path, _complete_rows())
    output_dir = tmp_path / "audit"
    rc = main(["--matrix-csv", str(matrix), "--failed-runs", str(failed), "--output-dir", str(output_dir)])
    assert rc == 0
    assert (output_dir / "audit_report.json").exists()
    assert (output_dir / "audit_report.txt").exists()
    report = json.loads((output_dir / "audit_report.json").read_text(encoding="utf-8"))
    assert report["ok"] is True
