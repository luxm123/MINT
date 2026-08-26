from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.audit_adaptive_stress_results import (
    EXPECTED_BASELINES,
    EXPECTED_BUDGETS,
    EXPECTED_DAGS,
    EXPECTED_EFFECTIVE_PLANNER,
    audit_adaptive_stress,
)


def _write_fixture(
    tmp_path: Path,
    *,
    skipped: list[dict] | None = None,
    omit_baseline: str | None = None,
) -> Path:
    results_dir = tmp_path / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for dag in EXPECTED_DAGS:
        for baseline in EXPECTED_BASELINES:
            if baseline == omit_baseline:
                continue
            for budget in EXPECTED_BUDGETS:
                rows.append(
                    {
                        "timestamp": "20260101T000000Z",
                        "dag": dag,
                        "baseline": baseline,
                        "planner_type": "markov",
                        "effective_planner": EXPECTED_EFFECTIVE_PLANNER[baseline],
                        "budget": budget,
                        "repetitions": 1,
                        "seed": 42,
                        "output_dir": str(results_dir / f"{dag}_{baseline}_B{budget}"),
                        "workflow_runs": 1,
                        "end_to_end_latency_ms_avg": 500 + budget * 100,
                        "p50_latency_ms": 450 + budget * 100,
                        "p95_latency_ms": 700 + budget * 100,
                        "p99_latency_ms": 900 + budget * 100,
                        "cold_start_count": (
                            1 if baseline == "no_warmup" else 0
                        ),
                        "cold_start_rate": (
                            0.2 if baseline == "no_warmup" else 0.0
                        ),
                        "total_warmup": (
                            0
                            if baseline in {"no_warmup", "provisioned_concurrency"}
                            else budget
                        ),
                        "useful_warmup": (
                            0
                            if baseline in {"no_warmup", "provisioned_concurrency"}
                            else budget
                        ),
                        "wasted_warmup": 0,
                        "missed_warmup": 0,
                        "unserved_intent_cold_start": 0,
                        "uncovered_cold_start": 1 if baseline == "no_warmup" else 0,
                        "useful_warmup_ratio": 1.0,
                        "execute_count": (
                            0
                            if baseline in {"no_warmup", "provisioned_concurrency"}
                            else budget
                        ),
                        "provisioned_slots_total": (
                            budget if baseline == "provisioned_concurrency" else 0
                        ),
                        "provisioned_duration_sec_total": (
                            1.0 if baseline == "provisioned_concurrency" else 0.0
                        ),
                        "delay_count": 0,
                        "cancel_count": 0,
                        "replace_count": 0,
                    }
                )
    pd.DataFrame(rows).to_csv(results_dir / "summary_matrix.csv", index=False)
    manifest = {
        "dags": EXPECTED_DAGS,
        "baselines": EXPECTED_BASELINES,
        "budgets": EXPECTED_BUDGETS,
        "repetitions": 1,
        "seeds": [42],
        "profile_mismatch": True,
        "timing_jitter_ms": 800.0,
        "branch_seed": 42,
        "randomize_order": True,
        "skipped_count": len(skipped or []),
        "skipped_configs": skipped or [],
        "dry_run": True,
    }
    (results_dir / "experiment_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (results_dir / "failed_runs.jsonl").write_text("", encoding="utf-8")
    (results_dir / "skipped_runs.jsonl").write_text(
        "\n".join(json.dumps(item, sort_keys=True) for item in (skipped or [])),
        encoding="utf-8",
    )
    (results_dir / "provenance.json").write_text(
        json.dumps({"git": {"commit_sha": "abc123"}}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return results_dir


def test_audit_accepts_complete_core_matrix(tmp_path):
    results_dir = _write_fixture(tmp_path)
    report = audit_adaptive_stress(results_dir, repetitions=1)
    assert report["ok"] is True
    assert report["error_count"] == 0


def test_audit_rejects_mint_b3_skip(tmp_path):
    skipped = [
        {
            "dag": "wide_branch",
            "baseline": "mint_markov_full",
            "budget": 3,
            "reason": "stale B=3 skip must not happen",
        }
    ]
    results_dir = _write_fixture(tmp_path, skipped=skipped)
    report = audit_adaptive_stress(results_dir, repetitions=1)
    assert report["ok"] is False
    assert any(issue["check"] == "skipped_runs" for issue in report["issues"])


def test_audit_rejects_missing_baseline(tmp_path):
    results_dir = _write_fixture(tmp_path, omit_baseline="faascache")
    report = audit_adaptive_stress(results_dir, repetitions=1)
    assert report["ok"] is False
    assert any(issue["check"] == "missing_configs" for issue in report["issues"])


def test_audit_rejects_wrong_manifest_settings(tmp_path):
    results_dir = _write_fixture(tmp_path)
    manifest_path = results_dir / "experiment_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["profile_mismatch"] = False
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    report = audit_adaptive_stress(results_dir, repetitions=1)
    assert report["ok"] is False
    assert any(
        issue["check"] == "manifest" and "profile_mismatch" in issue["message"]
        for issue in report["issues"]
    )
