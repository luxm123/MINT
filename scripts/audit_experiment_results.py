from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd


EXPECTED_DAGS = ["chain", "fanout", "branch", "join"]
EXPECTED_BASELINES = [
    "no_warmup",
    "periodic_keepwarm",
    "static_dag",
    "orion_like",
    "mint_markov_offline",
    "mint_markov_full",
]
EXPECTED_BUDGETS = [1, 2, 3]
EXPECTED_WORKFLOW_RUNS = 10
EXPECTED_EFFECTIVE_PLANNER = {
    "no_warmup": "none",
    "periodic_keepwarm": "periodic",
    "static_dag": "static",
    "orion_like": "orion_like",
    "mint_markov_offline": "markov",
    "mint_markov_full": "markov",
}
ACTIVE_PREWARM_BASELINES = {
    "periodic_keepwarm",
    "static_dag",
    "orion_like",
    "mint_markov_offline",
    "mint_markov_full",
}
LATENCY_COLUMNS = ["end_to_end_latency_ms_avg", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms"]
NONNEGATIVE_COLUMNS = [
    "total_warmup",
    "useful_warmup",
    "wasted_warmup",
    "missed_warmup",
    "unserved_intent_cold_start",
    "uncovered_cold_start",
    "cold_start_count",
    "execute_count",
    "delay_count",
    "cancel_count",
    "replace_count",
]
RATIO_COLUMNS = ["cold_start_rate", "useful_warmup_ratio"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit MINT experiment matrix results for consistency.")
    parser.add_argument("--matrix-csv", required=True)
    parser.add_argument("--failed-runs")
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def _issue(severity: str, check: str, message: str, details: Any | None = None) -> dict[str, Any]:
    return {"severity": severity, "check": check, "message": message, "details": details}


def _read_failed_runs(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _missing_expected_configs(df: pd.DataFrame) -> list[dict[str, Any]]:
    observed = {(str(row.dag), str(row.baseline), int(row.budget)) for row in df.itertuples(index=False)}
    missing = []
    for dag in EXPECTED_DAGS:
        for baseline in EXPECTED_BASELINES:
            for budget in EXPECTED_BUDGETS:
                if (dag, baseline, budget) not in observed:
                    missing.append({"dag": dag, "baseline": baseline, "budget": budget})
    return missing


def _unexpected_configs(df: pd.DataFrame) -> list[dict[str, Any]]:
    expected = {(dag, baseline, budget) for dag in EXPECTED_DAGS for baseline in EXPECTED_BASELINES for budget in EXPECTED_BUDGETS}
    unexpected = []
    for row in df.itertuples(index=False):
        key = (str(row.dag), str(row.baseline), int(row.budget))
        if key not in expected:
            unexpected.append({"dag": key[0], "baseline": key[1], "budget": key[2]})
    return unexpected


def audit_matrix(matrix_csv: str | Path, failed_runs: str | Path | None = None) -> dict[str, Any]:
    matrix_path = Path(matrix_csv)
    issues: list[dict[str, Any]] = []
    if not matrix_path.exists():
        return {
            "ok": False,
            "error_count": 1,
            "warning_count": 0,
            "issues": [_issue("error", "matrix_csv", f"Missing matrix CSV: {matrix_path}")],
            "summary": {},
        }

    df = pd.read_csv(matrix_path)
    summary = {"matrix_csv": str(matrix_path), "row_count": int(len(df))}
    expected_rows = len(EXPECTED_DAGS) * len(EXPECTED_BASELINES) * len(EXPECTED_BUDGETS)
    if len(df) != expected_rows:
        issues.append(_issue("error", "matrix_shape", f"Expected {expected_rows} rows, found {len(df)}"))

    required_columns = {
        "dag",
        "baseline",
        "budget",
        "workflow_runs",
        "effective_planner",
        "total_warmup",
        "cold_start_rate",
        "useful_warmup_ratio",
    }
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        issues.append(_issue("error", "required_columns", "Missing required columns", missing_columns))
        return _final_report(summary, issues)

    missing_configs = _missing_expected_configs(df)
    if missing_configs:
        issues.append(_issue("error", "missing_configs", "Missing expected DAG/baseline/budget configurations", missing_configs))
    unexpected_configs = _unexpected_configs(df)
    if unexpected_configs:
        issues.append(_issue("error", "unexpected_configs", "Unexpected DAG/baseline/budget configurations", unexpected_configs))

    bad_runs = df[pd.to_numeric(df["workflow_runs"], errors="coerce") != EXPECTED_WORKFLOW_RUNS]
    if not bad_runs.empty:
        issues.append(
            _issue(
                "error",
                "workflow_runs",
                f"All configurations must have workflow_runs={EXPECTED_WORKFLOW_RUNS}",
                bad_runs[["dag", "baseline", "budget", "workflow_runs"]].to_dict("records"),
            )
        )

    failed_path = Path(failed_runs) if failed_runs else matrix_path.parent / "failed_runs.jsonl"
    failed_lines = _read_failed_runs(failed_path)
    summary["failed_runs_path"] = str(failed_path)
    summary["failed_run_count"] = len(failed_lines)
    if failed_lines:
        issues.append(_issue("error", "failed_runs", "failed_runs.jsonl is not empty", failed_lines))

    no_warmup = df[df["baseline"] == "no_warmup"]
    bad_no_warmup = no_warmup[pd.to_numeric(no_warmup["total_warmup"], errors="coerce") != 0]
    if not bad_no_warmup.empty:
        issues.append(
            _issue(
                "error",
                "no_warmup_total_warmup",
                "no_warmup must have total_warmup=0",
                bad_no_warmup[["dag", "budget", "total_warmup"]].to_dict("records"),
            )
        )

    active = df[df["baseline"].isin(ACTIVE_PREWARM_BASELINES)].copy()
    active["budget_limit"] = pd.to_numeric(active["budget"], errors="coerce") * pd.to_numeric(active["workflow_runs"], errors="coerce")
    active["total_warmup_num"] = pd.to_numeric(active["total_warmup"], errors="coerce")
    over_budget = active[active["total_warmup_num"] > active["budget_limit"]]
    if not over_budget.empty:
        issues.append(
            _issue(
                "warning",
                "warmup_budget",
                "Some active prewarm baselines exceed budget x workflow_runs; inspect adaptive/rescheduled behavior.",
                over_budget[["dag", "baseline", "budget", "workflow_runs", "total_warmup", "budget_limit"]].to_dict("records"),
            )
        )

    planner_mismatch = []
    for row in df.itertuples(index=False):
        expected = EXPECTED_EFFECTIVE_PLANNER.get(str(row.baseline))
        actual = str(row.effective_planner)
        if expected is not None and actual != expected:
            planner_mismatch.append({"dag": row.dag, "baseline": row.baseline, "budget": int(row.budget), "expected": expected, "actual": actual})
    if planner_mismatch:
        issues.append(_issue("error", "effective_planner", "effective_planner values do not match expected mapping", planner_mismatch))

    issues.extend(_numeric_issues(df))
    issues.extend(_sanity_warnings(df))
    return _final_report(summary, issues)


def _numeric_issues(df: pd.DataFrame) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if df.isna().any().any():
        columns = sorted(df.columns[df.isna().any()].tolist())
        issues.append(_issue("error", "nan_or_empty", "Matrix contains NaN or empty values", columns))

    for column in LATENCY_COLUMNS:
        if column in df.columns:
            bad = df[pd.to_numeric(df[column], errors="coerce") < 0]
            if not bad.empty:
                issues.append(_issue("error", column, f"{column} must not be negative", bad[["dag", "baseline", "budget", column]].to_dict("records")))

    for column in NONNEGATIVE_COLUMNS:
        if column in df.columns:
            bad = df[pd.to_numeric(df[column], errors="coerce") < 0]
            if not bad.empty:
                issues.append(_issue("error", column, f"{column} must not be negative", bad[["dag", "baseline", "budget", column]].to_dict("records")))

    for column in RATIO_COLUMNS:
        if column in df.columns:
            values = pd.to_numeric(df[column], errors="coerce")
            bad = df[(values < 0) | (values > 1)]
            if not bad.empty:
                issues.append(_issue("error", column, f"{column} must be in [0, 1]", bad[["dag", "baseline", "budget", column]].to_dict("records")))
    return issues


def _sanity_warnings(df: pd.DataFrame) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    mint = df[df["baseline"] == "mint_markov_full"]
    expected_mint = len(EXPECTED_DAGS) * len(EXPECTED_BUDGETS)
    if len(mint) != expected_mint:
        issues.append(_issue("warning", "mint_markov_full_presence", f"Expected {expected_mint} mint_markov_full rows, found {len(mint)}"))
    if "cancel_count" in mint.columns and pd.to_numeric(mint["cancel_count"], errors="coerce").sum() <= 0:
        issues.append(_issue("warning", "mint_markov_full_cancel_count", "mint_markov_full cancel_count is zero across the matrix"))

    branch_mint = df[(df["dag"] == "branch") & (df["baseline"] == "mint_markov_full")]
    branch_static = df[(df["dag"] == "branch") & (df["baseline"] == "static_dag")]
    if not branch_mint.empty and not branch_static.empty:
        merged = branch_mint.merge(branch_static, on="budget", suffixes=("_mint", "_static"))
        concerning = []
        for row in merged.itertuples(index=False):
            mint_latency = float(getattr(row, "end_to_end_latency_ms_avg_mint"))
            static_latency = float(getattr(row, "end_to_end_latency_ms_avg_static"))
            mint_cold = float(getattr(row, "cold_start_count_mint"))
            static_cold = float(getattr(row, "cold_start_count_static"))
            if not (mint_latency < static_latency or mint_cold < static_cold):
                concerning.append(
                    {
                        "budget": int(row.budget),
                        "mint_latency": mint_latency,
                        "static_latency": static_latency,
                        "mint_cold_start_count": mint_cold,
                        "static_cold_start_count": static_cold,
                    }
                )
        if concerning:
            issues.append(
                _issue(
                    "warning",
                    "branch_mint_vs_static",
                    "mint_markov_full does not improve branch latency or cold_start_count over static_dag for some budgets",
                    concerning,
                )
            )
    return issues


def _final_report(summary: dict[str, Any], issues: list[dict[str, Any]]) -> dict[str, Any]:
    error_count = sum(1 for issue in issues if issue["severity"] == "error")
    warning_count = sum(1 for issue in issues if issue["severity"] == "warning")
    return {
        "ok": error_count == 0,
        "error_count": error_count,
        "warning_count": warning_count,
        "summary": summary,
        "issues": issues,
    }


def write_report(report: dict[str, Any], output_dir: str | Path) -> tuple[Path, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "audit_report.json"
    txt_path = out / "audit_report.txt"
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)
    lines = [
        f"Audit OK: {report['ok']}",
        f"Errors: {report['error_count']}",
        f"Warnings: {report['warning_count']}",
        "",
    ]
    for issue in report["issues"]:
        lines.append(f"[{issue['severity'].upper()}] {issue['check']}: {issue['message']}")
        if issue.get("details") is not None:
            lines.append(json.dumps(issue["details"], indent=2, sort_keys=True))
        lines.append("")
    txt_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, txt_path


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = audit_matrix(args.matrix_csv, args.failed_runs)
    json_path, txt_path = write_report(report, args.output_dir)
    print(f"Wrote {json_path}")
    print(f"Wrote {txt_path}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
