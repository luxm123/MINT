from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


EXPECTED_DAGS = ["wide_branch", "deep_mixed"]
EXPECTED_BASELINES = [
    "no_warmup",
    "orion_full",
    "faascache",
    "xanadu_full",
    "mint_markov_full",
]
EXPECTED_BUDGETS = [1, 2, 3]
EXPECTED_EFFECTIVE_PLANNER = {
    "no_warmup": "none",
    "orion_full": "orion_full",
    "faascache": "faascache",
    "xanadu_full": "xanadu_full",
    "mint_markov_full": "markov",
}
ACTIVE_PREWARM_BASELINES = {
    "orion_full",
    "faascache",
    "xanadu_full",
    "mint_markov_full",
}
# Provisioned Concurrency remains available as an appendix comparison; the
# core matrix does not require it, so the dedicated semantic check below is
# vacuous on the confirmed core table.
PROVISIONED_BASELINES = {"provisioned_concurrency"}
LATENCY_COLUMNS = [
    "end_to_end_latency_ms_avg",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
]
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


def _issue(severity: str, check: str, message: str, details: Any | None = None) -> dict[str, Any]:
    return {"severity": severity, "check": check, "message": message, "details": details}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _read_manifest(results_dir: Path) -> dict[str, Any]:
    manifest_path = results_dir / "experiment_manifest.json"
    if not manifest_path.exists():
        return {}
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _check_manifest(
    results_dir: Path,
    repetitions: int,
    seeds: list[int],
    issues: list[dict[str, Any]],
) -> dict[str, Any]:
    manifest_path = results_dir / "experiment_manifest.json"
    if not manifest_path.exists():
        issues.append(_issue("error", "manifest", "Missing experiment_manifest.json", str(manifest_path)))
        return {}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_manifest = {
        "dags": set(EXPECTED_DAGS),
        "baselines": set(EXPECTED_BASELINES),
        "budgets": EXPECTED_BUDGETS,
        "repetitions": repetitions,
        "seeds": seeds,
        "profile_mismatch": True,
        "timing_jitter_ms": 800.0,
        "randomize_order": True,
    }
    checks = [
        ("dags", set(manifest.get("dags", []))),
        ("baselines", set(manifest.get("baselines", []))),
        ("budgets", manifest.get("budgets", [])),
        ("repetitions", manifest.get("repetitions")),
        ("seeds", list(manifest.get("seeds", []))),
        ("profile_mismatch", manifest.get("profile_mismatch")),
        ("timing_jitter_ms", float(manifest.get("timing_jitter_ms", 0.0))),
        ("randomize_order", manifest.get("randomize_order")),
    ]
    for field, actual in checks:
        if actual != expected_manifest[field]:
            issues.append(
                _issue(
                    "error",
                    "manifest",
                    f"manifest {field} mismatch",
                    {"expected": expected_manifest[field], "actual": actual},
                )
            )
    if manifest.get("skipped_count", len(_read_jsonl(results_dir / "skipped_runs.jsonl"))) != 0:
        issues.append(
            _issue(
                "error",
                "skipped_runs",
                "adaptive stress core matrix must have zero skipped configurations",
                _read_jsonl(results_dir / "skipped_runs.jsonl"),
            )
        )
    provenance_path = results_dir / "provenance.json"
    if not provenance_path.exists():
        issues.append(_issue("error", "provenance", "Missing provenance.json", str(provenance_path)))
    else:
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        git = provenance.get("git", {})
        if not git.get("commit_sha"):
            issues.append(
                _issue("error", "provenance", "provenance.json lacks git commit_sha", git)
            )
    return manifest


def _check_matrix(
    results_dir: Path,
    repetitions: int,
    seeds: list[int],
    issues: list[dict[str, Any]],
) -> dict[str, Any]:
    matrix_path = results_dir / "summary_matrix.csv"
    if not matrix_path.exists():
        issues.append(_issue("error", "matrix_csv", "Missing summary_matrix.csv", str(matrix_path)))
        return {}
    df = pd.read_csv(matrix_path)
    summary = {"matrix_csv": str(matrix_path), "row_count": int(len(df))}
    expected_rows = (
        len(EXPECTED_DAGS)
        * len(EXPECTED_BASELINES)
        * len(EXPECTED_BUDGETS)
        * len(seeds)
    )
    if len(df) != expected_rows:
        issues.append(
            _issue(
                "error",
                "matrix_shape",
                f"Expected {expected_rows} rows, found {len(df)}",
            )
        )
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
        issues.append(
            _issue("error", "required_columns", "Missing required columns", missing_columns)
        )
        return summary

    observed = {
        (
            str(row.dag),
            str(row.baseline),
            int(row.budget),
            int(getattr(row, "seed", 0)),
        )
        for row in df.itertuples(index=False)
    }
    expected = {
        (dag, baseline, budget, seed)
        for dag in EXPECTED_DAGS
        for baseline in EXPECTED_BASELINES
        for budget in EXPECTED_BUDGETS
        for seed in seeds
    }
    missing_configs = sorted(expected - observed)
    if missing_configs:
        issues.append(
            _issue("error", "missing_configs", "Missing DAG/baseline/budget configurations", missing_configs)
        )
    unexpected_configs = sorted(observed - expected)
    if unexpected_configs:
        issues.append(
            _issue("error", "unexpected_configs", "Unexpected DAG/baseline/budget configurations", unexpected_configs)
        )

    bad_runs = df[pd.to_numeric(df["workflow_runs"], errors="coerce") != repetitions]
    if not bad_runs.empty:
        issues.append(
            _issue(
                "error",
                "workflow_runs",
                f"All configurations must have workflow_runs={repetitions}",
                bad_runs[["dag", "baseline", "budget", "workflow_runs"]].to_dict("records"),
            )
        )

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

    provisioned = df[df["baseline"].isin(PROVISIONED_BASELINES)]
    bad_provisioned = provisioned[
        (pd.to_numeric(provisioned["total_warmup"], errors="coerce") != 0)
        | (
            pd.to_numeric(provisioned["provisioned_slots_total"], errors="coerce")
            .fillna(0)
            .le(0)
        )
    ]
    if not bad_provisioned.empty:
        issues.append(
            _issue(
                "error",
                "provisioned_semantics",
                "provisioned_concurrency must have zero warmup invocations and positive provisioned slots",
                bad_provisioned[
                    ["dag", "budget", "total_warmup", "provisioned_slots_total"]
                ].to_dict("records"),
            )
        )

    active = df[df["baseline"].isin(ACTIVE_PREWARM_BASELINES)].copy()
    active["budget_limit"] = (
        pd.to_numeric(active["budget"], errors="coerce")
        * pd.to_numeric(active["workflow_runs"], errors="coerce")
    )
    active["total_warmup_num"] = pd.to_numeric(active["total_warmup"], errors="coerce")
    over_budget = active[active["total_warmup_num"] > active["budget_limit"]]
    if not over_budget.empty:
        issues.append(
            _issue(
                "warning",
                "warmup_budget",
                "Some active prewarm baselines exceed budget x workflow_runs",
                over_budget[["dag", "baseline", "budget", "workflow_runs", "total_warmup", "budget_limit"]].to_dict("records"),
            )
        )

    planner_mismatch = []
    for row in df.itertuples(index=False):
        expected_planner = EXPECTED_EFFECTIVE_PLANNER.get(str(row.baseline))
        if expected_planner is not None and str(row.effective_planner) != expected_planner:
            planner_mismatch.append(
                {
                    "dag": row.dag,
                    "baseline": row.baseline,
                    "budget": int(row.budget),
                    "expected": expected_planner,
                    "actual": row.effective_planner,
                }
            )
    if planner_mismatch:
        issues.append(
            _issue("error", "effective_planner", "effective_planner values do not match expected mapping", planner_mismatch)
        )

    for column in LATENCY_COLUMNS:
        if column in df.columns:
            bad = df[pd.to_numeric(df[column], errors="coerce") < 0]
            if not bad.empty:
                issues.append(
                    _issue(
                        "error",
                        column,
                        f"{column} must not be negative",
                        bad[["dag", "baseline", "budget", column]].to_dict("records"),
                    )
                )
    for column in NONNEGATIVE_COLUMNS:
        if column in df.columns:
            bad = df[pd.to_numeric(df[column], errors="coerce") < 0]
            if not bad.empty:
                issues.append(
                    _issue(
                        "error",
                        column,
                        f"{column} must not be negative",
                        bad[["dag", "baseline", "budget", column]].to_dict("records"),
                    )
                )
    for column in RATIO_COLUMNS:
        if column in df.columns:
            values = pd.to_numeric(df[column], errors="coerce")
            bad = df[(values < 0) | (values > 1)]
            if not bad.empty:
                issues.append(
                    _issue(
                        "error",
                        column,
                        f"{column} must be in [0, 1]",
                        bad[["dag", "baseline", "budget", column]].to_dict("records"),
                    )
                )

    mint = df[df["baseline"] == "mint_markov_full"]
    if mint.empty:
        issues.append(_issue("error", "mint_presence", "mint_markov_full rows are missing"))
    elif pd.to_numeric(mint["delay_count"], errors="coerce").sum() <= 0:
        issues.append(
            _issue(
                "warning",
                "delay_count",
                "mint_markov_full delay_count is zero across the matrix; Delay evidence must come from the delay-shift supplement",
            )
        )
    return summary


def audit_adaptive_stress(
    results_dir: str | Path,
    repetitions: int = 10,
    seeds: list[int] | None = None,
) -> dict[str, Any]:
    results_dir = Path(results_dir)
    issues: list[dict[str, Any]] = []
    summary: dict[str, Any] = {"results_dir": str(results_dir)}
    manifest = _read_manifest(results_dir)
    resolved_seeds = seeds or list(manifest.get("seeds", [42]))
    _check_manifest(results_dir, repetitions, resolved_seeds, issues)
    matrix_summary = _check_matrix(results_dir, repetitions, resolved_seeds, issues)
    summary.update(matrix_summary)

    failed = _read_jsonl(results_dir / "failed_runs.jsonl")
    if failed:
        issues.append(_issue("error", "failed_runs", "failed_runs.jsonl is not empty", failed))
    skipped = _read_jsonl(results_dir / "skipped_runs.jsonl")
    if skipped:
        issues.append(_issue("error", "skipped_runs", "skipped_runs.jsonl is not empty", skipped))

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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit the MINT adaptive stress core matrix.")
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument(
        "--seeds",
        nargs="+",
        type=int,
        default=None,
        help="Expected branch seeds; defaults to the manifest seeds.",
    )
    parser.add_argument("--output-dir")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    results_dir = Path(args.results_dir)
    output_dir = Path(args.output_dir) if args.output_dir else results_dir / "audit"
    report = audit_adaptive_stress(
        results_dir,
        repetitions=args.repetitions,
        seeds=args.seeds,
    )
    json_path, txt_path = write_report(report, output_dir)
    print(f"Wrote {json_path}")
    print(f"Wrote {txt_path}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
