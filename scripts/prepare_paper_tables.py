from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare paper-ready CSV tables from a MINT matrix summary.")
    parser.add_argument("--matrix-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def _pick_baseline(df: pd.DataFrame, preferred: list[str]) -> str | None:
    available = set(df["baseline"].dropna().astype(str))
    for name in preferred:
        if name in available:
            return name
    return None


def _safe_reduction(reference: pd.Series, candidate: pd.Series) -> pd.Series:
    reference = pd.to_numeric(reference, errors="coerce")
    candidate = pd.to_numeric(candidate, errors="coerce")
    return (reference - candidate) / reference.replace(0, np.nan)


def _ensure_effective_planner(df: pd.DataFrame) -> pd.DataFrame:
    if "effective_planner" in df.columns:
        return df
    mapping = {
        "no_warmup": "none",
        "periodic_keepwarm": "periodic",
        "static_dag": "static",
        "orion_like": "orion_like",
        "mint_offline": "heuristic",
        "mint_full": "heuristic",
        "mint_markov_offline": "markov",
        "mint_markov_full": "markov",
    }
    df = df.copy()
    df["effective_planner"] = df["baseline"].map(mapping).fillna(df.get("planner_type", "unknown"))
    return df


def _ensure_unserved_intent_metric(df: pd.DataFrame) -> pd.DataFrame:
    if "unserved_intent_cold_start" in df.columns:
        return df
    df = df.copy()
    warnings.warn(
        "summary_matrix.csv has no unserved_intent_cold_start column; "
        "using legacy missed_warmup as a compatibility fill.",
        stacklevel=2,
    )
    df["unserved_intent_cold_start"] = df.get("missed_warmup", np.nan)
    return df


def _prepare_variability(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "dag",
        "baseline",
        "effective_planner",
        "budget",
        "n",
        "mean",
        "std",
        "min",
        "max",
        "p50",
        "p95",
        "p99",
        "ci95_low",
        "ci95_high",
        "output_dir",
    ]
    rows = []
    for _, row in df.iterrows():
        output_dir = Path(str(row.get("output_dir", "")))
        runs_path = output_dir / "runs.csv"
        if not runs_path.exists():
            warnings.warn(f"Missing runs.csv for variability table: {runs_path}")
            continue
        runs = pd.read_csv(runs_path)
        if "latency_ms" not in runs.columns or runs.empty:
            warnings.warn(f"Skipping runs.csv without latency_ms rows: {runs_path}")
            continue
        latencies = pd.to_numeric(runs["latency_ms"], errors="coerce").dropna()
        if latencies.empty:
            warnings.warn(f"Skipping runs.csv without numeric latency_ms: {runs_path}")
            continue
        n = len(latencies)
        mean = float(latencies.mean())
        std = float(latencies.std(ddof=1)) if n > 1 else 0.0
        margin = 1.96 * std / np.sqrt(n) if n > 0 else np.nan
        rows.append(
            {
                "dag": row.get("dag"),
                "baseline": row.get("baseline"),
                "effective_planner": row.get("effective_planner"),
                "budget": row.get("budget"),
                "n": n,
                "mean": mean,
                "std": std,
                "min": float(latencies.min()),
                "max": float(latencies.max()),
                "p50": float(np.percentile(latencies, 50)),
                "p95": float(np.percentile(latencies, 95)),
                "p99": float(np.percentile(latencies, 99)),
                "ci95_low": mean - margin,
                "ci95_high": mean + margin,
                "output_dir": str(output_dir),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def prepare_tables(matrix_csv: str | Path, output_dir: str | Path) -> dict[str, Path]:
    df = _ensure_unserved_intent_metric(_ensure_effective_planner(pd.read_csv(matrix_csv)))
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    mint_full = _pick_baseline(df, ["mint_markov_full", "mint_full"])
    offline = _pick_baseline(df, ["mint_markov_offline", "mint_offline"])

    group_cols = ["dag", "baseline", "effective_planner"]
    latency = (
        df.groupby(group_cols, as_index=False)
        .agg(
            end_to_end_latency_ms_avg=("end_to_end_latency_ms_avg", "mean"),
            p50_latency_ms=("p50_latency_ms", "mean"),
            p95_latency_ms=("p95_latency_ms", "mean"),
            p99_latency_ms=("p99_latency_ms", "mean"),
            cold_start_rate=("cold_start_rate", "mean"),
            cold_start_count=("cold_start_count", "sum"),
        )
        .sort_values(group_cols)
    )

    warmup = (
        df.groupby(["dag", "budget", "baseline", "effective_planner"], as_index=False)
        .agg(
            total_warmup=("total_warmup", "sum"),
            useful_warmup=("useful_warmup", "sum"),
            wasted_warmup=("wasted_warmup", "sum"),
            missed_warmup=("missed_warmup", "sum"),
            unserved_intent_cold_start=("unserved_intent_cold_start", "sum"),
            uncovered_cold_start=("uncovered_cold_start", "sum"),
            useful_warmup_ratio=("useful_warmup_ratio", "mean"),
            end_to_end_latency_ms_avg=("end_to_end_latency_ms_avg", "mean"),
        )
        .sort_values(["dag", "budget", "baseline"])
    )

    refs = warmup[["dag", "budget", "baseline", "total_warmup", "end_to_end_latency_ms_avg"]]
    static_ref = refs[refs["baseline"] == "static_dag"].rename(
        columns={"total_warmup": "static_dag_total_warmup", "end_to_end_latency_ms_avg": "static_dag_latency_ms_avg"}
    )[["dag", "budget", "static_dag_total_warmup", "static_dag_latency_ms_avg"]]
    no_warmup_ref = refs[refs["baseline"] == "no_warmup"].rename(
        columns={"end_to_end_latency_ms_avg": "no_warmup_latency_ms_avg"}
    )[["dag", "budget", "no_warmup_latency_ms_avg"]]
    offline_ref = pd.DataFrame(columns=["dag", "budget", "offline_total_warmup", "offline_latency_ms_avg"])
    if offline:
        offline_ref = refs[refs["baseline"] == offline].rename(
            columns={"total_warmup": "offline_total_warmup", "end_to_end_latency_ms_avg": "offline_latency_ms_avg"}
        )[["dag", "budget", "offline_total_warmup", "offline_latency_ms_avg"]]

    warmup = warmup.merge(static_ref, on=["dag", "budget"], how="left")
    warmup = warmup.merge(offline_ref, on=["dag", "budget"], how="left")
    warmup = warmup.merge(no_warmup_ref, on=["dag", "budget"], how="left")
    warmup["primary_mint_baseline"] = mint_full
    warmup["offline_reference_baseline"] = offline
    for col in [
        "mint_vs_static_dag_warmup_reduction_ratio",
        "mint_vs_offline_warmup_reduction_ratio",
        "mint_vs_no_warmup_latency_reduction_ratio",
        "mint_vs_static_dag_latency_reduction_ratio",
        "mint_vs_offline_latency_reduction_ratio",
    ]:
        warmup[col] = np.nan

    if mint_full:
        mint_mask = warmup["baseline"] == mint_full
        warmup.loc[mint_mask, "mint_vs_static_dag_warmup_reduction_ratio"] = _safe_reduction(
            warmup.loc[mint_mask, "static_dag_total_warmup"], warmup.loc[mint_mask, "total_warmup"]
        )
        warmup.loc[mint_mask, "mint_vs_offline_warmup_reduction_ratio"] = _safe_reduction(
            warmup.loc[mint_mask, "offline_total_warmup"], warmup.loc[mint_mask, "total_warmup"]
        )
        warmup.loc[mint_mask, "mint_vs_no_warmup_latency_reduction_ratio"] = _safe_reduction(
            warmup.loc[mint_mask, "no_warmup_latency_ms_avg"], warmup.loc[mint_mask, "end_to_end_latency_ms_avg"]
        )
        warmup.loc[mint_mask, "mint_vs_static_dag_latency_reduction_ratio"] = _safe_reduction(
            warmup.loc[mint_mask, "static_dag_latency_ms_avg"], warmup.loc[mint_mask, "end_to_end_latency_ms_avg"]
        )
        warmup.loc[mint_mask, "mint_vs_offline_latency_reduction_ratio"] = _safe_reduction(
            warmup.loc[mint_mask, "offline_latency_ms_avg"], warmup.loc[mint_mask, "end_to_end_latency_ms_avg"]
        )

    actions = (
        df.groupby(["dag", "baseline", "effective_planner"], as_index=False)
        .agg(
            execute_count=("execute_count", "sum"),
            delay_count=("delay_count", "sum"),
            cancel_count=("cancel_count", "sum"),
            replace_count=("replace_count", "sum"),
        )
        .sort_values(["dag", "baseline"])
    )

    budget = (
        df.groupby(["dag", "baseline", "effective_planner", "budget"], as_index=False)
        .agg(
            end_to_end_latency_ms_avg=("end_to_end_latency_ms_avg", "mean"),
            cold_start_rate=("cold_start_rate", "mean"),
            total_warmup=("total_warmup", "sum"),
            useful_warmup_ratio=("useful_warmup_ratio", "mean"),
        )
        .sort_values(["dag", "baseline", "budget"])
    )

    overall = (
        df.groupby(["baseline", "effective_planner"], as_index=False)
        .agg(
            average_latency=("end_to_end_latency_ms_avg", "mean"),
            p95=("p95_latency_ms", "mean"),
            p99=("p99_latency_ms", "mean"),
            cold_start_count=("cold_start_count", "sum"),
            cold_start_rate=("cold_start_rate", "mean"),
            total_warmup=("total_warmup", "sum"),
            wasted_warmup=("wasted_warmup", "sum"),
            missed_warmup=("missed_warmup", "sum"),
            unserved_intent_cold_start=("unserved_intent_cold_start", "sum"),
            useful_warmup_ratio=("useful_warmup_ratio", "mean"),
            execute_count=("execute_count", "sum"),
            cancel_count=("cancel_count", "sum"),
            replace_count=("replace_count", "sum"),
            delay_count=("delay_count", "sum"),
        )
        .sort_values(["baseline"])
    )

    improvement_rows = []
    if mint_full:
        overall_ref = overall.set_index("baseline")
        mint_row = overall_ref.loc[mint_full] if mint_full in overall_ref.index else None
        if mint_row is not None:
            for reference_name, label in [("no_warmup", "no_warmup"), ("static_dag", "static_dag"), (offline, "offline")]:
                if reference_name and reference_name in overall_ref.index:
                    ref = overall_ref.loc[reference_name]
                    improvement_rows.append(
                        {
                            "primary_mint_baseline": mint_full,
                            "reference_baseline": reference_name,
                            "reference_label": label,
                            "latency_reduction_ratio": _safe_reduction(pd.Series([ref["average_latency"]]), pd.Series([mint_row["average_latency"]])).iloc[0],
                            "warmup_reduction_ratio": _safe_reduction(pd.Series([ref["total_warmup"]]), pd.Series([mint_row["total_warmup"]])).iloc[0],
                            "cold_start_rate_reduction_ratio": _safe_reduction(pd.Series([ref["cold_start_rate"]]), pd.Series([mint_row["cold_start_rate"]])).iloc[0],
                            "mint_unserved_intent_cold_start": mint_row["unserved_intent_cold_start"],
                            "reference_unserved_intent_cold_start": ref["unserved_intent_cold_start"],
                        }
                    )
    improvement = pd.DataFrame(
        improvement_rows,
        columns=[
            "primary_mint_baseline",
            "reference_baseline",
            "reference_label",
            "latency_reduction_ratio",
            "warmup_reduction_ratio",
            "cold_start_rate_reduction_ratio",
            "mint_unserved_intent_cold_start",
            "reference_unserved_intent_cold_start",
        ],
    )
    variability = _prepare_variability(df)

    paths = {
        "latency": out / "table_latency_by_dag.csv",
        "warmup": out / "table_warmup_efficiency.csv",
        "actions": out / "table_action_counts.csv",
        "budget": out / "table_budget_sensitivity.csv",
        "overall": out / "table_overall_summary.csv",
        "improvement": out / "table_mint_improvement.csv",
        "variability": out / "table_run_variability.csv",
    }
    latency.to_csv(paths["latency"], index=False)
    warmup.to_csv(paths["warmup"], index=False)
    actions.to_csv(paths["actions"], index=False)
    budget.to_csv(paths["budget"], index=False)
    overall.to_csv(paths["overall"], index=False)
    improvement.to_csv(paths["improvement"], index=False)
    variability.to_csv(paths["variability"], index=False)
    return paths


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = prepare_tables(args.matrix_csv, args.output_dir)
    for path in paths.values():
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
