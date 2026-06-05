from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare paper-ready CSV tables from a MINT matrix summary.")
    parser.add_argument("--matrix-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def _ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.where(denominator != 0, 0) / denominator.where(denominator != 0, 1)


def prepare_tables(matrix_csv: str | Path, output_dir: str | Path) -> dict[str, Path]:
    df = pd.read_csv(matrix_csv)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    group_cols = ["dag", "baseline"]
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
        df.groupby(["dag", "budget", "baseline"], as_index=False)
        .agg(
            total_warmup=("total_warmup", "sum"),
            useful_warmup=("useful_warmup", "sum"),
            wasted_warmup=("wasted_warmup", "sum"),
            missed_warmup=("missed_warmup", "sum"),
            uncovered_cold_start=("uncovered_cold_start", "sum"),
            useful_warmup_ratio=("useful_warmup_ratio", "mean"),
            end_to_end_latency_ms_avg=("end_to_end_latency_ms_avg", "mean"),
        )
        .sort_values(["dag", "budget", "baseline"])
    )

    static_ref = warmup[warmup["baseline"] == "static_dag"][["dag", "budget", "total_warmup"]].rename(
        columns={"total_warmup": "static_dag_total_warmup"}
    )
    offline_ref = warmup[warmup["baseline"] == "mint_offline"][["dag", "budget", "total_warmup"]].rename(
        columns={"total_warmup": "mint_offline_total_warmup"}
    )
    no_warmup_ref = warmup[warmup["baseline"] == "no_warmup"][["dag", "budget", "end_to_end_latency_ms_avg"]].rename(
        columns={"end_to_end_latency_ms_avg": "no_warmup_latency_ms_avg"}
    )
    warmup = warmup.merge(static_ref, on=["dag", "budget"], how="left")
    warmup = warmup.merge(offline_ref, on=["dag", "budget"], how="left")
    warmup = warmup.merge(no_warmup_ref, on=["dag", "budget"], how="left")
    warmup["mint_vs_static_dag_warmup_reduction_ratio"] = 0.0
    warmup["mint_vs_mint_offline_warmup_reduction_ratio"] = 0.0
    warmup["mint_vs_no_warmup_latency_reduction_ratio"] = 0.0
    mint_mask = warmup["baseline"] == "mint_full"
    warmup.loc[mint_mask, "mint_vs_static_dag_warmup_reduction_ratio"] = 1.0 - _ratio(
        warmup.loc[mint_mask, "total_warmup"], warmup.loc[mint_mask, "static_dag_total_warmup"].fillna(0)
    )
    warmup.loc[mint_mask, "mint_vs_mint_offline_warmup_reduction_ratio"] = 1.0 - _ratio(
        warmup.loc[mint_mask, "total_warmup"], warmup.loc[mint_mask, "mint_offline_total_warmup"].fillna(0)
    )
    warmup.loc[mint_mask, "mint_vs_no_warmup_latency_reduction_ratio"] = 1.0 - _ratio(
        warmup.loc[mint_mask, "end_to_end_latency_ms_avg"], warmup.loc[mint_mask, "no_warmup_latency_ms_avg"].fillna(0)
    )

    actions = (
        df.groupby(["dag", "baseline"], as_index=False)
        .agg(
            execute_count=("execute_count", "sum"),
            delay_count=("delay_count", "sum"),
            cancel_count=("cancel_count", "sum"),
            replace_count=("replace_count", "sum"),
        )
        .sort_values(["dag", "baseline"])
    )

    budget = (
        df.groupby(["dag", "baseline", "budget"], as_index=False)
        .agg(
            end_to_end_latency_ms_avg=("end_to_end_latency_ms_avg", "mean"),
            cold_start_rate=("cold_start_rate", "mean"),
            total_warmup=("total_warmup", "sum"),
            useful_warmup_ratio=("useful_warmup_ratio", "mean"),
        )
        .sort_values(["dag", "baseline", "budget"])
    )

    paths = {
        "latency": out / "table_latency_by_dag.csv",
        "warmup": out / "table_warmup_efficiency.csv",
        "actions": out / "table_action_counts.csv",
        "budget": out / "table_budget_sensitivity.csv",
    }
    latency.to_csv(paths["latency"], index=False)
    warmup.to_csv(paths["warmup"], index=False)
    actions.to_csv(paths["actions"], index=False)
    budget.to_csv(paths["budget"], index=False)
    return paths


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = prepare_tables(args.matrix_csv, args.output_dir)
    for path in paths.values():
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
