from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd


REFERENCE_BASELINES = {
    "static": "static_dag",
    "orion_like": "orion_like",
    "periodic": "periodic_keepwarm",
    "offline": "mint_markov_offline",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare Pareto plot data for latency/warmup tradeoffs.")
    parser.add_argument("--matrix-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def _per_warmup(numerator: pd.Series, warmups: pd.Series) -> pd.Series:
    warmups = pd.to_numeric(warmups, errors="coerce").replace(0, np.nan)
    return pd.to_numeric(numerator, errors="coerce") / warmups


def _safe_reduction(reference: pd.Series, candidate: pd.Series) -> pd.Series:
    reference = pd.to_numeric(reference, errors="coerce").replace(0, np.nan)
    candidate = pd.to_numeric(candidate, errors="coerce")
    return (reference - candidate) / reference


def _pareto_flags(df: pd.DataFrame, latency_col: str) -> tuple[list[bool], list[str]]:
    efficient = []
    dominated_by = []
    for row in df.itertuples(index=False):
        row_cost = float(row.total_warmup)
        row_latency = float(getattr(row, latency_col))
        dominators = []
        for other in df.itertuples(index=False):
            if other == row:
                continue
            other_cost = float(other.total_warmup)
            other_latency = float(getattr(other, latency_col))
            no_worse = other_cost <= row_cost and other_latency <= row_latency
            strictly_better = other_cost < row_cost or other_latency < row_latency
            if no_worse and strictly_better:
                dominators.append(f"{other.baseline}:B{other.budget}")
        efficient.append(not dominators)
        dominated_by.append(";".join(dominators))
    return efficient, dominated_by


def _add_reference_deltas(pareto: pd.DataFrame) -> pd.DataFrame:
    result = pareto.copy()
    for label, baseline in REFERENCE_BASELINES.items():
        ref = result[result["baseline"] == baseline][["dag", "budget", "average_latency", "p95_latency_ms", "total_warmup"]].rename(
            columns={
                "average_latency": f"{label}_average_latency",
                "p95_latency_ms": f"{label}_p95_latency",
                "total_warmup": f"{label}_total_warmup",
            }
        )
        result = result.merge(ref, on=["dag", "budget"], how="left")
        result[f"latency_delta_vs_{label}"] = result["average_latency"] - result[f"{label}_average_latency"]
        if label == "static":
            result["p95_delta_vs_static"] = result["p95_latency_ms"] - result[f"{label}_p95_latency"]
        result[f"warmup_reduction_vs_{label}"] = _safe_reduction(result[f"{label}_total_warmup"], result["total_warmup"])
    result = result.rename(
        columns={
            "latency_delta_vs_static": "latency_delta_vs_static",
            "warmup_reduction_vs_static": "warmup_reduction_vs_static",
            "latency_delta_vs_orion_like": "latency_delta_vs_orion_like",
            "warmup_reduction_vs_orion_like": "warmup_reduction_vs_orion_like",
            "latency_delta_vs_periodic": "latency_delta_vs_periodic",
            "warmup_reduction_vs_periodic": "warmup_reduction_vs_periodic",
            "latency_delta_vs_offline": "latency_delta_vs_offline",
            "warmup_reduction_vs_offline": "warmup_reduction_vs_offline",
        }
    )
    return result


def prepare_pareto(matrix_csv: str | Path, output_dir: str | Path) -> dict[str, Path]:
    df = pd.read_csv(matrix_csv)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    pareto = df[
        [
            "dag",
            "baseline",
            "budget",
            "end_to_end_latency_ms_avg",
            "p95_latency_ms",
            "p99_latency_ms",
            "total_warmup",
            "cold_start_rate",
            "wasted_warmup",
        ]
    ].rename(columns={"end_to_end_latency_ms_avg": "average_latency"})
    pareto["latency_per_warmup"] = _per_warmup(pareto["average_latency"], pareto["total_warmup"])
    pareto["p95_per_warmup"] = _per_warmup(pareto["p95_latency_ms"], pareto["total_warmup"])

    latency_flags = []
    p95_flags = []
    dominated_by = []
    for _, group in pareto.groupby("dag", sort=False):
        group = group.reset_index()
        flags, dominators = _pareto_flags(group, "average_latency")
        p95_group_flags, _ = _pareto_flags(group, "p95_latency_ms")
        latency_flags.extend(zip(group["index"], flags))
        p95_flags.extend(zip(group["index"], p95_group_flags))
        dominated_by.extend(zip(group["index"], dominators))
    pareto["is_pareto_efficient_latency"] = False
    pareto["is_pareto_efficient_p95"] = False
    pareto["dominated_by"] = ""
    for idx, value in latency_flags:
        pareto.loc[idx, "is_pareto_efficient_latency"] = value
    for idx, value in p95_flags:
        pareto.loc[idx, "is_pareto_efficient_p95"] = value
    for idx, value in dominated_by:
        pareto.loc[idx, "dominated_by"] = value

    pareto = _add_reference_deltas(pareto)
    pareto = pareto.sort_values(["dag", "budget", "baseline"])

    latency_path = out / "pareto_latency_warmup.csv"
    p95_path = out / "pareto_p95_warmup.csv"
    report_path = out / "pareto_report.txt"
    pareto.to_csv(latency_path, index=False)
    pareto[
        [
            "dag",
            "baseline",
            "budget",
            "p95_latency_ms",
            "total_warmup",
            "p95_per_warmup",
            "is_pareto_efficient_p95",
            "cold_start_rate",
            "wasted_warmup",
        ]
    ].to_csv(p95_path, index=False)

    lines = [
        f"Rows: {len(pareto)}",
        "Pareto data uses total_warmup as cost and average_latency or p95_latency_ms as performance.",
        "A point is dominated if another point has no more warmups and no worse latency, with at least one strictly better dimension.",
        "latency_per_warmup and p95_per_warmup are retained for inspection but are not the primary Pareto criterion.",
        "Rows with total_warmup=0 use NaN for latency_per_warmup and p95_per_warmup.",
        "",
    ]
    for dag, group in pareto.groupby("dag"):
        efficient = group[group["is_pareto_efficient_latency"]]
        lines.append(f"{dag}: {len(efficient)} latency-efficient points")
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return {"latency": latency_path, "p95": p95_path, "report": report_path}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = prepare_pareto(args.matrix_csv, args.output_dir)
    for path in paths.values():
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
