from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare Pareto plot data for latency/warmup tradeoffs.")
    parser.add_argument("--matrix-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def _per_warmup(numerator: pd.Series, warmups: pd.Series) -> pd.Series:
    warmups = pd.to_numeric(warmups, errors="coerce").replace(0, np.nan)
    return pd.to_numeric(numerator, errors="coerce") / warmups


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
    pareto = pareto.sort_values(["dag", "budget", "baseline"])

    latency_path = out / "pareto_latency_warmup.csv"
    p95_path = out / "pareto_p95_warmup.csv"
    report_path = out / "pareto_report.txt"
    pareto.to_csv(latency_path, index=False)
    pareto[["dag", "baseline", "budget", "p95_latency_ms", "total_warmup", "p95_per_warmup", "cold_start_rate", "wasted_warmup"]].to_csv(
        p95_path, index=False
    )

    lines = [
        f"Rows: {len(pareto)}",
        "Pareto data uses total_warmup as cost and latency/cold_start_rate as performance axes.",
        "Rows with total_warmup=0 use NaN for latency_per_warmup and p95_per_warmup.",
        "",
    ]
    if "mint_markov_full" in set(pareto["baseline"]):
        mint = pareto[pareto["baseline"] == "mint_markov_full"]
        lines.append(f"mint_markov_full rows: {len(mint)}")
        lines.append(f"mint_markov_full avg latency: {float(mint['average_latency'].mean()):.3f}")
        lines.append(f"mint_markov_full total warmup: {float(mint['total_warmup'].sum()):.3f}")
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
