from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


PROVISIONED_BASELINE = "provisioned_concurrency"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cost-latency analysis with an explicit pricing model.")
    parser.add_argument("--matrix-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--invocation-price-usd",
        type=float,
        default=2.0e-7,
        help="USD per warmup invocation at the base memory tier (128MB ~100ms).",
    )
    parser.add_argument(
        "--provisioned-price-usd-per-slot-sec",
        type=float,
        default=6.67e-6,
        help="USD per provisioned-concurrency slot per second (placeholder, must be updated for the chosen region/memory).",
    )
    return parser.parse_args(argv)


def build_cost_latency(
    df: pd.DataFrame,
    *,
    invocation_price_usd: float,
    provisioned_price_usd_per_slot_sec: float,
) -> pd.DataFrame:
    rows = []
    for (dag, baseline, budget, seed), group in df.groupby(
        ["dag", "baseline", "budget", "seed"]
    ):
        row = group.iloc[0]
        if baseline == PROVISIONED_BASELINE:
            cost_units = float(row.get("provisioned_duration_sec_total", 0.0))
            cost_usd = cost_units * provisioned_price_usd_per_slot_sec
        else:
            cost_units = float(row.get("total_warmup", 0.0))
            cost_usd = cost_units * invocation_price_usd
        rows.append(
            {
                "dag": dag,
                "baseline": baseline,
                "budget": int(budget),
                "seed": int(seed),
                "cost_units": cost_units,
                "cost_usd": cost_usd,
                "p95_latency_ms": float(row.get("p95_latency_ms", 0.0)),
                "average_latency_ms": float(row.get("end_to_end_latency_ms_avg", 0.0)),
                "cold_start_rate": float(row.get("cold_start_rate", 0.0)),
            }
        )
    out = pd.DataFrame(rows)
    agg = (
        out.groupby(["dag", "baseline", "budget"], as_index=False)
        .agg(
            cost_units=("cost_units", "mean"),
            cost_usd=("cost_usd", "mean"),
            p95_latency_ms=("p95_latency_ms", "mean"),
            average_latency_ms=("average_latency_ms", "mean"),
            cold_start_rate=("cold_start_rate", "mean"),
        )
    )
    return agg.sort_values(["dag", "cost_units", "p95_latency_ms"])


def pareto_front(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for dag, group in df.groupby("dag"):
        dominated: set[int] = set()
        for index_a, a in group.iterrows():
            for index_b, b in group.iterrows():
                if index_a == index_b:
                    continue
                if (
                    b["cost_units"] <= a["cost_units"]
                    and b["p95_latency_ms"] <= a["p95_latency_ms"]
                    and (
                        b["cost_units"] < a["cost_units"]
                        or b["p95_latency_ms"] < a["p95_latency_ms"]
                    )
                ):
                    dominated.add(index_a)
                    break
        for index, row in group.iterrows():
            rows.append(
                {
                    "dag": row["dag"],
                    "baseline": row["baseline"],
                    "budget": int(row["budget"]),
                    "cost_units": row["cost_units"],
                    "cost_usd": row["cost_usd"],
                    "p95_latency_ms": row["p95_latency_ms"],
                    "average_latency_ms": row["average_latency_ms"],
                    "cold_start_rate": row["cold_start_rate"],
                    "pareto_front": index not in dominated,
                }
            )
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    df = pd.read_csv(args.matrix_csv)
    cost_latency = build_cost_latency(
        df,
        invocation_price_usd=args.invocation_price_usd,
        provisioned_price_usd_per_slot_sec=args.provisioned_price_usd_per_slot_sec,
    )
    front = pareto_front(cost_latency)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cost_path = output_dir / "cost_latency.csv"
    front_path = output_dir / "cost_latency_pareto.csv"
    cost_latency.to_csv(cost_path, index=False, encoding="utf-8-sig")
    front.to_csv(front_path, index=False, encoding="utf-8-sig")
    print(f"Wrote {cost_path}")
    print(f"Wrote {front_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
