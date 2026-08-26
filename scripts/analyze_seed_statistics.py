from __future__ import annotations

import argparse
import math
import random
from pathlib import Path

import numpy as np
import pandas as pd


METRICS = {
    "p95_latency_ms": "p95_latency_ms",
    "average_latency_ms": "end_to_end_latency_ms_avg",
    "cold_start_rate": "cold_start_rate",
    "useful_warmup_ratio": "useful_warmup_ratio",
}
MINT_BASELINE = "mint_markov_full"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed-level statistics and bootstrap confidence intervals.")
    parser.add_argument("--matrix-csv", required=True)
    parser.add_argument("--seeds", nargs="+", type=int, default=None)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--bootstrap-iters", type=int, default=2000)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=0, help="RNG seed for bootstrap resampling.")
    return parser.parse_args(argv)


def bootstrap_ci(
    values: list[float],
    *,
    iterations: int,
    alpha: float,
    rng: random.Random,
) -> tuple[float, float, float]:
    values = [float(value) for value in values if value is not None]
    if not values:
        return (float("nan"), float("nan"), float("nan"))
    mean = float(np.mean(values))
    if len(values) == 1:
        return (mean, mean, mean)
    samples = []
    for _ in range(iterations):
        resampled = [values[rng.randrange(len(values))] for _ in values]
        samples.append(float(np.mean(resampled)))
    low = float(np.percentile(samples, 100.0 * alpha / 2.0))
    high = float(np.percentile(samples, 100.0 * (1.0 - alpha / 2.0)))
    return (mean, low, high)


def _seeds_from_df(df: pd.DataFrame, args: argparse.Namespace) -> list[int]:
    if args.seeds:
        return list(args.seeds)
    if "seed" in df.columns:
        return sorted(int(value) for value in df["seed"].dropna().unique())
    return [0]


def build_statistics_table(
    df: pd.DataFrame,
    seeds: list[int],
    *,
    iterations: int,
    alpha: float,
    rng: random.Random,
) -> pd.DataFrame:
    rows = []
    for (dag, baseline, budget), group in df.groupby(["dag", "baseline", "budget"]):
        for metric_name, column in METRICS.items():
            per_seed = [
                float(group.loc[group["seed"] == seed, column].mean())
                for seed in seeds
                if not group.loc[group["seed"] == seed, column].empty
            ]
            mean, low, high = bootstrap_ci(
                per_seed,
                iterations=iterations,
                alpha=alpha,
                rng=rng,
            )
            rows.append(
                {
                    "dag": dag,
                    "baseline": baseline,
                    "budget": int(budget),
                    "metric": metric_name,
                    "mean": round(mean, 6),
                    "ci_low": round(low, 6),
                    "ci_high": round(high, 6),
                    "n_seeds": len(per_seed),
                }
            )
    return pd.DataFrame(rows)


def build_dominance_table(
    df: pd.DataFrame,
    seeds: list[int],
    *,
    iterations: int,
    alpha: float,
    rng: random.Random,
) -> pd.DataFrame:
    rows = []
    mint = df[df["baseline"] == MINT_BASELINE]
    others = [
        baseline
        for baseline in sorted(df["baseline"].unique())
        if baseline != MINT_BASELINE
    ]
    for (dag, budget), group in df.groupby(["dag", "budget"]):
        mint_group = mint[(mint["dag"] == dag) & (mint["budget"] == budget)]
        for baseline in others:
            other_group = group[group["baseline"] == baseline]
            if other_group.empty or mint_group.empty:
                continue
            mint_seeds = [
                float(mint_group.loc[mint_group["seed"] == seed, "p95_latency_ms"].mean())
                for seed in seeds
                if not mint_group.loc[mint_group["seed"] == seed, "p95_latency_ms"].empty
            ]
            other_seeds = [
                float(other_group.loc[other_group["seed"] == seed, "p95_latency_ms"].mean())
                for seed in seeds
                if not other_group.loc[other_group["seed"] == seed, "p95_latency_ms"].empty
            ]
            if not mint_seeds or not other_seeds:
                continue
            diffs = []
            for _ in range(iterations):
                mint_sample = np.mean(
                    [mint_seeds[rng.randrange(len(mint_seeds))] for _ in mint_seeds]
                )
                other_sample = np.mean(
                    [other_seeds[rng.randrange(len(other_seeds))] for _ in other_seeds]
                )
                diffs.append(float(mint_sample - other_sample))
            mean_diff = float(np.mean(diffs))
            low = float(np.percentile(diffs, 100.0 * alpha / 2.0))
            high = float(np.percentile(diffs, 100.0 * (1.0 - alpha / 2.0)))
            dominance_probability = float(np.mean([1.0 if diff < 0.0 else 0.0 for diff in diffs]))
            rows.append(
                {
                    "dag": dag,
                    "budget": int(budget),
                    "baseline": baseline,
                    "mint_vs_baseline_p95_diff_mean_ms": round(mean_diff, 6),
                    "ci_low": round(low, 6),
                    "ci_high": round(high, 6),
                    "mint_p95_lower_probability": round(dominance_probability, 6),
                }
            )
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    df = pd.read_csv(args.matrix_csv)
    seeds = _seeds_from_df(df, args)
    rng = random.Random(args.seed)
    stats = build_statistics_table(
        df,
        seeds,
        iterations=args.bootstrap_iters,
        alpha=args.alpha,
        rng=rng,
    )
    dominance = build_dominance_table(
        df,
        seeds,
        iterations=args.bootstrap_iters,
        alpha=args.alpha,
        rng=rng,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stats_path = output_dir / "seed_statistics.csv"
    dominance_path = output_dir / "pairwise_dominance.csv"
    stats.to_csv(stats_path, index=False, encoding="utf-8-sig")
    dominance.to_csv(dominance_path, index=False, encoding="utf-8-sig")
    print(f"Wrote {stats_path}")
    print(f"Wrote {dominance_path}")
    print(f"seeds={seeds} bootstrap_iters={args.bootstrap_iters}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
