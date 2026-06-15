from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


METHOD_ORDER = [
    "path_aware_greedy",
    "mint_markov_offline",
    "mint_markov_no_long_horizon",
    "mint_markov_full",
    "oracle_path",
]
DISPLAY = {
    "path_aware_greedy": "Path-aware greedy",
    "mint_markov_offline": "MINT w/o runtime rescheduling",
    "mint_markov_no_long_horizon": "MINT w/o long-horizon benefit",
    "mint_markov_full": "MINT",
    "oracle_path": "Oracle",
}
COLORS = {
    "path_aware_greedy": "#4e79a7",
    "mint_markov_offline": "#b07aa1",
    "mint_markov_no_long_horizon": "#f28e2b",
    "mint_markov_full": "#d62728",
    "oracle_path": "#59a14f",
}
WORKLOAD_ORDER = ["deep_mixed", "greedy_trap", "wide_branch"]
WORKLOAD_DISPLAY = {
    "deep_mixed": "Deep mixed",
    "greedy_trap": "Branch-convergent",
    "wide_branch": "Wide branch",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot MINT ablation figures from summary_matrix.csv.")
    parser.add_argument("--matrix-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def setup_style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#333333",
            "axes.linewidth": 0.8,
            "axes.grid": True,
            "grid.color": "#dddddd",
            "grid.linewidth": 0.6,
            "grid.alpha": 0.65,
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "savefig.bbox": "tight",
        }
    )


def save_figure(fig: plt.Figure, output_dir: Path, stem: str) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for suffix in ("png", "pdf", "svg"):
        path = output_dir / f"{stem}.{suffix}"
        kwargs = {"dpi": 300} if suffix == "png" else {}
        fig.savefig(path, **kwargs)
        paths.append(path)
    plt.close(fig)
    return paths


def _load(matrix_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(matrix_csv)
    missing = [name for name in METHOD_ORDER if name not in set(df["baseline"].astype(str))]
    if missing:
        raise ValueError(f"summary matrix is missing ablation baselines: {', '.join(missing)}")
    df = df[df["baseline"].isin(METHOD_ORDER)].copy()
    df["method"] = df["baseline"].map(DISPLAY)
    df["workload"] = df["dag"].map(WORKLOAD_DISPLAY).fillna(df["dag"])
    return df


def _aggregate_by_workload(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(["dag", "workload", "baseline", "method"], as_index=False)
        .agg(
            avg_latency=("end_to_end_latency_ms_avg", "mean"),
            p95=("p95_latency_ms", "mean"),
            p99=("p99_latency_ms", "mean"),
            cold_starts=("cold_start_count", "sum"),
            total_warmup=("total_warmup", "sum"),
            useful_warmup=("useful_warmup", "sum"),
            wasted_warmup=("wasted_warmup", "sum"),
            useful_ratio=("useful_warmup_ratio", "mean"),
        )
    )
    grouped["_workload_order"] = grouped["dag"].map({name: idx for idx, name in enumerate(WORKLOAD_ORDER)}).fillna(99)
    grouped["_method_order"] = grouped["baseline"].map({name: idx for idx, name in enumerate(METHOD_ORDER)})
    return grouped.sort_values(["_workload_order", "_method_order"]).drop(columns=["_workload_order", "_method_order"])


def _aggregate_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["baseline", "method"], as_index=False)
        .agg(
            avg_latency=("end_to_end_latency_ms_avg", "mean"),
            p95=("p95_latency_ms", "mean"),
            p99=("p99_latency_ms", "mean"),
            cold_starts=("cold_start_count", "sum"),
            total_warmup=("total_warmup", "sum"),
            wasted_warmup=("wasted_warmup", "sum"),
            useful_ratio=("useful_warmup_ratio", "mean"),
        )
    )
    summary["_method_order"] = summary["baseline"].map({name: idx for idx, name in enumerate(METHOD_ORDER)})
    summary = summary.sort_values("_method_order").drop(columns="_method_order")
    return summary[["method", "avg_latency", "p95", "p99", "cold_starts", "total_warmup", "wasted_warmup", "useful_ratio"]]


def plot_p95_by_workload(by_workload: pd.DataFrame, output_dir: Path) -> list[Path]:
    workloads = [name for name in WORKLOAD_ORDER if name in set(by_workload["dag"])]
    pivot = by_workload.pivot(index="dag", columns="baseline", values="p95").reindex(workloads)
    x = np.arange(len(workloads))
    width = 0.15
    fig, ax = plt.subplots(figsize=(9.2, 4.2))
    for idx, baseline in enumerate(METHOD_ORDER):
        positions = x + (idx - (len(METHOD_ORDER) - 1) / 2) * width
        ax.bar(
            positions,
            pivot[baseline],
            width,
            label=DISPLAY[baseline],
            color=COLORS[baseline],
            edgecolor="#7f1d1d" if baseline == "mint_markov_full" else "#333333",
            linewidth=1.1 if baseline == "mint_markov_full" else 0.45,
        )
    ax.set_ylabel("P95 latency (ms)")
    ax.set_xticks(x, [WORKLOAD_DISPLAY.get(name, name) for name in workloads])
    ax.set_ylim(0, float(pivot.max().max()) * 1.18)
    ax.legend(ncol=3, frameon=False, loc="lower center", bbox_to_anchor=(0.5, 1.02))
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    return save_figure(fig, output_dir, "figure_ablation_p95_by_workload")


def plot_warmup_efficiency(by_workload: pd.DataFrame, output_dir: Path) -> list[Path]:
    method_summary = (
        by_workload.groupby(["baseline", "method"], as_index=False)
        .agg(useful_ratio=("useful_ratio", "mean"), wasted_warmup=("wasted_warmup", "sum"))
    )
    method_summary["_method_order"] = method_summary["baseline"].map({name: idx for idx, name in enumerate(METHOD_ORDER)})
    method_summary = method_summary.sort_values("_method_order")
    x = np.arange(len(method_summary))
    labels = method_summary["method"].tolist()

    fig, axes = plt.subplots(1, 2, figsize=(12.2, 4.0))
    axes[0].bar(x, method_summary["useful_ratio"], color=[COLORS[name] for name in method_summary["baseline"]], edgecolor="#333333", linewidth=0.5)
    axes[0].set_title("(a) Useful warmup ratio")
    axes[0].set_ylabel("Useful warmup ratio")
    axes[0].set_ylim(0, max(1.0, float(method_summary["useful_ratio"].max()) * 1.15))
    axes[0].set_xticks(x, labels, rotation=24, ha="right")

    axes[1].bar(x, method_summary["wasted_warmup"], color=[COLORS[name] for name in method_summary["baseline"]], edgecolor="#333333", linewidth=0.5)
    axes[1].set_title("(b) Wasted warmup count")
    axes[1].set_ylabel("Wasted warmup count")
    axes[1].set_ylim(0, float(method_summary["wasted_warmup"].max()) * 1.2 + 1)
    axes[1].set_xticks(x, labels, rotation=24, ha="right")
    for ax in axes:
        ax.grid(axis="y")
        ax.grid(axis="x", visible=False)
    fig.tight_layout()
    return save_figure(fig, output_dir, "figure_ablation_warmup_efficiency")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_style()
    matrix_csv = Path(args.matrix_csv)
    output_dir = Path(args.output_dir)
    df = _load(matrix_csv)
    by_workload = _aggregate_by_workload(df)
    summary = _aggregate_summary(df)

    output_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_dir / "table_ablation_summary.csv", index=False)
    by_workload.to_csv(output_dir / "table_ablation_by_workload.csv", index=False)

    generated = [output_dir / "table_ablation_summary.csv", output_dir / "table_ablation_by_workload.csv"]
    generated.extend(plot_p95_by_workload(by_workload, output_dir))
    generated.extend(plot_warmup_efficiency(by_workload, output_dir))
    for path in generated:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
