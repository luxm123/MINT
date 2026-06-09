from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_DIR = ROOT / "results" / "aws_adaptive_stress_main_20260607_141048"
FIXED_BASELINES = ["periodic_keepwarm", "static_dag", "orion_like"]
METHOD_ORDER = ["no_warmup", "best_fixed", "path_aware_greedy", "mint_markov_full", "oracle_path"]
BUDGET_METHOD_ORDER = ["best_fixed", "path_aware_greedy", "mint_markov_full", "oracle_path"]
WORKLOADS = ["wide_branch", "deep_mixed", "greedy_trap"]
DISPLAY = {
    "no_warmup": "No warmup",
    "best_fixed": "Best-fixed",
    "path_aware_greedy": "Path-aware greedy",
    "mint_markov_full": "MINT",
    "oracle_path": "Oracle",
    "periodic_keepwarm": "Periodic",
    "static_dag": "DAG-gain fixed",
    "orion_like": "Fixed look-ahead",
}
COLORS = {
    "no_warmup": "#9ca3af",
    "best_fixed": "#93c5fd",
    "path_aware_greedy": "#fbbf24",
    "mint_markov_full": "#fca5a5",
    "oracle_path": "#c4b5fd",
}
NOT_APPLICABLE = "\u2014"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot adaptive stress benchmark figures and derived tables.")
    parser.add_argument("--results-dir", default=str(DEFAULT_RESULTS_DIR))
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
            "axes.titlesize": 10.5,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "savefig.bbox": "tight",
        }
    )


def save_figure(fig: plt.Figure, figure_dir: Path, stem: str) -> list[Path]:
    figure_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for suffix in ("pdf", "svg", "png"):
        path = figure_dir / f"{stem}.{suffix}"
        kwargs = {"dpi": 300} if suffix == "png" else {}
        fig.savefig(path, **kwargs)
        paths.append(path)
    plt.close(fig)
    return paths


def _read_inputs(results_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary_path = results_dir / "summary_matrix.csv"
    paper_dir = results_dir / "paper_tables_clean"
    pareto_dir = results_dir / "pareto"
    required = [
        summary_path,
        paper_dir / "table_overall_summary.csv",
        paper_dir / "table_latency_by_dag.csv",
        pareto_dir / "pareto_latency_warmup.csv",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing adaptive stress input files: " + ", ".join(missing))
    # Load all declared inputs; pareto is not plotted directly but remains part of the scoped bundle.
    pd.read_csv(pareto_dir / "pareto_latency_warmup.csv")
    return (
        pd.read_csv(summary_path),
        pd.read_csv(paper_dir / "table_overall_summary.csv"),
        pd.read_csv(paper_dir / "table_latency_by_dag.csv"),
    )


def compute_best_fixed(summary: pd.DataFrame) -> pd.DataFrame:
    fixed = summary[summary["baseline"].isin(FIXED_BASELINES)].copy()
    if fixed.empty:
        return fixed
    fixed["_rank"] = fixed.groupby(["dag", "budget"])["p95_latency_ms"].rank(method="first", ascending=True)
    best = fixed[fixed["_rank"] == 1].drop(columns="_rank").copy()
    best["source_fixed_baseline"] = best["baseline"]
    best["baseline"] = "best_fixed"
    best["effective_planner"] = "best_fixed"
    return best


def build_adaptive_tables(summary: pd.DataFrame) -> dict[str, pd.DataFrame]:
    workloads = [name for name in WORKLOADS if name in set(summary["dag"])]
    workloads.extend(name for name in summary["dag"].drop_duplicates().tolist() if name not in workloads)
    best_fixed = compute_best_fixed(summary)
    combined = pd.concat([summary, best_fixed], ignore_index=True, sort=False)
    main = combined[combined["baseline"].isin(METHOD_ORDER)].copy()
    main["display_name"] = main["baseline"].map(DISPLAY)

    agg = (
        main.groupby(["baseline", "display_name"], as_index=False)
        .agg(
            average_latency=("end_to_end_latency_ms_avg", "mean"),
            p95=("p95_latency_ms", "mean"),
            p99=("p99_latency_ms", "mean"),
            cold_start_count=("cold_start_count", "sum"),
            cold_start_rate=("cold_start_rate", "mean"),
            total_warmup=("total_warmup", "sum"),
            useful_warmup=("useful_warmup", "sum"),
            wasted_warmup=("wasted_warmup", "sum"),
            useful_warmup_ratio=("useful_warmup_ratio", "mean"),
            unserved_intent_cold_start=("unserved_intent_cold_start", "sum"),
            execute_count=("execute_count", "sum"),
            cancel_count=("cancel_count", "sum"),
            replace_count=("replace_count", "sum"),
            delay_count=("delay_count", "sum"),
        )
    )
    agg["_order"] = agg["baseline"].map({name: idx for idx, name in enumerate(METHOD_ORDER)})
    agg = agg.sort_values("_order").drop(columns="_order")

    by_workload = (
        main.groupby(["dag", "baseline", "display_name"], as_index=False)
        .agg(
            average_latency=("end_to_end_latency_ms_avg", "mean"),
            p95=("p95_latency_ms", "mean"),
            p99=("p99_latency_ms", "mean"),
            cold_start_count=("cold_start_count", "sum"),
            cold_start_rate=("cold_start_rate", "mean"),
            total_warmup=("total_warmup", "sum"),
            useful_warmup=("useful_warmup", "sum"),
            wasted_warmup=("wasted_warmup", "sum"),
            useful_warmup_ratio=("useful_warmup_ratio", "mean"),
            unserved_intent_cold_start=("unserved_intent_cold_start", "sum"),
            execute_count=("execute_count", "sum"),
            cancel_count=("cancel_count", "sum"),
            replace_count=("replace_count", "sum"),
            delay_count=("delay_count", "sum"),
        )
    )
    by_workload["_workload_order"] = by_workload["dag"].map({name: idx for idx, name in enumerate(workloads)})
    by_workload["_method_order"] = by_workload["baseline"].map({name: idx for idx, name in enumerate(METHOD_ORDER)})
    by_workload = by_workload.sort_values(["_workload_order", "_method_order"]).drop(columns=["_workload_order", "_method_order"])

    budget = main[main["baseline"].isin(METHOD_ORDER)].copy()
    budget["_workload_order"] = budget["dag"].map({name: idx for idx, name in enumerate(workloads)})
    budget["_method_order"] = budget["baseline"].map({name: idx for idx, name in enumerate(METHOD_ORDER)})
    budget = budget.sort_values(["_workload_order", "_method_order", "budget"]).drop(columns=["_workload_order", "_method_order"])
    return {"main": agg, "by_workload": by_workload, "budget": budget, "workloads": workloads}


def write_tables(tables: dict[str, pd.DataFrame], figure_dir: Path) -> list[Path]:
    paths = {
        "main": figure_dir / "table_adaptive_main_summary.csv",
        "by_workload": figure_dir / "table_adaptive_by_workload.csv",
        "budget": figure_dir / "table_adaptive_budget_sensitivity.csv",
    }
    figure_dir.mkdir(parents=True, exist_ok=True)
    tables["main"].to_csv(paths["main"], index=False, encoding="utf-8-sig")
    tables["by_workload"].to_csv(paths["by_workload"], index=False, encoding="utf-8-sig")
    keep = [
        "dag",
        "budget",
        "baseline",
        "display_name",
        "p95_latency_ms",
        "end_to_end_latency_ms_avg",
        "p99_latency_ms",
        "cold_start_rate",
        "total_warmup",
        "wasted_warmup",
        "useful_warmup_ratio",
        "execute_count",
        "cancel_count",
        "replace_count",
        "delay_count",
        "source_fixed_baseline",
    ]
    available = [col for col in keep if col in tables["budget"].columns]
    tables["budget"][available].to_csv(paths["budget"], index=False, encoding="utf-8-sig")
    return [paths["main"], paths["by_workload"], paths["budget"]]


def write_baseline_definitions(figure_dir: Path) -> Path:
    rows = [
        {
            "display_name": "No warmup",
            "code_name": "no_warmup",
            "description": "No active prewarming.",
            "uses_dag": "no",
            "runtime_adaptive": "no",
        },
        {
            "display_name": "Best-fixed",
            "code_name": "best_fixed",
            "description": "Analysis-stage result that selects the lowest-P95 row among Periodic, DAG-gain fixed, and Fixed look-ahead for each workload-budget configuration.",
            "uses_dag": "mixed",
            "runtime_adaptive": "no",
        },
        {
            "display_name": "Path-aware greedy",
            "code_name": "path_aware_greedy",
            "description": "Simple online greedy prewarming using the realized path and current hot/cold state, without MINT's Markov policy or delay.",
            "uses_dag": "yes",
            "runtime_adaptive": "yes",
        },
        {
            "display_name": "MINT",
            "code_name": "mint_markov_full",
            "description": "Markov offline intent planning plus runtime adaptive scheduling.",
            "uses_dag": "yes",
            "runtime_adaptive": "yes",
        },
        {
            "display_name": "Oracle",
            "code_name": "oracle_path",
            "description": "Ideal path-aware upper bound with advance knowledge of the realized path; not a deployable fair baseline.",
            "uses_dag": "yes",
            "runtime_adaptive": "oracle",
        },
    ]
    path = figure_dir / "table_adaptive_baseline_definitions.csv"
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _bars(ax: plt.Axes, x: np.ndarray, values, baselines: list[str], ylabel: str) -> None:
    colors = [COLORS[name] for name in baselines]
    bars = ax.bar(x, values, color=colors, edgecolor="#333333", linewidth=0.5)
    for bar, baseline in zip(bars, baselines):
        if baseline == "mint_markov_full":
            bar.set_edgecolor("#7f1d1d")
            bar.set_linewidth(1.1)
        if baseline == "oracle_path":
            bar.set_edgecolor("#4c1d95")
            bar.set_linewidth(1.0)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)


def plot_figure1(tables: dict[str, pd.DataFrame], figure_dir: Path) -> list[Path]:
    view = tables["main"].set_index("baseline").reindex(METHOD_ORDER).reset_index()
    x = np.arange(len(METHOD_ORDER))
    labels = [DISPLAY[name] for name in METHOD_ORDER]
    useful = pd.to_numeric(view["useful_warmup"], errors="coerce").fillna(0)
    wasted = pd.to_numeric(view["wasted_warmup"], errors="coerce").fillna(0)

    fig, axes = plt.subplots(1, 3, figsize=(12.5, 3.8))
    for ax, title, column, ylabel in [
        (axes[0], "(a) Average latency", "average_latency", "Average latency (ms)"),
        (axes[1], "(b) P95 latency", "p95", "P95 latency (ms)"),
    ]:
        values = pd.to_numeric(view[column], errors="coerce")
        _bars(ax, x, values, METHOD_ORDER, ylabel)
        ax.set_title(title)
        ax.set_xticks(x, labels, rotation=24, ha="right")
        ax.set_ylim(0, float(values.max()) * 1.2)

    axes[2].bar(x, useful, color="#86efac", edgecolor="#333333", linewidth=0.5, label="Useful")
    axes[2].bar(x, wasted, bottom=useful, color="#fca5a5", edgecolor="#333333", linewidth=0.5, label="Wasted")
    axes[2].set_title("(c) Warmup count")
    axes[2].set_ylabel("Warmup count")
    axes[2].set_xticks(x, labels, rotation=24, ha="right")
    axes[2].set_ylim(0, float((useful + wasted).max()) * 1.25 + 1)
    axes[2].legend(frameon=False, loc="upper left")
    axes[2].grid(axis="y")
    axes[2].grid(axis="x", visible=False)
    fig.tight_layout()
    return save_figure(fig, figure_dir, "figure1_adaptive_overall_summary")


def plot_figure2(tables: dict[str, pd.DataFrame], figure_dir: Path) -> list[Path]:
    view = tables["by_workload"]
    workloads = tables["workloads"]
    pivot = view.pivot(index="dag", columns="baseline", values="p95").reindex(workloads)
    x = np.arange(len(workloads))
    width = 0.15
    fig, ax = plt.subplots(figsize=(8.8, 4.1))
    for idx, baseline in enumerate(METHOD_ORDER):
        positions = x + (idx - (len(METHOD_ORDER) - 1) / 2) * width
        ax.bar(
            positions,
            pivot[baseline],
            width,
            color=COLORS[baseline],
            edgecolor="#7f1d1d" if baseline == "mint_markov_full" else "#333333",
            linewidth=1.1 if baseline == "mint_markov_full" else 0.45,
            label=DISPLAY[baseline],
        )
    ax.set_ylabel("P95 latency (ms)")
    ax.set_xticks(x, [name.replace("_", " ") for name in workloads])
    ax.set_ylim(0, float(pivot.max().max()) * 1.2)
    ax.legend(ncol=5, frameon=False, loc="lower center", bbox_to_anchor=(0.5, 1.02))
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    return save_figure(fig, figure_dir, "figure2_workload_p95_latency")


def plot_figure3(tables: dict[str, pd.DataFrame], figure_dir: Path) -> list[Path]:
    budget = tables["budget"][tables["budget"]["baseline"].isin(BUDGET_METHOD_ORDER)]
    workloads = tables["workloads"]
    fig, axes = plt.subplots(1, len(workloads), figsize=(5.6 * len(workloads), 4.0), squeeze=False)
    axes = axes[0]
    markers = {"best_fixed": "o", "path_aware_greedy": "s", "mint_markov_full": "*", "oracle_path": "D"}
    for ax, workload in zip(axes, workloads):
        sub = budget[budget["dag"] == workload]
        for baseline in BUDGET_METHOD_ORDER:
            line = sub[sub["baseline"] == baseline].sort_values("budget")
            ax.plot(
                line["budget"],
                line["p95_latency_ms"],
                color=COLORS[baseline],
                marker=markers[baseline],
                markersize=10 if baseline == "mint_markov_full" else 5.5,
                linewidth=2.6 if baseline == "mint_markov_full" else 1.5,
                label=DISPLAY[baseline],
            )
        ax.set_title(f"({chr(ord('a') + workloads.index(workload))}) {workload}")
        ax.set_xlabel("Budget B")
        ax.set_ylabel("P95 latency (ms)")
        ax.set_xticks([1, 2, 3], ["B=1", "B=2", "B=3"])
        ax.grid(True)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, ncol=4, frameon=False, loc="upper center", bbox_to_anchor=(0.5, 1.05))
    fig.tight_layout()
    return save_figure(fig, figure_dir, "figure3_budget_sensitivity_p95")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_style()
    results_dir = Path(args.results_dir)
    if not results_dir.is_absolute():
        results_dir = ROOT / results_dir
    figure_dir = results_dir / "figures"
    summary, _overall, _latency = _read_inputs(results_dir)
    tables = build_adaptive_tables(summary)

    generated: list[Path] = []
    generated.extend(write_tables(tables, figure_dir))
    generated.append(write_baseline_definitions(figure_dir))
    generated.extend(plot_figure1(tables, figure_dir))
    generated.extend(plot_figure2(tables, figure_dir))
    generated.extend(plot_figure3(tables, figure_dir))
    for path in generated:
        print(path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
