from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = ROOT / "results" / "final_analysis"
FIGURE_DIR = ANALYSIS_DIR / "figures"

MAIN_BASELINES = [
    "no_warmup",
    "periodic_keepwarm",
    "static_dag",
    "orion_like",
    "mint_markov_full",
]
ABLATION_BASELINES = ["mint_markov_offline", "mint_markov_full"]
DAG_ORDER = ["chain", "fanout", "branch", "join", "wide_branch", "deep_mixed"]
LABELS = {
    "no_warmup": "No warmup",
    "periodic_keepwarm": "Periodic",
    "static_dag": "Static DAG",
    "orion_like": "ORION-like",
    "mint_markov_offline": "MINT offline",
    "mint_markov_full": "MINT full",
}
COLORS = {
    "no_warmup": "#6b7280",
    "periodic_keepwarm": "#4e79a7",
    "static_dag": "#f28e2b",
    "orion_like": "#59a14f",
    "mint_markov_offline": "#b07aa1",
    "mint_markov_full": "#d62728",
}
NOT_APPLICABLE = "\u2014"
OLD_FIGURE_STEMS = [
    "figure1_overall_latency_warmup",
    "figure2_dag_p95_latency",
    "figure3_cost_latency_pareto",
    "figure4_mixed_supplement",
    "figure5_runtime_actions",
    "figure6_delay_shift_stress_test",
    "figure1_cost_latency_tradeoff",
    "figure2_branch_mixed_p95",
    "figure3_warmup_efficiency",
    "figure1_standard_dag_benchmark",
    "figure2_dag_level_p95",
    "figure3_mixed_supplement",
    "figureS1_cost_latency_tradeoff",
]


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


def cleanup_old_figures() -> None:
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    for stem in OLD_FIGURE_STEMS:
        for suffix in ("pdf", "svg", "png"):
            path = FIGURE_DIR / f"{stem}.{suffix}"
            if path.exists():
                path.unlink()
    stale_table = FIGURE_DIR / "table_mint_ablation.csv"
    if stale_table.exists():
        stale_table.unlink()


def save_figure(fig: plt.Figure, stem: str) -> list[Path]:
    paths = []
    for suffix in ("pdf", "svg", "png"):
        path = FIGURE_DIR / f"{stem}.{suffix}"
        kwargs = {"dpi": 300} if suffix == "png" else {}
        fig.savefig(path, **kwargs)
        paths.append(path)
    plt.close(fig)
    return paths


def labels_for(baselines: list[str]) -> list[str]:
    return [LABELS.get(name, name) for name in baselines]


def ordered_view(df: pd.DataFrame, baselines: list[str]) -> pd.DataFrame:
    return df[df["baseline"].isin(baselines)].set_index("baseline").reindex(baselines).reset_index()


def annotate_bars(ax: plt.Axes, bars, values: pd.Series | np.ndarray, fmt: str = "{:.0f}", pad_ratio: float = 0.015) -> None:
    max_value = float(np.nanmax(values)) if len(values) else 0.0
    pad = max(max_value * pad_ratio, 1.0)
    for bar, value in zip(bars, values):
        if pd.isna(value):
            continue
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + pad,
            fmt.format(value),
            ha="center",
            va="bottom",
            fontsize=7.5,
        )


def four_panel_benchmark(overall: pd.DataFrame, experiment_type: str, title: str, stem: str) -> list[Path]:
    view = ordered_view(overall[overall["experiment_type"] == experiment_type], MAIN_BASELINES)
    x = np.arange(len(MAIN_BASELINES))
    colors = [COLORS[name] for name in MAIN_BASELINES]
    panels = [
        ("(a) Average latency", "average_latency_ms", "Latency (ms)", "{:.0f}"),
        ("(b) P95 latency", "p95_latency_ms_avg", "Latency (ms)", "{:.0f}"),
        ("(c) Total warmup", "total_warmup", "Count", "{:.0f}"),
        ("(d) Wasted warmup", "wasted_warmup", "Count", "{:.0f}"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(10.0, 6.2))
    for ax, (panel_title, column, ylabel, fmt) in zip(axes.flat, panels):
        values = pd.to_numeric(view[column], errors="coerce")
        bars = ax.bar(x, values, color=colors, edgecolor="#333333", linewidth=0.45)
        for bar, baseline in zip(bars, MAIN_BASELINES):
            if baseline == "mint_markov_full":
                bar.set_hatch("//")
                bar.set_linewidth(0.8)
        annotate_bars(ax, bars, values, fmt=fmt)
        ax.set_title(panel_title)
        ax.set_ylabel(ylabel)
        ax.set_xticks(x, labels_for(MAIN_BASELINES), rotation=25, ha="right")
        ax.set_ylim(0, float(values.max()) * 1.18 + 1)
        ax.grid(axis="y")
        ax.grid(axis="x", visible=False)
    fig.suptitle(title, y=1.02, fontsize=12)
    fig.tight_layout()
    return save_figure(fig, stem)


def dag_grouped_bar(latency: pd.DataFrame, column: str, ylabel: str, title: str, stem: str) -> list[Path]:
    main = latency[(latency["experiment_type"] == "main experiment") & (latency["baseline"].isin(MAIN_BASELINES))]
    present_dags = [dag for dag in DAG_ORDER if dag in set(main["dag"])]
    pivot = main.pivot(index="dag", columns="baseline", values=column).reindex(present_dags)
    baselines = [name for name in MAIN_BASELINES if name in pivot.columns]
    x = np.arange(len(present_dags))
    width = 0.15

    fig, ax = plt.subplots(figsize=(9.2, 4.4))
    for index, baseline in enumerate(baselines):
        positions = x + (index - (len(baselines) - 1) / 2) * width
        bars = ax.bar(
            positions,
            pivot[baseline],
            width,
            label=LABELS.get(baseline, baseline),
            color=COLORS[baseline],
            edgecolor="#333333",
            linewidth=0.4,
        )
        if baseline == "mint_markov_full":
            for bar in bars:
                bar.set_hatch("//")
                bar.set_linewidth(0.8)
    ax.set_title(title)
    ax.set_xlabel("DAG")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x, [name.replace("_", " ").title() for name in present_dags])
    ax.set_ylim(0, float(pivot.max().max()) * 1.18)
    ax.legend(ncol=5, frameon=False, loc="lower center", bbox_to_anchor=(0.5, 1.02))
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    return save_figure(fig, stem)


def figureS1_cost_latency_tradeoff(overall: pd.DataFrame) -> list[Path]:
    order = {baseline: index for index, baseline in enumerate(MAIN_BASELINES)}
    view = overall[
        overall["experiment_type"].isin(["main experiment", "mixed supplement"])
        & overall["baseline"].isin(MAIN_BASELINES)
    ].copy()
    view["_order"] = view["baseline"].map(order)
    view = view.sort_values(["experiment_type", "_order"])
    fig, ax = plt.subplots(figsize=(6.2, 4.0))
    markers = {"main experiment": "o", "mixed supplement": "s"}
    for experiment_type, group in view.groupby("experiment_type", sort=False):
        for _, row in group.iterrows():
            baseline = row["baseline"]
            marker = "*" if baseline == "mint_markov_full" else markers.get(experiment_type, "o")
            size = 150 if baseline == "mint_markov_full" else 55
            ax.scatter(
                row["total_warmup"],
                row["average_latency_ms"],
                marker=marker,
                s=size,
                color=COLORS[baseline],
                edgecolor="#111111" if baseline == "mint_markov_full" else "white",
                linewidth=0.7,
                alpha=0.92,
                label=f"{LABELS[baseline]} ({experiment_type.replace(' experiment', '').replace(' supplement', '')})",
            )
    handles, labels = ax.get_legend_handles_labels()
    dedup = dict(zip(labels, handles))
    ax.set_title("Cost-latency tradeoff")
    ax.set_xlabel("Total warmup")
    ax.set_ylabel("Average latency (ms)")
    ax.legend(dedup.values(), dedup.keys(), frameon=False, fontsize=7, loc="center left", bbox_to_anchor=(1.02, 0.5))
    ax.grid(True)
    return save_figure(fig, "figureS1_cost_latency_tradeoff")


def table_runtime_actions(actions: pd.DataFrame, delay: pd.DataFrame) -> Path:
    rows: list[dict[str, object]] = []
    for experiment_type, label in [("main experiment", "main MINT full"), ("mixed supplement", "mixed MINT full")]:
        subset = actions[(actions["experiment_type"] == experiment_type) & (actions["baseline"] == "mint_markov_full")]
        rows.append(
            {
                "summary": label,
                "baseline": "mint_markov_full",
                "execute_count": int(subset["execute_count"].sum()),
                "cancel_count": int(subset["cancel_count"].sum()),
                "replace_count": int(subset["replace_count"].sum()),
                "delay_count": int(subset["delay_count"].sum()),
                "delayed_execute_count": NOT_APPLICABLE,
                "served_after_delay_count": NOT_APPLICABLE,
                "cold_start_count": NOT_APPLICABLE,
            }
        )

    for _, row in delay.iterrows():
        baseline = row["baseline"]
        rows.append(
            {
                "summary": f"delay-shift {baseline}",
                "baseline": baseline,
                "execute_count": NOT_APPLICABLE,
                "cancel_count": NOT_APPLICABLE,
                "replace_count": NOT_APPLICABLE,
                "delay_count": int(row.get("delay_count", 0)),
                "delayed_execute_count": int(row.get("delayed_execute_count", 0)),
                "served_after_delay_count": int(row.get("served_after_delay_count", 0)),
                "cold_start_count": int(row.get("cold_start_count", 0)),
            }
        )

    path = FIGURE_DIR / "table_runtime_actions.csv"
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def table_baseline_definitions() -> Path:
    rows = [
        {
            "baseline": "no_warmup",
            "dag_aware": "no",
            "runtime_adaptive": "no",
            "description": "No prewarming; cold starts are uncovered by design.",
        },
        {
            "baseline": "periodic_keepwarm",
            "dag_aware": "no",
            "runtime_adaptive": "no",
            "description": "Industrial keep-warm style baseline that rotates through functions under the same warmup budget.",
        },
        {
            "baseline": "static_dag",
            "dag_aware": "yes",
            "runtime_adaptive": "no",
            "description": "Static DAG-aware offline prewarming plan with no runtime cancel, replace, or delay.",
        },
        {
            "baseline": "orion_like",
            "dag_aware": "yes",
            "runtime_adaptive": "no",
            "description": "ORION-style DAG-aware right-prewarming approximation with fixed look-ahead; not a complete ORION reproduction and excludes right-sizing and bundling.",
        },
        {
            "baseline": "mint_markov_offline",
            "dag_aware": "yes",
            "runtime_adaptive": "no",
            "description": "Markov offline policy analyzer used to generate warmup intents without runtime adaptation.",
        },
        {
            "baseline": "mint_markov_full",
            "dag_aware": "yes",
            "runtime_adaptive": "yes",
            "description": "Markov intent planning plus runtime adaptive scheduling with cancel, replace, and delay decisions.",
        },
    ]
    path = FIGURE_DIR / "table_baseline_definitions.csv"
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def table_mint_ablation(overall: pd.DataFrame) -> Path:
    rows = []
    for experiment_type in ["main experiment", "mixed supplement"]:
        subset = overall[
            (overall["experiment_type"] == experiment_type)
            & (overall["baseline"].isin(ABLATION_BASELINES))
        ].set_index("baseline")
        for baseline in ABLATION_BASELINES:
            if baseline not in subset.index:
                continue
            row = subset.loc[baseline]
            rows.append(
                {
                    "experiment_type": experiment_type,
                    "baseline": baseline,
                    "runtime_adaptive": "yes" if baseline == "mint_markov_full" else "no",
                    "average_latency_ms": row["average_latency_ms"],
                    "p95_latency_ms_avg": row["p95_latency_ms_avg"],
                    "total_warmup": row["total_warmup"],
                    "wasted_warmup": row["wasted_warmup"],
                    "useful_warmup_ratio_overall": row["useful_warmup_ratio_overall"],
                    "cancel_count": row["cancel_count"],
                    "replace_count": row["replace_count"],
                }
            )
    path = FIGURE_DIR / "table_mint_ablation.csv"
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def main() -> int:
    setup_style()
    cleanup_old_figures()

    overall = pd.read_csv(ANALYSIS_DIR / "final_overall_summary.csv")
    latency = pd.read_csv(ANALYSIS_DIR / "final_latency_by_dag.csv")
    actions = pd.read_csv(ANALYSIS_DIR / "final_action_counts.csv")
    delay = pd.read_csv(ANALYSIS_DIR / "final_delay_shift_summary.csv")

    generated: list[Path] = []
    generated.extend(
        dag_grouped_bar(
            latency,
            "p95_latency_ms_avg",
            "P95 latency (ms)",
            "P95 latency across representative DAG workflows",
            "figure1_dag_workflow_p95",
        )
    )
    generated.extend(
        dag_grouped_bar(
            latency,
            "total_warmup",
            "Total warmup",
            "Warmup cost across representative DAG workflows",
            "figure2_dag_workflow_warmup",
        )
    )
    generated.append(table_runtime_actions(actions, delay))
    generated.append(table_baseline_definitions())

    for path in generated:
        print(path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
