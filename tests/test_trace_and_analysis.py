from __future__ import annotations

import random
from pathlib import Path

import pandas as pd

from mint.orion import OrionProfile, build_bundles, decide
from mint.trace_profile import (
    apply_trace_calibration,
    calibrate_branch_probabilities,
    load_trace_profile,
)
from mint.workloads import get_workload
from scripts.analyze_cost_latency import build_cost_latency, pareto_front
from scripts.analyze_seed_statistics import bootstrap_ci, build_dominance_table, build_statistics_table


def _write_synthetic_trace(tmp_path: Path) -> Path:
    rows = []
    for index in range(100):
        function = f"f{2 + (index % 4)}"
        rows.append(
            {
                "function": function,
                "durationMs": 100 + (index % 5) * 10,
                "maxMemoryMB": 128 if index % 3 else 256,
                "endTime": f"2023-01-01T00:{(index // 60):02d}:{(index % 60):02d}Z",
                "cold_start": 1 if index % 10 == 0 else 0,
            }
        )
    path = tmp_path / "trace.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_trace_profile_load_and_calibrate_wide_branch(tmp_path):
    path = _write_synthetic_trace(tmp_path)
    profile = load_trace_profile(path, source="synthetic")
    assert profile.total_invocations == 100
    assert len(profile.call_counts) == 4
    assert profile.duration_ms_quantiles
    assert profile.interarrival_sec_quantiles is not None
    assert profile.cold_start_rate is not None

    dag = get_workload("wide_branch")
    probabilities = calibrate_branch_probabilities(profile, dag)
    total = sum(probabilities["f1"].values())
    assert abs(total - 1.0) < 1e-9
    assert set(probabilities["f1"]) == {"f2", "f3", "f4", "f5"}

    config = {
        "experiment": {},
        "platform": {},
        "planner": {},
    }
    apply_trace_calibration(config, profile, dag)
    assert config["experiment"]["trace_calibration"]["branch_probabilities"]["f1"]
    assert config["platform"]["default_cold_start_ms"] > 250.0
    assert config["experiment"]["stage_gap_sec"] > 0.0


def test_trace_calibration_deep_mixed_sets_left_probability(tmp_path):
    path = _write_synthetic_trace(tmp_path)
    profile = load_trace_profile(path)
    dag = get_workload("deep_mixed")
    config = {"experiment": {}, "platform": {}, "planner": {}}
    apply_trace_calibration(config, profile, dag)
    assert "branch_probability_left" in config["planner"]
    left = float(config["planner"]["branch_probability_left"])
    assert 0.0 <= left <= 1.0


def test_bootstrap_ci_single_seed_is_identity():
    mean, low, high = bootstrap_ci(
        [100.0],
        iterations=100,
        alpha=0.05,
        rng=random.Random(0),
    )
    assert (mean, low, high) == (100.0, 100.0, 100.0)


def test_seed_statistics_and_dominance_tables():
    rows = []
    seeds = [1, 2, 3]
    for dag in ("wide_branch", "deep_mixed"):
        for budget in (1, 2):
            for seed in seeds:
                for baseline, offset in (
                    ("mint_markov_full", 0.0),
                    ("static_dag", 300.0),
                    ("oracle_path", -50.0),
                ):
                    rows.append(
                        {
                            "dag": dag,
                            "baseline": baseline,
                            "budget": budget,
                            "seed": seed,
                            "p95_latency_ms": 800 + offset + seed,
                            "end_to_end_latency_ms_avg": 700 + offset + seed,
                            "cold_start_rate": 0.1,
                            "useful_warmup_ratio": 0.8,
                            "total_warmup": budget,
                            "provisioned_slots_total": 0,
                            "provisioned_duration_sec_total": 0.0,
                        }
                    )
    df = pd.DataFrame(rows)
    stats = build_statistics_table(
        df,
        seeds,
        iterations=50,
        alpha=0.05,
        rng=random.Random(0),
    )
    assert not stats.empty
    p95_mint = stats[
        (stats["dag"] == "wide_branch")
        & (stats["baseline"] == "mint_markov_full")
        & (stats["metric"] == "p95_latency_ms")
        & (stats["budget"] == 1)
    ].iloc[0]
    assert abs(p95_mint["mean"] - 802.0) < 1e-6
    assert p95_mint["ci_low"] <= p95_mint["mean"] <= p95_mint["ci_high"]

    dominance = build_dominance_table(
        df,
        seeds,
        iterations=50,
        alpha=0.05,
        rng=random.Random(0),
    )
    mint_vs_static = dominance[
        (dominance["dag"] == "wide_branch")
        & (dominance["baseline"] == "static_dag")
        & (dominance["budget"] == 1)
    ].iloc[0]
    assert mint_vs_static["mint_p95_lower_probability"] == 1.0
    mint_vs_oracle = dominance[
        (dominance["dag"] == "wide_branch")
        & (dominance["baseline"] == "oracle_path")
        & (dominance["budget"] == 1)
    ].iloc[0]
    assert mint_vs_oracle["mint_p95_lower_probability"] == 0.0


def test_cost_latency_and_pareto_separate_provisioned_cost_axis():
    rows = []
    for baseline, warmup, provisioned_sec in (
        ("mint_markov_full", 2, 0.0),
        ("static_dag", 3, 0.0),
        ("provisioned_concurrency", 0, 5.0),
    ):
        rows.append(
            {
                "dag": "wide_branch",
                "baseline": baseline,
                "budget": 2,
                "seed": 42,
                "p95_latency_ms": 500,
                "end_to_end_latency_ms_avg": 400,
                "cold_start_rate": 0.05,
                "total_warmup": warmup,
                "provisioned_slots_total": 2 if baseline == "provisioned_concurrency" else 0,
                "provisioned_duration_sec_total": provisioned_sec,
            }
        )
    df = pd.DataFrame(rows)
    cost_latency = build_cost_latency(
        df,
        invocation_price_usd=2.0e-7,
        provisioned_price_usd_per_slot_sec=6.67e-6,
    )
    provisioned = cost_latency[
        cost_latency["baseline"] == "provisioned_concurrency"
    ].iloc[0]
    assert provisioned["cost_units"] == 5.0
    assert provisioned["cost_usd"] > 0.0
    mint = cost_latency[cost_latency["baseline"] == "mint_markov_full"].iloc[0]
    assert mint["cost_units"] == 2.0
    front = pareto_front(cost_latency)
    assert set(front["baseline"]) == {"mint_markov_full", "static_dag", "provisioned_concurrency"}
    assert front["pareto_front"].any()
