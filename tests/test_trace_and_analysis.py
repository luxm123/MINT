from __future__ import annotations

import random
import subprocess
import sys
from pathlib import Path

import pandas as pd

from mint.orion import OrionProfile, build_bundles, decide
from mint.trace_profile import (
    apply_trace_calibration,
    calibrate_branch_probabilities,
    load_aggregate_profile,
    load_trace_profile,
)
from mint.workloads import get_workload
from scripts import apply_trace_calibration as apply_trace_calibration_cli
from scripts import download_azure_trace as download_azure_trace_cli
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
    assert config["experiment"]["trace_calibration"]["branch_probabilities"]["wide_branch"]["f1"]
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


def test_calibrate_rank_fallback_maps_top_functions_by_frequency(tmp_path):
    rows = []
    counts = {"func_a": 40, "func_b": 30, "func_c": 20, "func_d": 10}
    for function, count in counts.items():
        for _ in range(count):
            rows.append({"function": function, "durationMs": 100, "endTime": "2023-01-01T00:00:00Z"})
    path = tmp_path / "opaque_trace.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    profile = load_trace_profile(path)

    dag = get_workload("wide_branch")
    probabilities = calibrate_branch_probabilities(profile, dag)
    mapping = probabilities["f1"]
    assert mapping == {"f2": 0.4, "f3": 0.3, "f4": 0.2, "f5": 0.1}

    deep = get_workload("deep_mixed")
    deep_mapping = calibrate_branch_probabilities(profile, deep)["f1"]
    assert abs(sum(deep_mapping.values()) - 1.0) < 1e-9
    # deep_mixed has two successors, so the top-2 trace functions are used.
    assert abs(deep_mapping["f2"] - 40.0 / 70.0) < 1e-9
    assert abs(deep_mapping["f3"] - 30.0 / 70.0) < 1e-9


def _write_aggregate_trace(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Write three CSVs matching the official AzureFunctionsDataset2019 schema."""
    invocations = pd.DataFrame(
        {
            "HashOwner": ["owner1", "owner1"],
            "HashApp": ["appA", "appB"],
            "HashFunction": ["f1", "f2"],
            "Trigger": ["http", "timer"],
            "1": [10, 30],
            "2": [20, 60],
            "3": [30, 90],
        }
    )
    durations = pd.DataFrame(
        {
            "HashOwner": ["owner1", "owner1"],
            "HashApp": ["appA", "appB"],
            "HashFunction": ["f1", "f2"],
            "Average": [100, 200],
            "Count": [60, 180],
            "Minimum": [50, 100],
            "Maximum": [500, 900],
            "percentile_Average_1": [80, 150],
            "percentile_Average_25": [90, 170],
            "percentile_Average_50": [100, 200],
            "percentile_Average_75": [120, 240],
            "percentile_Average_99": [300, 600],
            "percentile_Average_100": [500, 900],
        }
    )
    memory = pd.DataFrame(
        {
            "HashOwner": ["owner1", "owner1"],
            "HashApp": ["appA", "appB"],
            "SampleCount": [1440, 1440],
            "AverageAllocatedMb": [256, 512],
            "AverageAllocatedMb_pct1": [128, 256],
            "AverageAllocatedMb_pct5": [128, 256],
            "AverageAllocatedMb_pct25": [192, 384],
            "AverageAllocatedMb_pct50": [256, 512],
            "AverageAllocatedMb_pct75": [320, 640],
            "AverageAllocatedMb_pct95": [512, 1024],
            "AverageAllocatedMb_pct99": [640, 1280],
            "AverageAllocatedMb_pct100": [1024, 2048],
        }
    )
    invocations_path = tmp_path / "invocations_per_function_md.anon.d01.csv"
    durations_path = tmp_path / "function_durations_percentiles.anon.d01.csv"
    memory_path = tmp_path / "app_memory_percentiles.anon.d01.csv"
    invocations.to_csv(invocations_path, index=False)
    durations.to_csv(durations_path, index=False)
    memory.to_csv(memory_path, index=False)
    return invocations_path, durations_path, memory_path


def test_load_aggregate_profile_official_schema(tmp_path):
    invocations, durations, memory = _write_aggregate_trace(tmp_path)
    profile = load_aggregate_profile(
        invocations, durations, memory, source="azure-2019-d01"
    )
    assert profile.source == "azure-2019-d01"
    assert profile.total_invocations == 240
    assert profile.call_counts == {"appA:f1": 60.0, "appB:f2": 180.0}
    assert profile.duration_ms_quantiles == {
        "appA:f1": (80.0, 100.0, 300.0),
        "appB:f2": (150.0, 200.0, 600.0),
    }
    p10, p50, p90 = profile.memory_mb_quantiles
    assert p50 == 384.0
    assert p10 < p50 < p90
    assert profile.interarrival_sec_quantiles is not None
    assert profile.interarrival_sec_quantiles[1] == 0.75  # 60/80 per-minute median
    assert profile.cold_start_rate is None


def test_load_aggregate_profile_memory_optional(tmp_path):
    invocations, durations, _memory = _write_aggregate_trace(tmp_path)
    profile = load_aggregate_profile(invocations, durations, None)
    assert profile.memory_mb_quantiles == (128.0, 128.0, 128.0)
    assert profile.total_invocations == 240


def test_apply_trace_calibration_cli_trace_dir(tmp_path, capsys):
    trace_dir = tmp_path / "trace"
    trace_dir.mkdir()
    invocations, durations, memory = _write_aggregate_trace(trace_dir)
    config_path = tmp_path / "base.yaml"
    config_path.write_text(
        "aws:\n  lambda_functions:\n    f1: mint-f1\n    f2: mint-f2\n"
        "experiment:\n  dag: wide_branch\n  dry_run: false\n"
        "planner:\n  type: markov\nplatform:\n  default_retention_sec: 300\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "tracecal.yaml"
    rc = apply_trace_calibration_cli.main(
        [
            "--trace-dir",
            str(trace_dir),
            "--config",
            str(config_path),
            "--output",
            str(output_path),
        ]
    )
    assert rc == 0
    assert output_path.exists()
    assert str(invocations) in capsys.readouterr().out

    import yaml

    calibrated = yaml.safe_load(output_path.read_text(encoding="utf-8"))
    branch_probabilities = calibrated["experiment"]["trace_calibration"][
        "branch_probabilities"
    ]
    assert set(branch_probabilities) == {"wide_branch", "deep_mixed"}
    # Rank fallback: appB:f2 (180) and appA:f1 (60) are the top-2 functions.
    assert branch_probabilities["wide_branch"]["f1"] == {
        "f2": 0.75,
        "f3": 0.25,
    }
    assert abs(calibrated["planner"]["branch_probability_left"] - 0.75) < 1e-9
    assert calibrated["platform"]["default_warm_duration_ms"] == 150.0
    assert calibrated["platform"]["default_cold_start_ms"] == 442.0
    assert calibrated["experiment"]["stage_gap_sec"] == 0.75


def test_download_azure_trace_dry_run_prints_github_url(tmp_path, capsys):
    rc = download_azure_trace_cli.main(
        [
            "--output-dir",
            str(tmp_path),
            "--days",
            "1",
            "2",
            "--dry-run",
        ]
    )
    assert rc == 0
    output = capsys.readouterr().out
    assert "github.com/Azure/AzurePublicDataset/releases/download" in output
    assert "invocations_per_function_md.anon.d01.csv" in output
    assert "app_memory_percentiles.anon.d02.csv" in output


def test_apply_trace_calibration_cli_runs_as_script_from_repo_root():
    """python scripts/apply_trace_calibration.py must import the mint package."""
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "apply_trace_calibration.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "--trace-dir" in result.stdout
    assert "--output" in result.stdout


def test_controller_reads_per_dag_trace_calibration(tmp_path):
    from mint.controller import MintController

    dag = get_workload("deep_mixed")
    config = {
        "aws": {"lambda_functions": {node: f"mint-{node}" for node in dag.nodes}},
        "experiment": {
            "baseline": "no_warmup",
            "warmup_budget": 2,
            "output_dir": str(tmp_path / "out"),
            "trace_calibration": {
                "branch_probabilities": {
                    "deep_mixed": {"f1": {"f2": 0.9, "f3": 0.1}},
                    "wide_branch": {"f1": {"f2": 0.25, "f3": 0.25, "f4": 0.25, "f5": 0.25}},
                }
            },
        },
        "platform": {"default_retention_sec": 300, "default_cold_start_ms": 800},
        "planner": {"type": "heuristic"},
    }
    controller = MintController(config, dag=dag, baseline="no_warmup", dry_run=True)
    probabilities = controller._profile_call_probability()
    assert probabilities["f2"] == 0.9
    assert probabilities["f3"] == 0.1
    assert probabilities["f6"] == 1.0


def test_apply_trace_calibration_cli_writes_calibrated_config(tmp_path):
    rows = []
    counts = {"func_a": 40, "func_b": 30, "func_c": 20, "func_d": 10}
    for function, count in counts.items():
        for _ in range(count):
            rows.append({"function": function, "durationMs": 120, "endTime": "2023-01-01T00:00:00Z"})
    trace_path = tmp_path / "trace.csv"
    pd.DataFrame(rows).to_csv(trace_path, index=False)

    config_path = tmp_path / "base.yaml"
    config_path.write_text(
        "aws:\n  lambda_functions:\n    f1: mint-f1\n    f2: mint-f2\n"
        "experiment:\n  dag: wide_branch\n  dry_run: false\n"
        "planner:\n  type: markov\nplatform:\n  default_retention_sec: 300\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "tracecal.yaml"
    rc = apply_trace_calibration_cli.main(
        [
            "--trace",
            str(trace_path),
            "--config",
            str(config_path),
            "--output",
            str(output_path),
        ]
    )
    assert rc == 0
    assert output_path.exists()
    import yaml

    calibrated = yaml.safe_load(output_path.read_text(encoding="utf-8"))
    branch_probabilities = calibrated["experiment"]["trace_calibration"]["branch_probabilities"]
    assert set(branch_probabilities) == {"wide_branch", "deep_mixed"}
    assert branch_probabilities["wide_branch"]["f1"] == {
        "f2": 0.4,
        "f3": 0.3,
        "f4": 0.2,
        "f5": 0.1,
    }
    # The Markov multi-branch consumer gets the flat wide_branch map.
    assert calibrated["planner"]["branch_probabilities"] == {
        "f2": 0.4,
        "f3": 0.3,
        "f4": 0.2,
        "f5": 0.1,
    }


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
