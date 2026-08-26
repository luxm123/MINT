"""Trace-driven calibration for the MINT workload parameters.

The MINT workloads are DAG microbenchmarks; their branch probabilities, stage
durations, arrival spacing and cold-start model can be calibrated from a
serverless invocation trace so the benchmark is representative of observed
serverless traffic instead of hand-set constants.

The primary supported source is the Microsoft Azure Functions public dataset
(https://github.com/Azure/AzurePublicDataset), whose per-invocation CSVs
contain function name, duration, memory and timestamp fields.  The loader is
column-name tolerant so other traces with similar fields can be used as-is.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from mint.workloads import WorkflowDAG


@dataclass
class TraceProfile:
    source: str
    total_invocations: int
    call_counts: dict[str, int]
    duration_ms_quantiles: dict[str, tuple[float, float, float]]
    memory_mb_quantiles: tuple[float, float, float]
    interarrival_sec_quantiles: tuple[float, float, float] | None
    cold_start_rate: float | None = None


def _first_matching(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def load_trace_profile(path: str | Path, source: str = "") -> TraceProfile:
    """Load a per-invocation trace CSV into a compact calibration profile."""
    path = Path(path)
    df = pd.read_csv(path)
    df.columns = [str(column).strip().lower() for column in df.columns]
    columns = set(df.columns)

    function_column = _first_matching(
        columns,
        ("function", "function_name", "functionname", "functionid", "function_id"),
    )
    duration_column = _first_matching(
        columns,
        ("duration", "duration_ms", "durationms", "duration_millis"),
    )
    memory_column = _first_matching(
        columns,
        ("memory", "memory_mb", "memorymb", "maxmemorymb", "max_memory_mb"),
    )
    timestamp_column = _first_matching(
        columns,
        ("timestamp", "endtime", "starttime", "end_time", "start_time"),
    )
    if function_column is None:
        raise ValueError("trace CSV is missing a function-name column")
    if duration_column is None:
        raise ValueError("trace CSV is missing a duration column")

    df[function_column] = df[function_column].astype(str)
    df[duration_column] = pd.to_numeric(df[duration_column], errors="coerce")
    if memory_column is not None:
        df[memory_column] = pd.to_numeric(df[memory_column], errors="coerce")
    call_counts = df[function_column].value_counts().to_dict()
    duration_quantiles = {}
    for function_name, group in df.groupby(function_column):
        values = group[duration_column].dropna()
        if values.empty:
            continue
        duration_quantiles[function_name] = tuple(
            float(value)
            for value in np.percentile(values, [10, 50, 90])
        )

    if memory_column is not None:
        memory_values = df[memory_column].dropna()
        memory_quantiles = (
            tuple(float(value) for value in np.percentile(memory_values, [10, 50, 90]))
            if not memory_values.empty
            else (128.0, 128.0, 128.0)
        )
    else:
        memory_quantiles = (128.0, 128.0, 128.0)

    interarrival_quantiles = None
    if timestamp_column is not None:
        timestamps = df[timestamp_column].astype(str).tolist()
        try:
            parsed = pd.to_datetime(timestamps, utc=True).sort_values()
            if len(parsed) >= 2:
                gaps = parsed.diff().dropna().total_seconds().to_numpy()
                gaps = gaps[gaps >= 0]
                if gaps.size:
                    interarrival_quantiles = tuple(
                        float(value) for value in np.percentile(gaps, [10, 50, 90])
                    )
        except (ValueError, TypeError):
            interarrival_quantiles = None

    cold_start_rate = None
    cold_column = _first_matching(columns, ("cold_start", "coldstart"))
    if cold_column is not None:
        cold_values = pd.to_numeric(df[cold_column], errors="coerce").dropna()
        if not cold_values.empty:
            cold_start_rate = float(cold_values.mean())

    return TraceProfile(
        source=source or str(path),
        total_invocations=int(len(df)),
        call_counts=call_counts,
        duration_ms_quantiles=duration_quantiles,
        memory_mb_quantiles=memory_quantiles,
        interarrival_sec_quantiles=interarrival_quantiles,
        cold_start_rate=cold_start_rate,
    )


def calibrate_branch_probabilities(
    profile: TraceProfile,
    dag: WorkflowDAG,
) -> dict[str, dict[str, float]]:
    """Normalize trace call counts of each branch successor into probabilities."""
    result: dict[str, dict[str, float]] = {}
    for decision_node, _rule in dag.branch_rules.items():
        successors = dag.successors.get(decision_node, [])
        counts = {
            successor: float(profile.call_counts.get(successor, 0.0))
            for successor in successors
        }
        total = sum(counts.values())
        if total <= 0.0:
            probabilities = {
                successor: 1.0 / len(successors) for successor in successors
            }
        else:
            probabilities = {
                successor: counts[successor] / total for successor in successors
            }
        result[decision_node] = probabilities
    return result


def apply_trace_calibration(
    config: dict[str, Any],
    profile: TraceProfile,
    dag: WorkflowDAG,
) -> dict[str, Any]:
    """Write trace-calibrated parameters into an experiment config (in place)."""
    experiment = config.setdefault("experiment", {})
    platform = config.setdefault("platform", {})
    planner = config.setdefault("planner", {})

    branch_probabilities = calibrate_branch_probabilities(profile, dag)
    calibration = {
        "source": profile.source,
        "branch_probabilities": branch_probabilities,
    }
    experiment["trace_calibration"] = calibration
    planner["branch_probabilities"] = dict(branch_probabilities)

    for decision_node, mapping in branch_probabilities.items():
        if dag.name in {"branch", "mixed", "deep_mixed"}:
            planner["branch_probability_left"] = float(
                mapping.get("f2", mapping.get("left", 0.5))
            )

    overall_durations = [
        quantiles[1]
        for quantiles in profile.duration_ms_quantiles.values()
    ]
    if overall_durations:
        platform["default_warm_duration_ms"] = round(
            float(np.mean(overall_durations)), 3
        )
    memory_p50 = float(profile.memory_mb_quantiles[1])
    # Documented heuristic: larger memory reduces cold-start latency.  Replace
    # with measured AWS cold-start data when available.
    platform["default_cold_start_ms"] = round(
        250.0 + 0.5 * memory_p50, 3
    )
    if profile.interarrival_sec_quantiles is not None:
        experiment["stage_gap_sec"] = round(
            max(0.05, float(profile.interarrival_sec_quantiles[1])), 3
        )
    if profile.cold_start_rate is not None:
        experiment.setdefault("trace_calibration", calibration)[
            "cold_start_rate"
        ] = profile.cold_start_rate
    return config
