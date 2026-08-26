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
    *,
    rank_fallback: bool = True,
) -> dict[str, dict[str, float]]:
    """Normalize trace call counts of each branch successor into probabilities.

    Direct name match: when the trace contains functions named like the DAG's
    branch successors (synthetic or derived traces), probabilities are the
    normalized call counts of those names.

    Rank fallback (public traces): production serverless datasets (e.g.
    Azure) identify functions by opaque names, not by DAG node names.  When
    no successor name is present in the trace, the top-k most frequently
    called trace functions are mapped to the branch successors in frequency
    rank order, and their normalized frequencies become the branch
    probabilities.  This keeps the trace's real call-frequency skew in the
    microbenchmark profile.  The convention is deterministic and is recorded
    in the calibration provenance (`branch_mapping`).
    """
    result: dict[str, dict[str, float]] = {}
    for decision_node, _rule in dag.branch_rules.items():
        successors = dag.successors.get(decision_node, [])
        if not successors:
            continue
        counts = {
            successor: float(profile.call_counts.get(successor, 0.0))
            for successor in successors
        }
        total = sum(counts.values())
        if total > 0.0:
            probabilities = {
                successor: counts[successor] / total for successor in successors
            }
        elif rank_fallback and profile.call_counts:
            ranked = sorted(
                profile.call_counts.items(),
                key=lambda item: (-float(item[1]), str(item[0])),
            )
            top = ranked[: len(successors)]
            top_total = sum(float(count) for _, count in top)
            if top_total > 0.0:
                probabilities = {
                    successor: float(count) / top_total
                    for successor, (_, count) in zip(successors, top)
                }
            else:
                probabilities = {
                    successor: 1.0 / len(successors) for successor in successors
                }
        else:
            probabilities = {
                successor: 1.0 / len(successors) for successor in successors
            }
        result[decision_node] = probabilities
    return result


def flatten_branch_probabilities(
    branch_probabilities: dict[str, dict[str, float]],
) -> dict[str, float]:
    """Flatten {decision_node: {target: probability}} into {target: probability}.

    The Markov policy model consumes the flat per-branch map
    (planner.branch_probabilities); the per-decision-node structure is kept
    in experiment.trace_calibration for provenance.
    """
    return {
        target: float(probability)
        for mapping in branch_probabilities.values()
        for target, probability in mapping.items()
    }


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
    used_rank_fallback = any(
        all(profile.call_counts.get(successor, 0.0) == 0.0 for successor in dag.successors.get(decision_node, []))
        for decision_node in dag.branch_rules
    )
    calibration = {
        "source": profile.source,
        "branch_probabilities": {dag.name: branch_probabilities},
        "branch_mapping": (
            "rank_fallback_by_call_count" if used_rank_fallback else "direct_name_match"
        ),
    }
    existing = experiment.get("trace_calibration")
    if existing:
        existing.setdefault("branch_probabilities", {}).update(
            calibration["branch_probabilities"]
        )
        existing["branch_mapping"] = calibration["branch_mapping"]
    else:
        experiment["trace_calibration"] = calibration
    planner["branch_probabilities"] = flatten_branch_probabilities(
        branch_probabilities
    )

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
        experiment["trace_calibration"]["cold_start_rate"] = profile.cold_start_rate
    return config
