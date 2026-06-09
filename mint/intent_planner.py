from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mint.utils import new_id
from mint.workloads import WorkflowDAG


@dataclass(frozen=True)
class WarmupIntent:
    intent_id: str
    logical_name: str
    function_name: str
    planned_time_sec: float
    window_start_sec: float
    window_end_sec: float
    priority: int
    offline_gain: float
    stage: int
    criticality: float


def _node_call_probability(dag: WorkflowDAG, node: str) -> float:
    if dag.name == "branch" and node in {"f2", "f3", "f4", "f5"}:
        return 0.5
    if dag.name == "mixed" and node in {"f2", "f3"}:
        return 0.5
    if dag.name == "wide_branch" and node in {"f2", "f3", "f4", "f5"}:
        return 0.25
    if dag.name == "deep_mixed" and node in {"f2", "f3", "f4", "f5"}:
        return 0.5
    if dag.name == "greedy_trap" and node in {"f2", "f3", "f4"}:
        return 1.0 / 3.0
    return 1.0


def _greedy_trap_offline_gain(node: str, stage: int, cold_ms: float, warm_ms: float) -> float | None:
    # The trap profile makes early branch candidates look locally attractive to a
    # one-step greedy policy, while the converged suffix is better for a
    # finite-horizon policy under a small warmup budget.
    local_branch_gain = {
        "f2": cold_ms * 3.10 - warm_ms,
        "f3": cold_ms * 3.00 - warm_ms,
        "f4": cold_ms * 2.90 - warm_ms,
    }
    long_horizon_gain = {
        "f5": cold_ms * 0.95 - warm_ms,
        "f6": cold_ms * 0.88 - warm_ms,
        "f7": cold_ms * 0.78 - warm_ms,
        "f8": cold_ms * 0.68 - warm_ms,
    }
    entry_gain = {"f1": cold_ms * 0.70 - warm_ms}
    values = {**entry_gain, **local_branch_gain, **long_horizon_gain}
    if node not in values:
        return None
    return values[node] - stage * 5.0


def plan_heuristic_intents(dag: WorkflowDAG, config: dict[str, Any]) -> list[WarmupIntent]:
    """Create offline warmup intents.

    This heuristic is a clear replacement point for a future Markov Policy Planner.
    The interface keeps all fields needed by a policy optimizer: state stage,
    validity window, call probability proxy, criticality, and expected gain.
    """
    aws_functions = config.get("aws", {}).get("lambda_functions", {})
    platform = config.get("platform", {})
    default_retention = float(platform.get("default_retention_sec", 300))
    cold_ms = float(platform.get("default_cold_start_ms", 800))
    warm_ms = float(platform.get("default_warm_duration_ms", 100))
    stage_gap_sec = float(config.get("experiment", {}).get("stage_gap_sec", 1.0))

    stages = dag.stages()
    downstream = dag.downstream_counts()
    max_downstream = max(downstream.values() or [1])
    intents: list[WarmupIntent] = []

    for node in dag.nodes:
        stage = stages.get(node, 0)
        planned_time = max(0.0, stage * stage_gap_sec - 0.2)
        criticality = 1.0 + (downstream[node] / max(max_downstream, 1))
        p_call = _node_call_probability(dag, node)
        p_cold = 0.8 if stage == 0 else 0.6
        validity = min(1.0, default_retention / max(default_retention, planned_time + 1.0))
        offline_gain = (p_call * p_cold * validity * criticality * cold_ms) - warm_ms
        if dag.name == "greedy_trap":
            offline_gain = _greedy_trap_offline_gain(node, stage, cold_ms, warm_ms) or offline_gain
        intents.append(
            WarmupIntent(
                intent_id=new_id("intent"),
                logical_name=node,
                function_name=aws_functions.get(node, node),
                planned_time_sec=planned_time,
                window_start_sec=max(0.0, planned_time - default_retention),
                window_end_sec=planned_time + default_retention,
                priority=int(round(criticality * 100)),
                offline_gain=round(offline_gain, 3),
                stage=stage,
                criticality=round(criticality, 3),
            )
        )

    return sorted(intents, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))


def plan_intents(dag: WorkflowDAG, config: dict[str, Any]) -> list[WarmupIntent]:
    planner_type = config.get("planner", {}).get("type", "heuristic")
    if planner_type == "markov":
        from mint.markov_policy import MarkovPolicyAnalyzer

        analyzer = MarkovPolicyAnalyzer(dag, config, budget=int(config.get("experiment", {}).get("warmup_budget", 1)))
        return analyzer.generate_intents()
    if planner_type != "heuristic":
        raise ValueError(f"Unsupported planner.type: {planner_type}")
    return plan_heuristic_intents(dag, config)
