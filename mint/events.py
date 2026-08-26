from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from mint.utils import utc_now_iso


@dataclass
class EventBase:
    event_type: str
    run_id: str
    timestamp: str = field(default_factory=utc_now_iso)
    workflow_index: int = -1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class InvocationEvent(EventBase):
    function_name: str = ""
    logical_name: str = ""
    invocation_type: str = "real"
    cold_start: bool = False
    request_id: str = ""
    execution_environment_id: str = ""
    latency_ms: float = 0.0
    function_duration_ms: float = 0.0
    stage: int = 0
    status: str = "ok"
    error_type: str = ""
    error_message: str = ""
    observed_branch: str = ""


@dataclass
class WarmupEvent(EventBase):
    function_name: str = ""
    logical_name: str = ""
    intent_id: str = ""
    action: str = "execute"
    # Compatibility metric: a successful on-path warmup that completed before
    # its explicitly recorded fairness deadline. This deliberately does not
    # claim that AWS reused the same execution environment; that is derived
    # separately from non-empty environment UUIDs and the real-call cold flag.
    useful: bool = False
    target_hit: bool = False
    ready_before_deadline: bool = False
    readiness_deadline_type: str = ""
    ready_before_arrival: bool | None = None
    ready_before_node_demand: bool | None = None
    # Deprecated compatibility alias for ready_before_deadline. Older result
    # readers expect this field, although an initial warmup's deadline is the
    # planned workflow arrival rather than the target node's invocation.
    ready_before_demand: bool = False
    action_reason: str = ""
    gain: float = 0.0
    invocation_type: str = "warmup"
    cold_start: bool = False
    request_id: str = ""
    execution_environment_id: str = ""
    latency_ms: float = 0.0
    function_duration_ms: float = 0.0
    status: str = "ok"
    error_type: str = ""
    error_message: str = ""
    overlap_duration_ms: float = 0.0
    blocking_wait_ms: float = 0.0
    submit_to_collect_ms: float = 0.0
    warmup_wall_ms: float = 0.0
    missed_at_arrival: bool = False
    missed_at_node_demand: bool = False
    # Deprecated compatibility alias paired with ready_before_demand.
    missed_at_demand: bool = False


@dataclass
class SchedulerDecision(EventBase):
    intent_id: str = ""
    function_name: str = ""
    logical_name: str = ""
    action: str = "execute"
    action_reason: str = ""
    gain: float = 0.0
    planned_time_sec: float = 0.0
    decision_phase: str = "initial"
    model_history_size: int = 0
    branch_probabilities: str = "{}"
    supersedes_intent_id: str = ""
    decision_node: str = ""


@dataclass
class BranchModelEvent(EventBase):
    decision_phase: str = "initial"
    history_size: int = 0
    branch_counts: str = "{}"
    branch_probabilities: str = "{}"
    observed_branch: str = ""
    decision_node: str = ""


@dataclass
class IntentLifecycleEvent(EventBase):
    intent_id: str = ""
    function_name: str = ""
    logical_name: str = ""
    state_before: str = ""
    state_after: str = ""
    action: str = ""
    reason: str = ""
    reserved_budget: int = 0
    consumed_budget: int = 0
    actual_call_submitted: bool = False
    supersedes_intent_id: str = ""
    decision_phase: str = "initial"
    accepted: bool = True
    submission_lateness_ms: float = 0.0
    submission_offset_ms: float = 0.0
    transition_seq: int = 0


@dataclass
class WorkflowRunSummary(EventBase):
    dag: str = ""
    baseline: str = ""
    planner_type: str = "heuristic"
    latency_ms: float = 0.0
    cold_start_count: int = 0
    warmup_count: int = 0
    provisioned_slots: int = 0
    provisioned_duration_sec: float = 0.0
    reserved_budget: int = 0
    consumed_budget: int = 0
    budget_limit: int = 0
    unused_budget: int = 0
    warmup_error_count: int = 0
    scheduler_error_count: int = 0
    scheduler_status: str = "ok"
    status: str = "ok"
    start_time: str = ""
    end_time: str = ""
    block_id: str = ""
    function_pool: str = ""
    branch_seed: int = 0
    strategy_order: str = ""
    planned_arrival_time: str = ""
    actual_start_time: str = ""
    arrival_lateness_ms: float = 0.0
    warmup_overrun_ms: float = 0.0
    initial_environment_ids: str = "{}"
