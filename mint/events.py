from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from mint.utils import utc_now_iso


@dataclass
class EventBase:
    event_type: str
    run_id: str
    timestamp: str = field(default_factory=utc_now_iso)

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


@dataclass
class WarmupEvent(EventBase):
    function_name: str = ""
    logical_name: str = ""
    intent_id: str = ""
    action: str = "execute"
    useful: bool = False
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


@dataclass
class SchedulerDecision(EventBase):
    intent_id: str = ""
    function_name: str = ""
    logical_name: str = ""
    action: str = "execute"
    action_reason: str = ""
    gain: float = 0.0
    planned_time_sec: float = 0.0


@dataclass
class WorkflowRunSummary(EventBase):
    dag: str = ""
    baseline: str = ""
    planner_type: str = "heuristic"
    latency_ms: float = 0.0
    cold_start_count: int = 0
    warmup_count: int = 0
    status: str = "ok"
    start_time: str = ""
    end_time: str = ""
