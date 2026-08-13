from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np

from mint.utils import read_jsonl


def _percentile(values: list[float], percentile: int) -> float:
    if not values:
        return 0.0
    return round(float(np.percentile(values, percentile)), 3)


def _ready_before_deadline(event: dict[str, Any]) -> bool:
    return bool(
        event.get(
            "ready_before_deadline",
            event.get("ready_before_demand", event.get("useful", False)),
        )
    )


def compute_summary(events_path: str | Path) -> dict[str, Any]:
    all_events = read_jsonl(events_path)
    summary_events = [e for e in all_events if e.get("event_type") == "workflow_summary"]
    completed_run_ids = {
        str(event.get("run_id")) for event in summary_events if event.get("run_id")
    }
    all_run_ids = {
        str(event.get("run_id")) for event in all_events if event.get("run_id")
    }
    aborted_run_ids = all_run_ids - completed_run_ids
    # Main metrics describe completed workflow samples only.  A partial run can
    # contain valid diagnostic events, but mixing those calls into the latency
    # and warmup denominators would corrupt the experiment summary.
    events = [
        event
        for event in all_events
        if event.get("event_type") == "workflow_summary"
        or str(event.get("run_id")) in completed_run_ids
    ]
    invocation_events = [e for e in events if e.get("event_type") == "invocation"]
    warmup_events = [e for e in events if e.get("event_type") == "warmup"]
    decision_events = [e for e in events if e.get("event_type") == "scheduler_decision"]
    lifecycle_events = [e for e in events if e.get("event_type") == "intent_lifecycle"]

    latencies = [float(e.get("latency_ms", 0.0)) for e in summary_events]
    invocation_latencies = [float(e.get("latency_ms", 0.0)) for e in invocation_events]
    cold_count = sum(1 for e in invocation_events if e.get("cold_start"))
    total_invocations = len(invocation_events)
    total_warmup = len(warmup_events)
    useful_warmup = sum(1 for e in warmup_events if e.get("useful"))
    target_hit_warmup = sum(1 for e in warmup_events if e.get("target_hit", e.get("useful")))
    ready_before_deadline_warmup = sum(
        1
        for e in warmup_events
        if _ready_before_deadline(e)
        and e.get("status", "ok") != "error"
    )
    ready_before_arrival_warmup = sum(
        1 for e in warmup_events if e.get("ready_before_arrival") is True
    )
    ready_before_node_demand_warmup = sum(
        1 for e in warmup_events if e.get("ready_before_node_demand") is True
    )
    off_path_warmup = sum(
        1 for e in warmup_events if not e.get("target_hit", e.get("useful"))
    )
    late_target_warmup = sum(
        1
        for e in warmup_events
        if e.get("target_hit", e.get("useful"))
        and not _ready_before_deadline(e)
        and e.get("status", "ok") != "error"
    )
    missed_at_demand = sum(1 for e in warmup_events if e.get("missed_at_demand"))
    missed_at_arrival = sum(1 for e in warmup_events if e.get("missed_at_arrival"))
    missed_at_node_demand = sum(
        1 for e in warmup_events if e.get("missed_at_node_demand")
    )
    wasted_warmup = total_warmup - useful_warmup
    warmed = {(e.get("run_id"), e.get("logical_name")) for e in warmup_events}
    intended = {
        (e.get("run_id"), e.get("logical_name"))
        for e in decision_events
        if e.get("action") in {"execute", "execute_pending", "replacement_warmup"}
    }
    covered = warmed | intended
    cold_reals = {(e.get("run_id"), e.get("logical_name")) for e in invocation_events if e.get("cold_start")}
    missed_warmup = len(cold_reals & warmed)
    unserved_intent_cold_start = len(cold_reals & (intended - warmed))
    uncovered_cold_start = len(cold_reals - covered)
    action_counts = Counter(e.get("action") for e in decision_events)
    planner_types = sorted({e.get("planner_type") for e in summary_events if e.get("planner_type")})

    # A target hit or a timely completion is not proof that AWS reused the
    # warmed execution environment.  Reuse is verified only when both sides
    # provide non-empty environment UUIDs.  Dry-run events intentionally have
    # empty IDs and therefore never enter these AWS-only counts.
    real_by_target: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
    for event in invocation_events:
        real_by_target.setdefault(
            (event.get("run_id"), event.get("logical_name")), []
        ).append(event)
    reuse_verified_warmup = 0
    environment_reused_warmup = 0
    effective_warmup = 0
    for warmup in warmup_events:
        matching_reals = real_by_target.get(
            (warmup.get("run_id"), warmup.get("logical_name")), []
        )
        if not matching_reals:
            continue
        real = matching_reals[0]
        warmup_environment = str(warmup.get("execution_environment_id") or "")
        real_environment = str(real.get("execution_environment_id") or "")
        if not warmup_environment or not real_environment:
            continue
        reuse_verified_warmup += 1
        reused = warmup_environment == real_environment
        environment_reused_warmup += int(reused)
        timely = _ready_before_deadline(warmup)
        target_hit = bool(warmup.get("target_hit", warmup.get("useful")))
        status_ok = warmup.get("status", "ok") != "error"
        effective_warmup += int(
            target_hit and timely and status_ok and reused and not bool(real.get("cold_start"))
        )

    return {
        "workflow_runs": len(summary_events),
        "aborted_run_count": len(aborted_run_ids),
        "aborted_run_ids": sorted(aborted_run_ids),
        "consumed_budget_total": sum(int(event.get("consumed_budget", event.get("warmup_count", 0))) for event in summary_events),
        "reserved_budget_final_total": sum(int(event.get("reserved_budget", 0)) for event in summary_events),
        "unused_budget_total": sum(int(event.get("unused_budget", 0)) for event in summary_events),
        "warmup_error_total": sum(
            int(event.get("warmup_error_count", 0)) for event in summary_events
        ),
        "scheduler_error_total": sum(
            int(event.get("scheduler_error_count", 0)) for event in summary_events
        ),
        "budget_limit_per_run": (
            int(summary_events[0].get("budget_limit", 0)) if summary_events else 0
        ),
        "planner_type": planner_types[0] if len(planner_types) == 1 else ",".join(planner_types),
        "end_to_end_latency_ms_avg": round(float(np.mean(latencies)), 3) if latencies else 0.0,
        "p50_latency_ms": _percentile(latencies, 50),
        "p95_latency_ms": _percentile(latencies, 95),
        "p99_latency_ms": _percentile(latencies, 99),
        "invocation_latency_ms_avg": round(float(np.mean(invocation_latencies)), 3) if invocation_latencies else 0.0,
        "cold_start_count": cold_count,
        "cold_start_rate": round(cold_count / total_invocations, 6) if total_invocations else 0.0,
        "total_warmup": total_warmup,
        "useful_warmup": useful_warmup,
        "target_hit_warmup": target_hit_warmup,
        "target_hit_ratio": round(target_hit_warmup / total_warmup, 6) if total_warmup else 0.0,
        "ready_before_deadline_warmup": ready_before_deadline_warmup,
        "ready_before_deadline_ratio": (
            round(ready_before_deadline_warmup / total_warmup, 6)
            if total_warmup
            else 0.0
        ),
        "ready_before_arrival_warmup": ready_before_arrival_warmup,
        "ready_before_node_demand_warmup": ready_before_node_demand_warmup,
        # Compatibility aliases retained for existing tables.
        "ready_before_demand_warmup": ready_before_deadline_warmup,
        "ready_before_demand_ratio": (
            round(ready_before_deadline_warmup / total_warmup, 6) if total_warmup else 0.0
        ),
        "off_path_warmup": off_path_warmup,
        "late_target_warmup": late_target_warmup,
        "missed_at_demand_count": missed_at_demand,
        "missed_at_arrival_count": missed_at_arrival,
        "missed_at_node_demand_count": missed_at_node_demand,
        "reuse_verified_warmup": reuse_verified_warmup,
        "environment_reused_warmup": environment_reused_warmup,
        "effective_warmup": effective_warmup,
        "environment_reuse_ratio": (
            round(environment_reused_warmup / reuse_verified_warmup, 6)
            if reuse_verified_warmup
            else None
        ),
        "effective_warmup_ratio": (
            round(effective_warmup / total_warmup, 6) if total_warmup else 0.0
        ),
        "reuse_audit_coverage_ratio": (
            round(reuse_verified_warmup / total_warmup, 6) if total_warmup else 0.0
        ),
        "verified_on_path_effectiveness_ratio": (
            round(effective_warmup / reuse_verified_warmup, 6)
            if reuse_verified_warmup
            else None
        ),
        "wasted_warmup": wasted_warmup,
        "missed_warmup": missed_warmup,
        "unserved_intent_cold_start": unserved_intent_cold_start,
        "uncovered_cold_start": uncovered_cold_start,
        "useful_warmup_ratio": round(useful_warmup / total_warmup, 6) if total_warmup else 0.0,
        "execute_count": action_counts.get("execute", 0),
        "execute_pending_count": action_counts.get("execute_pending", 0),
        "plan_pending_count": action_counts.get("plan_pending", 0),
        "not_selected_count": action_counts.get("not_selected", 0),
        "delay_count": action_counts.get("delay", 0),
        "cancel_pending_count": action_counts.get("cancel_pending", 0),
        "invalidate_executed_count": action_counts.get("invalidate_executed", 0),
        "replacement_warmup_count": action_counts.get("replacement_warmup", 0),
        "cancel_race_lost_count": action_counts.get("cancel_race_lost", 0),
        "in_flight_at_demand_count": action_counts.get("in_flight_at_demand", 0),
        "lifecycle_submitted_count": sum(
            1 for event in lifecycle_events if event.get("actual_call_submitted")
        ),
        "lifecycle_cancel_pending_count": sum(
            1
            for event in lifecycle_events
            if event.get("state_before") == "pending" and event.get("state_after") == "cancelled"
        ),
        "warmup_error_count": sum(1 for event in warmup_events if event.get("status") == "error"),
        "degraded_scheduler_run_count": sum(
            1 for event in summary_events if event.get("scheduler_status") == "degraded"
        ),
        # Compatibility aliases for old result-table readers.
        "cancel_count": action_counts.get("cancel_pending", 0) + action_counts.get("cancel", 0),
        "replace_count": action_counts.get("replacement_warmup", 0) + action_counts.get("replace", 0),
    }
