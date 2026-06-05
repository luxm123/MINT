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


def compute_summary(events_path: str | Path) -> dict[str, Any]:
    events = read_jsonl(events_path)
    invocation_events = [e for e in events if e.get("event_type") == "invocation"]
    warmup_events = [e for e in events if e.get("event_type") == "warmup"]
    summary_events = [e for e in events if e.get("event_type") == "workflow_summary"]
    decision_events = [e for e in events if e.get("event_type") == "scheduler_decision"]

    latencies = [float(e.get("latency_ms", 0.0)) for e in summary_events]
    invocation_latencies = [float(e.get("latency_ms", 0.0)) for e in invocation_events]
    cold_count = sum(1 for e in invocation_events if e.get("cold_start"))
    total_invocations = len(invocation_events)
    total_warmup = len(warmup_events)
    useful_warmup = sum(1 for e in warmup_events if e.get("useful"))
    wasted_warmup = total_warmup - useful_warmup
    warmed = {(e.get("run_id"), e.get("logical_name")) for e in warmup_events if e.get("useful")}
    cold_reals = {(e.get("run_id"), e.get("logical_name")) for e in invocation_events if e.get("cold_start")}
    missed_warmup = len(cold_reals - warmed)
    action_counts = Counter(e.get("action") for e in decision_events)

    return {
        "workflow_runs": len(summary_events),
        "end_to_end_latency_ms_avg": round(float(np.mean(latencies)), 3) if latencies else 0.0,
        "p50_latency_ms": _percentile(latencies, 50),
        "p95_latency_ms": _percentile(latencies, 95),
        "p99_latency_ms": _percentile(latencies, 99),
        "invocation_latency_ms_avg": round(float(np.mean(invocation_latencies)), 3) if invocation_latencies else 0.0,
        "cold_start_count": cold_count,
        "cold_start_rate": round(cold_count / total_invocations, 6) if total_invocations else 0.0,
        "total_warmup": total_warmup,
        "useful_warmup": useful_warmup,
        "wasted_warmup": wasted_warmup,
        "missed_warmup": missed_warmup,
        "useful_warmup_ratio": round(useful_warmup / total_warmup, 6) if total_warmup else 0.0,
        "execute_count": action_counts.get("execute", 0),
        "delay_count": action_counts.get("delay", 0),
        "cancel_count": action_counts.get("cancel", 0),
        "replace_count": action_counts.get("replace", 0),
    }
