import json

from mint.metrics import compute_summary


def test_metrics_compute_summary_from_events(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events = [
        {"event_type": "scheduler_decision", "run_id": "r1", "action": "execute"},
        {"event_type": "scheduler_decision", "run_id": "r1", "action": "cancel"},
        {"event_type": "warmup", "run_id": "r1", "logical_name": "f1", "useful": True},
        {"event_type": "warmup", "run_id": "r1", "logical_name": "f2", "useful": False},
        {"event_type": "invocation", "run_id": "r1", "logical_name": "f1", "cold_start": False, "latency_ms": 100},
        {"event_type": "invocation", "run_id": "r1", "logical_name": "f2", "cold_start": True, "latency_ms": 900},
        {"event_type": "workflow_summary", "run_id": "r1", "latency_ms": 1000},
    ]
    events_path.write_text("\n".join(json.dumps(item) for item in events), encoding="utf-8")
    summary = compute_summary(events_path)
    assert summary["workflow_runs"] == 1
    assert summary["cold_start_count"] == 1
    assert summary["total_warmup"] == 2
    assert summary["useful_warmup"] == 1
    assert summary["wasted_warmup"] == 1
    assert summary["missed_warmup"] == 1
    assert summary["execute_count"] == 1
    assert summary["cancel_count"] == 1
