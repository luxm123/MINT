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
    assert summary["unserved_intent_cold_start"] == 0
    assert summary["uncovered_cold_start"] == 0
    assert summary["execute_count"] == 1
    assert summary["cancel_count"] == 1
    assert summary["warmup_error_total"] == 0
    assert summary["scheduler_error_total"] == 0


def test_no_warmup_cold_starts_are_uncovered_not_missed(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events = [
        {"event_type": "invocation", "run_id": "r1", "logical_name": "f1", "cold_start": True, "latency_ms": 900},
        {"event_type": "invocation", "run_id": "r1", "logical_name": "f2", "cold_start": True, "latency_ms": 900},
        {"event_type": "workflow_summary", "run_id": "r1", "baseline": "no_warmup", "latency_ms": 1800},
    ]
    events_path.write_text("\n".join(json.dumps(item) for item in events), encoding="utf-8")
    summary = compute_summary(events_path)
    assert summary["cold_start_count"] == 2
    assert summary["missed_warmup"] == 0
    assert summary["unserved_intent_cold_start"] == 0
    assert summary["uncovered_cold_start"] == 2


def test_cold_start_with_unserved_intent_counts_separately(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events = [
        {"event_type": "scheduler_decision", "run_id": "r1", "logical_name": "f1", "action": "replacement_warmup"},
        {"event_type": "invocation", "run_id": "r1", "logical_name": "f1", "cold_start": True, "latency_ms": 900},
        {"event_type": "workflow_summary", "run_id": "r1", "latency_ms": 900},
    ]
    events_path.write_text("\n".join(json.dumps(item) for item in events), encoding="utf-8")
    summary = compute_summary(events_path)
    assert summary["missed_warmup"] == 0
    assert summary["unserved_intent_cold_start"] == 1
    assert summary["uncovered_cold_start"] == 0


def test_metrics_separate_target_timing_and_verified_environment_reuse(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events = [
        {
            "event_type": "warmup",
            "run_id": "r1",
            "logical_name": "f1",
            "target_hit": True,
            "ready_before_demand": True,
            "useful": True,
            "status": "ok",
            "execution_environment_id": "env-a",
        },
        {
            "event_type": "warmup",
            "run_id": "r1",
            "logical_name": "f2",
            "target_hit": True,
            "ready_before_demand": False,
            "useful": False,
            "status": "ok",
            "execution_environment_id": "env-b",
        },
        {
            "event_type": "warmup",
            "run_id": "r1",
            "logical_name": "f3",
            "target_hit": False,
            "ready_before_demand": False,
            "useful": False,
            "status": "ok",
            "execution_environment_id": "env-c",
        },
        {
            "event_type": "invocation",
            "run_id": "r1",
            "logical_name": "f1",
            "cold_start": False,
            "execution_environment_id": "env-a",
            "latency_ms": 1,
        },
        {
            "event_type": "invocation",
            "run_id": "r1",
            "logical_name": "f2",
            "cold_start": False,
            "execution_environment_id": "env-b",
            "latency_ms": 1,
        },
        {"event_type": "workflow_summary", "run_id": "r1", "latency_ms": 2},
    ]
    events_path.write_text("\n".join(json.dumps(item) for item in events), encoding="utf-8")

    summary = compute_summary(events_path)

    assert summary["target_hit_warmup"] == 2
    assert summary["ready_before_demand_warmup"] == 1
    assert summary["late_target_warmup"] == 1
    assert summary["off_path_warmup"] == 1
    assert summary["reuse_verified_warmup"] == 2
    assert summary["environment_reused_warmup"] == 2
    assert summary["effective_warmup"] == 1


def test_dry_run_empty_environment_ids_are_not_counted_as_reuse(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events = [
        {
            "event_type": "warmup",
            "run_id": "r1",
            "logical_name": "f1",
            "target_hit": True,
            "ready_before_demand": True,
            "useful": True,
            "execution_environment_id": "",
        },
        {
            "event_type": "invocation",
            "run_id": "r1",
            "logical_name": "f1",
            "cold_start": False,
            "execution_environment_id": "",
            "latency_ms": 1,
        },
        {"event_type": "workflow_summary", "run_id": "r1", "latency_ms": 1},
    ]
    events_path.write_text("\n".join(json.dumps(item) for item in events), encoding="utf-8")

    summary = compute_summary(events_path)

    assert summary["reuse_verified_warmup"] == 0
    assert summary["environment_reused_warmup"] == 0
    assert summary["effective_warmup"] == 0
    assert summary["environment_reuse_ratio"] is None
    assert summary["effective_warmup_ratio"] == 0.0
    assert summary["reuse_audit_coverage_ratio"] == 0.0
    assert summary["verified_on_path_effectiveness_ratio"] is None


def test_metrics_separate_arrival_and_node_demand_deadlines(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events = [
        {
            "event_type": "warmup",
            "run_id": "r1",
            "logical_name": "f2",
            "target_hit": True,
            "ready_before_deadline": True,
            "readiness_deadline_type": "planned_arrival",
            "ready_before_arrival": True,
            "ready_before_node_demand": None,
            "ready_before_demand": True,
            "useful": True,
        },
        {
            "event_type": "warmup",
            "run_id": "r1",
            "logical_name": "f6",
            "target_hit": True,
            "ready_before_deadline": False,
            "readiness_deadline_type": "node_demand",
            "ready_before_arrival": None,
            "ready_before_node_demand": False,
            "ready_before_demand": False,
            "missed_at_node_demand": True,
            "missed_at_demand": True,
            "useful": False,
        },
        {
            "event_type": "workflow_summary",
            "run_id": "r1",
            "latency_ms": 1,
            "warmup_error_count": 0,
            "scheduler_error_count": 1,
        },
    ]
    events_path.write_text(
        "\n".join(json.dumps(item) for item in events), encoding="utf-8"
    )

    summary = compute_summary(events_path)

    assert summary["ready_before_deadline_warmup"] == 1
    assert summary["ready_before_arrival_warmup"] == 1
    assert summary["ready_before_node_demand_warmup"] == 0
    assert summary["ready_before_demand_warmup"] == 1
    assert summary["missed_at_arrival_count"] == 0
    assert summary["missed_at_node_demand_count"] == 1
    assert summary["scheduler_error_total"] == 1


def test_aborted_partial_run_is_reported_but_excluded_from_main_metrics(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events = [
        {
            "event_type": "invocation",
            "run_id": "complete",
            "logical_name": "f1",
            "cold_start": False,
            "latency_ms": 10,
        },
        {
            "event_type": "workflow_summary",
            "run_id": "complete",
            "latency_ms": 10,
        },
        {
            "event_type": "warmup",
            "run_id": "aborted",
            "logical_name": "f2",
            "useful": False,
        },
        {
            "event_type": "invocation",
            "run_id": "aborted",
            "logical_name": "f2",
            "cold_start": True,
            "latency_ms": 900,
        },
    ]
    events_path.write_text("\n".join(json.dumps(item) for item in events), encoding="utf-8")

    summary = compute_summary(events_path)

    assert summary["workflow_runs"] == 1
    assert summary["aborted_run_count"] == 1
    assert summary["aborted_run_ids"] == ["aborted"]
    assert summary["total_warmup"] == 0
    assert summary["cold_start_count"] == 0
