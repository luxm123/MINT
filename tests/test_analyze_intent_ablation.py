from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from scripts import analyze_intent_ablation


BASELINES = analyze_intent_ablation.DEFAULT_BASELINES


def _event(event_type: str, **values: Any) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "run_id": "run-0",
        "workflow_index": 0,
        **values,
    }


def _warmup(target: str, useful: bool) -> dict[str, Any]:
    return _event(
        "warmup",
        logical_name=target,
        intent_id=f"intent-{target}",
        useful=useful,
        target_hit=useful,
        ready_before_deadline=useful,
        ready_before_demand=useful,
        missed_at_demand=False,
        status="ok",
        error_type="",
    )


def _lifecycle(seq: int, intent_id: str, target: str, **values: Any) -> dict[str, Any]:
    return _event(
        "intent_lifecycle",
        intent_id=intent_id,
        logical_name=target,
        transition_seq=seq,
        accepted=True,
        **values,
    )


def _summary(consumed: int, reserved: int = 0) -> dict[str, Any]:
    return _event(
        "workflow_summary",
        baseline="",
        consumed_budget=consumed,
        reserved_budget=reserved,
        budget_limit=2,
        warmup_count=consumed,
        warmup_error_count=0,
        scheduler_error_count=0,
        status="ok",
    )


def _common_prefix() -> list[dict[str, Any]]:
    return [
        _event(
            "branch_model",
            decision_phase="initial",
            history_size=100,
            branch_counts=json.dumps({"f2": 70, "f3": 10, "f4": 10, "f5": 10}),
            branch_probabilities=json.dumps({"f2": 0.7, "f3": 0.1, "f4": 0.1, "f5": 0.1}),
        ),
        _lifecycle(
            1,
            "intent-f2",
            "f2",
            state_before="",
            state_after="planned",
            action="create",
            reserved_budget=0,
            consumed_budget=0,
            actual_call_submitted=False,
        ),
        _event(
            "scheduler_decision",
            intent_id="intent-f2",
            logical_name="f2",
            action="execute",
            decision_phase="initial",
        ),
        _lifecycle(
            2,
            "intent-f2",
            "f2",
            state_before="planned",
            state_after="pending",
            action="reserve",
            reserved_budget=1,
            consumed_budget=0,
            actual_call_submitted=False,
        ),
        _lifecycle(
            3,
            "intent-f2",
            "f2",
            state_before="pending",
            state_after="in_flight",
            action="submit",
            reserved_budget=0,
            consumed_budget=1,
            actual_call_submitted=True,
        ),
        _warmup("f2", False),
        _lifecycle(
            4,
            "intent-f2",
            "f2",
            state_before="in_flight",
            state_after="succeeded",
            action="succeed",
            reserved_budget=0,
            consumed_budget=1,
            actual_call_submitted=False,
        ),
        _event(
            "scheduler_decision",
            intent_id="intent-f6",
            logical_name="f6",
            action="plan_pending",
            decision_phase="initial",
        ),
        _lifecycle(
            5,
            "intent-f6",
            "f6",
            state_before="",
            state_after="planned",
            action="create",
            reserved_budget=0,
            consumed_budget=1,
            actual_call_submitted=False,
        ),
        _lifecycle(
            6,
            "intent-f6",
            "f6",
            state_before="planned",
            state_after="pending",
            action="reserve",
            reserved_budget=1,
            consumed_budget=1,
            actual_call_submitted=False,
        ),
        _event(
            "invocation",
            logical_name="f1",
            observed_branch="f4",
            status="ok",
            error_type="",
        ),
    ]


def _baseline_events(baseline: str, *, invalid_cancel: bool = False) -> list[dict[str, Any]]:
    events = _common_prefix()
    if baseline == "mint_markov_no_cancel":
        events.extend(
            [
                _event(
                    "scheduler_decision",
                    intent_id="intent-f6",
                    logical_name="f6",
                    action="execute_pending",
                    decision_phase="runtime_after_branch",
                ),
                _lifecycle(
                    7,
                    "intent-f6",
                    "f6",
                    state_before="pending",
                    state_after="in_flight",
                    action="submit",
                    reserved_budget=0,
                    consumed_budget=2,
                    actual_call_submitted=True,
                ),
                _warmup("f6", False),
                _lifecycle(
                    8,
                    "intent-f6",
                    "f6",
                    state_before="in_flight",
                    state_after="succeeded",
                    action="succeed",
                    reserved_budget=0,
                    consumed_budget=2,
                    actual_call_submitted=False,
                ),
                _summary(2),
            ]
        )
    elif baseline == "mint_markov_cancel_only":
        events.extend(
            [
                _lifecycle(
                    7,
                    "intent-f6",
                    "f6",
                    state_before="pending",
                    state_after="cancelled",
                    action="cancel_pending",
                    reserved_budget=0,
                    consumed_budget=1,
                    actual_call_submitted=False,
                ),
                _event(
                    "scheduler_decision",
                    intent_id="intent-f6",
                    logical_name="f6",
                    action="cancel_pending",
                    decision_phase="runtime_after_branch",
                ),
                _summary(1),
            ]
        )
    else:
        cancel_before = "in_flight" if invalid_cancel else "pending"
        events.extend(
            [
                _lifecycle(
                    7,
                    "intent-f6",
                    "f6",
                    state_before=cancel_before,
                    state_after="cancelled",
                    action="cancel_pending",
                    reserved_budget=1,
                    consumed_budget=1,
                    actual_call_submitted=False,
                ),
                _lifecycle(
                    8,
                    "intent-f8",
                    "f8",
                    state_before="planned",
                    state_after="pending",
                    action="replacement_reserved",
                    reserved_budget=1,
                    consumed_budget=1,
                    actual_call_submitted=False,
                    supersedes_intent_id="intent-f6",
                ),
                _event(
                    "scheduler_decision",
                    intent_id="intent-f6",
                    logical_name="f6",
                    action="cancel_pending",
                    decision_phase="runtime_after_branch",
                ),
                _event(
                    "scheduler_decision",
                    intent_id="intent-f8",
                    logical_name="f8",
                    action="replacement_warmup",
                    decision_phase="runtime_after_branch",
                    supersedes_intent_id="intent-f6",
                ),
                _lifecycle(
                    9,
                    "intent-f8",
                    "f8",
                    state_before="pending",
                    state_after="in_flight",
                    action="submit",
                    reserved_budget=0,
                    consumed_budget=2,
                    actual_call_submitted=True,
                    supersedes_intent_id="intent-f6",
                ),
                _warmup("f8", True),
                _lifecycle(
                    10,
                    "intent-f8",
                    "f8",
                    state_before="in_flight",
                    state_after="succeeded",
                    action="succeed",
                    reserved_budget=0,
                    consumed_budget=2,
                    actual_call_submitted=False,
                    supersedes_intent_id="intent-f6",
                ),
                _summary(2),
            ]
        )
    return events


def _write_fixture(root: Path, *, invalid_full_cancel: bool = False) -> None:
    root.mkdir(parents=True)
    matrix_rows = []
    for baseline in BASELINES:
        output_dir = root / baseline
        output_dir.mkdir()
        events = _baseline_events(
            baseline,
            invalid_cancel=invalid_full_cancel and baseline == "mint_markov_full",
        )
        (output_dir / "events.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in events),
            encoding="utf-8",
        )
        matrix_rows.append(
            {
                "dag": "adaptive_branch",
                "baseline": baseline,
                "budget": "2",
                "repetitions": "1",
                "output_dir": str(output_dir),
            }
        )
    with (root / "summary_matrix.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(matrix_rows[0]))
        writer.writeheader()
        writer.writerows(matrix_rows)
    (root / "experiment_manifest.json").write_text(
        json.dumps({"dry_run": True}),
        encoding="utf-8",
    )


def test_analyzer_reports_paired_ablation_semantics_and_dry_run_scope(tmp_path: Path) -> None:
    root = tmp_path / "matrix"
    _write_fixture(root)

    report = analyze_intent_ablation.analyze(root, expected_runs=1, budget=2)

    assert report["quality_gate_passed"] is True
    assert report["quality_gate_failures"] == []
    assert report["execution_evidence"]["mode"] == "dry_run"
    assert report["execution_evidence"]["paper_performance_eligible"] is False
    assert "must not be used for paper performance conclusions" in report["execution_evidence"]["latency_warning"]
    assert report["quality_gates"]["same_initial_ablation_plan"] is True
    assert report["coverage_evidence"]["timer_wins_race_cases"] == 0
    assert report["coverage_evidence"]["warmup_failure_cases"] == 0
    assert "unit tests" in report["coverage_evidence"]["zero_case_note"]
    no_cancel = report["baseline_totals"]["mint_markov_no_cancel"]
    assert no_cancel["actual_warmups"] == no_cancel["warmups"] == 2
    assert no_cancel["target_hit_warmups"] == 0
    assert no_cancel["ready_before_demand_warmups"] == 0
    assert no_cancel["timely_warmups"] == no_cancel["useful_warmups"] == 0
    assert no_cancel["true_pending_cancellations"] == 0
    assert no_cancel["replacements"] == 0
    assert no_cancel["timer_wins_races"] == 0
    assert no_cancel["wrong_prediction_cases"] == 1
    assert no_cancel["wrong_prediction_semantic_failures"] == 0
    assert report["baseline_totals"]["mint_markov_cancel_only"]["warmups"] == 1
    assert report["baseline_totals"]["mint_markov_cancel_only"]["true_pending_cancellations"] == 1
    assert report["baseline_totals"]["mint_markov_full"]["replacements"] == 1
    row = report["paired_rows"][0]
    assert row["actual_branch"] == "f4"
    assert row["mint_markov_full_plan_pending"] == "f6"
    assert row["mint_markov_full_true_pending_cancelled"] == "f6"
    assert row["mint_markov_full_replacements"] == "f8"
    assert row["mint_markov_full_warmups"] == "f2/f8"
    assert row["mint_markov_full_consumed_budget"] == 2
    assert row["mint_markov_full_reserved_budget"] == 0

    csv_path, json_path = analyze_intent_ablation.write_report(root, report)
    assert csv_path.exists()
    assert json_path.exists()
    written = json.loads(json_path.read_text(encoding="utf-8"))
    assert written["quality_gate_passed"] is True


def test_analyzer_rejects_cancel_after_submission_and_unfunded_replacement(tmp_path: Path) -> None:
    root = tmp_path / "invalid"
    _write_fixture(root, invalid_full_cancel=True)

    report = analyze_intent_ablation.analyze(root, expected_runs=1, budget=2)

    assert report["quality_gate_passed"] is False
    assert report["quality_gates"]["true_cancellation_state_valid"] is False
    assert report["quality_gates"]["replacement_uses_cancelled_reservation"] is False
    assert report["quality_gates"]["adaptive_wrong_prediction_semantics"] is False
    assert "mint_markov_full[0]" in " ".join(
        report["quality_gate_details"]["true_cancellation_state_valid"]
    )


def test_analyzer_rejects_replacement_off_observed_path(tmp_path: Path) -> None:
    root = tmp_path / "off_path"
    _write_fixture(root)
    path = root / "mint_markov_full" / "events.jsonl"
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    for event in events:
        if event.get("logical_name") == "f8" and event.get("intent_id") == "intent-f8":
            event["logical_name"] = "f9"
    path.write_text(
        "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
    )

    report = analyze_intent_ablation.analyze(root, expected_runs=1, budget=2)

    assert report["quality_gate_passed"] is False
    assert report["quality_gates"]["adaptive_wrong_prediction_semantics"] is False


def test_analyzer_rejects_mismatched_initial_ablation_plan(tmp_path: Path) -> None:
    root = tmp_path / "mismatched_plan"
    _write_fixture(root)
    path = root / "mint_markov_cancel_only" / "events.jsonl"
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    for event in events:
        if (
            event.get("event_type") == "scheduler_decision"
            and event.get("decision_phase") == "initial"
            and event.get("action") == "execute"
        ):
            event["logical_name"] = "f3"
    path.write_text(
        "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
    )

    report = analyze_intent_ablation.analyze(root, expected_runs=1, budget=2)

    assert report["quality_gate_passed"] is False
    assert report["quality_gates"]["same_initial_ablation_plan"] is False


def test_analyzer_rejects_scheduler_errors_separately(tmp_path: Path) -> None:
    root = tmp_path / "scheduler_error"
    _write_fixture(root)
    path = root / "mint_markov_full" / "events.jsonl"
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    events.insert(
        -1,
        _event(
            "scheduler_decision",
            logical_name="f8",
            action="scheduler_error",
            decision_phase="scheduler",
        ),
    )
    for event in events:
        if event.get("event_type") == "workflow_summary":
            event["scheduler_error_count"] = 1
    path.write_text(
        "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
    )

    report = analyze_intent_ablation.analyze(root, expected_runs=1, budget=2)

    assert report["quality_gates"]["scheduler_error_count_consistent"] is True
    assert report["quality_gates"]["no_scheduler_errors"] is False
    assert report["coverage_evidence"]["scheduler_error_cases"] == 1
