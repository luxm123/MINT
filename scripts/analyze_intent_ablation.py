from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

DEFAULT_BASELINES = (
    "mint_markov_no_cancel",
    "mint_markov_cancel_only",
    "mint_markov_full",
)
OPTIONAL_BASELINE = "xanadu_like"
ADAPTIVE_BRANCH_REACHABLE = {
    "f2": {"f2", "f6"},
    "f3": {"f3", "f7"},
    "f4": {"f4", "f8"},
    "f5": {"f5", "f9"},
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit paired pending-intent cancellation ablations per workflow request."
    )
    parser.add_argument("--results-root", required=True, type=Path)
    parser.add_argument("--dag", default="adaptive_branch")
    parser.add_argument("--expected-runs", type=int)
    parser.add_argument("--budget", type=int)
    parser.add_argument("--baselines", nargs="+", default=list(DEFAULT_BASELINES))
    parser.add_argument(
        "--include-xanadu",
        action="store_true",
        help="Also include xanadu_like in the paired rows (not in MINT ablation semantics).",
    )
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument("--output-json", type=Path)
    return parser.parse_args(argv)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _resolve_output_dir(root: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute() or path.exists():
        return path
    candidate = root / path
    if candidate.exists():
        return candidate
    candidate = root.parent / path
    return candidate if candidate.exists() else path


def _load_matrix_rows(
    root: Path,
    baselines: tuple[str, ...],
    dag: str,
    budget: int | None,
) -> dict[str, dict[str, str]]:
    matrix_path = root / "summary_matrix.csv"
    if not matrix_path.exists():
        raise ValueError(f"missing experiment matrix: {matrix_path}")
    rows = list(csv.DictReader(matrix_path.open(newline="", encoding="utf-8")))
    selected: dict[str, dict[str, str]] = {}
    for baseline in baselines:
        matches = [
            row
            for row in rows
            if row.get("baseline") == baseline
            and (not row.get("dag") or row.get("dag") == dag)
            and (budget is None or int(row.get("budget", -1)) == budget)
        ]
        if len(matches) != 1:
            qualifier = f"dag={dag}" + (f", budget={budget}" if budget is not None else "")
            raise ValueError(
                f"expected exactly one matrix row for {baseline} ({qualifier}), found {len(matches)}"
            )
        selected[baseline] = matches[0]
    return selected


def _events_by_index(events: Iterable[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        try:
            index = int(event.get("workflow_index", -1))
        except (TypeError, ValueError):
            index = -1
        if index >= 0:
            grouped[index].append(event)
    return dict(grouped)


def _is_event_error(event: dict[str, Any]) -> bool:
    status = str(event.get("status", "")).strip().lower()
    return bool(event.get("error_type")) or status in {"error", "failed", "failure"}


def _actual_branch(events: list[dict[str, Any]]) -> str:
    for event in events:
        if event.get("event_type") == "invocation" and event.get("observed_branch"):
            return str(event["observed_branch"])
    for event in events:
        if event.get("event_type") == "branch_model" and event.get("observed_branch"):
            return str(event["observed_branch"])
    return ""


def _targets(events: Iterable[dict[str, Any]]) -> list[str]:
    return [str(event.get("logical_name", "")) for event in events if event.get("logical_name")]


def _join(values: Iterable[str]) -> str:
    return "/".join(value for value in values if value)


def _runtime_action_text(events: Iterable[dict[str, Any]]) -> str:
    return " | ".join(
        f"{event.get('action', '')}:{event.get('logical_name', '')}"
        for event in events
    )


def _single_summary(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    summaries = [event for event in events if event.get("event_type") == "workflow_summary"]
    return summaries[-1] if summaries else None


def _integer(event: dict[str, Any] | None, key: str, default: int = 0) -> int:
    if event is None:
        return default
    try:
        return int(event.get(key, default))
    except (TypeError, ValueError):
        return default


def _append_violation(violations: dict[str, list[str]], gate: str, detail: str) -> None:
    violations[gate].append(detail)


def _sequence(event: dict[str, Any]) -> int:
    return _integer(event, "transition_seq", -1)


def _accepted(event: dict[str, Any]) -> bool:
    return bool(event.get("accepted", True))


def _ready_before_deadline(event: dict[str, Any]) -> bool:
    """Read the explicit deadline label while accepting legacy pilot files."""

    return bool(
        event.get(
            "ready_before_deadline",
            event.get("ready_before_demand", event.get("useful", False)),
        )
    )


def _canonical_json(value: Any) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _initial_model_snapshot(events: list[dict[str, Any]]) -> tuple[int, str, str] | None:
    event = next(
        (
            item
            for item in events
            if item.get("event_type") == "branch_model"
            and item.get("decision_phase") == "initial"
        ),
        None,
    )
    if event is None:
        return None
    return (
        _integer(event, "history_size"),
        _canonical_json(event.get("branch_counts", {})),
        _canonical_json(event.get("branch_probabilities", {})),
    )


def _replacement_lifecycle_chain(
    events: list[dict[str, Any]],
    replacement: dict[str, Any],
) -> tuple[bool, str]:
    old_intent_id = str(replacement.get("supersedes_intent_id", ""))
    replacement_id = str(replacement.get("intent_id", ""))
    if not old_intent_id:
        return False, "replacement has no supersedes_intent_id"

    lifecycle = [
        event
        for event in events
        if event.get("event_type") == "intent_lifecycle"
    ]
    cancellations = [
        event
        for event in lifecycle
        if str(event.get("intent_id", "")) == old_intent_id
        and event.get("state_before") == "pending"
        and event.get("state_after") == "cancelled"
        and event.get("action") == "cancel_pending"
        and _accepted(event)
        and not bool(event.get("actual_call_submitted"))
    ]
    reservations = [
        event
        for event in lifecycle
        if str(event.get("intent_id", "")) == replacement_id
        and event.get("state_before") == "planned"
        and event.get("state_after") == "pending"
        and event.get("action") == "replacement_reserved"
        and str(event.get("supersedes_intent_id", "")) == old_intent_id
        and _accepted(event)
    ]
    submissions = [
        event
        for event in lifecycle
        if str(event.get("intent_id", "")) == replacement_id
        and event.get("state_before") == "pending"
        and event.get("state_after") == "in_flight"
        and bool(event.get("actual_call_submitted"))
        and _accepted(event)
    ]
    old_reservations = [
        event
        for event in lifecycle
        if str(event.get("intent_id", "")) == old_intent_id
        and event.get("state_before") == "planned"
        and event.get("state_after") == "pending"
        and event.get("action") == "reserve"
        and _accepted(event)
    ]
    if (
        len(old_reservations) != 1
        or len(cancellations) != 1
        or len(reservations) != 1
        or len(submissions) != 1
    ):
        return (
            False,
            "expected one original reserve, accepted cancel_pending, replacement_reserved, "
            "and replacement submit transition",
        )
    old_reserve_seq = _sequence(old_reservations[0])
    cancel_seq = _sequence(cancellations[0])
    reserve_seq = _sequence(reservations[0])
    submit_seq = _sequence(submissions[0])
    if min(old_reserve_seq, cancel_seq, reserve_seq, submit_seq) <= 0:
        return False, "replacement lifecycle is missing positive transition_seq"
    if not (
        old_reserve_seq < cancel_seq
        and reserve_seq == cancel_seq + 1
        and cancel_seq < reserve_seq < submit_seq
    ):
        return (
            False,
            "invalid transition order "
            f"old_reserve={old_reserve_seq}, cancel={cancel_seq}, "
            f"replacement_reserve={reserve_seq}, submit={submit_seq}",
        )
    return True, ""


def _legal_timer_wins_race(
    events: list[dict[str, Any]],
    planned_intent_ids: set[str],
) -> tuple[bool, bool, set[str], str]:
    decisions = [
        event
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "cancel_race_lost"
        and str(event.get("intent_id", "")) in planned_intent_ids
    ]
    if not decisions:
        return False, True, set(), ""
    legal_targets: set[str] = set()
    seen_intents: set[str] = set()
    for decision in decisions:
        intent_id = str(decision.get("intent_id", ""))
        if not intent_id or intent_id in seen_intents:
            return True, False, set(), f"duplicate or missing race intent_id={intent_id!r}"
        seen_intents.add(intent_id)
        submissions = [
            event
            for event in events
            if event.get("event_type") == "intent_lifecycle"
            and str(event.get("intent_id", "")) == intent_id
            and event.get("state_before") == "pending"
            and event.get("state_after") == "in_flight"
            and bool(event.get("actual_call_submitted"))
            and _accepted(event)
        ]
        rejections = [
            event
            for event in events
            if event.get("event_type") == "intent_lifecycle"
            and str(event.get("intent_id", "")) == intent_id
            and event.get("action") in {"cancel_pending_rejected", "atomic_replace_rejected"}
            and not _accepted(event)
            and str(event.get("state_before", "")) in {"in_flight", "succeeded", "failed"}
        ]
        if len(submissions) != 1 or len(rejections) != 1:
            return (
                True,
                False,
                set(),
                f"intent={intent_id} submits={len(submissions)} rejections={len(rejections)}",
            )
        if not (0 < _sequence(submissions[0]) < _sequence(rejections[0])):
            return (
                True,
                False,
                set(),
                f"intent={intent_id} submit_seq={_sequence(submissions[0])} "
                f"rejection_seq={_sequence(rejections[0])}",
            )
        legal_targets.add(str(decision.get("logical_name", "")))
    return True, True, legal_targets, ""


def _wrong_prediction_semantics(
    baseline: str,
    planned_targets: set[str],
    warmup_targets: set[str],
    cancelled_targets: set[str],
    replacement_targets: set[str],
    reachable_targets: set[str],
    consumed: int,
    budget_limit: int,
    timer_wins_race: bool,
) -> tuple[bool, str]:
    if baseline == "mint_markov_no_cancel":
        ok = (
            bool(planned_targets)
            and planned_targets <= warmup_targets
            and not cancelled_targets
            and not replacement_targets
            and consumed <= budget_limit
        )
        return ok, "stale pending must execute without cancellation or replacement"
    if baseline == "mint_markov_cancel_only":
        if timer_wins_race:
            ok = (
                bool(planned_targets)
                and planned_targets <= warmup_targets
                and not cancelled_targets
                and not replacement_targets
                and consumed <= budget_limit
            )
            return ok, "timer-wins race must keep the submitted stale intent and consume its budget"
        ok = (
            bool(planned_targets)
            and planned_targets <= cancelled_targets
            and planned_targets.isdisjoint(warmup_targets)
            and not replacement_targets
            and consumed < budget_limit
        )
        return ok, "stale pending must be cancelled, not submitted, and not replaced"
    if baseline == "mint_markov_full":
        if timer_wins_race:
            ok = (
                bool(planned_targets)
                and planned_targets <= warmup_targets
                and not cancelled_targets
                and not replacement_targets
                and consumed <= budget_limit
            )
            return ok, "timer-wins race must not refund or reuse already-consumed budget"
        ok = (
            bool(planned_targets)
            and planned_targets <= cancelled_targets
            and planned_targets.isdisjoint(warmup_targets)
            and bool(replacement_targets)
            and replacement_targets <= warmup_targets
            and replacement_targets <= reachable_targets
            and consumed <= budget_limit
        )
        return (
            ok,
            "stale pending must be cancelled and its reservation replaced by "
            "a submitted target on the observed path",
        )
    return True, "not a MINT cancellation ablation"


def _correct_prediction_semantics(
    planned_targets: set[str],
    warmup_targets: set[str],
    cancelled_targets: set[str],
    replacement_targets: set[str],
    reachable_targets: set[str],
    consumed: int,
    budget_limit: int,
) -> tuple[bool, str]:
    ok = (
        bool(planned_targets)
        and planned_targets <= reachable_targets
        and planned_targets <= warmup_targets
        and planned_targets.isdisjoint(cancelled_targets)
        and not replacement_targets
        and consumed <= budget_limit
    )
    return (
        ok,
        "a correct pending prediction must execute without cancellation or replacement",
    )


def analyze(
    root: Path,
    *,
    expected_runs: int | None = None,
    dag: str = "adaptive_branch",
    budget: int | None = None,
    baselines: Iterable[str] = DEFAULT_BASELINES,
    include_xanadu: bool = False,
) -> dict[str, Any]:
    root = Path(root)
    requested = list(dict.fromkeys(str(item) for item in baselines))
    if include_xanadu and OPTIONAL_BASELINE not in requested:
        requested.append(OPTIONAL_BASELINE)
    selected_baselines = tuple(requested)
    matrix_rows = _load_matrix_rows(root, selected_baselines, dag, budget)
    grouped: dict[str, dict[int, list[dict[str, Any]]]] = {}
    for baseline, row in matrix_rows.items():
        output_dir = _resolve_output_dir(root, str(row["output_dir"]))
        events_path = output_dir / "events.jsonl"
        if not events_path.exists():
            raise ValueError(f"missing events for {baseline}: {events_path}")
        grouped[baseline] = _events_by_index(_read_jsonl(events_path))

    matrix_expected = {
        int(row["repetitions"])
        for row in matrix_rows.values()
        if str(row.get("repetitions", "")).strip()
    }
    if expected_runs is None:
        if len(matrix_expected) != 1:
            raise ValueError(f"cannot infer one expected run count from matrix: {sorted(matrix_expected)}")
        expected_runs = next(iter(matrix_expected))
    if expected_runs < 1:
        raise ValueError("expected_runs must be positive")

    matrix_budgets = {
        int(row["budget"])
        for row in matrix_rows.values()
        if str(row.get("budget", "")).strip()
    }
    expected_indices = set(range(expected_runs))
    violations: dict[str, list[str]] = defaultdict(list)
    if len(matrix_budgets) != 1:
        _append_violation(violations, "same_budget_limit", f"matrix budgets differ: {sorted(matrix_budgets)}")

    observed_indices: dict[str, set[int]] = {
        baseline: set(rows) for baseline, rows in grouped.items()
    }
    for baseline, indices in observed_indices.items():
        if indices != expected_indices:
            _append_violation(
                violations,
                "expected_workflow_count",
                f"{baseline}: expected={sorted(expected_indices)} observed={sorted(indices)}",
            )

    paired_rows: list[dict[str, Any]] = []
    branch_traces: dict[str, list[str]] = {baseline: [] for baseline in selected_baselines}
    wrong_case_counts = {baseline: 0 for baseline in DEFAULT_BASELINES if baseline in selected_baselines}
    correct_case_counts = {
        baseline: 0 for baseline in DEFAULT_BASELINES if baseline in selected_baselines
    }
    baseline_totals: dict[str, dict[str, int]] = {
        baseline: {
            "actual_warmups": 0,
            "target_hit_warmups": 0,
            "ready_before_demand_warmups": 0,
            "timely_warmups": 0,
            # Compatibility labels retained for existing report readers.
            "warmups": 0,
            "useful_warmups": 0,
            "true_pending_cancellations": 0,
            "replacements": 0,
            "timer_wins_races": 0,
            "wrong_prediction_cases": 0,
            "wrong_prediction_semantic_failures": 0,
            "correct_prediction_cases": 0,
            "correct_prediction_semantic_failures": 0,
            "scheduler_errors": 0,
            "warmup_failures_exercised": 0,
        }
        for baseline in selected_baselines
    }

    for workflow_index in range(expected_runs):
        branches = {
            baseline: _actual_branch(grouped[baseline].get(workflow_index, []))
            for baseline in selected_baselines
        }
        for baseline, branch in branches.items():
            branch_traces[baseline].append(branch)
            if not branch:
                _append_violation(
                    violations,
                    "branch_trace_complete",
                    f"{baseline}[{workflow_index}] has no observed branch",
                )
        reference_branch = branches[selected_baselines[0]]
        if len(set(branches.values())) != 1:
            _append_violation(
                violations,
                "same_branch_trace",
                f"workflow_index={workflow_index}: {branches}",
            )
        snapshots = {
            baseline: _initial_model_snapshot(grouped[baseline].get(workflow_index, []))
            for baseline in selected_baselines
        }
        if any(snapshot is None for snapshot in snapshots.values()):
            _append_violation(
                violations,
                "same_history_snapshots",
                f"workflow_index={workflow_index}: missing initial snapshot in {snapshots}",
            )
        elif len(set(snapshots.values())) != 1:
            _append_violation(
                violations,
                "same_history_snapshots",
                f"workflow_index={workflow_index}: initial snapshots differ: {snapshots}",
            )
        reference_snapshot = snapshots[selected_baselines[0]]
        row_out: dict[str, Any] = {
            "workflow_index": workflow_index,
            "actual_branch": reference_branch,
            "model_history_size": reference_snapshot[0] if reference_snapshot else -1,
            "model_branch_counts": reference_snapshot[1] if reference_snapshot else "",
            "model_branch_probabilities": reference_snapshot[2] if reference_snapshot else "",
        }
        mint_initial_plans: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}

        for baseline in selected_baselines:
            events = grouped[baseline].get(workflow_index, [])
            planned = [
                event
                for event in events
                if event.get("event_type") == "scheduler_decision"
                and event.get("action") == "plan_pending"
            ]
            initial_executed = [
                event
                for event in events
                if event.get("event_type") == "scheduler_decision"
                and event.get("decision_phase") == "initial"
                and event.get("action") == "execute"
            ]
            if baseline in DEFAULT_BASELINES:
                mint_initial_plans[baseline] = (
                    tuple(_targets(initial_executed)),
                    tuple(_targets(planned)),
                )
            runtime = [
                event
                for event in events
                if event.get("event_type") == "scheduler_decision"
                and event.get("decision_phase") == "runtime_after_branch"
            ]
            warmups = [event for event in events if event.get("event_type") == "warmup"]
            target_hit_warmups = [event for event in warmups if bool(event.get("target_hit"))]
            ready_before_demand_warmups = [
                event
                for event in warmups
                if _ready_before_deadline(event)
            ]
            timely_warmups = [event for event in warmups if bool(event.get("useful"))]
            invocations = [event for event in events if event.get("event_type") == "invocation"]
            lifecycle = [event for event in events if event.get("event_type") == "intent_lifecycle"]
            accepted_lifecycle = [event for event in lifecycle if _accepted(event)]
            cancellations = [
                event
                for event in accepted_lifecycle
                if event.get("state_after") == "cancelled"
            ]
            true_cancellations = [
                event
                for event in cancellations
                if event.get("state_before") == "pending"
                and not bool(event.get("actual_call_submitted"))
            ]
            replacements = [
                event
                for event in runtime
                if event.get("action") == "replacement_warmup"
            ]
            planned_intent_ids = {str(event.get("intent_id", "")) for event in planned}
            (
                timer_wins_race,
                timer_wins_valid,
                timer_wins_targets,
                timer_wins_detail,
            ) = _legal_timer_wins_race(events, planned_intent_ids)
            if timer_wins_race and not timer_wins_valid:
                _append_violation(
                    violations,
                    "timer_wins_race_valid",
                    f"{baseline}[{workflow_index}] {timer_wins_detail}",
                )
            summary = _single_summary(events)
            row_budget = _integer(
                summary,
                "budget_limit",
                int(matrix_rows[baseline].get("budget", budget or 0)),
            )
            consumed = _integer(summary, "consumed_budget", len(warmups))
            reserved = _integer(summary, "reserved_budget", -1)
            summary_warmup_count = _integer(summary, "warmup_count", -1)
            summary_warmup_errors = _integer(summary, "warmup_error_count", -1)
            summary_scheduler_errors = _integer(summary, "scheduler_error_count", 0)
            invocation_errors = sum(_is_event_error(event) for event in invocations)
            warmup_errors = sum(_is_event_error(event) for event in warmups)
            scheduler_errors = sum(
                event.get("event_type") == "scheduler_decision"
                and event.get("action") == "scheduler_error"
                for event in events
            )
            submitted = [
                event
                for event in accepted_lifecycle
                if bool(event.get("actual_call_submitted"))
            ]
            terminals = [
                event
                for event in accepted_lifecycle
                if event.get("state_before") == "in_flight"
                and event.get("state_after") in {"succeeded", "failed"}
            ]

            if summary is None:
                _append_violation(
                    violations,
                    "workflow_summary_present",
                    f"{baseline}[{workflow_index}] has no workflow_summary",
                )
            elif summary_warmup_errors != warmup_errors:
                _append_violation(
                    violations,
                    "warmup_error_count_consistent",
                    f"{baseline}[{workflow_index}] summary={summary_warmup_errors} "
                    f"events={warmup_errors}",
                )
            if summary_scheduler_errors != scheduler_errors:
                _append_violation(
                    violations,
                    "scheduler_error_count_consistent",
                    f"{baseline}[{workflow_index}] summary={summary_scheduler_errors} "
                    f"events={scheduler_errors}",
                )
            if invocation_errors:
                _append_violation(
                    violations,
                    "no_invocation_errors",
                    f"{baseline}[{workflow_index}] invocation_errors={invocation_errors}",
                )
            if warmup_errors:
                _append_violation(
                    violations,
                    "no_warmup_errors",
                    f"{baseline}[{workflow_index}] warmup_errors={warmup_errors}",
                )
            if scheduler_errors:
                _append_violation(
                    violations,
                    "no_scheduler_errors",
                    f"{baseline}[{workflow_index}] scheduler_errors={scheduler_errors}",
                )
            for event in warmups:
                target_hit = bool(event.get("target_hit"))
                ready_before_demand = _ready_before_deadline(event)
                missed_at_demand = bool(event.get("missed_at_demand"))
                effective = bool(event.get("useful"))
                labels_valid = (
                    effective == ready_before_demand
                    and (not ready_before_demand or target_hit)
                    and (not ready_before_demand or not _is_event_error(event))
                    and not (ready_before_demand and missed_at_demand)
                    and (not missed_at_demand or target_hit)
                )
                if not labels_valid:
                    _append_violation(
                        violations,
                        "warmup_effectiveness_labels_consistent",
                        f"{baseline}[{workflow_index}] intent={event.get('intent_id')} "
                        f"target_hit={target_hit} ready={ready_before_demand} "
                        f"missed={missed_at_demand} effective={effective} "
                        f"error={_is_event_error(event)}",
                    )
            for event in lifecycle:
                event_consumed = _integer(event, "consumed_budget")
                event_reserved = _integer(event, "reserved_budget")
                if event_consumed > row_budget or event_consumed + event_reserved > row_budget:
                    _append_violation(
                        violations,
                        "consumed_within_budget",
                        f"{baseline}[{workflow_index}] lifecycle intent={event.get('intent_id')} "
                        f"reserved={event_reserved} consumed={event_consumed} budget={row_budget}",
                    )
            if consumed > row_budget:
                _append_violation(
                    violations,
                    "consumed_within_budget",
                    f"{baseline}[{workflow_index}] consumed={consumed} budget={row_budget}",
                )
            if reserved != 0:
                _append_violation(
                    violations,
                    "reserved_budget_released",
                    f"{baseline}[{workflow_index}] final reserved={reserved}",
                )
            if baseline in DEFAULT_BASELINES:
                sequences = [_sequence(event) for event in lifecycle]
                if (
                    not lifecycle
                    or any(sequence <= 0 for sequence in sequences)
                    or len(sequences) != len(set(sequences))
                ):
                    _append_violation(
                        violations,
                        "lifecycle_transition_sequence_valid",
                        f"{baseline}[{workflow_index}] transition_seq={sequences}",
                    )
                accepted_by_intent: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for event in accepted_lifecycle:
                    accepted_by_intent[str(event.get("intent_id", ""))].append(event)
                for intent_id, intent_events in accepted_by_intent.items():
                    ordered = sorted(intent_events, key=_sequence)
                    for previous, current in zip(ordered, ordered[1:]):
                        if current.get("state_before") != previous.get("state_after"):
                            _append_violation(
                                violations,
                                "lifecycle_transition_sequence_valid",
                                f"{baseline}[{workflow_index}] intent={intent_id} "
                                f"seq={_sequence(previous)}->{_sequence(current)} "
                                f"state={previous.get('state_after')}->{current.get('state_before')}",
                            )
                if not (
                    len(submitted)
                    == consumed
                    == summary_warmup_count
                    == len(warmups)
                ):
                    _append_violation(
                        violations,
                        "submitted_consumed_warmup_counts_match",
                        f"{baseline}[{workflow_index}] submitted={len(submitted)} "
                        f"consumed={consumed} summary_warmups={summary_warmup_count} "
                        f"warmup_events={len(warmups)}",
                    )
                submitted_by_intent: dict[str, list[dict[str, Any]]] = defaultdict(list)
                terminal_by_intent: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for event in submitted:
                    submitted_by_intent[str(event.get("intent_id", ""))].append(event)
                for event in terminals:
                    terminal_by_intent[str(event.get("intent_id", ""))].append(event)
                all_submitted_terminal = True
                for intent_id, submission_events in submitted_by_intent.items():
                    terminal_events = terminal_by_intent.get(intent_id, [])
                    if (
                        len(submission_events) != 1
                        or len(terminal_events) != 1
                        or _sequence(terminal_events[0]) <= _sequence(submission_events[0])
                    ):
                        all_submitted_terminal = False
                        _append_violation(
                            violations,
                            "submitted_intents_have_one_terminal",
                            f"{baseline}[{workflow_index}] intent={intent_id} "
                            f"submits={len(submission_events)} terminals={len(terminal_events)}",
                        )
                orphan_terminals = set(terminal_by_intent) - set(submitted_by_intent)
                if orphan_terminals:
                    all_submitted_terminal = False
                    _append_violation(
                        violations,
                        "submitted_intents_have_one_terminal",
                        f"{baseline}[{workflow_index}] orphan terminals={sorted(orphan_terminals)}",
                    )
                if all_submitted_terminal and not submitted_by_intent and consumed:
                    _append_violation(
                        violations,
                        "submitted_intents_have_one_terminal",
                        f"{baseline}[{workflow_index}] consumed budget without lifecycle submissions",
                    )
                latest_states: dict[str, tuple[int, str]] = {}
                for event in accepted_lifecycle:
                    intent_id = str(event.get("intent_id", ""))
                    sequence = _sequence(event)
                    if sequence > latest_states.get(intent_id, (-1, ""))[0]:
                        latest_states[intent_id] = (sequence, str(event.get("state_after", "")))
                active_at_summary = {
                    intent_id: state
                    for intent_id, (_, state) in latest_states.items()
                    if state in {"pending", "in_flight"}
                }
                if active_at_summary:
                    _append_violation(
                        violations,
                        "summary_has_no_active_intents",
                        f"{baseline}[{workflow_index}] active={active_at_summary}",
                    )
            for event in cancellations:
                if event not in true_cancellations:
                    _append_violation(
                        violations,
                        "true_cancellation_state_valid",
                        f"{baseline}[{workflow_index}] invalid cancellation intent={event.get('intent_id')} "
                        f"from={event.get('state_before')} submitted={event.get('actual_call_submitted')}",
                    )

            reveal_position = next(
                (
                    position
                    for position, event in enumerate(events)
                    if event.get("event_type") == "invocation" and event.get("observed_branch")
                ),
                None,
            )
            for planned_event in planned:
                planned_intent_id = str(planned_event.get("intent_id", ""))
                reservations = [
                    event
                    for event in accepted_lifecycle
                    if str(event.get("intent_id", "")) == planned_intent_id
                    and event.get("state_before") == "planned"
                    and event.get("state_after") == "pending"
                    and event.get("action") == "reserve"
                ]
                reservation_position = (
                    events.index(reservations[0]) if len(reservations) == 1 else None
                )
                if (
                    reveal_position is None
                    or reservation_position is None
                    or reservation_position >= reveal_position
                ):
                    _append_violation(
                        violations,
                        "pending_reserved_before_branch_reveal",
                        f"{baseline}[{workflow_index}] intent={planned_intent_id} "
                        f"reservations={len(reservations)}",
                    )
            for replacement in replacements:
                replacement_position = events.index(replacement)
                if reveal_position is None or replacement_position <= reveal_position:
                    _append_violation(
                        violations,
                        "replacement_after_branch_reveal",
                        f"{baseline}[{workflow_index}] replacement={replacement.get('logical_name')}",
                    )
                chain_valid, chain_detail = _replacement_lifecycle_chain(events, replacement)
                if not chain_valid:
                    _append_violation(
                        violations,
                        "replacement_uses_cancelled_reservation",
                        f"{baseline}[{workflow_index}] replacement={replacement.get('intent_id')} "
                        f"supersedes={replacement.get('supersedes_intent_id')}: {chain_detail}",
                    )

            planned_targets = set(_targets(planned))
            warmup_target_set = set(_targets(warmups))
            cancelled_targets = set(_targets(true_cancellations))
            replacement_targets = set(_targets(replacements))
            reachable = (
                ADAPTIVE_BRANCH_REACHABLE.get(reference_branch, set())
                if dag == "adaptive_branch"
                else set()
            )
            prediction_wrong = bool(planned_targets) and not planned_targets <= reachable
            if prediction_wrong and baseline in wrong_case_counts:
                wrong_case_counts[baseline] += 1
                baseline_totals[baseline]["wrong_prediction_cases"] += 1
                semantic_ok, expected = _wrong_prediction_semantics(
                    baseline,
                    planned_targets,
                    warmup_target_set,
                    cancelled_targets,
                    replacement_targets,
                    reachable,
                    consumed,
                    row_budget,
                    timer_wins_race and timer_wins_valid,
                )
                if not semantic_ok:
                    baseline_totals[baseline]["wrong_prediction_semantic_failures"] += 1
                    _append_violation(
                        violations,
                        "adaptive_wrong_prediction_semantics",
                        f"{baseline}[{workflow_index}]: {expected}",
                    )
            elif baseline in correct_case_counts:
                correct_case_counts[baseline] += 1
                baseline_totals[baseline]["correct_prediction_cases"] += 1
                semantic_ok, expected = _correct_prediction_semantics(
                    planned_targets,
                    warmup_target_set,
                    cancelled_targets,
                    replacement_targets,
                    reachable,
                    consumed,
                    row_budget,
                )
                if not semantic_ok:
                    baseline_totals[baseline][
                        "correct_prediction_semantic_failures"
                    ] += 1
                    _append_violation(
                        violations,
                        "adaptive_correct_prediction_semantics",
                        f"{baseline}[{workflow_index}]: {expected}",
                    )

            baseline_totals[baseline]["actual_warmups"] += len(warmups)
            baseline_totals[baseline]["target_hit_warmups"] += len(target_hit_warmups)
            baseline_totals[baseline]["ready_before_demand_warmups"] += len(
                ready_before_demand_warmups
            )
            baseline_totals[baseline]["timely_warmups"] += len(timely_warmups)
            baseline_totals[baseline]["warmups"] += len(warmups)
            baseline_totals[baseline]["useful_warmups"] += len(timely_warmups)
            baseline_totals[baseline]["true_pending_cancellations"] += len(true_cancellations)
            baseline_totals[baseline]["replacements"] += len(replacements)
            baseline_totals[baseline]["scheduler_errors"] += scheduler_errors
            baseline_totals[baseline]["warmup_failures_exercised"] += warmup_errors
            baseline_totals[baseline]["timer_wins_races"] += int(
                timer_wins_race and timer_wins_valid
            )
            prefix = f"{baseline}_"
            row_out.update(
                {
                    prefix + "plan_pending": _join(_targets(planned)),
                    prefix + "prediction_wrong": prediction_wrong,
                    prefix + "runtime_actions": _runtime_action_text(runtime),
                    prefix + "warmups": _join(_targets(warmups)),
                    prefix + "warmup_useful_flags": "/".join(
                        "true" if bool(event.get("useful")) else "false" for event in warmups
                    ),
                    prefix + "warmup_count": len(warmups),
                    prefix + "target_hit_warmups": _join(_targets(target_hit_warmups)),
                    prefix + "target_hit_warmup_count": len(target_hit_warmups),
                    prefix + "ready_before_demand_warmups": _join(
                        _targets(ready_before_demand_warmups)
                    ),
                    prefix + "ready_before_demand_warmup_count": len(
                        ready_before_demand_warmups
                    ),
                    prefix + "timely_warmups": _join(_targets(timely_warmups)),
                    prefix + "timely_warmup_count": len(timely_warmups),
                    # Compatibility alias; this means timely local completion,
                    # not verified AWS execution-environment reuse.
                    prefix + "useful_warmups": _join(_targets(timely_warmups)),
                    prefix + "useful_warmup_count": len(timely_warmups),
                    prefix + "true_pending_cancelled": _join(_targets(true_cancellations)),
                    prefix + "true_pending_cancel_count": len(true_cancellations),
                    prefix + "replacements": _join(_targets(replacements)),
                    prefix + "replacement_count": len(replacements),
                    prefix + "consumed_budget": consumed,
                    prefix + "reserved_budget": reserved,
                    prefix + "budget_limit": row_budget,
                    prefix + "invocation_errors": invocation_errors,
                    prefix + "warmup_errors": warmup_errors,
                    prefix + "scheduler_errors": scheduler_errors,
                    prefix + "timer_wins_race": timer_wins_race and timer_wins_valid,
                    prefix + "timer_wins_targets": _join(sorted(timer_wins_targets)),
                }
            )
        comparable_plans = {
            baseline: plan
            for baseline, plan in mint_initial_plans.items()
            if baseline in selected_baselines
        }
        if len(comparable_plans) > 1 and len(set(comparable_plans.values())) != 1:
            _append_violation(
                violations,
                "same_initial_ablation_plan",
                f"workflow_index={workflow_index}: {comparable_plans}",
            )
        paired_rows.append(row_out)

    if dag == "adaptive_branch":
        for baseline, count in wrong_case_counts.items():
            if count == 0:
                _append_violation(
                    violations,
                    "adaptive_wrong_prediction_cases_present",
                    f"{baseline}: no wrong-prediction request was observed",
                )

    manifest_path = root / "experiment_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    dry_run_value = manifest.get("dry_run")
    if dry_run_value is True:
        execution_mode = "dry_run"
        latency_warning = (
            "Dry-run latency is simulated/control-path timing and must not be used for paper "
            "performance conclusions. This audit validates decisions, lifecycle, and budgets only."
        )
        paper_performance_eligible: bool | None = False
    elif dry_run_value is False:
        execution_mode = "real"
        latency_warning = (
            "This lifecycle audit does not by itself validate AWS latency; CloudWatch and "
            "execution-environment truth audits are still required."
        )
        paper_performance_eligible = None
    else:
        execution_mode = "unknown"
        latency_warning = (
            "Execution mode is unknown, so latency must not be used for paper performance conclusions."
        )
        paper_performance_eligible = False

    gate_names = (
        "same_budget_limit",
        "expected_workflow_count",
        "branch_trace_complete",
        "same_branch_trace",
        "same_history_snapshots",
        "same_initial_ablation_plan",
        "workflow_summary_present",
        "no_invocation_errors",
        "no_warmup_errors",
        "no_scheduler_errors",
        "warmup_error_count_consistent",
        "scheduler_error_count_consistent",
        "warmup_effectiveness_labels_consistent",
        "consumed_within_budget",
        "reserved_budget_released",
        "lifecycle_transition_sequence_valid",
        "submitted_consumed_warmup_counts_match",
        "submitted_intents_have_one_terminal",
        "summary_has_no_active_intents",
        "true_cancellation_state_valid",
        "pending_reserved_before_branch_reveal",
        "replacement_after_branch_reveal",
        "replacement_uses_cancelled_reservation",
        "timer_wins_race_valid",
        "adaptive_wrong_prediction_cases_present",
        "adaptive_wrong_prediction_semantics",
        "adaptive_correct_prediction_semantics",
    )
    quality_gates = {name: not violations.get(name) for name in gate_names}
    quality_gate_failures = [name for name, passed in quality_gates.items() if not passed]
    return {
        "analysis_type": "paired_pending_intent_ablation_audit",
        "dag": dag,
        "baselines": list(selected_baselines),
        "expected_runs": expected_runs,
        "branch_traces": branch_traces,
        "execution_evidence": {
            "mode": execution_mode,
            "paper_performance_eligible": paper_performance_eligible,
            "latency_warning": latency_warning,
        },
        "coverage_evidence": {
            "timer_wins_race_cases": sum(
                totals["timer_wins_races"] for totals in baseline_totals.values()
            ),
            "warmup_failure_cases": sum(
                totals["warmup_failures_exercised"]
                for totals in baseline_totals.values()
            ),
            "scheduler_error_cases": sum(
                totals["scheduler_errors"] for totals in baseline_totals.values()
            ),
            "zero_case_note": (
                "A zero count means this pilot did not exercise the path; deterministic "
                "unit tests, not this report, provide coverage for zero-count race/failure paths."
            ),
        },
        "quality_gates": quality_gates,
        "quality_gate_failures": quality_gate_failures,
        "quality_gate_passed": not quality_gate_failures,
        "quality_gate_details": dict(violations),
        "baseline_totals": baseline_totals,
        "paired_rows": paired_rows,
    }


def write_report(
    root: Path,
    report: dict[str, Any],
    *,
    output_csv: Path | None = None,
    output_json: Path | None = None,
) -> tuple[Path, Path]:
    root = Path(root)
    csv_path = output_csv or root / "paired_intent_ablation.csv"
    json_path = output_json or root / "intent_ablation_report.json"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    rows = report["paired_rows"]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)
    return csv_path, json_path


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = analyze(
        args.results_root,
        expected_runs=args.expected_runs,
        dag=args.dag,
        budget=args.budget,
        baselines=args.baselines,
        include_xanadu=args.include_xanadu,
    )
    csv_path, json_path = write_report(
        args.results_root,
        report,
        output_csv=args.output_csv,
        output_json=args.output_json,
    )
    compact = {key: value for key, value in report.items() if key not in {"paired_rows", "branch_traces"}}
    print(json.dumps(compact, indent=2, sort_keys=True))
    print(f"Wrote {csv_path}")
    print(f"Wrote {json_path}")
    return 0 if report["quality_gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
