from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


CORE_BASELINES = (
    "no_warmup",
    "orion_like",
    "xanadu_like",
    "mint_markov_full",
    "oracle_path",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a paired per-request strategy decision report.")
    parser.add_argument("--results-root", required=True, type=Path)
    parser.add_argument("--expected-runs", type=int, default=20)
    parser.add_argument("--switch-index", type=int, default=10)
    parser.add_argument("--hot-target", default="f5")
    parser.add_argument("--budget", type=int, default=2)
    return parser.parse_args(argv)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_outputs(root: Path) -> dict[str, Path]:
    rows = list(csv.DictReader((root / "summary_matrix.csv").open(newline="", encoding="utf-8")))
    outputs = {row["baseline"]: Path(row["output_dir"]) for row in rows if row["baseline"] in CORE_BASELINES}
    missing = sorted(set(CORE_BASELINES) - set(outputs))
    if missing:
        raise ValueError(f"missing core baseline outputs: {', '.join(missing)}")
    return outputs


def _by_index(events: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        index = int(event.get("workflow_index", -1))
        if index < 0:
            raise ValueError("event missing non-negative workflow_index")
        grouped[index].append(event)
    return dict(grouped)


def _initial_targets(events: list[dict[str, Any]]) -> list[str]:
    return sorted({
        str(event["logical_name"])
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("decision_phase") == "initial"
        and event.get("action") == "execute"
    })


def _runtime_targets(events: list[dict[str, Any]]) -> list[str]:
    return sorted({
        str(event["logical_name"])
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "replacement_warmup"
    })


def _invalidated_targets(events: list[dict[str, Any]]) -> list[str]:
    return sorted({
        str(event["logical_name"])
        for event in events
        if event.get("event_type") == "scheduler_decision"
        and event.get("action") == "invalidate_executed"
    })


def _actual_branch(events: list[dict[str, Any]]) -> str:
    for event in events:
        if event.get("event_type") == "invocation" and event.get("observed_branch"):
            return str(event["observed_branch"])
    for event in events:
        if event.get("event_type") == "branch_model" and event.get("observed_branch"):
            return str(event["observed_branch"])
    return ""


def _format_stage(initial: list[str], runtime: list[str], invalidated: list[str]) -> str:
    parts = [f"initial:{'/'.join(initial) if initial else '-'}"]
    if invalidated:
        parts.append(f"invalidate:{'/'.join(invalidated)}")
    if runtime:
        parts.append(f"runtime:{'/'.join(runtime)}")
    return " -> ".join(parts)


def _first_consecutive(indices: list[int], selected: dict[int, set[str]], target: str, length: int = 3) -> int | None:
    for index in indices:
        if all(target in selected.get(candidate, set()) for candidate in range(index, index + length)):
            return index
    return None


def analyze(root: Path, expected_runs: int, switch_index: int, hot_target: str, budget: int) -> dict[str, Any]:
    outputs = _load_outputs(root)
    grouped = {baseline: _by_index(_read_jsonl(path / "events.jsonl")) for baseline, path in outputs.items()}
    expected_indices = set(range(expected_runs))
    for baseline, rows in grouped.items():
        if set(rows) != expected_indices:
            raise ValueError(f"{baseline} workflow indices differ: {sorted(rows)}")

    trace_by_baseline = {
        baseline: [_actual_branch(rows[index]) for index in range(expected_runs)]
        for baseline, rows in grouped.items()
    }
    reference_trace = trace_by_baseline[CORE_BASELINES[0]]
    trace_equal = all(trace == reference_trace for trace in trace_by_baseline.values())
    if not trace_equal:
        raise ValueError("strategies did not observe the same branch trace")

    model_snapshots: dict[str, list[tuple[str, str]]] = {}
    for baseline, rows in grouped.items():
        snapshots = []
        for index in range(expected_runs):
            event = next(
                (
                    item for item in rows[index]
                    if item.get("event_type") == "branch_model" and item.get("decision_phase") == "initial"
                ),
                None,
            )
            snapshots.append((str(event.get("branch_counts", "{}")), str(event.get("branch_probabilities", "{}"))) if event else ("{}", "{}"))
        model_snapshots[baseline] = snapshots
    history_snapshots_equal = all(
        snapshots == model_snapshots[CORE_BASELINES[0]] for snapshots in model_snapshots.values()
    )
    if not history_snapshots_equal:
        raise ValueError("strategy histories do not start/evolve from identical snapshots")

    paired_rows: list[dict[str, Any]] = []
    selected_by_strategy: dict[str, dict[int, set[str]]] = {baseline: {} for baseline in CORE_BASELINES}
    initial_by_strategy: dict[str, dict[int, set[str]]] = {baseline: {} for baseline in CORE_BASELINES}
    metrics: dict[str, dict[str, int]] = {
        baseline: {"warmups": 0, "useful": 0, "wasted": 0, "invalidated": 0, "replacements": 0}
        for baseline in CORE_BASELINES
    }
    phase_metrics = {
        baseline: {
            "pre_switch": {"warmups": 0, "useful": 0, "wasted": 0},
            "post_switch": {"warmups": 0, "useful": 0, "wasted": 0},
        }
        for baseline in CORE_BASELINES
    }
    warmup_error_count = 0
    runtime_order_valid = True
    for index in range(expected_runs):
        row: dict[str, Any] = {"workflow_index": index, "phase": "pre_switch" if index < switch_index else "post_switch", "actual_branch": reference_trace[index]}
        for baseline in CORE_BASELINES:
            events = grouped[baseline][index]
            initial = _initial_targets(events)
            runtime = _runtime_targets(events)
            invalidated = _invalidated_targets(events)
            final_targets = set(initial) - set(invalidated) | set(runtime)
            initial_by_strategy[baseline][index] = set(initial)
            selected_by_strategy[baseline][index] = final_targets
            warmups = [event for event in events if event.get("event_type") == "warmup"]
            warmup_error_count += sum(event.get("status") == "error" for event in warmups)
            phase = "pre_switch" if index < switch_index else "post_switch"
            metrics[baseline]["warmups"] += len(warmups)
            metrics[baseline]["useful"] += sum(bool(event.get("useful")) for event in warmups)
            metrics[baseline]["wasted"] += sum(not bool(event.get("useful")) for event in warmups)
            metrics[baseline]["invalidated"] += len(invalidated)
            metrics[baseline]["replacements"] += len(runtime)
            phase_metrics[baseline][phase]["warmups"] += len(warmups)
            phase_metrics[baseline][phase]["useful"] += sum(bool(event.get("useful")) for event in warmups)
            phase_metrics[baseline][phase]["wasted"] += sum(not bool(event.get("useful")) for event in warmups)
            if runtime:
                reveal_position = next(
                    (position for position, event in enumerate(events) if event.get("event_type") == "invocation" and event.get("observed_branch")),
                    None,
                )
                replacement_position = next(
                    (position for position, event in enumerate(events) if event.get("event_type") == "scheduler_decision" and event.get("action") == "replacement_warmup"),
                    None,
                )
                runtime_order_valid &= reveal_position is not None and replacement_position is not None and reveal_position < replacement_position
            row[f"{baseline}_initial"] = "/".join(initial)
            row[f"{baseline}_invalidated"] = "/".join(invalidated)
            row[f"{baseline}_runtime"] = "/".join(runtime)
            row[f"{baseline}_stage_targets"] = _format_stage(initial, runtime, invalidated)
            row[f"{baseline}_final_targets"] = "/".join(sorted(final_targets))
            if len(warmups) > budget:
                raise ValueError(f"{baseline} exceeded budget at workflow_index={index}: {len(warmups)} > {budget}")
        paired_rows.append(row)

    xanadu = selected_by_strategy["xanadu_like"]
    mint = selected_by_strategy["mint_markov_full"]
    oracle = selected_by_strategy["oracle_path"]
    target_disagreements = sum(xanadu[index] != mint[index] for index in range(expected_runs))
    initial_disagreements = sum(
        initial_by_strategy["xanadu_like"][index] != initial_by_strategy["mint_markov_full"][index]
        for index in range(expected_runs)
    )
    branch_nodes = {"f2", "f3", "f4", "f5"}
    branch_choice_disagreements = sum(
        (initial_by_strategy["xanadu_like"][index] & branch_nodes)
        != (initial_by_strategy["mint_markov_full"][index] & branch_nodes)
        for index in range(expected_runs)
    )
    stage_disagreements = sum(
        paired_rows[index]["xanadu_like_stage_targets"] != paired_rows[index]["mint_markov_full_stage_targets"]
        for index in range(expected_runs)
    )
    post_indices = list(range(switch_index, expected_runs))
    strategy_stats: dict[str, Any] = {}
    for baseline in CORE_BASELINES:
        selected = selected_by_strategy[baseline]
        initial_selected = initial_by_strategy[baseline]
        first_hot = next((index for index in post_indices if hot_target in initial_selected[index]), None)
        strategy_stats[baseline] = {
            **metrics[baseline],
            "local_target_hit_rate": metrics[baseline]["useful"] / metrics[baseline]["warmups"] if metrics[baseline]["warmups"] else 0.0,
            "phase_metrics": phase_metrics[baseline],
            "first_post_switch_hot_target_index": first_hot,
            "first_three_consecutive_hot_target_index": _first_consecutive(post_indices, initial_selected, hot_target),
            "post_switch_hot_target_ratio": sum(hot_target in initial_selected[index] for index in post_indices) / len(post_indices) if post_indices else 0.0,
            "mean_target_set_distance_to_oracle": sum(len(selected[index] ^ oracle[index]) for index in range(expected_runs)) / expected_runs,
        }

    if warmup_error_count:
        raise ValueError(f"warmup failures found in paired pilot: {warmup_error_count}")
    if not runtime_order_valid:
        raise ValueError("runtime replacement occurred before its branch reveal")

    return {
        "quality_gates": {
            "same_branch_trace": trace_equal,
            "same_history_snapshots": history_snapshots_equal,
            "no_warmup_errors": warmup_error_count == 0,
            "runtime_replanning_after_branch_reveal": runtime_order_valid,
            "expected_runs": expected_runs,
            "budget": budget,
        },
        "branch_trace": reference_trace,
        "strategy_stats": strategy_stats,
        "mint_xanadu": {
            "target_disagreement_count": target_disagreements,
            "target_disagreement_ratio": target_disagreements / expected_runs,
            "stage_aware_disagreement_count": stage_disagreements,
            "stage_aware_disagreement_ratio": stage_disagreements / expected_runs,
            "initial_set_disagreement_count": initial_disagreements,
            "initial_set_disagreement_ratio": initial_disagreements / expected_runs,
            "predicted_branch_disagreement_count": branch_choice_disagreements,
            "predicted_branch_disagreement_ratio": branch_choice_disagreements / expected_runs,
        },
        "paired_rows": paired_rows,
    }


def write_report(root: Path, report: dict[str, Any]) -> None:
    json_path = root / "strategy_difference_report.json"
    csv_path = root / "paired_strategy_decisions.csv"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    rows = report["paired_rows"]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)
    print(json.dumps({key: value for key, value in report.items() if key != "paired_rows"}, indent=2, sort_keys=True))
    print(f"Wrote {csv_path}")
    print(f"Wrote {json_path}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = analyze(args.results_root, args.expected_runs, args.switch_index, args.hot_target, args.budget)
    write_report(args.results_root, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
