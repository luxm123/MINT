from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and summarize a controlled-cold paired experiment.")
    parser.add_argument("--results-root", required=True, type=Path)
    parser.add_argument("--baselines", nargs=2, default=["no_warmup", "mint_markov_full"])
    parser.add_argument("--expected-blocks", required=True, type=int)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def _csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _summary(values: list[float]) -> dict[str, float]:
    return {
        "mean": statistics.mean(values),
        "p50": statistics.median(values),
        "p90": _percentile(values, 0.90),
        "p95": _percentile(values, 0.95),
        "min": min(values),
        "max": max(values),
    }


def analyze(root: Path, baselines: list[str], expected_blocks: int) -> dict[str, Any]:
    failures: list[str] = []
    validation = _csv(root / "cold_validation.csv")
    reset_rows = _csv(root / "reset_log.csv")
    if len(validation) != expected_blocks * len(baselines):
        failures.append(f"cold_validation_rows={len(validation)} expected={expected_blocks * len(baselines)}")
    invalid = [row for row in validation if row.get("valid", "").lower() != "true" or row.get("reason") != "valid"]
    if invalid:
        failures.append(f"invalid_cold_validations={len(invalid)}")

    hashes = {row.get("code_sha256", "") for row in reset_rows}
    if "" in hashes or len(hashes) != 1:
        failures.append(f"code_hashes={sorted(hashes)}")
    qualified = [row.get("qualified_function_name", "") for row in reset_rows]
    if any(":" not in name for name in qualified):
        failures.append("unqualified_function_names_present")
    version_keys = [(row.get("function_name"), row.get("published_version")) for row in reset_rows]
    if len(version_keys) != len(set(version_keys)):
        failures.append("published_version_reused_within_function")

    runs_by_baseline: dict[str, dict[str, dict[str, str]]] = {}
    baseline_summary: dict[str, Any] = {}
    for baseline in baselines:
        runs = _csv(root / baseline / "runs.csv")
        if len(runs) != expected_blocks:
            failures.append(f"{baseline}.runs={len(runs)} expected={expected_blocks}")
        by_block = {row["block_id"]: row for row in runs}
        if len(by_block) != len(runs):
            failures.append(f"{baseline}.duplicate_block_ids")
        runs_by_baseline[baseline] = by_block

        events = _jsonl(root / baseline / "events.jsonl")
        calls = [event for event in events if event.get("event_type") in {"invocation", "warmup"}]
        errors = [event for event in calls if event.get("status") != "ok"]
        if errors:
            failures.append(f"{baseline}.call_errors={len(errors)}")
        audit = _csv(root / baseline / "cloudwatch_audit.csv")
        audit_failures = [row for row in audit if row.get("audit_ok", "").lower() != "true"]
        if len(audit) != len(calls):
            failures.append(f"{baseline}.audit_rows={len(audit)} calls={len(calls)}")
        if audit_failures:
            failures.append(f"{baseline}.audit_failures={len(audit_failures)}")

        latency = [float(row["latency_ms"]) for row in runs]
        lateness = [float(row["arrival_lateness_ms"]) for row in runs]
        overruns = [float(row["warmup_overrun_ms"]) for row in runs]
        warmups = [event for event in calls if event.get("event_type") == "warmup"]
        baseline_summary[baseline] = {
            "n": len(runs),
            "latency_ms": _summary(latency),
            "cold_start_total": sum(int(row["cold_start_count"]) for row in runs),
            "warmup_total": sum(int(row["warmup_count"]) for row in runs),
            "useful_warmup_total": sum(bool(event.get("useful")) for event in warmups),
            "arrival_lateness_ms": _summary(lateness),
            "positive_warmup_overruns": sum(value > 0 for value in overruns),
            "max_warmup_overrun_ms": max(overruns),
            "call_types": dict(Counter(event.get("event_type") for event in calls)),
            "call_errors": len(errors),
            "cloudwatch_audit_failures": len(audit_failures),
        }

    common = set(runs_by_baseline[baselines[0]])
    for baseline in baselines[1:]:
        common &= set(runs_by_baseline[baseline])
    if len(common) != expected_blocks:
        failures.append(f"paired_blocks={len(common)} expected={expected_blocks}")

    comparison, treatment = baselines
    deltas = []
    treatment_slots: Counter[int] = Counter()
    slot_deltas: dict[int, list[float]] = {}
    for block_id in sorted(common):
        control_row = runs_by_baseline[comparison][block_id]
        treatment_row = runs_by_baseline[treatment][block_id]
        delta = float(treatment_row["latency_ms"]) - float(control_row["latency_ms"])
        deltas.append(delta)
        order = treatment_row["strategy_order"].split(",")
        slot = order.index(treatment)
        treatment_slots[slot] += 1
        slot_deltas.setdefault(slot, []).append(delta)

    if expected_blocks % 2 == 0 and sorted(treatment_slots.values()) != [expected_blocks // 2] * 2:
        failures.append(f"unbalanced_strategy_order={dict(treatment_slots)}")

    paired = {
        "comparison": f"{treatment} - {comparison}",
        "n": len(deltas),
        "latency_delta_ms": _summary(deltas),
        "treatment_faster_count": sum(delta < 0 for delta in deltas),
        "treatment_slots": dict(treatment_slots),
        "slot_mean_delta_ms": {str(slot): statistics.mean(values) for slot, values in slot_deltas.items()},
    }
    return {
        "quality_gate_passed": not failures,
        "quality_gate_failures": failures,
        "expected_blocks": expected_blocks,
        "code_sha256": next(iter(hashes)) if len(hashes) == 1 else "",
        "baselines": baseline_summary,
        "paired": paired,
    }


def main() -> int:
    args = parse_args()
    report = analyze(args.results_root, args.baselines, args.expected_blocks)
    text = json.dumps(report, indent=2, sort_keys=True)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    return 0 if report["quality_gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
