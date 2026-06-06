from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd


REFERENCE_BASELINES = ["static_dag", "orion_like", "mint_markov_full"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze warmup target, frequency, timing, and action overlap across baselines.")
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--time-bucket-sec", type=float, default=1.0)
    return parser.parse_args(argv)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _events_path(output_dir: str) -> Path:
    path = Path(output_dir) / "events.jsonl"
    return path if path.is_absolute() else ROOT / path


def _time_bucket(value: Any, bucket_sec: float) -> int:
    try:
        return int(float(value) // max(bucket_sec, 0.001))
    except (TypeError, ValueError):
        return -1


def _warmup_records(events_path: Path, bucket_sec: float) -> list[dict[str, Any]]:
    events = _read_jsonl(events_path)
    scheduler_by_intent = {
        event.get("intent_id"): event
        for event in events
        if event.get("event_type") == "scheduler_decision" and event.get("intent_id")
    }
    records = []
    for index, event in enumerate(events):
        if event.get("event_type") != "warmup":
            continue
        decision = scheduler_by_intent.get(event.get("intent_id"), {})
        planned = event.get("planned_time_sec", decision.get("planned_time_sec"))
        target = str(event.get("logical_name") or event.get("function_name"))
        action = str(event.get("action", "execute"))
        records.append(
            {
                "sequence_index": index,
                "logical_name": target,
                "function_name": event.get("function_name", target),
                "action": action,
                "action_reason": event.get("action_reason", decision.get("action_reason", "")),
                "planned_time_sec": planned,
                "planned_time_bucket": _time_bucket(planned, bucket_sec),
                "timestamp": event.get("timestamp", ""),
                "gain": event.get("gain", decision.get("gain", "")),
                "useful": event.get("useful", ""),
                "served_after_delay": event.get("served_after_delay", False),
            }
        )
    return records


def _jaccard(a: set[Any], b: set[Any]) -> float:
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def _frequency_overlap(a: Counter, b: Counter) -> float:
    keys = set(a) | set(b)
    denominator = sum(max(a.get(key, 0), b.get(key, 0)) for key in keys)
    if denominator == 0:
        return 1.0
    numerator = sum(min(a.get(key, 0), b.get(key, 0)) for key in keys)
    return numerator / denominator


def _action_summary(records: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(record["action"] for record in records)
    return {
        "execute_count": counts.get("execute", 0),
        "cancel_count": counts.get("cancel", 0),
        "replace_count": counts.get("replace", 0),
        "delay_count": counts.get("delay", 0),
        "delayed_execute_count": counts.get("delayed_execute", 0),
    }


def analyze_overlap(results_dir: str | Path, output_dir: str | Path, time_bucket_sec: float = 1.0) -> dict[str, Path]:
    root = Path(results_dir)
    matrix_csv = root / "summary_matrix.csv"
    if not matrix_csv.exists():
        raise FileNotFoundError(f"Missing summary matrix: {matrix_csv}")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    matrix = pd.read_csv(matrix_csv)

    target_rows = []
    sequence_rows = []
    target_sets: dict[tuple[str, int, str], set[str]] = {}
    timing_sets: dict[tuple[str, int, str], set[tuple[str, int]]] = {}
    frequency_vectors: dict[tuple[str, int, str], Counter] = {}

    for row in matrix.itertuples(index=False):
        dag = str(row.dag)
        baseline = str(row.baseline)
        budget = int(row.budget)
        records = _warmup_records(_events_path(str(row.output_dir)), time_bucket_sec)
        key = (dag, budget, baseline)
        targets = {record["logical_name"] for record in records}
        target_sets[key] = targets
        timing_sets[key] = {(record["logical_name"], record["planned_time_bucket"]) for record in records}
        frequency_vectors[key] = Counter(record["logical_name"] for record in records)
        summary = _action_summary(records)
        target_rows.append(
            {
                "dag": dag,
                "budget": budget,
                "baseline": baseline,
                "warmup_targets": " ".join(sorted(targets)),
                "total_warmup": len(records),
                **summary,
            }
        )
        for record in records:
            sequence_rows.append({"dag": dag, "budget": budget, "baseline": baseline, **record})

    overlap_rows = []
    timing_rows = []
    frequency_rows = []
    warnings = []
    for dag in sorted(matrix["dag"].dropna().unique()):
        for budget in sorted(int(value) for value in matrix[matrix["dag"] == dag]["budget"].dropna().unique()):
            baselines = sorted(str(value) for value in matrix[(matrix["dag"] == dag) & (matrix["budget"] == budget)]["baseline"].unique())
            pairs = set()
            for baseline in baselines:
                for reference in REFERENCE_BASELINES:
                    if baseline == reference or reference not in baselines:
                        continue
                    pair = tuple(sorted([baseline, reference]))
                    if pair in pairs:
                        continue
                    pairs.add(pair)
                    key_a = (str(dag), budget, pair[0])
                    key_b = (str(dag), budget, pair[1])
                    targets_a = target_sets.get(key_a, set())
                    targets_b = target_sets.get(key_b, set())
                    timing_a = timing_sets.get(key_a, set())
                    timing_b = timing_sets.get(key_b, set())
                    freq_a = frequency_vectors.get(key_a, Counter())
                    freq_b = frequency_vectors.get(key_b, Counter())
                    target_score = _jaccard(targets_a, targets_b)
                    timing_score = _jaccard(timing_a, timing_b)
                    freq_score = _frequency_overlap(freq_a, freq_b)
                    overlap_row = {
                        "dag": dag,
                        "budget": budget,
                        "baseline_a": pair[0],
                        "baseline_b": pair[1],
                        "jaccard_overlap": round(target_score, 6),
                        "targets_a": " ".join(sorted(targets_a)),
                        "targets_b": " ".join(sorted(targets_b)),
                    }
                    overlap_rows.append(overlap_row)
                    timing_rows.append(
                        {
                            "dag": dag,
                            "budget": budget,
                            "baseline_a": pair[0],
                            "baseline_b": pair[1],
                            "timing_jaccard_overlap": round(timing_score, 6),
                            "timing_keys_a": " ".join(f"{fn}@{bucket}" for fn, bucket in sorted(timing_a)),
                            "timing_keys_b": " ".join(f"{fn}@{bucket}" for fn, bucket in sorted(timing_b)),
                        }
                    )
                    frequency_rows.append(
                        {
                            "dag": dag,
                            "budget": budget,
                            "baseline_a": pair[0],
                            "baseline_b": pair[1],
                            "frequency_overlap": round(freq_score, 6),
                            "frequency_a": " ".join(f"{key}:{freq_a[key]}" for key in sorted(freq_a)),
                            "frequency_b": " ".join(f"{key}:{freq_b[key]}" for key in sorted(freq_b)),
                        }
                    )
                    if target_score > 0.9:
                        warnings.append((overlap_row, timing_score, freq_score))

    targets_path = out / "baseline_warmup_targets.csv"
    sequence_path = out / "baseline_warmup_sequence.csv"
    overlap_path = out / "baseline_overlap.csv"
    timing_path = out / "baseline_timing_overlap.csv"
    frequency_path = out / "baseline_frequency_overlap.csv"
    report_path = out / "overlap_report.txt"

    pd.DataFrame(target_rows).to_csv(targets_path, index=False)
    pd.DataFrame(sequence_rows).to_csv(sequence_path, index=False)
    pd.DataFrame(overlap_rows).to_csv(overlap_path, index=False)
    pd.DataFrame(timing_rows).to_csv(timing_path, index=False)
    pd.DataFrame(frequency_rows).to_csv(frequency_path, index=False)

    lines = [
        f"Analyzed result root: {root}",
        f"Configurations: {len(target_rows)}",
        f"Target-overlap rows: {len(overlap_rows)}",
        f"High target-set overlap warnings (>0.9): {len(warnings)}",
        "Target-set overlap alone can be misleading; inspect frequency and timing overlap before concluding two baselines are behaviorally identical.",
        "",
    ]
    for warning, timing_score, freq_score in warnings:
        lines.append(
            "WARNING high overlap (target-set): "
            f"dag={warning['dag']} budget={warning['budget']} "
            f"{warning['baseline_a']} vs {warning['baseline_b']} "
            f"target_jaccard={warning['jaccard_overlap']} "
            f"timing_jaccard={round(timing_score, 6)} "
            f"frequency_overlap={round(freq_score, 6)}"
        )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "targets": targets_path,
        "sequence": sequence_path,
        "overlap": overlap_path,
        "timing": timing_path,
        "frequency": frequency_path,
        "report": report_path,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = analyze_overlap(args.results_dir, args.output_dir, args.time_bucket_sec)
    for path in paths.values():
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
