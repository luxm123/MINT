from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd


REFERENCE_BASELINES = ["static_dag", "orion_like", "mint_markov_full"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze warmup target overlap across MINT baselines.")
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _targets_from_events(events_path: Path) -> tuple[set[str], int]:
    events = _read_jsonl(events_path)
    warmups = [event for event in events if event.get("event_type") == "warmup"]
    targets = {str(event.get("logical_name") or event.get("function_name")) for event in warmups}
    return targets, len(warmups)


def _jaccard(a: set[str], b: set[str]) -> float:
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def analyze_overlap(results_dir: str | Path, output_dir: str | Path) -> dict[str, Path]:
    root = Path(results_dir)
    matrix_csv = root / "summary_matrix.csv"
    if not matrix_csv.exists():
        raise FileNotFoundError(f"Missing summary matrix: {matrix_csv}")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    matrix = pd.read_csv(matrix_csv)

    target_rows = []
    target_sets: dict[tuple[str, int, str], set[str]] = {}
    for row in matrix.itertuples(index=False):
        dag = str(row.dag)
        baseline = str(row.baseline)
        budget = int(row.budget)
        events_path = Path(str(row.output_dir)) / "events.jsonl"
        if not events_path.is_absolute():
            events_path = ROOT / events_path
        targets, total_warmup = _targets_from_events(events_path)
        key = (dag, budget, baseline)
        target_sets[key] = targets
        target_rows.append(
            {
                "dag": dag,
                "budget": budget,
                "baseline": baseline,
                "warmup_targets": " ".join(sorted(targets)),
                "total_warmup": total_warmup,
            }
        )

    overlap_rows = []
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
                    targets_a = target_sets.get((str(dag), budget, pair[0]), set())
                    targets_b = target_sets.get((str(dag), budget, pair[1]), set())
                    score = _jaccard(targets_a, targets_b)
                    row = {
                        "dag": dag,
                        "budget": budget,
                        "baseline_a": pair[0],
                        "baseline_b": pair[1],
                        "jaccard_overlap": round(score, 6),
                        "targets_a": " ".join(sorted(targets_a)),
                        "targets_b": " ".join(sorted(targets_b)),
                    }
                    overlap_rows.append(row)
                    if score > 0.9:
                        warnings.append(row)

    targets_path = out / "baseline_warmup_targets.csv"
    overlap_path = out / "baseline_overlap.csv"
    report_path = out / "overlap_report.txt"
    with targets_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["dag", "budget", "baseline", "warmup_targets", "total_warmup"])
        writer.writeheader()
        writer.writerows(target_rows)
    with overlap_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["dag", "budget", "baseline_a", "baseline_b", "jaccard_overlap", "targets_a", "targets_b"])
        writer.writeheader()
        writer.writerows(overlap_rows)
    lines = [
        f"Analyzed result root: {root}",
        f"Configurations: {len(target_rows)}",
        f"Overlap rows: {len(overlap_rows)}",
        f"High-overlap warnings (>0.9): {len(warnings)}",
        "",
    ]
    for warning in warnings:
        lines.append(
            "WARNING high overlap: "
            f"dag={warning['dag']} budget={warning['budget']} "
            f"{warning['baseline_a']} vs {warning['baseline_b']} "
            f"jaccard={warning['jaccard_overlap']}"
        )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return {"targets": targets_path, "overlap": overlap_path, "report": report_path}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = analyze_overlap(args.results_dir, args.output_dir)
    for path in paths.values():
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
