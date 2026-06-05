from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.metrics import compute_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize MINT results.")
    parser.add_argument("--results-dir", default="results")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results_dir = Path(args.results_dir)
    summaries = []
    for events_path in sorted(results_dir.glob("**/events.jsonl")):
        summary = compute_summary(events_path)
        summary["events_path"] = str(events_path)
        summaries.append(summary)

    json_path = results_dir / "summary.json"
    csv_path = results_dir / "summary.csv"
    results_dir.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(summaries, fh, indent=2, sort_keys=True)
    if summaries:
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=sorted(summaries[0].keys()))
            writer.writeheader()
            writer.writerows(summaries)
    else:
        csv_path.write_text("", encoding="utf-8")
    print(f"Wrote {json_path} and {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
