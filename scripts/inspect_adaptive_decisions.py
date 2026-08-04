from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print causal model snapshots and runtime intent revisions.")
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def _read(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main() -> int:
    args = parse_args()
    by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order: list[str] = []
    for event in _read(args.events):
        run_id = str(event.get("run_id", ""))
        if run_id and run_id not in by_run:
            order.append(run_id)
        if run_id:
            by_run[run_id].append(event)

    for run_id in order[: max(0, args.limit)]:
        events = by_run[run_id]
        initial_model = next(
            (event for event in events if event.get("event_type") == "branch_model" and event.get("decision_phase") == "initial"),
            None,
        )
        runtime_model = next(
            (event for event in events if event.get("event_type") == "branch_model" and event.get("decision_phase") == "runtime_after_f1"),
            None,
        )
        if not initial_model or not runtime_model:
            continue
        initial = [
            event.get("logical_name") for event in events
            if event.get("event_type") == "scheduler_decision"
            and event.get("decision_phase") == "initial"
            and event.get("action") == "execute"
        ]
        runtime = [
            f"{event.get('action')}:{event.get('logical_name')}"
            for event in events
            if event.get("event_type") == "scheduler_decision"
            and event.get("decision_phase") == "runtime_after_f1"
        ]
        print(
            f"{run_id} history={initial_model['history_size']} "
            f"probabilities={initial_model['branch_probabilities']} "
            f"initial={initial} observed={runtime_model['observed_branch']} runtime={runtime}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
