from __future__ import annotations

import argparse
import copy
import csv
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.controller import MintController
from mint.utils import ensure_dir, load_yaml, monotonic_sec
from mint.workloads import get_workload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a paired, randomized, reproducible two-strategy pilot.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--dag", default="wide_branch")
    parser.add_argument("--baselines", nargs=2, default=["no_warmup", "mint_markov_full"])
    parser.add_argument("--pools", nargs=2, required=True)
    parser.add_argument("--blocks", type=int, default=50)
    parser.add_argument("--budget", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--slot-spacing-sec", type=float, default=2.0)
    parser.add_argument("--warmup-lead-sec", type=float, default=1.0)
    parser.add_argument("--initial-delay-sec", type=float, default=2.0)
    parser.add_argument("--profile-mismatch", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-real-run", action="store_true")
    parser.add_argument("--output-root", required=True)
    return parser.parse_args()


def _iso_at(start_wall: datetime, start_mono: float, target_mono: float) -> str:
    return (start_wall + timedelta(seconds=target_mono - start_mono)).isoformat()


def _write_trace(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

def balanced_orders(baselines: list[str], blocks: int, seed: int) -> list[list[str]]:
    if len(baselines) != 2:
        raise ValueError("paired pilot requires exactly two baselines")
    rng = random.Random(seed)
    first_strategies = [baselines[index % 2] for index in range(blocks)]
    rng.shuffle(first_strategies)
    return [[first, baselines[1] if first == baselines[0] else baselines[0]] for first in first_strategies]


def main() -> int:
    args = parse_args()
    dry_run = bool(args.dry_run or not args.confirm_real_run)
    if not dry_run and not args.confirm_real_run:
        raise SystemExit("Refusing real AWS run without --confirm-real-run")
    if args.blocks <= 0 or args.slot_spacing_sec < 0 or args.warmup_lead_sec < 0:
        raise SystemExit("blocks must be positive and timing values must be non-negative")

    output_root = ensure_dir(args.output_root)
    base_config = load_yaml(args.config)
    controllers: dict[str, MintController] = {}
    summaries: dict[str, list[dict[str, Any]]] = {baseline: [] for baseline in args.baselines}

    for baseline, pool in zip(args.baselines, args.pools):
        config = copy.deepcopy(base_config)
        exp = config.setdefault("experiment", {})
        exp.update(
            {
                "dag": args.dag,
                "baseline": baseline,
                "warmup_budget": args.budget,
                "branch_seed": args.seed,
                "profile_mismatch": args.profile_mismatch,
                "warmup_lead_sec": args.warmup_lead_sec,
                "function_pool": pool,
                "dry_run": dry_run,
            }
        )
        controllers[baseline] = MintController(
            config,
            dag=get_workload(args.dag),
            baseline=baseline,
            dry_run=dry_run,
            output_dir=output_root / baseline,
        )

    start_mono = monotonic_sec()
    start_wall = datetime.now(timezone.utc)
    next_slot = start_mono + args.initial_delay_sec
    trace_rows: list[dict[str, Any]] = []
    orders = balanced_orders(args.baselines, args.blocks, args.seed)
    for block_index, order in enumerate(orders):
        order_text = ",".join(order)
        block_id = f"block-{block_index:04d}"
        for slot_index, baseline in enumerate(order):
            planned_sec = next_slot
            planned_iso = _iso_at(start_wall, start_mono, planned_sec)
            trace_rows.append(
                {
                    "block_id": block_id,
                    "block_index": block_index,
                    "slot_index": slot_index,
                    "baseline": baseline,
                    "function_pool": controllers[baseline].function_pool,
                    "strategy_order": order_text,
                    "branch_seed": args.seed,
                    "trajectory_index": block_index,
                    "planned_arrival_time": planned_iso,
                }
            )
            summary = controllers[baseline].run_once(
                block_index,
                block_id=block_id,
                strategy_order=order_text,
                planned_arrival_sec=planned_sec,
                planned_arrival_time=planned_iso,
            )
            summaries[baseline].append(summary)
            next_slot += args.slot_spacing_sec

    for baseline, controller in controllers.items():
        controller.finalize(summaries[baseline])

    _write_trace(output_root / "paired_trace.csv", trace_rows)
    manifest = {
        "mode": "continuous_paired_pilot",
        "config": args.config,
        "dag": args.dag,
        "baselines": args.baselines,
        "pools": args.pools,
        "blocks": args.blocks,
        "budget": args.budget,
        "seed": args.seed,
        "slot_spacing_sec": args.slot_spacing_sec,
        "warmup_lead_sec": args.warmup_lead_sec,
        "profile_mismatch": args.profile_mismatch,
        "dry_run": dry_run,
        "started_at": start_wall.isoformat(),
    }
    (output_root / "experiment_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote paired pilot to {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
