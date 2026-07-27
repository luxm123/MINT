from __future__ import annotations

import argparse
import copy
import csv
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.controller import MintController
from mint.utils import ensure_dir, load_yaml, monotonic_sec, read_jsonl
from mint.workloads import get_workload
from scripts.run_paired_pilot import balanced_orders


RESET_ENV_KEY = "MINT_COLD_RESET_TOKEN"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run paired strategies from externally reset Lambda function pools.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--dag", default="wide_branch")
    parser.add_argument("--baselines", nargs=2, default=["no_warmup", "mint_markov_full"])
    parser.add_argument("--pools", nargs=2, required=True)
    parser.add_argument("--blocks", type=int, default=20)
    parser.add_argument("--budget", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--post-reset-delay-sec", type=float, default=3.0)
    parser.add_argument("--warmup-lead-sec", type=float, default=1.0)
    parser.add_argument("--profile-mismatch", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-real-run", action="store_true")
    parser.add_argument("--output-root", required=True)
    return parser.parse_args()


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def reset_function_pool(
    lambda_client: Any,
    function_map: dict[str, str],
    dag_nodes: list[str],
    token: str,
    *,
    dry_run: bool,
) -> list[dict[str, Any]]:
    rows = []
    for logical_name in dag_nodes:
        function_name = function_map[logical_name]
        started_at = datetime.now(timezone.utc).isoformat()
        if dry_run:
            completed_at = datetime.now(timezone.utc).isoformat()
            rows.append(
                {
                    "logical_name": logical_name,
                    "function_name": function_name,
                    "reset_token": token,
                    "reset_started_at": started_at,
                    "reset_completed_at": completed_at,
                    "last_update_status": "DryRun",
                }
            )
            continue

        current = lambda_client.get_function_configuration(FunctionName=function_name)
        variables = dict((current.get("Environment") or {}).get("Variables") or {})
        variables[RESET_ENV_KEY] = token
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            Environment={"Variables": variables},
        )
        lambda_client.get_waiter("function_updated_v2").wait(FunctionName=function_name)
        updated = lambda_client.get_function_configuration(FunctionName=function_name)
        status = str(updated.get("LastUpdateStatus") or "")
        if status != "Successful":
            raise RuntimeError(f"Lambda reset failed for {function_name}: LastUpdateStatus={status!r}")
        rows.append(
            {
                "logical_name": logical_name,
                "function_name": function_name,
                "reset_token": token,
                "reset_started_at": started_at,
                "reset_completed_at": datetime.now(timezone.utc).isoformat(),
                "last_update_status": status,
            }
        )
    return rows


def validate_controlled_cold_run(
    events: list[dict[str, Any]],
    previous_environment_ids: dict[str, str],
) -> tuple[bool, str, dict[str, str]]:
    calls = [event for event in events if event.get("event_type") in {"warmup", "invocation"}]
    if not calls:
        return False, "no_invocations", {}
    by_logical: dict[str, list[dict[str, Any]]] = {}
    for event in calls:
        if event.get("status") != "ok":
            return False, f"non_ok_invocation:{event.get('logical_name')}", {}
        logical_name = str(event.get("logical_name") or "")
        by_logical.setdefault(logical_name, []).append(event)

    observed: dict[str, str] = {}
    for logical_name, logical_calls in by_logical.items():
        first = logical_calls[0]
        environment_id = str(first.get("execution_environment_id") or "")
        if not environment_id:
            return False, f"missing_environment_id:{logical_name}", {}
        if not bool(first.get("cold_start")):
            return False, f"first_call_not_cold:{logical_name}", {}
        if previous_environment_ids.get(logical_name) == environment_id:
            return False, f"environment_not_replaced:{logical_name}", {}
        observed[logical_name] = environment_id

        warmups = [event for event in logical_calls if event.get("invocation_type") == "warmup"]
        reals = [event for event in logical_calls if event.get("invocation_type") == "real"]
        if warmups and reals:
            first_warmup = warmups[0]
            first_real = reals[0]
            if bool(first_real.get("cold_start")):
                return False, f"real_call_still_cold_after_warmup:{logical_name}", {}
            if first_real.get("execution_environment_id") != first_warmup.get("execution_environment_id"):
                return False, f"warmup_real_environment_mismatch:{logical_name}", {}
    return True, "valid", observed


def main() -> int:
    args = parse_args()
    dry_run = bool(args.dry_run or not args.confirm_real_run)
    if not dry_run and not args.confirm_real_run:
        raise SystemExit("Refusing real AWS run without --confirm-real-run")
    if args.blocks <= 0 or args.post_reset_delay_sec < 0 or args.warmup_lead_sec < 0:
        raise SystemExit("blocks must be positive and timing values must be non-negative")

    output_root = ensure_dir(args.output_root)
    base_config = load_yaml(args.config)
    dag = get_workload(args.dag)
    lambda_client = None
    if not dry_run:
        import boto3

        lambda_client = boto3.client("lambda", region_name=base_config.get("aws", {}).get("region"))

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
            dag=dag,
            baseline=baseline,
            dry_run=dry_run,
            output_dir=output_root / baseline,
        )

    reset_rows: list[dict[str, Any]] = []
    validation_rows: list[dict[str, Any]] = []
    orders = balanced_orders(args.baselines, args.blocks, args.seed)
    for block_index, order in enumerate(orders):
        block_id = f"block-{block_index:04d}"
        order_text = ",".join(order)
        for slot_index, baseline in enumerate(order):
            controller = controllers[baseline]
            previous_ids = controller.observed_environment_ids()
            token = f"{block_id}-{baseline}-{uuid.uuid4().hex}"
            controller.reset_runtime_state()
            block_reset_rows = reset_function_pool(
                lambda_client,
                controller.function_map,
                dag.nodes,
                token,
                dry_run=dry_run,
            )
            for row in block_reset_rows:
                row.update(
                    {
                        "block_id": block_id,
                        "block_index": block_index,
                        "slot_index": slot_index,
                        "baseline": baseline,
                        "function_pool": controller.function_pool,
                        "strategy_order": order_text,
                    }
                )
            reset_rows.extend(block_reset_rows)

            planned_sec = monotonic_sec() + args.post_reset_delay_sec
            planned_iso = (datetime.now(timezone.utc) + timedelta(seconds=args.post_reset_delay_sec)).isoformat()
            result = controller.run_once(
                block_index,
                block_id=block_id,
                strategy_order=order_text,
                planned_arrival_sec=planned_sec,
                planned_arrival_time=planned_iso,
            )
            run_events = [
                event
                for event in read_jsonl(controller.events_path)
                if event.get("run_id") == result["run_id"]
            ]
            if dry_run:
                valid, reason, observed_ids = True, "dry_run_validation_not_applicable", {}
            else:
                valid, reason, observed_ids = validate_controlled_cold_run(run_events, previous_ids)
            validation_rows.append(
                {
                    "block_id": block_id,
                    "block_index": block_index,
                    "slot_index": slot_index,
                    "baseline": baseline,
                    "function_pool": controller.function_pool,
                    "run_id": result["run_id"],
                    "reset_token": token,
                    "valid": valid,
                    "reason": reason,
                    "previous_environment_ids": json.dumps(previous_ids, sort_keys=True),
                    "observed_environment_ids": json.dumps(observed_ids, sort_keys=True),
                }
            )
            _write_csv(output_root / "reset_log.csv", reset_rows)
            _write_csv(output_root / "cold_validation.csv", validation_rows)
            if not valid:
                raise RuntimeError(
                    f"Controlled cold validation failed: block={block_id} baseline={baseline} reason={reason}"
                )
            summaries[baseline].append(result)

    for baseline, controller in controllers.items():
        controller.finalize(summaries[baseline])

    manifest = {
        "mode": "controlled_cold_paired_pilot",
        "config": args.config,
        "dag": args.dag,
        "baselines": args.baselines,
        "pools": args.pools,
        "blocks": args.blocks,
        "budget": args.budget,
        "seed": args.seed,
        "post_reset_delay_sec": args.post_reset_delay_sec,
        "warmup_lead_sec": args.warmup_lead_sec,
        "profile_mismatch": args.profile_mismatch,
        "dry_run": dry_run,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    (output_root / "experiment_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"Wrote controlled cold pilot to {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
