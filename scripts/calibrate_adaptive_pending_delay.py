from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.aws_client import invoke_lambda
from mint.provenance import write_experiment_provenance
from mint.utils import ensure_dir, load_yaml


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure real f1 branch-reveal latency before a dynamic AWS smoke test."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--pool", required=True)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--safety-margin-ms", type=float, default=100.0)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-real-run", action="store_true")
    return parser.parse_args(argv)


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    position = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[position]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    dry_run = bool(args.dry_run or not args.confirm_real_run)
    if args.samples < 3:
        raise SystemExit("--samples must be at least 3")
    if args.safety_margin_ms < 0:
        raise SystemExit("--safety-margin-ms must be non-negative")

    config = load_yaml(args.config)
    pools = config.get("aws", {}).get("lambda_function_pools", {})
    if args.pool not in pools:
        raise SystemExit(f"Unknown Lambda function pool: {args.pool}")
    function_name = str(pools[args.pool].get("f1") or "")
    if not function_name:
        raise SystemExit(f"Function pool {args.pool!r} has no f1")

    output_root = ensure_dir(args.output_root)
    rows: list[dict[str, Any]] = []
    for index in range(args.samples):
        response = invoke_lambda(
            function_name,
            {
                "function_name": "f1",
                "run_id": f"pending-calibration-{index:04d}",
                "invocation_type": "calibration",
                "branch": "f2",
                "sleep_ms": 10,
            },
            dry_run=dry_run,
            region_name=config.get("aws", {}).get("region"),
        )
        payload = response.get("payload") or {}
        if not dry_run:
            required = ("request_id", "execution_environment_id", "cold_start", "duration_ms")
            missing = [key for key in required if key not in payload]
            if missing:
                raise RuntimeError(f"real f1 calibration response missing: {missing}")
        rows.append(
            {
                "sample_index": index,
                "client_elapsed_ms": float(response.get("client_elapsed_ms", 0.0)),
                "function_duration_ms": float(payload.get("duration_ms", 0.0)),
                "request_id": str(payload.get("request_id", "")),
                "execution_environment_id": str(payload.get("execution_environment_id", "")),
                "cold_start": bool(payload.get("cold_start", False)),
                "status": str(payload.get("status", "dry_run" if dry_run else "")),
            }
        )

    elapsed = [row["client_elapsed_ms"] for row in rows]
    p95_ms = _percentile(elapsed, 0.95)
    recommended_ms = math.ceil(p95_ms + args.safety_margin_ms)
    report = {
        "mode": "adaptive_pending_delay_calibration",
        "dry_run": dry_run,
        "paper_performance_eligible": False,
        "config": args.config,
        "pool": args.pool,
        "function_name": function_name,
        "samples": args.samples,
        "safety_margin_ms": args.safety_margin_ms,
        "client_elapsed_ms": {
            "min": min(elapsed),
            "mean": statistics.mean(elapsed),
            "p50": statistics.median(elapsed),
            "p95": p95_ms,
            "max": max(elapsed),
        },
        "recommended_adaptive_pending_delay_ms": recommended_ms,
        "recommended_adaptive_pending_delay_sec": recommended_ms / 1000.0,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
    }
    report["provenance"] = write_experiment_provenance(
        output_root,
        {
            "source_config_path": str(Path(args.config).resolve()),
            "config": config,
            "arguments": {
                "pool": args.pool,
                "samples": args.samples,
                "safety_margin_ms": args.safety_margin_ms,
                "dry_run": dry_run,
            },
        },
        repository=ROOT,
    )
    path = output_root / "pending_delay_calibration.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key not in {"rows", "provenance"}}, indent=2, sort_keys=True))
    print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
