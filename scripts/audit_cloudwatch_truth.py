from __future__ import annotations

import argparse
import csv
import json
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


INIT_RE = re.compile(r"Init Duration:\s*([0-9.]+)\s*ms")


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cross-check MINT invocation events against AWS Lambda CloudWatch logs.")
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--log-wait-sec", type=float, default=30.0)
    return parser.parse_args()


def _read_invocations(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if event.get("event_type") in {"invocation", "warmup"}:
            rows.append(event)
    return rows


def _epoch_ms(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp() * 1000)

def _log_group_name(function_name: str) -> str:
    if function_name.startswith("arn:"):
        resource = function_name.split(":function:", 1)[-1]
        base_name = resource.split(":", 1)[0]
    else:
        base_name = function_name.split(":", 1)[0]
    return f"/aws/lambda/{base_name}"


def main() -> int:
    args = _args()
    import boto3

    rows = _read_invocations(args.events)
    if not rows:
        raise SystemExit("No invocation or warmup events found")
    if args.log_wait_sec > 0:
        time.sleep(args.log_wait_sec)

    logs = boto3.client("logs", region_name=args.region)
    by_function: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_function[str(row["function_name"])].append(row)

    messages_by_request: dict[str, list[str]] = defaultdict(list)
    for function_name, function_rows in by_function.items():
        timestamps = [_epoch_ms(str(row["timestamp"])) for row in function_rows]
        kwargs: dict[str, Any] = {
            "logGroupName": _log_group_name(function_name),
            "startTime": min(timestamps) - 60_000,
            "endTime": max(timestamps) + 300_000,
        }
        while True:
            response = logs.filter_log_events(**kwargs)
            for item in response.get("events", []):
                message = str(item.get("message", ""))
                for row in function_rows:
                    request_id = str(row.get("request_id", ""))
                    if request_id and request_id in message:
                        messages_by_request[request_id].append(message)
            token = response.get("nextToken")
            if not token or token == kwargs.get("nextToken"):
                break
            kwargs["nextToken"] = token

    audit_rows = []
    failures = 0
    for row in rows:
        request_id = str(row.get("request_id", ""))
        messages = messages_by_request.get(request_id, [])
        combined = "\n".join(messages)
        init_match = INIT_RE.search(combined)
        cloudwatch_init_ms = float(init_match.group(1)) if init_match else ""
        start_found = f"START RequestId: {request_id}" in combined
        report_found = f"REPORT RequestId: {request_id}" in combined
        local_cold = bool(row.get("cold_start"))
        init_consistent = bool(init_match) == local_cold
        ok = bool(request_id) and start_found and report_found and init_consistent
        failures += int(not ok)
        audit_rows.append(
            {
                "run_id": row.get("run_id", ""),
                "function_name": row.get("function_name", ""),
                "logical_name": row.get("logical_name", ""),
                "invocation_type": row.get("invocation_type", ""),
                "request_id": request_id,
                "execution_environment_id": row.get("execution_environment_id", ""),
                "local_cold_start": local_cold,
                "client_elapsed_ms": row.get("latency_ms", ""),
                "function_duration_ms": row.get("function_duration_ms", ""),
                "cloudwatch_init_duration_ms": cloudwatch_init_ms,
                "cloudwatch_start_found": start_found,
                "cloudwatch_report_found": report_found,
                "cold_start_consistent": init_consistent,
                "audit_ok": ok,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(audit_rows[0]))
        writer.writeheader()
        writer.writerows(audit_rows)
    print(f"Wrote {args.output}: rows={len(audit_rows)} failures={failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
