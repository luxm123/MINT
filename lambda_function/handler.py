from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

IS_COLD_START = True
EXECUTION_ENV_ID = str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    global IS_COLD_START
    start = time.time()
    start_time = _now()
    cold_start = IS_COLD_START
    IS_COLD_START = False

    sleep_ms = float(event.get("sleep_ms", 0))
    if sleep_ms > 0:
        time.sleep(sleep_ms / 1000.0)

    end_time = _now()
    duration_ms = round((time.time() - start) * 1000.0, 3)
    function_name = event.get("function_name") or os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")
    invocation_type = event.get("invocation_type") or event.get("type", "real")
    result = {
        "function_name": function_name,
        "run_id": event.get("run_id", ""),
        "invocation_type": invocation_type,
        "cold_start": cold_start,
        "execution_environment_id": EXECUTION_ENV_ID,
        "request_id": getattr(context, "aws_request_id", ""),
        "start_time": start_time,
        "end_time": end_time,
        "duration_ms": duration_ms,
        "status": "ok",
        "error_type": "",
        "error_message": "",
    }
    return json.loads(json.dumps(result))


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return handler(event, context)
