from __future__ import annotations

import json
import time
from typing import Any


class LambdaInvocationError(RuntimeError):
    pass


def invoke_lambda(
    function_name: str,
    payload: dict[str, Any],
    invocation_type: str = "RequestResponse",
    dry_run: bool = True,
    region_name: str | None = None,
    retries: int = 2,
    timeout_sec: float = 10.0,
) -> dict[str, Any]:
    if dry_run:
        sleep_ms = float(payload.get("sleep_ms", 0))
        if sleep_ms > 0:
            time.sleep(min(sleep_ms / 1000.0, timeout_sec))
        return {
            "dry_run": True,
            "function_name": function_name,
            "payload": payload,
            "invocation_type": invocation_type,
            "status_code": 202 if invocation_type == "Event" else 200,
        }

    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as exc:
        raise LambdaInvocationError("boto3 is required for real AWS Lambda invocation") from exc

    client = boto3.client(
        "lambda",
        region_name=region_name,
        config=Config(connect_timeout=timeout_sec, read_timeout=timeout_sec, retries={"max_attempts": retries}),
    )
    body = json.dumps(payload).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            invoke_start = time.perf_counter()
            response = client.invoke(FunctionName=function_name, InvocationType=invocation_type, Payload=body)
            parsed: dict[str, Any] = {
                "dry_run": False,
                "function_name": function_name,
                "status_code": response.get("StatusCode"),
            }
            if "Payload" in response:
                raw = response["Payload"].read()
                if raw:
                    parsed["payload"] = json.loads(raw.decode("utf-8"))
            parsed["client_elapsed_ms"] = round((time.perf_counter() - invoke_start) * 1000.0, 3)
            return parsed
        except (BotoCoreError, ClientError, TimeoutError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(0.2 * (attempt + 1))
    raise LambdaInvocationError(f"Lambda invocation failed for {function_name}: {last_error}")
