"""Configure or remove AWS Lambda Provisioned Concurrency for a MINT run.

The `provisioned_concurrency` baseline assumes the functions are provisioned
BEFORE the matrix starts and removed AFTER it finishes.  This helper performs
those AWS API calls (PutProvisionedConcurrencyConfig /
DeleteProvisionedConcurrencyConfig) and is intentionally NOT executed by the
experiment runner itself.

Usage:
    python scripts/manage_provisioned_concurrency.py --config configs/mint_aws_real.yaml --action configure --functions f1 f6 f7
    python scripts/manage_provisioned_concurrency.py --config configs/mint_aws_real.yaml --action remove --functions f1 f6 f7
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.utils import load_yaml


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage Lambda Provisioned Concurrency.")
    parser.add_argument("--config", default="configs/mint_aws_real.yaml")
    parser.add_argument("--action", choices=["configure", "remove"], required=True)
    parser.add_argument("--functions", nargs="+", required=True)
    parser.add_argument("--provisioned-concurrency", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_yaml(args.config)
    region = config.get("aws", {}).get("region", "us-east-1")
    function_map = config.get("aws", {}).get("lambda_functions", {})
    missing = [name for name in args.functions if name not in function_map]
    if missing:
        print(f"Unknown logical functions in config: {missing}", file=sys.stderr)
        return 2

    import boto3

    client = boto3.client("lambda", region_name=region)
    for logical in args.functions:
        function_name = function_map[logical]
        if args.action == "configure":
            response = client.put_provisioned_concurrency_config(
                FunctionName=function_name,
                Qualifier="$LATEST",
                ProvisionedConcurrentExecutions=args.provisioned_concurrency,
            )
            print(
                f"Configured {logical} -> {function_name}: "
                f"status={response.get('Status')} "
                f"allocated={response.get('AllocatedProvisionedConcurrentExecutions')}"
            )
        else:
            client.delete_provisioned_concurrency_config(
                FunctionName=function_name,
                Qualifier="$LATEST",
            )
            print(f"Removed Provisioned Concurrency from {logical} -> {function_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
