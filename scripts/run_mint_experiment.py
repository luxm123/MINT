from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mint.controller import MintController
from mint.utils import load_yaml
from mint.workloads import get_workload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a MINT DAG warmup experiment.")
    parser.add_argument("--config", default="configs/mint_aws.yaml")
    parser.add_argument("--dag")
    parser.add_argument("--baseline")
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--dry-run", action="store_true", help="Never call AWS; simulate invocations locally.")
    parser.add_argument("--confirm-real-run", action="store_true", help="Required for real AWS Lambda invocation.")
    parser.add_argument("--output-dir")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_yaml(args.config)
    exp = config.setdefault("experiment", {})
    if args.dag:
        exp["dag"] = args.dag
    if args.baseline:
        exp["baseline"] = args.baseline
    if args.repetitions is not None:
        exp["repetitions"] = args.repetitions
    if args.output_dir:
        exp["output_dir"] = args.output_dir

    dry_run = bool(args.dry_run or exp.get("dry_run", True))
    if not dry_run and not args.confirm_real_run:
        print("Refusing real AWS run: pass --confirm-real-run when --dry-run is not set.", file=sys.stderr)
        return 2

    dag = get_workload(exp.get("dag", "chain"))
    controller = MintController(
        config=config,
        dag=dag,
        baseline=exp.get("baseline", "mint_full"),
        dry_run=dry_run,
        output_dir=exp.get("output_dir", "results/default"),
    )
    summary = controller.run(int(exp.get("repetitions", 1)))
    print(f"dry_run={dry_run}")
    print(f"output_dir={controller.output_dir}")
    print(f"summary={summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
