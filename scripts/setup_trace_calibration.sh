#!/usr/bin/env bash
# One-command trace setup: download the Azure 2019 aggregate day (if not
# already present), calibrate the real-AWS config, and run the zero-cost
# dry-run core matrix as a validation gate.
#
# Usage:
#   bash scripts/setup_trace_calibration.sh [DAY]
set -euo pipefail

cd "$(dirname "$0")/.."

DAY="${1:-1}"

if [ ! -f "data/azure_trace/invocations_per_function_md.anon.d${DAY}.csv" ]; then
  python scripts/download_azure_trace.py --output-dir data/azure_trace --days "$DAY"
else
  echo "Trace day ${DAY} already present; skipping download."
fi

python scripts/apply_trace_calibration.py \
  --trace-dir data/azure_trace \
  --config configs/mint_aws_real.yaml \
  --output configs/mint_aws_real_tracecal.yaml

bash scripts/run_core_matrix.sh dryrun configs/mint_aws_real_tracecal.yaml
