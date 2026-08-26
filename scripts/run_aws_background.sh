#!/usr/bin/env bash
# Run the real-AWS core matrix detached from the SSH session so the user can
# close the laptop / Xshell without aborting it.  The smoke gate runs first;
# the full real matrix only starts if the smoke run exits 0.
#
# Usage:
#   bash scripts/run_aws_background.sh [CONFIG]
#
# Prints the background PID and log path.  Reconnect later and check
#   tail -f <log>
#   results/aws_adaptive_stress_main/audit/audit_report.json
set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG="${1:-configs/mint_aws_real_tracecal.yaml}"
mkdir -p results
LOG="results/aws_run_$(date +%Y%m%d_%H%M%S).log"

nohup bash -c "bash scripts/run_core_matrix.sh --smoke $CONFIG && bash scripts/run_core_matrix.sh --real $CONFIG" > "$LOG" 2>&1 &
PID=$!

echo "Background AWS run started: pid=$PID"
echo "Log: $LOG"
echo "Watch with: tail -f $LOG"
echo "Closing Xshell now is safe; the EC2 instance keeps running."
