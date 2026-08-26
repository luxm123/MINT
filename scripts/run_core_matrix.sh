#!/usr/bin/env bash
# Run the MINT adaptive stress core matrix.
#
# Usage:
#   bash scripts/run_core_matrix.sh             # dry-run validation (2 seeds x 2 reps, no cost)
#   bash scripts/run_core_matrix.sh --real      # real AWS core matrix (5 seeds x 10 reps)
#
# The core table is fixed: no_warmup / orion_full / faascache / xanadu_full /
# mint_markov_full on wide_branch + deep_mixed, budgets 1/2/3, with profile
# mismatch and timing jitter.  The audit/quality gate runs at the end.
set -euo pipefail

cd "$(dirname "$0")/.."

MODE="${1:-dryrun}"

if [[ "$MODE" == "--real" ]]; then
  EXTRA_ARGS=(--config configs/mint_aws_real.yaml --repetitions 10 --seeds 42 43 44 45 46 --cooldown-sec 120 --confirm-real-run)
  OUTPUT_ROOT="results/aws_adaptive_stress_main"
  echo "WARNING: real AWS run with --confirm-real-run; AWS costs apply."
else
  EXTRA_ARGS=(--config configs/mint_aws.yaml --repetitions 2 --seeds 42 43 --cooldown-sec 0 --dry-run)
  OUTPUT_ROOT="results/dryrun_core"
fi

python scripts/run_experiment_matrix.py \
  --dags wide_branch deep_mixed \
  --baselines no_warmup orion_full faascache xanadu_full mint_markov_full \
  --budgets 1 2 3 \
  --profile-mismatch \
  --timing-jitter-ms 800 \
  --branch-seed 42 \
  --randomize-order \
  --audit \
  "${EXTRA_ARGS[@]}" \
  --output-root "$OUTPUT_ROOT"
