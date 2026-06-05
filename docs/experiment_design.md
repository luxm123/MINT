# Experiment Design

MINT studies serverless DAG warmup as a two-phase decision problem.

Offline planning produces warmup intents for each DAG node. Each intent includes a planned time, validity window, criticality score, priority, and estimated offline gain.

Online scheduling observes runtime state and converts intents into actions:

- `execute`: invoke the warmup now.
- `delay`: keep the intent but move it later.
- `cancel`: discard the intent because it is not useful.
- `replace`: drop a lower-gain candidate when the budget is full.

The first implementation is heuristic. It estimates stage timing from DAG depth, call probability from branch structure, cold-start risk from simple platform defaults, and criticality from downstream count. This is the intended extension point for a future Markov Policy Planner that optimizes expected value over runtime states.

Measured metrics include end-to-end latency, latency percentiles, cold-start rate, total warmups, useful warmups, wasted warmups, missed warmups, useful warmup ratio, and scheduler action counts.

## Formal Matrix Experiments

Formal experiments should vary:

- DAG: `chain`, `fanout`, `branch`, `join`.
- Baseline: `no_warmup`, `static_dag`, `mint_offline`, `mint_full`.
- Warmup budget: usually `1`, `2`, and `3`.
- Repetitions: start with `10` for AWS pilot data, then increase if variance is high.

The recommended AWS command is:

```bash
python scripts/run_experiment_matrix.py \
  --config configs/mint_aws_real.yaml \
  --dags chain fanout branch join \
  --baselines no_warmup static_dag mint_offline mint_full \
  --budgets 1 2 3 \
  --repetitions 10 \
  --cooldown-sec 120 \
  --randomize-order \
  --confirm-real-run \
  --output-root results/aws_matrix_main
```

Use this dry-run matrix first:

```bash
python scripts/run_experiment_matrix.py \
  --config configs/mint_aws.yaml \
  --dags chain \
  --baselines no_warmup static_dag mint_offline mint_full \
  --budgets 2 \
  --repetitions 2 \
  --cooldown-sec 1 \
  --randomize-order \
  --dry-run \
  --output-root results/dryrun_matrix_test
```

Each configuration writes an independent output directory, and the matrix root writes `summary_matrix.csv`, `summary_matrix.json`, `failed_runs.jsonl`, and `experiment_manifest.json`.

Randomized order reduces systematic bias from fixed baseline ordering. Cooldown reduces contamination from Lambda execution environments retained by a previous configuration.

## Paper Tables

Generate clean CSV tables for later plotting:

```bash
python scripts/prepare_paper_tables.py \
  --matrix-csv results/aws_matrix_main/summary_matrix.csv \
  --output-dir results/aws_matrix_main/paper_tables
```

The tables summarize latency by DAG and baseline, warmup efficiency, scheduler action counts, and budget sensitivity. They also include MINT warmup reduction relative to `static_dag` and `mint_offline`, plus MINT latency reduction relative to `no_warmup`.

## Metric Notes

`uncovered_cold_start` counts cold starts without any warmup event or intent coverage. `missed_warmup` counts cold starts where the workflow had a warmup event or scheduler intent for that function but the real invocation was still cold. `wasted_warmup` currently means a warmup event that was not on the realized workflow path; already-HOT no-benefit warmups are not separated yet.

AWS real runs can create cloud charges. All smoke tests should run with `--dry-run`; matrix real runs require `--confirm-real-run`.
