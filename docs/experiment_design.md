# Experiment Design

MINT studies serverless DAG warmup as a two-phase decision problem.

Offline planning produces warmup intents for each DAG node. Each intent includes a planned time, validity window, criticality score, priority, and estimated offline gain.

Online scheduling observes runtime state and converts intents into actions:

- `execute`: invoke the warmup now.
- `delay`: keep the intent but move it later.
- `cancel`: discard the intent because it is not useful.
- `replace`: drop a lower-gain candidate when the budget is full.

The original implementation is heuristic. It estimates stage timing from DAG depth, call probability from branch structure, cold-start risk from simple platform defaults, and criticality from downstream count. This remains useful as a lightweight prototype and baseline.

The Markov planner implements the paper-oriented offline policy analyzer. It defines a finite-horizon controlled Markov model over DAG frontier, HOT/COLD state with retention buckets, completed functions, branch path, and time bucket. Actions are sets of functions to warm, constrained by warmup budget `B`. Transitions model real function calls, cold-start penalties, warmup effects, retention expiration, branch probability, fanout, and join readiness. Rewards are negative costs that include expected cold-start penalty, path penalty, warmup cost, wasted warmup penalty, and missed warmup penalty.

The runtime scheduler can consume intents from either planner. `mint_full` is heuristic intents plus runtime scheduling. `mint_markov_full` is Markov intents plus runtime scheduling and should be treated as the formal MINT result. Report `mint_full` as a heuristic prototype ablation.

Measured metrics include end-to-end latency, latency percentiles, cold-start rate, total warmups, useful warmups, wasted warmups, missed warmups, useful warmup ratio, and scheduler action counts.

## Formal Matrix Experiments

Formal experiments should vary:

- DAG: `chain`, `fanout`, `branch`, `join`.
- Baseline: `no_warmup`, `static_dag`, `mint_offline`, `mint_full`.
- Markov variants: `mint_markov_offline`, `mint_markov_full`.
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

The tables summarize latency by DAG and baseline, warmup efficiency, scheduler action counts, budget sensitivity, overall results, MINT improvement ratios, and run-level variability. Improvement ratios prefer `mint_markov_full` over `mint_full`, and prefer `mint_markov_offline` over `mint_offline` as the offline reference. Paper tables use `effective_planner` for method labeling; `planner_type` is retained only as configuration provenance.

## Delay-Shift Supplemental Experiment

The main matrix primarily validates Cancel, Replace, and warmup efficiency. It can show `delay_count=0`, especially when intents are already valid at scheduling time. In that case it does not prove Delay's contribution. Use the supplemental delay-shift stress test to construct an upstream-lag scenario where downstream intents are too early, the scheduler emits `delay`, and the delayed intent is rescheduled and executed before the downstream real call:

```bash
python scripts/run_delay_shift_experiment.py \
  --config configs/mint_aws.yaml \
  --baselines static_dag mint_markov_full \
  --repetitions 3 \
  --upstream-delay-ms 1200 \
  --dry-run \
  --output-dir results/dryrun_delay_shift_test
```

The output includes `delay_analysis.csv` with `delay_count`, `delayed_execute_count`, served-after-delay count, saved cold-start count, missed warmups, cold starts, controller wall-clock latency, and average latency. Real AWS mode requires `--confirm-real-run`. This is a constructed mechanism test, not evidence that production traffic naturally follows the same distribution.

Latency is reported with separate fields. `logical_end_to_end_latency_ms` is the controlled stress-test model value. `measured_wall_clock_latency_ms` is measured by the controller around the workflow execution. `lambda_invocation_latency_ms_sum` sums controller-side Lambda invoke elapsed time. `reported_end_to_end_latency_ms` uses the selected comparison metric, currently `measured_wall_clock_latency_ms` for every baseline. A paper table that discusses real AWS observed latency should use measured wall-clock latency; logical latency should be described only as mechanism-level stress-test latency.

## Planner Configuration

Use the heuristic planner:

```yaml
planner:
  type: heuristic
```

Use the Markov policy analyzer:

```yaml
planner:
  type: markov
  horizon: 5
  warmup_cost: 0.1
  cold_start_penalty_weight: 1.0
  wasted_warmup_penalty_weight: 0.2
  missed_warmup_penalty_weight: 0.5
  retention_bucket_sec: 60
```

The current analyzer is intended for `chain`, `fanout`, `branch`, and `join`. It enumerates states and actions directly, so it is not yet suitable for large DAGs without state abstraction or pruning.

## Metric Notes

`missed_warmup` counts cold starts after an executed warmup event for the same function and workflow run. `unserved_intent_cold_start` counts cold starts where a warmup intent or scheduler candidate existed but was not executed because of budget pressure, `replace`, `cancel`, or delay beyond the call. `uncovered_cold_start` counts cold starts without any warmup event or intent coverage. `wasted_warmup` currently means a warmup event that was not on the realized workflow path; already-HOT no-benefit warmups are not separated yet.

AWS real runs can create cloud charges. All smoke tests should run with `--dry-run`; matrix real runs require `--confirm-real-run`.
