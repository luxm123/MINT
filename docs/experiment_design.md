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
- Core baselines: `no_warmup`, `orion_full`, `faascache`, `xanadu_full`, `mint_markov_full`.
- Appendix/ablation baselines: `periodic_keepwarm`, `static_dag`, `orion_like`, `path_aware_greedy`, `xanadu_like`, `oracle_path`, `provisioned_concurrency`, `mint_offline`, `mint_full`.
- Markov variants: `mint_markov_offline`, `mint_markov_full`.
- Warmup budget: usually `1`, `2`, and `3`.
- Repetitions: `10` per (dag, baseline, budget, seed); paper runs use multiple
  branch seeds (`--seeds 42 43 44 45 46`) and report bootstrap confidence
  intervals over seeds.

The first main experiment should be described as a representative Serverless DAG workflow benchmark, not as a production workload benchmark or a different-load-intensity study. The four DAGs represent workflow structures: `chain` is a linear dependency workflow, `fanout` is a parallel dispatch workflow, `branch` is a runtime path-selection workflow, and `join` is a convergence workflow. Each DAG node is a controlled AWS Lambda microbenchmark function whose body performs controlled lightweight computation or fixed-duration sleep to simulate the function execution phase. The goal is to evaluate DAG-level warmup scheduling behavior across topology patterns, not to model a particular business application or claim coverage of real production traces.

Because fixed prewarming baselines can choose very similar warmup targets on the small standard DAGs, the paper core experiment should use the adaptive stress benchmark. It contains `wide_branch` (`f1 -> branch(f2/f3/f4/f5) -> f6 -> f7`) and `deep_mixed` (`f1 -> f2 or f3`, `f2 -> f4 -> f6`, `f3 -> f5 -> f6`, `f6 -> f7 -> f8`). Run it with profile mismatch enabled, fixed `branch_seed`, `timing_jitter_ms=800`, budgets `1 2 3`, and the five core rows `no_warmup`, `orion_full`, `faascache`, `xanadu_full`, `mint_markov_full`. This benchmark is intended to evaluate path uncertainty, profile mismatch, timing jitter, and budget-limited adaptive scheduling against the strongest academic remedies: full ORION (OSDI'22) container caching/bundling/right-sizing, FaasCache (ASPLOS'21) GDSF keep-alive, and Xanadu (Middleware'20) most-likely-path just-in-time provisioning.

The final reported methods are `MINT` (`mint_markov_full`), `ORION` (`orion_full`), `FaasCache` (`faascache`), `Xanadu` (`xanadu_full`), and the `No warmup` reference row. All five rows run under the same budget B (1/2/3), the same profile mismatch / timing jitter, the same seeds, and the same Azure-trace-calibrated parameters. `orion_full` reproduces ORION's container cache, right-sizing, and bundling at the scheduling layer (real AWS fidelity additionally requires per-memory deployments and composed functions). `faascache` reproduces the GDSF keep-alive cache at the scheduling layer with B proactive warmup slots. `xanadu_full` reproduces MLP detection plus just-in-time speculative provisioning at the scheduling layer. `provisioned_concurrency`, `Best-fixed`, `Path-aware greedy`, and `Oracle` remain appendix material only (Best-fixed is computed by the pre-registered rule "minimum P95 per (dag, budget) among periodic_keepwarm / static_dag / orion_like"; Oracle is an ideal upper bound, not a deployable method or fair baseline).

Recommended adaptive stress dry-run:

```bash
python scripts/run_experiment_matrix.py \
  --config configs/mint_aws.yaml \
  --dags wide_branch deep_mixed \
  --baselines no_warmup orion_full faascache xanadu_full mint_markov_full \
  --budgets 1 2 3 \
  --repetitions 10 \
  --seeds 42 43 44 45 46 \
  --cooldown-sec 0 \
  --profile-mismatch \
  --timing-jitter-ms 800 \
  --branch-seed 42 \
  --randomize-order \
  --dry-run \
  --output-root results/dryrun_adaptive_stress_core
```

Recommended EC2 real run:

```bash
nohup python scripts/run_experiment_matrix.py \
  --config configs/mint_aws_real.yaml \
  --dags wide_branch deep_mixed \
  --baselines no_warmup orion_full faascache xanadu_full mint_markov_full \
  --budgets 1 2 3 \
  --repetitions 10 \
  --seeds 42 43 44 45 46 \
  --cooldown-sec 120 \
  --profile-mismatch \
  --timing-jitter-ms 800 \
  --branch-seed 42 \
  --randomize-order \
  --confirm-real-run \
  --output-root results/aws_adaptive_stress_main \
  > adaptive_stress_main.log 2>&1 &
```

Dynamic MINT budget note: the runtime intent-maintenance implementation keeps
one immediate warmup slot plus a general multi-pending-intent queue, so
`mint_markov_full` supports `warmup_budget` 1/2/3 on branch DAGs. The
intent-maintenance ablations (`mint_markov_no_cancel`, `mint_markov_cancel_only`)
remain defined only for B=2, and the `adaptive_branch` workload keeps its
explicit B=2/initial=1 contract. The paper core matrix
(wide_branch/deep_mixed x 5 rows x B=1/2/3) is expected to complete with
zero skips; run `scripts/audit_adaptive_stress_results.py` (or pass `--audit`
to the matrix runner) as the quality gate.

Dry-run latency caveat: dry-run records simulated cold/warm latencies as event
fields but does not sleep cold-start wall-clock time, so dry-run end-to-end
latency is dominated by warmup overhead and should be used only as a pipeline
check. Paper latency claims must use real AWS runs and measured wall-clock
latency.

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

`periodic_keepwarm` represents a common industrial keep-warm strategy that ignores DAG structure, stage order, and branch/path information; it rotates through the function set under the same `warmup_budget`. `static_dag` is the fixed DAG-aware offline plan and does not perform runtime cancel, replace, or delay. `orion_like` represents a limited ORION-style DAG-aware right-prewarming approximation using stage order, expected start time, and fixed look-ahead prewarming. It intentionally excludes ORION right-sizing, bundling, and the full ORION optimizer, so results should be labeled ORION-like rather than a complete ORION reproduction.

`faascache` (FaasCache, ASPLOS'21) is the Greedy-Dual-Size-Frequency keep-alive cache at the scheduling layer: B bounds the number of proactive warmup slots; values are `cost * freq / size + L`, where cost is the cold-start latency saved, freq is the (optionally decayed) invocation frequency, size is the container memory footprint, and L is the GDS aging factor equal to the last evicted value. Real invocations update frequency and populate the cache like accesses in the original system. `xanadu_full` (Xanadu, Middleware'20) derives the most-likely path from trace-calibrated and learned branch probabilities and spends the B slots on the earliest-needed MLP members first (JIT), never warming the entry function or off-MLP functions.

When baseline results are close, use `scripts/analyze_baseline_overlap.py` to check target-set, frequency-vector, timing-bucket, and action-sequence overlap. High target-set overlap alone can be benign on tiny DAGs; high frequency and timing overlap as well means the workload may not distinguish the baselines. Use `scripts/prepare_pareto_data.py` to build cost-latency plot data, since MINT's benefit may appear as a Pareto tradeoff rather than a simple average-latency win. Pareto dominance uses `total_warmup` as cost and average latency or P95 latency as performance: a point is dominated only when another point has no more warmups and no worse latency, with at least one dimension strictly better.

## Seed Statistics, Cost-Latency, and Trace Calibration

The matrix runner stores one row per (dag, baseline, budget, seed).  Paper
comparisons must not rely on a single seed: run
`--seeds 42 43 44 45 46`, then
`scripts/analyze_seed_statistics.py` produces per-config bootstrap means and
95% CIs over seeds plus a pairwise MINT-vs-each-baseline P95 dominance
probability.  `scripts/analyze_cost_latency.py` uses an explicit pricing model:
warmup invocations cost `--invocation-price-usd` each; Provisioned Concurrency
is billed on a separate axis (`provisioned_slots_total` x
`provisioned_duration_sec_total` at `--provisioned-price-usd-per-slot-sec`).

`Best-fixed` is the pre-registered deterministic rule "minimum P95 per
(dag, budget) among periodic_keepwarm / static_dag / orion_like"; the three
fixed baselines are also reported separately so no post-hoc cherry-picking is
hidden.

The synthetic workloads can be calibrated from the public Microsoft Azure
Functions dataset.  One command downloads a slice, the second writes a
calibrated config without touching the original:

```bash
python scripts/download_azure_trace.py --output-dir data/azure_trace --days 1 2
python scripts/apply_trace_calibration.py \
  --trace data/azure_trace/function_benchmark_data_1.csv \
  --config configs/mint_aws_real.yaml \
  --output configs/mint_aws_real_tracecal.yaml
```

Calibrated parameters: stage spacing from inter-arrival time, warm duration
from the duration median, and the cold-start model from observed memory.
Branch probabilities use a deterministic documented convention: because
production traces identify functions by opaque names, the top-k most-called
trace functions are mapped to the DAG's branch successors by frequency rank,
preserving the trace's call-frequency skew (`branch_mapping` in
`experiment.trace_calibration` records whether rank fallback was used).  The
trace source is recorded in `experiment.trace_calibration.source` for
reproducibility.  Run the core matrix with the calibrated config via
`bash scripts/run_core_matrix.sh --real configs/mint_aws_real_tracecal.yaml`.

The optional `mixed` workload adds branch choice plus downstream convergence (`f1 -> f2 or f3 -> f4 -> f5`). It is intended to stress branch, join, and downstream timing interaction while preserving the original four workloads for comparability.

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
