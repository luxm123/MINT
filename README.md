# MINT

MINT is a Python experiment framework for serverless DAG warmup research. It separates offline warmup intent generation from online runtime scheduling, so a controller can execute, delay, cancel, or replace warmup actions based on observed state.

The first version is intentionally practical: it provides a local dry-run path, an AWS Lambda invocation wrapper, baseline modes, JSONL event logs, and summary metrics for end-to-end latency, cold starts, useful warmups, wasted warmups, missed warmups, uncovered cold starts, and scheduler actions.

## Project Structure

```text
configs/              Experiment configuration
docs/                 EC2 and experiment notes
lambda_function/      Example AWS Lambda handler
mint/                 Core MINT package
scripts/              CLI entry points
tests/                Pytest coverage
```

## Local Dry-Run

Dry-run mode never calls AWS. It simulates Lambda invocation results and writes experiment artifacts locally.

```bash
python scripts/run_mint_experiment.py --config configs/mint_aws.yaml --dag chain --baseline mint_full --repetitions 2 --dry-run
```

Outputs are written under the configured `experiment.output_dir`, or under `--output-dir` when provided.

## Smoke Test

Run a quick local smoke test before any real AWS experiment:

```bash
python -m compileall mint scripts lambda_function
pytest
python scripts/run_mint_experiment.py --config configs/mint_aws.yaml --dag chain --baseline mint_full --repetitions 2 --dry-run
```

## Configure AWS Lambda Functions

Edit `configs/mint_aws.yaml`:

```yaml
aws:
  region: us-east-1
  lambda_functions:
    f1: mint-f1
    f2: mint-f2
```

The DAG nodes use logical names such as `f1`; the controller resolves them to real Lambda function names through this mapping.

## Real AWS Experiment

Real Lambda invocation is disabled unless you explicitly omit `--dry-run` and provide `--confirm-real-run`.

```bash
python scripts/run_mint_experiment.py --config configs/mint_aws.yaml --dag chain --baseline mint_full --repetitions 5 --confirm-real-run --output-dir results/chain_mint_full
```

Safety rule: do not store AWS access keys, secret keys, session tokens, or local credential files in this repository. Use the normal AWS credential chain on your machine or EC2 role.

Real AWS experiments may produce Lambda request, duration, logging, and related cloud charges. Matrix scripts default to dry-run behavior and require `--confirm-real-run` before they will issue real AWS calls.

For real AWS runs, invocation events record the controller-observed Lambda invoke elapsed time and the Lambda handler's reported `cold_start` flag. Dry-run experiments continue to use the local simulated latency and hot-state model.

## Experiment Matrix

Use `scripts/run_experiment_matrix.py` to evaluate multiple DAGs, baselines, and warmup budgets with independent output directories and a shared summary table.

The main AWS benchmark is a representative Serverless DAG workflow benchmark rather than a production workload benchmark. The four DAGs cover common workflow structures: `chain` is a linear dependency workflow, `fanout` is a parallel dispatch workflow, `branch` is a runtime path-selection workflow, and `join` is a convergence workflow. Each DAG node maps to a controlled AWS Lambda microbenchmark function. The function body performs controlled lightweight computation or fixed-duration sleep to simulate the function execution phase. These experiments evaluate DAG-level warmup scheduling behavior under different workflow topologies; they do not claim to cover real production traces or application-specific business semantics.

For the paper core comparison, use the adaptive stress benchmark because the basic `chain`/`fanout`/`branch`/`join` DAGs can make fixed prewarming baselines choose highly overlapping warmup targets. The adaptive stress benchmark uses `wide_branch` and `deep_mixed` with `--profile-mismatch`, fixed `--branch-seed`, `--timing-jitter-ms 800`, and budgets `1 2 3`. It is designed to test MINT under path uncertainty, profile mismatch, timing jitter, and budget pressure.

The confirmed paper core comparison reports five rows:

| Row | Method | Role |
|---|---|---|
| 1 | `mint_markov_full` (MINT) | proposed method |
| 2 | `orion_full` (ORION, OSDI'22) | strongest DAG-aware prewarming baseline, scheduling-layer reproduction of the open-sourced system |
| 3 | `faascache` (FaasCache, ASPLOS'21) | strongest budgeted keep-alive baseline: Greedy-Dual-Size-Frequency cache where B is the number of proactive warmup slots |
| 4 | `xanadu_full` (Xanadu, Middleware'20) | strongest cascading-cold-start baseline: most-likely-path detection with just-in-time speculative provisioning |
| 5 | `no_warmup` | cold-start reference row (not a deployable baseline) |

Every baseline runs under the same budget B (1/2/3), the same
`wide_branch`/`deep_mixed` workloads, the same profile mismatch and timing
jitter, the same seeds, and the same trace-calibrated parameters (Azure
Functions public trace: branch probabilities, stage gaps, warm duration,
cold-start model). The remaining strategies (`periodic_keepwarm`,
`static_dag`, `orion_like`, `path_aware_greedy`, `xanadu_like`, `oracle_path`,
`provisioned_concurrency`, and the MINT ablations) stay available in the code
as appendix/ablation material, not core comparisons.

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
`mint_markov_full` supports `warmup_budget` 1/2/3 on branch DAGs
(`wide_branch`, `deep_mixed`, and the ablation workloads). The two
intent-maintenance ablations (`mint_markov_no_cancel` / `mint_markov_cancel_only`)
remain defined only for B=2, and the `adaptive_branch` workload keeps its
explicit B=2/initial=1 contract. The matrix runner records any genuinely
unsupported cells in `skipped_runs.jsonl` and the manifest lists
`skipped_count`; the paper core matrix (wide_branch/deep_mixed x 5 rows
x B=1/2/3) is expected to complete with zero skips and is checked by
`scripts/audit_adaptive_stress_results.py` (or `--audit` on the matrix
runner).

Dry-run scope note: dry-run mode is a pipeline and mechanism check. It
simulates cold/warm invocation latencies as recorded event fields but does not
sleep cold-start wall-clock time, so measured dry-run end-to-end latency mostly
reflects warmup overhead and must not be used as performance evidence. Paper
latency claims should come from real AWS runs (measured wall-clock latency).

Quick dry-run validation:

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

Recommended first formal AWS matrix:

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

Each run writes one directory named with DAG, baseline, budget, and timestamp. The matrix root also contains:

```text
summary_matrix.csv
summary_matrix.json
failed_runs.jsonl
experiment_manifest.json
```

Use `--randomize-order` to reduce bias from fixed Lambda heat state order. Use `--cooldown-sec` to give Lambda environments time to cool between configurations.

## Summarize Results

```bash
python scripts/summarize_results.py --results-dir results
```

This creates `summary.csv` and `summary.json` under the results directory.

For paper-ready CSV tables from a matrix run:

```bash
python scripts/prepare_paper_tables.py \
  --matrix-csv results/dryrun_matrix_test/summary_matrix.csv \
  --output-dir results/dryrun_matrix_test/paper_tables
```

This writes `table_latency_by_dag.csv`, `table_warmup_efficiency.csv`, `table_action_counts.csv`, `table_budget_sensitivity.csv`, `table_overall_summary.csv`, `table_mint_improvement.csv`, and `table_run_variability.csv`. Paper tables use `effective_planner` for method labeling; `planner_type` is retained only as configuration provenance.

Additional credibility checks:

```bash
python scripts/analyze_baseline_overlap.py \
  --results-dir results/aws_baseline_main_v2_20260606_114412 \
  --output-dir results/aws_baseline_main_v2_20260606_114412/overlap

python scripts/prepare_pareto_data.py \
  --matrix-csv results/aws_baseline_main_v2_20260606_114412/summary_matrix.csv \
  --output-dir results/aws_baseline_main_v2_20260606_114412/pareto
```

Overlap analysis checks whether baseline warmup target sets are nearly identical, and also writes sequence, frequency-vector, and timing-bucket overlap tables. High target-set overlap alone does not prove two baselines are equivalent; if target, frequency, timing, and action summaries are all high-overlap, the small DAG is probably not separating those strategies well. Pareto data supports cost-latency plots using `total_warmup` as cost and average or P95 latency as the performance axis; `latency_per_warmup` is retained only as a diagnostic ratio, not the primary comparison rule.

## Seed Statistics and Cost-Latency

The matrix runner accepts `--seeds 42 43 44 45 46`; each (dag, baseline, budget, seed)
cell runs `--repetitions` workflow samples.  Paper analysis should aggregate
over seeds with bootstrap confidence intervals rather than reporting a single
seed's point estimate:

```bash
python scripts/analyze_seed_statistics.py \
  --matrix-csv results/aws_adaptive_stress_main/summary_matrix.csv \
  --output-dir results/aws_adaptive_stress_main/seed_stats \
  --bootstrap-iters 2000
```

It writes `seed_statistics.csv` (mean + 95% CI per dag/baseline/budget/metric)
and `pairwise_dominance.csv` (bootstrap probability that MINT P95 is lower than
each other method, with mean difference and CI).  Cost-latency uses an explicit
pricing model: warmup invocations cost `--invocation-price-usd` each, while
Provisioned Concurrency costs `--provisioned-price-usd-per-slot-sec` per
provisioned slot-second on a separate axis:

```bash
python scripts/analyze_cost_latency.py \
  --matrix-csv results/aws_adaptive_stress_main/summary_matrix.csv \
  --output-dir results/aws_adaptive_stress_main/cost_latency
```

`Best-fixed` remains an analysis-stage envelope: it is the pre-registered,
deterministic rule "minimum P95 per (dag, budget) among periodic_keepwarm /
static_dag / orion_like", and paper tables also report those three fixed
baselines separately.

## Trace-Calibrated Workloads

The default workloads are synthetic DAG microbenchmarks.  To make them
representative of real serverless traffic, download a slice of the public
Microsoft Azure Functions dataset and calibrate the workload parameters:

```bash
python scripts/download_azure_trace.py --output-dir data/azure_trace --days 1
python scripts/apply_trace_calibration.py \
  --trace-dir data/azure_trace \
  --config configs/mint_aws_real.yaml \
  --output configs/mint_aws_real_tracecal.yaml
```

The downloader fetches the official Azure Functions 2019 dataset from GitHub
Releases (Azure/AzurePublicDataset), which works on networks where the legacy
blob-storage hostname does not resolve, and extracts the aggregate per-day CSVs
(invocation counts, duration percentiles, memory percentiles).  Calibration
sets branch probabilities from observed call counts, stage spacing from
inter-arrival time, warm duration from the duration median, and the cold-start
model from observed memory.  The same matrix protocol then runs on the
trace-calibrated parameters; the trace source is recorded in the config under
`experiment.trace_calibration.source`.

## Delay-Shift Experiment

The main matrix primarily evaluates Cancel, Replace, and warmup efficiency. It may produce `delay_count=0`, so it should not be used alone to claim that Delay contributes to MINT. Use the supplemental delay-shift stress test to validate the runtime rescheduling mechanism:

```bash
python scripts/run_delay_shift_experiment.py \
  --config configs/mint_aws.yaml \
  --baselines static_dag mint_markov_full \
  --repetitions 3 \
  --upstream-delay-ms 1200 \
  --dry-run \
  --output-dir results/dryrun_delay_shift_test
```

It writes per-baseline `events.jsonl`, `runs.csv`, `summary.json`, plus a shared `delay_analysis.csv`. A delayed intent is re-executed as a `delayed_execute` warmup before the downstream real invocation. This is a constructed mechanism test, not a natural production traffic distribution.

Delay-shift reports separate latency fields: `logical_end_to_end_latency_ms` is the stress-test model latency, `measured_wall_clock_latency_ms` is controller wall-clock time, `lambda_invocation_latency_ms_sum` is the sum of controller-side Lambda invoke elapsed times, and `reported_end_to_end_latency_ms` is the selected comparison metric. The default `latency_metric_used` is `measured_wall_clock_latency_ms`; use that for AWS-observed latency tables. Logical latency may be used only for mechanism explanation.

## EC2

See `docs/ec2_runbook.md` for setup, dry-run validation, real-run execution, result download, and troubleshooting steps.

## Baselines

Supported baselines are:

- `no_warmup`
- `periodic`
- `periodic_keepwarm`
- `independent`
- `static_dag`
- `static_dag_unlimited`
- `orion_like`
- `orion_full`
- `faascache`
- `xanadu_full`
- `xanadu_like`
- `path_aware_greedy`
- `oracle_path`
- `mint_offline`
- `mint_offline_unlimited`
- `mint_full`
- `mint_markov_offline`
- `mint_markov_full`
- `provisioned_concurrency`

`mint_full` uses offline intents plus the runtime scheduler. For fair comparison, `static_dag`, `mint_offline`, and `mint_full` all obey `experiment.warmup_budget`. The `_unlimited` variants are available only when you intentionally want an unbounded static/offline comparison.

`mint_offline` and `mint_full` preserve the original heuristic planner behavior. `mint_markov_offline` uses the Markov policy analyzer without runtime adaptation, while `mint_markov_full` combines Markov-generated intents with runtime-adaptive scheduling.

`periodic_keepwarm` is an industrial keep-warm baseline: it does not use DAG structure, stage order, or branch/path information, and selects functions in a round-robin keep-warm order while respecting `warmup_budget`. `static_dag` uses a fixed DAG-aware offline order and does not do runtime cancel, replace, or delay. `orion_like` is an ORION-style DAG-aware right-prewarming approximation: it uses DAG profile, stage order, expected function start time, and fixed look-ahead prewarming for downstream functions. It does not implement ORION right-sizing, bundling, or a complete ORION reproduction; describe it only as a DAG-aware fixed right-prewarming baseline.

`orion_full` is the full ORION reproduction at the scheduling layer: it maintains a container cache driven by exponentially-decayed historical invocation counts, performs right-sizing by choosing a memory tier that maximizes value/cost for each function or bundle, and bundles functions on the same DAG path so one warmup call covers the whole chain. In dry-run it models all three mechanisms; a fully faithful real AWS run additionally requires per-memory-tier deployments and composed (bundled) Lambda functions.

`faascache` is the FaasCache (ASPLOS'21) Greedy-Dual-Size-Frequency keep-alive
cache at the scheduling layer: the budget B bounds the number of proactive
warmup slots, values are `cost * freq / size + L` (cold-start latency saved,
invocation frequency with optional recency decay, container memory footprint,
and the GDS aging factor equal to the last evicted value), and warmups fill
free slots or replace the lowest-value resident. Real invocations update
frequency and populate the cache exactly like accesses in the original
system. `xanadu_full` is the Xanadu (Middleware'20) most-likely-path
just-in-time prewarming reproduction: it derives the MLP from the
trace-calibrated and learned branch probabilities, and spends the B warmup
slots on the earliest-needed MLP members first, never warming the entry
function (the workflow trigger) or off-MLP functions.

`path_aware_greedy` and `xanadu_like` are the older approximation rows kept
for appendix comparisons: they warm the highest expected-gain candidates
under the same budget without MINT's Markov policy and without a faithful
Xanadu MLP/JIT reproduction. `oracle_path` is an ideal upper bound with
advance knowledge of the realized path; it is not a deployable baseline.

`provisioned_concurrency` models AWS Lambda Provisioned Concurrency: the budget B is spent on pre-provisioned slots for the highest-value functions, which are always warm, and cost is accounted on a separate axis (`provisioned_slots_total` x `provisioned_duration_sec_total`) rather than warmup invocation count. Use `scripts/manage_provisioned_concurrency.py` to configure/remove real provisioned concurrency before and after an AWS matrix; the experiment runner never calls AWS provisioning itself.

The optional `mixed` workload combines branch choice, join-style downstream convergence, and f4/f5 downstream timing. It is intended for stronger runtime-adaptation evaluation without changing the original `chain`, `fanout`, `branch`, and `join` workloads.

## Planner Selection

Use the lightweight heuristic planner:

```yaml
planner:
  type: heuristic
```

Use the paper-oriented finite-horizon Markov analyzer:

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

For paper experiments, use `mint_markov_full` as the formal MINT result. Keep `mint_full` as a heuristic prototype ablation so the paper can separate Markov offline planning from runtime scheduling.

## Metrics

- `cold_start_count`: real invocations that observed a cold start.
- `missed_warmup`: cold real invocations for functions that had an executed warmup event in that workflow run, but were still cold at real invocation time.
- `unserved_intent_cold_start`: cold real invocations for functions that had a warmup intent or scheduler candidate, but the warmup was not executed because of budget pressure, `replace`, `cancel`, or delay beyond the call.
- `uncovered_cold_start`: cold real invocations with no warmup event and no warmup intent coverage. For `no_warmup`, `missed_warmup` and `unserved_intent_cold_start` are expected to be `0`; cold starts appear here instead.
- `useful_warmup`: warmup invocations marked useful by the current controller because the function is on the realized workflow path.
- `wasted_warmup`: warmup invocations not marked useful, including branch-path warmups that are never called. The current implementation does not yet distinguish already-HOT no-benefit warmups as a separate field.
- `useful_warmup_ratio`: `useful_warmup / total_warmup`.

## Heuristic Components

The current offline planner uses a heuristic approximation of a future Markov Policy Planner. It estimates stage timing, validity windows, criticality, call probability, cold-start risk, and downstream benefit. The interfaces are stable so the planner can later be replaced with a true Markov decision policy optimizer.

`mint/markov_policy.py` now provides that finite-horizon Markov analyzer for the small DAGs used in the current experiments. It enumerates budget-constrained warmup actions, applies branch probabilities and retention buckets, and converts the resulting policy into warmup intents. It is intentionally small-DAG oriented rather than a production-scale state-space optimizer.
