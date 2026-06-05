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

## Summarize Results

```bash
python scripts/summarize_results.py --results-dir results
```

This creates `summary.csv` and `summary.json` under the results directory.

## EC2

See `docs/ec2_runbook.md` for setup, dry-run validation, real-run execution, result download, and troubleshooting steps.

## Baselines

Supported baselines are:

- `no_warmup`
- `periodic`
- `independent`
- `static_dag`
- `static_dag_unlimited`
- `mint_offline`
- `mint_offline_unlimited`
- `mint_full`

`mint_full` uses offline intents plus the runtime scheduler. For fair comparison, `static_dag`, `mint_offline`, and `mint_full` all obey `experiment.warmup_budget`. The `_unlimited` variants are available only when you intentionally want an unbounded static/offline comparison.

## Metrics

- `cold_start_count`: real invocations that observed a cold start.
- `missed_warmup`: cold real invocations for functions that had a warmup event or warmup intent in that workflow run, but were still cold.
- `uncovered_cold_start`: cold real invocations with no warmup event and no warmup intent coverage. For `no_warmup`, `missed_warmup` is expected to be `0` and cold starts appear here instead.
- `useful_warmup`: warmup invocations that covered a function later called in the same workflow run.
- `wasted_warmup`: warmup invocations that did not cover a later real call in the same workflow run.
- `useful_warmup_ratio`: `useful_warmup / total_warmup`.

## Heuristic Components

The current offline planner uses a heuristic approximation of a future Markov Policy Planner. It estimates stage timing, validity windows, criticality, call probability, cold-start risk, and downstream benefit. The interfaces are stable so the planner can later be replaced with a true Markov decision policy optimizer.
