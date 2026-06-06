# EC2 Runbook

## 1. Install Tools

```bash
sudo yum update -y
sudo yum install -y git python3 python3-pip
python3 -m venv .venv
```

On Ubuntu, use `sudo apt-get update && sudo apt-get install -y git python3 python3-venv python3-pip`.

## 2. Clone Repository

```bash
git clone https://github.com/luxm123/MINT.git
cd MINT
```

## 3. Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 4. Install Dependencies

```bash
pip install -r requirements.txt
```

## 5. Configure AWS Region

Use the EC2 instance role when possible. Otherwise configure credentials outside the repository.

```bash
aws configure set region us-east-1
```

## 6. Confirm Lambda Functions

Check that the names in `configs/mint_aws.yaml` exist:

```bash
aws lambda get-function --function-name mint-f1
```

Repeat for `mint-f2` through `mint-f5`, or update the config to match your deployed functions.

## 7. Run Dry-Run First

```bash
python scripts/run_mint_experiment.py --config configs/mint_aws.yaml --dag chain --baseline mint_full --repetitions 2 --dry-run
```

For a matrix smoke test:

```bash
python scripts/run_experiment_matrix.py \
  --config configs/mint_aws.yaml \
  --dags chain \
  --baselines no_warmup static_dag mint_markov_offline mint_markov_full mint_full \
  --budgets 2 \
  --repetitions 2 \
  --cooldown-sec 1 \
  --randomize-order \
  --dry-run \
  --output-root results/dryrun_matrix_test
```

## 8. Run Real Experiment

```bash
python scripts/run_mint_experiment.py --config configs/mint_aws.yaml --dag chain --baseline mint_full --repetitions 5 --confirm-real-run --output-dir results/chain_mint_full
```

Real AWS calls are refused unless `--confirm-real-run` is present and dry-run is disabled in the config.

Recommended first formal matrix:

```bash
python scripts/run_experiment_matrix.py \
  --config configs/mint_aws_real.yaml \
  --dags chain fanout branch join \
  --baselines no_warmup static_dag mint_markov_offline mint_markov_full mint_full \
  --budgets 1 2 3 \
  --repetitions 10 \
  --cooldown-sec 120 \
  --randomize-order \
  --confirm-real-run \
  --output-root results/aws_matrix_main
```

Real AWS experiments may create Lambda, CloudWatch Logs, and data transfer costs. Keep `--dry-run` for local validation and use `--confirm-real-run` only when you are ready to spend real AWS resources.

## 9. Download Results

```bash
scp -r ec2-user@EC2_HOST:/path/to/MINT/results ./results
```

## 10. Summarize Results

```bash
python scripts/summarize_results.py --results-dir results
```

For matrix runs, use the generated `summary_matrix.csv` directly or prepare paper tables:

```bash
python scripts/prepare_paper_tables.py \
  --matrix-csv results/aws_matrix_main/summary_matrix.csv \
  --output-dir results/aws_matrix_main/paper_tables
```

`--randomize-order` avoids always giving the same baseline first access to freshly cold Lambda environments. `--cooldown-sec` inserts a pause between configurations so retained Lambda execution environments are less likely to leak across treatments.

Use `mint_markov_full` as the formal MINT baseline and keep `mint_full` as a heuristic ablation. The paper table step also writes `table_overall_summary.csv`, `table_mint_improvement.csv`, and `table_run_variability.csv`.

Run the delay-shift supplemental experiment when you need evidence for the Delay mechanism:

```bash
python scripts/run_delay_shift_experiment.py \
  --config configs/mint_aws.yaml \
  --baseline mint_markov_full \
  --repetitions 3 \
  --upstream-delay-ms 1200 \
  --dry-run \
  --output-dir results/dryrun_delay_shift_test
```

Main matrix results with `delay_count=0` should not be used to claim Delay has been fully validated.

## 11. Troubleshooting

- `AccessDeniedException`: attach an IAM policy that permits `lambda:InvokeFunction`.
- `ResourceNotFoundException`: update `configs/mint_aws.yaml` with the actual Lambda names.
- `NoCredentialsError`: use an EC2 role or configure AWS credentials outside this repository.
- High latency variance: increase repetitions and compare baselines under the same region and workload.
