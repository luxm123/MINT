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

## 8. Run Real Experiment

```bash
python scripts/run_mint_experiment.py --config configs/mint_aws.yaml --dag chain --baseline mint_full --repetitions 5 --confirm-real-run --output-dir results/chain_mint_full
```

Real AWS calls are refused unless `--confirm-real-run` is present and dry-run is disabled in the config.

## 9. Download Results

```bash
scp -r ec2-user@EC2_HOST:/path/to/MINT/results ./results
```

## 10. Summarize Results

```bash
python scripts/summarize_results.py --results-dir results
```

## 11. Troubleshooting

- `AccessDeniedException`: attach an IAM policy that permits `lambda:InvokeFunction`.
- `ResourceNotFoundException`: update `configs/mint_aws.yaml` with the actual Lambda names.
- `NoCredentialsError`: use an EC2 role or configure AWS credentials outside this repository.
- High latency variance: increase repetitions and compare baselines under the same region and workload.
