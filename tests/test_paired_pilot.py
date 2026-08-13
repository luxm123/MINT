import json
from pathlib import Path

import pytest

from scripts import run_paired_pilot
from scripts.run_paired_pilot import balanced_orders


def test_balanced_orders_are_reproducible_and_slot_balanced():
    baselines = ["no_warmup", "mint_markov_full"]
    first = balanced_orders(baselines, 50, 42)
    second = balanced_orders(baselines, 50, 42)

    assert first == second
    assert all(sorted(order) == sorted(baselines) for order in first)
    assert sum(order[0] == "no_warmup" for order in first) == 25
    assert sum(order[0] == "mint_markov_full" for order in first) == 25


def test_paired_pilot_writes_resolved_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_root = tmp_path / "paired"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
aws:
  lambda_functions: {f1: mint-f1, f2: mint-f2, f3: mint-f3}
  lambda_function_pools:
    no_warmup: {f1: mint-a-f1, f2: mint-a-f2, f3: mint-a-f3}
    static_dag: {f1: mint-b-f1, f2: mint-b-f2, f3: mint-b-f3}
experiment:
  dry_run: true
platform:
  default_retention_sec: 300
  default_cold_start_ms: 800
  default_warm_duration_ms: 100
scheduler:
  enable_delay: true
  enable_cancel: true
  enable_replace: true
  gain_threshold: 0.0
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_paired_pilot.py",
            "--config",
            str(config_path),
            "--dag",
            "chain",
            "--baselines",
            "no_warmup",
            "static_dag",
            "--pools",
            "no_warmup",
            "static_dag",
            "--blocks",
            "1",
            "--slot-spacing-sec",
            "0",
            "--initial-delay-sec",
            "0.01",
            "--warmup-lead-sec",
            "0",
            "--dry-run",
            "--output-root",
            str(output_root),
        ],
    )

    assert run_paired_pilot.main() == 0
    manifest = json.loads(
        (output_root / "experiment_manifest.json").read_text(encoding="utf-8")
    )
    resolved = json.loads(
        (output_root / "resolved_config.json").read_text(encoding="utf-8")
    )
    assert manifest["provenance"]["git"]["commit_sha"]
    assert manifest["completed_at"]
    assert resolved["pilot"]["realized_strategy_orders"]
    assert set(resolved["strategy_configs"]) == {"no_warmup", "static_dag"}
