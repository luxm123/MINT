from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import calibrate_adaptive_pending_delay


def test_pending_delay_calibration_dry_run_writes_recommendation(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "calibration"

    rc = calibrate_adaptive_pending_delay.main(
        [
            "--config",
            "configs/mint_aws_adaptive_smoke.yaml",
            "--pool",
            "mint_full_pool",
            "--samples",
            "3",
            "--safety-margin-ms",
            "25",
            "--dry-run",
            "--output-root",
            str(output_root),
        ]
    )

    assert rc == 0
    report = json.loads(
        (output_root / "pending_delay_calibration.json").read_text(encoding="utf-8")
    )
    assert report["dry_run"] is True
    assert report["paper_performance_eligible"] is False
    assert report["samples"] == 3
    assert report["recommended_adaptive_pending_delay_ms"] >= 25
    assert len(report["rows"]) == 3
    assert report["provenance"]["git"]["commit_sha"]


def test_pending_delay_calibration_rejects_too_few_samples(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="at least 3"):
        calibrate_adaptive_pending_delay.main(
            [
                "--config",
                "configs/mint_aws_adaptive_smoke.yaml",
                "--pool",
                "mint_full_pool",
                "--samples",
                "2",
                "--dry-run",
                "--output-root",
                str(tmp_path / "bad"),
            ]
        )
