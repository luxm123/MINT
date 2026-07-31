from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.analyze_controlled_paired import analyze


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_analyze_controlled_paired_passes_complete_batch(tmp_path: Path) -> None:
    baselines = ["no_warmup", "mint_markov_full"]
    _write_csv(
        tmp_path / "cold_validation.csv",
        [
            {"baseline": baseline, "valid": True, "reason": "valid"}
            for _block in range(2)
            for baseline in baselines
        ],
    )
    _write_csv(
        tmp_path / "reset_log.csv",
        [
            {
                "function_name": f"pool-{baseline}-f1",
                "published_version": block + 1,
                "qualified_function_name": f"pool-{baseline}-f1:{block + 1}",
                "code_sha256": "same",
            }
            for block in range(2)
            for baseline in baselines
        ],
    )
    for baseline in baselines:
        rows = []
        events = []
        audits = []
        for block in range(2):
            latency = 1000 if baseline == "no_warmup" else 700
            order = "mint_markov_full,no_warmup" if block == 0 else "no_warmup,mint_markov_full"
            rows.append(
                {
                    "block_id": f"block-{block:04d}",
                    "latency_ms": latency,
                    "cold_start_count": 4 if baseline == "no_warmup" else 2,
                    "warmup_count": 0 if baseline == "no_warmup" else 2,
                    "arrival_lateness_ms": 1,
                    "warmup_overrun_ms": 0,
                    "strategy_order": order,
                }
            )
            event = {"event_type": "invocation", "status": "ok"}
            events.append(event)
            audits.append({"audit_ok": True})
        _write_csv(tmp_path / baseline / "runs.csv", rows)
        _write_jsonl(tmp_path / baseline / "events.jsonl", events)
        _write_csv(tmp_path / baseline / "cloudwatch_audit.csv", audits)

    report = analyze(tmp_path, baselines, expected_blocks=2)

    assert report["quality_gate_passed"] is True
    assert report["paired"]["latency_delta_ms"]["mean"] == -300
    assert report["paired"]["treatment_slots"] == {0: 1, 1: 1}


def test_analyze_controlled_paired_rejects_audit_failure(tmp_path: Path) -> None:
    test_analyze_controlled_paired_passes_complete_batch(tmp_path)
    _write_csv(tmp_path / "no_warmup" / "cloudwatch_audit.csv", [{"audit_ok": False}])

    report = analyze(tmp_path, ["no_warmup", "mint_markov_full"], expected_blocks=2)

    assert report["quality_gate_passed"] is False
    assert "no_warmup.audit_failures=1" in report["quality_gate_failures"]
