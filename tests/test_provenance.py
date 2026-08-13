from __future__ import annotations

import hashlib
import json
from pathlib import Path

from mint.provenance import collect_git_provenance, write_experiment_provenance


def test_write_experiment_provenance_hashes_exact_resolved_config(tmp_path: Path) -> None:
    resolved = {"experiment": {"dag": "adaptive_branch", "budget": 2}}

    provenance = write_experiment_provenance(
        tmp_path,
        resolved,
        repository=Path(__file__).resolve().parents[1],
    )

    config_payload = (tmp_path / "resolved_config.json").read_bytes()
    assert json.loads(config_payload) == resolved
    assert provenance["resolved_config"]["sha256"] == hashlib.sha256(
        config_payload
    ).hexdigest()
    assert json.loads((tmp_path / "provenance.json").read_text(encoding="utf-8")) == provenance
    assert provenance["git"]["available"] is True
    assert provenance["git"]["commit_sha"]
    assert provenance["runtime"]["python_version"]


def test_collect_git_provenance_degrades_cleanly_outside_repository(tmp_path: Path) -> None:
    provenance = collect_git_provenance(tmp_path)

    assert provenance["available"] is False
    assert provenance["commit_sha"] is None
    assert provenance["dirty"] is None
