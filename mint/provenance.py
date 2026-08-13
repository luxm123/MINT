from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


PROVENANCE_SCHEMA_VERSION = 1


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _run_git(repository: Path, arguments: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "-C", str(repository), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=10,
    )


def _git_text(repository: Path, arguments: Sequence[str]) -> str | None:
    result = _run_git(repository, arguments)
    if result.returncode != 0:
        return None
    return result.stdout.decode("utf-8", errors="replace").strip()


def collect_git_provenance(repository: str | Path) -> dict[str, Any]:
    """Return a content-free fingerprint of the current Git checkout.

    The manifest deliberately stores hashes rather than the diff itself.  The
    tracked diff hash covers staged and unstaged changes relative to HEAD, and
    the dirty-state hash additionally covers untracked file paths and content.
    """

    requested_root = Path(repository).resolve()
    try:
        root_text = _git_text(requested_root, ["rev-parse", "--show-toplevel"])
        if not root_text:
            return {
                "available": False,
                "repository_root": str(requested_root),
                "commit_sha": None,
                "branch": None,
                "dirty": None,
                "dirty_diff_sha256": None,
                "dirty_state_sha256": None,
                "untracked_file_count": None,
            }

        repository_root = Path(root_text).resolve()
        commit_sha = _git_text(repository_root, ["rev-parse", "HEAD"])
        branch = _git_text(repository_root, ["rev-parse", "--abbrev-ref", "HEAD"])
        status = _run_git(
            repository_root,
            ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        )
        tracked_diff = _run_git(repository_root, ["diff", "--binary", "HEAD", "--", "."])
        untracked = _run_git(
            repository_root,
            ["ls-files", "--others", "--exclude-standard", "-z"],
        )
        if any(result.returncode != 0 for result in (status, tracked_diff, untracked)):
            raise RuntimeError("unable to inspect Git working tree")

        untracked_paths = sorted(path for path in untracked.stdout.split(b"\0") if path)
        state_parts = [b"status\0", status.stdout, b"tracked-diff\0", tracked_diff.stdout]
        for raw_path in untracked_paths:
            relative_path = Path(os.fsdecode(raw_path))
            candidate = (repository_root / relative_path).resolve()
            try:
                candidate.relative_to(repository_root)
                content_hash = _sha256(candidate.read_bytes()) if candidate.is_file() else ""
            except (OSError, ValueError):
                content_hash = "unreadable"
            state_parts.extend(
                [
                    b"untracked\0",
                    raw_path,
                    b"\0",
                    content_hash.encode("ascii", errors="replace"),
                    b"\0",
                ]
            )

        dirty = bool(status.stdout)
        return {
            "available": True,
            "repository_root": str(repository_root),
            "commit_sha": commit_sha,
            "branch": branch,
            "dirty": dirty,
            "dirty_diff_sha256": _sha256(tracked_diff.stdout),
            "dirty_state_sha256": _sha256(b"".join(state_parts)),
            "untracked_file_count": len(untracked_paths),
        }
    except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
        return {
            "available": False,
            "repository_root": str(requested_root),
            "commit_sha": None,
            "branch": None,
            "dirty": None,
            "dirty_diff_sha256": None,
            "dirty_state_sha256": None,
            "untracked_file_count": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def write_experiment_provenance(
    output_root: str | Path,
    resolved_config: Mapping[str, Any],
    *,
    repository: str | Path,
) -> dict[str, Any]:
    """Persist a resolved config and return metadata suitable for a manifest."""

    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    captured_at = datetime.now(timezone.utc)
    captured_unix_time_ns = time.time_ns()
    git = collect_git_provenance(repository)
    config_path = root / "resolved_config.json"
    config_payload = (
        json.dumps(resolved_config, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        + "\n"
    ).encode("utf-8")
    config_path.write_bytes(config_payload)

    provenance = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "resolved_config": {
            "path": config_path.name,
            "format": "json",
            "sha256": _sha256(config_payload),
        },
        "git": git,
        "runtime": {
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "python_executable": sys.executable,
            "platform": platform.platform(),
        },
        "time": {
            "captured_at_utc": captured_at.isoformat(),
            "unix_time_ns": captured_unix_time_ns,
        },
    }
    (root / "provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return provenance
