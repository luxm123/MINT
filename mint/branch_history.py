from __future__ import annotations

import csv
import json
from collections import Counter, deque
from pathlib import Path
from typing import Any, Iterable


class BranchHistoryModel:
    """Causal empirical branch model: a snapshot only contains past observations."""

    def __init__(
        self,
        branches: Iterable[str],
        records: Iterable[str] = (),
        prior_alpha: float = 0.0,
        window_size: int | None = None,
    ) -> None:
        self.branches = tuple(branches)
        if not self.branches:
            raise ValueError("branches must not be empty")
        self.prior_alpha = max(0.0, float(prior_alpha))
        self.window_size = int(window_size) if window_size else None
        if self.window_size is not None and self.window_size <= 0:
            raise ValueError("window_size must be positive")
        self._counts: Counter[str] = Counter()
        self._records: deque[str] = deque()
        for branch in records:
            self.observe(branch)

    @classmethod
    def from_config(cls, branches: Iterable[str], planner_cfg: dict[str, Any]) -> "BranchHistoryModel":
        records = list(planner_cfg.get("historical_branch_records", []))
        history_path = planner_cfg.get("branch_history_path")
        if history_path:
            records.extend(read_branch_records(Path(history_path)))
        return cls(
            branches,
            records,
            float(planner_cfg.get("branch_prior_alpha", 0.0)),
            planner_cfg.get("branch_history_window"),
        )

    def observe(self, branch: str) -> None:
        if branch not in self.branches:
            raise ValueError(f"unknown branch observation: {branch}")
        if self.window_size is not None and len(self._records) >= self.window_size:
            expired = self._records.popleft()
            self._counts[expired] -= 1
        self._records.append(branch)
        self._counts[branch] += 1

    def snapshot(self) -> dict[str, Any]:
        total = sum(self._counts.values())
        denominator = total + self.prior_alpha * len(self.branches)
        if denominator == 0:
            probabilities = {branch: 1.0 / len(self.branches) for branch in self.branches}
        else:
            probabilities = {
                branch: (self._counts[branch] + self.prior_alpha) / denominator
                for branch in self.branches
            }
        return {
            "history_size": total,
            "counts": {branch: self._counts[branch] for branch in self.branches},
            "probabilities": probabilities,
        }


def read_branch_records(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() == ".jsonl":
        records = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                value = json.loads(line)
                records.append(str(value["branch"] if isinstance(value, dict) else value))
        return records
    with path.open(newline="", encoding="utf-8") as fh:
        return [str(row["branch"]) for row in csv.DictReader(fh)]
