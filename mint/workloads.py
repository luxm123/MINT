from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


BranchRule = Callable[[str, dict], list[str]]


@dataclass(frozen=True)
class WorkflowDAG:
    name: str
    nodes: list[str]
    edges: list[tuple[str, str]]
    entry_nodes: list[str]
    terminal_nodes: list[str]
    branch_rules: dict[str, str] = field(default_factory=dict)

    @property
    def successors(self) -> dict[str, list[str]]:
        result = {node: [] for node in self.nodes}
        for src, dst in self.edges:
            result[src].append(dst)
        return result

    @property
    def predecessors(self) -> dict[str, list[str]]:
        result = {node: [] for node in self.nodes}
        for src, dst in self.edges:
            result[dst].append(src)
        return result

    def stages(self) -> dict[str, int]:
        preds = self.predecessors
        stages: dict[str, int] = {}
        queue = list(self.entry_nodes)
        for entry in queue:
            stages[entry] = 0
        while queue:
            node = queue.pop(0)
            for child in self.successors[node]:
                next_stage = stages[node] + 1
                if next_stage > stages.get(child, -1):
                    stages[child] = next_stage
                if all(parent in stages for parent in preds[child]):
                    queue.append(child)
        return stages

    def downstream_counts(self) -> dict[str, int]:
        succ = self.successors

        def visit(node: str, seen: set[str]) -> set[str]:
            for child in succ[node]:
                if child not in seen:
                    seen.add(child)
                    visit(child, seen)
            return seen

        return {node: len(visit(node, set())) for node in self.nodes}

    def next_nodes(self, node: str, context: dict | None = None) -> list[str]:
        if node != "f1":
            return self.successors[node]
        context = context or {}
        if self.name in {"branch", "mixed", "deep_mixed"}:
            choice = context.get("branch", "left")
            return ["f2"] if choice == "left" else ["f3"]
        if self.name == "wide_branch":
            choices = ["f2", "f3", "f4", "f5"]
            return [choices[int(context.get("branch_index", 0)) % len(choices)]]
        if self.name == "greedy_trap":
            choices = ["f2", "f3", "f4"]
            return [choices[int(context.get("branch_index", 0)) % len(choices)]]
        return self.successors[node]


WORKLOADS: dict[str, WorkflowDAG] = {
    "chain": WorkflowDAG(
        name="chain",
        nodes=["f1", "f2", "f3"],
        edges=[("f1", "f2"), ("f2", "f3")],
        entry_nodes=["f1"],
        terminal_nodes=["f3"],
    ),
    "fanout": WorkflowDAG(
        name="fanout",
        nodes=["f1", "f2", "f3", "f4"],
        edges=[("f1", "f2"), ("f1", "f3"), ("f1", "f4")],
        entry_nodes=["f1"],
        terminal_nodes=["f2", "f3", "f4"],
    ),
    "branch": WorkflowDAG(
        name="branch",
        nodes=["f1", "f2", "f3", "f4", "f5"],
        edges=[("f1", "f2"), ("f2", "f4"), ("f1", "f3"), ("f3", "f5")],
        entry_nodes=["f1"],
        terminal_nodes=["f4", "f5"],
        branch_rules={"f1": "context.branch == 'left' ? f2 : f3"},
    ),
    "join": WorkflowDAG(
        name="join",
        nodes=["f1", "f2", "f3", "f4", "f5"],
        edges=[("f1", "f2"), ("f1", "f3"), ("f2", "f4"), ("f3", "f4"), ("f4", "f5")],
        entry_nodes=["f1"],
        terminal_nodes=["f5"],
    ),
    "mixed": WorkflowDAG(
        name="mixed",
        nodes=["f1", "f2", "f3", "f4", "f5"],
        edges=[("f1", "f2"), ("f1", "f3"), ("f2", "f4"), ("f3", "f4"), ("f4", "f5")],
        entry_nodes=["f1"],
        terminal_nodes=["f5"],
        branch_rules={"f1": "context.branch == 'left' ? f2 : f3; f4 joins whichever branch was taken"},
    ),
    "wide_branch": WorkflowDAG(
        name="wide_branch",
        nodes=["f1", "f2", "f3", "f4", "f5", "f6", "f7"],
        edges=[
            ("f1", "f2"),
            ("f1", "f3"),
            ("f1", "f4"),
            ("f1", "f5"),
            ("f2", "f6"),
            ("f3", "f6"),
            ("f4", "f6"),
            ("f5", "f6"),
            ("f6", "f7"),
        ],
        entry_nodes=["f1"],
        terminal_nodes=["f7"],
        branch_rules={"f1": "runtime selects exactly one of f2/f3/f4/f5; profile_mismatch can skew the realized branch"},
    ),
    "deep_mixed": WorkflowDAG(
        name="deep_mixed",
        nodes=["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"],
        edges=[
            ("f1", "f2"),
            ("f1", "f3"),
            ("f2", "f4"),
            ("f4", "f6"),
            ("f3", "f5"),
            ("f5", "f6"),
            ("f6", "f7"),
            ("f7", "f8"),
        ],
        entry_nodes=["f1"],
        terminal_nodes=["f8"],
        branch_rules={"f1": "context.branch == 'left' ? f2 -> f4 : f3 -> f5; f6 joins whichever branch was taken"},
    ),
    "greedy_trap": WorkflowDAG(
        name="greedy_trap",
        nodes=["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"],
        edges=[
            ("f1", "f2"),
            ("f1", "f3"),
            ("f1", "f4"),
            ("f2", "f5"),
            ("f3", "f5"),
            ("f4", "f5"),
            ("f5", "f6"),
            ("f6", "f7"),
            ("f7", "f8"),
        ],
        entry_nodes=["f1"],
        terminal_nodes=["f8"],
        branch_rules={
            "f1": "runtime selects exactly one early branch f2/f3/f4; all branches converge to the critical suffix f5 -> f6 -> f7 -> f8"
        },
    ),
}


def get_workload(name: str) -> WorkflowDAG:
    try:
        return WORKLOADS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown workload DAG: {name}") from exc
