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
        if self.name != "branch" or node != "f1":
            return self.successors[node]
        choice = (context or {}).get("branch", "left")
        return ["f2"] if choice == "left" else ["f3"]


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
}


def get_workload(name: str) -> WorkflowDAG:
    try:
        return WORKLOADS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown workload DAG: {name}") from exc
