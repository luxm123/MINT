"""Xanadu-style most-likely-path just-in-time prewarming (Middleware'20).

Xanadu (Daw, Bellur & Kulkarni, "Xanadu: Mitigating Cascading Cold Starts in
Serverless Function Chain Deployments", Middleware'20) eliminates cascading
cold starts in function chains with three mechanisms:

  (i)   proactively detecting the (implicit) function workflow,
  (ii)  speculatively provisioning execution sandboxes along the
        most-likely path (MLP) ahead of time, and
  (iii) delaying provisioning until just-in-time to avoid idle-worker cost.

This module implements the scheduling-layer reproduction: the MLP is derived
from the trace-calibrated branch probabilities (or the controller's learned
branch model), and warmup targets are the earliest-stage MLP members that are
not already warm, bounded by the same budget B as every other baseline.
Just-in-time timing is represented by the harness's planned-arrival readiness
accounting rather than by sleeping: JIT ordering means the B slots are spent
on the earliest-needed chain members first, not on the whole path at once.
"""

from __future__ import annotations

from typing import Iterable

from mint.orion import root_to_terminal_paths
from mint.workloads import WorkflowDAG


def most_likely_path(
    dag: WorkflowDAG,
    call_probability: dict[str, float],
) -> list[str]:
    """Return the most-likely root-to-terminal path.

    A path's likelihood is the product of its members' call probabilities
    (entry nodes have probability 1.0).  Ties are broken lexicographically so
    the result is deterministic.
    """
    best_score = -1.0
    best_path: tuple[str, ...] | None = None
    for path in root_to_terminal_paths(dag):
        score = 1.0
        for node in path:
            score *= max(0.0, float(call_probability.get(node, 1.0)))
        path_key = tuple(path)
        if (
            best_path is None
            or score > best_score
            or (score == best_score and path_key < best_path)
        ):
            best_score = score
            best_path = path_key
    if best_path is None:
        return []
    return list(best_path)


def select_jit_targets(
    dag: WorkflowDAG,
    path: Iterable[str],
    hot_until: dict[str, float],
    now: float,
    budget: int,
    *,
    exclude_entry_nodes: bool = True,
) -> list[str]:
    """Pick warmup targets along the MLP in JIT (stage) order.

    Entry nodes are excluded by default: the entry is the workflow trigger and
    is always invoked first, so provisioning it ahead of time is not part of
    Xanadu's cascading-cold-start mechanism (mirrors ORION's entry exclusion).
    """
    if budget <= 0:
        return []
    stages = dag.stages()
    ordered = sorted(
        (
            node
            for node in path
            if not (exclude_entry_nodes and node in dag.entry_nodes)
        ),
        key=lambda node: (stages.get(node, 0), node),
    )
    targets: list[str] = []
    for node in ordered:
        if len(targets) >= budget:
            break
        if hot_until.get(node, 0.0) <= now:
            targets.append(node)
    return targets
