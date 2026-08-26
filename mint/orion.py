"""ORION-style baseline semantics for serverless DAG warmup.

This module implements the three mechanisms ORION (OSDI'22, "ORION and the
Three Rights: Sizing, Bundling, and Prewarming for Serverless DAGs") contributes:

- container cache: keep warmed containers alive across invocations, driven by
  exponentially-decayed historical invocation counts;
- right-sizing: per function/bundle choose a memory tier from a discrete set;
  larger memory shortens the modeled cold-start latency but raises the cost of
  every warmup invocation (Lambda pricing is memory x duration);
- bundling: functions on the same DAG invocation path are composed into one
  container, so a single warmup call covers the whole bundle.

The controller turns the decisions produced here into WarmupActions under the
same per-run warmup budget B as every other baseline.  On real AWS, a fully
faithful run additionally requires per-memory-tier deployments and composed
(bundled) functions; the dry-run/simulation models the decisions and their
latency/cost effects directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from mint.workloads import WorkflowDAG


MEMORY_OPTIONS = (128, 256, 512, 1024)


def cold_latency_ms(memory_mb: int, base_cold_ms: float) -> float:
    """Model cold-start latency as a decreasing function of memory (AWS-like)."""
    factor = (128.0 / float(memory_mb)) ** 0.25
    return base_cold_ms * factor


def memory_cost_units(memory_mb: int) -> float:
    """Cost of one warmup invocation at a memory tier, normalized to 128MB."""
    return float(memory_mb) / 128.0


@dataclass(frozen=True)
class OrionBundle:
    bundle_id: str
    members: tuple[str, ...]
    representative: str


@dataclass(frozen=True)
class OrionProfile:
    memory_options: tuple[int, ...] = MEMORY_OPTIONS
    decay: float = 0.5
    base_cold_ms: float = 800.0


@dataclass(frozen=True)
class OrionDecision:
    bundle: OrionBundle
    memory_mb: int
    score: float
    gain: float


def root_to_terminal_paths(dag: WorkflowDAG) -> list[list[str]]:
    paths: list[list[str]] = []

    def visit(node: str, path: list[str]) -> None:
        successors = dag.successors.get(node, [])
        if not successors:
            paths.append(path + [node])
            return
        for child in successors:
            visit(child, path + [node])

    for entry in dag.entry_nodes:
        visit(entry, [])
    return paths


def build_bundles(dag: WorkflowDAG) -> list[OrionBundle]:
    """One bundle per root-to-terminal path, excluding entry nodes.

    Warming the entry function is not useful in a per-run benchmark because
    the workflow always invokes it first; ORION bundling targets the
    downstream chain that can actually be served by a prewarmed container.
    """
    seen: set[str] = set()
    bundles: list[OrionBundle] = []
    for path in root_to_terminal_paths(dag):
        members = tuple(node for node in path if node not in dag.entry_nodes)
        if not members:
            continue
        bundle_id = "|".join(members)
        if bundle_id in seen:
            continue
        seen.add(bundle_id)
        bundles.append(
            OrionBundle(
                bundle_id=bundle_id,
                members=members,
                representative=members[0],
            )
        )
    return bundles


def _bundle_score(
    bundle: OrionBundle,
    memory_mb: int,
    call_probability: dict[str, float],
    downstream_weight: dict[str, float],
    profile: OrionProfile,
) -> float:
    benefit = 0.0
    for member in bundle.members:
        p_call = float(call_probability.get(member, 0.0))
        weight = float(downstream_weight.get(member, 1.0))
        benefit += p_call * cold_latency_ms(memory_mb, profile.base_cold_ms) * weight
    return benefit / memory_cost_units(memory_mb)


def decide(
    profile: OrionProfile,
    bundles: Iterable[OrionBundle],
    call_probability: dict[str, float],
    downstream_weight: dict[str, float],
    hot_nodes: set[str],
    warm_bundle_ids: set[str],
    budget: int,
) -> list[OrionDecision]:
    """Greedy ORION allocation: pick the top-B (bundle, memory) by value/cost.

    A bundle is a candidate when at least one of its members is not hot and
    the bundle is not already in the container cache.  Right-sizing picks the
    memory tier that maximizes value/cost for that bundle.
    """
    if budget <= 0:
        return []
    candidates: list[OrionDecision] = []
    for bundle in bundles:
        if bundle.bundle_id in warm_bundle_ids:
            continue
        if all(member in hot_nodes for member in bundle.members):
            continue
        best: OrionDecision | None = None
        for memory_mb in profile.memory_options:
            score = _bundle_score(
                bundle,
                memory_mb,
                call_probability,
                downstream_weight,
                profile,
            )
            if score <= 0.0:
                continue
            if best is None or score > best.score:
                best = OrionDecision(
                    bundle=bundle,
                    memory_mb=memory_mb,
                    score=score,
                    gain=round(score, 4),
                )
        if best is not None:
            candidates.append(best)
    candidates.sort(key=lambda item: (-item.score, item.bundle.bundle_id))
    return candidates[:budget]
