from __future__ import annotations

import itertools
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from mint.utils import new_id
from mint.workloads import WorkflowDAG


@dataclass(frozen=True)
class MarkovState:
    frontier: tuple[str, ...]
    hot_ttl: tuple[tuple[str, int], ...]
    completed: tuple[str, ...]
    branch_path: str | None
    time_bucket: int

    @property
    def hot_functions(self) -> frozenset[str]:
        return frozenset(name for name, ttl in self.hot_ttl if ttl > 0)


@dataclass(frozen=True)
class MarkovAction:
    warmup_functions: tuple[str, ...]


@dataclass(frozen=True)
class MarkovTransition:
    probability: float
    next_state: MarkovState
    called_functions: tuple[str, ...]
    cold_functions: tuple[str, ...]
    wasted_warmups: tuple[str, ...]


@dataclass(frozen=True)
class MarkovRewardModel:
    cold_start_penalty_ms: float
    warmup_cost: float
    cold_start_penalty_weight: float
    wasted_warmup_penalty_weight: float
    missed_warmup_penalty_weight: float

    def reward(self, action: MarkovAction, transition: MarkovTransition) -> float:
        cold_cost = self.cold_start_penalty_weight * self.cold_start_penalty_ms * len(transition.cold_functions)
        path_cost = self.cold_start_penalty_ms * len(transition.cold_functions)
        warm_cost = self.warmup_cost * len(action.warmup_functions)
        wasted_cost = self.wasted_warmup_penalty_weight * len(transition.wasted_warmups)
        missed_cost = self.missed_warmup_penalty_weight * len(set(action.warmup_functions) & set(transition.cold_functions))
        return -(cold_cost + path_cost + warm_cost + wasted_cost + missed_cost)


class MarkovTransitionModel:
    def __init__(self, dag: WorkflowDAG, config: dict[str, Any]) -> None:
        self.dag = dag
        planner_cfg = config.get("planner", {})
        platform = config.get("platform", {})
        retention_sec = float(platform.get("default_retention_sec", 300))
        bucket_sec = max(1.0, float(planner_cfg.get("retention_bucket_sec", 60)))
        self.retention_buckets = max(1, int(round(retention_sec / bucket_sec)))
        self.branch_probability_left = float(planner_cfg.get("branch_probability_left", 0.5))
        self.branch_probabilities = {
            str(name): float(probability)
            for name, probability in planner_cfg.get("branch_probabilities", {}).items()
        }

    def initial_state(self) -> MarkovState:
        return MarkovState(
            frontier=tuple(sorted(self.dag.entry_nodes)),
            hot_ttl=tuple(),
            completed=tuple(),
            branch_path=None,
            time_bucket=0,
        )

    def transition(self, state: MarkovState, action: MarkovAction) -> list[MarkovTransition]:
        hot = dict(state.hot_ttl)
        before_hot = set(name for name, ttl in hot.items() if ttl > 0)
        for fn in action.warmup_functions:
            hot[fn] = self.retention_buckets
        after_warm_hot = set(name for name, ttl in hot.items() if ttl > 0)

        if not state.frontier:
            next_state = self._tick(state, hot, tuple(), state.branch_path)
            wasted = tuple(sorted(fn for fn in action.warmup_functions if fn in before_hot))
            return [MarkovTransition(1.0, next_state, tuple(), tuple(), wasted)]

        called = tuple(sorted(state.frontier))
        cold = tuple(fn for fn in called if fn not in after_warm_hot)
        completed = tuple(sorted(set(state.completed) | set(called)))
        for fn in called:
            hot[fn] = self.retention_buckets

        branch_options = self._branch_options(called, state.branch_path)
        transitions: list[MarkovTransition] = []
        for probability, branch_path in branch_options:
            next_frontier = self._next_frontier(completed, called, branch_path)
            next_state = self._tick(state, hot, next_frontier, branch_path, completed)
            reachable = self._reachable_from_state(next_state)
            wasted = tuple(sorted(fn for fn in action.warmup_functions if fn in before_hot or fn not in reachable | set(called)))
            transitions.append(
                MarkovTransition(
                    probability=probability,
                    next_state=next_state,
                    called_functions=called,
                    cold_functions=cold,
                    wasted_warmups=wasted,
                )
            )
        return transitions

    def _branch_options(self, called: tuple[str, ...], current_branch: str | None) -> list[tuple[float, str | None]]:
        if self.dag.name == "branch" and "f1" in called and current_branch is None:
            left = min(max(self.branch_probability_left, 0.0), 1.0)
            return [(left, "left"), (1.0 - left, "right")]
        if self.dag.name in {"mixed", "deep_mixed"} and "f1" in called and current_branch is None:
            left = min(max(self.branch_probability_left, 0.0), 1.0)
            return [(left, "left"), (1.0 - left, "right")]
        if self.dag.name == "wide_branch" and "f1" in called and current_branch is None:
            return self._multi_branch_options(("f2", "f3", "f4", "f5"))
        if self.dag.name == "adaptive_branch" and "f1" in called and current_branch is None:
            return self._multi_branch_options(("f2", "f3", "f4", "f5"))
        if self.dag.name == "greedy_trap" and "f1" in called and current_branch is None:
            probability = 1.0 / 3.0
            return [(probability, branch) for branch in ("f2", "f3", "f4")]
        return [(1.0, current_branch)]

    def _multi_branch_options(self, branches: tuple[str, ...]) -> list[tuple[float, str]]:
        weights = [max(0.0, self.branch_probabilities.get(branch, 1.0 / len(branches))) for branch in branches]
        total = sum(weights)
        if total <= 0:
            weights = [1.0] * len(branches)
            total = float(len(branches))
        return [(weight / total, branch) for branch, weight in zip(branches, weights)]

    def _next_frontier(self, completed: tuple[str, ...], called: tuple[str, ...], branch_path: str | None) -> tuple[str, ...]:
        completed_set = set(completed)
        candidates: set[str] = set()
        for node in called:
            if self.dag.name == "branch" and node == "f1":
                candidates.update(["f2"] if branch_path == "left" else ["f3"])
            elif self.dag.name in {"mixed", "deep_mixed"} and node == "f1":
                candidates.update(["f2"] if branch_path == "left" else ["f3"])
            elif self.dag.name == "wide_branch" and node == "f1":
                candidates.update([branch_path or "f2"])
            elif self.dag.name == "adaptive_branch" and node == "f1":
                candidates.update([branch_path or "f2"])
            elif self.dag.name == "greedy_trap" and node == "f1":
                candidates.update([branch_path or "f2"])
            else:
                candidates.update(self.dag.successors[node])
        next_nodes = []
        preds = self.dag.predecessors
        for node in sorted(candidates):
            if node in completed_set:
                continue
            if self.dag.name == "mixed" and node == "f4" and any(parent in completed_set for parent in preds[node]):
                next_nodes.append(node)
                continue
            elif self.dag.name == "greedy_trap" and node == "f5" and any(parent in completed_set for parent in preds[node]):
                next_nodes.append(node)
                continue
            elif self.dag.name in {"wide_branch", "deep_mixed"} and node == "f6" and any(parent in completed_set for parent in preds[node]):
                next_nodes.append(node)
                continue
            if all(parent in completed_set for parent in preds[node]):
                next_nodes.append(node)
        return tuple(next_nodes)

    def _tick(
        self,
        state: MarkovState,
        hot: dict[str, int],
        next_frontier: tuple[str, ...],
        branch_path: str | None,
        completed: tuple[str, ...] | None = None,
    ) -> MarkovState:
        decayed = tuple(sorted((name, ttl - 1) for name, ttl in hot.items() if ttl - 1 > 0))
        return MarkovState(
            frontier=next_frontier,
            hot_ttl=decayed,
            completed=completed or state.completed,
            branch_path=branch_path,
            time_bucket=state.time_bucket + 1,
        )

    def _reachable_from_state(self, state: MarkovState) -> set[str]:
        reachable = set(state.frontier)
        queue = list(state.frontier)
        while queue:
            node = queue.pop(0)
            if self.dag.name == "branch" and node == "f1":
                children = ["f2"] if state.branch_path == "left" else ["f3"] if state.branch_path == "right" else self.dag.successors[node]
            elif self.dag.name in {"mixed", "deep_mixed"} and node == "f1":
                children = ["f2"] if state.branch_path == "left" else ["f3"] if state.branch_path == "right" else self.dag.successors[node]
            elif self.dag.name == "wide_branch" and node == "f1":
                children = [state.branch_path] if state.branch_path in {"f2", "f3", "f4", "f5"} else self.dag.successors[node]
            elif self.dag.name == "adaptive_branch" and node == "f1":
                children = [state.branch_path] if state.branch_path in {"f2", "f3", "f4", "f5"} else self.dag.successors[node]
            elif self.dag.name == "greedy_trap" and node == "f1":
                children = [state.branch_path] if state.branch_path in {"f2", "f3", "f4"} else self.dag.successors[node]
            else:
                children = self.dag.successors[node]
            for child in children:
                if child not in state.completed and child not in reachable:
                    reachable.add(child)
                    queue.append(child)
        return reachable


class MarkovPolicyAnalyzer:
    def __init__(self, dag: WorkflowDAG, config: dict[str, Any], budget: int | None = None) -> None:
        self.dag = dag
        self.config = config
        self.budget = int(budget if budget is not None else config.get("experiment", {}).get("warmup_budget", 1))
        planner_cfg = config.get("planner", {})
        platform = config.get("platform", {})
        self.horizon = int(planner_cfg.get("horizon", 5))
        self.transition_model = MarkovTransitionModel(dag, config)
        self.reward_model = MarkovRewardModel(
            cold_start_penalty_ms=float(platform.get("default_cold_start_ms", 800)),
            warmup_cost=float(planner_cfg.get("warmup_cost", 0.1)),
            cold_start_penalty_weight=float(planner_cfg.get("cold_start_penalty_weight", 1.0)),
            wasted_warmup_penalty_weight=float(planner_cfg.get("wasted_warmup_penalty_weight", 0.2)),
            missed_warmup_penalty_weight=float(planner_cfg.get("missed_warmup_penalty_weight", 0.5)),
        )
        self.policy: dict[MarkovState, MarkovAction] = {}
        self.values: dict[MarkovState, float] = {}

    def enumerate_actions(self, state: MarkovState) -> list[MarkovAction]:
        candidates = [node for node in self.dag.nodes if node not in state.completed]
        actions = [MarkovAction(tuple())]
        max_size = min(max(0, self.budget), len(candidates))
        for size in range(1, max_size + 1):
            for combo in itertools.combinations(candidates, size):
                actions.append(MarkovAction(tuple(sorted(combo))))
        return actions

    def analyze(self) -> dict[MarkovState, MarkovAction]:
        initial = self.transition_model.initial_state()

        @lru_cache(maxsize=None)
        def value(state: MarkovState, depth: int) -> float:
            if depth >= self.horizon or (not state.frontier and not self.transition_model._reachable_from_state(state)):
                self.values[state] = 0.0
                return 0.0
            best_value = float("-inf")
            best_action = MarkovAction(tuple())
            for action in self.enumerate_actions(state):
                expected = 0.0
                for transition in self.transition_model.transition(state, action):
                    immediate = self.reward_model.reward(action, transition)
                    expected += transition.probability * (immediate + value(transition.next_state, depth + 1))
                if expected > best_value:
                    best_value = expected
                    best_action = action
            self.policy[state] = best_action
            self.values[state] = best_value
            return best_value

        value(initial, 0)
        return self.policy

    def generate_intents(self) -> list[Any]:
        from mint.intent_planner import WarmupIntent

        if not self.policy:
            self.analyze()
        aws_functions = self.config.get("aws", {}).get("lambda_functions", {})
        platform = self.config.get("platform", {})
        default_retention = float(platform.get("default_retention_sec", 300))
        stages = self.dag.stages()
        downstream = self.dag.downstream_counts()
        max_downstream = max(downstream.values() or [1])
        best_by_node: dict[str, tuple[MarkovState, float]] = {}

        for state, action in self.policy.items():
            value = self.values.get(state, 0.0)
            for node in action.warmup_functions:
                current = best_by_node.get(node)
                if current is None or value > current[1]:
                    best_by_node[node] = (state, value)

        intents = []
        for node, (state, value) in best_by_node.items():
            stage = stages.get(node, state.time_bucket)
            planned_time = float(state.time_bucket)
            criticality = 1.0 + downstream.get(node, 0) / max(max_downstream, 1)
            intents.append(
                WarmupIntent(
                    intent_id=new_id("markov-intent"),
                    logical_name=node,
                    function_name=aws_functions.get(node, node),
                    planned_time_sec=planned_time,
                    window_start_sec=max(0.0, planned_time - default_retention),
                    window_end_sec=planned_time + default_retention,
                    priority=int(round(criticality * 100)),
                    offline_gain=round(max(0.0, -value), 3),
                    stage=stage,
                    criticality=round(criticality, 3),
                )
            )
        return sorted(intents, key=lambda item: (-item.offline_gain, item.planned_time_sec, item.logical_name))
