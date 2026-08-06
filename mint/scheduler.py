from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from mint.intent_planner import WarmupIntent

ActionType = Literal[
    "execute",
    "delay",
    "cancel_pending",
    "invalidate_executed",
    "replacement_warmup",
]


@dataclass(frozen=True)
class WarmupAction:
    action_type: ActionType
    intent: WarmupIntent
    gain: float
    action_reason: str
    scheduled_time_sec: float | None = None


def _compute_gain(intent: WarmupIntent, runtime_state: dict[str, Any], config: dict[str, Any]) -> float:
    logical = intent.logical_name
    platform = config.get("platform", {})
    warm_cost = float(platform.get("default_warm_duration_ms", 100)) / 1000.0
    p_call = float(runtime_state.get("call_probability", {}).get(logical, 1.0))
    hot_until_value = float(runtime_state.get("hot_until", {}).get(logical, 0.0))
    p_cold = 0.0 if hot_until_value > 0.0 and hot_until_value > runtime_state.get("now_sec", 0.0) else 1.0
    p_valid = 1.0 if intent.window_start_sec <= runtime_state.get("now_sec", 0.0) <= intent.window_end_sec else 0.5
    path_benefit = float(runtime_state.get("path_benefit", {}).get(logical, intent.criticality))
    return (p_call * p_cold * p_valid * path_benefit) - warm_cost


def schedule_intents(
    intents: list[WarmupIntent],
    runtime_state: dict[str, Any],
    budget: int,
    config: dict[str, Any],
) -> list[WarmupAction]:
    scheduler_cfg = config.get("scheduler", {})
    now_sec = float(runtime_state.get("now_sec", 0.0))
    threshold = float(scheduler_cfg.get("gain_threshold", 0.0))
    retention = float(config.get("platform", {}).get("default_retention_sec", 300))

    candidates: list[WarmupAction] = []
    non_candidates: list[WarmupAction] = []

    for intent in intents:
        logical = intent.logical_name
        gain = round(_compute_gain(intent, runtime_state, config), 4)
        call_prob = float(runtime_state.get("call_probability", {}).get(logical, 1.0))
        hot_until = float(runtime_state.get("hot_until", {}).get(logical, 0.0))

        if call_prob <= 0.0 and scheduler_cfg.get("enable_cancel", True):
            non_candidates.append(WarmupAction("cancel_pending", intent, gain, "function_not_expected_to_be_called"))
        elif hot_until > 0.0 and hot_until - now_sec >= retention * 0.5 and scheduler_cfg.get("enable_cancel", True):
            non_candidates.append(WarmupAction("cancel_pending", intent, gain, "function_already_hot_with_sufficient_retention"))
        elif now_sec < intent.window_start_sec and scheduler_cfg.get("enable_delay", True):
            non_candidates.append(WarmupAction("delay", intent, gain, "too_early_for_validity_window", intent.window_start_sec))
        elif gain < threshold and scheduler_cfg.get("enable_cancel", True):
            non_candidates.append(WarmupAction("cancel_pending", intent, gain, "negative_or_below_threshold_gain"))
        else:
            candidates.append(WarmupAction("execute", intent, gain, "positive_gain_within_budget"))

    budget = max(0, int(budget))
    ranked_candidates = sorted(candidates, key=lambda item: item.gain, reverse=True)
    selected = ranked_candidates[:budget]
    overflow = ranked_candidates[budget:]
    for action in overflow:
        if scheduler_cfg.get("enable_replace", True):
            non_candidates.append(WarmupAction("cancel_pending", action.intent, action.gain, "replaced_by_higher_gain_candidate"))
        else:
            non_candidates.append(WarmupAction("delay", action.intent, action.gain, "budget_exceeded", now_sec + 1.0))
    return selected + non_candidates
