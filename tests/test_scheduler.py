from mint.intent_planner import plan_intents
from mint.scheduler import schedule_intents
from mint.workloads import get_workload


def test_scheduler_can_produce_execute_delay_cancel_replace():
    config = {
        "experiment": {"warmup_budget": 1},
        "platform": {"default_retention_sec": 300, "default_warm_duration_ms": 100},
        "scheduler": {"enable_delay": True, "enable_cancel": True, "enable_replace": True, "gain_threshold": 0.0},
    }
    intents = plan_intents(get_workload("fanout"), config)
    runtime_state = {
        "now_sec": 0.0,
        "call_probability": {"f1": 1.0, "f2": 1.0, "f3": 1.0, "f4": 0.0},
        "hot_until": {},
        "path_benefit": {"f1": 3.0, "f2": 2.0, "f3": 1.5, "f4": 1.0},
    }
    actions = schedule_intents(intents, runtime_state, 1, config)
    action_types = {action.action_type for action in actions}
    assert "execute" in action_types
    assert "replace" in action_types
    assert "cancel" in action_types

    delayed = schedule_intents(intents, {"now_sec": -999.0, "call_probability": {}, "hot_until": {}}, 10, config)
    assert "delay" in {action.action_type for action in delayed}
