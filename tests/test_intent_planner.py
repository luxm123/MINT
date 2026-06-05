from mint.intent_planner import WarmupIntent, plan_intents
from mint.workloads import get_workload


def test_intent_planner_generates_intents():
    config = {"aws": {"lambda_functions": {"f1": "mint-f1"}}, "platform": {"default_retention_sec": 300}}
    intents = plan_intents(get_workload("chain"), config)
    assert intents
    assert all(isinstance(intent, WarmupIntent) for intent in intents)
    assert {intent.logical_name for intent in intents} == {"f1", "f2", "f3"}
    assert all(intent.window_end_sec >= intent.planned_time_sec for intent in intents)
