from mint.markov_policy import MarkovAction, MarkovPolicyAnalyzer, MarkovTransitionModel
from mint.workloads import get_workload


def _config(budget=2):
    return {
        "aws": {"lambda_functions": {f"f{i}": f"mint-f{i}" for i in range(1, 9)}},
        "experiment": {"warmup_budget": budget},
        "platform": {"default_retention_sec": 300, "default_cold_start_ms": 800, "default_warm_duration_ms": 100},
        "planner": {
            "type": "markov",
            "horizon": 5,
            "warmup_cost": 0.1,
            "cold_start_penalty_weight": 1.0,
            "wasted_warmup_penalty_weight": 0.2,
            "missed_warmup_penalty_weight": 0.5,
            "retention_bucket_sec": 60,
            "branch_probability_left": 0.7,
        },
    }


def test_chain_dag_generates_non_empty_policy_and_intents():
    analyzer = MarkovPolicyAnalyzer(get_workload("chain"), _config(), budget=2)
    policy = analyzer.analyze()
    intents = analyzer.generate_intents()
    assert policy
    assert intents
    assert {intent.logical_name for intent in intents}.issubset({"f1", "f2", "f3"})


def test_markov_actions_satisfy_warmup_budget():
    analyzer = MarkovPolicyAnalyzer(get_workload("fanout"), _config(budget=1), budget=1)
    state = analyzer.transition_model.initial_state()
    actions = analyzer.enumerate_actions(state)
    assert actions
    assert all(len(action.warmup_functions) <= 1 for action in actions)


def test_branch_dag_uses_branch_probability():
    dag = get_workload("branch")
    model = MarkovTransitionModel(dag, _config())
    state = model.initial_state()
    transitions = model.transition(state, MarkovAction(tuple()))
    assert len(transitions) == 2
    probabilities = sorted(round(t.probability, 3) for t in transitions)
    assert probabilities == [0.3, 0.7]
    assert {t.next_state.branch_path for t in transitions} == {"left", "right"}


def test_join_dag_generates_legal_stage_transitions():
    dag = get_workload("join")
    model = MarkovTransitionModel(dag, _config())
    initial = model.initial_state()
    after_f1 = model.transition(initial, MarkovAction(tuple()))[0].next_state
    assert after_f1.frontier == ("f2", "f3")
    after_parallel = model.transition(after_f1, MarkovAction(tuple()))[0].next_state
    assert after_parallel.frontier == ("f4",)


def test_value_iteration_runs_for_join():
    analyzer = MarkovPolicyAnalyzer(get_workload("join"), _config(), budget=2)
    policy = analyzer.analyze()
    assert policy
    assert all(len(action.warmup_functions) <= 2 for action in policy.values())


def test_greedy_trap_markov_transition_branches_then_converges():
    dag = get_workload("greedy_trap")
    model = MarkovTransitionModel(dag, _config())
    initial = model.initial_state()
    transitions = model.transition(initial, MarkovAction(tuple()))
    assert len(transitions) == 3
    assert {t.next_state.branch_path for t in transitions} == {"f2", "f3", "f4"}
    assert all(t.next_state.frontier in {("f2",), ("f3",), ("f4",)} for t in transitions)
    after_branch = model.transition(transitions[0].next_state, MarkovAction(tuple()))[0].next_state
    assert after_branch.frontier == ("f5",)


def test_adaptive_branch_uses_empirical_probability_map():
    config = _config()
    config["planner"]["branch_probabilities"] = {"f2": 0.7, "f3": 0.1, "f4": 0.1, "f5": 0.1}
    config["aws"]["lambda_functions"]["f9"] = "mint-f9"
    model = MarkovTransitionModel(get_workload("adaptive_branch"), config)
    transitions = model.transition(model.initial_state(), MarkovAction(tuple()))
    assert {transition.next_state.branch_path: transition.probability for transition in transitions} == {
        "f2": 0.7,
        "f3": 0.1,
        "f4": 0.1,
        "f5": 0.1,
    }
    assert {transition.next_state.frontier for transition in transitions} == {
        ("f2",), ("f3",), ("f4",), ("f5",)
    }
