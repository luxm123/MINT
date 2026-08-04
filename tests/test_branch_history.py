from mint.branch_history import BranchHistoryModel


def test_empirical_probabilities_come_from_history_counts():
    records = ["f2"] * 70 + ["f3"] * 10 + ["f4"] * 10 + ["f5"] * 10
    snapshot = BranchHistoryModel(("f2", "f3", "f4", "f5"), records).snapshot()
    assert snapshot["history_size"] == 100
    assert snapshot["probabilities"] == {"f2": 0.7, "f3": 0.1, "f4": 0.1, "f5": 0.1}


def test_snapshot_is_causal_and_only_changes_after_observe():
    model = BranchHistoryModel(("f2", "f3"), ["f2"])
    before = model.snapshot()
    model.observe("f3")
    after = model.snapshot()
    assert before["probabilities"] == {"f2": 1.0, "f3": 0.0}
    assert after["probabilities"] == {"f2": 0.5, "f3": 0.5}


def test_sliding_window_forgets_stale_branch_distribution():
    model = BranchHistoryModel(("f2", "f5"), ["f2"] * 4, window_size=4)
    for _ in range(4):
        model.observe("f5")
    assert model.snapshot()["probabilities"] == {"f2": 0.0, "f5": 1.0}
