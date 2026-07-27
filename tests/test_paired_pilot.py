from scripts.run_paired_pilot import balanced_orders


def test_balanced_orders_are_reproducible_and_slot_balanced():
    baselines = ["no_warmup", "mint_markov_full"]
    first = balanced_orders(baselines, 50, 42)
    second = balanced_orders(baselines, 50, 42)

    assert first == second
    assert all(sorted(order) == sorted(baselines) for order in first)
    assert sum(order[0] == "no_warmup" for order in first) == 25
    assert sum(order[0] == "mint_markov_full" for order in first) == 25
