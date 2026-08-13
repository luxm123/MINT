from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from mint.intent_lifecycle import IntentBudgetLedger, IntentState


def test_reservation_submission_and_failure_keep_exact_budget_accounting():
    ledger = IntentBudgetLedger(2)

    assert ledger.create("intent-a", "f2").accepted
    reserved = ledger.reserve("intent-a", reason="initial reservation")
    assert reserved.accepted
    assert reserved.snapshot.reserved_budget == 1
    assert reserved.snapshot.consumed_budget == 0

    submitted = ledger.submit("intent-a", reason="lambda call submitted")
    assert submitted.accepted
    assert submitted.transitions[0].actual_call_submitted is True
    assert submitted.snapshot.reserved_budget == 0
    assert submitted.snapshot.consumed_budget == 1

    failed = ledger.fail("intent-a", reason="lambda timeout")
    assert failed.accepted
    assert ledger.get_record("intent-a").state is IntentState.FAILED
    assert failed.snapshot.reserved_budget == 0
    assert failed.snapshot.consumed_budget == 1


def test_only_pending_intent_can_be_cancelled():
    ledger = IntentBudgetLedger(1)
    ledger.create("intent-a", "f6")
    ledger.reserve("intent-a")
    ledger.submit("intent-a")

    rejected = ledger.cancel_pending("intent-a", reason="too late")

    assert not rejected.accepted
    assert rejected.reason == "invalid_state:in_flight"
    assert ledger.get_record("intent-a").state is IntentState.IN_FLIGHT
    assert rejected.snapshot.consumed_budget == 1
    assert rejected.snapshot.reserved_budget == 0


def test_atomic_replace_releases_and_reacquires_one_reservation_without_exceeding_budget():
    ledger = IntentBudgetLedger(2)
    ledger.create("executed", "f2")
    ledger.reserve("executed")
    ledger.submit("executed")
    ledger.succeed("executed")
    ledger.create("old-pending", "f6")
    ledger.reserve("old-pending")

    replaced = ledger.atomic_replace(
        "old-pending",
        "replacement",
        "f8",
        reason="branch changed",
    )

    assert replaced.accepted
    assert [transition.operation for transition in replaced.transitions] == [
        "cancel_pending",
        "replacement_reserved",
    ]
    assert ledger.get_record("old-pending").state is IntentState.CANCELLED
    assert ledger.get_record("replacement").state is IntentState.PENDING
    assert replaced.snapshot.reserved_budget == 1
    assert replaced.snapshot.consumed_budget == 1
    submitted = ledger.submit("replacement")
    assert submitted.snapshot.reserved_budget == 0
    assert submitted.snapshot.consumed_budget == 2


def test_cancel_submit_race_has_exactly_one_winner_and_never_refunds_submitted_cost():
    for index in range(25):
        ledger = IntentBudgetLedger(1)
        intent_id = f"intent-{index}"
        ledger.create(intent_id, "f6")
        ledger.reserve(intent_id)
        barrier = Barrier(2)

        def cancel():
            barrier.wait()
            return ledger.cancel_pending(intent_id, reason="branch reveal")

        def submit():
            barrier.wait()
            return ledger.submit(intent_id, reason="timer fired")

        with ThreadPoolExecutor(max_workers=2) as executor:
            cancel_future = executor.submit(cancel)
            submit_future = executor.submit(submit)
            cancel_result = cancel_future.result()
            submit_result = submit_future.result()

        assert int(cancel_result.accepted) + int(submit_result.accepted) == 1
        snapshot = ledger.snapshot()
        record = ledger.get_record(intent_id)
        if submit_result.accepted:
            assert record.state is IntentState.IN_FLIGHT
            assert snapshot.consumed_budget == 1
            assert snapshot.reserved_budget == 0
        else:
            assert record.state is IntentState.CANCELLED
            assert snapshot.consumed_budget == 0
            assert snapshot.reserved_budget == 0
