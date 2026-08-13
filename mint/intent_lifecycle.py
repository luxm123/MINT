from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from threading import RLock
from typing import Any


class IntentState(str, Enum):
    PLANNED = "planned"
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class IntentRecord:
    intent_id: str
    target: str
    cost: int = 1
    state: IntentState = IntentState.PLANNED
    metadata: dict[str, Any] = field(default_factory=dict)
    scheduled_start_time: float | None = None
    supersedes_intent_id: str = ""


@dataclass(frozen=True)
class BudgetSnapshot:
    budget_limit: int
    reserved_budget: int
    consumed_budget: int
    intent_count: int

    @property
    def available_budget(self) -> int:
        return self.budget_limit - self.reserved_budget - self.consumed_budget


@dataclass(frozen=True)
class IntentTransition:
    intent_id: str
    target: str
    state_before: str
    state_after: str
    operation: str
    reason: str = ""
    actual_call_submitted: bool = False
    supersedes_intent_id: str = ""
    transition_seq: int = 0


@dataclass(frozen=True)
class TransitionResult:
    accepted: bool
    operation: str
    transitions: tuple[IntentTransition, ...]
    snapshot: BudgetSnapshot
    reason: str = ""
    decision_seq: int = 0


class IntentBudgetLedger:
    """Thread-safe lifecycle and budget ledger for one workflow run.

    Reservations consume capacity but are reversible. The moment a call is
    submitted, its cost moves atomically from reserved to consumed and is never
    refunded, including when the invocation subsequently fails.
    """

    def __init__(self, budget_limit: int) -> None:
        if int(budget_limit) < 0:
            raise ValueError("budget_limit must be non-negative")
        self._budget_limit = int(budget_limit)
        self._reserved_budget = 0
        self._consumed_budget = 0
        self._records: dict[str, IntentRecord] = {}
        self._lock = RLock()
        self._transition_seq = 0

    def snapshot(self) -> BudgetSnapshot:
        with self._lock:
            return self._snapshot_locked()

    def records(self) -> tuple[IntentRecord, ...]:
        with self._lock:
            return tuple(replace(record, metadata=dict(record.metadata)) for record in self._records.values())

    def get_record(self, intent_id: str) -> IntentRecord | None:
        with self._lock:
            record = self._records.get(intent_id)
            return replace(record, metadata=dict(record.metadata)) if record else None

    def create(
        self,
        intent_id: str,
        target: str,
        cost: int = 1,
        metadata: dict[str, Any] | None = None,
        scheduled_start_time: float | None = None,
    ) -> TransitionResult:
        operation = "create"
        with self._lock:
            if not intent_id:
                return self._reject_locked(operation, "intent_id_required")
            if intent_id in self._records:
                return self._reject_locked(operation, "intent_already_exists")
            if int(cost) <= 0:
                return self._reject_locked(operation, "cost_must_be_positive")
            record = IntentRecord(
                intent_id=intent_id,
                target=target,
                cost=int(cost),
                metadata=dict(metadata or {}),
                scheduled_start_time=scheduled_start_time,
            )
            self._records[intent_id] = record
            transition = self._transition(record, "", IntentState.PLANNED, operation)
            return self._accept_locked(operation, (transition,))

    def reserve(self, intent_id: str, reason: str = "") -> TransitionResult:
        operation = "reserve"
        with self._lock:
            record = self._records.get(intent_id)
            if record is None:
                return self._reject_locked(operation, "intent_not_found")
            if record.state is not IntentState.PLANNED:
                return self._reject_locked(operation, f"invalid_state:{record.state.value}")
            if self._reserved_budget + self._consumed_budget + record.cost > self._budget_limit:
                return self._reject_locked(operation, "budget_exhausted")
            updated = replace(record, state=IntentState.PENDING)
            self._records[intent_id] = updated
            self._reserved_budget += record.cost
            transition = self._transition(updated, IntentState.PLANNED, IntentState.PENDING, operation, reason)
            return self._accept_locked(operation, (transition,))

    def submit(self, intent_id: str, reason: str = "") -> TransitionResult:
        operation = "submit"
        with self._lock:
            record = self._records.get(intent_id)
            if record is None:
                return self._reject_locked(operation, "intent_not_found")
            if record.state is not IntentState.PENDING:
                return self._reject_locked(operation, f"invalid_state:{record.state.value}")
            updated = replace(record, state=IntentState.IN_FLIGHT)
            self._records[intent_id] = updated
            self._reserved_budget -= record.cost
            self._consumed_budget += record.cost
            self._assert_invariants_locked()
            transition = self._transition(
                updated,
                IntentState.PENDING,
                IntentState.IN_FLIGHT,
                operation,
                reason,
                actual_call_submitted=True,
            )
            return self._accept_locked(operation, (transition,))

    def succeed(self, intent_id: str, reason: str = "") -> TransitionResult:
        return self._finish(intent_id, IntentState.SUCCEEDED, "succeed", reason)

    def fail(self, intent_id: str, reason: str = "") -> TransitionResult:
        return self._finish(intent_id, IntentState.FAILED, "fail", reason)

    def cancel_pending(self, intent_id: str, reason: str = "") -> TransitionResult:
        operation = "cancel_pending"
        with self._lock:
            record = self._records.get(intent_id)
            if record is None:
                return self._reject_locked(operation, "intent_not_found")
            if record.state is not IntentState.PENDING:
                return self._reject_locked(operation, f"invalid_state:{record.state.value}")
            updated = replace(record, state=IntentState.CANCELLED)
            self._records[intent_id] = updated
            self._reserved_budget -= record.cost
            self._assert_invariants_locked()
            transition = self._transition(
                updated, IntentState.PENDING, IntentState.CANCELLED, operation, reason
            )
            return self._accept_locked(operation, (transition,))

    def atomic_replace(
        self,
        old_intent_id: str,
        new_intent_id: str,
        target: str,
        cost: int = 1,
        metadata: dict[str, Any] | None = None,
        scheduled_start_time: float | None = None,
        reason: str = "",
    ) -> TransitionResult:
        operation = "atomic_replace"
        with self._lock:
            old = self._records.get(old_intent_id)
            if old is None:
                return self._reject_locked(operation, "old_intent_not_found")
            if old.state is not IntentState.PENDING:
                return self._reject_locked(operation, f"old_intent_not_pending:{old.state.value}")
            if not new_intent_id:
                return self._reject_locked(operation, "new_intent_id_required")
            if new_intent_id in self._records:
                return self._reject_locked(operation, "new_intent_already_exists")
            if int(cost) <= 0:
                return self._reject_locked(operation, "cost_must_be_positive")
            projected_reserved = self._reserved_budget - old.cost + int(cost)
            if projected_reserved + self._consumed_budget > self._budget_limit:
                return self._reject_locked(operation, "budget_exhausted")

            cancelled = replace(old, state=IntentState.CANCELLED)
            replacement = IntentRecord(
                intent_id=new_intent_id,
                target=target,
                cost=int(cost),
                state=IntentState.PENDING,
                metadata=dict(metadata or {}),
                scheduled_start_time=scheduled_start_time,
                supersedes_intent_id=old_intent_id,
            )
            self._records[old_intent_id] = cancelled
            self._records[new_intent_id] = replacement
            self._reserved_budget = projected_reserved
            self._assert_invariants_locked()
            transitions = (
                self._transition(
                    cancelled,
                    IntentState.PENDING,
                    IntentState.CANCELLED,
                    "cancel_pending",
                    reason,
                ),
                self._transition(
                    replacement,
                    IntentState.PLANNED,
                    IntentState.PENDING,
                    "replacement_reserved",
                    reason,
                    supersedes_intent_id=old_intent_id,
                ),
            )
            return self._accept_locked(operation, transitions)

    def _finish(
        self,
        intent_id: str,
        terminal_state: IntentState,
        operation: str,
        reason: str,
    ) -> TransitionResult:
        with self._lock:
            record = self._records.get(intent_id)
            if record is None:
                return self._reject_locked(operation, "intent_not_found")
            if record.state is not IntentState.IN_FLIGHT:
                return self._reject_locked(operation, f"invalid_state:{record.state.value}")
            updated = replace(record, state=terminal_state)
            self._records[intent_id] = updated
            transition = self._transition(
                updated, IntentState.IN_FLIGHT, terminal_state, operation, reason
            )
            return self._accept_locked(operation, (transition,))

    def _snapshot_locked(self) -> BudgetSnapshot:
        return BudgetSnapshot(
            budget_limit=self._budget_limit,
            reserved_budget=self._reserved_budget,
            consumed_budget=self._consumed_budget,
            intent_count=len(self._records),
        )

    def _accept_locked(
        self, operation: str, transitions: tuple[IntentTransition, ...]
    ) -> TransitionResult:
        self._assert_invariants_locked()
        return TransitionResult(
            True,
            operation,
            transitions,
            self._snapshot_locked(),
            "",
            transitions[-1].transition_seq if transitions else self._next_seq_locked(),
        )

    def _reject_locked(self, operation: str, reason: str) -> TransitionResult:
        return TransitionResult(
            False,
            operation,
            tuple(),
            self._snapshot_locked(),
            reason,
            self._next_seq_locked(),
        )

    def _assert_invariants_locked(self) -> None:
        if self._reserved_budget < 0 or self._consumed_budget < 0:
            raise RuntimeError("intent budget counters became negative")
        if self._reserved_budget + self._consumed_budget > self._budget_limit:
            raise RuntimeError("intent budget capacity exceeded")
        reserved_from_records = sum(
            record.cost for record in self._records.values() if record.state is IntentState.PENDING
        )
        consumed_from_records = sum(
            record.cost
            for record in self._records.values()
            if record.state in {IntentState.IN_FLIGHT, IntentState.SUCCEEDED, IntentState.FAILED}
        )
        if reserved_from_records != self._reserved_budget:
            raise RuntimeError("reserved budget does not match pending intents")
        if consumed_from_records != self._consumed_budget:
            raise RuntimeError("consumed budget does not match submitted intents")

    def _transition(
        self,
        record: IntentRecord,
        state_before: IntentState | str,
        state_after: IntentState | str,
        operation: str,
        reason: str = "",
        actual_call_submitted: bool = False,
        supersedes_intent_id: str = "",
    ) -> IntentTransition:
        before = state_before.value if isinstance(state_before, IntentState) else str(state_before)
        after = state_after.value if isinstance(state_after, IntentState) else str(state_after)
        return IntentTransition(
            intent_id=record.intent_id,
            target=record.target,
            state_before=before,
            state_after=after,
            operation=operation,
            reason=reason,
            actual_call_submitted=actual_call_submitted,
            supersedes_intent_id=supersedes_intent_id or record.supersedes_intent_id,
            transition_seq=self._next_seq_locked(),
        )

    def _next_seq_locked(self) -> int:
        self._transition_seq += 1
        return self._transition_seq
