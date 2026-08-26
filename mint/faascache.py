"""FaasCache-style Greedy-Dual-Size-Frequency keep-alive cache (ASPLOS'21).

FaasCache (Fuerst & Sharma, "FaasCache: Keeping Serverless Computing Alive
With Greedy-Dual Caching", ASPLOS'21) keeps warm containers resident under a
capacity budget using Greedy-Dual-Size-Frequency (GDSF).  The GDSF value of a
function is

    value(x) = cost(x) * freq(x) / size(x) + L

where cost is the cold-start latency saved by keeping the container alive,
freq is the invocation frequency (a count, optionally recency-decayed), size
is the container memory footprint, and L is the GDS aging factor equal to the
value of the last evicted item.  The aging factor keeps recently-evicted
items from immediately re-entering the cache and is what distinguishes GDSF
from plain frequency caching.

Scheduling-layer reproduction note
----------------------------------
In the MINT harness, the budget B counts *proactive warmup invocations per
run*, exactly like every other baseline, rather than platform container
capacity.  The GDSF cache is therefore modeled as the set of functions that
currently hold a proactive keep-warm slot (`contents`, size <= B): warmup
actions fill free slots and replace the lowest-value resident when a
higher-value candidate appears, and real invocations update frequency and
populate the cache exactly like accesses in the original system.  Evicting a
function stops refreshing it; any warmth it still has from a recent real
invocation is left untouched (the harness models retention expiry
separately).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class FaasCacheProfile:
    """GDSF parameters for the scheduling-layer reproduction.

    frequency_decay: exponential recency decay applied on each observed
        invocation (0.0 = plain cumulative frequency, matching the classic
        GDSF count semantics).
    size_mb: per-function container memory footprint; defaults to 1.0 for
        every function when absent (uniform size).
    cold_start_ms: per-function cold-start latency saved; defaults to
        base_cold_ms when absent.
    base_cold_ms: platform default cold-start latency.
    """

    frequency_decay: float = 0.0
    size_mb: dict[str, float] | None = None
    cold_start_ms: dict[str, float] | None = None
    base_cold_ms: float = 800.0

    def cost(self, node: str) -> float:
        if self.cold_start_ms:
            value = self.cold_start_ms.get(node)
            if value is not None:
                return max(0.0, float(value))
        return max(0.0, float(self.base_cold_ms))

    def size(self, node: str) -> float:
        if self.size_mb:
            value = self.size_mb.get(node)
            if value is not None:
                return max(1.0, float(value))
        return 1.0


class GdsfCache:
    """A GDSF keep-alive cache over the per-run warmup slots.

    `contents` holds the functions currently allocated a proactive
    keep-warm slot (at most `capacity`).  `aging` is the GDS factor L.
    `frequency` holds the (optionally decayed) invocation counts that drive
    the value ranking.
    """

    def __init__(
        self,
        capacity: int,
        profile: FaasCacheProfile | None = None,
    ) -> None:
        self.capacity = max(0, int(capacity))
        self.profile = profile or FaasCacheProfile()
        self.frequency: dict[str, float] = {}
        self.contents: set[str] = set()
        self.aging = 0.0

    def seed_frequencies(self, priors: dict[str, float]) -> None:
        """Seed frequency from historical call probability (trace prior)."""
        for node, prior in priors.items():
            self.frequency[node] = max(0.0, float(prior))

    def value(self, node: str) -> float:
        cost = self.profile.cost(node)
        size = self.profile.size(node)
        return round(cost * self.frequency.get(node, 0.0) / size + self.aging, 6)

    def observe(self, node: str) -> str | None:
        """Record a real invocation: update frequency and populate the cache
        like a GDSF access.  Returns the evicted node, if any."""
        decay = self.profile.frequency_decay
        previous = self.frequency.get(node, 0.0)
        self.frequency[node] = 1.0 + (decay * previous if decay > 0.0 else previous)
        return self.insert(node)

    def insert(self, node: str) -> str | None:
        """Record a successfully warmed (or accessed) node as resident.

        When at capacity, evicts the lowest-value resident and advances the
        aging factor L to the evicted value.  Returns the evicted node, if
        any; eviction never touches the node's residual hotness, which is the
        harness's retention model, not the cache's decision.
        """
        if self.capacity <= 0 or node in self.contents:
            return None
        evicted: str | None = None
        if len(self.contents) >= self.capacity:
            evicted = self._evict_lowest()
            if evicted == node:
                return None
        self.contents.add(node)
        return evicted

    def _evict_lowest(self) -> str:
        evicted = min(self.contents, key=lambda node: (self.value(node), node))
        self.aging = self.value(evicted)
        self.contents.discard(evicted)
        return evicted

    def allocate(self, candidates: Iterable[str], budget: int) -> list[str]:
        """Pick warmup targets for one run.

        `candidates` are the non-hot functions, pre-ranked by the caller.
        Expired residents are refreshed; free slots are filled with the
        highest-value candidates; when the cache is full a candidate is only
        targeted if it beats the lowest-value resident (GDSF replacement,
        which `insert` realizes on warmup success).  Returns at most `budget`
        warmup targets and does not mutate cache state.
        """
        if self.capacity <= 0 or budget <= 0:
            return []
        budget = min(int(budget), self.capacity)
        targets: list[str] = []
        for node in candidates:
            if len(targets) >= budget:
                break
            if node in self.contents:
                # Resident whose warm retention expired; refresh it.
                targets.append(node)
                continue
            if len(self.contents) >= self.capacity:
                evicted = min(self.contents, key=lambda item: (self.value(item), item))
                if self.value(node) <= self.value(evicted):
                    continue
            targets.append(node)
        return targets
