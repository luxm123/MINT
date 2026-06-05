# Experiment Design

MINT studies serverless DAG warmup as a two-phase decision problem.

Offline planning produces warmup intents for each DAG node. Each intent includes a planned time, validity window, criticality score, priority, and estimated offline gain.

Online scheduling observes runtime state and converts intents into actions:

- `execute`: invoke the warmup now.
- `delay`: keep the intent but move it later.
- `cancel`: discard the intent because it is not useful.
- `replace`: drop a lower-gain candidate when the budget is full.

The first implementation is heuristic. It estimates stage timing from DAG depth, call probability from branch structure, cold-start risk from simple platform defaults, and criticality from downstream count. This is the intended extension point for a future Markov Policy Planner that optimizes expected value over runtime states.

Measured metrics include end-to-end latency, latency percentiles, cold-start rate, total warmups, useful warmups, wasted warmups, missed warmups, useful warmup ratio, and scheduler action counts.
