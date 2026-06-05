# Phase 7 Runtime Validation — Quickstart

Phase 7 is the runtime validation layer of the kernelized
architecture. It validates the system as an operating
organism, not as a collection of functions. The primary
unit of coverage is the **kernel**.

For the full plan, see `docs/phase-7/plan.md`. For the
contract, see `docs/phase-7-tests.md`.

## Directory layout

```
tests/phase-7/
├── runtime/                    # framework infrastructure
│   ├── runtime-simulator.js    # evolved: first-class testing
│   │                            #   platform with built-in
│   │                            #   recorders, inspectors,
│   │                            #   tracers, governance observer
│   ├── graph-simulator.js      # CANONICAL external Graph
│   │                            #   environment (HTTP + control
│   │                            #   API; runtime primitives, not
│   │                            #   1:1 Meta copies)
│   ├── event-recorder.js
│   ├── state-inspector.js
│   ├── mutation-tracker.js
│   ├── worker-tracer.js
│   ├── governance-observer.js
│   ├── cadence-accelerator.js
│   ├── fixtures/               # payload inputs under the
│   │                            #   canonical simulator
│   └── index.js                # public surface
├── kernels/                    # per-kernel validation battery
│   ├── _kernel-battery-contract.js    # 7-category surface
│   ├── _cognition-layer-cross-cuts.js # 3 cross-cuts for
│   │                                    #   capability +
│   │                                    #   acquisition +
│   │                                    #   publishing
│   ├── acquisition.test.js
│   ├── publishing.test.js
│   ├── capability.test.js
│   ├── reconciliation.test.js
│   ├── retry.test.js
│   ├── persistence.test.js
│   ├── telemetry.test.js
│   ├── scheduling.test.js
│   └── dedup.test.js
├── integration/                # runtime-wide integration
│   ├── full-runtime-composition.test.js
│   ├── multi-tick-survival.test.js
│   ├── cross-kernel-event-causality.test.js
│   ├── governance-boundaries.test.js
│   ├── capability-as-dependency.test.js
│   └── observability-coverage.test.js
├── reports/                    # runtime timelines, state diffs,
│                                #   worker traces, governance
│                                #   decision logs (per run)
├── phase-7-runner.sh           # orchestrator
└── QUICKSTART.md               # this file
```

## Running it

The runner is `tests/phase-7/phase-7-runner.sh`. It follows
the same docker-compose pattern as the existing
`tests/run-all-tests.sh` (Phases 1–6). Phase 7 sits ABOVE
Phases 1–6 as an additive validation layer — it does not
replace them.

```bash
# Run all 9 kernel batteries + 6 integration tests (medium tier)
./tests/phase-7/phase-7-runner.sh

# Short tier — every commit gate
./tests/phase-7/phase-7-runner.sh --tier=short

# Long tier — manual / pre-release
./tests/phase-7/phase-7-runner.sh --tier=long

# Single kernel battery
./tests/phase-7/phase-7-runner.sh --kernel=acquisition
./tests/phase-7/phase-7-runner.sh --kernel=capability
./tests/phase-7/phase-7-runner.sh --kernel=reconciliation
# ... any of: acquisition, publishing, capability,
#             reconciliation, retry, persistence,
#             telemetry, scheduling, dedup

# Kernel batteries only (no integration)
./tests/phase-7/phase-7-runner.sh --kernels-only

# Integration only (after kernel batteries pass elsewhere)
./tests/phase-7/phase-7-runner.sh --integration-only

# Keep Docker stack running after tests
./tests/phase-7/phase-7-runner.sh --keep-up
```

## Cadence tiers

The multi-tick-survival test reads `PHASE7_CADENCE_TIER`:

| Tier    | Tick count | When to run                        |
|---------|------------|------------------------------------|
| short   | 25–50      | Every commit                       |
| medium  | 100–250    | Phase 7 + CI (default)             |
| long    | 500–1000   | Manual / pre-release               |

The runner passes `PHASE7_CADENCE_TIER` into the test-runner
container automatically when you use `--tier=`.

The runtime only cares about causal progression, not
wall-clock duration. A 50ms tick and a 10s tick must produce
the same state evolution. This is what makes Phase 7 cadence
testing practical to run frequently.

## Reading reports

Every test that fails auto-dumps a runtime report to
`tests/phase-7/reports/<run-id>/report.json`. The report
contains:

- `runId`, `timestamp`, `booted`, `bootDurationMs`
- `timeline` — full causal event chain since boot
- `workerTrace` — every worker invocation (start, success,
  fail, duration, retry, degrade)
- `governanceLog` — every CK/FSM decision
- `mutations` — every state mutation
- `error` — the failing assertion, its label, and any
  violations

You can also call `simulator.report()` manually from any
test to capture a snapshot of the current state.

## Architecture notes

- **Runtime simulator** (`tests/phase-7/runtime/runtime-simulator.js`)
  extends the existing
  `tests/helpers/runtime-simulator.js` additively. The
  17-step boot, existing methods, and Phases 1–6 contract
  are unchanged. The Phase 7 version adds a built-in
  observation surface (event recorder, state inspector,
  mutation tracker, worker tracer, governance observer) on
  top.

- **Graph Runtime Simulator**
  (`tests/phase-7/runtime/graph-simulator.js`) is the
  CANONICAL external environment. It is NOT a 1:1 Meta API
  copy — the goal is **runtime fidelity**, not API
  fidelity. Workers encounter realistic payloads,
  pagination, failures, rate limits, auth failures, partial
  responses, and schema drift through runtime primitives
  any kernel can drive. A control API on a separate port
  lets the runtime simulator and tests deterministically
  inject failure scenarios.

  The simulator runs as a Node HTTP service inside
  `test-runtime-net`. The test-runner container reaches it
  via `graph-simulator:9100` (worker port) and
  `graph-simulator:9101` (control port). Configuration is
  via the `Phase7RuntimeSimulator({ graphSimulatorHost: ...,
  graphSimulatorPort: 9100 })` constructor option.

- **Cognition layer** — the `graph-capability`, `acquisition`,
  and `publishing` kernels are bound. They form the
  cognition layer of the system. Their batteries import
  `runCognitionLayerCrossCuts` from
  `tests/phase-7/kernels/_cognition-layer-cross-cuts.js`
  and run three cross-cuts in addition to the 7-category
  internal coverage:
  1. capability state change → other 2 consumers react
  2. other 2 consumers do NOT inspect token internals
  3. capability state is the sole authority signal

  The `capability-as-dependency` integration test re-runs
  the cross-cuts at scale.

- **Existing phases remain intact.** Phases 1–6 are
  permanent constitutional regression. Phase 7 is the
  runtime validation layer above them.

## Adding a new kernel

When a new kernel is added to the architecture:

1. Create `tests/phase-7/kernels/<kernel>.test.js`.
2. Follow the template in any existing battery file
   (`acquisition.test.js` is a good reference).
3. Implement the 7 categories via `runKernelBattery({...})`.
4. If the kernel is part of the cognition layer, also call
   `runCognitionLayerCrossCuts({...})` and update
   `_cognition-layer-cross-cuts.js` if new forbidden
   patterns emerge.
5. Add the kernel to the `KERNELS` array in
   `phase-7-runner.sh`.
