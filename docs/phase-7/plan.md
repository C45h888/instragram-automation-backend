Phase 7 Runtime Validation Framework — Implementation Plan
==========================================================

0. Relationship to the contract
-------------------------------

This plan implements `docs/phase-7-tests.md` (the Phase 7 Runtime
Validation Contract). The contract states the WHAT: a runtime
truth phase that validates the system as a governed organism, not
as a collection of functions. This plan states the HOW: the
directory layout, the evolved runtime simulator, the canonical
Graph simulator, the per-kernel battery contract, the runtime-wide
integration tests, the cadence tiers, and the additive relationship
to Phases 1–6.

Phase 7 sits ABOVE Phases 1–6. It does not replace them. Phases
1–6 remain the constitutional regression layer — every architectural
lesson that was made executable during the kernelization effort
stays executable. Phase 7 is a new roof on the same building.

The primary unit of coverage is the KERNEL. Not a contract
section. Not a function. Not a single event. The kernel.

1. Architectural framing
-----------------------

The runtime under test is a kernelized architecture. Each kernel
is a bounded organism: FSM (lifecycle legitimacy) + substrate
(execution environment) + workers (bounded I/O) + orchestrator
where present (orchestration of the kernel's internal work).
The Constitutional Kernel (CK) sits above the domain kernels as
the authority plane. The Graph Capability Kernel is a
constitutional dependency for Acquisition and Publishing — together
they form the cognition layer of the system.

Phase 7 mirrors this topology:

  - One validation battery per kernel (9 batteries).
  - One cross-kernel cross-cut for the cognition layer
    (graph-capability, acquisition, publishing are bound; they
    must be validated as a system inside the system).
  - One runtime-wide integration tier on top of all kernel
    batteries.
  - One canonical external environment (the Graph Runtime
    Simulator) consumed by every kernel that talks to Graph.
  - One evolved execution platform (the runtime simulator with
    built-in observation surface) that produces a runtime timeline
    on every test failure.

2. Directory layout
-------------------

Phase 7 is fundamentally different from Phases 1–6. It is a
runtime validation framework, not a collection of test files. It
gets its own hierarchy under `tests/phase-7/`, separated from
the existing `tests/` flat layout to make the architectural
distinction visible at the filesystem level.

  tests/phase-7/
  ├── runtime/                         # framework infrastructure
  │   ├── runtime-simulator.js         # evolved: first-class testing
  │   │                                #   platform with built-in
  │   │                                #   recorders, inspectors, tracers
  │   ├── graph-simulator.js           # CANONICAL external environment
  │   │                                #   for Graph (HTTP + control API)
  │   ├── event-recorder.js            # causal timeline per test
  │   ├── state-inspector.js           # pre/post snapshot + diff
  │   ├── mutation-tracker.js          # lineage + projection +
  │   │                                #   governance + capability state
  │   ├── worker-tracer.js             # start/success/fail/duration/
  │   │                                #   retry/degradation per worker
  │   ├── governance-observer.js       # every CK/FSM decision recorded
  │   ├── cadence-accelerator.js       # 3-tier configurable cadence
  │   ├── fixtures/                    # payload inputs UNDER the
  │   │   │                            #   canonical simulator
  │   │   ├── success/
  │   │   ├── rate-limited/
  │   │   ├── malformed/
  │   │   ├── partial/
  │   │   ├── duplicate/
  │   │   └── stale/
  │   └── index.js                     # framework entry
  ├── kernels/                         # per-kernel validation battery
  │   ├── _kernel-battery-contract.js  # 7-category assertion surface
  │   │                                #   shared by all kernel files
  │   ├── _cognition-layer-cross-cuts.js
  │   │                                # 3 cross-kernel assertions
  │   │                                #   for graph-capability +
  │   │                                #   acquisition + publishing
  │   ├── acquisition.test.js
  │   ├── publishing.test.js
  │   ├── capability.test.js
  │   ├── reconciliation.test.js
  │   ├── retry.test.js
  │   ├── persistence.test.js
  │   ├── telemetry.test.js
  │   ├── scheduling.test.js
  │   └── dedup.test.js
  ├── integration/                     # runtime-wide integration
  │   ├── full-runtime-composition.test.js
  │   ├── multi-tick-survival.test.js        # short/medium/long tiers
  │   ├── cross-kernel-event-causality.test.js
  │   ├── governance-boundaries.test.js
  │   ├── capability-as-dependency.test.js
  │   └── observability-coverage.test.js
  ├── reports/                         # runtime timelines, state diffs,
  │                                     #   worker traces, governance
  │                                     #   decision logs (per run)
  └── phase-7-runner.sh                # orchestrator: spins up
                                        #   docker-compose, runs all
                                        #   kernel batteries, then
                                        #   integration, writes reports

The legacy-regression-corpus test (contract §13) is deferred.
The plan owner will harvest bugs from the Phase-N-Findings docs
and stand it up after the framework is operational.

3. Runtime simulator evolution
------------------------------

The current `tests/helpers/runtime-simulator.js` is the foundation
that Phases 1–6 already depend on. It is MUTATED IN PLACE. The
17-step boot sequence is unchanged. The existing public methods
(`boot`, `shutdown`, `runEngineComparison`, `triggerReconciliationCycle`,
`killTelemetryWorkers`, `restartTelemetryWorkers`,
`killTransitionWriters`, `restartTransitionWriters`, `restartRedis`)
are unchanged in signature and behavior. Phases 1–6 keep working
without modification.

The evolution is ADDITIVE. New internal recorders are wired at
boot. New public methods are appended.

  3.1 Built-in observation surface (every test gets this for free)

    - event timeline
        timestamp | type | source | destination | payload snapshot
        | linked state transition
    - state inspector
        pre-snapshot, post-snapshot, per-store diff
    - mutation tracker
        lineage rows, projection rows, governance state, capability
        state — diffed against pre-snapshot
    - worker tracer
        start, success, fail, duration, retry, degradation
        recorded per worker invocation
    - governance observer
        every CK.validate* call, every CK.dispatch outcome,
        every FSM transition decision

  3.2 New public methods (additive, do not break existing API)

    sim.injectEvent({ type, payload, source, correlationId })
        Inject an event into the runtime. Bypasses no authority;
        enters the observability plane and is governed normally.

    sim.tick(n)
        Run n accelerated cadence ticks. tick(1) = one fast tick.
        Respects cadence-accelerator tier setting.

    sim.snapshot()
        Return a full state snapshot: lineage, projections,
        governance state, capability state, worker state.

    sim.diff(before, after)
        Return a structured diff between two snapshots.

    sim.timeline()
        Return the causal event chain captured since boot.

    sim.workerTrace(workerName?)
        Return the worker execution record, filtered by name.

    sim.governanceLog()
        Return all governance decisions in order.

    sim.assertStateMatches(expectation)
        Assert post-snapshot matches the supplied expectation.
        Produces a precise diff on failure.

    sim.assertCausality(chain)
        Assert the supplied chain of event types occurred in
        the recorded order, with linked state transitions.

    sim.assertMutation({ store, before, after })
        Assert the supplied mutation occurred in the supplied
        store.

    sim.report()
        Generate a runtime report: timeline, state diff, worker
        trace, governance log. Auto-dumped on any test failure.

  3.3 Wiring

    The recorders hook into the existing 17-step boot at the
    natural seams:
      - event timeline: observability.onWrite / onEmit hooks
      - mutation tracker: lineage ledger read; projection read;
        governance state read (CK.getState); capability state
        read (graph-capability-kernel fsm.getState())
      - worker tracer: substrate worker entry/exit points
      - governance observer: CK.validate* + CK.dispatch wrap

    No existing boot step is changed. Hooks are appended.

4. Graph Runtime Simulator (canonical, one source of truth)
-----------------------------------------------------------

The Graph simulator is NOT a 1:1 Meta API copy. The goal is
runtime fidelity, not API fidelity. Workers must encounter
realistic payloads, realistic pagination, realistic failures,
realistic rate limits, realistic auth failures, realistic
partial responses, and realistic schema drift — through
runtime primitives that any kernel can drive.

  4.1 Architecture

    HTTP server running inside the test-runtime-net (added to
    docker-compose.test.yml as a `graph-simulator` service).
    Transport.js substrates in the worker plane call it the way
    they would call Meta — full HTTP path, full axios round-trip,
    full serialization. This gives runtime fidelity.

    A control API (HTTP, on a different port, not exposed to
    workers) is consumed by the runtime simulator and by tests
    directly. The control API deterministically injects:
      - rate-limit state
      - failure injection (retryable, permanent)
      - pagination chains
      - token state (valid, expired, revoked, struck)
      - schema drift (missing fields, added fields, type changes)
      - partial responses
      - malformed payloads
      - duplicate-detection scenarios
      - stale-data scenarios

    This is the hybrid model: HTTP for runtime fidelity, control
    API for determinism.

  4.2 Endpoints (runtime primitives, not 1:1 Meta)

    Acquisition surface:
      - conversations list (paginated)
      - messages by thread (paginated)
      - media list (paginated)
      - comments by media (paginated)
      - insights by media
      - account metadata

    Publishing surface:
      - publishing creation (initiates async flow)
      - publishing status (pollable)
      - publish completion (asynchronous, can be deferred /
        failed / completed by control API)

    Capability surface:
      - token validation
      - scope validation
      - account capability inspection
      - simulated auth degradation (token revoked, auth strike,
        repeated graph failure)

  4.3 Failure vocabulary (the priority set)

    The simulator must support, deterministically and observable
    to the test:
      - successful responses
      - retryable failures
      - permanent failures
      - pagination chains
      - rate-limit responses
      - token failures
      - malformed payloads
      - partial responses
      - schema drift
      - duplicates
      - stale data

    If a future test needs a new failure shape, it is added to
    the simulator vocabulary rather than invented per-kernel.
    Acquisition, Publishing, Capability, and future kernels all
    consume the same vocabulary. This is the rule that prevents
    the "every domain invents its own Meta" failure mode.

  4.4 Fixtures

    Individual fixture JSONs live at
    `tests/phase-7/runtime/fixtures/<scenario>/`. They are
    PAYLOAD INPUTS that the simulator loads. The simulator
    itself is canonical. The fixtures are data, not behavior.

5. Per-kernel battery contract
------------------------------

Every kernel battery, without exception, must internally cover
seven categories of validation. This is the surface that makes
Phase 7 runtime-true instead of unit-true.

  5.1 The seven categories

    1. State mutation correctness
         Pre/post DB + projection + governance state snapshot.
         Real rows, real lineage, real state.
    2. Event causality correctness
         Inject event → assert chain event-A → event-B → event-C
         with linked state transitions. Not "something emitted";
         the full causal chain.
    3. Governance correctness
         Token expiry / rate limit / capability degradation →
         FSM/CK decides, worker does NOT self-govern.
    4. Cadence correctness
         Accelerated ticks per the active tier (25-50 / 100-250
         / 500-1000) → assert no duplicate dispatches, no
         missing transitions, no unobserved retries.
    5. Worker correctness
         Each worker: realistic / partial / degraded / edge-case
         payload → full path event → substrate → mutation →
         observability.
    6. Persistence correctness
         Rows present, lineage linked, no orphan mutations, no
         illegal authority paths.
    7. Observability correctness
         Every worker emit (start / success / fail / duration /
         retry / degradation) recorded. Every governance decision
         recorded. Reconstructable from timeline.

  5.2 Shared helper

    `tests/phase-7/kernels/_kernel-battery-contract.js` exports
    `runKernelBattery({ kernel, simulator, fixtures, options })`.
    It defines the seven-category assertion surface and calls
    into kernel-specific assertion functions supplied by each
    battery file. This guarantees the categories are uniform
    across all nine batteries.

    A battery file looks like:

      import { runKernelBattery } from './_kernel-battery-contract.js';
      runKernelBattery({
        kernel: 'acquisition',
        simulator,
        fixtures,
        assertions: {
          stateMutation:      async (s) => { ... },
          eventCausality:     async (s) => { ... },
          governance:         async (s) => { ... },
          cadence:            async (s) => { ... },
          workerCorrectness:  async (s) => { ... },
          persistence:        async (s) => { ... },
          observability:      async (s) => { ... },
        },
      });

    The shared helper is responsible for wiring the simulator's
    observation surface into each assertion, producing uniform
    failure reports, and ensuring no category is silently skipped.

6. Cognition layer cross-cuts
-----------------------------

The graph-capability, acquisition, and publishing kernels are
BOUND. They form the cognition layer of the system. They cannot
be validated in isolation as if they were independent. The
contract for these three extends the per-kernel contract with
three cross-kernel assertions.

  6.1 The three cross-cuts

    1. capability state change → other 2 consumers react
       Inject a capability state transition (token refresh, auth
       strike, account revoke) into the graph-capability kernel.
       Assert acquisition and publishing react EXACTLY as
       designed — defer, block, throttle, or continue.
    2. other 2 consumers do NOT inspect token internals
       Static + runtime assertion: acquisition and publishing
       workers (and their substrates) never read token
       internals, never call the Graph auth endpoints directly,
       never access the vault substrate. They consume capability
       state produced by graph-capability only.
    3. capability state is the sole authority signal
       Dynamic assertion: when capability state is degraded,
       acquisition/publishing block at the governance plane, not
       at the worker. The CK and the capability kernel are the
       sole authority sources for those two consumers.

  6.2 Implementation

    `tests/phase-7/kernels/_cognition-layer-cross-cuts.js`
    exports the three cross-cuts as composable assertions. The
    three batteries (acquisition, publishing, capability) import
    and run them as part of their battery, in addition to the
    seven-category internal coverage.

    The integration tier re-runs the three cross-cuts at scale
    in `capability-as-dependency.test.js` to catch drift that
    only appears under cross-kernel load.

7. Per-kernel coverage map
-------------------------

Drawn from the actual filesystem state (kernels on disk with
their worker/substrate/orchestrator surface).

  7.1 Acquisition Kernel

    Workers under test (5): comments, content, insights, messages, ugc
    Substrates: content/, engagement/, insights/, ugc/
    Orchestrator: yes (acquisition-kernel/orchestrator.js)
    Retry-worker: yes (acquisition-kernel/retry-worker.js)
    Battery focus:
      - every fetch-completion event through the parser/
        normalizer/hydrator chain → conversation row + lineage +
        no orphan repair event
      - all 5 worker types through realistic / partial /
        degraded / edge-case payloads
      - engagement fetch → conversation linkage
      - capability-degraded account → worker does NOT inspect
        token; consumes capability state only
      - dual-persist paths: Path A (retry-worker → parsing → CK →
        writers) and Path B (engagement persist()) — both
        validated; the documented dead Path B is asserted dead
        or surfaced as a bug per the kernelization memory

  7.2 Publishing Kernel

    Workers (2): content, engagement
    Substrates: content/, engagement/
    Orchestrator: yes
    Rate-limiters: per substrate
    Battery focus:
      - publishing event → governance decision → worker
        dispatch → transport → persistence
      - rate-limited Graph → retry-cadence consumes correctly
      - capability degraded → publishing blocks at CK (not at
        worker)
      - publishing status / completion async flow through the
        control API

  7.3 Capability Kernel (graph-capability)

    Substrates: graph-capability/ (observations, trigger-bridge,
      verdict-gate, wiring)
    Vault: api-surface, default-scopes, signal-dispatch
    PAT substrate workers: exchange, retrieve, store
    Scope substrate workers: detect-dynamic
    UAT substrate workers: detect, refresh, retrieve, store
    Battery focus:
      - new connection → expected capability state
      - token refresh → expected capability state
      - auth strike → expected capability state
      - repeated graph failure → expected capability state
      - downstream consumers (acquisition, publishing) react
        exactly as designed
      - the kernel itself never inspects token internals from
        outside (boundary check)

  7.4 Reconciliation Kernel

    Engine + worker + substrate + orphan-message-repair
    Battery focus:
      - drift detection → engine comparison → orphan repair →
        governance acceptance → ledger entry
      - multi-tick reconcile → no duplicate repairs, no missing
        lineage
      - reconciliation cycle through the CK bridge

  7.5 Retry Kernel (retry-cadence)

    Workers (4): content, engagement, insights, ugc
    Policy + registry
    Battery focus:
      - rate-limit window → retry dispatched with correct
        cadence semantics
      - capability-aware prioritization
      - recovery on Graph heal
      - worker does NOT self-authorize

  7.6 Persistence Kernel (postgres-telemetry)

    Readers (3): accounts, media, post-queue
    Writers (7): comments, content, conversations, message-fix,
      messages, publishing, ugc
    Cognition-scanner
    Battery focus:
      - raw event → cognition scan → writer dispatch → row
        materialization → lineage reference
      - no missing identifier (legacy kernelization bug class)
      - dual-persist paths validated per the kernelization note
        (Path A live, Path B asserted dead or surfaced as bug)

  7.7 Telemetry Kernel

    FSM + 5 projection workers (authority, health, integrity,
      runtime, systemic-pressure) + 5 transition writers +
      5 projection inputs + 5 projection syntheses
    Ingress-lag-worker
    Battery focus:
      - projection intent → coordination FSM → ordered
        transition → ledger entry
      - namespace priority + lexical order determinism
        (re-asserted from Phase 6, not replaced)
      - halt/resume at the kernel level
      - restart replay convergence

  7.8 Scheduling Kernel

    Cadence + lifecycle + operational-safety
    Battery focus:
      - lifecycle refresh → LIFECYCLE_REFRESHED → CK state
        transition
      - cadence tick → CADENCE_TICK → downstream reactions
      - operational-safety guard under degraded state

  7.9 Dedup Kernel

    Substrate: conversation-repair (worker + substrate)
    Battery focus:
      - duplicate detection → repair worker dispatched → no
        double-write, no missing repair event, lineage intact

8. Runtime-wide integration tests
---------------------------------

These run AFTER all nine kernel batteries pass. They validate
that the kernels behave correctly when composed.

  8.1 full-runtime-composition.test.js
    Boot all 9 kernels together. Inject cross-kernel events.
    Assert end-to-end causality. Assert no authority leak.

  8.2 multi-tick-survival.test.js
    Accelerated cadence per the active tier. Default tier
    selection is configurable per run.
      - short:  25–50 ticks (every commit)
      - medium: 100–250 ticks (Phase 7 + CI)
      - long:   500–1000 ticks (manual / pre-release)
    Watch (the five long-run failure modes):
      - state drift
      - duplicate dispatch
      - retry accumulation
      - lineage corruption
      - governance leakage
    The test passes only when the state invariants hold across
    the full tick count.

  8.3 cross-kernel-event-causality.test.js
    capability degrade → acquisition worker blocks → publishing
    worker blocks → reconciliation detects capability drift →
    retry-cadence pauses → telemetry surfaces authority health.
    Assert the chain. This is the cognition layer cross-cut at
    runtime scale.

  8.4 governance-boundaries.test.js
    Workers are observed via worker-tracer. Assert NONE of them
    call validate*, NONE inspect token state, NONE mutate
    governance state. FSM/CK are the sole authority.

  8.5 capability-as-dependency.test.js
    Acquisition and publishing consume graph-capability state
    only. Token internals never reach them. Re-runs the cog-layer
    cross-cuts at scale.

  8.6 observability-coverage.test.js
    Every emit, every decision, every deviation must be
    reconstructable from the timeline. Assert timeline
    completeness for a representative set of operational flows.

9. Cadence tier semantics
-------------------------

Cadence is accelerated, not real-time. The runtime only cares
about causal progression, not wall-clock duration. A 50ms tick
and a 10s tick must produce the same state evolution if the
cadence loop is correct. This is what makes Phase 7 cadence
testing practical to run frequently.

Tier configuration lives in
`tests/phase-7/runtime/cadence-accelerator.js` and is selected
per-run by `phase-7-runner.sh`:

  - `--tier=short`   → 25–50 ticks, every commit gate
  - `--tier=medium`  → 100–250 ticks, Phase 7 + CI validation
  - `--tier=long`    → 500–1000 ticks, manual / pre-release

Default tier for `phase-7-runner.sh` (no flag) is `medium`.
The long tier is opt-in because it is the one that will deepen
in value as worker intelligence deepens.

10. Phase 7 runner
------------------

`tests/phase-7/phase-7-runner.sh` is the orchestrator.

  - Brings up `docker-compose.test.yml` (the existing
    test-runtime-net: redis, postgres, test-runner, plus the
    new `graph-simulator` service).
  - Flushes Redis, applies init-scripts.
  - Runs the nine kernel batteries in order.
  - Runs the six integration tests.
  - Writes per-run reports to `tests/phase-7/reports/<run-id>/`
    containing:
      - full runtime timeline
      - state diff (lineage + projection + governance + capability)
      - worker trace
      - governance decision log
      - cadence metrics (for the active tier)
  - On any failure: dumps the report to stdout with the
    assertion location, the causal chain that led to the
    failure, and the state diff at the failure point.

Phases 1–6 keep their runner (`tests/run-all-tests.sh`)
unchanged. `phase-7-runner.sh` runs AFTER the existing suite,
additively. Both runners share the same docker-compose.

11. Contract adherence map
--------------------------

Every clause in `docs/phase-7-tests.md` is satisfied by a
specific element of this plan:

  §4 synthetic runtime .......... runtime-simulator (evolved)
                                     + graph-simulator
  §6 event-driven tests ......... sim.injectEvent() pattern in
                                     every kernel battery
  §7 event recorder as truth .... event-recorder + sim.timeline()
  §8 state mutation as target .. state-inspector + sim.diff()
  §9 governance separately ..... governance-observer +
                                     governance-boundaries.test.js
                                     + per-kernel category 3
  §10 cadence as survival ...... cadence-accelerator (3 tiers) +
                                     multi-tick-survival.test.js +
                                     per-kernel category 4
  §11 worker as bounded intel .. worker-tracer + per-kernel
                                     category 5
  §12 capability as dependency . capability-as-dependency.test.js
                                     + _cognition-layer-cross-cuts
                                     (3 cross-cuts per cog-layer
                                     battery)
  §13 legacy regressions ....... DEFERRED — plan owner will
                                     stand up
                                     legacy-regression-corpus.test.js
                                     after framework operational
  §14 simulated Graph .......... graph-simulator.js (canonical,
                                     runtime primitives, hybrid
                                     HTTP + control API)
  §15 observability first-class  observability-coverage.test.js
                                     + worker-tracer +
                                     per-kernel category 7

12. Deferred work
-----------------

  - Legacy regression corpus (contract §13). Deferred per plan
    owner direction. The plan owner will harvest the relevant
    bugs from the Phase-N-Findings docs and
    `decomposition-issues.md` after the framework is operational
    and will stand up `legacy-regression-corpus.test.js` in the
    integration tier.

  - Future kernel additions. When new kernels are added to the
    architecture, they receive a battery file following the
    same contract. The framework is designed to extend.

13. Success condition
--------------------

Phase 7 is complete when, on the active cadence tier:

  - All nine kernel batteries pass their seven-category
    internal contract.
  - The three cognition-layer cross-cuts pass for
    graph-capability, acquisition, and publishing.
  - All six runtime-wide integration tests pass.
  - No governance leakage, no state drift, no duplicate
    dispatch, no retry accumulation, no lineage corruption is
    observed across the full tick count.
  - The runtime produces a clean report with a complete
    causal timeline, state diff, worker trace, and governance
    decision log.

At that point the architecture is validated at the runtime
level. Contract testing against Meta and live operational
validation become the next stages, building on a Phase 7
runtime that has already proven it can think and act coherently
as a kernelized organism.
