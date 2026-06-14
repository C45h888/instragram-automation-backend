# PHASE 9 — RUNTIME VERIFICATION & CONSTITUTIONAL VALIDATION CONVERSION

## 0. Decisions locked with user

| # | Decision | Value |
|---|----------|-------|
| 1 | Harness boundary | REAL runtime — boot the actual control-plane, kernels, FSMs, signal-bus, governance, mutation-substrate. |
| 2 | Recorder | Passive observer. Recorder shape is a derived view computed from the lineage ledger — recorder cannot fabricate events. |
| 3 | Scope | FULL CONTRACT — harness + 6 webhook + 5 graph-worker + cross-kernel + drift + cadence (25/100/500/1000) in one pass. |
| 4 | Test DB | docker-compose.test.yml postgres with per-test-file TRUNCATE in beforeAll. |
| 5 | Snapshot output | `tests/phase-9/reports/<run>/lineage-observation.json` + `lineage-snapshot.json` — two artifacts, runtime drives both. |
| 6 | Acquisition priority | WEBHOOK-DRIVEN. Webhook is Tier 1 critical validation. Graph is Tier 2 support validation for Publishing/Insights/Capability/Recovery/Reconciliation. The runtime is webhook-first, not fetch-first. |
| 7 | Graph emulator scope | Worker validation tool, not a Meta emulator. Goal: "can the worker survive?" — not perfect Meta parity. |
| 8 | Fixture classes | Two classes: `canonical/` (perfect payloads) and `captured/` (real Meta payloads, populated from VPS). Captured payloads are the highest-value fixtures in the suite. |
| 9 | New artifact | `ownership-trace.json` — per-event_id, the owner of every link in the chain (governance, fsm, worker, mutation). Verifies the authority boundaries we spent months building. |
| 10 | New test | `kernel-sovereignty.test.mjs` — per kernel: "can this kernel function if every other kernel disappears?" Catches architectural coupling. |
| 11 | New test | `lineage-replay.test.mjs` — replay the observation log, reconstruct state, compare against actual state. Diverge = causality leak. |
| 12 | New invariant | Long-running integrity check: every N ticks, no increase in authority violations, drift findings, or orphan lineage events. Catches slow degradation. |

---

## 1. Problem statement (from phase 8 audit)

Phase 8 tests validate that the **recorder** can produce a constitutional chain, not that the **runtime** does. Example from `webhook-to-state.test.mjs`:

```
p8.recorder.ingress(eid, res.body);
p8.recorder.governance(eid, { actor: 'CK_DECISION', fixture: name });
p8.recorder.fsm(eid, { fsm: 'acquisition-fsm', from: 'IDLE', to: 'PROCESSING' });
p8.recorder.worker(eid, `worker-${name}`, { action: 'execute' });
p8.recorder.mutation(eid, { kernel: 'acquisition', kind: 'insert' });
```

The runtime never runs. Governance, FSM, worker, mutation are **asserted into existence** by the test. A runtime that never executes would pass phase 8. Phase 9 closes this gap.

The phase-8 contract is clear: "Recorder must never emit events. Recorder only observes." We implement that literally.

---

## 2. Architectural target

### 2.1 Acquisition model (the structural change)

The runtime is now **webhook-driven**, not fetch-driven. Meta acquisition is webhook-first: the simulator is no longer the primary validation surface — it is the transport for the **primary** validation surface.

```
TIER 1 (critical)               TIER 2 (support)
================                ==================
Webhook Ingress                 Graph Workers
  → message-created               → Publishing
  → comment-created               → Insights
  → mention-created               → Capability
  → story-reply                   → Recovery
  → conversation-update           → Reconciliation
  → media-update
```

Tier 1 is what produces constitutional state. Tier 2 consumes it. The test effort, the report emphasis, and the failure-mode priority follow the same ratio. If a Tier 1 test fails, the suite is red. If a Tier 2 test fails, the suite is amber until it's understood.

The runtime entry path is:

```
Webhook Simulator (transport only)
  └─ HTTP POST → Runtime Ingress
       └─ Parser
            └─ Governance (constitutional-kernel)
                 └─ FSM (acquisition-fsm, publishing-fsm, …)
                      └─ Worker Dispatch
                           └─ Worker Execution (real workers, real DB)
                                └─ Mutation (mutation-substrate, real DB)
                                     └─ EventBus emission (signal-bus)
                                          └─ RecorderObserver (passive subscriber)
                                               └─ lineage-observation.json
                                                    └─ lineage-snapshot.json (replay)
                                                         └─ Test assertion
```

Recorder is a subscriber, not an actor. The test asserts on what it sees, not what it told the recorder.

### 2.2 Current shape (phase 8, recorder-driven — what we are replacing)

---

## 3. File layout

```
tests/phase-9/
├── MANIFEST.mjs                              # phase-9 public surface
├── phase-9-runner.sh                         # docker compose + test invocation
├── QUICKSTART.md                             # how to run
├── vitest.config.js                          # inherits phase-8 config
│
├── runtime/
│   ├── runtime-harness.js                    # boots real control-plane + kernels + DB
│   ├── recorder-observer.mjs                 # EventBus subscriber; emits lineage-observation.json
│   ├── ownership-tracer.mjs                  # reads lineage ledger → ownership-trace.json
│   ├── snapshot-deriver.mjs                  # reads lineage ledger → lineage-snapshot.json (recorder shape)
│   ├── replay-engine.mjs                     # replay observation log → reconstructed state (for causality check)
│   ├── webhook-simulator.js                  # phase-8 simulator, transport-only, with retry/delay/dup/sig
│   ├── db-reset.js                           # TRUNCATE lineage + mutation tables per test file
│   ├── ingress-bridge.js                     # injects delivered payload into real runtime ingress
│   ├── graph-emulator.js                     # worker validation tool, not Meta emulator
│   ├── drift-detector.mjs                    # authority/semantic/ownership/contamination/leakage scans
│   ├── event-bus-spy.mjs                     # observability helper (event count, ordering)
│   ├── constitutional-path-assert.mjs        # asserts on derived snapshot, not on recorder
│   ├── kernel-isolation-harness.mjs          # boots single kernel, no others (for sovereignty)
│   └── report-writer.mjs                     # phase-9 report writer (observation-only)
│
├── fixtures/
│   └── webhooks/
│       ├── canonical/                        # perfect payloads, hand-crafted
│       │   ├── message-created.json
│       │   ├── comment-created.json
│       │   ├── mention-created.json
│       │   ├── story-reply.json
│       │   ├── conversation-update.json
│       │   └── media-update.json
│       └── captured/                         # real Meta payloads, populated from VPS testing
│           ├── README.md                     # how to add a captured payload
│           └── .gitkeep                      # directory exists; payloads land here in phase 10
│
├── webhook/                                  # TIER 1 — critical
│   ├── runtime-webhook-ingress.test.mjs      # 6 canonical fixtures, one test each
│   ├── runtime-webhook-captured.test.mjs     # captured/ fixtures, when present
│   ├── runtime-webhook-retry.test.mjs        # 429 → runtime re-queues
│   ├── runtime-webhook-duplicate.test.mjs    # dup delivery dedup-kernel absorbs
│   ├── runtime-webhook-schema-drift.test.mjs # unknown shape → constitutional reject
│   └── runtime-webhook-signature.test.mjs    # bad sig → ingress rejects before governance
│
├── graph/                                    # TIER 2 — support
│   ├── runtime-graph-insights.test.mjs
│   ├── runtime-graph-publishing.test.mjs
│   ├── runtime-graph-capability.test.mjs
│   ├── runtime-graph-recovery.test.mjs
│   └── runtime-graph-reconciliation.test.mjs
│
├── cross-kernel/
│   ├── _pair-helper.mjs                      # pairs use the runtime, not the recorder
│   ├── capability-to-acquisition.test.mjs    # 5×4 = 20 files (mirror phase-8 layout)
│   ├── capability-to-publishing.test.mjs
│   ├── … (20 files total)
│
├── ownership/
│   ├── ownership-trace-message.test.mjs      # message event → owner of every link
│   ├── ownership-trace-comment.test.mjs
│   ├── ownership-trace-mention.test.mjs
│   ├── ownership-trace-story-reply.test.mjs
│   ├── ownership-trace-conversation.test.mjs
│   ├── ownership-trace-media.test.mjs
│   └── ownership-assert.mjs                  # shared assertion: chain has correct owners
│
├── sovereignty/
│   ├── kernel-sovereignty-acquisition.test.mjs
│   ├── kernel-sovereignty-publishing.test.mjs
│   ├── kernel-sovereignty-capability.test.mjs
│   ├── kernel-sovereignty-recovery.test.mjs
│   ├── kernel-sovereignty-reconciliation.test.mjs
│   └── kernel-sovereignty-acquisition-coupling.test.mjs  # negative: confirm coupling, fail
│
├── replay/
│   └── lineage-replay.test.mjs               # replay observation log → reconstructed state → diff
│
├── drift/
│   ├── authority-drift.test.mjs              # no kernel writes outside its domain
│   ├── semantic-drift.test.mjs               # public_signal vs internal sentinel
│   ├── ownership-drift.test.mjs              # no foreign-table mutations
│   ├── cross-kernel-contamination.test.mjs   # sink never sees source's internals
│   ├── governance-leakage.test.mjs           # workers never call escalate()
│   ├── worker-autonomy.test.mjs              # workers never import scheduler/governance/fsm
│   └── fsm-ownership.test.mjs                # fsm transitions emit lineage, no foreign transitions
│
├── integration/
│   ├── phase-9-runtime-composition.test.mjs  # full chain across all domains
│   ├── phase-9-multi-tick-survival.test.mjs  # 25 / 100 / 500 / 1000 ticks
│   ├── phase-9-long-running-integrity.test.mjs  # invariant: violations do not increase over time
│   └── phase-9-scenario-message-thread.test.mjs  # end-to-end scenario
│
└── reports/                                  # per-run JSON output
    ├── <run-id>/
    │   ├── lineage-observation.json
    │   ├── lineage-snapshot.json
    │   ├── ownership-trace.json
    │   ├── constitutional-paths.json
    │   ├── drift-findings.json
    │   ├── replay-delta.json                 # reconstructed vs actual
    │   └── cadence-stats.json
```

---

## 4. Component design

### 4.1 runtime-harness.js (the heart of phase 9)

Owns: `boot()`, `shutdown()`, `getBus()`, `getRecorder()`, `getClock()`, `tick()`.

`boot()` sequence:
1. `dbReset()` — TRUNCATE lineage + mutation + dedup + scheduling tables.
2. `loadSubstrates()` — `mutation-substrate.js`, `postgres-telemetry-kernel`, `quota.js`, `rate-limiter`, `transport`.
3. `loadControlPlane()` — `signal-bus.js`, `constitutional-kernel.js`, `lineage-ledger.js`, governance domains, interpreters.
4. `loadOrchestration()` — `lifecycle-orchestrator.js`, `signal-orchestrator.js`, `degradation-orchestrator.js`.
5. `loadKernels()` — `acquisition-kernel`, `graph-capability-kernel`, `publishing-kernel`, `reconciliation-kernel`, `retry-cadence-kernel`, `dedup-kernel`, `scheduling-kernel`, `telemetry-kernel`.
6. `registerWorkers()` — for each kernel, register its workers with the orchestrator.
7. `startObservers()` — subscribe `recorder-observer.mjs` to the EventBus. Subscribe `drift-detector.mjs` to all mutation events.
8. `attachSimulator()` — boot the webhook-simulator on port 9200 and graph-emulator on port 9100.
9. Returns the live handle. Tests get the handle and call `harness.deliverWebhook(fixture)`, `harness.tickGraph(...)`, `harness.advanceTicks(n)`.

`shutdown()`: drain EventBus, close DB pool, stop simulators, flush observers.

`tick()`: advances the runtime clock by one tick. Workers scheduled in this tick execute before `tick()` returns. Used by cadence tests.

### 4.2 recorder-observer.mjs (passive, no fabrication)

```
class RecorderObserver {
  constructor() { this.observation = []; }
  onEvent(event) {
    // Pure projection of the bus event into the recorder shape
    this.observation.push({
      event_id: event.lineageId,
      kind: event.kind,        // 'ingress' | 'governance' | 'fsm' | 'worker' | 'mutation'
      source: event.source,    // bus-emitter kernel/module
      payload: event.payload,
      ts: event.ts,
    });
  }
  flush() { /* write lineage-observation.json */ }
}
```

No `ingress()` / `governance()` / `fsm()` / `worker()` / `mutation()` write methods on the public surface. Tests cannot fabricate. `recorder-observer` is a subscriber, not an actor.

### 4.3 snapshot-deriver.mjs (recorder shape as a derived view)

After the runtime completes, this module reads the lineage ledger from the DB and projects it into the phase-8 recorder shape:

```
{
  event_id,
  ingress_ts,
  governance_ts,
  fsm_ts,
  worker_count,
  mutation_count,
  ordering_ok,   // derived from ts ordering
}
```

`constitutional-path-assert.mjs` reads this derived view. The shape is preserved for continuity, but the **source of truth is the lineage ledger**, not test code.

### 4.4 webhook-simulator.js (transport only)

Inherits phase-8's `webhook-simulator.js`. No business logic. Adds:
- `armRetry(fixture, n)` — next n deliveries return 429.
- `armDelay(fixture, ms)` — delay before response.
- `armDuplicate(fixture, n)` — replay last delivery n times.
- `armSignature(valid)` — set X-Hub-Signature-256 to a valid/invalid HMAC.

The simulator delivers an HTTP body. Nothing else. The runtime ingress parses, the runtime governance decides, the runtime FSM transitions, the runtime worker executes. The simulator never knows what a "FSM" is.

### 4.5 ingress-bridge.js

After the simulator delivers, the bridge POSTs the delivered body to the real runtime's ingress endpoint (the same one production uses). This is the seam where phase 8 had `p8.recorder.ingress(...)` — phase 9 has `ingressBridge.inject(body)`.

### 4.6 graph-emulator.js (worker validation tool, NOT a Meta emulator)

Explicit scope statement: **the goal is "can the worker survive?", not perfect Meta emulation.** We do not invest in features that do not help validate worker behavior.

Boots an external service emulator on port 9100. Endpoints:
- `/v1/accounts` — used by capability worker.
- `/v1/media/{id}/insights` — used by insights worker.
- `/v1/media/{id}/comments` — used by publishing worker.
- `/v1/recovery/{kind}` — used by recovery worker.
- `/v1/reconciliation/state` — used by reconciliation worker.

Supports the chaos vocabulary needed to exercise worker failure handling: rate-limit, 5xx, schema-drift, partial payload, duplicate, stale, scope-revocation, token-degradation.

What the emulator does **not** do:
- Does not model Meta's pagination semantics in detail.
- Does not implement Meta's rate-limit recovery windows precisely.
- Does not model Meta's permission scope hierarchy.
- Does not simulate other apps or concurrent account activity.

These are intentionally out of scope. When phase 10 lands on a real VPS with real Meta traffic, `captured/` fixtures replace the emulator for Tier 1 validation. The emulator's job is to keep the worker test path exercisable in CI without Meta.

### 4.6a Fixtures: canonical/ vs captured/

`fixtures/webhooks/canonical/` — hand-crafted perfect payloads, one per Meta event type. These are the deterministic baseline. They will never change unless the Meta schema changes. Phase 9 development and CI run against canonical.

`fixtures/webhooks/captured/` — real Meta payloads, populated during phase 10 (VPS / RunPod validation). Each captured payload is verbatim what Meta delivered to the runtime, with PII fields redacted. The README in this directory specifies:
- How to record (a phase-10 utility will land here).
- How to redact (replace sender.ids, comment text, media URLs with stable hashes).
- How to commit (one file per delivery, named `<sha256-of-body-prefix>.json`).

When `captured/` is non-empty, `runtime-webhook-captured.test.mjs` runs as a Tier 1 test. Until then, it is `it.skip` with a TODO referencing the VPS phase.

Captured payloads are the highest-value fixtures in the entire suite because they encode Meta's actual behavior, including edge cases our canonical fixtures do not anticipate (nested changes, unexpected field order, locale-specific timestamps, re-delivery patterns).

### 4.6b ownership-tracer.mjs

Reads the lineage ledger. For each `event_id`, projects the owner of each link in the chain:

```
{
  "event_id": "abc123",
  "chain": {
    "ingress":     { "owner": "runtime/ingress",                 "ts": 1700000000123 },
    "governance":  { "owner": "constitutional-kernel",            "ts": 1700000000145, "actor": "CK_DECISION" },
    "fsm":         { "owner": "acquisition-fsm",                  "ts": 1700000000167, "from": "IDLE", "to": "PROCESSING" },
    "worker":      { "owner": "acquisition-kernel",               "ts": 1700000000189, "worker": "acquisition-message-worker" },
    "mutation":    { "owner": "mutation-substrate",               "ts": 1700000000211, "kernel": "acquisition", "table": "messages" }
  }
}
```

Writes `ownership-trace.json` per run. Tests assert that the owner of each link is the kernel/module that the architecture mandates. This is the verification layer for the authority boundaries built over months.

### 4.6c replay-engine.mjs

Reads `lineage-observation.json`. Reconstructs the state by walking the observation log in order: for each mutation event, apply its effect to a shadow state store. Compare the shadow state at end-of-run against the actual state read directly from the mutation tables.

If they diverge: a causality leak exists — an event happened that the observation log did not see, or a mutation was written without an observation event preceding it.

Writes `replay-delta.json`:
```
{
  "diverged_keys": ["messages:abc123:status"],
  "expected_state": { "status": "processed" },
  "actual_state":   { "status": "pending" },
  "missing_observations": ["mutation:1700000000211"]
}
```

The test (`lineage-replay.test.mjs`) asserts `diverged_keys` is empty and `missing_observations` is empty. Any entry there is a bug.

### 4.6d kernel-isolation-harness.mjs

Boots a single kernel in isolation — no other kernels, no cross-kernel bus, no cross-kernel signal. Used by the sovereignty tests to answer: "can this kernel function if every other kernel disappears?"

The harness registers only the target kernel and its dependencies (substrates, governance, FSM). It does not register any other kernel. The test then exercises the target kernel's main path and asserts:
- State mutates correctly.
- No errors about missing peers.
- No "degraded mode" warnings.
- The kernel's FSM reaches its end states.

A negative test (`kernel-sovereignty-acquisition-coupling.test.mjs`) injects a known coupling (e.g., publishing-kernel expects acquisition-kernel to provide a counter) and asserts the test FAILS — confirming the coupling detector works.

### 4.7 drift-detector.mjs

Subscribes to mutation events. Each mutation carries `kernel` and `table` in its payload. Drift rules:
- `authority` — every mutation's `kernel` must equal the kernel that emitted the event.
- `semantic` — no payload field crosses from internal sentinel to public signal.
- `ownership` — no mutation writes to a foreign-kernel table.
- `cross-kernel-contamination` — sink kernel's lineage must not include source's `internalState`.
- `governance-leakage` — no `escalate()` invocation observed on the worker channel.
- `worker-autonomy` — workers never import scheduler/governance/fsm (scanned at boot).
- `fsm-ownership` — every FSM transition event carries an `fsm` field whose value matches a registered FSM name.

Drift findings are written to `drift-findings.json`. Tests assert the file is empty (or contains only pre-declared drift).

### 4.8 constitutional-path-assert.mjs

Reads `lineage-snapshot.json`. For each `event_id`, checks: ingress_ts ≤ governance_ts ≤ fsm_ts ≤ min(worker_ts) ≤ min(mutation_ts). This is the same ordering check phase 8 had, but now applied to a snapshot **derived from the lineage ledger** — not asserted by the test.

---

## 5. The 7 test domains

### 5.1 Webhook domain — TIER 1 CRITICAL (6+ scenarios)

The runtime is webhook-driven. This is the dominant path. The test effort, the report emphasis, and the failure priority all reflect that.

For each of the 6 canonical fixtures in `tests/phase-9/fixtures/webhooks/canonical/`:

1. Harness delivers the fixture via webhook-simulator.
2. Ingress-bridge injects the body into the real runtime ingress.
3. Runtime parses → governance → FSM → worker → mutation → EventBus.
4. recorder-observer captures every event into `lineage-observation.json`.
5. ownership-tracer projects the chain into `ownership-trace.json`.
6. After delivery, snapshot-deriver reads the lineage ledger → `lineage-snapshot.json`.
7. Test asserts:
   - Snapshot contains the `event_id` (derived from the delivered body).
   - ordering_ok is true.
   - worker_count ≥ 1.
   - mutation_count ≥ 1.
   - All `mutation` events reference the **correct kernel** (acquisition for messages/comments/mentions, etc.).
   - **ownership**: governance.owner = constitutional-kernel, fsm.owner = <event-type>-fsm, worker.owner = <event-type>-kernel, mutation.owner = mutation-substrate.
   - No drift findings.

Plus chaos scenarios: retry (429 → re-queue), duplicate (dedup absorbs), schema-drift (parser rejects pre-governance), signature (bad sig → ingress rejects).

Plus `runtime-webhook-captured.test.mjs`: when `captured/` is non-empty, every captured payload is delivered, validated against the same 7-point assertion, and stored in a separate `captured-results.json` for human review (because captured payloads are the highest-fidelity test of whether we understand Meta's actual behavior).

### 5.2 Graph domain — TIER 2 SUPPORT (5 scenarios)

The graph is no longer the primary acquisition source. It is the support surface for downstream operations on data the webhook path already produced.
Tier 2 failures are amber, not red — the suite reports them but does not block on them, unless the worker has regressed in a way that corrupts state Tier 1 already produced.

### 5.3 Cross-kernel domain (20 pairs)

Mirror phase-8's 20 pair files. Each pair uses `runPair({ source, sink })` from `_pair-helper.mjs`. The helper:
1. Builds a `public_signal` packet from source kernel.
2. `crossKernelBridge.dispatchGlobal(source, sink, packet)`.
3. The real CK kernel routes to sink via the real EventBus.
4. recorder-observer captures.
5. Snapshot-deriver projects.
6. Assertions: ordering_ok, sink's observation list contains the public_signal but never the source's internalState, no foreign-table mutations.

### 5.4 Ownership domain (6 files)

For each Tier 1 fixture (message, comment, mention, story-reply, conversation, media):

1. Deliver the canonical fixture via Tier 1 path.
2. ownership-tracer produces a chain record for the resulting event_id.
3. Test asserts the owner of each link is the architecture-mandated module:
   - ingress.owner = `runtime/ingress`
   - governance.owner = `constitutional-kernel`
   - fsm.owner = `<event-type>-fsm`
   - worker.owner = `<event-type>-kernel`
   - mutation.owner = `mutation-substrate`, mutation.kernel = `<event-type>-kernel`, mutation.table = `<event-type>-table`

This is the verification layer for the authority boundaries we built over months. It catches the case where, for example, the publishing-kernel's worker writes to a table owned by the acquisition-kernel — that bug passes the snapshot's `mutation_count` check but fails the ownership check.

### 5.5 Sovereignty domain (5 + 1 files)

For each of the 5 Tier 2 kernels (acquisition, publishing, capability, recovery, reconciliation):

1. kernel-isolation-harness boots only the target kernel.
2. All other kernels are absent (no events on the cross-kernel bus, no peers in the registry).
3. The kernel's main path is exercised (deliver a webhook, or tick a worker).
4. Test asserts:
   - State mutates correctly (read from the kernel's table).
   - No "missing peer" errors in the log.
   - No "degraded mode" warnings.
   - The kernel's FSM reaches its expected end state.

Plus 1 negative test (`kernel-sovereignty-acquisition-coupling.test.mjs`): inject a deliberate coupling (e.g., publishing-kernel depends on a counter published by acquisition-kernel), boot publishing in isolation, and assert the test FAILS — this confirms the coupling detector actually detects coupling.

### 5.6 Replay domain (1 file)

`lineage-replay.test.mjs`:

1. Run the full Tier 1 + Tier 2 suite (or a focused subset).
2. `replay-engine` reads `lineage-observation.json` and reconstructs the post-run state by walking mutation events in order.
3. `replay-engine` reads the actual mutation tables and computes the current state.
4. Compare. Writes `replay-delta.json`.
5. Test asserts:
   - `diverged_keys` is empty.
   - `missing_observations` is empty.
   - For every mutation in the observation log, a corresponding row exists in the mutation table.
   - For every row in the mutation table, a corresponding observation event exists.

A divergence is a causality leak — the runtime did something the observation log did not see, or vice versa. This is the most expensive assertion in the suite, but it is the one that catches "the runtime silently mutated state without going through the bus."

### 5.7 Drift domain (7 files)

For each drift class (authority, semantic, ownership, contamination, leakage, worker-autonomy, fsm-ownership): one test file that runs a known-scenario runtime exercise and asserts the corresponding drift-finding is **absent** (or **present** when a deliberate drift is injected for the negative case).

### 5.8 Integration domain (4 files)

- `runtime-composition.test.mjs` — runs all 6 webhook + all 5 graph + all 20 cross-kernel scenarios in one pass. Asserts the snapshot is internally consistent.
- `multi-tick-survival.test.mjs` — runs 25 / 100 / 500 / 1000 ticks depending on `PHASE9_CADENCE_TIER`. Per tick: deliver one webhook, run one graph, dispatch one cross-kernel pair. Asserts no starvation, no deadlock, no event explosion, no state corruption.
- `long-running-integrity.test.mjs` — runs the long tier (500+) and asserts a *non-increasing* invariant over time:
  - At every N-tick window (N=50), the count of authority violations has not increased.
  - At every N-tick window, the count of drift findings has not increased.
  - At every N-tick window, the count of orphan lineage events (event_ids in the ledger with no terminating mutation) has not increased.
  - This catches slow degradation that a one-shot composition test would miss.
- `scenario-message-thread.test.mjs` — full end-to-end: message webhook → conversation created → FSM activated → capability evaluated → worker dispatched → mutation written → cross-kernel to publishing → reply published → insights fetched → reconciliation acknowledged.

---

## 6. Recorder API change (the core invariant)

Phase 8's `constitutional-recorder.mjs` exposes:
```
recorder.ingress(eid, payload)
recorder.governance(eid, decision)
recorder.fsm(eid, transition)
recorder.worker(eid, name, action)
recorder.mutation(eid, mutation)
recorder.assertConstitutionalPath(eid)
recorder.assertAllConstitutional(eids)
recorder.summarize()
recorder.reset()
recorder.events
```

Phase 9's `recorder-observer.mjs` exposes **only**:
```
recorder.onEvent(event)         // internal — called by EventBus subscriber
recorder.flush()                // writes lineage-observation.json
recorder.summarize()            // returns observation count + first/last ts
recorder.assertSnapshot(...)    // reads lineage-snapshot.json
```

The 5 write methods are **removed from the public surface**. The phase-9 directory ships a shim that throws if any test file imports them. The runtime is the only writer.

---

## 7. Snapshot output format

`tests/phase-9/reports/<run-id>/lineage-observation.json`:
```
[
  { "ts": 1700000000123, "event_id": "abc123", "kind": "ingress", "source": "runtime/ingress", "payload": {…} },
  { "ts": 1700000000145, "event_id": "abc123", "kind": "governance", "source": "constitutional-kernel", "payload": { "decision": "PROCEED" } },
  …
]
```

`tests/phase-9/reports/<run-id>/lineage-snapshot.json`:
```
{
  "derived_at": "2026-06-14T…",
  "events": {
    "abc123": {
      "ingress_ts": 1700000000123,
      "governance_ts": 1700000000145,
      "fsm_ts": 1700000000167,
      "worker_count": 2,
      "mutation_count": 1,
      "ordering_ok": true,
      "kernels_touched": ["acquisition", "publishing"]
    }
  },
  "constitutional_paths": [ … ],
  "drift_findings": [ … ]
}
```

`tests/phase-9/reports/<run-id>/ownership-trace.json`:
```
{
  "abc123": {
    "ingress":   { "owner": "runtime/ingress",       "ts": 1700000000123 },
    "governance":{ "owner": "constitutional-kernel", "ts": 1700000000145, "actor": "CK_DECISION" },
    "fsm":       { "owner": "acquisition-fsm",       "ts": 1700000000167, "from": "IDLE", "to": "PROCESSING" },
    "worker":    { "owner": "acquisition-kernel",    "ts": 1700000000189, "worker": "acquisition-message-worker" },
    "mutation":  { "owner": "mutation-substrate",    "ts": 1700000000211, "kernel": "acquisition", "table": "messages" }
  }
}
```

`tests/phase-9/reports/<run-id>/replay-delta.json`:
```
{
  "diverged_keys": [],
  "missing_observations": [],
  "matched_keys": 1234,
  "checks_performed": ["messages:status", "comments:text", "conversations:last_message_at", …]
}
```

The recorder shape lives on as a **derived view** of the lineage ledger. The lineage ledger is the source of truth. The ownership trace is a separate projection that verifies the architecture's authority boundaries. The replay delta verifies that every event in the ledger corresponds to a real state change, and vice versa.

---

## 8. Cadence verification

`PHASE9_CADENCE_TIER` env: `short` = 25, `medium` = 100, `long` = 500, `epic` = 1000.

Per tick:
1. Advance the runtime clock.
2. Deliver a webhook fixture (rotating).
3. Tick one graph worker (rotating).
4. Dispatch one cross-kernel pair (rotating).
5. Wait for EventBus drain.

Assertions per tier:
- `short` (25): no starvation, no deadlock, snapshot consistent.
- `medium` (100): + no event explosion (observation log size < 10× tick count).
- `long` (500): + no cadence collapse (avg tick duration < 2× first quartile).
- `epic` (1000): + no state corruption (snapshot's event_ids are all unique, all ordering_ok).

### 8.1 Long-running integrity invariant (separate test)

`phase-9-long-running-integrity.test.mjs` runs at the `long` or `epic` tier. Every N ticks (N=50), it captures a windowed state and asserts three non-increasing invariants over time:

- `authority_violations(window)` — count of events in the window whose `owner` does not match the architecture-mandated module for their `kind`. Non-increasing.
- `drift_findings(window)` — count of drift-detector findings in the window. Non-increasing.
- `orphan_lineage_events(window)` — count of event_ids in the window's lineage ledger that have no terminating `mutation` event. Non-increasing.

A slow degradation — where these counts grow by 1 per N ticks — is invisible to a one-shot composition test but immediately visible here. This is the equivalent of a memory leak detector, applied to architectural invariants.

---

## 9. What must NOT appear in phase-9 tests

Lint rule + manual review. Phase 9 test files MUST NOT contain:
- `recorder.ingress(`
- `recorder.governance(`
- `recorder.fsm(`
- `recorder.worker(`
- `recorder.mutation(`
- `p8.recorder.*` (any recorder write)

If a test needs to "make a worker execute", it must do so by delivering a webhook, ticking the graph, or dispatching a cross-kernel pair — **not** by calling the recorder.

---

## 10. Failure vectors to catch

- Worker never executes because runtime wiring is broken — test fails (snapshot has 0 workers for the event_id).
- FSM never transitions because governance pre-empts — test fails (snapshot has no `fsm_ts`).
- Mutation never lands because DB schema drift — test fails (snapshot has 0 mutations, or `ordering_ok` is null).
- Recorder fabrication sneaks back in — lint rule fails the test file at parse time.
- Drift detector misses a foreign mutation — `drift-findings.json` is empty when it shouldn't be, test asserts non-empty.
- Cadence collapse at long tier — test asserts `avg tick < 2× first quartile`, fails when runtime stalls.

---

## 11. Exit criteria (mirrors the contract)

Tier 1 (must pass before phase 10):
- [ ] Runtime is authoritative (real workers execute, real FSMs transition, real DB mutates).
- [ ] Recorder is passive (no write methods, only `onEvent`).
- [ ] **Webhook (Tier 1) flows operate correctly**: 6 canonical fixtures + 4 chaos variants all green.
- [ ] **Ownership chain is verified**: every Tier 1 fixture's event_id has the architecture-mandated owner at every link.
- [ ] **Replay is clean**: `replay-delta.json` has empty `diverged_keys` and `missing_observations`.
- [ ] State mutations occur naturally (verified by `mutation_count` and DB row existence).
- [ ] Cross-kernel communication remains constitutional (20 pairs, contamination check).
- [ ] Drift detection remains operational (7 drift classes, negative cases).
- [ ] Event causality remains traceable (`lineage-observation.json` is the audit trail).
- [ ] No constitutional violations are detected (`constitutional-paths.json` has no `ok: false`).

Tier 2 (must pass before phase 10, but amber-not-red on individual worker failures):
- [ ] **Graph (Tier 2) worker flows operate correctly**: 5 worker scenarios + chaos variants.
- [ ] **Sovereignty holds**: each kernel can function in isolation. Negative test confirms the detector works.
- [ ] **Multi-tick survivability passes**: 25 / 100 / 500 / 1000 tiers.
- [ ] **Long-running integrity passes**: authority violations, drift findings, orphan lineage events are non-increasing.

Captured-fixture readiness (deferred to phase 10, but the test slot is reserved):
- [ ] `fixtures/webhooks/captured/` exists with README and recording utility.
- [ ] `runtime-webhook-captured.test.mjs` is committed as `it.skip` with TODO, becomes active on first captured payload.

---

## 12. Order of execution (when implementation begins)

1. `runtime-harness.js` + `db-reset.js` + `recorder-observer.mjs` + `snapshot-deriver.mjs` + `ownership-tracer.mjs`. Smoke test: boot, deliver one webhook, see one event in observation log AND one chain record in ownership-trace.
2. `fixtures/webhooks/canonical/*.json` (6 perfect payloads) + `fixtures/webhooks/captured/README.md` + `fixtures/webhooks/captured/.gitkeep` (slot reserved for phase 10).
3. `webhook-simulator.js` (transport-only refactor) + `ingress-bridge.js`. Smoke test: 6 canonical fixtures deliver, observation log shows ingress events.
4. First webhook scenario (`message-created`) end-to-end. Verify ordering_ok in derived snapshot AND ownership chain.
5. Remaining 5 canonical webhook scenarios + 4 chaos variants.
6. **Ownership domain**: 6 files asserting owner of every link in the chain. Catches authority-boundary bugs that ordering_ok misses.
7. `graph-emulator.js` (worker-validation scope) + 5 graph scenarios.
8. **Sovereignty domain**: `kernel-isolation-harness.mjs` + 5 kernel tests + 1 negative test.
9. `cross-kernel/_pair-helper.mjs` + 20 pair files.
10. `drift-detector.mjs` + 7 drift files.
11. `replay-engine.mjs` + `lineage-replay.test.mjs`. Validates causality.
12. Integration: composition, multi-tick, **long-running integrity** (non-increasing invariants), full scenario.
13. `MANIFEST.mjs` + `phase-9-runner.sh` + `QUICKSTART.md` + `vitest.config.js`.
14. Run all tiers (short → medium → long → epic), write reports, audit.
15. Slot `runtime-webhook-captured.test.mjs` as `it.skip` for phase 10.

**Tier 1 work (1-6, 9-11) blocks phase 10.** Tier 2 work (7, 8, 12) is amber, can complete in parallel or after.

---

## 13. Files NOT touched

- `tests/phase-8/**` — phase 8 stays as the constitutional-contracts layer. Phase 9 lives next to it, not on top.
- `acquisition-kernel/**`, `publishing-kernel/**`, `graph-capability-kernel/**`, `reconciliation-kernel/**`, `retry-cadence-kernel/**`, `dedup-kernel/**`, `scheduling-kernel/**`, `telemetry-kernel/**` — these are the runtime. Phase 9 observes them; it does not modify them.
- `contracts/**`, `substrates/**`, `control-plane/**` — these are the substrate. Phase 9 boots them; it does not modify them.

Phase 9 is a test layer. It is **observational, not architectural**. If we find we need to change the runtime, that is a phase-9b finding, not a phase-9 patch.

---

## 14. Risk and rollback

Phase 9 is additive. It does not modify phase 8, the runtime, or the substrate. If phase 9 fails, phase 8 still passes. If a single test fails, the rest of the suite continues (vitest default).

The biggest risk: the runtime's EventBus is not yet wired to expose every mutation as a bus event. If that's the case, snapshot-deriver will read the lineage ledger directly (which is the durable source of truth anyway) and `lineage-observation.json` will be a derived view of the ledger, not a live bus subscription. The recorder-shape invariant still holds: the test sees what the runtime did, not what the test told the recorder.

---

## 15. Verification plan (no-sign-off handoff)

After implementation:
- Run `bash tests/phase-9/phase-9-runner.sh` (tier=short).
- Inspect `tests/phase-9/reports/<run-id>/lineage-observation.json` — must be non-empty for every delivered fixture.
- Inspect `lineage-snapshot.json` — every `event_id` must have `ordering_ok: true`.
- Inspect **`ownership-trace.json`** — every link in every event's chain must have the architecture-mandated `owner`. Catches authority-boundary bugs.
- Inspect `drift-findings.json` — must be empty (no drift in normal scenarios).
- Inspect `replay-delta.json` — `diverged_keys` empty, `missing_observations` empty. Catches causality leaks.
- Inspect `cadence-stats.json` — `avg_tick_ms` must be stable across the 25 ticks.
- **Long-running integrity**: re-run at `PHASE9_CADENCE_TIER=long` and confirm authority violations, drift findings, and orphan lineage events are non-increasing across 50-tick windows.
- Re-run with chaos: arm `rate-limit` on a fixture, confirm `worker_count` is still ≥ 1 (worker retried, did not fabricate success) and `mutation_count` is still ≥ 1 (mutation eventually landed).
- **Sovereignty**: run each kernel in isolation, confirm no "missing peer" or "degraded mode" errors.
- **Captured-fixture slot**: confirm `fixtures/webhooks/captured/README.md` exists and `runtime-webhook-captured.test.mjs` is `it.skip` with a phase-10 TODO.
- Grep the test directory for any of the 5 forbidden write methods — must return zero matches.
