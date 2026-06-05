# Test ↔ Architecture Audit

**Scope.** Comprehensive read-only audit comparing the test suite (`tests/`) against the
current state of the backend architecture on disk. No code changes were made.
The contract doc (`DEVELOPMENT-CONTRACT.md`) is treated as a target, not a source of
truth — every claim in this report is verified against actual files in the repo.

**Project root.** `/Users/kamii/instagram-automation/instragram-automation-backend/`

**Audit date.** June 5, 2026 (Friday session).

---

## 0. Executive Summary

The backend has been decomposed into 9 bounded domain kernels plus a `control-plane/`
governance core and a root-level `substrates/` plane. The decomposition is *partial and
uneven*:

- The production wiring (`server.js` → `control-plane/orchastrator.js`) follows the
  new topology. It uses the kernel roots (`acquisition-kernel/fsm`,
  `telemetry-kernel/index`, `scheduling-kernel/substrates/cadence/cadence`, etc.) and
  registers **9** domain FSMs with the constitutional kernel.
- The test suite (especially `tests/helpers/runtime-simulator.js`) was authored against
  an **earlier, flatter** topology where every domain FSM lived at
  `control-plane/governance/domains/<name>-fsm.js` and projection workers lived at
  `control-plane/telemetry-workers/`. That structure was deleted when the kernels were
  carved out, but the tests were not refactored to follow.
- Result: **15 broken module references** between the test suite and the current
  production code. The tests cannot boot on the current architecture without
  refactoring. None of Phase 4b–6a can run as written; Phase 1–3 tests can run in
  isolation if they don't transitively import `runtime-simulator.js` (most don't, but
  they still import the missing `telemetry-workers/index.js`).
- The core test parameters — what the tests are *optimised for* (constitutional
  invariants, membrane authority, projection convergence, lineage replay, causal
  ordering) — are still architecturally correct and must be preserved verbatim. The
  refactor must rewire **paths**, not change the invariants.

The fix surface is small: rewrite the test-side import graph to mirror the kernel
topology that `control-plane/orchastrator.js` already uses. Add one shim for the
`control-plane/telemetry-workers/index.js` callers (the kernel already exports
`startAll/stopAll` aliases for backward compat — the test suite can import the kernel
directly).

---

## 1. Current Architecture (verified on disk)

### 1.1 Server entry point — `server.js`

**Status:** live. Boots the runtime.

What it wires up:

```
server.js
├── config/supabase.js                        — Supabase client
├── config/redis.js                           — Redis client
├── services/sync/index.js                    — runStartupHealthChecks() (no cron)
├── graph-capability-kernel/substrates/graph-capability/wiring.js
├── control-plane/governance/constitutional-kernel.js
├── control-plane/orchastrator.js             — startAllWorkers / stopAllWorkers
├── routes/webhook.js                         — Meta webhook reception
├── routes/legal.js                           — compliance pages
├── routes/instagram-api.js                   — frontend-facing rate-limited routes
├── routes/agents/oversight.js                — oversight chat proxy
└── routes/agents/heartbeat.js                — agent liveness heartbeat
```

**Note.** The `/api/instagram` mount is split: `routes/instagram-api.js` (frontend)
**and** `routes/agents/oversight.js` (agent) are both mounted on the same prefix.
This is fine for routing but is not documented in the contract.

### 1.2 `control-plane/orchastrator.js` (note: typo in filename)

**Status:** live. Sole composition root. `_wire()` and `startAllWorkers()` are the
canonical boot sequence.

**9 domain FSMs registered with CK** (line 67–75):

| # | Domain FSM | Path |
|---|------------|------|
| 1 | acquisition | `acquisition-kernel/fsm` |
| 2 | publishing | `publishing-kernel/fsm` |
| 3 | graph-capability | `graph-capability-kernel/fsm` |
| 4 | scheduling | `scheduling-kernel/fsm` |
| 5 | dedup | `dedup-kernel/fsm` |
| 6 | engagement | `retry-cadence-kernel/fsm` |
| 7 | reconciliation | `reconciliation-kernel/fsm` |
| 8 | telemetry-coordination | `telemetry-kernel/fsm` (telemetryCoordinationFsm) |
| 9 | persist-telemetry | `postgres-telemetry-kernel/fsm` |

Note that the test suite (`runtime-simulator.js`, line 65–73) only registers **7** of
these — it's missing `graph-capability` and `persist-telemetry`. The 7th it does
register (`telemetryCoordinationFsm`) corresponds to the new `telemetry-kernel/fsm`.

**Membrane orchestrators wired** (line 81–85):

- `scheduling-kernel/orchestrator` — cadence
- `acquisition-kernel/orchestrator` — acquisition membrane
- `publishing-kernel/orchestrator` — emission
- `control-plane/orchestration/lifecycle-orchestrator` — lifecycle
- `control-plane/orchestration/degradation-orchestrator` — degradation

**Key wiring assertions the production orchestrator makes** (these must hold
in the refactored tests too):

- `constitutional.rehydrate()` is called **after** transition-writers start (line 145).
- `cognitionScanner.start(constitutional, accounts, publishingFsm)` is the sole
  publishing trigger (line 158).
- `telemetryCoordinationFsm.start(ckCtx)` is started **after** `startLoop()`
  (line 175).
- Reconciliation is **trigger-driven**, not timer-driven (line 195 comment).
- `cadence.every(REFRESH_INTERVAL_MS, ...)` emits `CADENCE_TICK` only (line 188).

### 1.3 Kernel inventory (9 kernels, root paths)

Each kernel follows the `fsm.js + orchestrator.js + substrates/` pattern, except
where the kernel is purely a substrate plane (postgres-telemetry) or a single-FSM
kernel (graph-capability, scheduling).

| Kernel | fsm.js | orchestrator.js | substrates/ | workers/ |
|--------|--------|-----------------|-------------|----------|
| acquisition-kernel/ | ✓ 17.2KB | ✓ 7.4KB | content, engagement, insights, ugc | parsing/workers + retry-cadence hooks |
| dedup-kernel/ | ✓ 16.5KB | (uses index.js) | dedup, repair | — |
| graph-capability-kernel/ | ✓ 21.7KB | (via wiring.js) | graph-capability, vault | — |
| postgres-telemetry-kernel/ | ✓ 15.7KB | (n/a — substrate kernel) | readers, reading, writers | — |
| publishing-kernel/ | ✓ 16.0KB | ✓ 9.5KB | content, engagement | — |
| reconciliation-kernel/ | ✓ 21.8KB | (n/a — has engine.js + substrate.js + worker.js) | — | — |
| retry-cadence-kernel/ | ✓ 30.3KB | (uses index.js) | — | content/engagement/insights/ugc |
| scheduling-kernel/ | ✓ 11.1KB | ✓ 2.7KB | cadence, reading-substrate | — |
| telemetry-kernel/ | ✓ 36.8KB | (uses index.js — exports startAll/stopAll) | ingress-lag-worker, projection | — |

### 1.4 Substrate planes (root-level + kernel-local)

The root `substrates/` directory is mostly **stale**. It still contains the old
sync/realtime/rate-limiter/retry/quota/telemetry surfaces that have been migrated into
the kernel-local substrate directories. Verified by `server.js` (which never imports
from `substrates/`) and `control-plane/orchastrator.js` (which only imports
`substrates/metrics-substrate.js` and `substrates/sync-substrate.js`).

`substrates/metrics-substrate.js` and `substrates/sync-substrate.js` are still live
because they're imported by the orchestrator. Everything else in `substrates/` is
residual from the pre-kernel era.

### 1.5 Routes (live + eliminated)

**Live (mounted by `server.js`):**
- `routes/webhook.js` — Meta webhook (note: still contains the contract-forbidden
  `forwardToAgent()` pattern, which the contract says must be eliminated)
- `routes/legal.js` — compliance
- `routes/instagram-api.js` — thin router that mounts `routes/frontend/*`
- `routes/agents/oversight.js` — oversight chat
- `routes/agents/heartbeat.js` — agent heartbeat

**Frontend sub-routers (mounted by `routes/instagram-api.js`):**
- `routes/frontend/tokens.js`
- `routes/frontend/media.js`
- `routes/frontend/sync.js`
- `routes/frontend/inbox.js`
- `routes/frontend/ugc.js`

**Agent sub-routers — ELIMINATED (per contract):**
- `routes/agents/ugc.js` — does not exist
- `routes/agents/engagement.js` — does not exist
- `routes/agents/publishing.js` — does not exist
- `routes/agents/analytics.js` — does not exist
- `routes/agents/queue.js` — does not exist

The contract says these have been migrated to the HSM governance pipeline. Agents
emit AcquisitionIntents directly to Redis queues; the substrate workers consume them.
This is reflected in `acquisition-kernel/retry-worker.js`,
`acquisition-kernel/parsing/workers/`, and the `retry-cadence-kernel/workers/` set
(`content-retry-worker.js`, `engagement-retry-worker.js`, `insights-retry-worker.js`,
`ugc-retry-worker.js`).

### 1.6 Constitutional kernel exports

Verified at the bottom of `control-plane/governance/constitutional-kernel.js`. Public
API the tests can rely on:

```
dispatch, subscribeAction, onAction, registerDomain, validateDomainTransition,
tick, startLoop, stopLoop, rehydrate, status, getState, getLineage, getAccountIds,
isCircuitBreakerActive, getAuthStrikes, getRetryCount, resetAuthStrikes,
clearCircuitBreaker, triggerReconciliation, validateMembraneTransition,
recordMembraneBypassAnomaly, SIGNAL_CLASS, getTransitionWriterHealth,
getIngressState, getReadingSubstrate, governedRead
```

### 1.7 Observability API surface (verified)

`control-plane/observability/index.js` exports:

- `transition({...})` — emit a state transition
- `capture(topic, data)` — signal-bus capture
- `query.{getState, getDomainState, getTransitionLog, getCrossDomain,
  getFullSnapshot, getEntriesSince, getLogSize, registerConsumer,
  unregisterConsumer, updateConsumerCursor, getConsumerLag}` — query projection
- `getSnapshot()` — alias for getFullSnapshot
- `onWrite(callback)` — reactive write hook
- `init()` / `stop()` — lifecycle

This is the API the test suite assumes and it is intact. Tests at the
`observability` layer (Phase 1a, 1c, 1d) should run cleanly once the import paths
are corrected.

---

## 2. Test Suite Inventory

### 2.1 Test files (37 files in `tests/`)

| File | Size | What it tests | Import path concerns |
|------|------|---------------|----------------------|
| `phase-1a-observability.test.js` | 5.9KB | observability transition emission, projection, lineage markers | None (uses `../config/redis.js`, `../control-plane/observability/index.js`) |
| `phase-1a-observability-contracts.test.js` | 4.2KB | observability + smoke contracts with telemetry workers | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-1b-deterministic-simulation.test.js` | 10.5KB | success/malformed/duplicate/stale/rate-limited/partial payloads via event-injector | None on the import path (uses observability + redis + event-injector) |
| `phase-1c-chaos-stress.test.js` | 4.3KB | duplicate burst, replay safety | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-1c-constitutional-verification.test.js` | 3.5KB | rejects malformed transitions | None |
| `phase-1c-replay-repair.test.js` | 4.2KB | replay ordering corruption | **Imports `../control-plane/governance/domains/publishing-fsm` — MISSING** (the publishing FSM lives at `publishing-kernel/fsm`) |
| `phase-1d-projection-integrity.test.js` | 6.4KB | projection shape, replay collision | None |
| `phase-2a-lineage-accumulation.test.js` | 6.1KB | continuous ticker lineage growth | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-2b-reconciliation-drift.test.js` | 9.7KB | corruption injection across domains | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-2c-long-run-endurance.test.js` | 5.6KB | 3-min soak | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-2d-redis-durability.test.js` | 4.0KB | Redis stack restart | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-3a-mixed-domain-concurrency.test.js` | 2.4KB | 24-wave mixed-domain concurrency | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-3b-membrane-boundary-integrity.test.js` | 1.8KB | cross-domain disorder quarantine | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-3c-cross-domain-reconciliation-isolation.test.js` | 1.8KB | reconciliation isolation | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-3d-unified-projection-determinism.test.js` | 2.2KB | wave signature determinism | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-4a-projection-ownership-integrity.test.js` | 0.7KB | static file-content check (no live runtime) | **Reads from `control-plane/telemetry-workers/runtime-projection-worker.js` etc. — directory was DELETED, moved to `telemetry-kernel/substrates/projection/workers/`** |
| `phase-4b-relay-lineage-immutability.test.js` | 1.9KB | SEMANTIC_PROJECTION_TRANSITION persistence | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-4c-cross-domain-pressure-stability.test.js` | 1.9KB | mixed-domain pressure + lag | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-4d-restart-recovery-determinism.test.js` | 1.9KB | restart recovery, projection continuity | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-4e-replay-reconstruction.test.js` | 4.5KB | lineage replay → projection convergence | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-4f-causal-ordering.test.js` | 6.7KB | timestamp monotonicity, cursor monotonicity, replay idempotence | None on import path |
| `phase-4g-membrane-attack.test.js` | 5.5KB | cross-domain authority violations | None on import path |
| `phase-4h-consumer-pressure.test.js` | 6.6KB | consumer lag resilience | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-4i-concurrency-corruption.test.js` | 6.9KB | adversarial concurrency | None on import path |
| `phase-4j-telemetry-isolation-pressure.test.js` | 11.9KB | high-freq polling, no recursive influence | **Imports `../control-plane/telemetry-workers/index.js` — MISSING** |
| `phase-4k-durable-persistence-integrity.test.js` | 5.6KB | snapshot/rehydration cycle | None on import path |
| `phase-4l-periodic-hash-convergence.test.js` | 5.1KB | periodic hash convergence | None on import path |
| `phase-4m-unified-worker-recycle.test.js` | 5.8KB | unified recycle (5 properties together) | None on import path |
| `phase-4n-mixed-constitutional-soak.test.js` | 18.2KB | 30-min mixed legal + adversarial soak | None on import path |
| `phase-5a-reconciliation-gap-tests.test.js` | 27.9KB | reconciliation engine gaps (1–3, 6) | None on import path (but uses runtime-simulator transitively in some sections) |
| `phase-5b-concurrent-ecosystem.test.js` | 8.4KB | 5-min 5-domain concurrent | **Imports `runtime-simulator.js` which has 15 broken refs** |
| `phase-5c-catastrophic-fault-recovery.test.js` | 10.9KB | worker massacre / Redis restart / corruption | (uses `runtime-simulator.js`) |
| `phase-5d-longitudinal-constitutional-soak.test.js` | 20.4KB | 1-hr soak, every 5-min checkpoint | (uses `runtime-simulator.js`) |
| `phase-6-telemetry-coordination-fsm.test.js` | 42.6KB | T1–T12 FSM coordination, 45-min soak | (uses `runtime-simulator.js`) |
| `phase-6a-transition-writers-redis.test.js` | 15.7KB | 5 transition writers' Redis write path | (uses `runtime-simulator.js`) |
| `constitutional-runtime.test.js` | 3.5KB | legacy governance runtime test | None on import path |
| `substrate-isolation.test.js` | 4.2KB | legacy substrate isolation | None on import path |
| `event-injector.js` | 16.5KB | test helper — not a test, but imported by all phase-2+ tests | OK (uses kernel paths via `lineage-ledger.js` and `lineageLedger.injectTestEntry`) |
| `helpers/sync-barriers.js` | 8.5KB | poll-based sync barriers | OK (requires `control-plane/observability` and `control-plane/governance/lineage-ledger`) |
| `helpers/constitutional-invariants.js` | 18.9KB | invariant assertion helpers | (read separately) |
| `helpers/runtime-simulator.js` | 23.4KB | full boot — parallel to orchastrator.js | **15 broken refs — see §3** |
| `helpers/runtime-monitor.js` | 5.8KB | periodic snapshot probe | OK (uses observability + lineage-ledger + invariants) |
| `mock-substrates/{success,partial,malformed,duplicate,stale,rate-limited}/` | dirs | fixture payloads | (used by event-injector; not validated here) |

### 2.2 Test runner harness

- `tests/run-all-tests.sh` — orchestrator for phases 1–6, runs inside Docker
  test-runner container via `docker-compose.test.yml`.
- `tests/vitest.config.js` — vitest 4, `pool: 'forks'`, `singleFork: true`,
  `testTimeout: 3_800_000` (5D soak needs up to 1hr + buffer).
- `tests/setup/global-setup.js` — flushes `test:*` Redis keys once.
- `tests/setup/test-setup.js` — flushes per-test-file.

The runner is **not the problem**. The runner is correct. The problem is that the
test files it loads have stale imports.

### 2.3 The two-tier test structure

There are two ways a test boots the runtime:

**Tier A (light):** `observability.init()` + `telemetryWorkers.startAll(N)` only.
Used by Phase 1a, 1c, 1d, 2a–2d, 3a–3d, 4b–4m, 4j.

This needs the import path
`../control-plane/telemetry-workers/index.js` to expose `startAll/stopAll`. The
kernel (`telemetry-kernel/index.js`) already does. The fix is a one-line import
swap.

**Tier B (heavy):** `new RuntimeSimulator().boot()`.
Used by Phase 5a–5d, 6, 6a. This needs `runtime-simulator.js` itself, which has
15 broken module references — see §3.

**Tier C (file-content static):** Phase 4a only. Reads files from
`control-plane/telemetry-workers/` directly, which has been deleted.

---

## 3. Broken Module References — The Delta

The 15 test-side import paths that no longer resolve. For each, the **current**
location on disk and the **test-target** location.

### 3.1 `control-plane/telemetry-workers/index.js`

**Used by (16 files):**
- `tests/phase-1a-observability-contracts.test.js`
- `tests/phase-1c-chaos-stress.test.js`
- `tests/phase-2a-lineage-accumulation.test.js`
- `tests/phase-2b-reconciliation-drift.test.js`
- `tests/phase-2c-long-run-endurance.test.js`
- `tests/phase-2d-redis-durability.test.js`
- `tests/phase-3a-mixed-domain-concurrency.test.js`
- `tests/phase-3b-membrane-boundary-integrity.test.js`
- `tests/phase-3c-cross-domain-reconciliation-isolation.test.js`
- `tests/phase-3d-unified-projection-determinism.test.js`
- `tests/phase-4b-relay-lineage-immutability.test.js`
- `tests/phase-4c-cross-domain-pressure-stability.test.js`
- `tests/phase-4d-restart-recovery-determinism.test.js`
- `tests/phase-4e-replay-reconstruction.test.js`
- `tests/phase-4h-consumer-pressure.test.js`
- `tests/phase-4j-telemetry-isolation-pressure.test.js`
- `tests/helpers/runtime-simulator.js`

**On disk:** does not exist. Was deleted when the telemetry kernel was carved out.

**Current location:** `telemetry-kernel/index.js` exports `startAll`, `stopAll`
as aliases to `substrate.startProjections()` / `substrate.stopProjections()`.
The contract is preserved — the old call shape still works.

**Fix.** Replace `import telemetryWorkers from '../control-plane/telemetry-workers/index.js'`
with `import telemetryWorkers from '../telemetry-kernel/index.js'` (or its
default). One-line per file. Or add a one-line shim at
`control-plane/telemetry-workers/index.js` that re-exports from the kernel.

### 3.2 `control-plane/telemetry-workers/transition-writers/index.js`

**Used by (1 file):**
- `tests/helpers/runtime-simulator.js` (line 44)

**On disk:** `control-plane/telemetry-workers/transition-writers/index.js` exists.
The directory and `index.js` are intact. **No fix needed** — but the parent
`control-plane/telemetry-workers/index.js` is the one missing, not the writers.

(Note: the production wiring in `control-plane/orchastrator.js` imports
`./telemetry-workers/transition-writers` as a sub-path, which resolves fine. The
test-side import does the same and works.)

### 3.3 `control-plane/governance/reconciliation-engine.js`

**Used by (1 file):**
- `tests/helpers/runtime-simulator.js` (line 48)

**On disk:** does not exist.

**Current location:** `reconciliation-kernel/engine.js` (32.7KB).

**Fix.** Update the runtime-simulator import to
`require('../../reconciliation-kernel/engine.js')`.

### 3.4 `control-plane/runtime/lifecycle.js`

**Used by (1 file):**
- `tests/helpers/runtime-simulator.js` (line 50)

**On disk:** does not exist at that path.

**Current location:** `scheduling-kernel/substrates/cadence/lifecycle.js`
(the orchestrator also imports it from there).

**Fix.** Update runtime-simulator import.

### 3.5 `control-plane/runtime/cadence.js`

**Used by (1 file):**
- `tests/helpers/runtime-simulator.js` (line 54)

**On disk:** does not exist.

**Current location:** `scheduling-kernel/substrates/cadence/cadence.js`.

**Fix.** Update runtime-simulator import.

### 3.6 `control-plane/governance/domains/acquisition-fsm.js`

**Used by (1 file):**
- `tests/helpers/runtime-simulator.js` (line 57)

**On disk:** does not exist.

**Current location:** `acquisition-kernel/fsm`.

**Fix.** Update runtime-simulator import. The `registerDomain()` call in
`runtime-simulator.js` should be updated to use the kernel root paths and include
all **9** domain FSMs the production orchestrator registers (it currently misses
`graph-capability-kernel/fsm` and `postgres-telemetry-kernel/fsm`).

### 3.7–3.10. Other domain FSM paths in runtime-simulator

- `control-plane/governance/domains/publishing-fsm.js` → `publishing-kernel/fsm`
- `control-plane/governance/domains/scheduling-fsm.js` → `scheduling-kernel/fsm`
- `control-plane/governance/domains/engagement-fsm.js` → `retry-cadence-kernel/fsm`
- `control-plane/governance/domains/reconciliation-fsm.js` → `reconciliation-kernel/fsm`
- `control-plane/governance/domains/telemetry-coordination-fsm.js` → `telemetry-kernel/fsm`

The current `runtime-simulator.js` registers only **7** domains. The production
orchestrator registers **9**. To preserve the test invariants (and to be
constitutionally equivalent to production), the test-side registration must add
`graph-capability-kernel/fsm` and `postgres-telemetry-kernel/fsm`.

### 3.11 `control-plane/governance/domains/telemetry-coordination-fsm.js`

Same as 3.10. Same fix.

### 3.12 `tests/phase-1c-replay-repair.test.js`

This test imports `../control-plane/governance/domains/publishing-fsm`. The
publishing FSM is at `../publishing-kernel/fsm`. **Test-side fix needed.**

### 3.13 `tests/phase-4a-projection-ownership-integrity.test.js`

**Static file check.** Reads from
`control-plane/telemetry-workers/{runtime,integrity,authority,health,systemic-pressure}-projection-worker.js`.
The directory was deleted. The workers now live at
`telemetry-kernel/substrates/projection/workers/`.

The test itself is checking that workers do NOT import
`governance/lineage-worker` — a static import-graph assertion. The test's intent
is sound but its path is wrong.

**Fix.** Update the file path glob to
`telemetry-kernel/substrates/projection/workers/{...}.js`. Same `lineage-worker`
string check still applies.

### 3.14 Routes that don't exist (and shouldn't)

The `routes/agents/{ugc,engagement,publishing,analytics,queue}.js` files do not
exist and **must not** be created. The contract has eliminated these endpoints —
agents emit AcquisitionIntents to Redis, the substrate workers consume them.

The test suite does not import these (because there's nothing to import). This
is a non-issue. The contract is correct, the absence is intentional.

### 3.15 `substrates/*` stale surface

The root `substrates/` directory contains:
- `metrics-substrate.js` — **live** (used by orchestrator and runtime-simulator)
- `sync-substrate.js` — **live** (used by orchestrator and runtime-simulator)
- `realtime.js`, `telemetry.js`, `quota.js`, `retry.js` — **stale** (not imported anywhere except possibly helpers)
- `rate-limiter/`, `transport/` — **stale** (not imported anywhere)
- `mutation-substrate.js` — does not exist (was at `control-plane/mutation-substrate.js` per old contract; also doesn't exist there now)

The runtime-simulator and event-injector only import the live two. No fix
required for stale surfaces — but they are dead code and should be removed in a
follow-up (not in this refactor).

---

## 4. Refactor Plan — What the Tests Need

**Principle: preserve every invariant, change every path.** The 7 constitutional
laws (L1–L7) the tests assert are still architecturally correct. The refactor
must rewire import paths to the kernel topology without weakening any assertion.

### 4.1 Tier-A tests (Phase 1a/1c/1d, 2a–2d, 3a–3d, 4b–4m, 4j) — bulk fix

**Single import swap.** Change
`import telemetryWorkers from '../control-plane/telemetry-workers/index.js'`
to
`import telemetryWorkers from '../telemetry-kernel/index.js'`

(or the equivalent `require` form in Phase 1b).

This works because `telemetry-kernel/index.js` already exports `startAll/stopAll`
with the same signatures the tests call.

**Alternatively**, a one-line shim at
`control-plane/telemetry-workers/index.js` that re-exports from the kernel:

```js
// control-plane/telemetry-workers/index.js (SHIM — for test suite compat)
module.exports = require('../../telemetry-kernel');
```

Either works. The shim is less invasive (16 file edits → 1 file create).

### 4.2 Tier-B tests (Phase 5*, 6*, 6a) — runtime-simulator rewrite

The runtime-simulator needs to be brought into alignment with
`control-plane/orchastrator.js`. Concretely:

**Imports** (15 lines changed):

| Current import | New import |
|----------------|-----------|
| `../../control-plane/governance/reconciliation-engine.js` | `../../reconciliation-kernel/engine.js` |
| `../../control-plane/runtime/lifecycle.js` | `../../scheduling-kernel/substrates/cadence/lifecycle.js` |
| `../../control-plane/runtime/cadence.js` | `../../scheduling-kernel/substrates/cadence/cadence.js` |
| `../../control-plane/governance/domains/acquisition-fsm.js` | `../../acquisition-kernel/fsm` |
| `../../control-plane/governance/domains/publishing-fsm.js` | `../../publishing-kernel/fsm` |
| `../../control-plane/governance/domains/scheduling-fsm.js` | `../../scheduling-kernel/fsm` |
| `../../control-plane/governance/domains/engagement-fsm.js` | `../../retry-cadence-kernel/fsm` |
| `../../control-plane/governance/domains/reconciliation-fsm.js` | `../../reconciliation-kernel/fsm` |
| `../../control-plane/governance/domains/telemetry-coordination-fsm.js` | `../../telemetry-kernel/fsm` |
| `../../control-plane/telemetry-workers/index.js` | `../../telemetry-kernel` (kernel) |

**Domain FSM registration** must add the two missing kernels:
- `graph-capability-kernel/fsm`
- `postgres-telemetry-kernel/fsm`

This brings runtime-simulator into constitutional equivalence with the production
orchestrator (both register the same 9 domains).

**Boot sequence parity** with `control-plane/orchastrator.js`:

The 17-step boot list documented in runtime-simulator's header (lines 9–26) is
already an accurate description of the orchestrator's `startAllWorkers()`. The
implementation must match. Specific things to verify:

- `constitutional.rehydrate()` happens **after** `telemetryWorkers.startAll()`
  and `transitionWriters.startAll()` (not before).
- `cognitionScanner.start(constitutional, accounts, publishingFsm)` is the
  publishing trigger — required for the `publishing` domain to ever leave BOOTING.
- `telemetryCoordinationFsm.start(ckCtx)` is started **after** `startLoop()`.
- Reconciliation has no timer (the 60s `cadence.every` in runtime-simulator's
  step 17 must be removed — the orchestrator explicitly comments that this was
  deprecated).
- `BOOT_COMPLETE` is dispatched after `LIFECYCLE_REFRESHED`, before `startLoop()`.

**Boilerplate assertion sites** in the test files that need to be aware of the
new boot order:

- `phase-5a-reconciliation-gap-tests.test.js` — drives reconciliation directly
  via `reconciliationEngine`. After the import swap, this is
  `reconciliation-kernel/engine.js`. The test calls `reconciliationEngine.start()`
  and `reconciliationEngine.trigger()`. The engine API has not changed.
- `phase-5c-catastrophic-fault-recovery.test.js` — kills workers, restarts,
  triggers reconciliation. The `sim.shutdown()` and `sim.boot()` cycle is the
  natural entry point.
- `phase-6-telemetry-coordination-fsm.test.js` — uses `telemetryCoordinationFsm`
  directly. After the import swap, this is `telemetry-kernel/fsm`. The FSM
  must expose `start(ckCtx)`, `stop()`, and accept
  `{validate, dispatchGlobal, getGlobalState}` as the ctx shape.
- `phase-6a-transition-writers-redis.test.js` — exercises the 5 transition
  writers. The writers live at
  `control-plane/telemetry-workers/transition-writers/{runtime,integrity,authority,health,systemic}-transition-writer.js`
  (still on the old path; verified on disk). This test does not import the
  parent `control-plane/telemetry-workers/index.js`, so it does not break on
  the broken Tier-A import. But it may transitively require the simulator —
  verify.

### 4.3 Tier-C test (Phase 4a) — path-only fix

`tests/phase-4a-projection-ownership-integrity.test.js`. Update the directory
glob from `control-plane/telemetry-workers/` to
`telemetry-kernel/substrates/projection/workers/`. Update the worker filenames
to include the `-projection-` infix (the new naming is
`{runtime,integrity,authority,health,systemic-pressure}-projection-worker.js`).

The test's invariant check is unchanged: "do these workers import
`governance/lineage-worker` directly?" — still a valid architectural assertion.

### 4.4 Tier-A edge case — `phase-1c-replay-repair.test.js`

Imports `../control-plane/governance/domains/publishing-fsm`. Fix:
`../publishing-kernel/fsm`. Also verify the test's `publishingFsm.init('IDLE')`
and `publishingFsm.dispatch({type, ...})` API matches the kernel publishing FSM.

### 4.5 What must NOT change

The refactor is path-only. The following must be preserved:

- **All 7 constitutional laws** (L1 projection convergence, L2 no timestamp
  regression, L3 no cross-domain membrane bypass, L4 monotonic cursors, L5
  idempotent replay, L6 stale entries flagged, L7 signal ownership contract).
- **All test parameters** (TICK_INTERVAL_MS, SOAK_DURATION_MS,
  RECONCILIATION_INTERVAL_MS, PHASE*_SOAK_MS env vars, etc.) — these are
  tuned for the runtime's load profile, not its path structure.
- **All assertion semantics** — `assertNoTimestampRegression`,
  `assertMonotonicCursors`, `assertIdempotentReplay`, `assertStaleEntriesFlagged`,
  `assertCausalChainIntegrity`, `deterministicEntryHash`. Their implementations
  in `tests/helpers/constitutional-invariants.js` operate on lineage entries,
  not on module paths, so they're not affected.
- **Mock substrate fixtures** in `tests/mock-substrates/{success,partial,malformed,duplicate,stale,rate-limited}/` — these are JSON files injected via `event-injector.js` and do not depend on the production code structure.
- **Sync barriers** in `tests/helpers/sync-barriers.js` — they poll the
  observability and lineage APIs, which are unchanged.

### 4.6 Order of operations for the refactor

1. Create the shim (or do the bulk import swap in 16 Tier-A tests).
2. Refactor `runtime-simulator.js` imports + add 2 missing domain FSMs.
3. Fix `phase-4a` and `phase-1c-replay-repair` path globs.
4. Run the Tier-A tests (Phases 1–3) in isolation — these are the fastest
   signal that paths are right.
5. Run the Tier-B tests (Phases 5a, 5b) — these exercise the full boot path.
6. Run the soak tests (4N, 5D, 6) only after Phases 1–5b pass.

### 4.7 Risks

- **FSM API drift.** The kernel-root FSMs (`acquisition-kernel/fsm`,
  `publishing-kernel/fsm`, etc.) may have a different external API than the old
  `control-plane/governance/domains/*-fsm.js` versions. The runtime-simulator
  uses `fsm.name`, `fsm.init(state)`, `fsm.dispatch(event, ctx)`, and
  `fsm.start(ctx)` / `fsm.stop()`. These must be verified against each kernel
  FSM before relying on them. If any are missing, the test refactor is
  insufficient — the kernel FSMs need extension.

- **Reconciliation engine API.** `tests/phase-5a` calls
  `reconciliationEngine.start()`, `trigger()`, and `getState()`. The kernel
  version (`reconciliation-kernel/engine.js`) must expose at least these. The
  `engine.js` is 32.7KB and likely does, but the audit didn't read its full
  body — must be verified.

- **`publishingFsm.init('IDLE')` in `phase-1c-replay-repair`.** The kernel
  publishing FSM must accept an initial state argument. The contract says
  FSMs are pure state machines, so this should be a method on the FSM object.
  Verify it exists.

- **Test mock-substrates may have stale format.** The mock-substrates in
  `tests/mock-substrates/` were authored against the old acquisition shape. The
  contract reworked AcquisitionIntents — the `intent_type` and `parameters`
  schema are defined in `contracts/acquisition-intents.js`. If event-injector
  uses the old shape, mock-substrates will produce data the runtime rejects.
  The event-injector is currently passing tests against the in-memory
  observability plane (not the actual substrate), so this risk is latent but
  not blocking.

### 4.8 Items outside the scope of this refactor (parked)

- Removing dead code in root `substrates/` (realtime, quota, telemetry, retry,
  rate-limiter, transport).
- Removing the still-present `forwardToAgent()` in `routes/webhook.js` (contract
  violation, but not a test refactor concern).
- Deciding the final shape of the `routes/agents/oversight.js` proxy
  (it's mounted on `/api/instagram` and not under `agents/` per the contract).
- Cleaning up `control-plane/signal-bus.js` (only used by tests/event-injector
  and the deleted control-plane/mutation-substrate.js).

These are follow-up tasks, not blockers for the test refactor.

---

## 5. Quick Reference — Import Path Mapping

For quick reference, the test-side import path → current production path table:

| Test imports (old path) | Resolves to (current path) |
|--------------------------|----------------------------|
| `control-plane/telemetry-workers/index.js` | `telemetry-kernel/index.js` |
| `control-plane/telemetry-workers/transition-writers/index.js` | (unchanged) |
| `control-plane/governance/reconciliation-engine.js` | `reconciliation-kernel/engine.js` |
| `control-plane/runtime/lifecycle.js` | `scheduling-kernel/substrates/cadence/lifecycle.js` |
| `control-plane/runtime/cadence.js` | `scheduling-kernel/substrates/cadence/cadence.js` |
| `control-plane/runtime/signal-intake.js` | (unchanged) |
| `control-plane/runtime/buffer.js` | (unchanged) |
| `control-plane/runtime/evaluation.js` | (unchanged) |
| `control-plane/governance/domains/acquisition-fsm.js` | `acquisition-kernel/fsm` |
| `control-plane/governance/domains/publishing-fsm.js` | `publishing-kernel/fsm` |
| `control-plane/governance/domains/scheduling-fsm.js` | `scheduling-kernel/fsm` |
| `control-plane/governance/domains/engagement-fsm.js` | `retry-cadence-kernel/fsm` |
| `control-plane/governance/domains/reconciliation-fsm.js` | `reconciliation-kernel/fsm` |
| `control-plane/governance/domains/telemetry-coordination-fsm.js` | `telemetry-kernel/fsm` |
| `control-plane/governance/domains/reading-substrate` | `control-plane/governance/domains/reading-substrate/index.js` (unchanged) |
| `control-plane/governance/constitutional-kernel.js` | (unchanged) |
| `control-plane/governance/lineage-ledger.js` | (unchanged) |
| `control-plane/governance/lineage-checkpointer.js` | (unchanged) |
| `control-plane/governance/interpreters/namespace-projection-interpreter.js` | (unchanged) |
| `control-plane/governance/interpreters/engagement-telemetry-adapter.js` | (unchanged) |
| `control-plane/governance/ingress-consistency/substrate.js` | (unchanged) |
| `control-plane/observability/index.js` | (unchanged) |
| `control-plane/observability/projection.js` | (unchanged) |
| `control-plane/observability/normalizer.js` | (unchanged) |
| `control-plane/observability/bus/signal-bus-integration.js` | (unchanged) |
| `control-plane/observability/emitters/transition-emitter.js` | (unchanged) |
| `substrates/metrics-substrate.js` | (unchanged) |
| `substrates/sync-substrate.js` | (unchanged) |
| `config/redis.js` | (unchanged) |
| `config/supabase.js` | (unchanged) |
| `helpers/credential-cache.js` | (unchanged) |
| `contracts/acquisition-intents.js` | (unchanged) |
| `dedup-kernel/fsm` | (unchanged) |
| `dedup-kernel/index` | (unchanged) |
| `acquisition-kernel/orchestrator` | (unchanged) |
| `acquisition-kernel/parsing` | `acquisition-kernel/parsing/index.js` (unchanged) |
| `publishing-kernel/orchestrator` | (unchanged) |
| `scheduling-kernel/orchestrator` | (unchanged) |
| `retry-cadence-kernel/index` | (unchanged) |
| `retry-cadence-kernel/fsm` | (unchanged) |
| `reconciliation-kernel/fsm` | (unchanged) |
| `reconciliation-kernel/orphan-message-repair` | (unchanged) |
| `postgres-telemetry-kernel/writers` | `postgres-telemetry-kernel/writers/index.js` (unchanged) |
| `postgres-telemetry-kernel/readers` | `postgres-telemetry-kernel/readers/index.js` (unchanged) |
| `postgres-telemetry-kernel/cognition-scanner` | (unchanged) |
| `postgres-telemetry-kernel/fsm` | (unchanged) |
| `telemetry-kernel/fsm` | (unchanged) |
| `telemetry-kernel` | `telemetry-kernel/index.js` (unchanged) |
| `telemetry-kernel/substrates/projection` | `telemetry-kernel/substrates/projection/index.js` (unchanged) |

**Missing on disk (and not used by tests):**
- `routes/agents/{ugc,engagement,publishing,analytics,queue}.js` — intentionally
  absent per contract.

**Stale on disk (not used by tests, not used by production):**
- `substrates/realtime.js`, `telemetry.js`, `quota.js`, `retry.js`,
  `rate-limiter/`, `transport/` — pre-kernel residuals, dead code.
- `control-plane/mutation-substrate.js` — referenced only by the old contract
  and by `tests/event-injector.js` history; do not recreate.

---

## 6. Summary of the 15 Broken Refs

1. `tests/phase-1a-observability-contracts.test.js` — `telemetry-workers/index.js`
2. `tests/phase-1c-chaos-stress.test.js` — `telemetry-workers/index.js`
3. `tests/phase-1c-replay-repair.test.js` — `domains/publishing-fsm.js`
4. `tests/phase-2a-lineage-accumulation.test.js` — `telemetry-workers/index.js`
5. `tests/phase-2b-reconciliation-drift.test.js` — `telemetry-workers/index.js`
6. `tests/phase-2c-long-run-endurance.test.js` — `telemetry-workers/index.js`
7. `tests/phase-2d-redis-durability.test.js` — `telemetry-workers/index.js`
8. `tests/phase-3a-mixed-domain-concurrency.test.js` — `telemetry-workers/index.js`
9. `tests/phase-3b-membrane-boundary-integrity.test.js` — `telemetry-workers/index.js`
10. `tests/phase-3c-cross-domain-reconciliation-isolation.test.js` — `telemetry-workers/index.js`
11. `tests/phase-3d-unified-projection-determinism.test.js` — `telemetry-workers/index.js`
12. `tests/phase-4a-projection-ownership-integrity.test.js` — `telemetry-workers/` directory glob
13. `tests/phase-4b-relay-lineage-immutability.test.js` — `telemetry-workers/index.js`
14. `tests/phase-4c-cross-domain-pressure-stability.test.js` — `telemetry-workers/index.js`
15. `tests/phase-4d-restart-recovery-determinism.test.js` — `telemetry-workers/index.js`
16. `tests/phase-4e-replay-reconstruction.test.js` — `telemetry-workers/index.js`
17. `tests/phase-4h-consumer-pressure.test.js` — `telemetry-workers/index.js`
18. `tests/phase-4j-telemetry-isolation-pressure.test.js` — `telemetry-workers/index.js`
19. `tests/helpers/runtime-simulator.js` — 11 broken refs (telemetry-workers index, reconciliation-engine, runtime/lifecycle, runtime/cadence, 6× domains/*-fsm) plus 2 missing domain FSMs in the registration list

**The vast majority (18 of 20 sites) collapse to a single fix:** point
`telemetry-workers/index.js` consumers at `telemetry-kernel/index.js` (or
introduce a shim). The other 2 are path globs inside specific test files.

---

## 7. Invariants to Preserve (test parameter sanity check)

The refactor must not change any of these — they are the *optimised* test
parameters that stay intact:

- Phase 1 TICK_INTERVAL_MS: 100ms (event-injector cadence)
- Phase 2 TICK: 80–120ms; SOAK: 180s default
- Phase 3 wave counts: 24 / 18 / 30 / (variable)
- Phase 4b/c/d/e/j TICK: 100ms; consumer pool: 35–40 workers
- Phase 4N SOAK_MS: 1,800,000 (30 min)
- Phase 5 SOAK_MS: 3,600,000 (1 hr) for 5D; recon interval: 60s
- Phase 6 SOAK_MS: 2,700,000 (45 min); coordination interval: 30s
- vitest testTimeout: 3,800,000 (5D needs 1hr + 2min buffer)
- pool: 'forks', singleFork: true (prevents cross-test Redis contamination)
- setupFiles: per-file `test:*` Redis flush
- globalSetup: one-time `test:*` flush + wait-for-ready

All of these are runtime-load parameters, not code-structure parameters. The
refactor does not touch them.

---

## 8. Constitutional Law Coverage (L1–L7)

The test suite covers the 7 constitutional laws exhaustively. **None of this
coverage is impacted by the import-path refactor.** Verified by inspecting the
test descriptions and helper functions:

| Law | Test files | Helper |
|-----|-----------|--------|
| L1: Projection convergence from lineage | 4E, 4L, 4M, 5A, 5D, 6 | `deterministicEntryHash` |
| L2: No timestamp regression + causal chain | 4F, 4H, 4I, 5A, 5D, 6 | `assertNoTimestampRegression`, `assertCausalChainIntegrity` |
| L3: No cross-domain membrane bypass | 4G, 4I, 4N, 5A, 5D, 6 | (membrane attack + adversarial injection) |
| L4: Monotonic cursors | 4F, 4H, 5A, 5D, 6 | `assertMonotonicCursors` |
| L5: Idempotent replay | 4F, 4I, 4N, 5D, 6 | `assertIdempotentReplay` |
| L6: Stale entries flagged | 4F, 4I, 4N, 5D, 6 | `assertStaleEntriesFlagged` |
| L7: Signal ownership contract | 5A, 5D, 6 | (signal-ownership contract test in phase 6) |

The helpers in `tests/helpers/constitutional-invariants.js` (18.9KB) implement
all of these. The refactor does not modify them.

---

## 9. Closing Notes

The test suite is the canary in the coal mine for the kernel decomposition. The
production wiring (`server.js` → `control-plane/orchastrator.js`) is correct and
runs the full 17-step boot. The tests were authored against the pre-kernel
flat structure and never refactored. The fix is a *path-only* refactor —
mechanical, scoped, and reversible. The constitutional invariants and test
parameters are intact and must stay intact.

Recommended sequencing for the refactor (to be done in a separate session —
this audit does not modify code):

1. Add the shim at `control-plane/telemetry-workers/index.js` (or do the bulk
   import swap in 16 Tier-A files). One of these is sufficient.
2. Update `tests/helpers/runtime-simulator.js`: 11 import paths + add
   `graph-capability-kernel/fsm` and `postgres-telemetry-kernel/fsm` to the
   domain FSM list.
3. Update `tests/phase-1c-replay-repair.test.js` and
   `tests/phase-4a-projection-ownership-integrity.test.js` to use kernel paths.
4. Run Phase 1–3 to validate the Tier-A path swap.
5. Run Phase 5a to validate the Tier-B simulator refactor.
6. Run Phase 5b, 5c, 5d, 6, 6a.
7. Run the soaks (4N, 5D, 6) only after all of the above pass.
