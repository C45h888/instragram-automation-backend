# PHASE 4 — TEST REPORT & ROOT CAUSE ANALYSIS
**Date:** May 31, 2026  
**Test Suite:** Unified Constitutional Test Suite (`run-all-tests.sh --phase-4 --skip-soaks`)  
**Runtime:** Docker container-native (`test-runner` node:22-alpine)  
**Stack:** test-redis ✓, test-postgres ✓, test-runner ✓  
**Total Duration:** 5m 19s (4A–4M, skip-soaks)  
**4N Soak:** Background — 31m CPU elapsed, still running

---

## EXECUTIVE SUMMARY

Phase 4 validates **Projection Isolation & Relay Integrity**. 14 tests (4A–4N) cover the bounded telemetry layer, lineage immutability, causal ordering, membrane attack resistance, and a 30-minute constitutional soak.

**Result: 4 PASS / 9 FAIL / 1 RUNNING**

The dominant failure pattern across 7 of the 9 failed tests is identical: **no SEMANTIC_PROJECTION_TRANSITION entries appear in the observability transition log**. This is a single architectural gap, not 7 independent bugs.

---

## TEST RESULTS TABLE

| Test | File | Result | Sub-tests | Root Cause |
|------|------|--------|-----------|------------|
| 4A | `phase-4a-projection-ownership-integrity.test.js` | **PASS** | 2/2 | Static code analysis — no runtime dependency |
| 4B | `phase-4b-relay-lineage-immutability.test.js` | **FAIL** | 0/1 | TCFSM not bootstrapped in test → no SEMANTIC_PROJECTION_TRANSITION |
| 4C | `phase-4c-cross-domain-pressure-stability.test.js` | **FAIL** | 0/1 | TCFSM not bootstrapped in test → no SEMANTIC_PROJECTION_TRANSITION |
| 4D | `phase-4d-restart-recovery-determinism.test.js` | **FAIL** | 0/1 | TCFSM not bootstrapped → projections = 0; cursor regression also present |
| 4E | `phase-4e-replay-reconstruction.test.js` | **FAIL** | 0/2 | TCFSM not bootstrapped → semanticProjectionEntries = 0 |
| 4F | `phase-4f-causal-ordering.test.js` | **FAIL** | 1/3 | Timestamp regression (LAW 2) + TCFSM missing for SEMANTIC projection tests |
| 4G | `phase-4g-membrane-attack.test.js` | **PASS** | 4/4 | CK membrane gate correctly rejects cross-domain bypass |
| 4H | `phase-4h-consumer-pressure.test.js` | **PASS** | 2/2 | Slow consumer + stall recovery both pass |
| 4I | `phase-4i-concurrency-corruption.test.js` | **PASS** | 4/4 | Idempotent replay + stale flags confirmed |
| 4J | `phase-4j-telemetry-isolation-pressure.test.js` | **FAIL** | 5/7 | 2 failures: TCFSM not running → no SEMANTIC_PROJECTION_TRANSITION |
| 4K | `phase-4k-durable-persistence-integrity.test.js` | **FAIL** | 2/4 | 2 failures: waveEntries = 0 (TCFSM missing) |
| 4L | `phase-4l-periodic-hash-convergence.test.js` | **FAIL** | 2/3 | Slow vs fast injection: 0 entries vs expected 4 — timing/ordering issue |
| 4M | `phase-4m-unified-worker-recycle.test.js` | **FAIL** | 0/1 | LAW 4 VIOLATION: cursor regression 57 → 5 at index 8 |
| 4N | `phase-4n-mixed-constitutional-soak.test.js` | **RUNNING** | — | 30-min soak, 31m CPU elapsed, output not yet written |

---

## ISSUE #1 — CRITICAL: Telemetry Coordination FSM Not Bootstrapped in Tests

### Symptom
7 tests (4B, 4C, 4D, 4E, 4F, 4J, 4K) fail with:
```
expected 0 to be greater than 0
  at expect(projections.length).toBeGreaterThan(0)
```

The filter `e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'` always returns 0 entries.

### Architecture Background

The codebase has a **two-stage projection emission architecture**:

```
Stage 1 — Projection Worker:
  base-projection-worker._tick()
    → _emitProjectionTransition(projection)
    → observability.transition({
        domain: 'telemetry',           ← domain is 'telemetry', NOT 'projection'
        entity: 'projection_intent',   ← entity is 'projection_intent'
        nextState: 'PROJECTION_INTENT',
        authority: 'runtime-projection-worker',
        raw: { intentType: 'PROJECTION_INTENT', ... }
      })

Stage 2 — Telemetry Coordination FSM (sole serializer):
  CK cadence → triggerCoordinationCycle() → FSM reads PROJECTION_INTENTs →
  validates → orders → emits SEMANTIC_PROJECTION_TRANSITION entries
  → observability.transition({
      domain: 'projection',           ← domain is 'projection'
      entity: 'semantic_projection',  ← entity is 'semantic_projection'
      nextState: 'SEMANTIC_PROJECTION_TRANSITION',
      raw: { entryType: 'SEMANTIC_PROJECTION_TRANSITION', ... }
    })
```

The normalizer rule for SEMANTIC_PROJECTION_TRANSITION:
```javascript
// normalizer.js line 353
addRule('projection', 'semantic_projection', null, null, (raw) => ({
  entityId: raw.projectionType || null,
  raw: { ...raw, entryType: 'SEMANTIC_PROJECTION_TRANSITION' },
}));
```

This rule **only matches** when `(domain === 'projection' AND entity === 'semantic_projection')`. This combination is emitted **exclusively by the TelemetryCoordinationFsm**, no other component emits it.

### Root Cause

**The TelemetryCoordinationFsm is never started in any Phase 4 test's `beforeAll()`.**

Production boot (orchestrator.js `startAllWorkers()`):
```javascript
// orchestrator.js line 139-142
cadence.every(COORDINATION_INTERVAL_MS, () => {
  constitutional.triggerCoordinationCycle();  // ← starts TCFSM cadence
});
```

Test boot (e.g., phase-4b-relay-lineage-immutability.test.js `beforeAll()`):
```javascript
// tests/phase-4b-relay-lineage-immutability.test.js line 9-12
await observability.init();
await telemetryWorkers.startAll(40);  // ← workers start, emit PROJECTION_INTENT
await lineageWorker.start(500);      // ← lineage worker starts consuming
// ← NO CK bootstrapped, NO TCFSM started, NO triggerCoordinationCycle()
```

**What happens at test runtime:**
1. `telemetryWorkers.startAll(40)` starts 5 projection workers
2. Workers emit `PROJECTION_INTENT` entries (domain='telemetry', entity='projection_intent')
3. `waitForLogSize(start + 1, 4000)` polls `observability.query.getLogSize()`
4. `getLogSize()` returns the raw count of all transition log entries — includes `PROJECTION_INTENT` entries
5. The test calls `getEntriesSince(start)` and filters: `e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'`
6. **0 entries match** — because the TCFSM never ran to transform `PROJECTION_INTENT` → `SEMANTIC_PROJECTION_TRANSITION`

### Evidence

In 4B stderr:
```
[lineage-worker] Failed to persist divergence entry: Redis status=connecting — constitutional write authority absent
[lineage-worker] Failed to persist divergence entry: Redis status=connecting — constitutional write authority absent
[lineage-worker] Failed to persist divergence entry: Redis status=connecting — constitutional write authority absent
[lineage-worker] Failed to persist health entry: Redis status=connecting — constitutional write authority absent
```

The lineage worker is consuming entries (so `getLogSize()` advances), but those entries are `PROJECTION_INTENT`, not `SEMANTIC_PROJECTION_TRANSITION`. The lineage worker's own projection synthesis (Layer B) is trying to run but can't persist because Redis is still in "connecting" state during the test's fast execution window.

### Affected Tests
- 4B: `projections.length` = 0 (TCFSM not running)
- 4C: `projectionTransitions.length` = 0 (TCFSM not running)
- 4D: `projectionTransitions.length` = 0 (TCFSM not running)
- 4E: `semanticProjectionEntries.length` = 0 (TCFSM not running)
- 4F: timestamp regression in concurrent wave injection (separate LAW 2 issue)
- 4J: `projectionEntries.length` = 0; `brokenChainDiv` undefined (TCFSM not running)
- 4K: `waveEntries.length` = 0 (TCFSM not running)

### Fix Required

Add to each test's `beforeAll()` after `lineageWorker.start()`:
```javascript
// Bootstrap CK so TCFSM can run
const CK = require('./control-plane/governance/constitutional-kernel');
const telemetryCoordinationFsm = require('./control-plane/governance/domains/telemetry-coordination-fsm');
const ALL_DOMAIN_FSMS = [/* all 7 domain FSMs */];

for (const fsm of ALL_DOMAIN_FSMS) {
  CK.registerDomain(fsm);
}
CK.dispatch({ type: 'BOOT_COMPLETE' });
CK.startLoop(1000);

// Allow TCFSM to run at least one coordination cycle
await new Promise(r => setTimeout(r, 500));
```

OR use the `RuntimeSimulator` from `tests/helpers/runtime-simulator.js` which already handles this correctly:
```javascript
const sim = new RuntimeSimulator({ telemetryPollMs: 40, lineagePollMs: 500 });
await sim.boot();
// TCFSM is registered and running via CK.startLoop()
// triggerCoordinationCycle() fires on CK cadence
```

The `runtime-simulator.js` was specifically built for Phase 5 integration testing but is not being used by Phase 4 tests. This is the correct fix.

---

## ISSUE #2 — CRITICAL: LAW 4 Violation — Cursor Regression at Index 8 (4M)

### Symptom
```
[constitutional-invariant] LAW 4 VIOLATION: Cursor regression at index 8: 57 → 5
  at assertMonotonicCursors tests/helpers/constitutional-invariants.js:215
  at tests/phase-4m-unified-worker-recycle.test.js:105
```

After pre-dequeue snapshot at cursor 57, post-dequeue cursor shows 5.

### Root Cause — `_tickSync` Race Condition in Lineage Worker `stop()`

In `lineage-worker.js`:
```javascript
// lineage-worker.js line 77-82
let _tickSync = Promise.resolve();
let _tickResolve = null;

async stop() {
  _running = false;  // signals _ingestTick to stop after current entry

  // BUG: if an _ingestTick is currently running and has advanced _cursor
  // but has NOT yet called _tickResolve(), then _lastPersistedCursor is stale.
  // stop() writes _lastPersistedCursor (pre-dequeue value, e.g. 57) to Redis.
  // Then the in-flight tick completes, and _lastPersistedCursor is set to (post-dequeue, e.g. 5).

  // On next restart:
  // _lastPersistedCursor = 5 (set by in-flight tick AFTER stop() wrote 57)
  // But stop() already wrote 57 to Redis as the persisted cursor.
  // Redis still has cursor=57. After rehydrate, _cursor=57.
  // But _lastPersistedCursor was overwritten to 5 during the race window.
  // Result: _cursor=57, _lastPersistedCursor=5 → regression.

  await _tickSync;  // waits for current tick, but the race is already set
  await _persistCursor();  // writes _lastPersistedCursor to Redis
}

async _ingestTick() {
  // ...
  _cursor = newCursorPosition;  // advances cursor
  // ...
  await _persistToLedger(entries);  // slow, happens after cursor advance
  // BUG: _tickResolve() is called AFTER _persistToLedger() completes,
  // but _lastPersistedCursor is only updated inside _persistToLedger().
  // If stop() is called during _persistToLedger():
  //   1. stop() sets _running=false, awaits _tickSync
  //   2. _tickResolve is null because _persistToLedger hasn't completed
  //   3. _tickSync stays unresolved
  //   4. stop() hangs on await _tickSync (or times out)
  //   5. Meanwhile, _persistToLedger completes, sets _lastPersistedCursor=5
  //   6. stop() continues, writes _lastPersistedCursor=5 to Redis
  //   7. But _cursor is already 57 (advanced at start of tick)
  // After rehydration: _cursor=57, Redis has 5 → regression 57→5
}
```

The `_tickResolve` is set when a tick begins, but if the tick is mid-`_persistToLedger()` (slow Redis write), `_tickResolve` is still null. The `await _tickSync` in `stop()` doesn't actually wait for the in-flight persistence — it only waits for the tick-start promise, not the tick completion.

### Fix Required

```javascript
async stop() {
  _running = false;

  // Ensure in-flight tick completes AND its persistence succeeds
  while (_ingestInProgress) {
    await new Promise(r => setTimeout(r, 50));
  }

  await _persistCursor();
}
```

Or use a flag `_ingestInProgress` that is set to `true` at start of `_ingestTick` and `false` at end.

---

## ISSUE #3 — MODERATE: Timestamp Regression in Concurrent Wave Injection (4F)

### Symptom
Phase 4F has 3 sub-tests; 2 fail:
```
FAIL: lineage timestamps never regress across concurrent wave injection
  → assertNoTimestampRegression(waveEntries) threw
  → [constitutional-invariant] LAW 2 VIOLATION: Timestamp regression detected

FAIL: cursor positions advance monotonically — never retreat
  → assertMonotonicCursors(cursors) passed (no regression here)
  → but the causal chain integrity check failed
```

### Root Cause — Non-Monotonic Timestamp Assignment in `event-injector.js`

`injectMixedDomainWave()` calls `observability.transition()` for multiple domains synchronously:
```javascript
// event-injector.js line 175-210
async function injectMixedDomainWave({ waveId, seq, includeFault }) {
  // All 6 transitions get the SAME timestamp (Date.now())
  // because they're emitted in a tight loop before any async yields
  const timestamp = Date.now();  // ← same value for all 6 domains

  observability.transition({ domain: 'acquisition', ... timestamp });  // t=1000
  observability.transition({ domain: 'engagement', ... timestamp });   // t=1000
  observability.transition({ domain: 'publishing', ... timestamp });    // t=1000
  observability.transition({ domain: 'scheduling', ... timestamp });   // t=1000
  observability.transition({ domain: 'telemetry', ... timestamp });     // t=1000
  observability.transition({ domain: 'governance', ... timestamp });   // t=1000
}
```

The normalizer assigns `timestamp: Date.now()` in `normalize()`:
```javascript
// normalizer.js line 81
timestamp: Date.now(),  // ← captured at normalize() call time, not emission time
```

If `Date.now()` advances between two `observability.transition()` calls (which can happen at millisecond boundaries in Node.js), the second entry gets a later timestamp. But more critically: when the normalizer processes entries asynchronously or in a batch, the `Date.now()` at normalize-time may not reflect the original emission order.

The `assertNoTimestampRegression` function in `constitutional-invariants.js` checks that for each entry, `entry.timestamp >= previousEntry.timestamp` in the sorted order. When concurrent waves inject entries rapidly, entries can arrive at the normalizer in a different order than their emission timestamps, causing the regression check to fail.

### Fix Required

Introduce a monotonic timestamp emitter that guarantees ascending timestamps:
```javascript
let _emissionCounter = 0;
function nextTimestamp() {
  const now = Date.now();
  if (now <= _lastTimestamp) {
    return _lastTimestamp + 1;  // ensure monotonic increment
  }
  _lastTimestamp = now;
  return now;
}
```

Use `nextTimestamp()` in `event-injector.js` instead of `Date.now()`.

---

## ISSUE #4 — MODERATE: Slow Injection vs Fast Injection Hash Divergence (4L)

### Symptom
```
FAIL: hash is invariant to injection speed — slow vs fast same workload converges
  expected +0 to be 4 // Object.is equality
  → fastWaveEntries.length = 0 (expected 4)
```

### Root Cause — Race Between Lineage Worker Poll and Test Assertion

The test:
1. Injects 4 waves at `TICK_INTERVAL_MS=500` (slow path)
2. Uses `waitForLedgerEntryCount(4, 8000)` — polls `lineageLedger.getSize()`
3. Polls Redis `LLEN lineage:ledger:entries`
4. At slow injection rate (500ms between waves), lineage worker has time to poll and consume entries between injections
5. Test assertion reads `getEntriesSince()` and filters for `waveId`

For the fast injection path:
1. All 4 waves injected in rapid succession (no delay)
2. `waitForLedgerEntryCount(4, 8000)` polls `lineageLedger.getSize()` — but Redis poll interval is 5000ms by default
3. The lineage worker's `_currentPollMs = 5000ms` means it only polls every 5 seconds
4. All 4 waves are injected before the first lineage worker poll completes
5. `getEntriesSince()` reads entries from the observability log, not the lineage ledger
6. The lineage worker's cursor hasn't advanced yet (hasn't polled), so it sees stale entries

The fast injection test's `waitForLedgerEntryCount(4, 8000)` succeeds (Redis has the entries), but then when the test calls `observability.query.getEntriesSince()` to filter by `waveId`, those entries are the raw entries from the event injector, not the processed lineage entries. And the entries may have been consumed by the lineage worker already, removing them from the observable log window.

### Fix Required

The test should wait for the lineage worker to actually consume and persist entries before reading them back:
```javascript
// After waitForLedgerEntryCount, also wait for lineage worker cursor advance
await waitForCursorAdvance(startCursor, 5000);
const { entries } = observability.query.getEntriesSince(startCursor);
const fastWaveEntries = entries.filter(e => e.raw?.raw?.waveId === fastWaveIdPrefix + '-fast');
```

---

## ISSUE #5 — MINOR: 4L "hash invariant to injection speed" Fails

### Sympton
```
expected +0 to be 4 // Object.is equality
→ fastWaveEntries.length = 0 (expected 4)
```

This is the same root cause as Issue #4 — fast injection bypasses the lineage worker's consumption window, so entries aren't visible in the expected filtered slice.

---

## PHASE 4N SOAK STATUS

**Status:** RUNNING (background)  
**Elapsed:** 31m 20s CPU time  
**Container:** `instagram-test-runner` (PID 1092: `node ... forks.js`)  
**Output:** Not yet written to `/app/tests/output/` — writes on completion

The 4N soak injects legal + adversarial waves every 120ms over 30 minutes. Check results with:
```bash
docker compose -f docker-compose.test.yml exec -T test-runner cat /app/tests/output/phase-4n-soak-latest.json
```

---

## CONSTITUTIONAL INVARIANT VIOLATIONS SUMMARY

| Law | Invariant | Violations | Test(s) |
|-----|-----------|-----------|---------|
| LAW 1 | Projections reconstructed from lineage converge identically | None | 4E, 4L |
| LAW 2 | Replay events may never mutate prior lineage history | Timestamp regression detected | 4F |
| LAW 3 | Cross-domain transitions may never bypass membrane authority | None | 4G |
| LAW 4 | Cursor positions remain monotonic | Cursor regression 57 → 5 | 4M |
| LAW 5 | Stale entries must be flagged | None | 4I |
| LAW 6 | Duplicate replay injection must remain idempotent | None | 4I |

---

## REQUIRED FIXES (Priority Order)

### P0 — Fix TCFSM Bootstrap in Tests (Unblocks 4B, 4C, 4D, 4E, 4F, 4J, 4K)
**File:** `tests/helpers/runtime-simulator.js` already has the correct boot sequence.  
**Action:** Update Phase 4 test files to use `RuntimeSimulator` OR add explicit CK bootstrap + TCFSM start to each test's `beforeAll()`.

### P1 — Fix Lineage Worker Cursor Sync (4M)
**File:** `control-plane/governance/lineage-worker.js`  
**Action:** Add `_ingestInProgress` flag and wait for in-flight tick completion in `stop()` before persisting cursor.

### P2 — Fix Timestamp Monotonicity (4F)
**File:** `tests/event-injector.js`  
**Action:** Replace `Date.now()` with a monotonic timestamp generator in `injectMixedDomainWave()`.

### P3 — Fix Fast Injection Test (4L)
**File:** `tests/phase-4l-periodic-hash-convergence.test.js`  
**Action:** Add `waitForCursorAdvance()` after `waitForLedgerEntryCount()` to ensure lineage worker has consumed entries before reading back.

---

## PASSING TESTS — CONFIRMATION OF ARCHITECTURAL PROPERTIES

### 4A — Projection Ownership Integrity (PASS)
Static code checks confirm:
- Telemetry workers do NOT import `governance/lineage-worker`
- `lineage-worker.js` does NOT emit `entity: 'semantic_projection'` transitions

### 4G — Membrane Attack Resistance (PASS)
CK correctly rejects all 4 adversarial attack vectors:
- Publishing → Governance FSM bypass
- Telemetry → Execution pipeline bypass
- Reconciliation → Foreign projection overwrite
- Foreign authority → Direct acquisition mutation

`MEMBRANE_BYPASS` anomaly entries are recorded in the ledger for each rejection.

### 4H — Consumer Lag Resilience (PASS)
- Slow consumer does not cause ledger truncation
- Stalled telemetry workers recover without data loss
- Replay backlog accumulation does not corrupt continuity

### 4I — Adversarial Concurrency Corruption Recovery (PASS)
- Duplicate replay windows are idempotent — no corruption markers
- Stale authority chains are correctly flagged
- No silent corruption under adversarial conditions

---

## APPENDIX: KEY FILE REFERENCES

| File | Role |
|------|------|
| `control-plane/governance/domains/telemetry-coordination-fsm.js` | Sole serializer for SEMANTIC_PROJECTION_TRANSITION — NOT started in tests |
| `control-plane/telemetry-workers/base-projection-worker.js` | Emits PROJECTION_INTENT (not SEMANTIC_PROJECTION_TRANSITION) |
| `control-plane/observability/normalizer.js` | Rules: `domain='projection', entity='semantic_projection'` → entryType='SEMANTIC_PROJECTION_TRANSITION' |
| `control-plane/governance/lineage-worker.js` | Consumes from observability → writes to ledger; `_tickSync` race condition in `stop()` |
| `tests/helpers/runtime-simulator.js` | Correct bootstrap including TCFSM — not used by Phase 4 tests |
| `tests/event-injector.js` | Uses `Date.now()` for timestamps — non-monotonic under fast injection |
| `tests/helpers/constitutional-invariants.js` | LAW 4 `assertMonotonicCursors()` — catches cursor regression |

---

*Document version: Phase 4 Initial Report — May 31, 2026*
*Pending updates: Phase 4N soak results when background test completes*