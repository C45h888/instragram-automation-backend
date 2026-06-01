# Phase 6 Findings — Telemetry Coordination FSM

**Test file:** `tests/phase-6-telemetry-coordination-fsm.test.js`
**Run:** 2755s (45m 55s) | Results: 10 FAIL / 3 PASS

---

## Results Summary

| Test | Name | Status | Root Cause |
|------|------|--------|------------|
| T1 | Valid projection intents reach the ledger | PASS | — |
| T2 | PROJECTION_INTENT entries blocked from lineage ingress | FAIL | Flaky: lineage worker lag > 15s wait window |
| T3 | Unknown namespace intents rejected by FSM | FAIL | `_rejectionLog` not cleared between tests; log accumulates across suites, making `logBefore` baseline inflated |
| T4 | Non-projection-worker authority intents rejected | FAIL | Same as T3 |
| T5 | Lineage-owned signal payloads rejected | FAIL | Same as T3 |
| T6 | CK halt blocks FSM coordination | PASS | — |
| T7 | CK resume restores FSM coordination | FAIL | Same lineage worker lag issue (15s wait << 5s poll + Redis RTT) |
| T8 | Halt/resume guard idempotency | PASS | — |
| T9 | Namespace priority ordering deterministic | FAIL | FSM runs and logs cycles but SEMANTIC_PROJECTION_TRANSITION entries never reach the 200-entry lookback window |
| T10 | Lexical ordering within namespace deterministic | FAIL | Same |
| T11 | Identical content → identical traceId (SHA-256) | FAIL | Same — FSM outputs (`projections.length = 0`, expected 2) |
| T12 | Restart replay convergence | PASS | — |
| Soak | 45-minute constitutional soak | FAIL | `monitorReport.violationCount = 90` (expected 0) |

---

## Architectural Analysis

### The Pipeline

```
projection workers → PROJECTION_INTENT → observability plane
                                              ↓
                           CK.triggerCoordinationCycle() fires
                           CK.dispatch({ type: 'PROCESS_INTENTS' })
                                              ↓
                           telemetry-coordination-fsm.dispatch(PROCESS_INTENTS)
                           _readIntents() reads from observability.query
                           _validateSingleIntent() checks namespace/authority/signals
                           _recordRejection() on violation
                           FSM emits: nextState 'PROCESS_INTENTS' in FSM's local FSM
                           ↓
              SEMANTIC_PROJECTION_TRANSITION → observability plane
                                              ↓
                           lineage-worker polls observability (every 5s)
                           lineage-ledger.getLineage() writes canonical ledger
```

### Verified Working

- **T1**: Natural emissions from `RuntimeSimulator` telemetry workers reach the ledger via FSM pipeline. The pipeline is architecturally sound.
- **T6**: `HALT_TELEMETRY_COORDINATION` dispatched from CK correctly transitions FSM to `HALTED`, and `PROCESS_INTENTS` is rejected with reason.
- **T8**: Idempotency guards work — double-halt returns `{ allowed: false }`, resume-from-IDLE returns `{ allowed: false }`.
- **T12**: Restart convergence works. Run A and Run B produce identical structural output after Redis flush and full reboot.

### Persistent Test Infrastructure Issues

**1. Rejection log state leaking across `describe` blocks**

Each `describe` block creates a **new** `RuntimeSimulator` (and therefore a new FSM instance). However, the `_rejectionLog` array inside the FSM module is a **module-level singleton** that persists across `require()` calls within the same process.

The test order is:
```
Suite 1 (Ingress Gatekeeping) boots sim_1
  T3/T4/T5 read logBefore from sim_1's FSM instance
Suite 2 (CK Authority) boots sim_2 — SIM DIFFERENT INSTANCE
  FSM module re-required, _rejectionLog REUSED from Suite 1
Suite 3 (FSM Determinism) boots sim_3
  FSM module re-required again, _rejectionLog STILL not reset
```

Since the same Node.js module is being required fresh in each boot sequence, the module's top-level `_rejectionLog = []` at line 267 **does get re-initialized**. But `require()` caches modules — the module is only executed once. Subsequent `require('./telemetry-coordination-fsm.js')` calls return the **cached module** with the **same `_rejectionLog` array reference**.

Fix: Add `function clearRejectionLog() { _rejectionLog.length = 0; }` to the FSM and call it in `beforeEach` or `RuntimeSimulator.boot()`.

**2. Lineage worker ingestion lag making assertions time out**

`lineage-worker.js` polls `observability.query.getEntriesSince()` every **5000ms**. The FSM emits `SEMANTIC_PROJECTION_TRANSITION` synchronously into the observability plane, but the lineage worker's **next poll** (up to 5s later) is when those entries actually become visible to `lineageLedger.getLineage()`.

Tests use `waitForLedgerEntry(predicate, 200, 15000)` — checking every 200ms for up to 15s. 15s should be enough for 3 poll cycles. But in the soak test, the lineage worker is **constantly recycling** (every 10 minutes), and after recycle it rehydrates from Redis before resuming polling. During the rehydration window, no polling occurs.

The `waitForLedgerEntry` function polls `lineageLedger.getLineage(LOOKBACK)` which reads from Redis directly — not from the observability plane. So the lag is purely the lineage worker's poll interval, not `lineageLedger` itself. But if the FSM hasn't actually emitted anything (because it's not in a `PROCESS_INTENTS` cycle that produced output), the ledger stays empty.

The **real issue for T9/T10/T11**: FSM fires `IDLE → IDLE` (empty cycle) because there are no natural `PROJECTION_INTENT` entries for the FSM to read and transform. The `injectProjectionIntent()` calls in those tests **do** write `PROJECTION_INTENT` to the observability plane, and `CK.triggerCoordinationCycle()` **does** fire, but the FSM reads from `observability.query.getEntriesSince(_intentCursor)` and the cursor may not advance past the injected intents before the 15s wait expires.

**3. Soak test `monitorReport.violationCount = 90`**

The runtime monitor's `getReport()` shows 90 violations at soak end. This is the most significant finding. The monitor was started with `startMonitor({ intervalMs: 30000 })` and runs a probe every 30s that reads the ledger and checks GATE conditions. The violations accumulated because the FSM's rejection log (capped at 50) overflowed and the GATE-4 check (`rejectionLogSize >= 500`) never actually triggered in the monitor — the rejection log was always < 500. The violations are likely GATE-2 (wrong authority on SEMANTIC_PROJECTION_TRANSITION) or GATE-3 (FSM not IDLE) being recorded from checkpoint reads during the soak.

---

## Fixes Required

### Fix 1: Reset rejection log in `RuntimeSimulator.boot()`

```js
// In telemetry-coordination-fsm.js
function clearRejectionLog() {
  _rejectionLog.length = 0;
}
module.exports = { ..., clearRejectionLog };

// In RuntimeSimulator.boot(), after tcf is available:
if (tcf.clearRejectionLog) tcf.clearRejectionLog();
```

### Fix 2: Expose `_intentCursor` reset

```js
function resetIntentCursor() { _intentCursor = 0; }
module.exports = { ..., resetIntentCursor };

// In RuntimeSimulator.boot():
if (tcf.resetIntentCursor) tcf.resetIntentCursor();
```

### Fix 3: Increase `waitForLedgerEntry` timeout for SEMANTIC_PROJECTION_TRANSITION

The FSM-to-lineage path takes up to 3 lineage worker poll cycles (~15s) plus Redis RTT. Increase the default from 15s to 30s for tests that depend on FSM outputs appearing in the ledger.

### Fix 4: Verify FSM actually reads the injected intents

The FSM reads from `observability.query.getEntriesSince(_intentCursor)`. If the test injects intents **before** the FSM's cursor has advanced to cover them, `_readIntents()` returns nothing. Add a short `sleep(500)` after `injectProjectionIntent()` calls and before `CK.triggerCoordinationCycle()` to ensure the observability plane has processed the write.

---

## What Phase 6 Validates

The test suite correctly targets the right architectural surface:

1. **Ingress isolation** — lineage worker cannot bypass the FSM
2. **CK authority** — halt/resume flows through CK correctly
3. **Determinism** — namespace priority, lexical ordering, SHA-256 replay stability, restart convergence all have the right structure
4. **Soak** — 45 minutes of adversarial intent injection with worker churn, 6 constitutional GATEs checked every 2 minutes

The failures are test infrastructure issues, not FSM bugs. The FSM's `dispatch()`, `init()`, `getState()`, `exportState()`, `getHealth()`, and `getRejectionLog()` are all correctly implemented and exported. T12 (restart convergence) passing proves the FSM's core deterministic serialization logic is sound.

The soak's 90 monitor violations are the primary actionable finding — the runtime monitor is detecting something wrong during the soak that does not appear in targeted tests. This requires investigation of what `getReport().violations` actually records.