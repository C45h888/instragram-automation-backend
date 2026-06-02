# Reconciliation Subsystem — Phase Decomposition Plan

> Document for agent continuity. Phase 1 (substrate + worker extraction) is complete.
> Another agent should be able to read this and continue from where the previous agent left off.

---

## What Was Done (Phase 1)

### Files Changed

| File | Change |
|------|--------|
| `control-plane/governance/constitutional-kernel.js` | Removed ~140 lines of reconciliation bridge code; `triggerReconciliation()` now calls `reconciliationSubstrate.triggerCycle()` directly |
| `control-plane/governance/reconciliation-substrate.js` | **NEW** — 182 lines; owns snapshot building, substrate queries, checkpoint gate, worker orchestration |
| `control-plane/governance/reconciliation-worker.js` | **NEW** — 52 lines; execution-blind unit, receives `{ entries, fsms, substrates }` data only |
| `control-plane/governance/reconciliation-engine.js` | Doc comment updated only; logic unchanged; cognitive blindness preserved |

### What Was Removed From CK

```
_registerReconciliationTrigger()          → trigger criteria moved to FSM (FSM already has RECONCILIATION_TICK in DOMAIN_EVENT_MAP)
bridge subscriber (subscribeAction 'RECONCILIATION_CYCLE_STARTED') → moved to substrate
_buildSubstrateQueries()                  → moved to substrate
_canCheckpoint()                          → moved to substrate
_reconPromise* state                      → eliminated (substrate returns directly)
_reconciliationSnapshot                   → substrate captures T0 snapshot
MIN_RECON_INTERVAL_MS + _lastReconTriggeredAt → CK keeps these; used in triggerReconciliation() anti-thrash gate
```

### CK Line Count

| Before | After |
|--------|-------|
| 1486 | 1349 |

### Architecture After Phase 1

```
CK (HSM — ~1349 lines)
├── dispatch() — routing
├── validateDomainTransition() — membrane authority
├── registerDomain() — domain registry
├── triggerReconciliation() — calls substrate.triggerCycle()
├── _detectConstitutionalDeath() + _triggerConstitutionalDeath()
├── rehydrate() + startLoop() + tick()
├── subscribeAction() / onAction()
└── OBSERVABILITY: status(), getState(), getLineage()

Reconciliation FSM (owns trigger criteria)
├── evaluates: IDLE + MIN_INTERVAL + no drift
├── dispatches: RECONCILIATION_TICK → CK
└── transitions: IDLE → RECONCILIATING → CONVERGENT/DRIFTED → IDLE

Reconciliation Substrate (182 lines — owns authority artifacts)
├── triggerCycle({ fsms, currentState })
├── snapshot building (lineageLedger.getLineageWithHash())
├── _buildSubstrateQueries() (dedup, retry, metrics, cadence, buffer)
├── canCheckpoint({ fsms, currentState }) — G1-G5 gates
├── checkpointer.createSnapshot()
└── returns: { observations, worstSeverity, hash, snapshotHash }

Reconciliation Worker (52 lines — execution-blind)
├── run({ entries, fsms, substrates })
├── calls: engine.compare()
└── returns: { observations, worstSeverity, hash }

Reconciliation Engine (656 lines — cognitively blind, unchanged)
├── compare({ fsms, substrates, lineageLedger, snapshotEntries })
└── deterministic comparison primitive only

lineage-ledger.js, lineage-checkpointer.js (unchanged)
```

### Data Flow After Phase 1

```
CK.triggerReconciliation()
  ├── reentrancy guard
  ├── death detection
  ├── FSM IDLE check
  ├── anti-thrash gate (MIN_RECON_INTERVAL_MS)
  ├── dispatch RECONCILIATION_TICK → FSM transitions IDLE → RECONCILING
  └── reconciliationSubstrate.triggerCycle({ fsms, currentState })
        ├── lineageLedger.getLineageWithHash() → { entries, hash }
        ├── _buildSubstrateQueries() → { dedupIsInFlight, retryInFlight, ... }
        ├── worker.run({ entries, fsms, substrates })
        │     └── engine.compare() — cognitively blind
        ├── canCheckpoint() evaluation (G1-G5)
        ├── checkpointer.createSnapshot() if all gates pass
        └── return { observations, worstSeverity, hash, snapshotHash }
  ├── dispatch RECONCILIATION_RESULTS_RECEIVED → FSM transitions RECONCILING → CONVERGENT/DRIFTED
  ├── dispatch RECONCILIATION_CYCLE_COMPLETE → FSM transitions → IDLE
  └── return result
```

---

## What Remains

### Phase 2: FSM Trigger Criteria Extraction

**Current state:** `MIN_RECON_INTERVAL_MS` and `_lastReconTriggeredAt` live in CK (`constitutional-kernel.js` lines 565-566). The anti-thrash gate is evaluated in `triggerReconciliation()`.

**Problem:** The trigger criteria evaluation (IDLE check + MIN_INTERVAL + no drift) is split. Part lives in CK (`triggerReconciliation()`), part is supposed to live in the reconciliation FSM. The FSM already has `RECONCILIATION_TICK` in `DOMAIN_EVENT_MAP`, but the actual MIN_INTERVAL check and drift evaluation happen in CK before dispatching.

**Goal:** Move the deterministic trigger criteria entirely into `reconciliation-fsm.js`. CK's `triggerReconciliation()` becomes a pure pass-through — it dispatches `RECONCILIATION_TICK` to the FSM, and the FSM itself evaluates whether to accept it based on its own state + criteria.

**What changes:**
- CK removes: MIN_RECON_INTERVAL_MS, _lastReconTriggeredAt, the anti-thrash gate check in `triggerReconciliation()`
- CK's `triggerReconciliation()` becomes: reentrancy guard → dispatch TICK to FSM → return promise
- FSM adds: `lastReconTriggeredAt` state + `MIN_RECON_INTERVAL_MS` constant; `RECONCILIATION_TICK` guard evaluates IDLE + MIN_INTERVAL + no active drift
- The `_detectConstitutionalDeath()` and FSM IDLE check remain in CK (CK owns death detection, not trigger criteria)

**File changes:** `constitutional-kernel.js` (trigger criteria removed), `domains/reconciliation-fsm.js` (trigger criteria added)

---

### Phase 3: Orchestrator Integration

**Current state:** Orchestrator calls `CK.triggerReconciliation()` directly on a 60s cadence (line 162 of `orchestrator.js`). No knowledge of the new substrate/worker split.

**Goal:** Orchestrator calls CK as HSM interface. No changes needed to call site since CK interface (triggerReconciliation) is unchanged.

**What may be needed:**
- Confirm `reconciliationSubstrate.init(constitutional)` is called at boot if substrate needs CK reference
- Confirm `reconciliation-fsm.js` is registered with CK before `triggerReconciliation()` is called
- Confirm boot order: `CK.registerDomain()` for all 4 domains → `CK.rehydrate()` → `cadence.every(60s, CK.triggerReconciliation)`

**Files to verify:** `orchestrator.js`, `runtime-simulator.js`

---

### Phase 4: CK Module Cleanup and Final Line Count Target

**Current state:** CK is 1349 lines. The plan target was ~920 lines after all phases.

**Goal:** Get CK to ~920 lines by removing remaining reconciliation concerns.

**What else can be removed from CK:**
- The `MIN_RECON_INTERVAL_MS` / `_lastReconTriggeredAt` anti-thrash vars (moves to FSM in Phase 2)
- `_reconInProgress` guard — consider if this is still needed after FSM owns the trigger; FSM already has IDLE guard
- The FSM IDLE check in `triggerReconciliation()` — if FSM owns trigger criteria, this is redundant with FSM's own guard
- Any remaining reconciliation FSM state checks in CK (`_domains.get('reconciliation')` calls)

**After Phase 2 + 3:** CK should be close to 920 lines with only:
- dispatch()
- validateDomainTransition()
- registerDomain()
- triggerReconciliation() — thin wrapper calling FSM via dispatch
- _detectConstitutionalDeath() + _triggerConstitutionalDeath()
- rehydrate() + startLoop() + tick()
- subscribeAction() / onAction()
- OBSERVABILITY exports

---

## Known Pre-Existing Failures

These are NOT caused by Phase 1. They existed before:

**phase-6-telemetry-coordination-fsm.test.js:**
- T12: restart replay convergence, `waitForLedgerEntryCount(1, 15000)` timed out
- 45-minute soak: `monitorReport.violationCount = 3` (3 runtime violations detected)

**Root cause unknown** — `runtime-monitor.js` only records the first violation description. The specific violation strings from the soak test have not been extracted. Likely causes:
- Ingress lag violations (T12)
- Drift detection during soak (3 violations from runtime-monitor)
- FSM state regressions during extended run

---

## What NOT To Change

1. **Engine cognitive blindness** — `reconciliation-engine.js` must never receive `dispatch`, `checkpointer`, `canCheckpointFn`, `currentState`, or any governance/topology context. It is a deterministic comparison primitive only.

2. **Worker execution-blind invariant** — `reconciliation-worker.js` receives `{ entries, fsms, substrates }` only. No dispatch, no checkpointer, no lineageLedger reference (lineageLedger is accessed via engine.compare which receives it as a parameter, not as an authority artifact).

3. **Substrate owns authority artifacts** — checkpoint creation, canCheckpoint evaluation, and substrate query building all live in `reconciliation-substrate.js`. Worker never touches these.

4. **CK remains HSM** — CK dispatches all FSM transitions. No autonomous FSM operations that bypass CK dispatch.

---

## File Map (Current State)

```
control-plane/governance/
├── constitutional-kernel.js       1349 lines  (HSM authority — thin wrapper)
├── reconciliation-substrate.js    182 lines  (NEW — owns authority artifacts)
├── reconciliation-worker.js        52 lines  (NEW — execution-blind)
├── reconciliation-engine.js       656 lines  (cognitively blind — unchanged)
├── domains/
│   └── reconciliation-fsm.js     469 lines  (owns lifecycle + trigger criteria [Phase 2])
├── lineage-ledger.js             (unchanged)
└── lineage-checkpointer.js       (unchanged)
```

---

## Next Agent Checklist

- [ ] Read this document fully
- [ ] Read `reconciliation-substrate.js` (182 lines) to understand current substrate API
- [ ] Read `reconciliation-worker.js` (52 lines) to understand execution-blind invariant
- [ ] Read `constitutional-kernel.js` triggerReconciliation() to understand CK→substrate flow
- [ ] Phase 2: Move MIN_RECON_INTERVAL_MS and anti-thrash logic from CK to FSM
- [ ] Phase 2: Remove _reconInProgress, FSM IDLE check, anti-thrash vars from CK
- [ ] Phase 3: Verify orchestrator boot order
- [ ] Phase 4: Confirm CK line count reaches ~920 target
- [ ] Do NOT run tests until all phases are complete — user said "don't care about tests"
- [ ] Syntax check all modified files before declaring done: `node --check <file>`