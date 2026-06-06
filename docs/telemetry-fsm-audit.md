# Telemetry FSM — Audit Report

**Date:** 2026-06-06
**Auditor:** Hermes Agent
**Scope:** `telemetry-kernel/` + `control-plane/governance/domains/telemetry-coordination-fsm.js`

---

## Pipeline Architecture (as implemented)

```
5 Projection Workers (health / integrity / authority / runtime / systemic)
    ↓ _emitProjectionTransition() → PROJECTION_INTENT
    ↓ observability.onWrite() fires (setImmediate trigger)
    ↓ FSM _triggerReactiveCoordination()
    ↓ FSM _readIntents() → getDomainEntriesSince()
    ↓ FSM _validateIntents() / _orderIntents() / _serializeIntent()
    ↓ FSM _emitTransition() → SEMANTIC_PROJECTION_TRANSITION
    ↓ observability.onWrite() fires again
    ↓ 5 Transition Writers (namespace-filtered)
    ↓ lineageLedger.recordWorkerEntry()
    ↓ CK.dispatch(PROJECTION_PERSISTED) [fire-and-forget]
    ↓ CK async validation → PROJECTION_ACCEPTED
    ↓ namespace-projection-interpreter
```

---

## FSM Local States

| State | Description | Status |
|---|---|---|
| `IDLE` | Awaiting CK PROCESS_INTENTS tick | LIVE |
| `HALTED` | CK-ordered halt | LIVE |
| `INGRESS_LAG_RETRYING` | Lag detected, retry active | LIVE |
| `INGRESS_ESCALATED` | 3+ retries in 60s | LIVE |
| `INGRESS_DEGRADED` | 6+ retries in 60s | LIVE |
| `WORKER_DEGRADED` | Referenced in TRANSITION_WRITER_HEALTH_CHANGED | **DEAD — not in STATE_REGISTRY** |

---

## Critical Issues (blocking / logic-breaking)

### 1. DOMAIN MISMATCH — FSM name vs CK routing authority

FSM exports: `name: 'telemetry-coordination'`
CK routes `PROJECTION_PERSISTED` → `'telemetry-coordination'` ✓

BUT `MEMBRANE_AUTHORITY_MAP` in CK:
```js
'telemetry-worker': ['telemetry'],
```

The FSM writes authority as `'telemetry-coordination-fsm'` in `_serializeIntent()`. The CK has **no membrane entry for this authority**. CK's `validateDomainTransition()` will fail closed on any cross-domain mutation attempt from this FSM.

**Fix:** Add `'telemetry-coordination-fsm': ['telemetry']` to `MEMBRANE_AUTHORITY_MAP`, or change FSM authority to match an existing entry.

---

### 2. FSM REACTIVE MODE ONWRITE — WRONG DOMAIN FILTER (ALL5 WORKERS DEAD)

`base-projection-worker.js` line 211:
```js
domain: this._domain,  // base class default = 'projection'
```

Each worker overrides `_domain` to return `'health'`, `'integrity'`, etc. — but the `_emitProjectionTransition()` call in `_tick()` uses `this._domain`, which is `'projection'` from the base class. The emitted transition has `domain: 'projection'`.

The worker's `start()` subscribes filtering on:
```js
if (transition.domain !== domain) return;  // domain = 'health' etc.
```

Since emitted transitions have `domain: 'projection'`, the subscription **never matches**. Event-driven tick triggering is completely dead for all 5 workers. Workers fall back to `setInterval` polling on every tick.

**Fix:** Change `domain: this._domain` in `_emitProjectionTransition()` to use the worker's overridden `_domain` getter (which correctly returns `'health'`, etc.).

---

### 3. FSM DOUBLE ONWRITE — COORDINATION LOOP RACE

FSM `start()` subscribes to `observability.onWrite(_onTransitionLogWrite)`.
`_onTransitionLogWrite` filters for `nextState === 'PROJECTION_INTENT'` and calls `_triggerReactiveCoordination()`.
`_triggerReactiveCoordination()` uses `setImmediate()` which fires **after** the current event loop tick.

But `_emitTransition()` (inside `buildActions`) calls `observability.transition()` which **synchronously fires ALL onWrite subscribers**.

Result: the FSM's own onWrite callback fires **again** for the transition it just emitted, causing a second `_triggerReactiveCoordination()` call in the same cycle. `_coordinationPending` guard blocks the second actual dispatch, but the guard is timing-dependent, not architecturally prevented.

**Fix:** Add a cycle-scoped `_emitting` flag inside `dispatch()` that suppresses the onWrite callback for transitions emitted by the FSM itself during `buildActions`.

---

### 4. DUAL LINEAGE WRITE PATH — NO ARBITRATION

**Path A** (controlled): projection-worker → FSM → transition-writer → lineage ledger

**Path B** (uncontrolled): engagement substrate → direct `persist()` to lineage ledger

Both paths write to the same `lineage:ledger:entries` Redis key. There is:
- No sequence number
- No idempotency key
- No mutex

If both fire in the same cycle, ledger entries are interleaved. The CK's `PROJECTION_PERSISTED` flow assumes Path A is the only writer.

**Fix:** Introduce an idempotency key (e.g., `projectionId`) on Path A entries. Path B must either be removed or use the same FSM serialization path.

---

### 5. TRANSITION WRITER CK.DISPATCH — FIRE-AND-FORGET WITH NO TRACKING

```js
CK.dispatch({ type: 'PROJECTION_PERSISTED', ledgerId, entry }).catch(async (err) => {
  const category = _classifyError(err);
  _failedWrites++;
  // ...
});
```

The `.catch()` is on the **promise returned by `CK.dispatch()`**. If `CK.dispatch()` throws synchronously (which it can — CK validates every event), the promise is rejected and the catch fires. This is fine.

BUT: `_ckDispatchFailures` counter in `base-transition-writer.js` is **never incremented** from this path. The counter exists on the writer health object but is only written from a different code path (not shown in this file). If CK is overloaded or down, every projection in that cycle is silently dropped from the async validation path, and the failure is invisible in health metrics.

**Fix:** Increment `_ckDispatchFailures` in the catch block. Add a `ckDispatchFailures` increment in the transition-writer catch handler.

---

### 6. FSM BUILDACTIONS ASYNC VOID — PROCESS_INTENTS

```js
const actions = txn.buildActions ? txn.buildActions(event, ctx) : [];
```

`buildActions` is `async` but called **synchronously**. If `buildActions` throws inside an `await`, the error propagates as an unhandled promise rejection. The `try/catch` in `dispatch()` only wraps the sync parts:

```js
try {
  // ... obs.transition() sync call only
} catch (_) {}

return { allowed: true, from: priorState, to: target, actions };
```

The async `_readIntents()`, `_persistCursors()`, and loop over `_emitTransition()` are **not protected**.

**Fix:** Await `buildActions()` inside `dispatch()`, or wrap the entire `buildActions` call in try/catch.

---

### 7. PROCESS_INTENTS BACKPRESSURE — FLAG STUCK FOREVER

```js
if (intents.length > MAX_BUFFERED_INTENTS) {
  _backpressureSignaled = true;
  ctx.dispatchGlobal({ type: 'BACKPRESSURE_DETECTED', ... });
}
```

`_backpressureSignaled` is set to `true` when saturation is detected. It is cleared only when:
```js
if (_backpressureSignaled && emittedCount > 0) {
  _backpressureSignaled = false;
}
```

If `emittedCount === 0` (all intents rejected, or zero intents), `_backpressureSignaled` stays `true` forever. The next cycle with0 intents will NOT re-enter the saturation branch, so the flag is stuck.

**Fix:** Clear `_backpressureSignaled` when `intents.length === 0` (no work to do, no backpressure to report), or clear it unconditionally at the start of each cycle.

---

### 8. FSM CURSOR PERSISTENCE — NO RETRY, SILENT FAILURE

```js
async function _persistCursors(newCursors) {
  try {
    // ...
    await pipeline.exec();
  } catch (err) {
    console.error('[telemetry-coordination-fsm] _persistCursors error:', err.message);
    // returns silently — cursor update is lost
  }
}
```

If Redis is temporarily unavailable during `_persistCursors()`, the FSM will resume from the **old cursor** on restart, causing a replay storm. No circuit breaker, no write-ahead log, no secondary persistence.

**Fix:** Add a retry with exponential backoff. On final failure, emit a `CURSOR_PERSISTENCE_FAILED` event via `ctx.dispatchGlobal()` so CK is aware.

---

### 9. INGRESS_LAG_WORKER — REQUIRE PATH WRONG

```js
const transitionWriters = require('../../substrates/projection/transition-writers');
```

File is at: `telemetry-kernel/substrates/ingress-lag-worker.js`

`../../substrates/` from there resolves to: `telemetry-kernel/substrates/`

So the require resolves to: `telemetry-kernel/substrates/substrates/projection/transition-writers`

**Double `substrates` — this require will fail at runtime.**

**Fix:** Change to `require('../projection/transition-writers')`.

---

## Moderate Issues (functional but fragile)

### 10. FSM NAME EXPORT vs AUTHORITY STRING INCONSISTENCY

FSM exports: `name: 'telemetry-coordination'`
FSM emits authority: `'telemetry-coordination-fsm'`

These are two different strings. If CK or any consumer logs/tracks by `fsm.name`, the FSM's own emitted authority won't match. Low severity but confusing.

**Fix:** Unify to one canonical name.

---

### 11. NO FSM LIFECYCLE IN SERVER.JS

No evidence in `server.js` of:
- `telemetryKernel.fsm.init(rehydratedState)`
- `telemetryKernel.fsm.start(ctx)`
- `telemetryKernel.fsm.stop()`

If these are not called:
- `init()` → consumer not registered, cursors not restored
- `start()` → reactive onWrite mode never activated
- `stop()` → onWrite hook never cleaned up

The FSM only processes CK-cadence `PROCESS_INTENTS` events if `start()` is not called.

**Fix:** Wire FSM lifecycle into server.js boot sequence alongside CK and observability.

---

### 12. TRANSITION WRITER START() — LAZY REQUIRE AT CALL TIME

```js
function start() {
  const CK = require('../../../../control-plane/governance/constitutional-kernel');
  const lineageLedger = require('../../../../control-plane/governance/lineage-ledger');
  const observability = require('../../../../control-plane/observability');
```

Requires are inside `start()`, not at module load. This avoids circular dependency at load time but means every `startAll()` call re-requires the same modules. Harmless but unusual.

**Fix:** Move requires to module scope with lazy initialization pattern if circular dependency is the concern.

---

### 13. FSM `_obs()` LAZY IMPORT — DISAPPEARS ON FIRST ERROR

```js
function _obs() {
  if (!_observability) {
    try { _observability = _require('...'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}
```

If the require fails once, `_observability` stays `null` forever. No retry. `_emitTransition()` will silently return `false` on every subsequent call until process restart.

**Fix:** On require failure, schedule a retry on next coordination cycle instead of caching `null`.

---

### 14. FSM CURSOR READ — DUAL CONSUMPTION PATH RISK

FSM reads intents via `_readIntents()` which uses `getDomainEntriesSince()` (cursor-based).
FSM also subscribes to `onWrite()` which triggers `PROCESS_INTENTS` reactively.

If CK cadence fires `PROCESS_INTENTS` AND reactive mode fires `TELEMETRY_PROCESS_INTENTS` in the same cycle, intents could be double-processed. The cursor advances after reading, so the second call gets zero intents — but `_serializedTransitionCount` is double-incremented.

**Fix:** Deduplicate by `traceId` in the intent list before ordering, or use a cycle-scoped `seenTraceIds` Set.

---

### 15. NO FSM STATE SNAPSHOT ON SHUTDOWN

`stop()` only unsubscribes the onWrite hook. `_intentCursors`, `_retryAttempts`, `_retryEscalationState`, `_localState` are all in-memory. On restart, `_localState` defaults to `'IDLE'`. If the FSM was `HALTED` or `INGRESS_DEGRADED` at shutdown, that state is lost.

`init(rehydratedState)` exists but is never called by anything.

**Fix:** Call `ctx.dispatchGlobal({ type: 'FSM_STATE_SNAPSHOT_REQUESTED' })` before shutdown, and persist full state to Redis. Restore on boot.

---

### 16. DETERMINISTIC RECYCLE — NESTED CALL RISK

```js
if (currentStatus === 'DEGRADED' && !_recycleScheduled) {
  _recycleScheduled = true;
  setTimeout(() => { _deterministicRecycle()... }, 5000);
}
```

`_deterministicRecycle()` calls `getHealth()` internally (line 61). If the recycle takes >5s and the health is still `DEGRADED`, the nested `getHealth()` call could schedule a **second** recycle. The `_recycleScheduled` guard only works across external calls, not within the recycle function itself.

**Fix:** Set `_recycleScheduled = false` at the start of `_deterministicRecycle()`, not at the end. Or use a mutex flag that protects the entire recycle lifecycle.

---

## Issue Priority Summary

| # | Issue | Severity | File |
|---|---|---|---|
| 1 | Domain mismatch — FSM authority not in CK membrane map | CRITICAL | constitutional-kernel.js |
| 2 | Domain mismatch — workers emit `domain: 'projection'` not per-namespace | CRITICAL | base-projection-worker.js |
| 3 | Double onWrite — FSM triggers itself during own emission | CRITICAL | fsm.js |
| 4 | Dual lineage write path — no arbitration | CRITICAL | lineage-ledger / engagement substrate |
| 5 | CK.dispatch fire-and-forget — failures invisible in health metrics | CRITICAL | base-transition-writer.js |
| 6 | buildActions async void — unhandled rejections | CRITICAL | fsm.js |
| 7 | Backpressure flag stuck forever | CRITICAL | fsm.js |
| 8 | Cursor persistence silent failure — replay storm risk | CRITICAL | fsm.js |
| 9 | ingress-lag-worker require path double-substrates | CRITICAL | ingress-lag-worker.js |
| 10 | FSM name vs authority string inconsistency | MODERATE | fsm.js |
| 11 | No FSM lifecycle wired in server.js | MODERATE | server.js |
| 12 | Lazy requires inside start() | MODERATE | base-transition-writer.js |
| 13 | _obs() caches null forever on first failure | MODERATE | fsm.js |
| 14 | Dual intent consumption path — double-processing risk | MODERATE | fsm.js |
| 15 | No FSM state snapshot on shutdown | MODERATE | fsm.js |
| 16 | Deterministic recycle nested call risk | MODERATE | transition-writers/index.js |
